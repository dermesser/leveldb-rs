use std::collections::HashMap;

#[derive(Clone, Copy, Debug, PartialEq)]
pub struct LRUHandle(usize);

struct LRUNode<T> {
    next: Option<usize>,
    prev: Option<usize>,
    data: Option<T>, // None if it's the head node, or if it's a free slot
}

struct LRUList<T> {
    nodes: Vec<LRUNode<T>>,
    head: usize, // index of the dummy head node
    free_head: Option<usize>,
    count: usize,
}

impl<T> LRUList<T> {
    fn new() -> LRUList<T> {
        let nodes = vec![LRUNode {
            next: None,
            prev: None,
            data: None,
        }];
        LRUList {
            nodes,
            head: 0,
            free_head: None,
            count: 0,
        }
    }

    fn alloc_node(&mut self, data: T) -> usize {
        if let Some(free_idx) = self.free_head {
            self.free_head = self.nodes[free_idx].next;
            self.nodes[free_idx] = LRUNode {
                next: None,
                prev: None,
                data: Some(data),
            };
            free_idx
        } else {
            let idx = self.nodes.len();
            self.nodes.push(LRUNode {
                next: None,
                prev: None,
                data: Some(data),
            });
            idx
        }
    }

    fn free_node(&mut self, idx: usize) -> T {
        let data = self.nodes[idx].data.take().unwrap();
        self.nodes[idx].next = self.free_head;
        self.nodes[idx].prev = None;
        self.free_head = Some(idx);
        data
    }

    /// Inserts new element at front (least recently used element)
    fn insert(&mut self, elem: T) -> LRUHandle {
        let new_idx = self.alloc_node(elem);
        self.count += 1;

        let old_next = self.nodes[self.head].next;

        self.nodes[new_idx].prev = Some(self.head);
        self.nodes[new_idx].next = old_next;

        self.nodes[self.head].next = Some(new_idx);

        if let Some(old_next_idx) = old_next {
            self.nodes[old_next_idx].prev = Some(new_idx);
        } else {
            self.nodes[self.head].prev = Some(new_idx); // track tail in head's prev
        }

        LRUHandle(new_idx)
    }

    fn remove_last(&mut self) -> Option<T> {
        if self.count == 0 {
            return None;
        }
        let tail_idx = self.nodes[self.head].prev.unwrap();
        let prev_idx = self.nodes[tail_idx].prev.unwrap();

        self.nodes[prev_idx].next = None;
        if prev_idx == self.head {
            self.nodes[self.head].prev = None;
        } else {
            self.nodes[self.head].prev = Some(prev_idx);
        }

        self.count -= 1;
        Some(self.free_node(tail_idx))
    }

    fn remove(&mut self, node_handle: LRUHandle) -> T {
        let idx = node_handle.0;
        let prev_idx = self.nodes[idx].prev.unwrap();
        let next_idx = self.nodes[idx].next;

        self.nodes[prev_idx].next = next_idx;

        if let Some(next_idx) = next_idx {
            self.nodes[next_idx].prev = Some(prev_idx);
        } else {
            // Removing the tail
            if self.count > 1 {
                self.nodes[self.head].prev = Some(prev_idx);
            } else {
                self.nodes[self.head].prev = None;
            }
        }

        self.count -= 1;
        self.free_node(idx)
    }

    /// Reinserts the referenced node at the front.
    fn reinsert_front(&mut self, node_handle: &LRUHandle) {
        let idx = node_handle.0;
        let prev_idx = self.nodes[idx].prev.unwrap();
        let next_idx = self.nodes[idx].next;

        if prev_idx == self.head {
            return; // Already at front
        }

        // Remove from current position
        self.nodes[prev_idx].next = next_idx;
        if let Some(n) = next_idx {
            self.nodes[n].prev = Some(prev_idx);
        } else {
            // It was the tail
            self.nodes[self.head].prev = Some(prev_idx);
        }

        // Insert at front
        let old_next = self.nodes[self.head].next;
        self.nodes[idx].prev = Some(self.head);
        self.nodes[idx].next = old_next;
        self.nodes[self.head].next = Some(idx);

        if let Some(old_next_idx) = old_next {
            self.nodes[old_next_idx].prev = Some(idx);
        } else {
            self.nodes[self.head].prev = Some(idx);
        }
    }

    fn count(&self) -> usize {
        self.count
    }

    fn _testing_head_ref(&self) -> Option<&T> {
        if let Some(first) = self.nodes[self.head].next {
            self.nodes[first].data.as_ref()
        } else {
            None
        }
    }
}

pub type CacheKey = [u8; 16];
pub type CacheID = u64;
type CacheEntry<T> = (T, LRUHandle);

/// Implementation of `ShardedLRUCache`.
/// Based on a HashMap; the elements are linked in order to support the LRU ordering.
pub struct Cache<T> {
    // note: CacheKeys (Vec<u8>) are duplicated between list and map. If this turns out to be a
    // performance bottleneck, another layer of indirection™ can solve this by mapping the key
    // to a numeric handle that keys both list and map.
    list: LRUList<CacheKey>,
    map: HashMap<CacheKey, CacheEntry<T>>,
    cap: usize,
    id: u64,
}

impl<T> Cache<T> {
    pub fn new(capacity: usize) -> Cache<T> {
        assert!(capacity > 0);
        Cache {
            list: LRUList::new(),
            map: HashMap::with_capacity(1024),
            cap: capacity,
            id: 0,
        }
    }

    /// Returns an ID that is unique for this cache and that can be used to partition the cache
    /// among several users.
    pub fn new_cache_id(&mut self) -> CacheID {
        self.id += 1;
        self.id
    }

    /// How many the cache currently contains
    pub fn count(&self) -> usize {
        self.list.count()
    }

    /// The capacity of this cache
    pub fn cap(&self) -> usize {
        self.cap
    }

    /// Insert a new element into the cache. The returned `CacheHandle` can be used for further
    /// operations on that element.
    /// If the capacity has been reached, the least recently used element is removed from the
    /// cache.
    pub fn insert(&mut self, key: &CacheKey, elem: T) {
        if let Some((_, old_handle)) = self.map.remove(key) {
            self.list.remove(old_handle);
        }

        if self.list.count() >= self.cap {
            if let Some(removed_key) = self.list.remove_last() {
                assert!(self.map.remove(&removed_key).is_some());
            } else {
                panic!("could not remove_last(); bug!");
            }
        }

        let lru_handle = self.list.insert(*key);
        self.map.insert(*key, (elem, lru_handle));
    }

    /// Retrieve an element from the cache.
    /// If the element has been preempted from the cache in the meantime, this returns None.
    pub fn get<'a>(&'a mut self, key: &CacheKey) -> Option<&'a T> {
        if let Some((elem, lru_handle)) = self.map.get(key) {
            self.list.reinsert_front(lru_handle);
            Some(elem)
        } else {
            None
        }
    }

    /// Remove an element from the cache (for invalidation).
    pub fn remove(&mut self, key: &CacheKey) -> Option<T> {
        match self.map.remove(key) {
            None => None,
            Some((elem, lru_handle)) => {
                self.list.remove(lru_handle);
                Some(elem)
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::LRUList;
    use super::*;

    fn make_key(a: u8, b: u8, c: u8) -> CacheKey {
        [a, b, c, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0]
    }

    #[test]
    fn test_blockcache_cache_add_rm() {
        let mut cache = Cache::new(128);

        let h_123 = make_key(1, 2, 3);
        let h_521 = make_key(1, 2, 4);
        let h_372 = make_key(3, 4, 5);
        let h_332 = make_key(6, 3, 1);
        let h_899 = make_key(8, 2, 1);

        cache.insert(&h_123, 123);
        cache.insert(&h_332, 332);
        cache.insert(&h_521, 521);
        cache.insert(&h_372, 372);
        cache.insert(&h_899, 899);

        assert_eq!(cache.count(), 5);

        assert_eq!(cache.get(&h_123), Some(&123));
        assert_eq!(cache.get(&h_372), Some(&372));

        assert_eq!(cache.remove(&h_521), Some(521));
        assert_eq!(cache.get(&h_521), None);
        assert_eq!(cache.remove(&h_521), None);

        assert_eq!(cache.count(), 4);
    }

    #[test]
    fn test_blockcache_cache_capacity() {
        let mut cache = Cache::new(3);

        let h_123 = make_key(1, 2, 3);
        let h_521 = make_key(1, 2, 4);
        let h_372 = make_key(3, 4, 5);
        let h_332 = make_key(6, 3, 1);
        let h_899 = make_key(8, 2, 1);

        cache.insert(&h_123, 123);
        cache.insert(&h_332, 332);
        cache.insert(&h_521, 521);
        cache.insert(&h_372, 372);
        cache.insert(&h_899, 899);

        assert_eq!(cache.count(), 3);

        assert_eq!(cache.get(&h_123), None);
        assert_eq!(cache.get(&h_332), None);
        assert_eq!(cache.get(&h_521), Some(&521));
        assert_eq!(cache.get(&h_372), Some(&372));
        assert_eq!(cache.get(&h_899), Some(&899));
    }

    #[test]
    fn test_blockcache_lru_remove() {
        let mut lru = LRUList::<usize>::new();

        let h_56 = lru.insert(56);
        lru.insert(22);
        lru.insert(223);
        let h_244 = lru.insert(244);
        lru.insert(1111);
        let h_12 = lru.insert(12);

        assert_eq!(lru.count(), 6);
        assert_eq!(244, lru.remove(h_244));
        assert_eq!(lru.count(), 5);
        assert_eq!(12, lru.remove(h_12));
        assert_eq!(lru.count(), 4);
        assert_eq!(56, lru.remove(h_56));
        assert_eq!(lru.count(), 3);
    }

    #[test]
    fn test_blockcache_lru_1() {
        let mut lru = LRUList::<usize>::new();

        lru.insert(56);
        lru.insert(22);
        lru.insert(244);
        lru.insert(12);

        assert_eq!(lru.count(), 4);

        assert_eq!(Some(56), lru.remove_last());
        assert_eq!(Some(22), lru.remove_last());
        assert_eq!(Some(244), lru.remove_last());

        assert_eq!(lru.count(), 1);

        assert_eq!(Some(12), lru.remove_last());

        assert_eq!(lru.count(), 0);

        assert_eq!(None, lru.remove_last());
    }

    #[test]
    fn test_blockcache_lru_reinsert() {
        let mut lru = LRUList::<usize>::new();

        let handle1 = lru.insert(56);
        let handle2 = lru.insert(22);
        let handle3 = lru.insert(244);

        assert_eq!(lru._testing_head_ref().copied().unwrap(), 244);

        lru.reinsert_front(&handle1);

        assert_eq!(lru._testing_head_ref().copied().unwrap(), 56);

        lru.reinsert_front(&handle3);

        assert_eq!(lru._testing_head_ref().copied().unwrap(), 244);

        lru.reinsert_front(&handle2);

        assert_eq!(lru._testing_head_ref().copied().unwrap(), 22);

        assert_eq!(lru.remove_last(), Some(56));
        assert_eq!(lru.remove_last(), Some(244));
        assert_eq!(lru.remove_last(), Some(22));
    }

    #[test]
    fn test_blockcache_lru_reinsert_2() {
        let mut lru = LRUList::<usize>::new();

        let handles = [
            lru.insert(0),
            lru.insert(1),
            lru.insert(2),
            lru.insert(3),
            lru.insert(4),
            lru.insert(5),
            lru.insert(6),
            lru.insert(7),
            lru.insert(8),
        ];

        (0..9).for_each(|i| {
            lru.reinsert_front(&handles[i]);
            assert_eq!(lru._testing_head_ref().copied(), Some(i));
        });
    }

    #[test]
    fn test_blockcache_lru_edge_cases() {
        let mut lru = LRUList::<usize>::new();

        let handle = lru.insert(3);

        lru.reinsert_front(&handle);
        assert_eq!(lru._testing_head_ref().copied(), Some(3));
        assert_eq!(lru.remove_last(), Some(3));
        assert_eq!(lru.remove_last(), None);
        assert_eq!(lru.remove_last(), None);
    }

    #[test]
    fn test_cache_duplicate_insert() {
        let mut cache = Cache::new(2);
        let key = make_key(1, 0, 0);
        let key2 = make_key(2, 0, 0);
        let key3 = make_key(3, 0, 0);

        cache.insert(&key, 10);
        cache.insert(&key, 20); // duplicate
        cache.insert(&key2, 30);
        cache.insert(&key3, 40);

        assert_eq!(cache.get(&key), None); // key was evicted
        assert_eq!(cache.get(&key2), Some(&30));
        assert_eq!(cache.get(&key3), Some(&40));
    }
}
