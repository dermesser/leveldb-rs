use std::collections::HashMap;

// No clone, no copy! That asserts that an LRUHandle exists only once.
struct LRUHandle<T>(*mut LRUNode<T>);

struct LRUNode<T> {
    next: *mut LRUNode<T>,
    prev: *mut LRUNode<T>,
    data: Option<T>, // None only in the head (sentinel) node
}

/// Circular doubly linked list. Every node, including the sentinel `head`, is a heap allocation
/// owned through a raw pointer. Node addresses therefore stay valid when the list is moved, and
/// no `Box` or reference to a node is ever moved or kept while an `LRUHandle` points at it.
struct LRUList<T> {
    head: *mut LRUNode<T>,
    count: usize,
}

impl<T> LRUList<T> {
    fn new() -> LRUList<T> {
        let head = Box::into_raw(Box::new(LRUNode {
            next: std::ptr::null_mut(),
            prev: std::ptr::null_mut(),
            data: None,
        }));
        unsafe {
            (*head).next = head;
            (*head).prev = head;
        }
        LRUList { head, count: 0 }
    }

    /// Links `node` right after the head. `node` must not be linked.
    unsafe fn link_front(&mut self, node: *mut LRUNode<T>) {
        (*node).prev = self.head;
        (*node).next = (*self.head).next;
        (*(*node).next).prev = node;
        (*self.head).next = node;
    }

    /// Unlinks `node` from its neighbours. `node` must be linked and must not be the head.
    unsafe fn unlink(node: *mut LRUNode<T>) {
        (*(*node).prev).next = (*node).next;
        (*(*node).next).prev = (*node).prev;
    }

    /// Inserts new element at front (least recently used element)
    fn insert(&mut self, elem: T) -> LRUHandle<T> {
        let node = Box::into_raw(Box::new(LRUNode {
            next: std::ptr::null_mut(),
            prev: std::ptr::null_mut(),
            data: Some(elem),
        }));
        unsafe { self.link_front(node) };
        self.count += 1;
        LRUHandle(node)
    }

    fn remove_last(&mut self) -> Option<T> {
        if self.count() == 0 {
            return None;
        }
        unsafe {
            let last = (*self.head).prev;
            Self::unlink(last);
            self.count -= 1;
            Box::from_raw(last).data
        }
    }

    fn remove(&mut self, node_handle: LRUHandle<T>) -> T {
        unsafe {
            Self::unlink(node_handle.0);
            self.count -= 1;
            Box::from_raw(node_handle.0).data.unwrap()
        }
    }

    /// Reinserts the referenced node at the front.
    fn reinsert_front(&mut self, node_handle: &LRUHandle<T>) {
        unsafe {
            Self::unlink(node_handle.0);
            self.link_front(node_handle.0);
        }
    }

    fn count(&self) -> usize {
        self.count
    }

    fn _testing_head_ref(&self) -> Option<&T> {
        unsafe { (*(*self.head).next).data.as_ref() }
    }
}

impl<T> Drop for LRUList<T> {
    fn drop(&mut self) {
        unsafe {
            let mut node = (*self.head).next;
            while node != self.head {
                let next = (*node).next;
                drop(Box::from_raw(node));
                node = next;
            }
            drop(Box::from_raw(self.head));
        }
    }
}

pub type CacheKey = [u8; 16];
pub type CacheID = u64;
type CacheEntry<T> = (T, LRUHandle<CacheKey>);

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
        match self.map.get(key) {
            None => None,
            Some((elem, lru_handle)) => {
                self.list.reinsert_front(lru_handle);
                Some(elem)
            }
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
}
