use block::BlockContents;

use std::collections::HashMap;
use std::mem::{swap, replace, transmute_copy};

type LRUHandle<T> = *mut LRUNode<T>;

struct LRUNode<T> {
    next: Option<Box<LRUNode<T>>>, // None in the list's last node
    prev: Option<*mut LRUNode<T>>,
    data: Option<T>, // if None, then we have reached the head node
}

struct LRUList<T> {
    head: LRUNode<T>,
    count: usize,
}
/// This is likely unstable; more investigation is needed into correct behavior!
impl<T> LRUList<T> {
    fn new() -> LRUList<T> {
        LRUList {
            head: LRUNode {
                data: None,
                next: None,
                prev: None,
            },
            count: 0,
        }
    }

    /// Inserts new element at front (least recently used element)
    fn insert(&mut self, elem: T) -> LRUHandle<T> {
        self.count += 1;
        // Not first element
        if self.head.next.is_some() {
            let mut new = Box::new(LRUNode {
                data: Some(elem),
                next: None,
                prev: unsafe { Some(transmute_copy(&&self.head)) },
            });
            let newp = unsafe { transmute_copy(&new.as_mut()) };

            // Set up the node after the new one
            self.head.next.as_mut().unwrap().prev = Some(newp);
            // Replace head.next with None and set the new node's next to that
            new.next = replace(&mut self.head.next, None);
            self.head.next = Some(new);

            newp
        } else {
            // First node; the only node right now is an empty head node
            let mut new = Box::new(LRUNode {
                data: Some(elem),
                next: None,
                prev: unsafe { Some(transmute_copy(&&self.head)) },
            });
            let newp = unsafe { transmute_copy(&new.as_mut()) };

            // Set tail
            self.head.prev = Some(newp);
            // Set first node
            self.head.next = Some(new);

            newp
        }
    }

    fn remove_last(&mut self) -> Option<T> {
        if self.head.prev.is_some() {
            let mut lasto = unsafe {
                replace(&mut (*((*self.head.prev.unwrap()).prev.unwrap())).next,
                        None)
            };

            if let Some(ref mut last) = lasto {
                self.head.prev = last.prev;
                self.count -= 1;
                return replace(&mut (*last).data, None);
            } else {
                None
            }
        } else {
            None
        }
    }


    /// Reinserts the referenced node at the front.
    fn reinsert_front(&mut self, node_handle: LRUHandle<T>) {
        unsafe {
            let prevp = (*node_handle).prev.unwrap();

            // If not last node, update following node's prev
            if let Some(next) = (*node_handle).next.as_mut() {
                next.prev = Some(prevp);
            } else {
                // If last node, update head
                self.head.prev = Some(prevp);
            }

            // Swap this.next with prev.next. After that, this.next refers to this (!)
            swap(&mut (*prevp).next, &mut (*node_handle).next);
            // To reinsert at head, swap head's next with this.next
            swap(&mut (*node_handle).next, &mut self.head.next);
            // Update this' prev reference to point to head.

            // Update the second node's prev reference.
            if let Some(ref mut newnext) = (*node_handle).next {
                (*node_handle).prev = newnext.prev;
                newnext.prev = Some(node_handle);
            } else {
                // Only one node, being the last one; avoid head.prev pointing to head
                self.head.prev = Some(node_handle);
            }

            assert!(self.head.next.is_some());
            assert!(self.head.prev.is_some());
        }
    }

    fn count(&self) -> usize {
        self.count
    }

    fn _testing_head_ref(&self) -> Option<&T> {
        if let Some(ref first) = self.head.next {
            first.data.as_ref()
        } else {
            None
        }
    }
}

type CacheHandle = usize;

/// Implementation of `ShardedLRUCache`.
/// Based on a HashMap; the elements are linked in order to support the LRU ordering.
pub struct BlockCache {
    list: LRUList<CacheHandle>,
    map: HashMap<CacheHandle, BlockContents>,
    handle_counter: CacheHandle,
}

#[cfg(test)]
mod tests {
    use super::LRUList;

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

        assert_eq!(lru._testing_head_ref().map(|r| (*r)).unwrap(), 244);

        lru.reinsert_front(handle1);

        assert_eq!(lru._testing_head_ref().map(|r| (*r)).unwrap(), 56);

        lru.reinsert_front(handle3);

        assert_eq!(lru._testing_head_ref().map(|r| (*r)).unwrap(), 244);

        lru.reinsert_front(handle2);

        assert_eq!(lru._testing_head_ref().map(|r| (*r)).unwrap(), 22);

        assert_eq!(lru.remove_last(), Some(56));
        assert_eq!(lru.remove_last(), Some(244));
        assert_eq!(lru.remove_last(), Some(22));
    }

    #[test]
    fn test_blockcache_lru_reinsert_2() {
        let mut lru = LRUList::<usize>::new();

        let handles = vec![
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

        for i in 0..9 {
            lru.reinsert_front(handles[i]);
            assert_eq!(lru._testing_head_ref().map(|x| *x), Some(i));
        }
    }

    #[test]
    fn test_blockcache_lru_edge_cases() {
        let mut lru = LRUList::<usize>::new();

        let handle = lru.insert(3);

        lru.reinsert_front(handle);
        assert_eq!(lru._testing_head_ref().map(|x| *x), Some(3));
        assert_eq!(lru.remove_last(), Some(3));
        assert_eq!(lru.remove_last(), None);
        assert_eq!(lru.remove_last(), None);
    }
}
