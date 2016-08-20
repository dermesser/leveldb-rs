use block::BlockContents;

use std::collections::HashMap;
use std::mem::{replace, transmute_copy};


struct LRUNode<T> {
    next: Option<Box<LRUNode<T>>>,
    prev: Option<*mut LRUNode<T>>,
    data: Option<T>, // if None, then we have reached the head node
}

pub struct LRUList<T> {
    head: LRUNode<T>,
    count: usize,
}

impl<T> LRUList<T> {
    pub fn new() -> LRUList<T> {
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
    pub fn insert(&mut self, elem: T) {
        // Not first element
        if self.head.next.is_some() {
            // todo: replace next by head.next; set head.next to new; set next.prev to new
            let mut new = Box::new(LRUNode {
                data: Some(elem),
                next: None,
                prev: self.head.next.as_ref().clone().map(|next| next.prev).unwrap(),
            });

            unsafe {
                // Set up the node after the new one
                self.head.next.as_mut().unwrap().prev = transmute_copy(&new.as_mut());
                // Replace head.next with None and set the new node's next to that
                new.next = replace(&mut self.head.next, None);
                self.head.next = Some(new);
            }
        } else {
            // First node; the only node right now is an empty head node
            let mut new = Box::new(LRUNode {
                data: Some(elem),
                next: None,
                prev: unsafe { Some(transmute_copy(&&self.head)) },
            });
            // Set tail
            self.head.prev = unsafe { Some(transmute_copy(&new.as_mut())) };
            // Set first node
            self.head.next = Some(new);
        }
        self.count += 1;
    }

    pub fn remove_last(&mut self) -> Option<T> {
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

    pub fn count(&self) -> usize {
        self.count
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
    use super::*;

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
}
