use crate::cmp::{Cmp, MemtableKeyCmp};
use crate::rand::rngs::StdRng;
use crate::rand::{RngCore, SeedableRng};
use crate::types::LdbIterator;

use bytes::Bytes;
use std::cmp::Ordering;
use std::mem::size_of;
use std::sync::Arc;
use std::sync::RwLock;

const MAX_HEIGHT: usize = 12;
const BRANCHING_FACTOR: u32 = 4;

/// A node in a skipmap contains links to the next node and others that are further away (skips);
/// `skips[0]` is the immediate element after.
struct Node {
    skips: [Option<usize>; MAX_HEIGHT],
    key: Bytes,
    value: Bytes,
}

/// Implements the backing store for a `MemTable`. The important methods are `insert()` and
/// `contains()`; in order to get full key and value for an entry, use a `SkipMapIter` instance,
/// `seek()` to the key to look up (this is as fast as any lookup in a skip map), and then call
/// `current()`.
struct InnerSkipMap {
    nodes: Vec<Node>,
    head: usize,
    rand: StdRng,
    len: usize,
    // approximation of memory used.
    approx_mem: usize,
    cmp: Arc<Box<dyn Cmp>>,
}

pub struct SkipMap {
    map: Arc<RwLock<InnerSkipMap>>,
}

impl SkipMap {
    /// Returns a SkipMap that wraps the comparator inside a MemtableKeyCmp.
    pub fn new_memtable_map(cmp: Arc<Box<dyn Cmp>>) -> SkipMap {
        SkipMap::new(Arc::new(Box::new(MemtableKeyCmp(cmp))))
    }

    /// Returns a SkipMap that uses the specified comparator.
    pub fn new(cmp: Arc<Box<dyn Cmp>>) -> SkipMap {
        let head_node = Node {
            skips: [None; MAX_HEIGHT],
            key: Bytes::new(),
            value: Bytes::new(),
        };

        SkipMap {
            map: Arc::new(RwLock::new(InnerSkipMap {
                nodes: vec![head_node],
                head: 0,
                rand: StdRng::seed_from_u64(0xdeadbeef),
                len: 0,
                approx_mem: size_of::<Self>() + size_of::<InnerSkipMap>() + size_of::<Node>(),
                cmp,
            })),
        }
    }

    pub fn len(&self) -> usize {
        self.map.read().unwrap().len
    }

    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    pub fn approx_memory(&self) -> usize {
        self.map.read().unwrap().approx_mem
    }

    pub fn contains(&self, key: &[u8]) -> bool {
        self.map.read().unwrap().contains(key)
    }

    /// Inserts a key into the table. `key` may not be empty.
    pub fn insert(&mut self, key: Vec<u8>, val: Vec<u8>) {
        assert!(!key.is_empty());
        self.map.write().unwrap().insert(key, val);
    }

    pub fn iter(&self) -> SkipMapIter {
        SkipMapIter {
            map: self.map.clone(),
            current: 0, // head
        }
    }
}

impl InnerSkipMap {
    fn random_height(&mut self) -> usize {
        let mut height = 1;
        while height < MAX_HEIGHT && self.rand.next_u32().is_multiple_of(BRANCHING_FACTOR) {
            height += 1;
        }
        height
    }

    fn contains(&self, key: &[u8]) -> bool {
        if let Some(n_idx) = self.get_greater_or_equal(key) {
            self.nodes[n_idx].key.starts_with(key)
        } else {
            false
        }
    }

    /// Returns the node with key or the next greater one
    /// Returns None if the given key lies past the greatest key in the table.
    fn get_greater_or_equal(&self, key: &[u8]) -> Option<usize> {
        // Start at the highest skip link of the head node, and work down from there
        let mut current = self.head;
        let mut level = self.nodes[self.head].skips.len() - 1;

        loop {
            if let Some(next) = self.nodes[current].skips[level] {
                let ord = self.cmp.cmp(self.nodes[next].key.iter().as_slice(), key);

                match ord {
                    Ordering::Less => {
                        current = next;
                        continue;
                    }
                    Ordering::Equal => return Some(next),
                    Ordering::Greater => {
                        if level == 0 {
                            return Some(next);
                        }
                    }
                }
            }
            if level == 0 {
                break;
            }
            level -= 1;
        }

        if current == self.head || self.cmp.cmp(&self.nodes[current].key, key) == Ordering::Less {
            None
        } else {
            Some(current)
        }
    }

    /// Finds the node immediately before the node with key.
    /// Returns None if no smaller key was found.
    fn get_next_smaller(&self, key: &[u8]) -> Option<usize> {
        // Start at the highest skip link of the head node, and work down from there
        let mut current = self.head;
        let mut level = self.nodes[self.head].skips.len() - 1;

        loop {
            if let Some(next) = self.nodes[current].skips[level] {
                let ord = self.cmp.cmp(self.nodes[next].key.iter().as_slice(), key);

                if let Ordering::Less = ord {
                    current = next;
                    continue;
                }
            }

            if level == 0 {
                break;
            }
            level -= 1;
        }

        if current == self.head {
            // If we're past the end for some reason or at the head
            None
        } else if self.cmp.cmp(&self.nodes[current].key, key) != Ordering::Less {
            None
        } else {
            Some(current)
        }
    }

    fn insert(&mut self, key: Vec<u8>, val: Vec<u8>) {
        assert!(!key.is_empty());

        // Keeping track of skip entries that will need to be updated
        let mut prevs = [0; MAX_HEIGHT];
        let new_height = self.random_height();

        let mut level = MAX_HEIGHT - 1;
        let mut current = self.head;

        // Set previous node for all levels to current node.
        for prev in prevs.iter_mut().take(new_height) {
            *prev = current;
        }

        // Find the node after which we want to insert the new node; this is the node with the key
        // immediately smaller than the key to be inserted.
        loop {
            if let Some(next) = self.nodes[current].skips[level] {
                // If the wanted position is after the current node
                let ord = self.cmp.cmp(self.nodes[next].key.iter().as_slice(), &key);
                assert!(ord != Ordering::Equal, "No duplicates allowed");

                if ord == Ordering::Less {
                    current = next;
                    continue;
                }
            }

            if level < new_height {
                prevs[level] = current;
            }

            if level == 0 {
                break;
            } else {
                level -= 1;
            }
        }

        // Construct new node
        let new_idx = self.nodes.len();

        let new_node = Node {
            skips: [None; MAX_HEIGHT],
            key: key.into(),
            value: val.into(),
        };

        let added_mem = size_of::<Node>() + new_node.key.len() + new_node.value.len();
        self.approx_mem += added_mem;
        self.len += 1;

        self.nodes.push(new_node);

        // Insert new node by first replacing the previous element's next field with None and
        // assigning its value to new.next...
        // ...and then setting the previous element's next field to the new node
        for (i, &prev) in prevs.iter().enumerate().take(new_height) {
            self.nodes[new_idx].skips[i] = self.nodes[prev].skips[i];
            self.nodes[prev].skips[i] = Some(new_idx);
        }
    }

    /// Runs through the skipmap and prints everything including indices
    fn dbg_print(&self) {
        let mut current = self.head;
        loop {
            let current_node = &self.nodes[current];
            eprintln!(
                "{} {:?}/{:?} - {:?}",
                current, current_node.key, current_node.value, current_node.skips
            );
            if let Some(next) = current_node.skips[0] {
                current = next;
            } else {
                break;
            }
        }
    }
}

pub struct SkipMapIter {
    map: Arc<RwLock<InnerSkipMap>>,
    current: usize,
}

impl LdbIterator for SkipMapIter {
    fn advance(&mut self) -> bool {
        // we first go to the next element, then return that -- in order to skip the head node
        let r = {
            let map = self.map.read().unwrap();
            if let Some(next) = map.nodes[self.current].skips[0] {
                self.current = next;
                true
            } else {
                false
            }
        };
        if !r {
            self.reset();
        }
        r
    }
    fn reset(&mut self) {
        self.current = self.map.read().unwrap().head;
    }
    fn seek(&mut self, key: &[u8]) {
        let map = self.map.read().unwrap();
        if let Some(node_idx) = map.get_greater_or_equal(key) {
            self.current = node_idx;
            return;
        }
        drop(map);
        self.reset();
    }
    fn valid(&self) -> bool {
        self.current != self.map.read().unwrap().head
    }
    fn current(&self) -> Option<(Bytes, Bytes)> {
        if self.valid() {
            let map = self.map.read().unwrap();
            let node = &map.nodes[self.current];
            Some((
                Bytes::copy_from_slice(&node.key),
                Bytes::copy_from_slice(&node.value),
            ))
        } else {
            None
        }
    }
    fn prev(&mut self) -> bool {
        if self.valid() {
            // Going after the original implementation here; we just seek to the node before current().
            let map = self.map.read().unwrap();
            if let Some(prev_idx) = map.get_next_smaller(&map.nodes[self.current].key) {
                self.current = prev_idx;
                if !map.nodes[prev_idx].key.is_empty() {
                    return true;
                }
            }
        }
        self.reset();
        false
    }
}

#[cfg(test)]
pub mod tests {
    use super::*;
    use crate::cmp::MemtableKeyCmp;
    use crate::options;
    use crate::test_util::{test_iterator_properties, LdbIteratorIter};
    use crate::types::current_key_val;

    pub fn make_skipmap() -> SkipMap {
        let mut skm = SkipMap::new(options::for_test().cmp);
        let keys = vec![
            "aba", "abb", "abc", "abd", "abe", "abf", "abg", "abh", "abi", "abj", "abk", "abl",
            "abm", "abn", "abo", "abp", "abq", "abr", "abs", "abt", "abu", "abv", "abw", "abx",
            "aby", "abz",
        ];

        for k in keys {
            skm.insert(k.as_bytes().to_vec(), b"def".to_vec());
        }
        skm
    }

    #[test]
    fn test_insert() {
        let skm = make_skipmap();
        assert_eq!(skm.len(), 26);
        skm.map.read().unwrap().dbg_print();
    }

    #[test]
    #[should_panic]
    fn test_no_dupes() {
        let mut skm = make_skipmap();
        // this should panic
        skm.insert(b"abc".to_vec(), b"def".to_vec());
        skm.insert(b"abf".to_vec(), b"def".to_vec());
    }

    #[test]
    fn test_contains() {
        let skm = make_skipmap();
        assert!(skm.contains(b"aby"));
        assert!(skm.contains(b"abc"));
        assert!(skm.contains(b"abz"));
        assert!(!skm.contains(b"ab{"));
        assert!(!skm.contains(b"123"));
        assert!(!skm.contains(b"aaa"));
        assert!(!skm.contains(b"456"));
    }

    #[test]
    fn test_find() {
        let skm = make_skipmap();
        assert_eq!(
            skm.map.read().unwrap().nodes[skm
                .map
                .read()
                .unwrap()
                .get_greater_or_equal(b"abf")
                .unwrap()]
            .key
            .as_ref(),
            b"abf"
        );
        assert!(skm
            .map
            .read()
            .unwrap()
            .get_greater_or_equal(b"ab{")
            .is_none());
        assert_eq!(
            skm.map.read().unwrap().nodes[skm
                .map
                .read()
                .unwrap()
                .get_greater_or_equal(b"aaa")
                .unwrap()]
            .key
            .as_ref(),
            b"aba"
        );
        assert_eq!(
            skm.map.read().unwrap().nodes
                [skm.map.read().unwrap().get_greater_or_equal(b"ab").unwrap()]
            .key
            .as_ref(),
            b"aba"
        );
        assert_eq!(
            skm.map.read().unwrap().nodes[skm
                .map
                .read()
                .unwrap()
                .get_greater_or_equal(b"abc")
                .unwrap()]
            .key
            .as_ref(),
            b"abc"
        );
        assert!(skm.map.read().unwrap().get_next_smaller(b"ab0").is_none());
        assert_eq!(
            skm.map.read().unwrap().nodes
                [skm.map.read().unwrap().get_next_smaller(b"abd").unwrap()]
            .key
            .as_ref(),
            b"abc"
        );
        assert_eq!(
            skm.map.read().unwrap().nodes
                [skm.map.read().unwrap().get_next_smaller(b"ab{").unwrap()]
            .key
            .as_ref(),
            b"abz"
        );
    }

    #[test]
    fn test_empty_skipmap_find_memtable_cmp() {
        // Regression test: Make sure comparator isn't called with empty key.
        let cmp: Arc<Box<dyn Cmp>> = Arc::new(Box::new(MemtableKeyCmp(options::for_test().cmp)));
        let skm = SkipMap::new(cmp);

        let mut it = skm.iter();
        it.seek(b"abc");
        assert!(!it.valid());
    }

    #[test]
    fn test_skipmap_iterator_0() {
        let skm = SkipMap::new(options::for_test().cmp);
        let mut i = 0;

        for (_, _) in LdbIteratorIter::wrap(&mut skm.iter()) {
            i += 1;
        }

        assert_eq!(i, 0);
        assert!(!skm.iter().valid());
    }

    #[test]
    fn test_skipmap_iterator_init() {
        let skm = make_skipmap();
        let mut iter = skm.iter();

        assert!(!iter.valid());
        iter.next();
        assert!(iter.valid());
        iter.reset();
        assert!(!iter.valid());

        iter.next();
        assert!(iter.valid());
        iter.prev();
        assert!(!iter.valid());
    }

    #[test]
    fn test_skipmap_iterator() {
        let skm = make_skipmap();
        let mut i = 0;

        for (k, v) in LdbIteratorIter::wrap(&mut skm.iter()) {
            assert!(!k.is_empty());
            assert!(!v.is_empty());
            i += 1;
        }
        assert_eq!(i, 26);
    }

    #[test]
    fn test_skipmap_iterator_seek_valid() {
        let skm = make_skipmap();
        let mut iter = skm.iter();

        iter.next();
        assert!(iter.valid());
        assert_eq!(current_key_val(&iter).unwrap().0, b"aba".to_vec());
        iter.seek(b"abz");
        assert_eq!(
            current_key_val(&iter).unwrap(),
            (b"abz".to_vec(), b"def".to_vec())
        );
        // go back to beginning
        iter.seek(b"aba");
        assert_eq!(
            current_key_val(&iter).unwrap(),
            (b"aba".to_vec(), b"def".to_vec())
        );

        iter.seek(b"");
        assert!(iter.valid());
        iter.prev();
        assert!(!iter.valid());

        while iter.advance() {}
        assert!(!iter.valid());
        assert!(!iter.prev());
        assert_eq!(current_key_val(&iter), None);
    }

    #[test]
    fn test_skipmap_behavior() {
        let mut skm = SkipMap::new(options::for_test().cmp);
        let keys = vec!["aba", "abb", "abc", "abd"];
        for k in keys {
            skm.insert(k.as_bytes().to_vec(), b"def".to_vec());
        }
        test_iterator_properties(skm.iter());
    }

    #[test]
    fn test_skipmap_iterator_prev() {
        let skm = make_skipmap();
        let mut iter = skm.iter();

        iter.next();
        assert!(iter.valid());
        iter.prev();
        assert!(!iter.valid());
        iter.seek(b"abc");
        iter.prev();
        assert_eq!(
            current_key_val(&iter).unwrap(),
            (b"abb".to_vec(), b"def".to_vec())
        );
    }

    #[test]
    fn test_skipmap_iterator_concurrent_insert() {
        time_test!();
        // Asserts that the map can be mutated while an iterator exists; this is intentional.
        let mut skm = make_skipmap();
        let mut iter = skm.iter();

        assert!(iter.advance());
        skm.insert(b"abccc".to_vec(), b"defff".to_vec());
        // Assert that value inserted after obtaining iterator is present.
        for (k, _) in LdbIteratorIter::wrap(&mut iter) {
            if k == b"abccc" {
                return;
            }
        }
        panic!("abccc not found in map.");
    }
}
