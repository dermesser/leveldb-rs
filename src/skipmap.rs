
use cmp::MemtableKeyCmp;
use options::Options;
use types::LdbIterator;
use rand::{Rng, SeedableRng, StdRng};

use std::cmp::Ordering;
use std::mem::{replace, size_of};
use std::sync::Arc;

const MAX_HEIGHT: usize = 12;
const BRANCHING_FACTOR: u32 = 4;

/// A node in a skipmap contains links to the next node and others that are further away (skips);
/// `skips[0]` is the immediate element after, that is, the element contained in `next`.
struct Node {
    skips: Vec<Option<*mut Node>>,
    next: Option<Box<Node>>,
    key: Vec<u8>,
    value: Vec<u8>,
}

/// Implements the backing store for a `MemTable`. The important methods are `insert()` and
/// `contains()`; in order to get full key and value for an entry, use a `SkipMapIter` instance,
/// `seek()` to the key to look up (this is as fast as any lookup in a skip map), and then call
/// `current()`.
pub struct SkipMap {
    head: Box<Node>,
    rand: StdRng,
    len: usize,
    // approximation of memory used.
    approx_mem: usize,
    opt: Options,
}

impl SkipMap {
    /// Returns a SkipMap that wraps the comparator from opt inside a MemtableKeyCmp
    pub fn new_memtable_map(mut opt: Options) -> SkipMap {
        opt.cmp = Arc::new(Box::new(MemtableKeyCmp(opt.cmp.clone())));
        let skm = SkipMap::new(opt);
        skm
    }

    /// Returns a SkipMap that uses the comparator from opt
    pub fn new(opt: Options) -> SkipMap {
        let mut s = Vec::new();
        s.resize(MAX_HEIGHT, None);

        SkipMap {
            head: Box::new(Node {
                skips: s,
                next: None,
                key: Vec::new(),
                value: Vec::new(),
            }),
            rand: StdRng::from_seed(&[0xde, 0xad, 0xbe, 0xef]),
            len: 0,
            approx_mem: size_of::<Self>() + MAX_HEIGHT * size_of::<Option<*mut Node>>(),
            opt: opt,
        }
    }

    pub fn len(&self) -> usize {
        self.len
    }
    pub fn approx_memory(&self) -> usize {
        self.approx_mem
    }

    fn random_height(&mut self) -> usize {
        let mut height = 1;

        while height < MAX_HEIGHT && self.rand.next_u32() % BRANCHING_FACTOR == 0 {
            height += 1;
        }

        height
    }

    pub fn contains(&self, key: &[u8]) -> bool {
        if let Some(n) = self.get_greater_or_equal(key) {
            n.key.starts_with(&key)
        } else {
            false
        }
    }

    /// Returns the node with key or the next greater one
    /// Returns None if the given key lies past the greatest key in the table.
    fn get_greater_or_equal<'a>(&'a self, key: &[u8]) -> Option<&'a Node> {
        // Start at the highest skip link of the head node, and work down from there
        let mut current = self.head.as_ref() as *const Node;
        let mut level = self.head.skips.len() - 1;

        loop {
            unsafe {
                if let Some(next) = (*current).skips[level] {
                    let ord = self.opt.cmp.cmp((*next).key.as_slice(), key);

                    match ord {
                        Ordering::Less => {
                            current = next;
                            continue;
                        }
                        Ordering::Equal => return Some(&(*next)),
                        Ordering::Greater => {
                            if level == 0 {
                                return Some(&(*next));
                            }
                        }
                    }
                }
            }
            if level == 0 {
                break;
            }
            level -= 1;
        }

        unsafe {
            if current.is_null() {
                return None;
            } else if self.opt.cmp.cmp(&(*current).key, key) == Ordering::Less {
                return None;
            } else {
                return Some(&(*current));
            }
        }
    }

    /// Finds the node immediately before the node with key.
    /// Returns None if no smaller key was found.
    fn get_next_smaller<'a>(&'a self, key: &[u8]) -> Option<&'a Node> {
        // Start at the highest skip link of the head node, and work down from there
        let mut current = self.head.as_ref() as *const Node;
        let mut level = self.head.skips.len() - 1;

        loop {
            unsafe {
                if let Some(next) = (*current).skips[level] {
                    let ord = self.opt.cmp.cmp((*next).key.as_slice(), key);

                    match ord {
                        Ordering::Less => {
                            current = next;
                            continue;
                        }
                        _ => (),
                    }
                }
            }
            if level == 0 {
                break;
            }
            level -= 1;
        }

        unsafe {
            if current.is_null() || (*current).key.is_empty() {
                // If we're past the end for some reason or at the head
                return None;
            } else if self.opt.cmp.cmp(&(*current).key, key) != Ordering::Less {
                return None;
            } else {
                return Some(&(*current));
            }
        }
    }

    pub fn insert(&mut self, key: Vec<u8>, val: Vec<u8>) {
        assert!(!key.is_empty());

        // Keeping track of skip entries that will need to be updated
        let new_height = self.random_height();
        let mut prevs: Vec<Option<*mut Node>> = Vec::with_capacity(new_height);

        let mut level = MAX_HEIGHT - 1;
        let mut current = self.head.as_mut() as *mut Node;
        // Initialize all prevs entries with *head
        prevs.resize(new_height, Some(current));

        // Find the node after which we want to insert the new node; this is the node with the key
        // immediately smaller than the key to be inserted.
        loop {
            unsafe {
                if let Some(next) = (*current).skips[level] {
                    // If the wanted position is after the current node
                    let ord = self.opt.cmp.cmp(&(*next).key, &key);

                    assert!(ord != Ordering::Equal, "No duplicates allowed");

                    if ord == Ordering::Less {
                        current = next;
                        continue;
                    }
                }
            }

            if level < new_height {
                prevs[level] = Some(current);
            }

            if level == 0 {
                break;
            } else {
                level -= 1;
            }
        }

        // Construct new node
        let mut new_skips = Vec::with_capacity(new_height);
        new_skips.resize(new_height, None);
        let mut new = Box::new(Node {
            skips: new_skips,
            next: None,
            key: key,
            value: val,
        });
        let newp = new.as_mut() as *mut Node;

        for i in 0..new_height {
            if let Some(prev) = prevs[i] {
                unsafe {
                    new.skips[i] = (*prev).skips[i];
                    (*prev).skips[i] = Some(newp);
                }
            }
        }

        let added_mem = size_of::<Node>() + size_of::<Option<*mut Node>>() * new.skips.len() +
                        new.key.len() + new.value.len();
        self.approx_mem += added_mem;
        self.len += 1;

        // Insert new node by first replacing the previous element's next field with None and
        // assigning its value to new.next...
        new.next = unsafe { replace(&mut (*current).next, None) };
        // ...and then setting the previous element's next field to the new node
        unsafe { replace(&mut (*current).next, Some(new)) };
    }

    pub fn iter<'a>(&'a self) -> SkipMapIter<'a> {
        SkipMapIter {
            map: self,
            current: self.head.as_ref() as *const Node,
        }
    }

    /// Runs through the skipmap and prints everything including addresses
    fn dbg_print(&self) {
        let mut current = self.head.as_ref() as *const Node;
        loop {
            unsafe {
                println!("{:?} {:?}/{:?} - {:?}",
                         current,
                         (*current).key,
                         (*current).value,
                         (*current).skips);
                if let Some(next) = (*current).skips[0].clone() {
                    current = next;
                } else {
                    break;
                }
            }
        }
    }
}

pub struct SkipMapIter<'a> {
    map: &'a SkipMap,
    current: *const Node,
}

impl<'a> Iterator for SkipMapIter<'a> {
    type Item = (&'a [u8], &'a [u8]);

    fn next(&mut self) -> Option<Self::Item> {
        // we first go to the next element, then return that -- in order to skip the head node
        unsafe {
            (*self.current).next.as_ref().map(|next| {
                self.current = next.as_ref() as *const Node;
                ((*self.current).key.as_slice(), (*self.current).value.as_slice())
            })
        }
    }
}

impl<'a> LdbIterator for SkipMapIter<'a> {
    fn reset(&mut self) {
        let new = self.map.iter();
        self.current = new.current;
    }
    fn seek(&mut self, key: &[u8]) {
        if let Some(node) = self.map.get_greater_or_equal(key) {
            self.current = node as *const Node
        } else {
            self.reset();
        }
    }
    fn valid(&self) -> bool {
        unsafe { !(*self.current).key.is_empty() }
    }
    fn current(&self) -> Option<Self::Item> {
        if self.valid() {
            Some(unsafe { (&(*self.current).key, &(*self.current).value) })
        } else {
            None
        }
    }
    fn prev(&mut self) -> Option<Self::Item> {
        // Going after the original implementation here; we just seek to the node before current().
        if let Some(current) = self.current() {
            if let Some(prev) = self.map.get_next_smaller(current.0) {
                self.current = prev as *const Node;

                if !prev.key.is_empty() {
                    return Some(unsafe { (&(*self.current).key, &(*self.current).value) });
                }
            }
        }
        self.reset();
        None
    }
}

#[cfg(test)]
pub mod tests {
    use super::*;
    use options::Options;

    pub fn make_skipmap() -> SkipMap {
        let mut skm = SkipMap::new(Options::default());
        let keys = vec!["aba", "abb", "abc", "abd", "abe", "abf", "abg", "abh", "abi", "abj",
                        "abk", "abl", "abm", "abn", "abo", "abp", "abq", "abr", "abs", "abt",
                        "abu", "abv", "abw", "abx", "aby", "abz"];

        for k in keys {
            skm.insert(k.as_bytes().to_vec(), "def".as_bytes().to_vec());
        }
        skm
    }

    #[test]
    fn test_insert() {
        let skm = make_skipmap();
        assert_eq!(skm.len(), 26);
        skm.dbg_print();
    }

    #[test]
    #[should_panic]
    fn test_no_dupes() {
        let mut skm = make_skipmap();
        // this should panic
        skm.insert("abc".as_bytes().to_vec(), "def".as_bytes().to_vec());
        skm.insert("abf".as_bytes().to_vec(), "def".as_bytes().to_vec());
    }

    #[test]
    fn test_contains() {
        let skm = make_skipmap();
        assert!(skm.contains(&"aby".as_bytes().to_vec()));
        assert!(skm.contains(&"abc".as_bytes().to_vec()));
        assert!(skm.contains(&"abz".as_bytes().to_vec()));
        assert!(!skm.contains(&"ab{".as_bytes().to_vec()));
        assert!(!skm.contains(&"123".as_bytes().to_vec()));
        assert!(!skm.contains(&"aaa".as_bytes().to_vec()));
        assert!(!skm.contains(&"456".as_bytes().to_vec()));
    }

    #[test]
    fn test_find() {
        let skm = make_skipmap();
        assert_eq!(skm.get_greater_or_equal(&"abf".as_bytes().to_vec()).unwrap().key,
                   "abf".as_bytes().to_vec());
        assert!(skm.get_greater_or_equal(&"ab{".as_bytes().to_vec()).is_none());
        assert_eq!(skm.get_greater_or_equal(&"aaa".as_bytes().to_vec()).unwrap().key,
                   "aba".as_bytes().to_vec());
        assert_eq!(skm.get_greater_or_equal(&"ab".as_bytes()).unwrap().key.as_slice(),
                   "aba".as_bytes());
        assert_eq!(skm.get_greater_or_equal(&"abc".as_bytes()).unwrap().key.as_slice(),
                   "abc".as_bytes());
        assert!(skm.get_next_smaller(&"ab0".as_bytes()).is_none());
        assert_eq!(skm.get_next_smaller(&"abd".as_bytes()).unwrap().key.as_slice(),
                   "abc".as_bytes());
        assert_eq!(skm.get_next_smaller(&"ab{".as_bytes()).unwrap().key.as_slice(),
                   "abz".as_bytes());
    }

    #[test]
    fn test_iterator_0() {
        let skm = SkipMap::new(Options::default());
        let mut i = 0;

        for (_, _) in skm.iter() {
            i += 1;
        }

        assert_eq!(i, 0);
        assert!(!skm.iter().valid());
    }

    #[test]
    fn test_iterator_init() {
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
    fn test_iterator() {
        let skm = make_skipmap();
        let mut i = 0;

        for (k, v) in skm.iter() {
            assert!(!k.is_empty());
            assert!(!v.is_empty());
            i += 1;
        }
        assert_eq!(i, 26);
    }

    #[test]
    fn test_iterator_seek_valid() {
        let skm = make_skipmap();
        let mut iter = skm.iter();

        iter.next();
        assert!(iter.valid());
        assert_eq!(iter.current().unwrap().0, "aba".as_bytes());
        iter.seek(&"abz".as_bytes().to_vec());
        assert_eq!(iter.current().unwrap(),
                   ("abz".as_bytes(), "def".as_bytes()));
        // go back to beginning
        iter.seek(&"aba".as_bytes().to_vec());
        assert_eq!(iter.current().unwrap(),
                   ("aba".as_bytes(), "def".as_bytes()));

        iter.seek(&"".as_bytes().to_vec());
        assert!(iter.valid());
        iter.prev();
        assert!(!iter.valid());

        loop {
            if let Some(_) = iter.next() {

            } else {
                break;
            }
        }
        assert_eq!(iter.next(), None);
        assert_eq!(iter.prev(), Some(("aby".as_bytes(), "def".as_bytes())));
    }

    #[test]
    fn test_iterator_prev() {
        let skm = make_skipmap();
        let mut iter = skm.iter();

        iter.next();
        assert!(iter.valid());
        iter.prev();
        assert!(!iter.valid());
        iter.seek(&"abc".as_bytes());
        iter.prev();
        assert_eq!(iter.current().unwrap(),
                   ("abb".as_bytes(), "def".as_bytes()));
    }
}
