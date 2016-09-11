use std::cmp::Ordering;

use key_types::{LookupKey, UserKey, InternalKey, parse_memtable_key, build_memtable_key, parse_tag};
use types::{Comparator, StandardComparator};
use types::{ValueType, SequenceNumber, Status, LdbIterator};
use skipmap::{SkipMap, SkipMapIter};

/// An internal comparator wrapping a user-supplied comparator. This comparator is used to compare
/// memtable keys, which contain length prefixes and a sequence number.
/// The ordering is determined by asking the wrapped comparator; ties are broken by *reverse*
/// ordering the sequence numbers. (This means that when having an entry abx/4 and searching for
/// abx/5, then abx/4 is counted as "greater-or-equal", making snapshot functionality work at all)
#[derive(Clone, Copy)]
struct MemtableKeyComparator<C: Comparator> {
    internal: C,
}

impl<C: Comparator> Comparator for MemtableKeyComparator<C> {
    fn cmp(&self, a: &[u8], b: &[u8]) -> Ordering {
        let (akeylen, akeyoff, atag, _, _) = parse_memtable_key(a);
        let (bkeylen, bkeyoff, btag, _, _) = parse_memtable_key(b);

        let userkey_a = &a[akeyoff..akeyoff + akeylen];
        let userkey_b = &b[bkeyoff..bkeyoff + bkeylen];

        let userkey_order = self.internal.cmp(userkey_a, userkey_b);
        println!("{:?}", userkey_order);

        if userkey_order != Ordering::Equal {
            userkey_order
        } else {
            // look at sequence number, in reverse order
            let (_, aseq) = parse_tag(atag);
            let (_, bseq) = parse_tag(btag);

            // reverse!
            bseq.cmp(&aseq)
        }
    }
}

/// Provides Insert/Get/Iterate, based on the SkipMap implementation.
pub struct MemTable<C: Comparator> {
    map: SkipMap<MemtableKeyComparator<C>>,
    cmp: C,
}

impl MemTable<StandardComparator> {
    pub fn new() -> MemTable<StandardComparator> {
        MemTable::new_custom_cmp(StandardComparator {})
    }
}

impl<C: Comparator> MemTable<C> {
    pub fn new_custom_cmp(comparator: C) -> MemTable<C> {
        MemTable {
            map: SkipMap::new_with_cmp(MemtableKeyComparator { internal: comparator }),
            cmp: comparator,
        }
    }
    pub fn approx_mem_usage(&self) -> usize {
        self.map.approx_memory()
    }

    pub fn add<'a>(&mut self, seq: SequenceNumber, t: ValueType, key: UserKey<'a>, value: &[u8]) {
        self.map.insert(build_memtable_key(key, value, t, seq), Vec::new())
    }

    #[allow(unused_variables)]
    pub fn get(&self, key: &LookupKey) -> Result<Vec<u8>, Status> {
        let mut iter = self.map.iter();
        iter.seek(key.memtable_key());

        if let Some(e) = iter.current() {
            let foundkey = e.0;
            let (lkeylen, lkeyoff, _, _, _) = parse_memtable_key(key.memtable_key());
            let (fkeylen, fkeyoff, tag, vallen, valoff) = parse_memtable_key(foundkey);

            // Compare user key -- if equal, proceed
            if self.cmp.cmp(&key.memtable_key()[lkeyoff..lkeyoff + lkeylen],
                            &foundkey[fkeyoff..fkeyoff + fkeylen]) ==
               Ordering::Equal {
                if tag & 0xff == ValueType::TypeValue as u64 {
                    return Result::Ok(foundkey[valoff..valoff + vallen].to_vec());
                } else {
                    return Result::Err(Status::NotFound(String::new()));
                }
            }
        }
        Result::Err(Status::NotFound("not found".to_string()))
    }

    pub fn iter<'a>(&'a self) -> MemtableIterator<'a, C> {
        MemtableIterator {
            _tbl: self,
            skipmapiter: self.map.iter(),
        }
    }
}

pub struct MemtableIterator<'a, C: 'a + Comparator> {
    _tbl: &'a MemTable<C>,
    skipmapiter: SkipMapIter<'a, MemtableKeyComparator<C>>,
}

impl<'a, C: 'a + Comparator> Iterator for MemtableIterator<'a, C> {
    type Item = (InternalKey<'a>, &'a [u8]);

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            if let Some((foundkey, _)) = self.skipmapiter.next() {
                let (keylen, keyoff, tag, vallen, valoff) = parse_memtable_key(foundkey);

                if tag & 0xff == ValueType::TypeValue as u64 {
                    return Some((&foundkey[keyoff..keyoff + keylen],
                                 &foundkey[valoff..valoff + vallen]));
                } else {
                    continue;
                }
            } else {
                return None;
            }
        }
    }
}

impl<'a, C: 'a + Comparator> LdbIterator for MemtableIterator<'a, C> {
    fn reset(&mut self) {
        self.skipmapiter.reset();
    }
    fn prev(&mut self) -> Option<Self::Item> {
        loop {
            if let Some((foundkey, _)) = self.skipmapiter.prev() {
                let (keylen, keyoff, tag, vallen, valoff) = parse_memtable_key(foundkey);

                if tag & 0xff == ValueType::TypeValue as u64 {
                    return Some((&foundkey[keyoff..keyoff + keylen],
                                 &foundkey[valoff..valoff + vallen]));
                } else {
                    continue;
                }
            } else {
                return None;
            }
        }
    }
    fn valid(&self) -> bool {
        self.skipmapiter.valid()
    }
    fn current(&self) -> Option<Self::Item> {
        if !self.valid() {
            return None;
        }

        if let Some((foundkey, _)) = self.skipmapiter.current() {
            let (keylen, keyoff, tag, vallen, valoff) = parse_memtable_key(foundkey);

            if tag & 0xff == ValueType::TypeValue as u64 {
                return Some((&foundkey[keyoff..keyoff + keylen + 8],
                             &foundkey[valoff..valoff + vallen]));
            } else {
                panic!("should not happen");
            }
        } else {
            panic!("should not happen");
        }
    }
    fn seek(&mut self, to: &[u8]) {
        self.skipmapiter.seek(LookupKey::new(to, 0).memtable_key());
    }
}

#[cfg(test)]
#[allow(unused_variables)]
mod tests {
    use super::*;
    use key_types::*;
    use types::*;

    fn get_memtable() -> MemTable<StandardComparator> {
        let mut mt = MemTable::new();
        let entries = vec![(115, "abc", "122"),
                           (120, "abc", "123"),
                           (121, "abd", "124"),
                           (122, "abe", "125"),
                           (123, "abf", "126")];

        for e in entries.iter() {
            mt.add(e.0, ValueType::TypeValue, e.1.as_bytes(), e.2.as_bytes());
        }
        mt
    }

    #[test]
    fn test_memtable_parse_tag() {
        let tag = (12345 << 8) | 67;
        assert_eq!(parse_tag(tag), (67, 12345));
    }

    #[test]
    fn test_memtable_add() {
        let mut mt = MemTable::new();
        mt.add(123,
               ValueType::TypeValue,
               "abc".as_bytes(),
               "123".as_bytes());

        assert_eq!(mt.map.iter().next().unwrap().0,
                   vec![11, 97, 98, 99, 1, 123, 0, 0, 0, 0, 0, 0, 3, 49, 50, 51].as_slice());
    }

    #[test]
    fn test_memtable_add_get() {
        let mt = get_memtable();

        // Smaller sequence number doesn't find entry
        if let Result::Ok(v) = mt.get(&LookupKey::new("abc".as_bytes(), 110)) {
            println!("{:?}", v);
            panic!("found");
        }

        // Bigger sequence number falls back to next smaller
        if let Result::Ok(v) = mt.get(&LookupKey::new("abc".as_bytes(), 116)) {
            assert_eq!(v, "122".as_bytes());
        } else {
            panic!("not found");
        }

        // Exact match works
        if let Result::Ok(v) = mt.get(&LookupKey::new("abc".as_bytes(), 120)) {
            assert_eq!(v, "123".as_bytes());
        } else {
            panic!("not found");
        }

        if let Result::Ok(v) = mt.get(&LookupKey::new("abe".as_bytes(), 122)) {
            assert_eq!(v, "125".as_bytes());
        } else {
            panic!("not found");
        }
    }

    #[test]
    fn test_memtable_iterator_init() {
        let mt = get_memtable();
        let mut iter = mt.iter();

        assert!(!iter.valid());
        iter.next();
        assert!(iter.valid());
        assert_eq!(iter.current().unwrap().0,
                   vec![97, 98, 99, 1, 120, 0, 0, 0, 0, 0, 0].as_slice());
        iter.reset();
        assert!(!iter.valid());
    }

    #[test]
    fn test_memtable_iterator_fwd_seek() {
        let mt = get_memtable();
        let iter = mt.iter();

        let expected = vec!["123".as_bytes(), /* i.e., the abc entry with
                                               * higher sequence number comes first */
                            "122".as_bytes(),
                            "124".as_bytes(),
                            "125".as_bytes(),
                            "126".as_bytes()];
        let mut i = 0;

        for (k, v) in iter {
            assert_eq!(v, expected[i]);
            i += 1;
        }
    }

    #[test]
    fn test_memtable_iterator_reverse() {
        let mt = get_memtable();
        let mut iter = mt.iter();

        // Bigger sequence number comes first
        iter.next();
        assert!(iter.valid());
        assert_eq!(iter.current().unwrap().0,
                   vec![97, 98, 99, 1, 120, 0, 0, 0, 0, 0, 0].as_slice());

        iter.next();
        assert!(iter.valid());
        assert_eq!(iter.current().unwrap().0,
                   vec![97, 98, 99, 1, 115, 0, 0, 0, 0, 0, 0].as_slice());

        iter.next();
        assert!(iter.valid());
        assert_eq!(iter.current().unwrap().0,
                   vec![97, 98, 100, 1, 121, 0, 0, 0, 0, 0, 0].as_slice());

        iter.prev();
        assert!(iter.valid());
        assert_eq!(iter.current().unwrap().0,
                   vec![97, 98, 99, 1, 115, 0, 0, 0, 0, 0, 0].as_slice());

        iter.prev();
        assert!(iter.valid());
        assert_eq!(iter.current().unwrap().0,
                   vec![97, 98, 99, 1, 120, 0, 0, 0, 0, 0, 0].as_slice());

        iter.prev();
        assert!(!iter.valid());
    }

    #[test]
    fn test_memtable_parse_key() {
        let key = vec![11, 1, 2, 3, 1, 123, 0, 0, 0, 0, 0, 0, 3, 4, 5, 6];
        let (keylen, keyoff, tag, vallen, valoff) = parse_memtable_key(&key);
        assert_eq!(keylen, 3);
        assert_eq!(&key[keyoff..keyoff + keylen], vec![1, 2, 3].as_slice());
        assert_eq!(tag, 123 << 8 | 1);
        assert_eq!(vallen, 3);
        assert_eq!(&key[valoff..valoff + vallen], vec![4, 5, 6].as_slice());
    }
}
