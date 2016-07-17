use std::cmp::Ordering;
use types::{Comparator, StandardComparator};
use types::{ValueType, SequenceNumber, Status, LdbIterator};
use skipmap::{SkipMap, SkipMapIter};

use integer_encoding::{FixedInt, VarInt};

pub struct LookupKey {
    key: Vec<u8>,
    key_offset: usize,
}

/// Encapsulates a user key + sequence number, which is used for lookups in the internal map
/// implementation of a MemTable.
impl LookupKey {
    #[allow(unused_assignments)]
    fn new(k: &[u8], s: SequenceNumber) -> LookupKey {
        let mut key = Vec::with_capacity(k.len() + k.len().required_space() +
                                         <u64 as FixedInt>::required_space());
        let mut i = 0;

        key.reserve(8 + k.len().required_space() + k.len());

        key.resize(k.len().required_space(), 0);
        i += k.len().encode_var(&mut key[i..]);

        key.extend_from_slice(k);
        i += k.len();

        key.resize(i + <u64 as FixedInt>::required_space(), 0);
        (s << 8 | ValueType::TypeValue as u64).encode_fixed(&mut key[i..]);
        i += <u64 as FixedInt>::required_space();

        LookupKey {
            key: key,
            key_offset: k.len().required_space(),
        }
    }

    // Returns full key
    fn memtable_key<'a>(&'a self) -> &'a [u8] {
        self.key.as_slice()
    }

    // Returns only key
    fn user_key<'a>(&'a self) -> &'a [u8] {
        &self.key[self.key_offset..self.key.len() - 8]
    }

    // Returns key+tag
    fn internal_key<'a>(&'a self) -> &'a [u8] {
        &self.key[self.key_offset..]
    }
}

/// Provides Insert/Get/Iterate, based on the SkipMap implementation.
pub struct MemTable<C: Comparator> {
    map: SkipMap<C>,
}

impl MemTable<StandardComparator> {
    pub fn new() -> MemTable<StandardComparator> {
        MemTable::new_custom_cmp(StandardComparator {})
    }
}

impl<C: Comparator> MemTable<C> {
    pub fn new_custom_cmp(comparator: C) -> MemTable<C> {
        MemTable { map: SkipMap::new_with_cmp(comparator) }
    }
    pub fn approx_mem_usage(&self) -> usize {
        self.map.approx_memory()
    }

    pub fn add(&mut self, seq: SequenceNumber, t: ValueType, key: &[u8], value: &[u8]) {
        self.map.insert(Self::build_memtable_key(key, value, t, seq), Vec::new())
    }

    /// A memtable key is a bytestring containing (keylen, key, tag, vallen, val). This function
    /// builds such a key. It's called key because the underlying Map implementation will only be
    /// concerned with keys; the value field is not used (instead, the value is encoded in the key,
    /// and for lookups we just search for the next bigger entry).
    fn build_memtable_key(key: &[u8], value: &[u8], t: ValueType, seq: SequenceNumber) -> Vec<u8> {
        // We are using the original LevelDB approach here -- encoding key and value into the
        // key that is used for insertion into the SkipMap.
        // The format is: [key_size: varint32, key_data: [u8], flags: u64, value_size: varint32,
        // value_data: [u8]]

        let mut i = 0;
        let keysize = key.len();
        let valsize = value.len();

        let mut buf = Vec::with_capacity(keysize + valsize + keysize.required_space() +
                                         valsize.required_space() +
                                         <u64 as FixedInt>::required_space());
        buf.resize(keysize.required_space(), 0);
        i += keysize.encode_var(&mut buf[i..]);

        buf.extend(key.iter());
        i += key.len();

        let flag = (t as u64) | (seq << 8);
        buf.resize(i + <u64 as FixedInt>::required_space(), 0);
        flag.encode_fixed(&mut buf[i..]);
        i += <u64 as FixedInt>::required_space();

        buf.resize(i + valsize.required_space(), 0);
        i += valsize.encode_var(&mut buf[i..]);

        buf.extend(value.iter());
        i += value.len();

        assert_eq!(i, buf.len());
        buf
    }

    /// Parses a memtable key and returns  (keylen, key offset, tag, vallen, val offset).
    /// If the key only contains (keylen, key, tag), the vallen and val offset return values will be
    /// meaningless.
    fn parse_memtable_key(mkey: &[u8]) -> (usize, usize, u64, usize, usize) {
        let (keylen, mut i): (usize, usize) = VarInt::decode_var(&mkey);

        let keyoff = i;
        i += keylen;

        if mkey.len() > i {
            let tag = FixedInt::decode_fixed(&mkey[i..i + 8]);
            i += 8;

            let (vallen, j): (usize, usize) = VarInt::decode_var(&mkey[i..]);

            i += j;

            let valoff = i;

            return (keylen, keyoff, tag, vallen, valoff);
        } else {
            return (keylen, keyoff, 0, 0, 0);
        }
    }

    #[allow(unused_variables)]
    pub fn get(&self, key: &LookupKey) -> Result<Vec<u8>, Status> {
        let mut iter = self.map.iter();
        iter.seek(key.memtable_key());

        if let Some(e) = iter.current() {
            let foundkey = e.0;
            let (lkeylen, lkeyoff, _, _, _) = Self::parse_memtable_key(key.memtable_key());
            let (fkeylen, fkeyoff, tag, vallen, valoff) = Self::parse_memtable_key(foundkey);

            if C::cmp(&key.memtable_key()[lkeyoff..lkeyoff + lkeylen],
                      &foundkey[fkeyoff..fkeyoff + fkeylen]) == Ordering::Equal {
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
    skipmapiter: SkipMapIter<'a, C>,
}

impl<'a, C: 'a + Comparator> Iterator for MemtableIterator<'a, C> {
    type Item = (&'a [u8], &'a [u8]);

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            if let Some((foundkey, _)) = self.skipmapiter.next() {
                let (keylen, keyoff, tag, vallen, valoff) =
                    MemTable::<C>::parse_memtable_key(foundkey);

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
                let (keylen, keyoff, tag, vallen, valoff) =
                    MemTable::<C>::parse_memtable_key(foundkey);

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
            let (keylen, keyoff, tag, vallen, valoff) = MemTable::<C>::parse_memtable_key(foundkey);

            if tag & 0xff == ValueType::TypeValue as u64 {
                return Some((&foundkey[keyoff..keyoff + keylen],
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
    use types::*;

    fn get_memtable() -> MemTable<StandardComparator> {
        let mut mt = MemTable::new();
        let entries = vec![(120, "abc", "123"),
                           (121, "abd", "124"),
                           (122, "abe", "125"),
                           (123, "abf", "126")];

        for e in entries.iter() {
            mt.add(e.0, ValueType::TypeValue, e.1.as_bytes(), e.2.as_bytes());
        }
        mt
    }

    #[test]
    fn test_memtable_lookupkey() {
        use integer_encoding::VarInt;

        let lk1 = LookupKey::new("abcde".as_bytes(), 123);
        let lk2 = LookupKey::new("xyabxy".as_bytes(), 97);

        // Assert correct allocation strategy
        assert_eq!(lk1.key.len(), 14);
        assert_eq!(lk1.key.capacity(), 14);

        assert_eq!(lk1.user_key(), "abcde".as_bytes());
        assert_eq!(u32::decode_var(lk1.memtable_key()), (5, 1));
        assert_eq!(lk2.internal_key(),
                   vec![120, 121, 97, 98, 120, 121, 1, 97, 0, 0, 0, 0, 0, 0].as_slice());
    }

    #[test]
    fn test_memtable_add() {
        let mut mt = MemTable::new();
        mt.add(123,
               ValueType::TypeValue,
               "abc".as_bytes(),
               "123".as_bytes());

        assert_eq!(mt.map.iter().next().unwrap().0,
                   vec![3, 97, 98, 99, 1, 123, 0, 0, 0, 0, 0, 0, 3, 49, 50, 51].as_slice());
    }

    #[test]
    fn test_memtable_add_get() {
        let mt = get_memtable();

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

        if let Result::Ok(v) = mt.get(&LookupKey::new("abc".as_bytes(), 124)) {
            println!("{:?}", v);
            panic!("found");
        }
    }

    #[test]
    fn test_memtable_iterator_init() {
        let mt = get_memtable();
        let mut iter = mt.iter();

        assert!(!iter.valid());
        iter.next();
        assert!(iter.valid());
    }

    #[test]
    fn test_memtable_iterator() {
        let mt = get_memtable();
        let mut iter = mt.iter();

        iter.next();
        assert!(iter.valid());
        assert_eq!(iter.current().unwrap().0, vec![97, 98, 99].as_slice());
        assert_eq!(iter.current().unwrap().1, vec![49, 50, 51].as_slice());

        iter.seek("abf".as_bytes());
        assert_eq!(iter.current().unwrap().0, vec![97, 98, 102].as_slice());
        assert_eq!(iter.current().unwrap().1, vec![49, 50, 54].as_slice());
    }

    #[test]
    fn test_memtable_iterator_reverse() {
        let mt = get_memtable();
        let mut iter = mt.iter();

        iter.next();
        assert!(iter.valid());
        assert_eq!(iter.current().unwrap().0, vec![97, 98, 99].as_slice());

        iter.next();
        assert!(iter.valid());
        assert_eq!(iter.current().unwrap().0, vec![97, 98, 100].as_slice());

        iter.prev();
        assert!(iter.valid());
        assert_eq!(iter.current().unwrap().0, vec![97, 98, 99].as_slice());

        iter.prev();
        assert!(!iter.valid());
    }

    #[test]
    fn test_memtable_parse_key() {
        let key = vec![3, 1, 2, 3, 1, 123, 0, 0, 0, 0, 0, 0, 3, 4, 5, 6];
        let (keylen, keyoff, tag, vallen, valoff) =
            MemTable::<StandardComparator>::parse_memtable_key(&key);
        assert_eq!(keylen, 3);
        assert_eq!(&key[keyoff..keyoff + keylen], vec![1, 2, 3].as_slice());
        assert_eq!(tag, 123 << 8 | 1);
        assert_eq!(vallen, 3);
        assert_eq!(&key[valoff..valoff + vallen], vec![4, 5, 6].as_slice());
    }
}
