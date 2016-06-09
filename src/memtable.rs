use std::mem::size_of;

use types::{ValueType, SequenceNumber};
use skipmap::{SkipMap, SkipMapIter, Comparator, StandardComparator};

use integer_encoding::{FixedInt, VarInt};

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

    pub fn add(&mut self, seq: SequenceNumber, t: ValueType, key: &Vec<u8>, value: &Vec<u8>) {
        // We are using the original LevelDB approach here -- encoding key and value into the
        // key that is used for insertion into the SkipMap.
        // The format is: [key_size: varint32, key_data: [u8], flags: u64, value_size: varint32,
        // value_data: [u8]]
        let keysize = key.len();
        let valsize = value.len();

        let mut i = 0;
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

        self.map.insert(buf, Vec::new())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use types::*;

    #[test]
    fn test_add() {
        let mut mt = MemTable::new();
        mt.add(123,
               ValueType::TypeValue,
               &"abc".as_bytes().to_vec(),
               &"123".as_bytes().to_vec());

        assert_eq!(mt.map.iter().next().unwrap().0,
                   &vec![3, 97, 98, 99, 1, 123, 0, 0, 0, 0, 0, 0, 3, 49, 50, 51]);
    }
}
