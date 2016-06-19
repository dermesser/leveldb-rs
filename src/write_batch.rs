use memtable::MemTable;
use types::{Comparator, SequenceNumber, ValueType};
use integer_encoding::{VarInt, FixedInt};

const SEQNUM_OFFSET: usize = 0;
const COUNT_OFFSET: usize = 8;
const HEADER_SIZE: usize = 12;

pub struct WriteBatch {
    entries: Vec<u8>,
}

impl WriteBatch {
    fn new(seq: SequenceNumber) -> WriteBatch {
        let mut v = Vec::with_capacity(128);
        v.resize(HEADER_SIZE, 0);

        WriteBatch { entries: v }
    }

    fn from(buf: Vec<u8>) -> WriteBatch {
        WriteBatch { entries: buf }
    }

    fn put(&mut self, k: &[u8], v: &[u8]) {
        let mut ix = self.entries.len();

        self.entries.push(ValueType::TypeValue as u8);
        ix += 1;

        self.entries.resize(ix + k.len().required_space(), 0);
        ix += k.len().encode_var(&mut self.entries[ix..]);

        self.entries.extend_from_slice(k);
        ix += k.len();

        self.entries.resize(ix + v.len().required_space(), 0);
        ix += v.len().encode_var(&mut self.entries[ix..]);

        self.entries.extend_from_slice(v);
        // ix += v.len();

        let c = self.count();
        self.set_count(c + 1);
    }

    fn delete(&mut self, k: &[u8]) {
        let mut ix = self.entries.len();

        self.entries.push(ValueType::TypeDeletion as u8);
        ix += 1;

        self.entries.resize(ix + k.len().required_space(), 0);
        ix += k.len().encode_var(&mut self.entries[ix..]);

        self.entries.extend_from_slice(k);
        // ix += k.len();

        let c = self.count();
        self.set_count(c + 1);
    }

    fn clear(&mut self) {
        self.entries.clear()
    }

    fn byte_size(&self) -> usize {
        self.entries.len()
    }

    fn set_count(&mut self, c: u32) {
        c.encode_fixed(&mut self.entries[COUNT_OFFSET..COUNT_OFFSET + 4]);
    }

    fn count(&self) -> u32 {
        u32::decode_fixed(&self.entries[COUNT_OFFSET..COUNT_OFFSET + 4])
    }

    fn set_sequence(&mut self, s: SequenceNumber) {
        s.encode_fixed(&mut self.entries[SEQNUM_OFFSET..SEQNUM_OFFSET + 8]);
    }

    fn sequence(&self) -> SequenceNumber {
        u64::decode_fixed(&self.entries[SEQNUM_OFFSET..SEQNUM_OFFSET + 8])
    }

    fn iter<'a>(&'a self) -> WriteBatchIter<'a> {
        WriteBatchIter {
            batch: self,
            ix: HEADER_SIZE,
        }
    }

    fn insert_into_memtable<C: Comparator>(&self, mt: &mut MemTable<C>) {
        let mut sequence_num = self.sequence();

        for (k, v) in self.iter() {
            match v {
                Some(v_) => mt.add(sequence_num, ValueType::TypeValue, k, v_),
                None => mt.add(sequence_num, ValueType::TypeDeletion, k, "".as_bytes()),
            }
            sequence_num += 1;
        }
    }

    fn encode(self) -> Vec<u8> {
        self.entries
    }
}

pub struct WriteBatchIter<'a> {
    batch: &'a WriteBatch,
    ix: usize,
}

/// The iterator also plays the role of the decoder.
impl<'a> Iterator for WriteBatchIter<'a> {
    type Item = (&'a [u8], Option<&'a [u8]>);
    fn next(&mut self) -> Option<Self::Item> {
        if self.ix >= self.batch.entries.len() {
            return None;
        }

        let tag = self.batch.entries[self.ix];
        self.ix += 1;

        let (klen, l) = usize::decode_var(&self.batch.entries[self.ix..]);
        self.ix += l;
        let k = &self.batch.entries[self.ix..self.ix + klen];
        self.ix += klen;


        if tag == ValueType::TypeValue as u8 {
            let (vlen, m) = usize::decode_var(&self.batch.entries[self.ix..]);
            self.ix += m;
            let v = &self.batch.entries[self.ix..self.ix + vlen];
            self.ix += vlen;

            return Some((k, Some(v)));
        } else {
            return Some((k, None));
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::iter::Iterator;

    #[test]
    fn test_write_batch() {
        let mut b = WriteBatch::new(1);
        let entries = vec![("abc".as_bytes(), "def".as_bytes()),
                           ("123".as_bytes(), "456".as_bytes()),
                           ("xxx".as_bytes(), "yyy".as_bytes()),
                           ("zzz".as_bytes(), "".as_bytes()),
                           ("010".as_bytes(), "".as_bytes())];

        for &(k, v) in entries.iter() {
            if !v.is_empty() {
                b.put(k, v);
            } else {
                b.delete(k)
            }
        }

        println!("{:?}", b.entries);
        assert_eq!(b.byte_size(), 49);
        assert_eq!(b.iter().count(), 5);

        let mut i = 0;

        for (k, v) in b.iter() {
            assert_eq!(k, entries[i].0);

            match v {
                None => assert!(entries[i].1.is_empty()),
                Some(v_) => assert_eq!(v_, entries[i].1),
            }

            i += 1;
        }

        assert_eq!(i, 5);
        assert_eq!(b.encode().len(), 49);
    }
}
