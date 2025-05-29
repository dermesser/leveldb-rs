use crate::integer_encoding::{FixedInt, VarInt, VarIntWriter};
use crate::key_types::ValueType;
use crate::memtable::MemTable;
use crate::types::SequenceNumber;

use std::io::Write;

const SEQNUM_OFFSET: usize = 0;
const COUNT_OFFSET: usize = 8;
const HEADER_SIZE: usize = 12;

/// A WriteBatch contains entries to be written to a MemTable (for example) in a compact form.
///
/// The storage format has a 12-byte header: an 8-byte little-endian sequence number, followed by
/// a 4-byte little-endian count of entries. The header's sequence number is zero until the
/// WriteBatch is encoded with a given sequence number.
///
/// After the header are entries with one of the following formats
/// (with respective lengths in bytes, where ~var denotes a varint):
/// - [tag: 1, keylen: ~var, key: keylen]
/// - [tag: 1, keylen: ~var, key: keylen, vallen: ~var, val: vallen]
///
/// A WriteBatch entry for `delete` uses a tag of 0 and the first format (with only a key).
///
/// A WriteBatch entry for `put` uses a tag of 1 and the second format
/// (that includes both a key and value).
pub struct WriteBatch {
    entries: Vec<u8>,
}

impl WriteBatch {
    /// Initializes an empty WriteBatch with only a 12-byte header, set to zero.
    pub fn new() -> WriteBatch {
        let mut v = Vec::with_capacity(128);
        v.resize(HEADER_SIZE, 0);

        WriteBatch { entries: v }
    }

    /// Initializes a WriteBatch with a serialized WriteBatch.
    pub fn set_contents(&mut self, from: &[u8]) {
        self.entries.clear();
        self.entries.extend_from_slice(from);
    }

    /// Adds an entry to a WriteBatch, to be added to the database.
    #[allow(unused_assignments)]
    pub fn put(&mut self, k: &[u8], v: &[u8]) {
        self.entries
            .write_all(&[ValueType::TypeValue as u8])
            .unwrap();
        self.entries.write_varint(k.len()).unwrap();
        self.entries.write_all(k).unwrap();
        self.entries.write_varint(v.len()).unwrap();
        self.entries.write_all(v).unwrap();

        let c = self.count();
        self.set_count(c + 1);
    }

    /// Marks an entry to be deleted from the database.
    #[allow(unused_assignments)]
    pub fn delete(&mut self, k: &[u8]) {
        self.entries
            .write_all(&[ValueType::TypeDeletion as u8])
            .unwrap();
        self.entries.write_varint(k.len()).unwrap();
        self.entries.write_all(k).unwrap();

        let c = self.count();
        self.set_count(c + 1);
    }

    /// Clear the contents of a WriteBatch, and set the 12 header bytes to 0.
    pub fn clear(&mut self) {
        self.entries.clear();
        self.entries.resize(HEADER_SIZE, 0);
    }

    fn byte_size(&self) -> usize {
        self.entries.len()
    }

    fn set_count(&mut self, c: u32) {
        c.encode_fixed(&mut self.entries[COUNT_OFFSET..COUNT_OFFSET + 4]);
    }

    /// Returns how many operations are in a batch.
    pub fn count(&self) -> u32 {
        u32::decode_fixed(&self.entries[COUNT_OFFSET..COUNT_OFFSET + 4])
    }

    fn set_sequence(&mut self, s: SequenceNumber) {
        s.encode_fixed(&mut self.entries[SEQNUM_OFFSET..SEQNUM_OFFSET + 8]);
    }

    pub fn sequence(&self) -> SequenceNumber {
        u64::decode_fixed(&self.entries[SEQNUM_OFFSET..SEQNUM_OFFSET + 8])
    }

    pub fn iter(&self) -> WriteBatchIter {
        WriteBatchIter {
            batch: self,
            ix: HEADER_SIZE,
        }
    }

    pub fn insert_into_memtable(&self, mut seq: SequenceNumber, mt: &mut MemTable) {
        for (k, v) in self.iter() {
            match v {
                Some(v_) => mt.add(seq, ValueType::TypeValue, k, v_),
                None => mt.add(seq, ValueType::TypeDeletion, k, b""),
            }
            seq += 1;
        }
    }

    pub fn encode(mut self, seq: SequenceNumber) -> Vec<u8> {
        self.set_sequence(seq);
        self.entries
    }
}

impl Default for WriteBatch {
    fn default() -> Self {
        Self::new()
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

        let (klen, l) = usize::decode_var(&self.batch.entries[self.ix..])?;
        self.ix += l;
        let k = &self.batch.entries[self.ix..self.ix + klen];
        self.ix += klen;

        if tag == ValueType::TypeValue as u8 {
            let (vlen, m) = usize::decode_var(&self.batch.entries[self.ix..])?;
            self.ix += m;
            let v = &self.batch.entries[self.ix..self.ix + vlen];
            self.ix += vlen;

            Some((k, Some(v)))
        } else {
            Some((k, None))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::iter::Iterator;

    #[test]
    fn test_write_batch() {
        let mut b = WriteBatch::new();
        let entries: [(&[u8], &[u8]); 5] = [
            (b"abc", b"def"),
            (b"123", b"456"),
            (b"xxx", b"yyy"),
            (b"zzz", b""),
            (b"010", b""),
        ];

        for &(k, v) in entries.iter() {
            if !v.is_empty() {
                b.put(k, v);
            } else {
                b.delete(k)
            }
        }

        eprintln!("{:?}", b.entries);
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
        assert_eq!(b.encode(1).len(), 49);
    }
}
