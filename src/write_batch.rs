use memtable::MemTable;
use types::{Comparator, SequenceNumber, ValueType};

struct BatchEntry<'a> {
    key: &'a [u8],
    // None => value type is delete, Some(x) => value type is add
    val: Option<&'a [u8]>,
}

pub struct WriteBatch<'a> {
    entries: Vec<BatchEntry<'a>>,
    seq: SequenceNumber,
}

impl<'a> WriteBatch<'a> {
    fn new(seq: SequenceNumber) -> WriteBatch<'a> {
        WriteBatch {
            entries: Vec::new(),
            seq: seq,
        }
    }

    fn with_capacity(seq: SequenceNumber, c: usize) -> WriteBatch<'a> {
        WriteBatch {
            entries: Vec::with_capacity(c),
            seq: seq,
        }
    }

    fn put(&mut self, k: &'a [u8], v: &'a [u8]) {
        self.entries.push(BatchEntry {
            key: k,
            val: Some(v),
        })
    }

    fn delete(&mut self, k: &'a [u8]) {
        self.entries.push(BatchEntry {
            key: k,
            val: None,
        })
    }

    fn clear(&mut self) {
        self.entries.clear()
    }

    fn iter<'b>(&'b self) -> WriteBatchIter<'b, 'a> {
        WriteBatchIter {
            batch: self,
            ix: 0,
        }
    }

    fn insert_into_memtable<C: Comparator>(&self, mt: &mut MemTable<C>) {
        let mut sequence_num = self.seq;

        for (k, v) in self.iter() {
            match v {
                Some(v_) => mt.add(sequence_num, ValueType::TypeValue, k, v_),
                None => mt.add(sequence_num, ValueType::TypeDeletion, k, "".as_bytes()),
            }
            sequence_num += 1;
        }
    }
}

pub struct WriteBatchIter<'b, 'a: 'b> {
    batch: &'b WriteBatch<'a>,
    ix: usize,
}

/// `'b` is the lifetime of the WriteBatch; `'a` is the lifetime of the slices contained in the
/// batch.
impl<'b, 'a: 'b> Iterator for WriteBatchIter<'b, 'a> {
    type Item = (&'a [u8], Option<&'a [u8]>);
    fn next(&mut self) -> Option<Self::Item> {
        if self.ix < self.batch.entries.len() {
            self.ix += 1;
            Some((self.batch.entries[self.ix - 1].key, self.batch.entries[self.ix - 1].val))
        } else {
            None
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::iter::Iterator;

    #[test]
    fn test_write_batch() {
        let mut b = WriteBatch::with_capacity(1, 16);
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
    }
}
