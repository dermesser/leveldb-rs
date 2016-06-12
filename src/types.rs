use std::default::Default;
use std::collections::HashMap;

pub enum ValueType {
    TypeDeletion = 0,
    TypeValue = 1,
}

/// Represents a sequence number of a single entry.
pub type SequenceNumber = u64;

#[allow(dead_code)]
pub enum Status {
    OK,
    NotFound(String),
    Corruption(String),
    NotSupported(String),
    InvalidArgument(String),
    IOError(String),
}

/// An extension of the standard `Iterator` trait that supports some methods necessary for LevelDB.
/// This works because the iterators used are stateful and keep the last returned element.
pub trait LdbIterator<'a>: Iterator {
    // We're emulating LevelDB's Slice type here using actual slices with the lifetime of the
    // iterator. The lifetime of the iterator is usually the one of the backing storage (Block,
    // MemTable, SkipMap...)
    //type Item = (&'a [u8], &'a [u8]);
    fn seek(&mut self, key: &Vec<u8>);
    fn valid(&self) -> bool;
    fn current(&'a self) -> Self::Item;
}

/// Supplied to DB read operations.
pub struct ReadOptions {
    pub verify_checksums: bool,
    pub fill_cache: bool,
    pub snapshot: Option<SequenceNumber>,
}

impl Default for ReadOptions {
    fn default() -> Self {
        ReadOptions {
            verify_checksums: false,
            fill_cache: true,
            snapshot: None,
        }
    }
}

// Opaque snapshot handle; Represents index to SnapshotList.map
pub type Snapshot = u64;

/// A list of all snapshots is kept in the DB.
pub struct SnapshotList {
    map: HashMap<Snapshot, SequenceNumber>,
    newest: Snapshot,
    oldest: Snapshot,
}

impl SnapshotList {
    pub fn new() -> SnapshotList {
        SnapshotList {
            map: HashMap::new(),
            newest: 0,
            oldest: 0,
        }
    }

    pub fn new_snapshot(&mut self, seq: SequenceNumber) -> Snapshot {
        self.newest += 1;
        self.map.insert(self.newest, seq);

        if self.oldest == 0 {
            self.oldest = self.newest;
        }

        self.newest
    }

    pub fn oldest(&self) -> SequenceNumber {
        self.map.get(&self.oldest).unwrap().clone()
    }

    pub fn newest(&self) -> SequenceNumber {
        self.map.get(&self.newest).unwrap().clone()
    }

    pub fn delete(&mut self, ss: Snapshot) {
        if self.oldest == ss {
            self.oldest += 1;
        }
        if self.newest == ss {
            self.newest -= 1;
        }
        self.map.remove(&ss);
    }

    pub fn empty(&self) -> bool {
        self.oldest == 0
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_snapshot_list() {
        let mut l = SnapshotList::new();

        assert!(l.empty());

        let oldest = l.new_snapshot(1);
        l.new_snapshot(2);
        let newest = l.new_snapshot(0);

        assert!(!l.empty());

        assert_eq!(l.oldest(), 1);
        assert_eq!(l.newest(), 0);

        l.delete(newest);

        assert_eq!(l.newest(), 2);
        assert_eq!(l.oldest(), 1);

        l.delete(oldest);

        assert_eq!(l.oldest(), 2);
    }

}
