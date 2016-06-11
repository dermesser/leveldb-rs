use std::default::Default;
use std::collections::LinkedList;

pub enum ValueType {
    TypeDeletion = 0,
    TypeValue = 1,
}

/// Represents a snapshot or the sequence number of a single entry.
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

/// A list of all snapshots is kept in the DB.
pub struct SnapshotList {
    snapshots: LinkedList<SequenceNumber>,
}

impl SnapshotList {
    pub fn new() -> SnapshotList {
        SnapshotList { snapshots: LinkedList::new() }
    }

    pub fn new_snapshot(&mut self, seq: SequenceNumber) {
        self.snapshots.push_back(seq);
    }

    pub fn oldest(&self) -> SequenceNumber {
        assert!(!self.snapshots.is_empty());
        self.snapshots.front().map(|s| *s).unwrap()
    }

    pub fn newest(&self) -> SequenceNumber {
        assert!(!self.snapshots.is_empty());
        self.snapshots.back().map(|s| *s).unwrap()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_snapshot_list() {
        let mut l = SnapshotList::new();

        l.new_snapshot(1);
        l.new_snapshot(2);

        assert_eq!(l.oldest(), 1);
        assert_eq!(l.newest(), 2);
    }
}
