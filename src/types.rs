//! A collection of fundamental and/or simple types used by other modules

use std::cmp::Ordering;

pub enum ValueType {
    TypeDeletion = 0,
    TypeValue = 1,
}

/// Represents a sequence number of a single entry.
pub type SequenceNumber = u64;

#[derive(Clone, Debug)]
#[allow(dead_code)]
pub enum Status {
    OK,
    NotFound(String),
    Corruption(String),
    NotSupported(String),
    InvalidArgument(String),
    IOError(String),
}

/// Comparator trait, supporting types that can be nested (i.e., add additional functionality on
/// top of an inner comparator)
pub trait Cmp {
    fn cmp(&self, &[u8], &[u8]) -> Ordering;
}

/// Lexical comparator.
#[derive(Clone)]
pub struct DefaultCmp;

impl Cmp for DefaultCmp {
    fn cmp(&self, a: &[u8], b: &[u8]) -> Ordering {
        a.cmp(b)
    }
}

/// Denotes a key range
pub struct Range<'a> {
    pub start: &'a [u8],
    pub limit: &'a [u8],
}

/// An extension of the standard `Iterator` trait that supports some methods necessary for LevelDB.
/// This works because the iterators used are stateful and keep the last returned element.
///
/// Note: Implementing types are expected to hold `!valid()` before the first call to `next()`.
pub trait LdbIterator: Iterator {
    // We're emulating LevelDB's Slice type here using actual slices with the lifetime of the
    // iterator. The lifetime of the iterator is usually the one of the backing storage (Block,
    // MemTable, SkipMap...)
    // type Item = (&'a [u8], &'a [u8]);

    /// Seek the iterator to `key` or the next bigger key. If the seek is invalid (past last
    /// element), the iterator is reset() and not valid.
    fn seek(&mut self, key: &[u8]);
    /// Resets the iterator to be `!valid()` again (before first element)
    fn reset(&mut self);
    /// Returns true if `current()` would return a valid item.
    fn valid(&self) -> bool;
    /// Return the current item.
    fn current(&self) -> Option<Self::Item>;
    /// Go to the previous item.
    fn prev(&mut self) -> Option<Self::Item>;

    fn seek_to_first(&mut self) {
        self.reset();
        self.next();
    }
}

/// Describes a file on disk.
#[derive(Clone, Debug, PartialEq)]
pub struct FileMetaData {
    pub allowed_seeks: isize,
    pub num: u64,
    pub size: u64,
    // these are in InternalKey format:
    pub smallest: Vec<u8>,
    pub largest: Vec<u8>,
}
