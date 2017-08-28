//! A collection of fundamental and/or simple types used by other modules

#[derive(Debug, PartialOrd, PartialEq)]
pub enum ValueType {
    TypeDeletion = 0,
    TypeValue = 1,
}

/// Represents a sequence number of a single entry.
pub type SequenceNumber = u64;

pub const MAX_SEQUENCE_NUMBER: SequenceNumber = (1 << 56) - 1;


/// Denotes a key range
pub struct Range<'a> {
    pub start: &'a [u8],
    pub limit: &'a [u8],
}

/// An extension of the standard `Iterator` trait that supports some methods necessary for LevelDB.
/// This works because the iterators used are stateful and keep the last returned element.
///
/// Note: Implementing types are expected to hold `!valid()` before the first call to `advance()`.
///
/// test_util::test_iterator_properties() verifies that all properties hold.
pub trait LdbIterator {
    /// advance advances the position of the iterator by one element (which can be retrieved using
    /// current(). If no more elements are available, advance() returns false, and the iterator
    /// becomes invalid (i.e. like reset() has been called).
    fn advance(&mut self) -> bool;
    /// Return the current item (i.e. the item most recently returned by get_next())
    fn current(&self, key: &mut Vec<u8>, val: &mut Vec<u8>) -> bool;
    /// Seek the iterator to `key` or the next bigger key. If the seek is invalid (past last
    /// element), the iterator is reset() and not valid.
    /// After a seek to an existing key, current() returns that entry.
    fn seek(&mut self, key: &[u8]);
    /// Resets the iterator to be `!valid()` again (before first element)
    fn reset(&mut self);
    /// Returns true if the iterator is not positioned before the first or after the last element,
    /// i.e. if current() would return an entry.
    fn valid(&self) -> bool;
    /// Go to the previous item; if the iterator has reached the "before-first" item, prev()
    /// returns false, and the iterator is invalid. This is inefficient for most iterators.
    fn prev(&mut self) -> bool;

    // default implementations.

    /// next is like Iterator::next(). It's implemented here because "only traits defined in the
    /// current crate can be implemented for a type parameter" (says rustc).
    fn next(&mut self) -> Option<(Vec<u8>, Vec<u8>)> {
        if !self.advance() {
            return None;
        }
        let (mut key, mut val) = (vec![], vec![]);
        if self.current(&mut key, &mut val) {
            Some((key, val))
        } else {
            None
        }
    }
    /// seek_to_first seeks to the first element.
    fn seek_to_first(&mut self) {
        self.reset();
        self.advance();
    }
}

/// current_key_val is a helper allocating two vectors and filling them with the current key/value
/// of the specified iterator.
pub fn current_key_val<It: LdbIterator + ?Sized>(it: &It) -> Option<(Vec<u8>, Vec<u8>)> {
    let (mut k, mut v) = (vec![], vec![]);
    if it.current(&mut k, &mut v) {
        Some((k, v))
    } else {
        None
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
