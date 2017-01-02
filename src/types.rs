//! A collection of fundamental and/or simple types used by other modules

use std::error::Error;
use std::fmt::{self, Display, Formatter};
use std::io;
use std::result;

#[derive(Debug, PartialOrd, PartialEq)]
pub enum ValueType {
    TypeDeletion = 0,
    TypeValue = 1,
}

/// Represents a sequence number of a single entry.
pub type SequenceNumber = u64;

pub const MAX_SEQUENCE_NUMBER: SequenceNumber = (1 << 56) - 1;

#[derive(Clone, Debug)]
#[allow(dead_code)]
pub enum Status {
    OK,
    NotFound(String),
    Corruption(String),
    NotSupported(String),
    InvalidArgument(String),
    PermissionDenied(String),
    IOError(String),
    Unknown(String),
}

impl Display for Status {
    fn fmt(&self, fmt: &mut Formatter) -> result::Result<(), fmt::Error> {
        fmt.write_str(self.description())
    }
}

impl Error for Status {
    fn description(&self) -> &str {
        match *self {
            Status::OK => "ok",
            Status::NotFound(ref s) => s,
            Status::Corruption(ref s) => s,
            Status::NotSupported(ref s) => s,
            Status::InvalidArgument(ref s) => s,
            Status::PermissionDenied(ref s) => s,
            Status::IOError(ref s) => s,
            Status::Unknown(ref s) => s,
        }
    }
}

/// LevelDB's result type
pub type Result<T> = result::Result<T, Status>;

pub fn from_io_result<T>(e: io::Result<T>) -> Result<T> {
    match e {
        Ok(r) => result::Result::Ok(r),
        Err(e) => {
            let err = e.description().to_string();

            let r = match e.kind() {
                io::ErrorKind::NotFound => Err(Status::NotFound(err)),
                io::ErrorKind::InvalidData => Err(Status::Corruption(err)),
                io::ErrorKind::InvalidInput => Err(Status::InvalidArgument(err)),
                io::ErrorKind::PermissionDenied => Err(Status::PermissionDenied(err)),
                _ => Err(Status::IOError(err)),
            };

            r
        }
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
    /// After a seek to an existing key, current() returns that entry.
    fn seek(&mut self, key: &[u8]);
    /// Resets the iterator to be `!valid()` again (before first element)
    fn reset(&mut self);
    /// Returns true if `current()` would return a valid item.
    fn valid(&self) -> bool;
    /// Return the current item (i.e. the item most recently returned by next())
    fn current(&self) -> Option<Self::Item>;
    /// Go to the previous item. This is inefficient for most iterators.
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
