use cache::Cache;
use cmp::{Cmp, DefaultCmp};
use disk_env;
use env::Env;
use filter;
use table_reader::TableBlock;
use types::SequenceNumber;

use std::default::Default;
use std::sync::{Arc, Mutex};

const KB: usize = 1 << 10;
const MB: usize = KB * KB;

const BLOCK_MAX_SIZE: usize = 4 * KB;
const BLOCK_CACHE_CAPACITY: usize = 8 * MB;
const WRITE_BUFFER_SIZE: usize = 4 * MB;
const DEFAULT_BITS_PER_KEY: u32 = 10; // NOTE: This may need to be optimized.

#[derive(Clone, Copy, PartialEq, Debug)]
pub enum CompressionType {
    CompressionNone = 0,
    CompressionSnappy = 1,
}

pub fn int_to_compressiontype(i: u32) -> Option<CompressionType> {
    match i {
        0 => Some(CompressionType::CompressionNone),
        1 => Some(CompressionType::CompressionSnappy),
        _ => None,
    }
}

/// [not all member types implemented yet]
///
#[derive(Clone)]
pub struct Options {
    pub cmp: Arc<Box<Cmp>>,
    pub env: Arc<Box<Env>>,
    pub create_if_missing: bool,
    pub error_if_exists: bool,
    pub paranoid_checks: bool,
    // pub logger: Logger,
    pub write_buffer_size: usize,
    pub max_open_files: usize,
    pub block_cache: Arc<Mutex<Cache<TableBlock>>>,
    pub block_size: usize,
    pub block_restart_interval: usize,
    pub compression_type: CompressionType,
    pub reuse_logs: bool,
    pub filter_policy: filter::BoxedFilterPolicy,
}

impl Default for Options {
    fn default() -> Options {
        Options {
            cmp: Arc::new(Box::new(DefaultCmp)),
            env: Arc::new(Box::new(disk_env::PosixDiskEnv::new())),
            create_if_missing: true,
            error_if_exists: false,
            paranoid_checks: false,
            write_buffer_size: WRITE_BUFFER_SIZE,
            max_open_files: 1 << 10,
            // 2000 elements by default
            block_cache: Arc::new(Mutex::new(Cache::new(BLOCK_CACHE_CAPACITY / BLOCK_MAX_SIZE))),
            block_size: BLOCK_MAX_SIZE,
            block_restart_interval: 16,
            reuse_logs: false,
            compression_type: CompressionType::CompressionNone,
            filter_policy: filter::BloomPolicy::new(DEFAULT_BITS_PER_KEY),
        }
    }
}

impl Options {
    /// Set the comparator to use in all operations and structures that need to compare keys.
    ///
    /// DO NOT set the comparator after having written any record with a different comparator.
    /// If the comparator used differs from the one used when writing a database that is being
    /// opened, the library is free to panic.
    pub fn set_comparator<C: Cmp>(&mut self, c: Box<Cmp>) {
        self.cmp = Arc::new(c);
    }
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

/// Supplied to write operations
pub struct WriteOptions {
    pub sync: bool,
}

impl Default for WriteOptions {
    fn default() -> Self {
        WriteOptions { sync: false }
    }
}
