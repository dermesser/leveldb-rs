use block::Block;
use cache::Cache;
use cmp::{Cmp, DefaultCmp};
use types::SequenceNumber;

use std::default::Default;
use std::sync::{Arc, Mutex};

const KB: usize = 1 << 10;
const MB: usize = KB * KB;

const BLOCK_MAX_SIZE: usize = 4 * KB;
const BLOCK_CACHE_CAPACITY: usize = 8 * MB;
const WRITE_BUFFER_SIZE: usize = 4 * MB;

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
    pub create_if_missing: bool,
    pub error_if_exists: bool,
    pub paranoid_checks: bool,
    // pub logger: Logger,
    pub write_buffer_size: usize,
    pub max_open_files: usize,
    pub block_cache: Arc<Mutex<Cache<Block>>>,
    pub block_size: usize,
    pub block_restart_interval: usize,
    pub compression_type: CompressionType,
    pub reuse_logs: bool,
}

impl Default for Options {
    fn default() -> Options {
        Options {
            cmp: Arc::new(Box::new(DefaultCmp)),
            create_if_missing: true,
            error_if_exists: false,
            paranoid_checks: false,
            write_buffer_size: WRITE_BUFFER_SIZE,
            max_open_files: 1 << 10,
            block_cache: Arc::new(Mutex::new(Cache::new(BLOCK_CACHE_CAPACITY / BLOCK_MAX_SIZE))), /* 2000 elements */
            block_size: BLOCK_MAX_SIZE,
            block_restart_interval: 16,
            reuse_logs: false,
            compression_type: CompressionType::CompressionNone,
        }
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
