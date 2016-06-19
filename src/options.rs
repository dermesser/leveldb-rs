use types::{Comparator, SequenceNumber, StandardComparator};
use filter::FilterPolicy;

use std::default::Default;

/// [not all member types implemented yet]
///
pub struct Options<C: Comparator> {
    pub cmp: C,
    pub create_if_missing: bool,
    pub error_if_exists: bool,
    pub paranoid_checks: bool,
    // pub logger: Logger,
    pub write_buffer_size: usize,
    pub max_open_files: usize,
    // pub block_cache: Cache,
    pub block_size: usize,
    pub block_restart_interval: usize,
    // pub compression_type: CompressionType,
    pub reuse_logs: bool,
    pub filter_policy: Option<Box<FilterPolicy>>,
}

impl Default for Options<StandardComparator> {
    fn default() -> Options<StandardComparator> {
        Options {
            cmp: StandardComparator,
            create_if_missing: true,
            error_if_exists: false,
            paranoid_checks: false,
            write_buffer_size: 4 << 20,
            max_open_files: 1 << 10,
            block_size: 4 << 10,
            block_restart_interval: 16,
            reuse_logs: false,
            filter_policy: None,
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
