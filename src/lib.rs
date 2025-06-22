//! rusty-leveldb is a reimplementation of LevelDB in pure rust. It depends only on a few crates,
//! and is very close to the original, implementation-wise. The external API is relatively small
//! and should be easy to use.
//!
//! ```
//! use rusty_leveldb::{DB, DBIterator, LdbIterator, Options};
//!
//! let opt = rusty_leveldb::in_memory();
//! let mut db = DB::open("mydatabase", opt).unwrap();
//!
//! db.put(b"Hello", b"World").unwrap();
//! assert_eq!(b"World", &*db.get(b"Hello").unwrap());
//!
//! let mut iter = db.new_iter().unwrap();
//! // Note: For efficiency reasons, it's recommended to use advance() and current() instead of
//! // next() when iterating over many elements.
//! assert_eq!((b"Hello".to_vec(), b"World".to_vec()), iter.next().unwrap());
//!
//! db.delete(b"Hello").unwrap();
//! db.flush().unwrap();
//! ```
//!

#![allow(dead_code)]

#[cfg(feature = "fs")]
extern crate errno;

#[cfg(feature = "fs")]
extern crate fs2;

extern crate integer_encoding;
extern crate rand;
extern crate snap;

#[cfg(test)]
#[macro_use]
extern crate time_test;

#[macro_use]
pub mod infolog;

#[cfg(any(
    feature = "asyncdb-tokio",
    feature = "asyncdb-async-std",
    feature = "asyncdb-wasm-bindgen-futures"
))]
mod asyncdb;

#[cfg(feature = "asyncdb-tokio")]
mod asyncdb_tokio;
#[cfg(feature = "asyncdb-tokio")]
use asyncdb_tokio::{send_response, send_response_result, Message};

#[cfg(feature = "asyncdb-async-std")]
mod asyncdb_async_std;
#[cfg(feature = "asyncdb-async-std")]
use asyncdb_async_std::{send_response, send_response_result, Message};

#[cfg(feature = "asyncdb-wasm-bindgen-futures")]
mod asyncdb_wasm_bindgen_futures;
#[cfg(feature = "asyncdb-wasm-bindgen-futures")]
use self::asyncdb_wasm_bindgen_futures::{send_response, send_response_result, Message};

mod block;
mod block_builder;
mod blockhandle;
mod cache;
mod cmp;
mod crc;

#[cfg(feature = "fs")]
mod disk_env;

mod env_common;
mod error;
mod filter;
mod filter_block;
mod key_types;
mod log;
mod mem_env;
mod memtable;
mod merging_iter;
mod options;
mod skipmap;
mod snapshot;
mod table_block;
mod table_builder;
mod table_cache;
mod table_reader;
mod test_util;
mod types;
mod version;
mod version_edit;
mod version_set;
mod write_batch;

mod db_impl;
mod db_iter;

pub mod compressor;
pub mod env;

#[cfg(feature = "asyncdb-async-std")]
pub use asyncdb_async_std::AsyncDB;
#[cfg(feature = "asyncdb-tokio")]
pub use asyncdb_tokio::AsyncDB;
#[cfg(feature = "asyncdb-wasm-bindgen-futures")]
pub use asyncdb_wasm_bindgen_futures::AsyncDB;
pub use cmp::{Cmp, DefaultCmp};
pub use compressor::{Compressor, CompressorId};
pub use db_impl::DB;
pub use db_iter::DBIterator;

#[cfg(feature = "fs")]
pub use disk_env::PosixDiskEnv;

pub use error::{Result, Status, StatusCode};
pub use filter::{BloomPolicy, FilterPolicy};
pub use mem_env::MemEnv;
pub use options::{in_memory, CompressorList, Options};
pub use skipmap::SkipMap;
pub use types::LdbIterator;
pub use write_batch::WriteBatch;
