//! An `env` is an abstraction layer that allows the database to run both on different platforms as
//! well as persisting data on disk or in memory.

use error::{self, Result};

use std::io::prelude::*;
use std::fs::File;
use std::os::unix::fs::FileExt;
use std::path::Path;

pub trait RandomAccess {
    fn read_at(&self, off: usize, dst: &mut [u8]) -> Result<usize>;
}

impl RandomAccess for File {
    fn read_at(&self, off: usize, dst: &mut [u8]) -> Result<usize> {
        error::from_io_result((self as &FileExt).read_at(dst, off as u64))
    }
}

pub struct FileLock {
    pub id: String,
}

pub trait Env {
    fn open_sequential_file(&self, &Path) -> Result<Box<Read>>;
    fn open_random_access_file(&self, &Path) -> Result<Box<RandomAccess>>;
    fn open_writable_file(&self, &Path) -> Result<Box<Write>>;
    fn open_appendable_file(&self, &Path) -> Result<Box<Write>>;

    fn exists(&self, &Path) -> Result<bool>;
    fn children(&self, &Path) -> Result<Vec<String>>;
    fn size_of(&self, &Path) -> Result<usize>;

    fn delete(&self, &Path) -> Result<()>;
    fn mkdir(&self, &Path) -> Result<()>;
    fn rmdir(&self, &Path) -> Result<()>;
    fn rename(&self, &Path, &Path) -> Result<()>;

    fn lock(&self, &Path) -> Result<FileLock>;
    fn unlock(&self, l: FileLock);

    fn new_logger(&self, &Path) -> Result<Logger>;

    fn micros(&self) -> u64;
    fn sleep_for(&self, micros: u32);
}

pub struct Logger {
    dst: Box<Write>,
}

impl Logger {
    pub fn new(w: Box<Write>) -> Logger {
        Logger { dst: w }
    }

    pub fn log(&mut self, message: &String) {
        let _ = self.dst.write(message.as_bytes());
        let _ = self.dst.write("\n".as_bytes());
    }
}
