//! An `env` is an abstraction layer that allows the database to run both on different platforms as
//! well as persisting data on disk or in memory.

use error::{self, Result};

use std::io::{Read, Write};
use std::os::unix::fs::FileExt;
use std::path::Path;

pub trait RandomAccess {
    fn read_at(&self, off: usize, len: usize) -> Result<Vec<u8>>;
}
impl<T: FileExt> RandomAccess for T {
    fn read_at(&self, off: usize, len: usize) -> Result<Vec<u8>> {
        let mut buf = vec![0 as u8; len];
        error::from_io_result((self as &FileExt).read_at(buf.as_mut(), off as u64)).map(|_| buf)
    }
}
// impl<T: AsRef<[u8]>> RandomAccess for Cursor<T> {
// fn read_at(&self, off: usize, len: usize) -> Result<Vec<u8>> {
// self.seek(io::SeekFrom::Start(off));
// let mut buf = vec![0 as u8; len];
// error::from_io_result(self.read_exact(&mut buf)).map(|_| buf)
// }
// }
//

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
