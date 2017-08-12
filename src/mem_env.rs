//! An in-memory implementation of Env.

use env::{BufferBackedFile, Env, FileLock, RandomAccess};
use error::{Result, Status, StatusCode};

use std::collections::HashMap;
use std::collections::hash_map::Entry;
use std::io::{self, Read, Write};
use std::ops::Deref;
use std::path::Path;
use std::sync::{Arc, Mutex};

fn path_to_string(p: &Path) -> String {
    p.to_str().map(String::from).unwrap()
}

/// A MemFile holds a shared, concurrency-safe buffer. It can be shared among several
/// MemFileReaders and MemFileWriters, each with an independent offset.
#[derive(Clone)]
pub struct MemFile(Arc<Mutex<BufferBackedFile>>);

impl MemFile {
    fn new() -> MemFile {
        MemFile(Arc::new(Mutex::new(Vec::new())))
    }
}

/// A MemFileReader holds a reference to a MemFile and a read offset.
struct MemFileReader(MemFile, usize);

impl MemFileReader {
    fn new(f: MemFile, from: usize) -> MemFileReader {
        MemFileReader(f, from)
    }
}

// We need Read/Write/Seek implementations for our MemFile in order to work well with the
// concurrency requirements. It's very hard or even impossible to implement those traits just by
// wrapping MemFile in other types.
impl Read for MemFileReader {
    fn read(&mut self, dst: &mut [u8]) -> io::Result<usize> {
        let buf = (self.0).0.lock().unwrap();
        if self.1 >= buf.len() {
            // EOF
            return Ok(0);
        }
        let remaining = buf.len() - self.1;
        let to_read = if dst.len() > remaining {
            remaining
        } else {
            dst.len()
        };

        (&mut dst[0..to_read]).copy_from_slice(&buf[self.1..self.1 + to_read]);
        self.1 += to_read;
        Ok(to_read)
    }
}

/// A MemFileWriter holds a reference to a MemFile and a write offset.
struct MemFileWriter(MemFile, usize);

impl MemFileWriter {
    fn new(f: MemFile, append: bool) -> MemFileWriter {
        let len = f.0.lock().unwrap().len();
        MemFileWriter(f, if append { len } else { 0 })
    }
}

impl Write for MemFileWriter {
    fn write(&mut self, src: &[u8]) -> io::Result<usize> {
        let mut buf = (self.0).0.lock().unwrap();
        // Write is append.
        if self.1 == buf.len() {
            buf.extend_from_slice(src);
        } else {
            // Write in the middle, possibly appending.
            let remaining = buf.len() - self.1;
            if src.len() <= remaining {
                // src fits into buffer.
                (&mut buf[self.1..self.1 + src.len()]).copy_from_slice(src);
            } else {
                // src doesn't fit; first copy what fits, then append the rest/
                (&mut buf[self.1..self.1 + remaining]).copy_from_slice(&src[0..remaining]);
                buf.extend_from_slice(&src[remaining..src.len()]);
            }
        }
        self.1 += src.len();
        Ok(src.len())
    }
    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

impl RandomAccess for MemFile {
    fn read_at(&self, off: usize, dst: &mut [u8]) -> Result<usize> {
        let grd = self.0.lock().unwrap();
        let buf: &BufferBackedFile = grd.deref();
        buf.read_at(off, dst)
    }
}

struct MemFSEntry {
    f: MemFile,
    locked: bool,
}

/// MemFS implements a completely in-memory file system, both for testing and temporary in-memory
/// databases. It supports full concurrency.
pub struct MemFS {
    store: Arc<Mutex<HashMap<String, MemFSEntry>>>,
}

impl MemFS {
    fn new() -> MemFS {
        MemFS { store: Arc::new(Mutex::new(HashMap::new())) }
    }

    /// Open a file for reading. The caller can use the MemFile either inside a MemFileReader or as
    /// RandomAccess.
    fn open_r(&self, p: &Path) -> Result<MemFile> {
        let mut fs = self.store.lock().unwrap();
        match fs.entry(path_to_string(p)) {
            Entry::Occupied(o) => Ok(o.get().f.clone()),
            Entry::Vacant(v) => {
                let f = MemFile::new();
                v.insert(MemFSEntry {
                    f: f.clone(),
                    locked: false,
                });
                Ok(f)
            }
        }
    }
    /// Open a file for writing.
    fn open_w(&self, p: &Path, append: bool, truncate: bool) -> Result<Box<Write>> {
        // We can reuse open_r, because we just need to pack the MemFile inside a MemFileWriter.
        let f = self.open_r(p)?;
        if truncate {
            f.0.lock().unwrap().clear();
        }
        Ok(Box::new(MemFileWriter::new(f, append)))
    }
    fn exists(&self, p: &Path) -> Result<bool> {
        let fs = self.store.lock().unwrap();
        Ok(fs.contains_key(&path_to_string(p)))
    }
    fn children_of(&self, p: &Path) -> Result<Vec<String>> {
        let fs = self.store.lock().unwrap();
        let prefix = path_to_string(p);
        let mut children = Vec::new();
        for k in fs.keys() {
            if k.starts_with(&prefix) {
                children.push(k.clone());
            }
        }
        Ok(children)
    }
    fn size_of(&self, p: &Path) -> Result<usize> {
        let mut fs = self.store.lock().unwrap();
        match fs.entry(path_to_string(p)) {
            Entry::Occupied(o) => Ok(o.get().f.0.lock().unwrap().len()),
            _ => Err(Status::new(StatusCode::NotFound, "not found")),
        }
    }
    fn delete(&self, p: &Path) -> Result<()> {
        let mut fs = self.store.lock().unwrap();
        match fs.entry(path_to_string(p)) {
            Entry::Occupied(o) => {
                o.remove_entry();
                Ok(())
            }
            _ => Err(Status::new(StatusCode::NotFound, "not found")),
        }
    }
    // mkdir and rmdir are no-ops in MemFS.
    fn rename(&self, from: &Path, to: &Path) -> Result<()> {
        let mut fs = self.store.lock().unwrap();
        match fs.remove(&path_to_string(from)) {
            Some(v) => {
                fs.insert(path_to_string(to), v);
                Ok(())
            }
            None => Err(Status::new(StatusCode::NotFound, "not found")),
        }
    }
    fn lock(&self, p: &Path) -> Result<FileLock> {
        let mut fs = self.store.lock().unwrap();
        match fs.entry(path_to_string(p)) {
            Entry::Occupied(mut o) => {
                if o.get().locked {
                    Err(Status::new(StatusCode::LockError, "already locked"))
                } else {
                    o.get_mut().locked = true;
                    Ok(FileLock { id: path_to_string(p) })
                }
            }
            Entry::Vacant(_) => Err(Status::new(StatusCode::NotFound, "not found")),
        }
    }
    fn unlock(&self, l: FileLock) -> Result<()> {
        let mut fs = self.store.lock().unwrap();
        match fs.entry(l.id) {
            Entry::Occupied(mut o) => {
                if !o.get().locked {
                    Err(Status::new(StatusCode::LockError, "unlocking unlocked file"))
                } else {
                    o.get_mut().locked = false;
                    Ok(())
                }
            }
            Entry::Vacant(_) => Err(Status::new(StatusCode::NotFound, "not found")),
        }
    }
}

pub struct MemEnv {
}

#[cfg(test)]
mod tests {
    use super::*;

    fn new_memfile(v: Vec<u8>) -> MemFile {
        MemFile(Arc::new(Mutex::new(v)))
    }

    #[test]
    fn test_mem_env_memfile_read() {
        let f = new_memfile(vec![1, 2, 3, 4, 5, 6, 7, 8]);
        let mut buf: [u8; 1] = [0];
        let mut reader = MemFileReader(f, 0);

        for i in [1, 2, 3, 4, 5, 6, 7, 8].iter() {
            assert_eq!(reader.read(&mut buf).unwrap(), 1);
            assert_eq!(buf, [*i]);
        }
    }

    #[test]
    fn test_mem_env_memfile_write() {
        let f = new_memfile(vec![]);
        let mut w1 = MemFileWriter::new(f.clone(), false);
        assert_eq!(w1.write(&[1, 2, 3]).unwrap(), 3);

        let mut w2 = MemFileWriter::new(f, true);
        assert_eq!(w1.write(&[1, 7, 8, 9]).unwrap(), 4);
        assert_eq!(w2.write(&[4, 5, 6]).unwrap(), 3);

        assert_eq!((w1.0).0.lock().unwrap().as_ref() as &Vec<u8>,
                   &[1, 2, 3, 4, 5, 6, 9]);
    }

    #[test]
    fn test_mem_env_memfile_readat() {
        let f = new_memfile(vec![1, 2, 3, 4, 5]);

        let mut buf = [0; 3];
        assert_eq!(f.read_at(2, &mut buf).unwrap(), 3);
        assert_eq!(buf, [3, 4, 5]);

        assert_eq!(f.read_at(0, &mut buf[0..1]).unwrap(), 1);
        assert_eq!(buf, [1, 4, 5]);

        assert_eq!(f.read_at(5, &mut buf).unwrap(), 0);
        assert_eq!(buf, [1, 4, 5]);

        let mut buf2 = [0; 6];
        assert_eq!(f.read_at(0, &mut buf2[0..5]).unwrap(), 5);
        assert_eq!(buf2, [1, 2, 3, 4, 5, 0]);
        assert_eq!(f.read_at(0, &mut buf2[0..6]).unwrap(), 5);
        assert_eq!(buf2, [1, 2, 3, 4, 5, 0]);
    }

    #[test]
    fn test_mem_env_fs_open_read_write() {
        let fs = MemFS::new();
        let path = Path::new("/a/b/hello.txt");

        {
            let mut w = fs.open_w(&path, false, false).unwrap();
            write!(w, "Hello").unwrap();
            // Append.
            let mut w2 = fs.open_w(&path, true, false).unwrap();
            write!(w2, "World").unwrap();
        }
        {
            let mut r = MemFileReader::new(fs.open_r(&path).unwrap(), 0);
            let mut s = String::new();
            assert_eq!(r.read_to_string(&mut s).unwrap(), 10);
            assert_eq!(s, "HelloWorld");

            let mut r2 = MemFileReader::new(fs.open_r(&path).unwrap(), 2);
            s.clear();
            assert_eq!(r2.read_to_string(&mut s).unwrap(), 8);
            assert_eq!(s, "lloWorld");

        }
        assert_eq!(fs.size_of(&path).unwrap(), 10);
        assert!(fs.exists(&path).unwrap());
        assert!(!fs.exists(&Path::new("/non/existing/path")).unwrap());
    }

    #[test]
    fn test_mem_env_fs_open_read_write_append_truncate() {
        let fs = MemFS::new();
        let path = Path::new("/a/b/hello.txt");

        {
            let mut w0 = fs.open_w(&path, false, true).unwrap();
            write!(w0, "GarbageGarbageGarbageGarbageGarbage").unwrap();

            // Truncate.
            let mut w = fs.open_w(&path, false, true).unwrap();
            write!(w, "Xyz").unwrap();
            // Write to the beginning.
            let mut w2 = fs.open_w(&path, false, false).unwrap();
            write!(w2, "1").unwrap();
        }
        {
            let mut r = MemFileReader::new(fs.open_r(&path).unwrap(), 0);
            let mut s = String::new();
            assert_eq!(r.read_to_string(&mut s).unwrap(), 3);
            assert_eq!(s, "1yz");

        }
        assert_eq!(fs.size_of(&path).unwrap(), 3);
        assert!(fs.exists(&path).unwrap());
        assert!(!fs.exists(&Path::new("/non/existing/path")).unwrap());
    }

    #[test]
    fn test_mem_env_fs_metadata_operations() {
        let fs = MemFS::new();
        let path = Path::new("/a/b/hello.file");

        // Make file/remove file.
        {
            let mut w = fs.open_w(&path, false, false).unwrap();
            write!(w, "Hello").unwrap();
        }
        assert!(fs.exists(&path).unwrap());
        assert_eq!(fs.size_of(&path).unwrap(), 5);
        fs.delete(&path).is_ok();
        assert!(!fs.exists(&path).unwrap());

        // Rename file.
        let newpath = Path::new("/a/b/hello2.file");
        {
            let mut w = fs.open_w(&path, false, false).unwrap();
            write!(w, "Hello").unwrap();
        }
        assert!(fs.exists(&path).unwrap());
        assert!(!fs.exists(&newpath).unwrap());
        assert_eq!(fs.size_of(&path).unwrap(), 5);
        assert!(fs.size_of(&newpath).is_err());

        fs.rename(&path, &newpath).unwrap();

        assert!(!fs.exists(&path).unwrap());
        assert!(fs.exists(&newpath).unwrap());
        assert_eq!(fs.size_of(&newpath).unwrap(), 5);
        assert!(fs.size_of(&path).is_err());
    }

    #[test]
    fn test_mem_env_fs_children() {
        let fs = MemFS::new();
        let (path1, path2, path3) =
            (Path::new("/a/1.txt"), Path::new("/a/2.txt"), Path::new("/b/1.txt"));

        for p in &[&path1, &path2, &path3] {
            fs.open_w(*p, false, false).unwrap();
        }
        let children = fs.children_of(&Path::new("/a/")).unwrap();
        // TODO: Find proper testing framework.
        assert!((children == vec!["/a/1.txt", "/a/2.txt"]) ||
                (children == vec!["/a/2.txt", "/a/1.txt"]));
    }
}
