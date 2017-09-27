use env::{Env, FileLock, Logger, RandomAccess};
use env_common::{micros, sleep_for};
use error::{err, Status, StatusCode, Result};

use std::collections::HashMap;
use std::fs;
use std::io::{Read, Write};
use std::iter::FromIterator;
use std::mem;
use std::os::unix::io::IntoRawFd;
use std::path::Path;
use std::sync::{Arc, Mutex};

use libc;

const F_RDLCK: libc::c_short = 0;
const F_WRLCK: libc::c_short = 1;
const F_UNLCK: libc::c_short = 2;

type FileDescriptor = i32;

#[derive(Clone)]
pub struct PosixDiskEnv {
    locks: Arc<Mutex<HashMap<String, FileDescriptor>>>,
}

impl PosixDiskEnv {
    pub fn new() -> PosixDiskEnv {
        PosixDiskEnv { locks: Arc::new(Mutex::new(HashMap::new())) }
    }
}

// Note: We're using Ok(f()?) in several locations below in order to benefit from the automatic
// error conversion using std::convert::From.
impl Env for PosixDiskEnv {
    fn open_sequential_file(&self, p: &Path) -> Result<Box<Read>> {
        Ok(Box::new(try!(fs::OpenOptions::new().read(true).open(p))))
    }
    fn open_random_access_file(&self, p: &Path) -> Result<Box<RandomAccess>> {
        Ok(fs::OpenOptions::new().read(true)
            .open(p)
            .map(|f| {
                let b: Box<RandomAccess> = Box::new(f);
                b
            })?)
    }
    fn open_writable_file(&self, p: &Path) -> Result<Box<Write>> {
        Ok(Box::new(try!(fs::OpenOptions::new()
            .create(true)
            .write(true)
            .append(false)
            .open(p))))
    }
    fn open_appendable_file(&self, p: &Path) -> Result<Box<Write>> {
        Ok(Box::new(try!(fs::OpenOptions::new()
            .create(true)
            .write(true)
            .append(true)
            .open(p))))
    }

    fn exists(&self, p: &Path) -> Result<bool> {
        Ok(p.exists())
    }
    fn children(&self, p: &Path) -> Result<Vec<String>> {
        let dir_reader = try!(fs::read_dir(p));
        let filenames = dir_reader.map(|r| {
                if !r.is_ok() {
                    "".to_string()
                } else {
                    let direntry = r.unwrap();
                    direntry.file_name().into_string().unwrap_or("".to_string())
                }
            })
            .filter(|s| !s.is_empty());
        Ok(Vec::from_iter(filenames))
    }
    fn size_of(&self, p: &Path) -> Result<usize> {
        let meta = try!(fs::metadata(p));
        Ok(meta.len() as usize)
    }

    fn delete(&self, p: &Path) -> Result<()> {
        Ok(fs::remove_file(p)?)
    }
    fn mkdir(&self, p: &Path) -> Result<()> {
        Ok(fs::create_dir(p)?)
    }
    fn rmdir(&self, p: &Path) -> Result<()> {
        Ok(fs::remove_dir_all(p)?)
    }
    fn rename(&self, old: &Path, new: &Path) -> Result<()> {
        Ok(fs::rename(old, new)?)
    }

    fn lock(&self, p: &Path) -> Result<FileLock> {
        let mut locks = self.locks.lock().unwrap();

        if locks.contains_key(&p.to_str().unwrap().to_string()) {
            Err(Status::new(StatusCode::AlreadyExists, "Lock is held"))
        } else {
            let f = try!(fs::OpenOptions::new().write(true).create(true).open(p));

            let flock_arg = libc::flock {
                l_type: F_WRLCK,
                l_whence: libc::SEEK_SET as libc::c_short,
                l_start: 0,
                l_len: 0,
                l_pid: 0,
            };
            let fd = f.into_raw_fd();
            let result = unsafe {
                libc::fcntl(fd,
                            libc::F_SETLK,
                            mem::transmute::<&libc::flock, *const libc::flock>(&&flock_arg))
            };

            if result < 0 {
                return Err(Status::new(StatusCode::AlreadyExists, "Lock is held (fcntl)"));
            }

            locks.insert(p.to_str().unwrap().to_string(), fd);
            let lock = FileLock { id: p.to_str().unwrap().to_string() };
            Ok(lock)
        }
    }
    fn unlock(&self, l: FileLock) -> Result<()> {
        let mut locks = self.locks.lock().unwrap();

        if !locks.contains_key(&l.id) {
            return err(StatusCode::LockError, "unlocking a file that is not locked!");
        } else {
            let fd = locks.remove(&l.id).unwrap();
            let flock_arg = libc::flock {
                l_type: F_UNLCK,
                l_whence: libc::SEEK_SET as libc::c_short,
                l_start: 0,
                l_len: 0,
                l_pid: 0,
            };
            let result = unsafe {
                libc::fcntl(fd,
                            libc::F_SETLK,
                            mem::transmute::<&libc::flock, *const libc::flock>(&&flock_arg))
            };
            if result < 0 {
                return err(StatusCode::LockError, "unlock failed");
            }
            Ok(())
        }
    }

    fn new_logger(&self, p: &Path) -> Result<Logger> {
        self.open_appendable_file(p).map(|dst| Logger::new(Box::new(dst)))
    }

    fn micros(&self) -> u64 {
        micros()
    }

    fn sleep_for(&self, micros: u32) {
        sleep_for(micros);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use std::convert::AsRef;
    use std::io::Write;
    use std::iter::FromIterator;

    #[test]
    fn test_files() {
        let n = "testfile.xyz".to_string();
        let name = n.as_ref();
        let env = PosixDiskEnv::new();

        assert!(env.open_appendable_file(name).is_ok());
        assert!(env.exists(name).unwrap_or(false));
        assert_eq!(env.size_of(name).unwrap_or(1), 0);
        assert!(env.delete(name).is_ok());

        assert!(env.open_writable_file(name).is_ok());
        assert!(env.exists(name).unwrap_or(false));
        assert_eq!(env.size_of(name).unwrap_or(1), 0);
        assert!(env.delete(name).is_ok());

        {
            let mut f = env.open_writable_file(name).unwrap();
            let _ = f.write("123xyz".as_bytes());
            assert_eq!(env.size_of(name).unwrap_or(0), 6);
        }

        assert!(env.open_sequential_file(name).is_ok());
        assert!(env.open_random_access_file(name).is_ok());

        assert!(env.delete(name).is_ok());
    }

    #[test]
    fn test_locking() {
        let env = PosixDiskEnv::new();
        let n = "testfile.123".to_string();
        let name = n.as_ref();

        {
            let mut f = env.open_writable_file(name).unwrap();
            let _ = f.write("123xyz".as_bytes());
            assert_eq!(env.size_of(name).unwrap_or(0), 6);
        }

        {
            let r = env.lock(name);
            assert!(r.is_ok());
            env.unlock(r.unwrap());
        }

        {
            let r = env.lock(name);
            assert!(r.is_ok());
            let s = env.lock(name);
            assert!(s.is_err());
            env.unlock(r.unwrap());
        }

        assert!(env.delete(name).is_ok());
    }

    #[test]
    fn test_dirs() {
        let d = "subdir/";
        let dirname = d.as_ref();
        let env = PosixDiskEnv::new();

        assert!(env.mkdir(dirname).is_ok());
        assert!(env.open_writable_file(String::from_iter(vec![d.to_string(), "f1.txt".to_string()]
                    .into_iter())
                .as_ref())
            .is_ok());
        assert_eq!(env.children(dirname).unwrap().len(), 1);
        assert!(env.rmdir(dirname).is_ok());
    }
}
