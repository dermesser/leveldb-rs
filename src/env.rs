//! An `env` is an abstraction layer that allows the database to run both on different platforms as
//! well as persisting data on disk or in memory.

use std::collections::HashSet;
use std::fs;
use std::io::{Read, Write, Seek, Result, Error, ErrorKind};
use std::iter::FromIterator;
use std::mem;
use std::os::unix::io::{FromRawFd, IntoRawFd};
use std::path::Path;
use std::sync::Mutex;
use std::thread;
use std::time;

use libc;

const F_RDLCK: libc::c_short = 0;
const F_WRLCK: libc::c_short = 1;
const F_UNLCK: libc::c_short = 2;

pub trait Env {
    type SequentialReader: Read;
    type RandomReader: Read + Seek;
    type Writer: Write;

    fn open_sequential_file(&self, &Path) -> Result<Self::SequentialReader>;
    fn open_random_access_file(&self, &Path) -> Result<Self::RandomReader>;
    fn open_writable_file(&self, &Path) -> Result<Self::Writer>;
    fn open_appendable_file(&self, &Path) -> Result<Self::Writer>;

    fn exists(&self, &Path) -> Result<bool>;
    fn children(&self, &Path) -> Result<Vec<String>>;
    fn size_of(&self, &Path) -> Result<usize>;

    fn delete(&self, &Path) -> Result<()>;
    fn mkdir(&self, &Path) -> Result<()>;
    fn rmdir(&self, &Path) -> Result<()>;
    fn rename(&self, &Path, &Path) -> Result<()>;

    fn lock(&mut self, &Path) -> Result<FileLock>;
    fn unlock(&mut self, l: FileLock);

    fn new_logger(&self, &Path) -> Result<Logger>;

    fn micros(&self) -> u64;
    fn sleep_for(&self, micros: u32);
}

pub struct Logger {
    dst: fs::File,
}

impl Logger {
    fn log(&mut self, message: &String) {
        let _ = self.dst.write(message.as_bytes());
        let _ = self.dst.write("\n".as_bytes());
    }
}

pub struct FileLock {
    p: String,
    f: fs::File,
}

pub struct DiskPosixEnv {
    locks: Mutex<HashSet<String>>,
}

impl Env for DiskPosixEnv {
    type SequentialReader = fs::File;
    type RandomReader = fs::File;
    type Writer = fs::File;

    fn open_sequential_file(&self, p: &Path) -> Result<Self::SequentialReader> {
        fs::OpenOptions::new().read(true).open(p)
    }
    fn open_random_access_file(&self, p: &Path) -> Result<Self::RandomReader> {
        fs::OpenOptions::new().read(true).open(p)
    }
    fn open_writable_file(&self, p: &Path) -> Result<Self::Writer> {
        fs::OpenOptions::new().write(true).append(false).open(p)
    }
    fn open_appendable_file(&self, p: &Path) -> Result<Self::Writer> {
        fs::OpenOptions::new().write(true).append(true).open(p)
    }

    fn exists(&self, p: &Path) -> Result<bool> {
        Ok(p.exists())
    }
    fn children(&self, p: &Path) -> Result<Vec<String>> {
        let dir_reader = try!(fs::read_dir(p));
        let filenames = dir_reader.map(|r| {
                if !r.is_ok() {
                    return "".to_string();
                }
                let direntry = r.unwrap();
                direntry.file_name().into_string().unwrap_or("".to_string())
            })
            .filter(|s| !s.is_empty());
        Ok(Vec::from_iter(filenames))
    }
    fn size_of(&self, p: &Path) -> Result<usize> {
        let meta = try!(fs::metadata(p));
        Ok(meta.len() as usize)
    }

    fn delete(&self, p: &Path) -> Result<()> {
        fs::remove_file(p)
    }
    fn mkdir(&self, p: &Path) -> Result<()> {
        fs::create_dir(p)
    }
    fn rmdir(&self, p: &Path) -> Result<()> {
        fs::remove_dir_all(p)
    }
    fn rename(&self, old: &Path, new: &Path) -> Result<()> {
        fs::rename(old, new)
    }

    fn lock(&mut self, p: &Path) -> Result<FileLock> {
        let mut locks = self.locks.lock().unwrap();

        if locks.contains(&p.to_str().unwrap().to_string()) {
            Err(Error::new(ErrorKind::AlreadyExists, "Lock is held"))
        } else {
            let f = try!(fs::OpenOptions::new().read(true).open(p));

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
                return Err(Error::new(ErrorKind::PermissionDenied, "Lock is held (fcntl)"));
            }

            locks.insert(p.to_str().unwrap().to_string());
            let lock = FileLock {
                p: p.to_str().unwrap().to_string(),
                f: unsafe { fs::File::from_raw_fd(fd) },
            };
            Ok(lock)
        }
    }
    fn unlock(&mut self, l: FileLock) {
        let mut locks = self.locks.lock().unwrap();

        if !locks.contains(&l.p) {
            panic!("Unlocking a file that is not locked!");
        } else {
            locks.remove(&l.p);

            let flock_arg = libc::flock {
                l_type: F_UNLCK,
                l_whence: libc::SEEK_SET as libc::c_short,
                l_start: 0,
                l_len: 0,
                l_pid: 0,
            };
            let result = unsafe {
                libc::fcntl(l.f.into_raw_fd(),
                            libc::F_SETLK,
                            mem::transmute::<&libc::flock, *const libc::flock>(&&flock_arg))
            };

            if result < 0 {
                // ignore for now
            }

            ()
        }
    }

    fn new_logger(&self, p: &Path) -> Result<Logger> {
        self.open_appendable_file(p).map(|dst| Logger { dst: dst })
    }

    fn micros(&self) -> u64 {
        loop {
            let now = time::SystemTime::now().duration_since(time::UNIX_EPOCH);

            match now {
                Err(_) => continue,
                Ok(dur) => return dur.as_secs() * 1000000 + (dur.subsec_nanos() / 1000) as u64,
            }
        }
    }

    fn sleep_for(&self, micros: u32) {
        thread::sleep(time::Duration::new(0, micros * 1000));
    }
}
