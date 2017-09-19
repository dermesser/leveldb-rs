//! db_impl contains the implementation of the database interface and high-level compaction and
//! maintenance logic.

#![allow(unused_attributes)]

use cmp::InternalKeyCmp;
use env::{Env, FileLock};
use error::{err, StatusCode, Result};
use filter::{BoxedFilterPolicy, InternalFilterPolicy};
use infolog::Logger;
use log::LogWriter;
use key_types::parse_internal_key;
use memtable::MemTable;
use options::Options;
use table_builder::TableBuilder;
use table_cache::{table_file_name, TableCache};
use types::{share, FileMetaData, FileNum, LdbIterator, NUM_LEVELS, Shared};
use version_edit::VersionEdit;
use version_set::VersionSet;
use version::Version;

use std::io::{self, Write};
use std::mem;
use std::path::Path;

/// DB contains the actual database implemenation. As opposed to the original, this implementation
/// is not concurrent (yet).
pub struct DB {
    name: String,
    lock: Option<FileLock>,

    cmp: InternalKeyCmp,
    fpol: InternalFilterPolicy<BoxedFilterPolicy>,
    opt: Options,

    mem: MemTable,
    imm: Option<MemTable>,

    log: Option<LogWriter<Box<Write>>>,
    log_num: Option<FileNum>,
    cache: Shared<TableCache>,
    vset: VersionSet,

    cstats: [CompactionStats; NUM_LEVELS],
}

impl DB {
    fn new(name: &str, mut opt: Options) -> DB {
        let cache = share(TableCache::new(&name, opt.clone(), opt.max_open_files - 10));
        let vset = VersionSet::new(&name, opt.clone(), cache.clone());

        let log = open_info_log(opt.env.as_ref().as_ref(), &name);
        opt.log = share(log);

        DB {
            name: name.to_string(),
            lock: None,
            cmp: InternalKeyCmp(opt.cmp.clone()),
            fpol: InternalFilterPolicy::new(opt.filter_policy.clone()),

            mem: MemTable::new(opt.cmp.clone()),
            imm: None,

            opt: opt,

            log: None,
            log_num: None,
            cache: cache,
            vset: vset,

            cstats: Default::default(),
        }
    }

    fn add_stats(&mut self, level: usize, cs: CompactionStats) {
        assert!(level < NUM_LEVELS);
        self.cstats[level].add(cs);
    }

    /// make_room_for_write checks if the memtable has become too large, and triggers a compaction
    /// if it's the case.
    fn make_room_for_write(&mut self) -> Result<()> {
        if self.mem.approx_mem_usage() < self.opt.write_buffer_size {
            return Ok(());
        } else {
            // Create new memtable.
            let logn = self.vset.new_file_number();
            let logf = self.opt.env.open_writable_file(Path::new(&log_file_name(&self.name, logn)));
            if logf.is_err() {
                self.vset.reuse_file_number(logn);
                logf?;
            } else {
                self.log = Some(LogWriter::new(logf.unwrap()));
                self.log_num = Some(logn);

                let mut imm = MemTable::new(self.opt.cmp.clone());
                mem::swap(&mut imm, &mut self.mem);
                self.imm = Some(imm);
                self.maybe_do_compaction();
            }

            return Ok(());
        }
    }

    /// maybe_do_compaction starts a blocking compaction if it makes sense.
    fn maybe_do_compaction(&mut self) {
        if self.imm.is_none() && !self.vset.needs_compaction() {
            return;
        }
        self.start_compaction();
    }

    /// compaction dispatches the different kinds of compactions depending on the current state of
    /// the database.
    fn start_compaction(&mut self) {
        // TODO (maybe): Support manual compactions.
        if self.imm.is_some() {
            if let Err(e) = self.compact_memtable() {
                log!(self.opt.log, "Error while compacting memtable: {}", e);
            }
            return;
        }
        // TODO: rest
    }

    fn compact_memtable(&mut self) -> Result<()> {
        assert!(self.imm.is_some());
        let mut ve = VersionEdit::new();
        let base = self.vset.current();

        let imm = self.imm.take().unwrap();
        if let Err(e) = self.write_l0_table(&imm, &mut ve, Some(&base.borrow())) {
            self.imm = Some(imm);
            return Err(e);
        }
        ve.set_log_num(self.log_num.unwrap_or(0));
        self.vset.log_and_apply(ve)?;
        if let Err(e) = self.delete_obsolete_files() {
            log!(self.opt.log, "Error deleting obsolete files: {}", e);
        }
        Ok(())
    }

    /// write_l0_table writes the given memtable to a table file.
    fn write_l0_table(&mut self,
                      memt: &MemTable,
                      ve: &mut VersionEdit,
                      base: Option<&Version>)
                      -> Result<()> {
        let start_ts = self.opt.env.micros();
        let num = self.vset.new_file_number();
        log!(self.opt.log, "Start write of L0 table {:06}", num);
        let fmd = build_table(&self.name, &self.opt, memt.iter(), num)?;
        log!(self.opt.log, "L0 table {:06} has {} bytes", num, fmd.size);

        let cache_result = self.cache.borrow_mut().get_table(num);
        if let Err(e) = cache_result {
            log!(self.opt.log,
                 "L0 table {:06} not returned by cache: {}",
                 num,
                 e);
            self.opt.env.delete(Path::new(&table_file_name(&self.name, num))).is_ok();
            return Err(e);
        }

        let mut stats = CompactionStats::default();
        stats.micros = self.opt.env.micros() - start_ts;
        stats.written = fmd.size;

        let mut level = 0;
        if let Some(b) = base {
            level = b.pick_memtable_output_level(parse_internal_key(&fmd.smallest).2,
                                                 parse_internal_key(&fmd.largest).2);
        }

        self.add_stats(level, stats);
        ve.add_file(level, fmd);

        Ok(())
    }

    fn delete_obsolete_files(&mut self) -> Result<()> {
        // TODO: implement
        Ok(())
    }
}

#[derive(Debug, Default)]
struct CompactionStats {
    micros: u64,
    read: usize,
    written: usize,
}

impl CompactionStats {
    fn add(&mut self, cs: CompactionStats) {
        self.micros += cs.micros;
        self.read += cs.read;
        self.written += cs.written;
    }
}

pub fn build_table<I: LdbIterator>(dbname: &str,
                                   opt: &Options,
                                   mut from: I,
                                   num: FileNum)
                                   -> Result<FileMetaData> {
    from.reset();
    let filename = table_file_name(dbname, num);
    let mut md = FileMetaData::default();

    let (mut kbuf, mut vbuf) = (vec![], vec![]);
    let mut firstkey = None;
    // lastkey is what remains in kbuf.

    // Clean up file if write fails at any point.
    //
    // TODO: Replace with catch {} when available.
    let r = (|| -> Result<()> {
        let f = opt.env.open_writable_file(Path::new(&filename))?;
        let mut builder = TableBuilder::new(opt.clone(), f);
        while from.advance() {
            assert!(from.current(&mut kbuf, &mut vbuf));
            if firstkey.is_none() {
                firstkey = Some(kbuf.clone());
            }
            builder.add(&kbuf, &vbuf)?;
        }
        builder.finish()?;
        Ok(())
    })();

    if let Err(e) = r {
        opt.env.delete(Path::new(&filename)).is_ok();
        return Err(e);
    }

    md.num = num;
    md.size = opt.env.size_of(Path::new(&filename))?;
    md.smallest = firstkey.unwrap();
    md.largest = kbuf;
    Ok(md)
}

fn log_file_name(db: &str, num: FileNum) -> String {
    format!("{}/{:06}.log", db, num)
}

/// open_info_log opens an info log file in the given database. It transparently returns a
/// /dev/null logger in case the open fails.
fn open_info_log<E: Env + ?Sized>(env: &E, db: &str) -> Logger {
    let logfilename = format!("{}/LOG", db);
    let oldlogfilename = format!("{}/LOG.old", db);
    env.mkdir(Path::new(db)).is_ok();
    if let Ok(e) = env.exists(Path::new(&logfilename)) {
        if e {
            env.rename(Path::new(&logfilename), Path::new(&oldlogfilename)).is_ok();
        }
    }
    if let Ok(w) = env.open_writable_file(Path::new(&logfilename)) {
        Logger(w)
    } else {
        Logger(Box::new(io::sink()))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use options;
    use key_types::LookupKey;
    use mem_env::MemEnv;
    use test_util::LdbIteratorIter;
    use types::ValueType;

    #[test]
    fn test_db_impl_open_info_log() {
        let e = MemEnv::new();
        {
            let l = share(open_info_log(&e, "abc"));
            assert!(e.exists(Path::new("abc/LOG")).unwrap());
            log!(l, "hello {}", "world");
            assert_eq!(12, e.size_of(Path::new("abc/LOG")).unwrap());
        }
        {
            let l = share(open_info_log(&e, "abc"));
            assert!(e.exists(Path::new("abc/LOG.old")).unwrap());
            assert!(e.exists(Path::new("abc/LOG")).unwrap());
            assert_eq!(12, e.size_of(Path::new("abc/LOG.old")).unwrap());
            assert_eq!(0, e.size_of(Path::new("abc/LOG")).unwrap());
            log!(l, "something else");
            log!(l, "and another {}", 1);

            let mut s = String::new();
            let mut r = e.open_sequential_file(Path::new("abc/LOG")).unwrap();
            r.read_to_string(&mut s).unwrap();
            assert_eq!("something else\nand another 1\n", &s);
        }
    }

    fn build_memtable() -> MemTable {
        let mut mt = MemTable::new(options::for_test().cmp);
        let mut i = 1;
        for k in ["abc", "def", "ghi", "jkl", "mno", "aabc", "test123"].iter() {
            mt.add(i,
                   ValueType::TypeValue,
                   k.as_bytes(),
                   "looooongval".as_bytes());
            i += 1;
        }
        mt
    }

    #[test]
    fn test_db_impl_build_table() {
        let mut opt = options::for_test();
        opt.block_size = 128;
        let mt = build_memtable();

        let f = build_table("db", &opt, mt.iter(), 123).unwrap();
        let path = Path::new("db/000123.ldb");

        assert_eq!(LookupKey::new("aabc".as_bytes(), 6).internal_key(),
                   f.smallest.as_slice());
        assert_eq!(LookupKey::new("test123".as_bytes(), 7).internal_key(),
                   f.largest.as_slice());
        assert_eq!(379, f.size);
        assert_eq!(123, f.num);
        assert!(opt.env.exists(path).unwrap());

        {
            // Read table back in.
            let mut tc = TableCache::new("db", opt.clone(), 100);
            let tbl = tc.get_table(123).unwrap();
            assert_eq!(mt.len(), LdbIteratorIter::wrap(&mut tbl.iter()).count());
        }

        {
            // Corrupt table; make sure it doesn't load fully.
            let mut buf = vec![];
            opt.env.open_sequential_file(path).unwrap().read_to_end(&mut buf).unwrap();
            buf[150] += 1;
            opt.env.open_writable_file(path).unwrap().write_all(&buf).unwrap();

            let mut tc = TableCache::new("db", opt.clone(), 100);
            let tbl = tc.get_table(123).unwrap();
            // The last two entries are skipped due to the corruption above.
            assert_eq!(5,
                       LdbIteratorIter::wrap(&mut tbl.iter()).map(|v| println!("{:?}", v)).count());
        }
    }

    #[test]
    fn test_db_impl_make_room_for_write() {
        let mut opt = options::for_test();
        opt.write_buffer_size = 25;
        let mut db = DB::new("db", opt);

        // Fill up memtable.
        db.mem = build_memtable();

        // Trigger memtable compaction.
        db.make_room_for_write().unwrap();
        assert_eq!(0, db.mem.len());
        assert!(db.opt.env.exists(Path::new("db/000002.log")).unwrap());
        assert!(db.opt.env.exists(Path::new("db/000003.ldb")).unwrap());
        assert_eq!(351, db.opt.env.size_of(Path::new("db/000003.ldb")).unwrap());
    }
}
