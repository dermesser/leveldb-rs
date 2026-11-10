//! Bulk loading: writes a new database directly from sorted entries.
//!
//! A database that is filled once, from entries whose order is known, does not need the
//! write-ahead log, the memtable or any compaction: its tables can be written directly, each
//! entry compressed exactly once. [`TableWriter`] builds one table file. Tables that cover
//! disjoint key ranges are independent of each other, so they can be built on separate
//! threads, each thread with its own [`Options`] (which are not `Send`).
//! [`install_tables`] then writes the manifest that makes the tables a database.
//!
//! ```
//! use rusty_leveldb::bulk::{install_tables, level_for_size, TableWriter};
//! use rusty_leveldb::{LdbIterator, DB};
//! use std::path::Path;
//!
//! let opt = rusty_leveldb::in_memory();
//! let db = Path::new("bulkdb");
//!
//! // File numbers must be unique within the database and greater than zero.
//! let mut writer = TableWriter::create(&opt, db, 1, 1).unwrap();
//! writer.add(b"apple", b"red").unwrap();
//! writer.add(b"banana", b"yellow").unwrap();
//! let table = writer.finish().unwrap().unwrap();
//!
//! install_tables(&opt, db, level_for_size(table.size() as u64), vec![table]).unwrap();
//!
//! let mut db = DB::open(db, opt).unwrap();
//! assert_eq!(b"yellow", &*db.get(b"banana").unwrap());
//! ```

use crate::env::Env;
use crate::error::{err, Result, StatusCode};
use crate::key_types::{LookupKey, ValueType};
use crate::log::LogWriter;
use crate::options::Options;
use crate::table_builder::TableBuilder;
use crate::table_cache::table_file_name;
use crate::types::{FileMetaData, FileNum, SequenceNumber, MAX_SEQUENCE_NUMBER, NUM_LEVELS};
use crate::version_edit::VersionEdit;
use crate::version_set::{current_file_name, manifest_file_name, set_current_file};

use std::cmp::Ordering;
use std::io::{BufWriter, Write};
use std::path::{Path, PathBuf};

/// Buffer between the table builder and its file, so a block is one write, not three.
const FILE_BUFFER_SIZE: usize = 64 << 10;

/// Highest number a table may take, which leaves the database all the numbers it can use.
const MAX_TABLE_NUMBER: FileNum = FileNum::MAX / 2;

/// Largest total size of the files on level 1; each deeper level holds ten times more.
const LEVEL_1_MAX_BYTES: u64 = 10 << 20;

/// A finished table file, as [`install_tables`] needs it. `Send`, unlike the writer.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct BulkTable {
    num: FileNum,
    size: usize,
    entries: usize,
    last_seq: SequenceNumber,
    // Both in internal key format, as the manifest stores them.
    smallest: Vec<u8>,
    largest: Vec<u8>,
}

impl BulkTable {
    /// The number the file is named after.
    pub fn num(&self) -> FileNum {
        self.num
    }

    /// Size of the file in bytes.
    pub fn size(&self) -> usize {
        self.size
    }

    /// Number of entries in the table.
    pub fn entries(&self) -> usize {
        self.entries
    }

    /// Highest sequence number of the table's entries.
    pub fn last_seq(&self) -> SequenceNumber {
        self.last_seq
    }

    /// The first key of the table.
    pub fn smallest_key(&self) -> &[u8] {
        user_key(&self.smallest)
    }

    /// The last key of the table.
    pub fn largest_key(&self) -> &[u8] {
        user_key(&self.largest)
    }
}

/// The user key of an internal key, which ends in an 8 byte tag.
fn user_key(internal_key: &[u8]) -> &[u8] {
    &internal_key[..internal_key.len() - 8]
}

/// Whether `db` holds a database, or the log or manifest of one. A log left behind would be
/// replayed into the new database when it is opened.
fn holds_database(env: &dyn Env, db: &Path) -> Result<bool> {
    if env.exists(&current_file_name(db))? {
        return Ok(true);
    }
    Ok(env.children(db)?.iter().any(|child| {
        let name = child.to_string_lossy();
        name.starts_with("MANIFEST-") || name.ends_with(".log")
    }))
}

fn refuse_existing_database<T>() -> Result<T> {
    err(
        StatusCode::AlreadyExists,
        "the directory already holds a database",
    )
}

/// Writes one table file of a database that is being bulk loaded.
pub struct TableWriter {
    opt: Options,
    path: PathBuf,
    builder: Option<TableBuilder<BufWriter<Box<dyn Write>>>>,
    /// Set by a failed write. The table builder cannot go on after one, so the writer
    /// refuses instead of handing it another entry.
    failed: bool,
    num: FileNum,
    next_seq: SequenceNumber,
    entries: usize,
    smallest: Vec<u8>,
    largest: Vec<u8>,
}

impl TableWriter {
    /// Creates the table file numbered `num` in the database directory `db`, which must
    /// exist and must not hold a database: a table of that number would be overwritten.
    /// Entries get consecutive sequence numbers from `first_seq` on.
    ///
    /// File numbers must be unique within the database and greater than zero. Sequence
    /// numbers need not be unique across tables, since no key occurs twice.
    pub fn create(
        opt: &Options,
        db: &Path,
        num: FileNum,
        first_seq: SequenceNumber,
    ) -> Result<TableWriter> {
        // The manifest and whatever the database creates later are numbered after the tables.
        if num == 0 || num > MAX_TABLE_NUMBER {
            return err(
                StatusCode::InvalidArgument,
                "table file number out of range",
            );
        }
        if first_seq >= MAX_SEQUENCE_NUMBER {
            return err(StatusCode::InvalidArgument, "sequence number out of range");
        }
        if opt.env.exists(&current_file_name(db))? {
            return refuse_existing_database();
        }
        let path = table_file_name(db, num);
        let file = opt.env.open_writable_file(&path)?;
        let dst = BufWriter::with_capacity(FILE_BUFFER_SIZE, file);
        Ok(TableWriter {
            opt: opt.clone(),
            path,
            builder: Some(TableBuilder::new(opt.clone(), dst)),
            failed: false,
            num,
            next_seq: first_seq,
            entries: 0,
            smallest: Vec::new(),
            largest: Vec::new(),
        })
    }

    /// Adds an entry. Keys must be added in strictly ascending order of the comparator.
    ///
    /// After an error from the file or the compressor the table is lost: every later call
    /// returns an error, and [`finish`](TableWriter::finish) removes the file.
    pub fn add(&mut self, key: &[u8], value: &[u8]) -> Result<()> {
        if self.failed {
            return err(StatusCode::IOError, "an earlier write to the table failed");
        }
        if self.entries > 0 && self.opt.cmp.cmp(user_key(&self.largest), key) != Ordering::Less {
            return err(
                StatusCode::InvalidArgument,
                "bulk-loaded keys must be strictly ascending",
            );
        }
        // The highest sequence number stays free for the database's own writes.
        if self.next_seq >= MAX_SEQUENCE_NUMBER {
            return err(StatusCode::InvalidArgument, "sequence number out of range");
        }
        let internal = LookupKey::new_full(key, self.next_seq, ValueType::TypeValue);
        let internal = internal.internal_key();
        let added = self
            .builder
            .as_mut()
            .expect("builder is present until finish")
            .add(internal, value);
        if added.is_err() {
            self.failed = true;
        }
        added?;
        if self.entries == 0 {
            self.smallest = internal.to_vec();
        }
        self.largest.clear();
        self.largest.extend_from_slice(internal);
        self.entries += 1;
        self.next_seq += 1;
        Ok(())
    }

    /// Number of entries added so far.
    pub fn entries(&self) -> usize {
        self.entries
    }

    /// Roughly the size the file would have if it were finished now.
    pub fn size_estimate(&self) -> usize {
        self.builder
            .as_ref()
            .expect("builder is present until finish")
            .size_estimate()
    }

    /// Finishes the file. A writer that got no entry leaves no file and returns `None`,
    /// and one whose write failed removes what it wrote and returns an error.
    pub fn finish(mut self) -> Result<Option<BulkTable>> {
        let builder = self
            .builder
            .take()
            .expect("builder is present until finish");
        if self.failed {
            drop(builder);
            let _ = self.opt.env.delete(&self.path);
            return err(StatusCode::IOError, "an earlier write to the table failed");
        }
        if self.entries == 0 {
            drop(builder);
            self.opt.env.delete(&self.path)?;
            return Ok(None);
        }
        let size = match builder.finish() {
            Ok(size) => size,
            Err(e) => {
                let _ = self.opt.env.delete(&self.path);
                return Err(e);
            }
        };
        Ok(Some(BulkTable {
            num: self.num,
            size,
            entries: self.entries,
            last_seq: self.next_seq - 1,
            smallest: std::mem::take(&mut self.smallest),
            largest: std::mem::take(&mut self.largest),
        }))
    }
}

/// The shallowest level a bulk load of `total_bytes` fits without being compacted further
/// on the first open: level 1 below 10 MiB, ten times more on each level below it. A level
/// that is exactly full is already due for compaction.
pub fn level_for_size(total_bytes: u64) -> usize {
    let mut level = 1;
    let mut max_bytes = LEVEL_1_MAX_BYTES;
    while total_bytes >= max_bytes && level < NUM_LEVELS - 1 {
        level += 1;
        max_bytes = max_bytes.saturating_mul(10);
    }
    level
}

/// Makes `tables` the contents of a new database in the directory `db`, all on `level`.
///
/// The tables must cover disjoint key ranges, as the files of every level but the first do,
/// and must have been written into `db` through the same `Env`. The directory must not
/// hold a database yet, nor the log or manifest of one. An empty list makes an empty
/// database.
pub fn install_tables(
    opt: &Options,
    db: &Path,
    level: usize,
    mut tables: Vec<BulkTable>,
) -> Result<()> {
    if level >= NUM_LEVELS {
        return err(StatusCode::InvalidArgument, "no such level");
    }
    let env = opt.env.as_ref().as_ref();
    if holds_database(env, db)? {
        return refuse_existing_database();
    }

    tables.sort_by(|a, b| opt.cmp.cmp(a.smallest_key(), b.smallest_key()));
    for pair in tables.windows(2) {
        if opt.cmp.cmp(pair[0].largest_key(), pair[1].smallest_key()) != Ordering::Less {
            return err(
                StatusCode::InvalidArgument,
                "bulk-loaded tables must cover disjoint key ranges",
            );
        }
    }
    let mut nums: Vec<FileNum> = tables.iter().map(|t| t.num).collect();
    nums.sort_unstable();
    if nums.windows(2).any(|pair| pair[0] == pair[1]) {
        return err(StatusCode::InvalidArgument, "duplicate table file number");
    }

    for table in &tables {
        if env.size_of(&table_file_name(db, table.num))? != table.size {
            return err(
                StatusCode::InvalidArgument,
                "a table file differs from what its writer finished",
            );
        }
    }

    // `TableWriter::create` keeps the numbers low enough for these two to fit.
    let manifest_num = nums.last().map_or(1, |last| last + 1);
    let mut edit = VersionEdit::new();
    edit.set_comparator_name(opt.cmp.id());
    edit.set_log_num(0);
    edit.set_next_file(manifest_num + 1);
    edit.set_last_seq(tables.iter().map(|t| t.last_seq).max().unwrap_or(0));
    for table in tables {
        edit.add_file(
            level,
            FileMetaData {
                allowed_seeks: 0,
                num: table.num,
                size: table.size,
                smallest: table.smallest.into(),
                largest: table.largest.into(),
            },
        );
    }

    let manifest_path = manifest_file_name(db, manifest_num);
    let written = (|| {
        let manifest = env.open_writable_file(&manifest_path)?;
        let mut log = LogWriter::new(BufWriter::new(manifest));
        log.add_record(&edit.encode())?;
        log.flush()?;
        drop(log);
        set_current_file(env, db, manifest_num)
    })();
    if written.is_err() {
        // Left behind, it would have the next attempt take the directory for a database.
        let _ = env.delete(&manifest_path);
    }
    written
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::LdbIterator;
    use crate::{options, DB};

    fn key(i: usize) -> Vec<u8> {
        format!("key{i:06}").into_bytes()
    }

    fn value(i: usize) -> Vec<u8> {
        // Some of the values are larger than a block.
        vec![(i % 251) as u8; 16 + (i % 7) * 1500]
    }

    /// Tables of `per_table` entries each, covering `0..count`.
    fn build_tables(opt: &Options, db: &Path, count: usize, per_table: usize) -> Vec<BulkTable> {
        let mut tables = Vec::new();
        for (t, start) in (0..count).step_by(per_table).enumerate() {
            let mut writer =
                TableWriter::create(opt, db, t as FileNum + 1, start as u64 + 1).unwrap();
            for i in start..(start + per_table).min(count) {
                writer.add(&key(i), &value(i)).unwrap();
            }
            assert!(writer.size_estimate() > 0);
            tables.push(writer.finish().unwrap().unwrap());
        }
        tables
    }

    #[test]
    fn test_bulk_read_back() {
        let opt = options::for_test();
        let db = Path::new("bulk");
        const COUNT: usize = 2000;
        let mut tables = build_tables(&opt, db, COUNT, 300);
        assert_eq!(tables.len(), 7);
        assert_eq!(tables[0].smallest_key(), key(0));
        assert_eq!(tables[0].largest_key(), key(299));
        assert_eq!(tables[6].last_seq(), COUNT as u64);
        assert_eq!(tables[6].entries(), COUNT - 6 * 300);
        for table in &tables {
            let file = table_file_name(db, table.num());
            assert_eq!(opt.env.size_of(&file).unwrap(), table.size());
        }
        let total: u64 = tables.iter().map(|t| t.size() as u64).sum();
        // Installed in any order.
        tables.reverse();
        install_tables(&opt, db, level_for_size(total), tables).unwrap();

        let mut db = DB::open(db, opt).unwrap();
        for i in (0..COUNT).step_by(37) {
            assert_eq!(db.get(&key(i)).as_deref(), Some(&value(i)[..]));
        }
        assert!(db.get(b"key").is_none());
        assert!(db.get(&key(COUNT)).is_none());

        let mut iter = db.new_iter().unwrap();
        let mut seen = 0;
        while let Some((k, v)) = iter.next() {
            assert_eq!(k, key(seen));
            assert_eq!(v, value(seen));
            seen += 1;
        }
        assert_eq!(seen, COUNT);
    }

    #[test]
    fn test_bulk_later_writes() {
        let opt = options::for_test();
        let db = Path::new("bulk");
        let tables = build_tables(&opt, db, 500, 100);
        install_tables(&opt, db, 1, tables).unwrap();

        let mut db = DB::open(db, opt).unwrap();
        // A later write must win over the bulk-loaded entry of the same key.
        db.put(&key(7), b"new").unwrap();
        db.delete(&key(8)).unwrap();
        db.flush().unwrap();
        db.compact_range(b"key", b"kez").unwrap();
        assert_eq!(db.get(&key(7)).as_deref(), Some(&b"new"[..]));
        assert!(db.get(&key(8)).is_none());
        assert_eq!(db.get(&key(9)).as_deref(), Some(&value(9)[..]));
    }

    #[test]
    fn test_bulk_empty_database() {
        let opt = options::for_test();
        let db = Path::new("bulk");
        let writer = TableWriter::create(&opt, db, 1, 1).unwrap();
        assert!(writer.finish().unwrap().is_none());
        assert!(!opt.env.exists(&table_file_name(db, 1)).unwrap());
        install_tables(&opt, db, 1, Vec::new()).unwrap();

        let mut db = DB::open(db, opt).unwrap();
        assert!(db.get(b"anything").is_none());
        db.put(b"anything", b"now").unwrap();
        assert_eq!(db.get(b"anything").as_deref(), Some(&b"now"[..]));
    }

    #[test]
    fn test_bulk_keys_out_of_order() {
        let opt = options::for_test();
        let db = Path::new("bulk");
        let mut writer = TableWriter::create(&opt, db, 1, 1).unwrap();
        writer.add(b"b", b"1").unwrap();
        assert_eq!(
            writer.add(b"a", b"2").unwrap_err().code,
            StatusCode::InvalidArgument
        );
        assert_eq!(
            writer.add(b"b", b"2").unwrap_err().code,
            StatusCode::InvalidArgument
        );
        writer.add(b"c", b"3").unwrap();
        assert_eq!(writer.entries(), 2);
        assert!(TableWriter::create(&opt, db, 0, 1).is_err());
    }

    #[test]
    fn test_bulk_overlapping_tables() {
        let opt = options::for_test();
        let db = Path::new("bulk");
        let table = |num, keys: &[&[u8]]| {
            let mut writer = TableWriter::create(&opt, db, num, 1).unwrap();
            for key in keys {
                writer.add(key, b"v").unwrap();
            }
            writer.finish().unwrap().unwrap()
        };
        let overlapping = vec![table(1, &[b"a", b"c"]), table(2, &[b"b", b"d"])];
        assert_eq!(
            install_tables(&opt, db, 1, overlapping).unwrap_err().code,
            StatusCode::InvalidArgument
        );
        let touching = vec![table(3, &[b"a", b"c"]), table(4, &[b"c", b"d"])];
        assert_eq!(
            install_tables(&opt, db, 1, touching).unwrap_err().code,
            StatusCode::InvalidArgument
        );
        let same_number = vec![table(5, &[b"a"]), table(5, &[b"b"])];
        assert_eq!(
            install_tables(&opt, db, 1, same_number).unwrap_err().code,
            StatusCode::InvalidArgument
        );
        assert_eq!(
            install_tables(&opt, db, NUM_LEVELS, Vec::new())
                .unwrap_err()
                .code,
            StatusCode::InvalidArgument
        );
    }

    #[test]
    fn test_bulk_existing_database() {
        let opt = options::for_test();
        let db = Path::new("bulk");
        let mut writer = TableWriter::create(&opt, db, 1, 1).unwrap();
        writer.add(b"a", b"first").unwrap();
        let table = writer.finish().unwrap().unwrap();
        let size = table.size();
        install_tables(&opt, db, 1, vec![table.clone()]).unwrap();

        // A second load must not get as far as truncating the live table of that number.
        let refused = TableWriter::create(&opt, db, 1, 1).err().unwrap();
        assert_eq!(refused.code, StatusCode::AlreadyExists);
        assert_eq!(opt.env.size_of(&table_file_name(db, 1)).unwrap(), size);
        assert_eq!(
            install_tables(&opt, db, 1, vec![table]).unwrap_err().code,
            StatusCode::AlreadyExists
        );

        let mut db = DB::open(db, opt).unwrap();
        assert_eq!(db.get(b"a").as_deref(), Some(&b"first"[..]));
    }

    #[test]
    fn test_bulk_leftover_log() {
        let opt = options::for_test();
        let db = Path::new("bulk");
        opt.env
            .open_writable_file(&db.join("000007.log"))
            .unwrap()
            .write_all(b"stale")
            .unwrap();
        let mut writer = TableWriter::create(&opt, db, 1, 1).unwrap();
        writer.add(b"a", b"v").unwrap();
        let table = writer.finish().unwrap().unwrap();
        assert_eq!(
            install_tables(&opt, db, 1, vec![table]).unwrap_err().code,
            StatusCode::AlreadyExists
        );
    }

    #[test]
    fn test_bulk_table_from_other_directory() {
        let opt = options::for_test();
        let mut writer = TableWriter::create(&opt, Path::new("other"), 1, 1).unwrap();
        writer.add(b"a", b"v").unwrap();
        let table = writer.finish().unwrap().unwrap();
        assert!(install_tables(&opt, Path::new("bulk"), 1, vec![table]).is_err());
    }

    #[test]
    fn test_bulk_adjacent_keys_in_two_blocks() {
        let opt = options::for_test();
        let db = Path::new("bulk");
        let big = vec![7u8; opt.block_size * 2];
        let mut writer = TableWriter::create(&opt, db, 1, 1).unwrap();
        // Each value fills a block, so every key ends one and the next key starts another.
        // Nothing sorts between "a" and "a\0", nor above the empty key and below "\0".
        let keys: [&[u8]; 5] = [b"", b"\0", b"a", b"a\0", b"a\0\0"];
        for key in keys {
            writer.add(key, &big).unwrap();
        }
        let table = writer.finish().unwrap().unwrap();
        install_tables(&opt, db, 1, vec![table]).unwrap();

        let mut db = DB::open(db, opt).unwrap();
        for key in keys {
            assert_eq!(db.get(key).as_deref(), Some(&big[..]), "key {key:?}");
        }
        assert!(db.get(b"a\0\0\0").is_none());
    }

    #[test]
    fn test_bulk_single_empty_key() {
        // The index entry of the only block is the key itself.
        let opt = options::for_test();
        let db = Path::new("bulk");
        let mut writer = TableWriter::create(&opt, db, 1, 1).unwrap();
        writer.add(b"", b"only").unwrap();
        let table = writer.finish().unwrap().unwrap();
        install_tables(&opt, db, 1, vec![table]).unwrap();
        let mut db = DB::open(db, opt).unwrap();
        assert_eq!(db.get(b"").as_deref(), Some(&b"only"[..]));
    }

    #[test]
    fn test_bulk_failed_writer() {
        let mut opt = options::for_test();
        // No compressor is registered under this id, so the first block fails to write.
        opt.compressor = 42;
        let db = Path::new("bulk");
        let big = vec![7u8; opt.block_size * 2];
        let mut writer = TableWriter::create(&opt, db, 1, 1).unwrap();
        writer.add(b"a", &big).unwrap();
        assert!(writer.add(b"b", &big).is_err());
        assert!(writer.add(b"c", &big).is_err());
        assert!(writer.finish().is_err());
        assert!(!opt.env.exists(&table_file_name(db, 1)).unwrap());
    }

    #[test]
    fn test_bulk_number_limits() {
        let opt = options::for_test();
        let db = Path::new("bulk");
        assert!(TableWriter::create(&opt, db, FileNum::MAX / 2 + 1, 1).is_err());
        assert!(TableWriter::create(&opt, db, 1, MAX_SEQUENCE_NUMBER).is_err());
        let mut writer = TableWriter::create(&opt, db, 1, MAX_SEQUENCE_NUMBER - 1).unwrap();
        writer.add(b"a", b"v").unwrap();
        assert_eq!(
            writer.add(b"b", b"v").unwrap_err().code,
            StatusCode::InvalidArgument
        );
        assert_eq!(
            writer.finish().unwrap().unwrap().last_seq(),
            MAX_SEQUENCE_NUMBER - 1
        );
    }

    #[cfg(feature = "fs")]
    #[test]
    fn test_bulk_on_disk_reopen() {
        let dir = std::env::temp_dir().join(format!("rusty-leveldb-bulk-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        let opt = Options::default();
        let tables = build_tables(&opt, &dir, 900, 200);
        let empty = TableWriter::create(&opt, &dir, 99, 1).unwrap();
        assert!(empty.finish().unwrap().is_none());
        assert!(!dir.join("000099.ldb").exists());
        install_tables(&opt, &dir, 2, tables).unwrap();
        let mut names: Vec<String> = std::fs::read_dir(&dir)
            .unwrap()
            .map(|e| e.unwrap().file_name().into_string().unwrap())
            .collect();
        names.sort();
        assert_eq!(
            names,
            [
                "000001.ldb",
                "000002.ldb",
                "000003.ldb",
                "000004.ldb",
                "000005.ldb",
                "CURRENT",
                "MANIFEST-000006"
            ]
        );

        for reuse_manifest in [true, false] {
            let opt = Options {
                reuse_manifest,
                ..Options::default()
            };
            let mut db = DB::open(&dir, opt).unwrap();
            assert_eq!(db.get(&key(450)).as_deref(), Some(&value(450)[..]));
            db.put(&key(3), b"later").unwrap();
            db.put(b"zzz", b"new").unwrap();
            db.close().unwrap();
        }
        let mut db = DB::open(&dir, Options::default()).unwrap();
        assert_eq!(db.get(&key(3)).as_deref(), Some(&b"later"[..]));
        assert_eq!(db.get(b"zzz").as_deref(), Some(&b"new"[..]));
        assert_eq!(db.get(&key(899)).as_deref(), Some(&value(899)[..]));
        drop(db);
        std::fs::remove_dir_all(&dir).unwrap();
    }

    #[test]
    fn test_bulk_level_for_size() {
        const MIB: u64 = 1 << 20;
        assert_eq!(level_for_size(0), 1);
        assert_eq!(level_for_size(10 * MIB - 1), 1);
        // A level that is exactly full would be compacted on the first open.
        assert_eq!(level_for_size(10 * MIB), 2);
        assert_eq!(level_for_size(100 * MIB - 1), 2);
        assert_eq!(level_for_size(100 * MIB), 3);
        assert_eq!(level_for_size(10_000 * MIB), 5);
        assert_eq!(level_for_size(u64::MAX), NUM_LEVELS - 1);
    }
}
