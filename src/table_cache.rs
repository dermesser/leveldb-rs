//! table_cache implements a cache providing access to the immutable SSTables on disk. It's a
//! read-through cache, meaning that non-present tables are read from disk and cached before being
//! returned.

use cache::{self, Cache};
use error::{err, StatusCode, Result};
use key_types::InternalKey;
use options::Options;
use table_reader::Table;
use types::FileNum;

use integer_encoding::FixedIntWriter;

use std::path::Path;
use std::rc::Rc;

const DEFAULT_SUFFIX: &str = "ldb";

pub fn table_name(name: &str, num: FileNum, suff: &str) -> String {
    assert!(num > 0);
    format!("{}/{:06}.{}", name, num, suff)
}

fn filenum_to_key(num: FileNum) -> cache::CacheKey {
    let mut buf = [0; 16];
    (&mut buf[..]).write_fixedint(num).unwrap();
    buf
}

pub struct TableCache {
    dbname: String,
    cache: Cache<Table>,
    opts: Options,
}

impl TableCache {
    /// Create a new TableCache for the database named `db`, caching up to `entries` tables.
    ///
    /// opt.cmp should be the user-supplied comparator.
    pub fn new(db: &str, opt: Options, entries: usize) -> TableCache {
        TableCache {
            dbname: String::from(db),
            cache: Cache::new(entries),
            opts: opt,
        }
    }

    pub fn get<'a>(&mut self,
                   file_num: FileNum,
                   key: InternalKey<'a>)
                   -> Result<Option<(Vec<u8>, Vec<u8>)>> {
        let tbl = self.get_table(file_num)?;
        Ok(tbl.get(key))
    }

    /// Return a table from cache, or open the backing file, then cache and return it.
    pub fn get_table(&mut self, file_num: FileNum) -> Result<Table> {
        let key = filenum_to_key(file_num);
        if let Some(t) = self.cache.get(&key) {
            return Ok(t.clone());
        }
        self.open_table(file_num)
    }

    /// Open a table on the file system and read it.
    fn open_table(&mut self, file_num: FileNum) -> Result<Table> {
        let name = table_name(&self.dbname, file_num, DEFAULT_SUFFIX);
        let path = Path::new(&name);
        let file_size = self.opts.env.size_of(&path)?;
        if file_size == 0 {
            return err(StatusCode::InvalidData, "file is empty");
        }
        let file = Rc::new(self.opts.env.open_random_access_file(&path)?);
        // No SSTable file name compatibility.
        let table = Table::new(self.opts.clone(), file, file_size)?;
        self.cache.insert(&filenum_to_key(file_num), table.clone());
        Ok(table)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use cache;
    use mem_env::MemEnv;
    use options;
    use table_builder::TableBuilder;
    use test_util::LdbIteratorIter;

    #[test]
    fn test_table_name() {
        assert_eq!("abc/000122.ldb", table_name("abc", 122, "ldb"));
        assert_eq!("abc/1234567.ldb", table_name("abc", 1234567, "ldb"));
    }

    fn make_key(a: u8, b: u8, c: u8) -> cache::CacheKey {
        [a, b, c, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0]
    }

    #[test]
    fn test_filenum_to_key() {
        assert_eq!(make_key(16, 0, 0), filenum_to_key(0x10));
        assert_eq!(make_key(16, 1, 0), filenum_to_key(0x0110));
        assert_eq!(make_key(1, 2, 3), filenum_to_key(0x030201));
    }

    fn write_table_to(o: Options, p: &Path) {
        let w = o.env.open_writable_file(p).unwrap();
        let mut b = TableBuilder::new_raw(o, w);

        let data = vec![("abc", "def"), ("abd", "dee"), ("bcd", "asa"), ("bsr", "a00")];

        for &(k, v) in data.iter() {
            b.add(k.as_bytes(), v.as_bytes()).unwrap();
        }
        b.finish().unwrap();
    }

    #[test]
    fn test_table_cache() {
        // Tests that a table can be written to a MemFS file, read back by the table cache and
        // parsed/iterated by the table reader.
        let mut opt = options::for_test();
        opt.set_env(Box::new(MemEnv::new()));
        let dbname = "testdb1";
        let tablename = table_name(dbname, 123, DEFAULT_SUFFIX);
        let tblpath = Path::new(&tablename);

        write_table_to(opt.clone(), tblpath);
        assert!(opt.env.exists(tblpath).unwrap());
        assert!(opt.env.size_of(tblpath).unwrap() > 20);

        let mut cache = TableCache::new(dbname, opt.clone(), 10);
        assert_eq!(LdbIteratorIter::wrap(&mut cache.get_table(123).unwrap().iter()).count(),
                   4);
        // Test cached table.
        assert_eq!(LdbIteratorIter::wrap(&mut cache.get_table(123).unwrap().iter()).count(),
                   4);
    }
}
