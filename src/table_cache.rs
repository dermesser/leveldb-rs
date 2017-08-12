
use cache::{self, Cache};
use env::RandomAccess;
use error::Result;
use options::Options;
use table_reader::Table;

use integer_encoding::FixedIntWriter;

use std::path::Path;
use std::sync::Arc;

const DEFAULT_SUFFIX: &str = "ldb";

fn table_name(name: &str, num: u64, suff: &str) -> String {
    assert!(num > 0);
    format!("{}/{:06}.{}", name, num, suff)
}

fn filenum_to_key(num: u64) -> cache::CacheKey {
    let mut buf = [0; 16];
    (&mut buf[..]).write_fixedint(num).unwrap();
    buf
}

struct TableAndFile {
    file: Arc<Box<RandomAccess>>,
    table: Table,
}

pub struct TableCache {
    dbname: String,
    cache: Cache<TableAndFile>,
    opts: Options,
}

impl TableCache {
    pub fn new(db: &str, opt: Options, entries: usize) -> TableCache {
        TableCache {
            dbname: String::from(db),
            cache: Cache::new(entries),
            opts: opt,
        }
    }
    pub fn evict(&mut self, id: u64) {
        self.cache.remove(&filenum_to_key(id));
    }

    /// Return a table from cache, or open the backing file, then cache and return it.
    pub fn get_table(&mut self, file_num: u64, file_size: usize) -> Result<Table> {
        let key = filenum_to_key(file_num);
        match self.cache.get(&key) {
            Some(t) => return Ok(t.table.clone()),
            _ => {}
        }
        self.open_table(file_num, file_size)
    }

    /// Open a table on the file system and read it.
    fn open_table(&mut self, file_num: u64, file_size: usize) -> Result<Table> {
        let name = table_name(&self.dbname, file_num, DEFAULT_SUFFIX);
        let path = Path::new(&name);
        let file = Arc::new(self.opts.env.open_random_access_file(&path)?);
        // No SSTable file name compatibility.
        let table = Table::new(self.opts.clone(), file.clone(), file_size)?;
        self.cache.insert(&filenum_to_key(file_num),
                          TableAndFile {
                              file: file.clone(),
                              table: table.clone(),
                          });
        Ok(table)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use cache;
    use table_builder::TableBuilder;

    use std::io::Write;

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

    fn write_table_to(w: Box<Write>) {
        let mut opt = Options::default();
        opt.block_restart_interval = 3;
        let mut b = TableBuilder::new_raw(opt, w);

        let data = vec![("abc", "def"), ("abd", "dee"), ("bcd", "asa"), ("bsr", "a00")];

        for &(k, v) in data.iter() {
            b.add(k.as_bytes(), v.as_bytes());
        }
        b.finish();
    }

    // TODO: Write tests after memenv has been implemented.
    #[test]
    fn test_table_cache() {}
}
