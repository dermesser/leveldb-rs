
use cache::{self, Cache};
use env::RandomAccess;
use error::Result;
use options::Options;
use table_reader::Table;

use integer_encoding::FixedIntWriter;

use std::path::Path;
use std::sync::Arc;

const DEFAULT_SUFFIX: &str = "ldb";

fn table_name(name: &str, num: usize, suff: &str) -> String {
    assert!(num > 0);
    format!("{}/{:06}.{}", name, num, suff)
}

fn filenum_to_key(num: usize) -> cache::CacheKey {
    let mut buf = Vec::new();
    buf.write_fixedint(num).unwrap();
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
    pub fn evict(&mut self, id: usize) {
        self.cache.remove(&filenum_to_key(id));
    }
    /// Return a table from cache, or open the backing file, then cache and return it.
    pub fn get_table(&mut self, file_num: usize, file_size: usize) -> Result<Table> {
        let key = filenum_to_key(file_num);
        match self.cache.get(&key) {
            Some(t) => return Ok(t.table.clone()),
            _ => {}
        }
        self.open_table(file_num, file_size)
    }

    fn open_table(&mut self, file_num: usize, file_size: usize) -> Result<Table> {
        let name = table_name(&self.dbname, file_num, DEFAULT_SUFFIX);
        let path = Path::new(&name);
        let file = self.opts.env.open_random_access_file(&path)?;
        let rc_file = Arc::new(file);
        // No SSTable file name compatibility.
        let table = Table::new(self.opts.clone(), rc_file.clone(), file_size)?;
        self.cache.insert(&filenum_to_key(file_num),
                          TableAndFile {
                              file: rc_file.clone(),
                              table: table.clone(),
                          });
        Ok(table)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_table_name() {
        assert_eq!("abc/000122.ldb", table_name("abc", 122, "ldb"));
    }

    // TODO: Write tests after memenv has been implemented.
}
