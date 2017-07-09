
use cache::Cache;
use env::RandomAccessFile;
use options::Options;
use table_reader::Table;

const DEFAULT_SUFFIX: &str = "ldb";

fn table_name(name: &str, num: usize, suff: &str) -> String {
    assert!(num > 0);
    format!("{}/{:06}.{}", name, num, suff)
}

struct TableAndFile {
    file: Box<RandomAccessFile>,
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
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_table_name() {
        assert_eq!("abc/000122.ldb", table_name("abc", 122, "ldb"));
    }
}
