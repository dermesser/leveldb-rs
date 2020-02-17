use rusty_leveldb as leveldb;

use std::fs::OpenOptions;
use std::io::{self, BufRead};
use std::path::Path;

fn update_count(w: &str, db: &mut leveldb::DB) -> Option<()> {
    let mut count: usize = 0;
    if let Some(v) = db.get(w.as_bytes()) {
        let s = String::from_utf8(v).unwrap();
        count = usize::from_str_radix(&s, 10).unwrap();
    }
    count += 1;
    let s = count.to_string();
    db.put(w.as_bytes(), s.as_bytes()).unwrap();
    Some(())
}

fn run(mut db: leveldb::DB) -> io::Result<()> {
    let files = std::env::args().skip(1);

    for f in files {
        let f = OpenOptions::new().read(true).open(Path::new(&f))?;
        for line in io::BufReader::new(f).lines() {
            for word in line.unwrap().split_whitespace() {
                let mut word = word.to_ascii_lowercase();
                word.retain(|c| c.is_ascii_alphanumeric());
                update_count(&word, &mut db);
            }
        }
    }

    Ok(())
}

fn main() {
    let mut opts = leveldb::Options::default();
    opts.compression_type = leveldb::CompressionType::CompressionNone;
    let db = leveldb::DB::open("wordsdb", opts).unwrap();

    run(db).unwrap();
}
