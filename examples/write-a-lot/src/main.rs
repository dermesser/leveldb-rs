extern crate rusty_leveldb;
extern crate rand;

use rusty_leveldb::CompressionType;
use rusty_leveldb::DB;
use rusty_leveldb::Options;
use rand::Rng;

use std::error::Error;
use std::iter::FromIterator;

const KEY_LEN: usize = 16;
const VAL_LEN: usize = 48;

fn gen_string(len: usize) -> String {
    let mut rng = rand::thread_rng();
    String::from_iter(rng.gen_ascii_chars().take(len))
}

fn fill_db(db: &mut DB, entries: usize) -> Result<(), Box<Error>> {
    for i in 0..entries {
        let (k, v) = (gen_string(KEY_LEN), gen_string(VAL_LEN));
        db.put(k.as_bytes(), v.as_bytes())?;

        if i % 100 == 0 {
            db.flush()?;
        }
    }
    Ok(())
}

fn main() {
    let mut opt = Options::default();
    opt.compression_type = CompressionType::CompressionSnappy;
    let mut db = DB::open("test1", opt).unwrap();

    fill_db(&mut db, 32768).unwrap();
}
