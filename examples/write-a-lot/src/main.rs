extern crate rand;
extern crate rusty_leveldb;

use rand::Rng;
use rusty_leveldb::{compressor, CompressorId, Options, DB};

use std::error::Error;
use std::iter::FromIterator;

const KEY_LEN: usize = 16;
const VAL_LEN: usize = 48;

fn gen_string(len: usize) -> String {
    let mut rng = rand::thread_rng();
    String::from_iter(rng.gen_ascii_chars().take(len))
}

fn fill_db(db: &mut DB, entries: usize) -> Result<(), Box<dyn Error>> {
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
    let opt = Options {
        compressor: compressor::SnappyCompressor::ID,
        ..Default::default()
    };
    let mut db = DB::open("test1", opt).unwrap();

    fill_db(&mut db, 32768).unwrap();

    db.close().unwrap();
}
