extern crate leveldb_rs;

use leveldb_rs::{DB, Options};

use std::env::args;
use std::iter::FromIterator;

fn get(db: &mut DB, k: &str) {
    match db.get(k.as_bytes()) {
        Some(v) => println!("{} => {}", k, String::from_utf8(v).unwrap()),
        None => println!("{} => <not found>", k),
    }
}

fn put(db: &mut DB, k: &str, v: &str) {
    db.put(k.as_bytes(), v.as_bytes()).unwrap();
    db.flush().unwrap();
}

fn delete(db: &mut DB, k: &str) {
    db.delete(k.as_bytes()).unwrap();
    db.flush().unwrap();
}

fn main() {
    let args = Vec::from_iter(args());

    if args.len() < 3 {
        panic!("Usage: {} [get|put|delete] key [val]", args[0]);
    }

    let mut opt = Options::default();
    opt.reuse_logs = true;
    let mut db = DB::open("tooldb", opt).unwrap();

    match args[1].as_str() {
        "get" => get(&mut db, &args[2]),
        "put" => {
            if args.len() < 4 {
                panic!("Usage: {} put key val", args[0]);
            }
            put(&mut db, &args[2], &args[3]);
        }
        "delete" => delete(&mut db, &args[2]),
        _ => unimplemented!(),
    }
}
