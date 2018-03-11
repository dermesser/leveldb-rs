extern crate rusty_leveldb;

use rusty_leveldb::{LdbIterator, Options, DB};

use std::env::args;
use std::io::{self, Write};
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

fn iter(db: &mut DB) {
    let mut it = db.new_iter().unwrap();
    let (mut k, mut v) = (vec![], vec![]);
    let mut out = io::BufWriter::new(io::stdout());
    while it.advance() {
        it.current(&mut k, &mut v);
        out.write_all(&k).unwrap();
        out.write_all(b" => ").unwrap();
        out.write_all(&v).unwrap();
        out.write_all(b"\n").unwrap();
    }
}

fn compact(db: &mut DB, from: &str, to: &str) {
    db.compact_range(from.as_bytes(), to.as_bytes()).unwrap();
}

fn main() {
    let args = Vec::from_iter(args());

    if args.len() < 2 {
        panic!(
            "Usage: {} [get|put|delete|iter|compact] [key|from] [val|to]",
            args[0]
        );
    }

    let mut opt = Options::default();
    opt.reuse_logs = false;
    opt.reuse_manifest = false;
    let mut db = DB::open("tooldb", opt).unwrap();

    match args[1].as_str() {
        "get" => {
            if args.len() < 3 {
                panic!("Usage: {} get key", args[0]);
            }
            get(&mut db, &args[2]);
        }
        "put" => {
            if args.len() < 4 {
                panic!("Usage: {} put key val", args[0]);
            }
            put(&mut db, &args[2], &args[3]);
        }
        "delete" => {
            if args.len() < 3 {
                panic!("Usage: {} delete key", args[0]);
            }
            delete(&mut db, &args[2]);
        }
        "iter" => iter(&mut db),
        "compact" => {
            if args.len() < 4 {
                panic!("Usage: {} compact from to", args[0]);
            }
            compact(&mut db, &args[2], &args[3]);
        }
        _ => unimplemented!(),
    }
}
