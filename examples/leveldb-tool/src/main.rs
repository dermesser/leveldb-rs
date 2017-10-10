extern crate leveldb_rs;

use leveldb_rs::{DB, LdbIterator, Options};

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

fn iter(db: &mut DB) {
    let mut it = db.new_iter().unwrap();
    while let Some((k, v)) = it.next() {
        match (String::from_utf8(k), String::from_utf8(v)) {
            (Ok(sk), Ok(sv)) => println!("{} => {}", sk, sv),
            (Err(utf8e), Ok(sv)) => println!("{:?} => {}", utf8e.into_bytes(), sv),
            (Ok(sk), Err(utf8e)) => println!("{} => {:?}", sk, utf8e.into_bytes()),
            (Err(utf81), Err(utf82)) => {
                println!("{:?} => {:?}", utf81.into_bytes(), utf82.into_bytes())
            }
        }
    }
}

fn compact(db: &mut DB, from: &str, to: &str) {
    db.compact_range(from.as_bytes(), to.as_bytes()).unwrap();
}

fn main() {
    let args = Vec::from_iter(args());

    if args.len() < 2 {
        panic!("Usage: {} [get|put|delete|iter|compact] [key|from] [val|to]",
               args[0]);
    }

    let mut opt = Options::default();
    opt.reuse_logs = true;
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
