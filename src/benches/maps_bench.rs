//! Compare different implementations of bytestring->bytestring maps. This is built as separate
//! binary.

#[macro_use]
extern crate bencher;
extern crate rand;
extern crate rusty_leveldb;

use bencher::Bencher;
use rand::Rng;

use rusty_leveldb::DefaultCmp;
use rusty_leveldb::SkipMap;

use std::collections::HashMap;
use std::rc::Rc;

fn gen_key_val<I: Iterator<Item = char>>(
    gen: &mut I,
    keylen: usize,
    vallen: usize,
) -> (Vec<u8>, Vec<u8>) {
    let mut key = Vec::with_capacity(keylen);
    let mut val = Vec::with_capacity(vallen);

    let mut k = 0;
    let mut v = 0;
    for c in gen {
        if k < keylen {
            key.push(c as u8);
            k += 1;
            continue;
        }

        if v < vallen {
            val.push(c as u8);
            v += 1;
            continue;
        }
        break;
    }
    (key, val)
}

fn bench_gen_key_val(b: &mut Bencher) {
    let mut gen = rand::thread_rng();
    let mut gen = gen.gen_ascii_chars();
    b.iter(|| {
        let (k, v) = gen_key_val(&mut gen, 10, 10);
        k.len();
    });
}

fn bench_skipmap_insert(b: &mut Bencher) {
    let mut gen = rand::thread_rng();
    let mut gen = gen.gen_ascii_chars();

    let mut skm = SkipMap::new(Rc::new(Box::new(DefaultCmp)));

    b.iter(|| {
        let (k, v) = gen_key_val(&mut gen, 10, 10);
        skm.insert(k, v);
    });
}

fn bench_hashmap_insert(b: &mut Bencher) {
    let mut gen = rand::thread_rng();
    let mut gen = gen.gen_ascii_chars();
    let mut hm = HashMap::new();

    b.iter(|| {
        let (k, v) = gen_key_val(&mut gen, 10, 10);
        hm.insert(k, v);
    });
}

benchmark_group!(
    basic,
    bench_gen_key_val,
    bench_skipmap_insert,
    bench_hashmap_insert
);
benchmark_main!(basic);
