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

use std::collections::BTreeMap;
use std::collections::HashMap;
use std::rc::Rc;

fn gen_key_val<R: Rng>(gen: &mut R, keylen: usize, vallen: usize) -> (Vec<u8>, Vec<u8>) {
    let mut key = Vec::with_capacity(keylen);
    let mut val = Vec::with_capacity(vallen);

    for _i in 0..keylen {
        key.push(gen.gen_range(b'a', b'z'));
    }
    for _i in 0..vallen {
        val.push(gen.gen_range(b'a', b'z'));
    }
    (key, val)
}

fn bench_gen_key_val(b: &mut Bencher) {
    let mut gen = rand::thread_rng();
    b.iter(|| {
        let (k, _v) = gen_key_val(&mut gen, 10, 10);
        k.len();
    });
}

fn bench_skipmap_insert(b: &mut Bencher) {
    let mut gen = rand::thread_rng();

    let mut skm = SkipMap::new(Rc::new(Box::new(DefaultCmp)));

    b.iter(|| {
        let (mut k, v) = gen_key_val(&mut gen, 10, 10);
        skm.insert(k.clone(), v.clone());
        k[9] += 1;
        skm.insert(k.clone(), v.clone());
        k[9] += 1;
        skm.insert(k.clone(), v.clone());
        k[9] += 1;
        skm.insert(k.clone(), v.clone());
        k[9] += 1;
        skm.insert(k.clone(), v.clone());
        k[9] += 1;
        skm.insert(k.clone(), v.clone());
        k[9] += 1;
        skm.insert(k.clone(), v.clone());
        k[9] += 1;
        skm.insert(k.clone(), v.clone());
        k[9] += 1;
        skm.insert(k.clone(), v.clone());
        k[9] += 1;
        skm.insert(k, v);
    });
}

fn bench_hashmap_insert(b: &mut Bencher) {
    let mut gen = rand::thread_rng();
    let mut hm = HashMap::new();

    b.iter(|| {
        let (mut k, v) = gen_key_val(&mut gen, 10, 10);
        hm.insert(k.clone(), v.clone());
        k[9] += 1;
        hm.insert(k.clone(), v.clone());
        k[9] += 1;
        hm.insert(k.clone(), v.clone());
        k[9] += 1;
        hm.insert(k.clone(), v.clone());
        k[9] += 1;
        hm.insert(k.clone(), v.clone());
        k[9] += 1;
        hm.insert(k.clone(), v.clone());
        k[9] += 1;
        hm.insert(k.clone(), v.clone());
        k[9] += 1;
        hm.insert(k.clone(), v.clone());
        k[9] += 1;
        hm.insert(k.clone(), v.clone());
        k[9] += 1;
        hm.insert(k, v);
    });
}

fn bench_btree_insert(b: &mut Bencher) {
    let mut gen = rand::thread_rng();
    let mut btm = BTreeMap::new();

    b.iter(|| {
        let (mut k, v) = gen_key_val(&mut gen, 10, 10);
        btm.insert(k.clone(), v.clone());
        k[9] += 1;
        btm.insert(k.clone(), v.clone());
        k[9] += 1;
        btm.insert(k.clone(), v.clone());
        k[9] += 1;
        btm.insert(k.clone(), v.clone());
        k[9] += 1;
        btm.insert(k.clone(), v.clone());
        k[9] += 1;
        btm.insert(k.clone(), v.clone());
        k[9] += 1;
        btm.insert(k.clone(), v.clone());
        k[9] += 1;
        btm.insert(k.clone(), v.clone());
        k[9] += 1;
        btm.insert(k.clone(), v.clone());
        k[9] += 1;
        btm.insert(k, v);
    });
}

benchmark_group!(
    basic,
    bench_gen_key_val,
    bench_skipmap_insert,
    bench_hashmap_insert,
    bench_btree_insert,
);
benchmark_main!(basic);
