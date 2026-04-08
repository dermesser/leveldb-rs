#[macro_use]
extern crate bencher;
extern crate rusty_leveldb;

use bencher::Bencher;
use rusty_leveldb::cache::Cache;

fn bench_cache_insert_evict(b: &mut Bencher) {
    let mut cache = Cache::new(1000);
    let mut i = 0u64;
    b.iter(|| {
        let mut key = [0u8; 16];
        key[0..8].copy_from_slice(&i.to_le_bytes());
        cache.insert(&key, i);
        i += 1;
    });
}

fn bench_cache_get_hit(b: &mut Bencher) {
    let mut cache = Cache::new(1000);
    let mut key = [0u8; 16];
    key[0..8].copy_from_slice(&1u64.to_le_bytes());
    cache.insert(&key, 1);

    b.iter(|| {
        bencher::black_box(cache.get(&key));
    });
}

benchmark_group!(benches, bench_cache_insert_evict, bench_cache_get_hit,);
benchmark_main!(benches);
