
/// Encapsulates a filter algorithm allowing to search for keys more efficiently.
pub trait FilterPolicy {
    fn name(&self) -> &'static str;
    fn create_filter(&self, keys: &Vec<&[u8]>) -> Vec<u8>;
    fn key_may_match(&self, key: &[u8], filter: &[u8]) -> bool;
}

pub struct BloomPolicy {
    bits_per_key: usize,
    k: usize,
}

impl BloomPolicy {
    pub fn new(bits_per_key: usize) -> BloomPolicy {
        let mut k = (bits_per_key as f32 * 0.69) as usize;

        if k < 1 {
            k = 1;
        } else if k > 30 {
            k = 30;
        }

        BloomPolicy {
            bits_per_key: bits_per_key,
            k: k,
        }
    }
}

impl FilterPolicy for BloomPolicy {
    fn name(&self) -> &'static str {
        "leveldb.BuiltinBloomFilter2"
    }
    fn create_filter(&self, keys: &Vec<&[u8]>) -> Vec<u8> {
        let filter_size = keys.len() * self.bits_per_key;
        let mut filter = Vec::new();

        if filter_size < 64 {
            filter.resize(8, 0 as u8);
        } else {
            filter.resize((filter_size + 7) / 8, 0);
        }

        filter
    }
    fn key_may_match(&self, key: &[u8], filter: &[u8]) -> bool {
        true
    }
}
