
use integer_encoding::FixedInt;

/// Encapsulates a filter algorithm allowing to search for keys more efficiently.
pub trait FilterPolicy {
    fn name(&self) -> &'static str;
    fn create_filter(&self, keys: &Vec<&[u8]>) -> Vec<u8>;
    fn key_may_match(&self, key: &[u8], filter: &[u8]) -> bool;
}

const BLOOM_SEED: u32 = 0xbc9f1d34;

pub struct BloomPolicy {
    bits_per_key: u32,
    k: u32,
}

/// Beware the magic numbers...
impl BloomPolicy {
    pub fn new(bits_per_key: u32) -> BloomPolicy {
        let mut k = (bits_per_key as f32 * 0.69) as u32;

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

    fn bloom_hash(&self, data: &[u8]) -> u32 {
        let m: u32 = 0xc6a4a793;
        let r: u32 = 24;

        let mut ix = 0;
        let limit = data.len();

        let mut h: u32 = BLOOM_SEED ^ (limit as u64 * m as u64) as u32;

        while ix + 4 <= limit {
            let w = u32::decode_fixed(&data[ix..ix + 4]);
            ix += 4;

            h = (h as u64 + w as u64) as u32;
            h = (h as u64 * m as u64) as u32;
            h ^= h >> 16;
        }

        // Process left-over bytes
        assert!(limit - ix < 4);

        if limit - ix > 0 {
            let mut i = 0;

            for b in data[ix..].iter() {
                h += (*b as u32) << (8 * i);
                i += 1;
            }

            h = (h as u64 * m as u64) as u32;
            h ^= h >> r;
        }
        h
    }
}

impl FilterPolicy for BloomPolicy {
    fn name(&self) -> &'static str {
        "leveldb.BuiltinBloomFilter2"
    }
    fn create_filter(&self, keys: &Vec<&[u8]>) -> Vec<u8> {
        let filter_bits = keys.len() * self.bits_per_key as usize;
        let mut filter = Vec::new();

        if filter_bits < 64 {
            // Preallocate, then resize
            filter.reserve(8 + 1);
            filter.resize(8, 0 as u8);
        } else {
            // Preallocate, then resize
            filter.reserve(1 + ((filter_bits + 7) / 8));
            filter.resize((filter_bits + 7) / 8, 0);
        }

        let adj_filter_bits = (filter.len() * 8) as u32;

        // Encode k at the end of the filter.
        filter.push(self.k as u8);

        for key in keys {
            let mut h = self.bloom_hash(key);
            let delta = (h >> 17) | (h << 15);

            for _ in 0..self.k {
                let bitpos = (h % adj_filter_bits) as usize;
                filter[bitpos / 8] |= 1 << (bitpos % 8);
                h = (h as u64 + delta as u64) as u32;
            }
        }

        filter
    }
    fn key_may_match(&self, key: &[u8], filter: &[u8]) -> bool {
        let len = filter.len();

        if len < 2 {
            assert!(false);
            return false;
        }

        let bits = (len - 1) as u32 * 8;
        let k = filter[filter.len() - 1];
        let filter_adj = &filter[0..filter.len() - 1];

        if k > 30 {
            return true;
        }

        let mut h = self.bloom_hash(key);
        let delta = (h >> 17) | (h << 15);
        for _ in 0..k {
            let bitpos = (h % bits) as usize;
            if (filter_adj[bitpos / 8] & (1 << (bitpos % 8))) == 0 {
                return false;
            }
            h = (h as u64 + delta as u64) as u32;
        }
        true
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const _BITS_PER_KEY: u32 = 12;

    fn input_data() -> Vec<&'static [u8]> {
        let data = vec!["abc123def456".as_bytes(),
                        "xxx111xxx222".as_bytes(),
                        "ab00cd00ab".as_bytes(),
                        "908070605040302010".as_bytes()];
        data
    }
    fn create_filter() -> Vec<u8> {
        let fpol = BloomPolicy::new(_BITS_PER_KEY);
        let filter = fpol.create_filter(&input_data());

        assert_eq!(filter, vec![194, 148, 129, 140, 192, 196, 132, 164, 8]);
        filter
    }

    #[test]
    fn test_filter() {
        let f = create_filter();
        let fp = BloomPolicy::new(_BITS_PER_KEY);

        for k in input_data().iter() {
            assert!(fp.key_may_match(k, &f));
        }
    }

    #[test]
    fn hash_test() {
        let d1 = vec![0x62];
        let d2 = vec![0xc3, 0x97];
        let d3 = vec![0xe2, 0x99, 0xa5];
        let d4 = vec![0xe1, 0x80, 0xb9, 0x32];

        let fp = BloomPolicy::new(_BITS_PER_KEY);

        assert_eq!(fp.bloom_hash(&d1), 0xef1345c4);
        assert_eq!(fp.bloom_hash(&d2), 0x5b663814);
        assert_eq!(fp.bloom_hash(&d3), 0x323c078f);
        assert_eq!(fp.bloom_hash(&d4), 0xed21633a);
    }
}
