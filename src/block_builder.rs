use std::cmp::Ordering;

use block::BlockContents;
use options::Options;

use integer_encoding::FixedInt;
use integer_encoding::VarInt;

/// BlockBuilder contains functionality for building a block consisting of consecutive key-value
/// entries.
pub struct BlockBuilder {
    opt: Options,
    buffer: Vec<u8>,
    restarts: Vec<u32>,

    last_key: Vec<u8>,
    counter: usize,
}

impl BlockBuilder {
    pub fn new(o: Options) -> BlockBuilder {
        let mut restarts = vec![0];
        restarts.reserve(1023);

        BlockBuilder {
            buffer: Vec::with_capacity(o.block_size),
            opt: o,
            restarts: restarts,
            last_key: Vec::new(),
            counter: 0,
        }
    }

    pub fn entries(&self) -> usize {
        self.counter
    }

    pub fn last_key<'a>(&'a self) -> &'a [u8] {
        &self.last_key
    }

    pub fn size_estimate(&self) -> usize {
        self.buffer.len() + 4 * self.restarts.len() + 4
    }

    pub fn reset(&mut self) {
        self.buffer.clear();
        self.restarts.clear();
        self.last_key.clear();
        self.counter = 0;
    }

    pub fn add(&mut self, key: &[u8], val: &[u8]) {
        assert!(self.counter <= self.opt.block_restart_interval);
        assert!(self.buffer.is_empty() ||
                self.opt.cmp.cmp(self.last_key.as_slice(), key) == Ordering::Less);

        let mut shared = 0;

        if self.counter < self.opt.block_restart_interval {
            let smallest = if self.last_key.len() < key.len() {
                self.last_key.len()
            } else {
                key.len()
            };

            while shared < smallest && self.last_key[shared] == key[shared] {
                shared += 1;
            }
        } else {
            self.restarts.push(self.buffer.len() as u32);
            self.last_key.resize(0, 0);
            self.counter = 0;
        }

        let non_shared = key.len() - shared;

        let mut buf = [0 as u8; 4];

        let mut sz = shared.encode_var(&mut buf[..]);
        self.buffer.extend_from_slice(&buf[0..sz]);
        sz = non_shared.encode_var(&mut buf[..]);
        self.buffer.extend_from_slice(&buf[0..sz]);
        sz = val.len().encode_var(&mut buf[..]);
        self.buffer.extend_from_slice(&buf[0..sz]);

        self.buffer.extend_from_slice(&key[shared..]);
        self.buffer.extend_from_slice(val);

        // Update key
        self.last_key.resize(shared, 0);
        self.last_key.extend_from_slice(&key[shared..]);

        self.counter += 1;
    }

    pub fn finish(mut self) -> BlockContents {
        // 1. Append RESTARTS
        let mut i = self.buffer.len();
        self.buffer.resize(i + self.restarts.len() * 4 + 4, 0);

        for r in self.restarts.iter() {
            r.encode_fixed(&mut self.buffer[i..i + 4]);
            i += 4;
        }

        // 2. Append N_RESTARTS
        (self.restarts.len() as u32).encode_fixed(&mut self.buffer[i..i + 4]);

        // done
        self.buffer
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use options;

    fn get_data() -> Vec<(&'static [u8], &'static [u8])> {
        vec![("key1".as_bytes(), "value1".as_bytes()),
             ("loooooooooooooooooooooooooooooooooongerkey1".as_bytes(), "shrtvl1".as_bytes()),
             ("medium length key 1".as_bytes(), "some value 2".as_bytes()),
             ("prefix_key1".as_bytes(), "value".as_bytes()),
             ("prefix_key2".as_bytes(), "value".as_bytes()),
             ("prefix_key3".as_bytes(), "value".as_bytes())]
    }

    #[test]
    fn test_block_builder_sanity() {
        let mut o = options::for_test();
        o.block_restart_interval = 3;

        let mut builder = BlockBuilder::new(o);

        for &(k, v) in get_data().iter() {
            builder.add(k, v);
            assert!(builder.counter <= 3);
            assert_eq!(builder.last_key(), k);
        }

        assert_eq!(149, builder.size_estimate());
        let block = builder.finish();
        assert_eq!(block.len(), 149);
    }

    #[test]
    #[should_panic]
    fn test_block_builder_panics() {
        let mut d = get_data();
        // Identical key as d[3].
        d[4].0 = b"prefix_key1";

        let mut builder = BlockBuilder::new(options::for_test());
        for &(k, v) in d.iter() {
            builder.add(k, v);
            assert_eq!(k, builder.last_key());
        }
        assert_eq!(d.len(), builder.entries());
    }

    // Additional test coverage is provided by tests in block.rs.
}
