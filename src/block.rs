use std::cmp::Ordering;

use std::rc::Rc;

use options::Options;
use types::LdbIterator;
use types::Comparator;

use integer_encoding::FixedInt;
use integer_encoding::VarInt;

pub type BlockContents = Vec<u8>;

/// A block is a list of ENTRIES followed by a list of RESTARTS, terminated by a fixed u32
/// N_RESTARTS.
///
/// An ENTRY consists of three varints, SHARED, NON_SHARED, VALSIZE, a KEY and a VALUE.
///
/// SHARED denotes how many bytes the entry's key shares with the previous one.
///
/// NON_SHARED is the size of the key minus SHARED.
///
/// VALSIZE is the size of the value.
///
/// KEY and VALUE are byte strings; the length of KEY is NON_SHARED.
///
/// A RESTART is a fixed u32 pointing to the beginning of an ENTRY.
///
/// N_RESTARTS contains the number of restarts.
pub struct BlockIter<C: Comparator> {
    block: Rc<BlockContents>,
    cmp: C,
    // start of next entry
    offset: usize,
    // offset of restarts area
    restarts_off: usize,
    // start of current entry
    current_entry_offset: usize,
    // tracks the last restart we encountered
    current_restart_ix: usize,

    // We assemble the key from two parts usually, so we keep the current full key here.
    key: Vec<u8>,
    val_offset: usize,
}

impl<C: Comparator> Clone for BlockIter<C> {
    fn clone(&self) -> BlockIter<C> {
        BlockIter {
            block: self.block.clone(),
            cmp: self.cmp,
            offset: self.offset,
            restarts_off: self.restarts_off,
            current_entry_offset: self.current_entry_offset,
            current_restart_ix: self.current_restart_ix,

            key: Vec::new(),
            val_offset: 0,
        }
    }
}

impl<C: Comparator> BlockIter<C> {
    pub fn new(contents: BlockContents, cmp: C) -> BlockIter<C> {
        assert!(contents.len() > 4);
        let restarts = u32::decode_fixed(&contents[contents.len() - 4..]);
        let restart_offset = contents.len() - 4 - 4 * restarts as usize;

        BlockIter {
            block: Rc::new(contents),
            restarts_off: restart_offset,
            cmp: cmp,
            current_restart_ix: 0,
            offset: 0,
            key: Vec::new(),
            val_offset: 0,
            current_entry_offset: 0,
        }
    }

    pub fn obtain(self) -> Rc<Vec<u8>> {
        self.block.clone()
    }

    fn number_restarts(&self) -> usize {
        ((self.block.len() - self.restarts_off) / 4) - 1
    }

    fn get_restart_point(&self, ix: usize) -> usize {
        let restart = self.restarts_off + 4 * ix;
        u32::decode_fixed(&self.block[restart..restart + 4]) as usize
    }
}

impl<C: Comparator> BlockIter<C> {
    // Returns SHARED, NON_SHARED and VALSIZE from the current position. Advances self.offset.
    fn parse_entry(&mut self) -> (usize, usize, usize) {
        let mut i = 0;
        let (shared, sharedlen) = usize::decode_var(&self.block[self.offset..]);
        i += sharedlen;

        let (non_shared, non_sharedlen) = usize::decode_var(&self.block[self.offset + i..]);
        i += non_sharedlen;

        let (valsize, valsizelen) = usize::decode_var(&self.block[self.offset + i..]);
        i += valsizelen;

        self.offset += i;

        (shared, non_shared, valsize)
    }

    /// offset is assumed to be at the beginning of the non-shared key part.
    /// offset is not advanced.
    fn assemble_key(&mut self, shared: usize, non_shared: usize) {
        self.key.resize(shared, 0);
        self.key.extend_from_slice(&self.block[self.offset..self.offset + non_shared]);
    }
}

impl<C: Comparator> Iterator for BlockIter<C> {
    type Item = (Vec<u8>, Vec<u8>);

    fn next(&mut self) -> Option<Self::Item> {
        self.current_entry_offset = self.offset;

        if self.current_entry_offset >= self.restarts_off {
            return None;
        }
        let (shared, non_shared, valsize) = self.parse_entry();
        self.assemble_key(shared, non_shared);

        self.val_offset = self.offset + non_shared;
        self.offset = self.val_offset + valsize;

        let num_restarts = self.number_restarts();
        while self.current_restart_ix + 1 < num_restarts &&
              self.get_restart_point(self.current_restart_ix + 1) < self.current_entry_offset {
            self.current_restart_ix += 1;
        }
        Some((self.key.clone(), Vec::from(&self.block[self.val_offset..self.val_offset + valsize])))
    }
}

impl<C: Comparator> LdbIterator for BlockIter<C> {
    fn reset(&mut self) {
        self.offset = 0;
        self.current_restart_ix = 0;
        self.key.clear();
        self.val_offset = 0;
    }
    fn prev(&mut self) -> Option<Self::Item> {
        // as in the original implementation -- seek to last restart point, then look for key
        let current_offset = self.current_entry_offset;

        // At the beginning, can't go further back
        if current_offset == 0 {
            self.reset();
            return None;
        }

        while self.get_restart_point(self.current_restart_ix) >= current_offset {
            self.current_restart_ix -= 1;
        }

        self.offset = self.get_restart_point(self.current_restart_ix);
        assert!(self.offset < current_offset);

        let mut result;

        loop {
            result = self.next();

            if self.offset >= current_offset {
                break;
            }
        }
        result

    }
    fn seek(&mut self, to: &[u8]) {
        self.reset();

        let mut left = 0;
        let mut right = self.number_restarts() - 1;

        // Do a binary search over the restart points.
        while left < right {
            let middle = (left + right + 1) / 2;
            self.offset = self.get_restart_point(middle);
            // advances self.offset
            let (shared, non_shared, _) = self.parse_entry();

            // At a restart, the shared part is supposed to be 0.
            assert_eq!(shared, 0);

            let cmp = self.cmp.cmp(to, &self.block[self.offset..self.offset + non_shared]);

            if cmp == Ordering::Less {
                right = middle - 1;
            } else {
                left = middle;
            }
        }

        assert_eq!(left, right);
        self.current_restart_ix = left;
        self.offset = self.get_restart_point(left);

        // Linear search from here on
        while let Some((k, _)) = self.next() {
            if self.cmp.cmp(k.as_slice(), to) >= Ordering::Equal {
                return;
            }
        }
    }

    fn valid(&self) -> bool {
        !self.key.is_empty() && self.val_offset > 0 && self.val_offset < self.restarts_off
    }

    fn current(&self) -> Option<Self::Item> {
        if self.valid() {
            Some((self.key.clone(), Vec::from(&self.block[self.val_offset..self.offset])))
        } else {
            None
        }
    }
}

pub struct BlockBuilder<C: Comparator> {
    opt: Options,
    cmp: C,
    buffer: Vec<u8>,
    restarts: Vec<u32>,

    last_key: Vec<u8>,
    counter: usize,
}

impl<C: Comparator> BlockBuilder<C> {
    pub fn new(o: Options, cmp: C) -> BlockBuilder<C> {
        let mut restarts = vec![0];
        restarts.reserve(1023);

        BlockBuilder {
            buffer: Vec::with_capacity(o.block_size),
            opt: o,
            cmp: cmp,
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
        self.buffer.len() + self.restarts.len() * 4 + 4
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
                self.cmp.cmp(self.last_key.as_slice(), key) == Ordering::Less);

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
        sz = val.len().encode_var(&mut buf[0..sz]);
        self.buffer.extend_from_slice(&buf[0..sz]);

        self.buffer.extend_from_slice(&key[shared..]);
        self.buffer.extend_from_slice(val);

        // Update key
        self.last_key.resize(shared, 0);
        self.last_key.extend_from_slice(&key[shared..]);

        // assert_eq!(&self.last_key[..], key);

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
    use types::*;
    use options::*;

    fn get_data() -> Vec<(&'static [u8], &'static [u8])> {
        vec![("key1".as_bytes(), "value1".as_bytes()),
             ("loooooooooooooooooooooooooooooooooongerkey1".as_bytes(), "shrtvl1".as_bytes()),
             ("medium length key 1".as_bytes(), "some value 2".as_bytes()),
             ("prefix_key1".as_bytes(), "value".as_bytes()),
             ("prefix_key2".as_bytes(), "value".as_bytes()),
             ("prefix_key3".as_bytes(), "value".as_bytes())]
    }

    #[test]
    fn test_block_builder() {
        let mut o = Options::default();
        o.block_restart_interval = 3;

        let mut builder = BlockBuilder::new(o, StandardComparator);

        for &(k, v) in get_data().iter() {
            builder.add(k, v);
            assert!(builder.counter <= 3);
            assert_eq!(builder.last_key(), k);
        }

        let block = builder.finish();
        assert_eq!(block.len(), 149);
    }

    #[test]
    fn test_block_empty() {
        let mut o = Options::default();
        o.block_restart_interval = 16;
        let builder = BlockBuilder::new(o, StandardComparator);

        let blockc = builder.finish();
        assert_eq!(blockc.len(), 8);
        assert_eq!(blockc, vec![0, 0, 0, 0, 1, 0, 0, 0]);

        let block = BlockIter::new(blockc, StandardComparator);

        for _ in block {
            panic!("expected 0 iterations");
        }
    }

    #[test]
    fn test_block_build_iterate() {
        let data = get_data();
        let mut builder = BlockBuilder::new(Options::default(), StandardComparator);

        for &(k, v) in data.iter() {
            builder.add(k, v);
        }

        let block_contents = builder.finish();
        let block = BlockIter::new(block_contents, StandardComparator);
        let mut i = 0;

        assert!(!block.valid());

        for (k, v) in block {
            assert_eq!(&k[..], data[i].0);
            assert_eq!(v, data[i].1);
            i += 1;
        }
        assert_eq!(i, data.len());
    }

    #[test]
    fn test_block_iterate_reverse() {
        let mut o = Options::default();
        o.block_restart_interval = 3;
        let data = get_data();
        let mut builder = BlockBuilder::new(o, StandardComparator);

        for &(k, v) in data.iter() {
            builder.add(k, v);
        }

        let block_contents = builder.finish();
        let mut block = BlockIter::new(block_contents, StandardComparator);

        assert!(!block.valid());
        assert_eq!(block.next(),
                   Some(("key1".as_bytes().to_vec(), "value1".as_bytes().to_vec())));
        assert!(block.valid());
        block.next();
        assert!(block.valid());
        block.prev();
        assert!(block.valid());
        assert_eq!(block.current(),
                   Some(("key1".as_bytes().to_vec(), "value1".as_bytes().to_vec())));
        block.prev();
        assert!(!block.valid());
    }

    #[test]
    fn test_block_seek() {
        let mut o = Options::default();
        o.block_restart_interval = 3;

        let data = get_data();
        let mut builder = BlockBuilder::new(o, StandardComparator);

        for &(k, v) in data.iter() {
            builder.add(k, v);
        }

        let block_contents = builder.finish();

        let mut block = BlockIter::new(block_contents, StandardComparator);

        block.seek(&"prefix_key2".as_bytes());
        assert!(block.valid());
        assert_eq!(block.current(),
                   Some(("prefix_key2".as_bytes().to_vec(), "value".as_bytes().to_vec())));

        block.seek(&"key1".as_bytes());
        assert!(block.valid());
        assert_eq!(block.current(),
                   Some(("key1".as_bytes().to_vec(), "value1".as_bytes().to_vec())));
    }
}
