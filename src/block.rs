#![allow(dead_code)]

use std::cmp::Ordering;

use types::LdbIterator;
use types::{Comparator, StandardComparator};

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
pub struct Block<C: Comparator> {
    data: BlockContents,
    restarts_off: usize,
    cmp: C,
}

impl Block<StandardComparator> {
    pub fn new(contents: BlockContents) -> Block<StandardComparator> {
        Self::new_with_cmp(contents, StandardComparator)
    }
}

impl<C: Comparator> Block<C> {
    pub fn new_with_cmp(contents: BlockContents, cmp: C) -> Block<C> {
        assert!(contents.len() > 4);
        let restarts = u32::decode_fixed(&contents[contents.len() - 4..]);
        let restart_offset = contents.len() - 4 - 4 * restarts as usize;

        Block {
            data: contents,
            restarts_off: restart_offset,
            cmp: cmp,
        }
    }

    fn number_restarts(&self) -> usize {
        ((self.data.len() - self.restarts_off) / 4) - 1
    }

    fn get_restart_point(&self, ix: usize) -> usize {
        let restart = self.restarts_off + 4 * ix;
        usize::decode_fixed(&self.data[restart..restart + 4])
    }

    pub fn iter<'a>(&'a self) -> BlockIter<'a, C> {
        BlockIter {
            block: self,
            current_restart_ix: 0,
            offset: 0,
            key: Vec::new(),
            val_offset: 0,
        }
    }
}


pub struct BlockIter<'a, C: 'a + Comparator> {
    block: &'a Block<C>,
    offset: usize,
    current_restart_ix: usize,

    // We assemble the key from two parts usually, so we keep the current full key here.
    key: Vec<u8>,
    val_offset: usize,
}

impl<'a, C: Comparator> BlockIter<'a, C> {
    // Returns SHARED, NON_SHARED and VALSIZE from the current position. Advances self.offset.
    fn parse_entry(&mut self) -> (usize, usize, usize) {
        let mut i = 0;
        let (shared, sharedlen) = usize::decode_var(&self.block.data[self.offset..]);
        i += sharedlen;

        let (non_shared, non_sharedlen) = usize::decode_var(&self.block.data[self.offset + i..]);
        i += non_sharedlen;

        let (valsize, valsizelen) = usize::decode_var(&self.block.data[self.offset + i..]);
        i += valsizelen;

        self.offset += i;

        (shared, non_shared, valsize)
    }

    /// offset is assumed to be at the beginning of the non-shared key part.
    /// offset is not advanced.
    fn assemble_key(&mut self, shared: usize, non_shared: usize) {
        self.key.resize(shared, 0);
        self.key.extend_from_slice(&self.block.data[self.offset..self.offset + non_shared]);
    }
}

impl<'a, C: Comparator> Iterator for BlockIter<'a, C> {
    // This is ugly, but necessary because of Iterator's signature
    type Item = (Vec<u8>, &'a [u8]);

    fn next(&mut self) -> Option<Self::Item> {
        let current_offset = self.offset;

        if current_offset >= self.block.restarts_off {
            return None;
        }
        let (shared, non_shared, valsize) = self.parse_entry();
        self.assemble_key(shared, non_shared);

        self.val_offset = self.offset + non_shared;
        self.offset = self.val_offset + valsize;

        let num_restarts = self.block.number_restarts();
        while self.current_restart_ix + 1 < num_restarts &&
              self.block.get_restart_point(self.current_restart_ix + 1) < current_offset {
            self.current_restart_ix += 1;
        }
        Some((self.key.clone(), &self.block.data[self.val_offset..self.val_offset + valsize]))
    }
}

impl<'a, C: 'a + Comparator> LdbIterator<'a> for BlockIter<'a, C> {
    // TODO: Use binary search here
    fn seek(&mut self, to: &[u8]) {
        loop {
            if let Some((k, _)) = self.next() {
                if C::cmp(k.as_slice(), to) != Ordering::Less {
                    break;
                }
            } else {
                break;
            }
        }
    }

    fn valid(&self) -> bool {
        !self.key.is_empty() && self.val_offset > 0 && self.val_offset < self.block.restarts_off
    }

    fn current(&self) -> Self::Item {
        assert!(self.valid());
        (self.key.clone(), &self.block.data[self.val_offset..self.offset])
    }
}

#[cfg(test)]
mod tests {
    #[test]
    fn test_iter() {}
}
