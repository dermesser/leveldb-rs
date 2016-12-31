use block::{BlockBuilder, BlockContents};
use blockhandle::BlockHandle;
use filter::{FilterPolicy, NoFilterPolicy};
use filter_block::FilterBlockBuilder;
use options::{CompressionType, Options};
use key_types::{InternalKey, InternalKeyCmp};
use types::Cmp;

use std::io::Write;
use std::cmp::Ordering;
use std::sync::Arc;

use crc::crc32;
use crc::Hasher32;
use integer_encoding::FixedInt;

pub const FOOTER_LENGTH: usize = 40;
pub const FULL_FOOTER_LENGTH: usize = FOOTER_LENGTH + 8;
pub const MAGIC_FOOTER_NUMBER: u64 = 0xdb4775248b80fb57;
pub const MAGIC_FOOTER_ENCODED: [u8; 8] = [0x57, 0xfb, 0x80, 0x8b, 0x24, 0x75, 0x47, 0xdb];

pub const TABLE_BLOCK_COMPRESS_LEN: usize = 1;
pub const TABLE_BLOCK_CKSUM_LEN: usize = 4;

fn find_shortest_sep<'a>(cmp: &Arc<Box<Cmp>>, lo: InternalKey<'a>, hi: InternalKey<'a>) -> Vec<u8> {
    let min;

    if lo.len() < hi.len() {
        min = lo.len();
    } else {
        min = hi.len();
    }

    let mut diff_at = 0;

    while diff_at < min && lo[diff_at] == hi[diff_at] {
        diff_at += 1;
    }

    if diff_at == min {
        return Vec::from(lo);
    } else {
        if lo[diff_at] < 0xff && lo[diff_at] + 1 < hi[diff_at] {
            let mut result = lo.to_vec();
            result[diff_at] += 1;
            println!("{:?}", (&result, hi));
            assert_eq!(cmp.cmp(&result, hi), Ordering::Less);
            return result;
        }
        return Vec::from(lo);
    }
}

/// Footer is a helper for encoding/decoding a table footer.
#[derive(Debug)]
pub struct Footer {
    pub meta_index: BlockHandle,
    pub index: BlockHandle,
}

impl Footer {
    pub fn new(metaix: BlockHandle, index: BlockHandle) -> Footer {
        Footer {
            meta_index: metaix,
            index: index,
        }
    }

    pub fn decode(from: &[u8]) -> Footer {
        assert!(from.len() >= FULL_FOOTER_LENGTH);
        assert_eq!(&from[FOOTER_LENGTH..], &MAGIC_FOOTER_ENCODED);
        let (meta, metalen) = BlockHandle::decode(&from[0..]);
        let (ix, _) = BlockHandle::decode(&from[metalen..]);

        Footer {
            meta_index: meta,
            index: ix,
        }
    }

    pub fn encode(&self, to: &mut [u8]) {
        assert!(to.len() >= FOOTER_LENGTH + 8);

        let s1 = self.meta_index.encode_to(to);
        let s2 = self.index.encode_to(&mut to[s1..]);

        for i in s1 + s2..FOOTER_LENGTH {
            to[i] = 0;
        }
        for i in FOOTER_LENGTH..FULL_FOOTER_LENGTH {
            to[i] = MAGIC_FOOTER_ENCODED[i - FOOTER_LENGTH];
        }
    }
}

/// A table consists of DATA BLOCKs, META BLOCKs, a METAINDEX BLOCK, an INDEX BLOCK and a FOOTER.
///
/// DATA BLOCKs, META BLOCKs, INDEX BLOCK and METAINDEX BLOCK are built using the code in
/// the `block` module.
///
/// The FOOTER consists of a BlockHandle wthat points to the metaindex block, another pointing to
/// the index block, padding to fill up to 40 B and at the end the 8B magic number
/// 0xdb4775248b80fb57.

pub struct TableBuilder<'a, Dst: Write, FilterPol: FilterPolicy> {
    opt: Options,
    dst: Dst,

    offset: usize,
    num_entries: usize,
    prev_block_last_key: Vec<u8>,

    data_block: Option<BlockBuilder>,
    index_block: Option<BlockBuilder>,
    filter_block: Option<FilterBlockBuilder<'a, FilterPol>>,
}

impl<'a, Dst: Write> TableBuilder<'a, Dst, NoFilterPolicy> {
    pub fn new_no_filter(opt: Options, dst: Dst) -> TableBuilder<'a, Dst, NoFilterPolicy> {
        TableBuilder::new(opt, dst, NoFilterPolicy)
    }
}

/// TableBuilder is used for building a new SSTable. It groups entries into blocks,
/// calculating checksums and bloom filters.
/// It's recommended that you use InternalFilterPolicy as FilterPol, as that policy extracts the
/// underlying user keys from the InternalKeys used as keys in the table.
impl<'a, Dst: Write, FilterPol: FilterPolicy> TableBuilder<'a, Dst, FilterPol> {
    /// Create a new table builder.
    /// The comparator in opt will be wrapped in a InternalKeyCmp.
    pub fn new(mut opt: Options, dst: Dst, fpol: FilterPol) -> TableBuilder<'a, Dst, FilterPol> {
        opt.cmp = Arc::new(Box::new(InternalKeyCmp(opt.cmp.clone())));
        TableBuilder::new_raw(opt, dst, fpol)
    }

    /// Like new(), but doesn't wrap the comparator in an InternalKeyCmp (for testing)
    pub fn new_raw(opt: Options, dst: Dst, fpol: FilterPol) -> TableBuilder<'a, Dst, FilterPol> {
        TableBuilder {
            opt: opt.clone(),
            dst: dst,
            offset: 0,
            prev_block_last_key: vec![],
            num_entries: 0,
            data_block: Some(BlockBuilder::new(opt.clone())),
            index_block: Some(BlockBuilder::new(opt)),
            filter_block: Some(FilterBlockBuilder::new(fpol)),
        }
    }

    pub fn entries(&self) -> usize {
        self.num_entries
    }

    /// Add a key to the table. The key as to be lexically greater or equal to the last one added.
    pub fn add(&mut self, key: InternalKey<'a>, val: &[u8]) {
        assert!(self.data_block.is_some());

        if !self.prev_block_last_key.is_empty() {
            assert!(self.opt.cmp.cmp(&self.prev_block_last_key, key) == Ordering::Less);
        }

        if self.data_block.as_ref().unwrap().size_estimate() > self.opt.block_size {
            self.write_data_block(key);
        }

        let dblock = &mut self.data_block.as_mut().unwrap();

        if let Some(ref mut fblock) = self.filter_block {
            fblock.add_key(key);
        }

        self.num_entries += 1;
        dblock.add(key, val);
    }

    /// Writes an index entry for the current data_block where `next_key` is the first key of the
    /// next block.
    /// Calls write_block() for writing the block to disk.
    fn write_data_block<'b>(&mut self, next_key: InternalKey<'b>) {
        assert!(self.data_block.is_some());

        let block = self.data_block.take().unwrap();
        let sep = find_shortest_sep(&self.opt.cmp, &block.last_key(), next_key);
        self.prev_block_last_key = Vec::from(block.last_key());
        let contents = block.finish();

        let handle = BlockHandle::new(self.offset, contents.len());
        let mut handle_enc = [0 as u8; 16];
        let enc_len = handle.encode_to(&mut handle_enc);

        self.index_block.as_mut().unwrap().add(&sep, &handle_enc[0..enc_len]);
        self.data_block = Some(BlockBuilder::new(self.opt.clone()));

        let ctype = self.opt.compression_type;

        self.write_block(contents, ctype);

        if let Some(ref mut fblock) = self.filter_block {
            fblock.start_block(self.offset);
        }
    }

    /// Calculates the checksum, writes the block to disk and updates the offset.
    fn write_block(&mut self, block: BlockContents, t: CompressionType) -> BlockHandle {
        // compression is still unimplemented
        assert_eq!(t, CompressionType::CompressionNone);

        let mut buf = [0 as u8; TABLE_BLOCK_CKSUM_LEN];
        let mut digest = crc32::Digest::new(crc32::CASTAGNOLI);

        digest.write(&block);
        digest.write(&[self.opt.compression_type as u8; TABLE_BLOCK_COMPRESS_LEN]);
        digest.sum32().encode_fixed(&mut buf);

        // TODO: Handle errors here.
        let _ = self.dst.write(&block);
        let _ = self.dst.write(&[t as u8; TABLE_BLOCK_COMPRESS_LEN]);
        let _ = self.dst.write(&buf);

        let handle = BlockHandle::new(self.offset, block.len());

        self.offset += block.len() + TABLE_BLOCK_COMPRESS_LEN + TABLE_BLOCK_CKSUM_LEN;

        handle
    }

    pub fn finish(mut self) {
        assert!(self.data_block.is_some());
        let ctype = self.opt.compression_type;

        // If there's a pending data block, write it
        if self.data_block.as_ref().unwrap().entries() > 0 {
            // Find a key reliably past the last key
            // NOTE: This only works if the basic comparator is DefaultCmp. (not a problem as long
            // as we don't accept comparators from users)
            let mut past_block =
                Vec::with_capacity(self.data_block.as_ref().unwrap().last_key().len() + 1);
            // Push 255 to the beginning
            past_block.extend_from_slice(&[0xff; 1]);
            past_block.extend_from_slice(self.data_block.as_ref().unwrap().last_key());

            self.write_data_block(&past_block);
        }

        // Create metaindex block
        let mut meta_ix_block = BlockBuilder::new(self.opt.clone());

        if self.filter_block.is_some() {
            // if there's a filter block, write the filter block and add it to the metaindex block.
            let fblock = self.filter_block.take().unwrap();
            let filter_key = format!("filter.{}", fblock.filter_name());
            let fblock_data = fblock.finish();
            let fblock_handle = self.write_block(fblock_data, CompressionType::CompressionNone);

            let mut handle_enc = [0 as u8; 16];
            let enc_len = fblock_handle.encode_to(&mut handle_enc);

            meta_ix_block.add(filter_key.as_bytes(), &handle_enc[0..enc_len]);
        }

        // write metaindex block
        let meta_ix = meta_ix_block.finish();
        let meta_ix_handle = self.write_block(meta_ix, ctype);

        // write index block
        let index_cont = self.index_block.take().unwrap().finish();
        let ix_handle = self.write_block(index_cont, ctype);

        // write footer.
        let footer = Footer::new(meta_ix_handle, ix_handle);
        let mut buf = [0; FULL_FOOTER_LENGTH];
        footer.encode(&mut buf);

        self.offset += self.dst.write(&buf[..]).unwrap();
    }
}

#[cfg(test)]
mod tests {
    use super::{find_shortest_sep, Footer, TableBuilder};
    use blockhandle::BlockHandle;
    use filter::BloomPolicy;
    use options::Options;
    use types::{DefaultCmp, Cmp};

    use std::sync::Arc;

    #[test]
    fn test_shortest_sep() {
        let cmp = Arc::new(Box::new(DefaultCmp) as Box<Cmp>);
        assert_eq!(find_shortest_sep(&cmp, "abcd".as_bytes(), "abcf".as_bytes()),
                   "abce".as_bytes());
        assert_eq!(find_shortest_sep(&cmp, "abcdefghi".as_bytes(), "abcffghi".as_bytes()),
                   "abceefghi".as_bytes());
        assert_eq!(find_shortest_sep(&cmp, "a".as_bytes(), "a".as_bytes()),
                   "a".as_bytes());
        assert_eq!(find_shortest_sep(&cmp, "a".as_bytes(), "b".as_bytes()),
                   "a".as_bytes());
        assert_eq!(find_shortest_sep(&cmp, "abc".as_bytes(), "zzz".as_bytes()),
                   "bbc".as_bytes());
        assert_eq!(find_shortest_sep(&cmp, "".as_bytes(), "".as_bytes()),
                   "".as_bytes());
    }

    #[test]
    fn test_footer() {
        let f = Footer::new(BlockHandle::new(44, 4), BlockHandle::new(55, 5));
        let mut buf = [0; 48];
        f.encode(&mut buf[..]);

        let f2 = Footer::decode(&buf);
        assert_eq!(f2.meta_index.offset(), 44);
        assert_eq!(f2.meta_index.size(), 4);
        assert_eq!(f2.index.offset(), 55);
        assert_eq!(f2.index.size(), 5);

    }

    #[test]
    fn test_table_builder() {
        let mut d = Vec::with_capacity(512);
        let mut opt = Options::default();
        opt.block_restart_interval = 3;
        let mut b = TableBuilder::new_raw(opt, &mut d, BloomPolicy::new(4));

        let data = vec![("abc", "def"), ("abd", "dee"), ("bcd", "asa"), ("bsr", "a00")];

        for &(k, v) in data.iter() {
            b.add(k.as_bytes(), v.as_bytes());
        }

        assert!(b.filter_block.is_some());
        b.finish();
    }

    #[test]
    #[should_panic]
    fn test_bad_input() {
        let mut d = Vec::with_capacity(512);
        let mut opt = Options::default();
        opt.block_restart_interval = 3;
        let mut b = TableBuilder::new_raw(opt, &mut d, BloomPolicy::new(4));

        // Test two equal consecutive keys
        let data = vec![("abc", "def"), ("abc", "dee"), ("bcd", "asa"), ("bsr", "a00")];

        for &(k, v) in data.iter() {
            b.add(k.as_bytes(), v.as_bytes());
        }
    }
}
