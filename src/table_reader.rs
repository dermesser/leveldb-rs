use block::{Block, BlockIter};
use blockhandle::BlockHandle;
use filter::FilterPolicy;
use filter_block::FilterBlockReader;
use options::{self, CompressionType, Options};
use table_builder::{self, Footer};
use types::{cmp, LdbIterator};
use key_types::InternalKey;

use std::io::{self, Read, Seek, SeekFrom, Result};
use std::cmp::Ordering;

use integer_encoding::FixedInt;
use crc::crc32::{self, Hasher32};

/// Reads the table footer.
fn read_footer<R: Read + Seek>(f: &mut R, size: usize) -> Result<Footer> {
    try!(f.seek(SeekFrom::Start((size - table_builder::FULL_FOOTER_LENGTH) as u64)));
    let mut buf = [0; table_builder::FULL_FOOTER_LENGTH];
    try!(f.read_exact(&mut buf));
    Ok(Footer::decode(&buf))
}

fn read_bytes<R: Read + Seek>(f: &mut R, location: &BlockHandle) -> Result<Vec<u8>> {
    try!(f.seek(SeekFrom::Start(location.offset() as u64)));

    let mut buf = Vec::new();
    buf.resize(location.size(), 0);

    try!(f.read_exact(&mut buf[0..location.size()]));

    Ok(buf)
}

/// Reads a block at location.
fn read_block<R: Read + Seek>(f: &mut R, location: &BlockHandle) -> Result<TableBlock> {
    // The block is denoted by offset and length in BlockHandle. A block in an encoded
    // table is followed by 1B compression type and 4B checksum.
    let buf = try!(read_bytes(f, location));
    let compress = try!(read_bytes(f,
                                   &BlockHandle::new(location.offset() + location.size(),
                                                     table_builder::TABLE_BLOCK_COMPRESS_LEN)));
    let cksum = try!(read_bytes(f,
                                &BlockHandle::new(location.offset() + location.size() +
                                                  table_builder::TABLE_BLOCK_COMPRESS_LEN,
                                                  table_builder::TABLE_BLOCK_CKSUM_LEN)));
    Ok(TableBlock {
        block: Block::new(buf),
        checksum: u32::decode_fixed(&cksum),
        compression: options::int_to_compressiontype(compress[0] as u32)
            .unwrap_or(CompressionType::CompressionNone),
    })
}

struct TableBlock {
    block: Block,
    checksum: u32,
    compression: CompressionType,
}

impl TableBlock {
    /// Verify checksum of block
    fn verify(&self) -> bool {
        let mut digest = crc32::Digest::new(crc32::CASTAGNOLI);
        digest.write(&self.block.contents());
        digest.write(&[self.compression as u8; 1]);

        digest.sum32() == self.checksum
    }
}

pub struct Table<R: Read + Seek, FP: FilterPolicy> {
    file: R,
    file_size: usize,

    opt: Options,

    footer: Footer,
    indexblock: Block,
    filters: Option<FilterBlockReader<FP>>,
}

impl<R: Read + Seek, FP: FilterPolicy> Table<R, FP> {
    pub fn new(mut file: R, size: usize, fp: FP, opt: Options) -> Result<Table<R, FP>> {
        let footer = try!(read_footer(&mut file, size));

        let indexblock = try!(read_block(&mut file, &footer.index));
        let metaindexblock = try!(read_block(&mut file, &footer.meta_index));

        if !indexblock.verify() || !metaindexblock.verify() {
            return Err(io::Error::new(io::ErrorKind::InvalidData,
                                      "Indexblock/Metaindexblock failed verification"));
        }

        // Open filter block for reading
        let mut filter_block_reader = None;
        let filter_name = format!("filter.{}", fp.name()).as_bytes().to_vec();

        let mut metaindexiter = metaindexblock.block.iter();

        metaindexiter.seek(&filter_name);

        if let Some((_key, val)) = metaindexiter.current() {
            let filter_block_location = BlockHandle::decode(&val).0;

            if filter_block_location.size() > 0 {
                let buf = try!(read_bytes(&mut file, &filter_block_location));
                filter_block_reader = Some(FilterBlockReader::new_owned(fp, buf));
            }
        }

        metaindexiter.reset();

        Ok(Table {
            file: file,
            file_size: size,
            opt: opt,
            footer: footer,
            filters: filter_block_reader,
            indexblock: indexblock.block,
        })
    }

    fn read_block(&mut self, location: &BlockHandle) -> Result<TableBlock> {
        let b = try!(read_block(&mut self.file, location));

        if !b.verify() {
            Err(io::Error::new(io::ErrorKind::InvalidData, "Data block failed verification"))
        } else {
            Ok(b)
        }
    }

    /// Returns the offset of the block that contains `key`.
    pub fn approx_offset_of(&self, key: &[u8]) -> usize {
        let mut iter = self.indexblock.iter();

        iter.seek(key);

        if let Some((_, val)) = iter.current() {
            let location = BlockHandle::decode(&val).0;
            return location.offset();
        }

        return self.footer.meta_index.offset();
    }

    // Iterators read from the file; thus only one iterator can be borrowed (mutably) per scope
    fn iter<'a>(&'a mut self) -> TableIterator<'a, R, FP> {
        let iter = TableIterator {
            current_block: self.indexblock.iter(), // just for filling in here
            current_block_off: 0,
            index_block: self.indexblock.iter(),
            table: self,
            init: false,
        };
        iter
    }

    /// Retrieve value from table. This function uses the attached filters, so is better suited if
    /// you frequently look for non-existing values (as it will detect the non-existence of an
    /// entry in a block without having to load the block).
    pub fn get<'a>(&mut self, to: InternalKey<'a>) -> Option<Vec<u8>> {
        let mut iter = self.iter();

        iter.seek(to);

        if let Some((k, v)) = iter.current() {
            if k == to { Some(v) } else { None }
        } else {
            None
        }

        // Future impl:
        //
        // let mut index_block = self.indexblock.iter();
        //
        // index_block.seek(to);
        //
        // if let Some((past_block, handle)) = index_block.current() {
        // if cmp(to, &past_block) == Ordering::Less {
        // ok, found right block: continue
        // if let Ok(()) = self.load_block(&handle) {
        // self.current_block.seek(to);
        // } else {
        // return None;
        // }*/
        // return None;
        // } else {
        // return None;
        // }
        // } else {
        // return None;
        // }
        //
    }
}

/// This iterator is a "TwoLevelIterator"; it uses an index block in order to get an offset hint
/// into the data blocks.
pub struct TableIterator<'a, R: 'a + Read + Seek, FP: 'a + FilterPolicy> {
    table: &'a mut Table<R, FP>,
    current_block: BlockIter,
    current_block_off: usize,
    index_block: BlockIter,

    init: bool,
}

impl<'a, R: Read + Seek, FP: FilterPolicy> TableIterator<'a, R, FP> {
    // Skips to the entry referenced by the next entry in the index block.
    // This is called once a block has run out of entries.
    fn skip_to_next_entry(&mut self) -> Result<bool> {
        if let Some((_key, val)) = self.index_block.next() {
            self.load_block(&val).map(|_| true)
        } else {
            Ok(false)
        }
    }

    // Load the block at `handle` into `self.current_block`
    fn load_block(&mut self, handle: &[u8]) -> Result<()> {
        let (new_block_handle, _) = BlockHandle::decode(handle);

        let block = try!(self.table.read_block(&new_block_handle));
        self.current_block = block.block.iter();
        self.current_block_off = new_block_handle.offset();

        Ok(())
    }
}

impl<'a, R: Read + Seek, FP: FilterPolicy> Iterator for TableIterator<'a, R, FP> {
    type Item = (Vec<u8>, Vec<u8>);

    fn next(&mut self) -> Option<Self::Item> {
        if !self.init {
            self.init = true;
            if self.skip_to_next_entry().is_err() {
                return None;
            }
        }

        if let Some((key, val)) = self.current_block.next() {
            Some((key, val))
        } else {
            if self.skip_to_next_entry().unwrap_or(false) {
                self.next()
            } else {
                None
            }
        }
    }
}

impl<'a, R: Read + Seek, FP: FilterPolicy> LdbIterator for TableIterator<'a, R, FP> {
    // A call to valid() after seeking is necessary to ensure that the seek worked (e.g., no error
    // while reading from disk)
    fn seek(&mut self, to: &[u8]) {
        // first seek in index block, rewind by one entry (so we get the next smaller index entry),
        // then set current_block and seek there

        self.index_block.seek(to);

        if let Some((past_block, handle)) = self.index_block.current() {
            if cmp(to, &past_block) == Ordering::Less {
                // ok, found right block: continue
                if let Ok(()) = self.load_block(&handle) {
                    self.current_block.seek(to);
                    self.init = true;
                } else {
                    self.reset();
                    return;
                }
            } else {
                self.reset();
                return;
            }
        } else {
            panic!("Unexpected None from current() (bug)");
        }
    }

    fn prev(&mut self) -> Option<Self::Item> {
        // happy path: current block contains previous entry
        if let Some(result) = self.current_block.prev() {
            Some(result)
        } else {
            // Go back one block and look for the last entry in the previous block
            if let Some((_, handle)) = self.index_block.prev() {
                if self.load_block(&handle).is_ok() {
                    self.current_block.seek_to_last();
                    self.current_block.current()
                } else {
                    self.reset();
                    None
                }
            } else {
                None
            }
        }
    }

    fn reset(&mut self) {
        self.index_block.reset();
        self.init = false;
    }

    // This iterator is special in that it's valid even before the first call to next(). It behaves
    // correctly, though.
    fn valid(&self) -> bool {
        self.init && (self.current_block.valid() || self.index_block.valid())
    }

    fn current(&self) -> Option<Self::Item> {
        if self.init {
            self.current_block.current()
        } else {
            None
        }
    }
}

#[cfg(test)]
mod tests {
    use filter::BloomPolicy;
    use options::Options;
    use table_builder::TableBuilder;
    use types::LdbIterator;

    use std::io::Cursor;

    use super::*;

    fn build_data() -> Vec<(&'static str, &'static str)> {
        vec![("abc", "def"),
             ("abd", "dee"),
             ("bcd", "asa"),
             ("bsr", "a00"),
             ("xyz", "xxx"),
             ("xzz", "yyy"),
             ("zzz", "111")]
    }


    fn build_table() -> (Vec<u8>, usize) {
        let mut d = Vec::with_capacity(512);
        let mut opt = Options::default();
        opt.block_restart_interval = 2;
        opt.block_size = 32;

        {
            let mut b = TableBuilder::new(opt, &mut d, BloomPolicy::new(4));
            let data = build_data();

            for &(k, v) in data.iter() {
                b.add(k.as_bytes(), v.as_bytes());
            }

            b.finish();

        }

        let size = d.len();

        (d, size)
    }

    #[test]
    fn test_table_reader_checksum() {
        let (mut src, size) = build_table();
        println!("{}", size);

        src[45] = 0;

        let mut table = Table::new(Cursor::new(&src as &[u8]),
                                   size,
                                   BloomPolicy::new(4),
                                   Options::default())
            .unwrap();

        assert!(table.filters.is_some());
        assert_eq!(table.filters.as_ref().unwrap().num(), 1);

        {
            let iter = table.iter();
            // Last block is skipped
            assert_eq!(iter.count(), 3);

        }

        {
            let iter = table.iter();

            for (k, _) in iter {
                if k == build_data()[2].0.as_bytes() {
                    return;
                }
            }

            panic!("Should have hit 3rd record in table!");
        }
    }

    #[test]
    fn test_table_iterator_fwd() {
        let (src, size) = build_table();
        let data = build_data();

        let mut table = Table::new(Cursor::new(&src as &[u8]),
                                   size,
                                   BloomPolicy::new(4),
                                   Options::default())
            .unwrap();
        let iter = table.iter();
        let mut i = 0;

        for (k, v) in iter {
            assert_eq!((data[i].0.as_bytes(), data[i].1.as_bytes()),
                       (k.as_ref(), v.as_ref()));
            i += 1;
        }

        assert_eq!(i, data.len());
    }

    #[test]
    fn test_table_iterator_filter() {
        let (src, size) = build_table();

        let mut table = Table::new(Cursor::new(&src as &[u8]),
                                   size,
                                   BloomPolicy::new(4),
                                   Options::default())
            .unwrap();
        let filter_reader = table.filters.clone().unwrap();
        let mut iter = table.iter();

        loop {
            if let Some((k, _)) = iter.next() {
                assert!(filter_reader.key_may_match(iter.current_block_off, &k));
                assert!(!filter_reader.key_may_match(iter.current_block_off,
                                                     "somerandomkey".as_bytes()));
            } else {
                break;
            }
        }
    }

    #[test]
    fn test_table_iterator_state_behavior() {
        let (src, size) = build_table();

        let mut table = Table::new(Cursor::new(&src as &[u8]),
                                   size,
                                   BloomPolicy::new(4),
                                   Options::default())
            .unwrap();
        let mut iter = table.iter();

        // behavior test

        // See comment on valid()
        assert!(!iter.valid());
        assert!(iter.current().is_none());

        assert!(iter.next().is_some());
        let first = iter.current();
        assert!(iter.valid());
        assert!(iter.current().is_some());

        assert!(iter.next().is_some());
        assert!(iter.prev().is_some());
        assert!(iter.current().is_some());

        iter.reset();
        assert!(!iter.valid());
        assert!(iter.current().is_none());
        assert_eq!(first, iter.next());
    }

    #[test]
    fn test_table_iterator_values() {
        let (src, size) = build_table();
        let data = build_data();

        let mut table = Table::new(Cursor::new(&src as &[u8]),
                                   size,
                                   BloomPolicy::new(4),
                                   Options::default())
            .unwrap();
        let mut iter = table.iter();
        let mut i = 0;

        iter.next();
        iter.next();

        // Go back to previous entry, check, go forward two entries, repeat
        // Verifies that prev/next works well.
        while iter.valid() && i < data.len() {
            iter.prev();

            if let Some((k, v)) = iter.current() {
                assert_eq!((data[i].0.as_bytes(), data[i].1.as_bytes()),
                           (k.as_ref(), v.as_ref()));
            } else {
                break;
            }

            i += 1;
            iter.next();
            iter.next();
        }

        assert_eq!(i, 7);
    }

    #[test]
    fn test_table_iterator_seek() {
        let (src, size) = build_table();

        let mut table = Table::new(Cursor::new(&src as &[u8]),
                                   size,
                                   BloomPolicy::new(4),
                                   Options::default())
            .unwrap();
        let mut iter = table.iter();

        iter.seek("bcd".as_bytes());
        assert!(iter.valid());
        assert_eq!(iter.current(),
                   Some(("bcd".as_bytes().to_vec(), "asa".as_bytes().to_vec())));
        iter.seek("abc".as_bytes());
        assert!(iter.valid());
        assert_eq!(iter.current(),
                   Some(("abc".as_bytes().to_vec(), "def".as_bytes().to_vec())));
    }

    #[test]
    fn test_table_get() {
        let (src, size) = build_table();

        let mut table = Table::new(Cursor::new(&src as &[u8]),
                                   size,
                                   BloomPolicy::new(4),
                                   Options::default())
            .unwrap();

        assert!(table.get("aaa".as_bytes()).is_none());
        assert_eq!(table.get("abc".as_bytes()), Some("def".as_bytes().to_vec()));
        assert!(table.get("abcd".as_bytes()).is_none());
        assert_eq!(table.get("bcd".as_bytes()), Some("asa".as_bytes().to_vec()));
        assert_eq!(table.get("zzz".as_bytes()), Some("111".as_bytes().to_vec()));
        assert!(table.get("zz1".as_bytes()).is_none());
    }
}
