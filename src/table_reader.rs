use block::{Block, BlockIter};
use blockhandle::BlockHandle;
use filter::FilterPolicy;
use filter_block::FilterBlockReader;
use options::{self, CompressionType, Options};
use table_builder::{self, Footer};
use types::{Comparator, LdbIterator};

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
fn read_block<R: Read + Seek, C: Comparator>(cmp: &C,
                                             f: &mut R,
                                             location: &BlockHandle)
                                             -> Result<TableBlock<C>> {
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
        block: Block::new(buf, *cmp),
        checksum: u32::decode_fixed(&cksum),
        compression: options::int_to_compressiontype(compress[0] as u32)
            .unwrap_or(CompressionType::CompressionNone),
    })
}

struct TableBlock<C: Comparator> {
    block: Block<C>,
    checksum: u32,
    compression: CompressionType,
}

impl<C: Comparator> TableBlock<C> {
    /// Verify checksum of block
    fn verify(&self) -> bool {
        let mut digest = crc32::Digest::new(crc32::CASTAGNOLI);
        digest.write(&self.block.contents());
        digest.write(&[self.compression as u8; 1]);

        digest.sum32() == self.checksum
    }
}

pub struct Table<R: Read + Seek, C: Comparator, FP: FilterPolicy> {
    file: R,
    file_size: usize,

    opt: Options,
    cmp: C,

    footer: Footer,
    indexblock: Block<C>,
    filters: Option<FilterBlockReader<FP>>,
}

impl<R: Read + Seek, C: Comparator, FP: FilterPolicy> Table<R, C, FP> {
    pub fn new(mut file: R, size: usize, cmp: C, fp: FP, opt: Options) -> Result<Table<R, C, FP>> {
        let footer = try!(read_footer(&mut file, size));

        let indexblock = try!(read_block(&cmp, &mut file, &footer.index));
        let metaindexblock = try!(read_block(&cmp, &mut file, &footer.meta_index));

        if !indexblock.verify() || !metaindexblock.verify() {
            return Err(io::Error::new(io::ErrorKind::InvalidData,
                                      "Indexblock/Metaindexblock failed verification"));
        }

        let mut filter_block_reader = None;
        let mut filter_name = "filter.".as_bytes().to_vec();
        filter_name.extend_from_slice(fp.name().as_bytes());

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
            cmp: cmp,
            opt: opt,
            footer: footer,
            filters: filter_block_reader,
            indexblock: indexblock.block,
        })
    }

    fn read_block(&mut self, location: &BlockHandle) -> Result<TableBlock<C>> {
        let b = try!(read_block(&self.cmp, &mut self.file, location));

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
    fn iter<'a>(&'a mut self) -> TableIterator<'a, R, C, FP> {
        let mut iter = TableIterator {
            current_block: self.indexblock.iter(), // just for filling in here
            index_block: self.indexblock.iter(),
            table: self,
            init: false,
        };
        iter.skip_to_next_entry();
        iter
    }
}

/// This iterator is a "TwoLevelIterator"; it uses an index block in order to get an offset hint
/// into the data blocks.
pub struct TableIterator<'a, R: 'a + Read + Seek, C: 'a + Comparator, FP: 'a + FilterPolicy> {
    table: &'a mut Table<R, C, FP>,
    current_block: BlockIter<C>,
    index_block: BlockIter<C>,

    init: bool,
}

impl<'a, C: Comparator, R: Read + Seek, FP: FilterPolicy> TableIterator<'a, R, C, FP> {
    // Skips to the entry referenced by the next entry in the index block.
    // This is called once a block has run out of entries.
    fn skip_to_next_entry(&mut self) -> bool {
        if let Some((_key, val)) = self.index_block.next() {
            self.load_block(&val).is_ok()
        } else {
            false
        }
    }

    // Load the block at `handle` into `self.current_block`
    fn load_block(&mut self, handle: &[u8]) -> Result<()> {
        let (new_block_handle, _) = BlockHandle::decode(handle);

        let block = try!(self.table.read_block(&new_block_handle));
        self.current_block = block.block.iter();

        Ok(())
    }
}

impl<'a, C: Comparator, R: Read + Seek, FP: FilterPolicy> Iterator for TableIterator<'a, R, C, FP> {
    type Item = (Vec<u8>, Vec<u8>);

    fn next(&mut self) -> Option<Self::Item> {
        self.init = true;
        if let Some((key, val)) = self.current_block.next() {
            Some((key, val))
        } else {
            if self.skip_to_next_entry() {
                self.next()
            } else {
                None
            }
        }
    }
}

impl<'a, C: Comparator, R: Read + Seek, FP: FilterPolicy> LdbIterator for TableIterator<'a,
                                                                                        R,
                                                                                        C,
                                                                                        FP> {
    // A call to valid() after seeking is necessary to ensure that the seek worked (e.g., no error
    // while reading from disk)
    fn seek(&mut self, to: &[u8]) {
        // first seek in index block, rewind by one entry (so we get the next smaller index entry),
        // then set current_block and seek there

        self.index_block.seek(to);

        if let Some((k, _)) = self.index_block.current() {
            if self.table.cmp.cmp(to, &k) <= Ordering::Equal {
                // ok, found right block: continue below
            } else {
                self.reset();
            }
        } else {
            panic!("Unexpected None from current() (bug)");
        }

        // Read block and seek to entry in that block
        if let Some((k, handle)) = self.index_block.current() {
            assert!(self.table.cmp.cmp(to, &k) <= Ordering::Equal);

            if let Ok(()) = self.load_block(&handle) {
                self.current_block.seek(to);
                self.init = true;
            } else {
                self.reset();
            }
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
        self.skip_to_next_entry();
    }

    // This iterator is special in that it's valid even before the first call to next(). It behaves
    // correctly, though.
    fn valid(&self) -> bool {
        self.init && (self.current_block.valid() || self.index_block.valid())
    }

    fn current(&self) -> Option<Self::Item> {
        self.current_block.current()
    }
}

#[cfg(test)]
mod tests {
    use filter::BloomPolicy;
    use options::Options;
    use table_builder::TableBuilder;
    use types::{StandardComparator, LdbIterator};

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
        opt.block_size = 64;

        {
            let mut b = TableBuilder::new(opt, StandardComparator, &mut d, BloomPolicy::new(4));
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
    fn test_table_iterator_fwd() {
        let (src, size) = build_table();
        let data = build_data();

        let mut table = Table::new(Cursor::new(&src as &[u8]),
                                   size,
                                   StandardComparator,
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
    }

    #[test]
    fn test_table_iterator_state_behavior() {
        let (src, size) = build_table();

        let mut table = Table::new(Cursor::new(&src as &[u8]),
                                   size,
                                   StandardComparator,
                                   BloomPolicy::new(4),
                                   Options::default())
            .unwrap();
        let mut iter = table.iter();

        // behavior test

        // See comment on valid()
        assert!(!iter.valid());
        assert!(iter.current().is_none());

        assert!(iter.next().is_some());
        assert!(iter.valid());
        assert!(iter.current().is_some());

        assert!(iter.next().is_some());
        assert!(iter.prev().is_some());
        assert!(iter.current().is_some());

        iter.reset();
        assert!(!iter.valid());
        assert!(iter.current().is_none());
    }

    #[test]
    fn test_table_iterator_values() {
        let (src, size) = build_table();
        let data = build_data();

        let mut table = Table::new(Cursor::new(&src as &[u8]),
                                   size,
                                   StandardComparator,
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
                                   StandardComparator,
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
}
