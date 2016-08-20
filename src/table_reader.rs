use block::Block;
use blockhandle::BlockHandle;
use filter::FilterPolicy;
use filter_block::FilterBlockReader;
use options::Options;
use table_builder;
use types::{Comparator, LdbIterator};

use std::io::{Read, Seek, SeekFrom, Result};

struct TableFooter {
    pub metaindex: BlockHandle,
    pub index: BlockHandle,
}

impl TableFooter {
    fn parse(footer: &[u8]) -> TableFooter {
        assert_eq!(footer.len(), table_builder::FULL_FOOTER_LENGTH);
        assert_eq!(&footer[footer.len() - 8..],
                   table_builder::MAGIC_FOOTER_ENCODED);

        let (mi, n1) = BlockHandle::decode(footer);
        let (ix, _) = BlockHandle::decode(&footer[n1..]);

        TableFooter {
            metaindex: mi,
            index: ix,
        }
    }
}

pub struct Table<R: Read + Seek, C: Comparator, FP: FilterPolicy> {
    file: R,
    file_size: usize,

    opt: Options,
    c: C,

    footer: TableFooter,
    indexblock: Block<C>,
    filters: Option<FilterBlockReader<FP>>,
}

impl<R: Read + Seek, C: Comparator, FP: FilterPolicy> Table<R, C, FP> {
    pub fn new(mut file: R, size: usize, cmp: C, fp: FP, opt: Options) -> Result<Table<R, C, FP>> {
        let footer = try!(Table::<R, C, FP>::read_footer(&mut file, size));

        let indexblock = try!(Table::<R, C, FP>::read_block(&mut file, &footer.index));
        let metaindexblock = try!(Table::<R, C, FP>::read_block(&mut file, &footer.metaindex));

        let mut filter_block_reader = None;
        let mut filter_block_location = BlockHandle::new(0, 0);
        let mut filter_name = "filter.".as_bytes().to_vec();
        filter_name.extend_from_slice(fp.name().as_bytes());

        for (key, val) in metaindexblock.iter() {
            if key == filter_name {
                filter_block_location = BlockHandle::decode(val).0;
                break;
            }
        }

        if filter_block_location.size() > 0 {
            let filter_block = try!(Table::<R, C, FP>::read_block(&mut file,
                                                                  &filter_block_location));
            filter_block_reader = Some(FilterBlockReader::new(fp, filter_block.obtain()));
        }

        Ok(Table {
            file: file,
            file_size: size,
            c: cmp,
            opt: opt,
            footer: footer,
            filters: filter_block_reader,
            indexblock: indexblock,
        })
    }

    /// Reads the table footer.
    fn read_footer(f: &mut R, size: usize) -> Result<TableFooter> {
        try!(f.seek(SeekFrom::Start((size - table_builder::FULL_FOOTER_LENGTH) as u64)));
        let mut buf = [0; table_builder::FULL_FOOTER_LENGTH];
        try!(f.read_exact(&mut buf));
        Ok(TableFooter::parse(&buf))
    }

    /// Reads a block at location.
    fn read_block(f: &mut R, location: &BlockHandle) -> Result<Block<C>> {
        try!(f.seek(SeekFrom::Start(location.offset() as u64)));

        let mut buf = Vec::new();
        buf.resize(location.size(), 0);

        try!(f.read_exact(&mut buf[0..location.size()]));

        Ok(Block::new_with_cmp(buf, C::default()))
    }

    /// Returns the offset of the block that contains `key`.
    pub fn approx_offset_of(&self, key: &[u8]) -> usize {
        let mut iter = self.indexblock.iter();

        iter.seek(key);

        if iter.valid() {
            if let Some((_, val)) = iter.current() {
                let location = BlockHandle::decode(val).0;
                return location.offset();
            }
        }

        return self.footer.metaindex.offset();
    }
}
