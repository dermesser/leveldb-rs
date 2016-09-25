use block::BlockIter;
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
    cmp: C,

    footer: TableFooter,
    indexblock: BlockIter<C>,
    filters: Option<FilterBlockReader<FP>>,
}

impl<R: Read + Seek, C: Comparator, FP: FilterPolicy> Table<R, C, FP> {
    pub fn new(mut file: R, size: usize, cmp: C, fp: FP, opt: Options) -> Result<Table<R, C, FP>> {
        let footer = try!(Table::<R, C, FP>::read_footer(&mut file, size));

        let indexblock = try!(Table::<R, C, FP>::read_block(&cmp, &mut file, &footer.index));
        let mut metaindexblock =
            try!(Table::<R, C, FP>::read_block(&cmp, &mut file, &footer.metaindex));

        let mut filter_block_reader = None;
        let mut filter_name = "filter.".as_bytes().to_vec();
        filter_name.extend_from_slice(fp.name().as_bytes());

        metaindexblock.seek(&filter_name);
        if let Some((_key, val)) = metaindexblock.current() {
            let filter_block_location = BlockHandle::decode(&val).0;

            if filter_block_location.size() > 0 {
                let filter_block =
                    try!(Table::<R, C, FP>::read_block(&cmp, &mut file, &filter_block_location));
                filter_block_reader = Some(FilterBlockReader::new(fp, filter_block.obtain()));
            }
        }
        metaindexblock.reset();

        Ok(Table {
            file: file,
            file_size: size,
            cmp: cmp,
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
    fn read_block(cmp: &C, f: &mut R, location: &BlockHandle) -> Result<BlockIter<C>> {
        try!(f.seek(SeekFrom::Start(location.offset() as u64)));

        let mut buf = Vec::new();
        buf.resize(location.size(), 0);

        try!(f.read_exact(&mut buf[0..location.size()]));

        Ok(BlockIter::new(buf, *cmp))
    }

    fn read_block_(&mut self, location: &BlockHandle) -> Result<BlockIter<C>> {
        Table::<R, C, FP>::read_block(&self.cmp, &mut self.file, location)
    }

    /// Returns the offset of the block that contains `key`.
    pub fn approx_offset_of(&self, key: &[u8]) -> usize {
        // cheap clone!
        let mut iter = self.indexblock.clone();

        iter.seek(key);

        if let Some((_, val)) = iter.current() {
            let location = BlockHandle::decode(&val).0;
            return location.offset();
        }

        return self.footer.metaindex.offset();
    }
}

/// This iterator is a "TwoLevelIterator"; it uses an index block in order to get an offset hint
/// into the data blocks.
pub struct TableIterator<'a, R: 'a + Read + Seek, C: 'a + Comparator, FP: 'a + FilterPolicy> {
    table: &'a mut Table<R, C, FP>,
    current_block: BlockIter<C>,
    index_block: BlockIter<C>,
}

impl<'a, C: Comparator, R: Read + Seek, FP: FilterPolicy> TableIterator<'a, R, C, FP> {
    // Skips to the entry referenced by the next index block.
    fn skip_to_next_entry(&mut self) -> bool {
        if let Some((_key, val)) = self.index_block.next() {
            let (new_block_h, _) = BlockHandle::decode(&val);
            if let Ok(block) = self.table.read_block_(&new_block_h) {
                self.current_block = block;
                true
            } else {
                false
            }
        } else {
            false
        }
    }
}

impl<'a, C: Comparator, R: Read + Seek, FP: FilterPolicy> Iterator for TableIterator<'a, R, C, FP> {
    type Item = (Vec<u8>, Vec<u8>);

    fn next(&mut self) -> Option<Self::Item> {
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
