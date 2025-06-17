use crate::block::Block;
use crate::blockhandle::BlockHandle;
use crate::crc;
use crate::env::RandomAccess;
use crate::error::{err, Result, StatusCode};
use crate::filter;
use crate::filter_block::FilterBlockReader;
use crate::log::unmask_crc;
use crate::options::Options;
use crate::table_builder;

use integer_encoding::FixedInt;

/// Reads the data for the specified block handle from a file.
fn read_bytes_from_file(f: &dyn RandomAccess, offset: usize, size: usize) -> Result<Vec<u8>> {
    let mut buf = vec![0; size];
    let bytes_read = f.read_at(offset, &mut buf)?;
    if bytes_read != size {
        return err(
            StatusCode::IOError,
            "Failed to read complete data from file",
        );
    }
    Ok(buf)
}

/// Reads a serialized filter block from a file and returns a FilterBlockReader.
pub fn read_filter_block(
    src: &dyn RandomAccess,
    location: &BlockHandle,
    policy: filter::BoxedFilterPolicy,
) -> Result<FilterBlockReader> {
    if location.size() == 0 {
        return err(
            StatusCode::InvalidArgument,
            "no filter block in empty location",
        );
    }
    let buf = read_bytes_from_file(src, location.offset(), location.size())?;
    Ok(FilterBlockReader::new_owned(policy, buf))
}

/// Reads a table block from a random-access source.
/// A table block consists of [bytes..., compress (1B), checksum (4B)]; the handle only refers to
/// the location and length of [bytes...].
pub fn read_table_block(
    opt: Options,
    f: &dyn RandomAccess,
    location: &BlockHandle,
) -> Result<Block> {
    let data_size = location.size();
    let trailer_size =
        table_builder::TABLE_BLOCK_COMPRESS_LEN + table_builder::TABLE_BLOCK_CKSUM_LEN;
    let total_read_size = data_size + trailer_size;

    let mut combined_buf = vec![0; total_read_size];
    let bytes_read = f.read_at(location.offset(), &mut combined_buf)?;

    if bytes_read != total_read_size {
        return err(
            StatusCode::IOError,
            "Failed to read complete block and trailer",
        );
    }

    let block_data_slice = &combined_buf[0..data_size];
    let compress_type = combined_buf[data_size];
    let cksum_slice =
        &combined_buf[data_size + table_builder::TABLE_BLOCK_COMPRESS_LEN..total_read_size];

    let expected_cksum = unmask_crc(u32::decode_fixed(cksum_slice));

    if !verify_table_block(block_data_slice, compress_type, expected_cksum) {
        return err(
            StatusCode::Corruption,
            &format!(
                "checksum verification failed for block at {}",
                location.offset()
            ),
        );
    }
    let compressor_list = opt.compressor_list.clone();

    // The compressor's decode method expects a Vec<u8>.
    // If block_data_slice is empty, to_vec() is fine.
    let decoded_data = compressor_list
        .get(compress_type)?
        .decode(block_data_slice.to_vec())?;

    Ok(Block::new(opt, decoded_data.into()))
}

/// Verify checksum of block
fn verify_table_block(data: &[u8], compression: u8, want: u32) -> bool {
    let mut digest = crc::digest();
    digest.update(data);
    digest.update(&[compression; 1]);
    digest.finalize() == want
}
