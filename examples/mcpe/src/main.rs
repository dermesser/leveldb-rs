use miniz_oxide::deflate::{compress_to_vec, compress_to_vec_zlib, CompressionLevel};
use miniz_oxide::inflate::{decompress_to_vec, decompress_to_vec_zlib};
use rusty_leveldb::compressor::NoneCompressor;
use rusty_leveldb::{Compressor, CompressorList, Options, DB};
use std::rc::Rc;

/// A zlib compressor that with zlib wrapper
///
/// This is use for old world format
struct ZlibCompressor(u8);

impl ZlibCompressor {
    /// compression level 0-10
    pub fn new(level: u8) -> Self {
        assert!(level <= 10);
        Self(level)
    }
}

impl Compressor for ZlibCompressor {
    fn encode(&self, block: Vec<u8>) -> rusty_leveldb::Result<Vec<u8>> {
        Ok(compress_to_vec_zlib(&block, self.0))
    }

    fn decode(&self, block: Vec<u8>) -> rusty_leveldb::Result<Vec<u8>> {
        decompress_to_vec_zlib(&block).map_err(|e| rusty_leveldb::Status {
            code: rusty_leveldb::StatusCode::CompressionError,
            err: e.to_string(),
        })
    }
}

/// A zlib compressor that without zlib wrapper
///
/// > windowBits can also be –8..–15 for raw deflate. In this case, -windowBits determines the window size. deflate() will then generate raw deflate data with no zlib header or trailer, and will not compute a check value.
/// >
/// > From [zlib manual](https://zlib.net/manual.html)
///
/// It seems like Mojang use this for newer version
///
/// A copy of Mojang's implementation can be find [here](https://github.com/reedacartwright/rbedrock/blob/fb32a899da4e15c1aaa0d6de2b459e914e183516/src/leveldb-mcpe/db/zlib_compressor.cc#L119).
struct RawZlibCompressor(u8);

impl RawZlibCompressor {
    /// compression level 0-10
    pub fn new(level: u8) -> Self {
        assert!(level <= 10);
        Self(level)
    }
}

impl Compressor for RawZlibCompressor {
    fn encode(&self, block: Vec<u8>) -> rusty_leveldb::Result<Vec<u8>> {
        Ok(compress_to_vec(&block, self.0))
    }

    fn decode(&self, block: Vec<u8>) -> rusty_leveldb::Result<Vec<u8>> {
        decompress_to_vec(&block).map_err(|e| rusty_leveldb::Status {
            code: rusty_leveldb::StatusCode::CompressionError,
            err: e.to_string(),
        })
    }
}

pub fn mcpe_options(compression_level: u8) -> Options {
    let mut opt = Options::default();

    // Mojang create a custom [compressor list](https://github.com/reedacartwright/rbedrock/blob/fb32a899da4e15c1aaa0d6de2b459e914e183516/src/leveldb-mcpe/include/leveldb/options.h#L123)
    // Sample config for compressor list can be find in [here](https://github.com/reedacartwright/rbedrock/blob/fb32a899da4e15c1aaa0d6de2b459e914e183516/src/leveldb-mcpe/mcpe_sample_setup.cpp#L24-L28)
    //
    // Their compression id can be find in [here](https://github.com/reedacartwright/rbedrock/blob/fb32a899da4e15c1aaa0d6de2b459e914e183516/src/leveldb-mcpe/include/leveldb/zlib_compressor.h#L38)
    // and [here](https://github.com/reedacartwright/rbedrock/blob/fb32a899da4e15c1aaa0d6de2b459e914e183516/src/leveldb-mcpe/include/leveldb/zlib_compressor.h#L48)
    //
    // Compression id will be use in [here](https://github.com/reedacartwright/rbedrock/blob/fb32a899da4e15c1aaa0d6de2b459e914e183516/src/leveldb-mcpe/table/format.cc#L125-L150)
    let mut list = CompressorList::new();
    list.set_with_id(0, NoneCompressor {});
    list.set_with_id(2, ZlibCompressor::new(compression_level));
    list.set_with_id(4, RawZlibCompressor::new(compression_level));
    opt.compressor_list = Rc::new(list);

    // Set compressor
    // Minecraft bedrock may use other id than 4 however default is 4. [Mojang's implementation](https://github.com/reedacartwright/rbedrock/blob/fb32a899da4e15c1aaa0d6de2b459e914e183516/src/leveldb-mcpe/table/table_builder.cc#L152)
    //
    // There is a bug in this library that you have to open a database with the same compression type as it was written to.
    // If raw data is smaller than compression, Mojang will use raw data. [Mojang's implementation](https://github.com/reedacartwright/rbedrock/blob/fb32a899da4e15c1aaa0d6de2b459e914e183516/src/leveldb-mcpe/table/table_builder.cc#L155-L165)
    // There is a small chance that compression id 0 exists, you should use compression id 0 to write it.
    opt.compressor = 4;

    opt
}

/// Path to world's db folder
const PATH: &str = "mcpe_db";

/// Mojang use `DefaultLevel` for world compression
const COMPRESSION_LEVEL: u8 = CompressionLevel::DefaultLevel as u8;

fn main() {
    let opt = mcpe_options(COMPRESSION_LEVEL);
    let mut db = DB::open(PATH, opt).unwrap();
    db.put(b"~local_player", b"NBT data goes here").unwrap();
    let value = db.get(b"~local_player").unwrap();
    assert_eq!(value, b"NBT data goes here")
}
