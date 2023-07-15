/// Custom compression method
///
/// ```
/// # use rusty_leveldb::{Compressor, CompressorId};
///
/// #[derive(Debug, Clone, Copy, Default)]
/// pub struct CustomCompressor;
///
/// impl CompressorId for CustomCompressor {
///     // a unique id to identify what compressor should DB use
///     const ID: u8 = 42;
/// }
///
/// impl Compressor for CustomCompressor {
///     fn encode(&self, block: Vec<u8>) -> rusty_leveldb::Result<Vec<u8>> {
///         // Do something
///         Ok(block)
///     }
///
///     fn decode(&self, block: Vec<u8>) -> rusty_leveldb::Result<Vec<u8>> {
///         // Do something
///         Ok(block)
///     }
/// }
/// ```
///
/// See [crate::CompressorList] for usage
pub trait Compressor {
    fn encode(&self, block: Vec<u8>) -> crate::Result<Vec<u8>>;

    fn decode(&self, block: Vec<u8>) -> crate::Result<Vec<u8>>;
}

/// Set default compressor id
pub trait CompressorId {
    const ID: u8;
}

/// A compressor that do **Nothing**
///
/// It default id is `0`
#[derive(Debug, Clone, Copy, Default)]
pub struct NoneCompressor;

impl CompressorId for NoneCompressor {
    const ID: u8 = 0;
}

impl Compressor for NoneCompressor {
    fn encode(&self, block: Vec<u8>) -> crate::Result<Vec<u8>> {
        Ok(block)
    }

    fn decode(&self, block: Vec<u8>) -> crate::Result<Vec<u8>> {
        Ok(block)
    }
}

/// A compressor that compress data with Google's Snappy
///
/// It default id is `1`
#[derive(Debug, Clone, Copy, Default)]
pub struct SnappyCompressor;

impl CompressorId for SnappyCompressor {
    const ID: u8 = 1;
}

impl Compressor for SnappyCompressor {
    fn encode(&self, block: Vec<u8>) -> crate::Result<Vec<u8>> {
        Ok(snap::raw::Encoder::new().compress_vec(&block)?)
    }

    fn decode(&self, block: Vec<u8>) -> crate::Result<Vec<u8>> {
        Ok(snap::raw::Decoder::new().decompress_vec(&block)?)
    }
}
