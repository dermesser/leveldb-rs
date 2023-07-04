pub trait Compressor {
    fn encode(&self, block: Vec<u8>) -> crate::Result<Vec<u8>>;

    fn decode(&self, block: Vec<u8>) -> crate::Result<Vec<u8>>;
}

pub trait CompressorId {
    const ID: u8;
}

#[derive(Debug, Clone, Copy, Default)]
pub struct NoneCompressor;

impl NoneCompressor {
    pub fn new() -> Box<dyn Compressor> {
        Box::new(Self)
    }
}

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

#[derive(Debug, Clone, Copy, Default)]
pub struct SnappyCompressor;

impl SnappyCompressor {
    pub fn new() -> Box<dyn Compressor> {
        Box::new(Self)
    }
}

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
