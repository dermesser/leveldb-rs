pub(crate) fn crc32(data: impl AsRef<[u8]>) -> u32 {
    crc32c::crc32c(data.as_ref())
}

pub(crate) struct Digest {
    hasher: crc32c::Crc32cHasher,
}

impl Digest {
    pub fn update(&mut self, data: &[u8]) {
        use std::hash::Hasher;
        self.hasher.write(data);
    }

    pub fn finalize(self) -> u32 {
        use std::hash::Hasher;
        self.hasher.finish() as u32
    }
}

pub(crate) fn digest() -> Digest {
    Digest {
        hasher: crc32c::Crc32cHasher::new(0),
    }
}
