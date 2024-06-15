const CRC: crc::Crc<u32, crc::Table<1>> = crc::Crc::<u32, crc::Table<1>>::new(&crc::CRC_32_ISCSI);

pub(crate) fn crc32(data: impl AsRef<[u8]>) -> u32 {
    let mut digest = CRC.digest();
    digest.update(data.as_ref());
    digest.finalize()
}

pub(crate) fn digest() -> crc::Digest<'static, u32> {
    CRC.digest()
}
