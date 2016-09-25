
use types::{ValueType, SequenceNumber};

use integer_encoding::{FixedInt, VarInt};

// The following typedefs are used to distinguish between the different key formats used internally
// by different modules.

/// A MemtableKey consists of the following elements: [keylen, key, tag, (vallen, value)] where
/// keylen is a varint32 encoding the length of key+tag. tag is a fixed 8 bytes segment encoding
/// the entry type and the sequence number. vallen and value are optional components at the end.
pub type MemtableKey<'a> = &'a [u8];

/// A UserKey is the actual key supplied by the calling application, without any internal
/// decorations.
pub type UserKey<'a> = &'a [u8];

/// An InternalKey consists of [key, tag], so it's basically a MemtableKey without the initial
/// length specification. This type is used as item type of MemtableIterator, and as the key
/// type of tables.
pub type InternalKey<'a> = &'a [u8];

/// A LookupKey is the first part of a memtable key, consisting of [keylen: varint32, key: *u8,
/// tag: u64]
/// keylen is the length of key plus 8 (for the tag; this for LevelDB compatibility)
pub struct LookupKey {
    key: Vec<u8>,
    key_offset: usize,
}

impl LookupKey {
    #[allow(unused_assignments)]
    pub fn new(k: &[u8], s: SequenceNumber) -> LookupKey {
        let mut key = Vec::with_capacity(k.len() + k.len().required_space() +
                                         <u64 as FixedInt>::required_space());
        let internal_keylen = k.len() + 8;
        let mut i = 0;

        key.reserve(internal_keylen.required_space() + internal_keylen);

        key.resize(internal_keylen.required_space(), 0);
        i += internal_keylen.encode_var(&mut key[i..]);

        key.extend_from_slice(k);
        i += k.len();

        key.resize(i + <u64 as FixedInt>::required_space(), 0);
        (s << 8 | ValueType::TypeValue as u64).encode_fixed(&mut key[i..]);
        i += <u64 as FixedInt>::required_space();

        LookupKey {
            key: key,
            key_offset: k.len().required_space(),
        }
    }

    // Returns full key
    pub fn memtable_key<'a>(&'a self) -> MemtableKey<'a> {
        self.key.as_slice()
    }

    // Returns only key
    pub fn user_key<'a>(&'a self) -> UserKey<'a> {
        &self.key[self.key_offset..self.key.len() - 8]
    }

    // Returns key+tag
    pub fn internal_key<'a>(&'a self) -> InternalKey<'a> {
        &self.key[self.key_offset..]
    }
}

/// Parses a tag into (type, sequence number)
pub fn parse_tag(tag: u64) -> (u8, u64) {
    let seq = tag >> 8;
    let typ = tag & 0xff;
    (typ as u8, seq)
}

/// A memtable key is a bytestring containing (keylen, key, tag, vallen, val). This function
/// builds such a key. It's called key because the underlying Map implementation will only be
/// concerned with keys; the value field is not used (instead, the value is encoded in the key,
/// and for lookups we just search for the next bigger entry).
/// keylen is the length of key + 8 (to account for the tag)
pub fn build_memtable_key(key: &[u8], value: &[u8], t: ValueType, seq: SequenceNumber) -> Vec<u8> {
    // We are using the original LevelDB approach here -- encoding key and value into the
    // key that is used for insertion into the SkipMap.
    // The format is: [key_size: varint32, key_data: [u8], flags: u64, value_size: varint32,
    // value_data: [u8]]

    let mut i = 0;
    let keysize = key.len() + 8;
    let valsize = value.len();

    let mut buf = Vec::with_capacity(keysize + valsize + keysize.required_space() +
                                     valsize.required_space() +
                                     <u64 as FixedInt>::required_space());
    buf.resize(keysize.required_space(), 0);
    i += keysize.encode_var(&mut buf[i..]);

    buf.extend(key.iter());
    i += key.len();

    let flag = (t as u64) | (seq << 8);
    buf.resize(i + <u64 as FixedInt>::required_space(), 0);
    flag.encode_fixed(&mut buf[i..]);
    i += <u64 as FixedInt>::required_space();

    buf.resize(i + valsize.required_space(), 0);
    i += valsize.encode_var(&mut buf[i..]);

    buf.extend(value.iter());
    i += value.len();

    assert_eq!(i, buf.len());
    buf
}

/// Parses a memtable key and returns  (keylen, key offset, tag, vallen, val offset).
/// If the key only contains (keylen, key, tag), the vallen and val offset return values will be
/// meaningless.
pub fn parse_memtable_key<'a>(mkey: MemtableKey<'a>) -> (usize, usize, u64, usize, usize) {
    let (keylen, mut i): (usize, usize) = VarInt::decode_var(&mkey);

    let keyoff = i;
    i += keylen - 8;

    if mkey.len() > i {
        let tag = FixedInt::decode_fixed(&mkey[i..i + 8]);
        i += 8;

        let (vallen, j): (usize, usize) = VarInt::decode_var(&mkey[i..]);

        i += j;

        let valoff = i;

        return (keylen - 8, keyoff, tag, vallen, valoff);
    } else {
        return (keylen - 8, keyoff, 0, 0, 0);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_memtable_lookupkey() {
        use integer_encoding::VarInt;

        let lk1 = LookupKey::new("abcde".as_bytes(), 123);
        let lk2 = LookupKey::new("xyabxy".as_bytes(), 97);

        // Assert correct allocation strategy
        assert_eq!(lk1.key.len(), 14);
        assert_eq!(lk1.key.capacity(), 14);

        assert_eq!(lk1.user_key(), "abcde".as_bytes());
        assert_eq!(u32::decode_var(lk1.memtable_key()), (13, 1));
        assert_eq!(lk2.internal_key(),
                   vec![120, 121, 97, 98, 120, 121, 1, 97, 0, 0, 0, 0, 0, 0].as_slice());
    }
}
