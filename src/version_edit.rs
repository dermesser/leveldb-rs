use types::{FileMetaData, SequenceNumber, Status};
use key_types::InternalKey;

use integer_encoding::VarIntWriter;
use integer_encoding::VarIntReader;

use std::collections::HashSet;
use std::io::Write;
use std::io::Read;

#[derive(PartialEq, Debug, Clone)]
struct CompactionPointer {
    level: isize,
    // This key is in InternalKey format.
    key: Vec<u8>,
}

enum EditTag {
    Comparator = 1,
    LogNumber = 2,
    NextFileNumber = 3,
    LastSequence = 4,
    CompactPointer = 5,
    DeletedFile = 6,
    NewFile = 7,
    // sic!
    PrevLogNumber = 9,
}

fn tag_to_enum(t: u32) -> Option<EditTag> {
    match t {
        1 => Some(EditTag::Comparator),
        2 => Some(EditTag::LogNumber),
        3 => Some(EditTag::NextFileNumber),
        4 => Some(EditTag::LastSequence),
        5 => Some(EditTag::CompactPointer),
        6 => Some(EditTag::DeletedFile),
        7 => Some(EditTag::NewFile),
        9 => Some(EditTag::PrevLogNumber),
        _ => None,
    }
}

fn read_length_prefixed<R: Read>(reader: &mut R) -> Result<Vec<u8>, Status> {
    if let Ok(klen) = reader.read_varint() {
        let mut keybuf = Vec::new();
        keybuf.resize(klen, 0);

        if let Ok(l) = reader.read(&mut keybuf) {
            if l != klen {
                return Err(Status::IOError("Couldn't read full key".to_string()));
            }
            return Ok(keybuf);
        } else {
            return Err(Status::IOError("Couldn't read key".to_string()));
        }
    } else {
        return Err(Status::IOError("Couldn't read key length".to_string()));
    }
}

/// Manages changes to the set of managed SSTables and logfiles.
pub struct VersionEdit {
    comparator: Option<String>,
    log_number: Option<u64>,
    prev_log_number: Option<u64>,
    next_file_number: Option<u64>,
    last_seq: Option<SequenceNumber>,

    compaction_ptrs: Vec<CompactionPointer>,
    deleted: HashSet<(isize, u64)>,
    new_files: Vec<(isize, FileMetaData)>,
}

impl VersionEdit {
    pub fn new() -> VersionEdit {
        VersionEdit {
            comparator: None,
            log_number: None,
            prev_log_number: None,
            next_file_number: None,
            last_seq: None,
            compaction_ptrs: Vec::with_capacity(8),
            deleted: HashSet::with_capacity(8),
            new_files: Vec::with_capacity(8),
        }
    }

    pub fn clear(&mut self) {
        *self = VersionEdit::new();
    }

    pub fn add_file(&mut self, level: isize, file: FileMetaData) {
        self.new_files.push((level, file.clone()))
    }

    pub fn delete_file(&mut self, level: isize, file_num: u64) {
        self.deleted.insert((level, file_num));
    }

    pub fn set_comparator_name(&mut self, name: String) {
        self.comparator = Some(name)
    }

    pub fn set_log_num(&mut self, num: u64) {
        self.log_number = Some(num)
    }

    pub fn set_prev_log_num(&mut self, num: u64) {
        self.prev_log_number = Some(num)
    }

    pub fn set_next_file(&mut self, num: u64) {
        self.next_file_number = Some(num)
    }

    pub fn set_compact_pointer(&mut self, level: isize, key: InternalKey) {
        self.compaction_ptrs.push(CompactionPointer {
            level: level,
            key: Vec::from(key),
        })
    }

    /// Encode this VersionEdit into a buffer.
    pub fn encode(&self) -> Vec<u8> {
        let mut buf = Vec::with_capacity(256);

        if let Some(ref cmp) = self.comparator {
            buf.write_varint(EditTag::Comparator as u32).is_ok(); // swallow errors, because it's a pure in-memory write
            // data is prefixed by a varint32 describing the length of the following chunk
            buf.write_varint(cmp.len()).is_ok();
            buf.write(cmp.as_bytes()).is_ok();
        }

        if let Some(lognum) = self.log_number {
            buf.write_varint(EditTag::LogNumber as u32).is_ok();
            buf.write_varint(lognum).is_ok();
        }

        if let Some(plognum) = self.prev_log_number {
            buf.write_varint(EditTag::PrevLogNumber as u32).is_ok();
            buf.write_varint(plognum).is_ok();
        }

        if let Some(nfn) = self.next_file_number {
            buf.write_varint(EditTag::NextFileNumber as u32).is_ok();
            buf.write_varint(nfn).is_ok();
        }

        if let Some(ls) = self.last_seq {
            buf.write_varint(EditTag::LastSequence as u32).is_ok();
            buf.write_varint(ls).is_ok();
        }

        for cptr in self.compaction_ptrs.iter() {
            buf.write_varint(EditTag::CompactPointer as u32).is_ok();
            buf.write_varint(cptr.level).is_ok();
            buf.write_varint(cptr.key.len()).is_ok();
            buf.write(cptr.key.as_ref()).is_ok();
        }

        for df in self.deleted.iter() {
            buf.write_varint(EditTag::DeletedFile as u32).is_ok();
            buf.write_varint(df.0).is_ok();
            buf.write_varint(df.1).is_ok();
        }

        for nf in self.new_files.iter() {
            buf.write_varint(EditTag::NewFile as u32).is_ok();
            buf.write_varint(nf.0).is_ok();
            buf.write_varint(nf.1.num).is_ok();
            buf.write_varint(nf.1.size).is_ok();

            buf.write_varint(nf.1.smallest.len()).is_ok();
            buf.write(nf.1.smallest.as_ref()).is_ok();
            buf.write_varint(nf.1.largest.len()).is_ok();
            buf.write(nf.1.largest.as_ref()).is_ok();
        }

        buf
    }

    pub fn decode_from(src: &[u8]) -> Result<VersionEdit, Status> {
        let mut reader = src;
        let mut ve = VersionEdit::new();

        while let Ok(tag) = reader.read_varint::<u32>() {
            if let Some(tag) = tag_to_enum(tag) {
                match tag {
                    EditTag::Comparator => {
                        if let Ok(buf) = read_length_prefixed(&mut reader) {
                            if let Ok(c) = String::from_utf8(buf) {
                                ve.comparator = Some(c);
                            } else {
                                return Err(Status::Corruption("Bad comparator encoding"
                                    .to_string()));
                            }
                        } else {
                            return Err(Status::IOError("Couldn't read comparator name"
                                .to_string()));
                        }
                    }

                    EditTag::LogNumber => {
                        if let Ok(ln) = reader.read_varint() {
                            ve.log_number = Some(ln);
                        } else {
                            return Err(Status::IOError("Couldn't read lognumber".to_string()));
                        }
                    }

                    EditTag::NextFileNumber => {
                        if let Ok(nfn) = reader.read_varint() {
                            ve.next_file_number = Some(nfn);
                        } else {
                            return Err(Status::IOError("Couldn't read next_file_number"
                                .to_string()));
                        }
                    }

                    EditTag::LastSequence => {
                        if let Ok(ls) = reader.read_varint() {
                            ve.last_seq = Some(ls);
                        } else {
                            return Err(Status::IOError("Couldn't read last_sequence".to_string()));
                        }
                    }

                    EditTag::CompactPointer => {
                        // Monads by indentation...
                        if let Ok(lvl) = reader.read_varint() {
                            if let Ok(key) = read_length_prefixed(&mut reader) {
                                ve.compaction_ptrs.push(CompactionPointer {
                                    level: lvl,
                                    key: key,
                                });
                            } else {
                                return Err(Status::IOError("Couldn't read key".to_string()));
                            }
                        } else {
                            return Err(Status::IOError("Couldn't read level".to_string()));
                        }
                    }

                    EditTag::DeletedFile => {
                        if let Ok(lvl) = reader.read_varint() {
                            if let Ok(num) = reader.read_varint() {
                                ve.deleted.insert((lvl, num));
                            } else {
                                return Err(Status::IOError("Couldn't read file num".to_string()));
                            }
                        } else {
                            return Err(Status::IOError("Couldn't read level".to_string()));
                        }
                    }

                    EditTag::NewFile => {
                        if let Ok(lvl) = reader.read_varint() {
                            if let Ok(num) = reader.read_varint() {
                                if let Ok(size) = reader.read_varint() {
                                    if let (Ok(smallest), Ok(largest)) =
                                           (read_length_prefixed(&mut reader),
                                            read_length_prefixed(&mut reader)) {
                                        ve.new_files.push((lvl,
                                                           FileMetaData {
                                            num: num,
                                            size: size,
                                            smallest: smallest,
                                            largest: largest,
                                            allowed_seeks: 0,
                                        }))
                                    } else {
                                        return Err(Status::IOError("Couldn't read \
                                                                    smallest/largest keys"
                                            .to_string()));
                                    }
                                } else {
                                    return Err(Status::IOError("Couldn't read file size"
                                        .to_string()));
                                }
                            } else {
                                return Err(Status::IOError("Couldn't read file num".to_string()));
                            }
                        } else {
                            return Err(Status::IOError("Couldn't read file level".to_string()));
                        }
                    }

                    EditTag::PrevLogNumber => {
                        if let Ok(pln) = reader.read_varint() {
                            ve.prev_log_number = Some(pln);
                        } else {
                            return Err(Status::IOError("Couldn't read prev_log_number"
                                .to_string()));
                        }
                    }
                }
            } else {
                println!("{}", tag);
                return Err(Status::Corruption("Invalid tag number".to_string()));
            }
        }

        Ok(ve)
    }
}

#[cfg(test)]
mod tests {
    use super::CompactionPointer;
    use super::VersionEdit;

    use types::FileMetaData;

    #[test]
    fn test_version_edit_encode_decode() {
        let mut ve = VersionEdit::new();

        ve.set_comparator_name("abcdef".to_string());
        ve.set_log_num(123);
        ve.set_next_file(456);
        ve.set_prev_log_num(789);
        ve.set_compact_pointer(0, &[0, 1, 2]);
        ve.set_compact_pointer(1, &[3, 4, 5]);
        ve.set_compact_pointer(2, &[6, 7, 8]);
        ve.add_file(0,
                    FileMetaData {
                        allowed_seeks: 12345,
                        num: 901,
                        size: 234,
                        smallest: vec![5, 6, 7],
                        largest: vec![8, 9, 0],
                    });
        ve.delete_file(1, 132);

        let encoded = ve.encode();

        let decoded = VersionEdit::decode_from(encoded.as_ref()).unwrap();

        assert_eq!(decoded.comparator, Some("abcdef".to_string()));
        assert_eq!(decoded.log_number, Some(123));
        assert_eq!(decoded.next_file_number, Some(456));
        assert_eq!(decoded.prev_log_number, Some(789));
        assert_eq!(decoded.compaction_ptrs.len(), 3);
        assert_eq!(decoded.compaction_ptrs[0],
                   CompactionPointer {
                       level: 0,
                       key: vec![0, 1, 2],
                   });
        assert_eq!(decoded.compaction_ptrs[1],
                   CompactionPointer {
                       level: 1,
                       key: vec![3, 4, 5],
                   });
        assert_eq!(decoded.compaction_ptrs[2],
                   CompactionPointer {
                       level: 2,
                       key: vec![6, 7, 8],
                   });
        assert_eq!(decoded.new_files.len(), 1);
        assert_eq!(decoded.new_files[0],
                   (0,
                    FileMetaData {
                       allowed_seeks: 0,
                       num: 901,
                       size: 234,
                       smallest: vec![5, 6, 7],
                       largest: vec![8, 9, 0],
                   }));
        assert_eq!(decoded.deleted.len(), 1);
        assert!(decoded.deleted.contains(&(1, 132)));
    }
}
