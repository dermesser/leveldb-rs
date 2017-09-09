
use cmp::{Cmp, InternalKeyCmp};
use key_types::{parse_internal_key, InternalKey, UserKey};
use log::LogWriter;
use options::Options;
use table_cache::TableCache;
use types::{share, NUM_LEVELS, FileNum, Shared};
use version::{FileMetaHandle, Version};
use version_edit::VersionEdit;

use std::cmp::Ordering;
use std::collections::HashSet;
use std::io::Write;
use std::rc::Rc;

struct Compaction {
    level: usize,
    max_file_size: usize,
    input_version: Option<Shared<Version>>,
    level_ixs: [usize; NUM_LEVELS],
    cmp: Rc<Box<Cmp>>,

    // "parent" inputs from level and level+1.
    inputs: [Vec<FileMetaHandle>; 2],
    grandparent_ix: usize,
    // remaining inputs from level+2..NUM_LEVELS
    grandparents: Option<Vec<FileMetaHandle>>,
    overlapped_bytes: usize,
    seen_key: bool,
    pub edit: VersionEdit,
}

impl Compaction {
    // Note: opt.cmp should be the user-supplied or default comparator (not an InternalKeyCmp).
    fn new(opt: &Options, level: usize) -> Compaction {
        Compaction {
            level: level,
            max_file_size: opt.max_file_size,
            input_version: None,
            level_ixs: Default::default(),
            cmp: opt.cmp.clone(),

            inputs: Default::default(),
            grandparent_ix: 0,
            grandparents: Default::default(),
            overlapped_bytes: 0,
            seen_key: false,
            edit: VersionEdit::new(),
        }
    }

    /// add_input_deletions marks the current input files as deleted in the inner VersionEdit.
    fn add_input_deletions(&mut self) {
        for parent in 0..2 {
            for f in &self.inputs[parent] {
                self.edit.delete_file(self.level + parent, f.borrow().num);
            }
        }
    }

    fn input(&self, parent: usize, i: usize) -> FileMetaHandle {
        assert!(parent < 2);
        assert!(i < self.inputs[parent].len());

        self.inputs[parent][i].clone()
    }

    /// is_base_level_for checks whether the given key may exist in levels higher than this
    /// compaction's level plus 2. I.e., whether the levels for this compaction are the last ones
    /// to contain the key.
    fn is_base_level_for<'a>(&mut self, k: UserKey<'a>) -> bool {
        if let Some(ref inp_version) = self.input_version {
            for level in self.level + 2..NUM_LEVELS {
                let files = &inp_version.borrow().files[level];
                while self.level_ixs[level] < files.len() {
                    let f = files[self.level_ixs[level]].borrow();
                    if self.cmp.cmp(k, parse_internal_key(&f.largest).2) <= Ordering::Equal {
                        if self.cmp.cmp(k, parse_internal_key(&f.smallest).2) >= Ordering::Equal {
                            // key is in this file's range, so this is not the base level.
                            return false;
                        }
                        break;
                    }
                    self.level_ixs[level] += 1;
                }
            }
            true
        } else {
            unimplemented!()
        }
    }

    fn num_inputs(&self, parent: usize) -> usize {
        assert!(parent < 2);
        self.inputs[parent].len()
    }

    fn is_trivial_move(&self) -> bool {
        let inputs_size = self.grandparents
            .as_ref()
            .unwrap_or(&vec![])
            .iter()
            .fold(0, |a, f| a + f.borrow().size);
        self.num_inputs(0) == 1 && self.num_inputs(1) == 0 && inputs_size < 10 * self.max_file_size
    }

    fn should_stop_before<'a>(&mut self, k: InternalKey<'a>) -> bool {
        assert!(self.grandparents.is_some());
        let grandparents = self.grandparents.as_ref().unwrap();
        let icmp = InternalKeyCmp(self.cmp.clone());
        while self.grandparent_ix < grandparents.len() &&
              icmp.cmp(k, &grandparents[self.grandparent_ix].borrow().largest) == Ordering::Greater {
            if self.seen_key {
                self.overlapped_bytes += grandparents[self.grandparent_ix].borrow().size;
            }
            self.grandparent_ix += 1;
        }
        self.seen_key = true;

        if self.overlapped_bytes > 10 * self.max_file_size {
            self.overlapped_bytes = 0;
            true
        } else {
            false
        }
    }
}

/// VersionSet managed the various versions that are live within a database. A single version
/// contains references to the files on disk as they were at a certain point.
pub struct VersionSet {
    dbname: String,
    opt: Options,
    cmp: InternalKeyCmp,
    cache: Shared<TableCache>,

    next_file_num: u64,
    manifest_num: u64,
    last_seq: u64,
    log_num: u64,
    prev_log_num: u64,

    log: Option<LogWriter<Box<Write>>>,
    versions: Vec<Shared<Version>>,
    current: Option<Shared<Version>>,
}

impl VersionSet {
    // Note: opt.cmp should not contain an InternalKeyCmp at this point, but instead the default or
    // user-supplied one.
    pub fn new(db: String, opt: Options, cache: Shared<TableCache>) -> VersionSet {
        VersionSet {
            dbname: db,
            cmp: InternalKeyCmp(opt.cmp.clone()),
            opt: opt,
            cache: cache,

            next_file_num: 2,
            manifest_num: 2,
            last_seq: 0,
            log_num: 0,
            prev_log_num: 0,

            log: None,
            versions: vec![],
            current: None,
        }
    }

    fn live_files(&self) -> Vec<FileNum> {
        let mut files = HashSet::new();
        for version in &self.versions {
            for level in 0..NUM_LEVELS {
                for file in &version.borrow().files[level] {
                    files.insert(file.borrow().num);
                }
            }
        }
        files.into_iter().collect()
    }

    fn add_version(&mut self, v: Version) {
        let sv = share(v);
        self.current = Some(sv.clone());
        self.versions.push(sv);
    }

    fn approximate_offset<'a>(&self, v: Shared<Version>, key: InternalKey<'a>) -> usize {
        let mut offset = 0;
        for level in 0..NUM_LEVELS {
            for f in &v.borrow().files[level] {
                if self.opt.cmp.cmp(&f.borrow().largest, key) <= Ordering::Equal {
                    offset += f.borrow().size;
                } else if self.opt.cmp.cmp(&f.borrow().smallest, key) == Ordering::Greater {
                    // In higher levels, files are sorted; we don't need to search further.
                    if level > 0 {
                        break;
                    }
                } else {
                    if let Ok(tbl) = self.cache.borrow_mut().get_table(f.borrow().num) {
                        offset += tbl.approx_offset_of(key);
                    }
                }
            }
        }
        offset
    }
}
