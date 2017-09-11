
use cmp::{Cmp, InternalKeyCmp};
use env::Env;
use error::{err, Status, StatusCode, Result};
use key_types::{parse_internal_key, InternalKey, LookupKey, UserKey};
use log::{LogWriter, LogReader};
use options::Options;
use table_cache::TableCache;
use types::{parse_file_name, share, NUM_LEVELS, FileNum, FileType, Shared};
use version::{FileMetaHandle, Version};
use version_edit::VersionEdit;

use std::cmp::Ordering;
use std::collections::HashSet;
use std::io::Write;
use std::ops::Deref;
use std::path::Path;
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
    fn new(opt: &Options, level: usize, input: Option<Shared<Version>>) -> Compaction {
        Compaction {
            level: level,
            max_file_size: opt.max_file_size,
            input_version: input,
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
        assert!(self.input_version.is_some());
        let inp_version = self.input_version.as_ref().unwrap();
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
    }

    fn num_inputs(&self, parent: usize) -> usize {
        assert!(parent < 2);
        self.inputs[parent].len()
    }

    fn is_trivial_move(&self) -> bool {
        let inputs_size = total_size(self.grandparents.as_ref().unwrap().iter());
        self.num_inputs(0) == 1 && self.num_inputs(1) == 0 && inputs_size < 10 * self.max_file_size
    }

    fn should_stop_before<'a>(&mut self, k: InternalKey<'a>) -> bool {
        assert!(self.grandparents.is_some());
        let grandparents = self.grandparents.as_ref().unwrap();
        let icmp = InternalKeyCmp(self.cmp.clone());
        while self.grandparent_ix < grandparents.len() &&
              icmp.cmp(k, &grandparents[self.grandparent_ix].borrow().largest) ==
              Ordering::Greater {
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

    pub next_file_num: u64,
    pub manifest_num: u64,
    pub last_seq: u64,
    pub log_num: u64,
    pub prev_log_num: u64,

    versions: Vec<Shared<Version>>,
    current: Option<Shared<Version>>,
    compaction_ptrs: [Vec<u8>; NUM_LEVELS],

    descriptor_log: Option<LogWriter<Box<Write>>>,
}

impl VersionSet {
    // Note: opt.cmp should not contain an InternalKeyCmp at this point, but instead the default or
    // user-supplied one.
    pub fn new(db: String, opt: Options, cache: Shared<TableCache>) -> VersionSet {
        let v = share(Version::new(cache.clone(), opt.cmp.clone()));
        VersionSet {
            dbname: db,
            cmp: InternalKeyCmp(opt.cmp.clone()),
            opt: opt,
            cache: cache,

            next_file_num: 2,
            manifest_num: 0,
            last_seq: 0,
            log_num: 0,
            prev_log_num: 0,

            versions: vec![v.clone()],
            current: Some(v),
            compaction_ptrs: Default::default(),
            descriptor_log: None,
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

    fn current(&self) -> Option<Shared<Version>> {
        self.current.clone()
    }

    fn num_level_bytes(&self, l: usize) -> usize {
        assert!(l < NUM_LEVELS);
        assert!(self.current.is_some());
        total_size(self.current.as_ref().unwrap().borrow().files[l].iter())
    }
    fn num_level_files(&self, l: usize) -> usize {
        assert!(l < NUM_LEVELS);
        assert!(self.current.is_some());
        self.current.as_ref().unwrap().borrow().files[l].len()
    }

    fn add_version(&mut self, v: Version) {
        let sv = share(v);
        self.current = Some(sv.clone());
        self.versions.push(sv);
    }

    fn new_file_number(&mut self) -> FileNum {
        self.next_file_num += 1;
        self.next_file_num - 1
    }

    fn mark_file_number_used(&mut self, n: FileNum) {
        if self.next_file_num <= n {
            self.next_file_num = n + 1;
        }
    }

    fn approximate_offset<'a>(&self, v: &Shared<Version>, key: InternalKey<'a>) -> usize {
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

    fn compact_range<'a, 'b>(&mut self,
                             level: usize,
                             from: InternalKey<'a>,
                             to: InternalKey<'b>)
                             -> Option<Compaction> {
        assert!(self.current.is_some());
        let mut inputs =
            self.current.as_ref().unwrap().borrow().overlapping_inputs(level, from, to);
        if inputs.is_empty() {
            return None;
        }

        if level > 0 {
            let mut total = 0;
            for i in 0..inputs.len() {
                total += inputs[i].borrow().size;
                if total > self.opt.max_file_size {
                    inputs.truncate(i + 1);
                    break;
                }
            }
        }

        let mut c = Compaction::new(&self.opt, level, self.current.clone());
        c.inputs[0] = inputs;
        self.setup_other_inputs(&mut c);
        Some(c)
    }

    fn setup_other_inputs(&mut self, compaction: &mut Compaction) {
        let level = compaction.level;
        let (smallest, mut largest) = get_range(&self.cmp, compaction.inputs[0].iter());

        assert!(self.current.is_some());
        // Set up level+1 inputs.
        compaction.inputs[1] = self.current
            .as_ref()
            .unwrap()
            .borrow()
            .overlapping_inputs(level + 1, &smallest, &largest);

        let (mut allstart, mut alllimit) =
            get_range(&self.cmp,
                      compaction.inputs[0].iter().chain(compaction.inputs[1].iter()));

        // Check if we can add more inputs in the current level without having to compact more
        // inputs from level+1.
        if !compaction.inputs[1].is_empty() {
            let expanded0 = self.current
                .as_ref()
                .unwrap()
                .borrow()
                .overlapping_inputs(level, &allstart, &alllimit);
            let inputs1_size = total_size(compaction.inputs[1].iter());
            let expanded0_size = total_size(expanded0.iter());
            // ...if we picked up more files in the current level, and the total size is acceptable
            if expanded0.len() > compaction.num_inputs(0) &&
               (inputs1_size + expanded0_size) < 25 * self.opt.max_file_size {
                let (new_start, new_limit) = get_range(&self.cmp, expanded0.iter());
                let expanded1 = self.current
                    .as_ref()
                    .unwrap()
                    .borrow()
                    .overlapping_inputs(level + 1, &new_start, &new_limit);
                if expanded1.len() == compaction.num_inputs(1) {
                    // TODO: Log this.

                    // smallest = new_start;
                    largest = new_limit;
                    compaction.inputs[0] = expanded0;
                    compaction.inputs[1] = expanded1;
                    let (newallstart, newalllimit) =
                        get_range(&self.cmp,
                                  compaction.inputs[0].iter().chain(compaction.inputs[1].iter()));
                    allstart = newallstart;
                    alllimit = newalllimit;
                }
            }
        }

        // Set the list of grandparent (l+2) inputs to the files overlapped by the current overall
        // range.
        if level + 2 < NUM_LEVELS {
            let grandparents = self.current
                .as_ref()
                .unwrap()
                .borrow()
                .overlapping_inputs(level + 2, &allstart, &alllimit);
            compaction.grandparents = Some(grandparents);
        }

        // TODO: add log statement about compaction.

        compaction.edit.set_compact_pointer(level, &largest);
        self.compaction_ptrs[level] = largest;
    }

    /// write_snapshot writes the current version, with all files, to the manifest.
    fn write_snapshot(&mut self) -> Result<usize> {
        assert!(self.descriptor_log.is_some());

        let mut edit = VersionEdit::new();
        edit.set_comparator_name(self.opt.cmp.id());

        // Save compaction pointers.
        for level in 0..NUM_LEVELS {
            if !self.compaction_ptrs[level].is_empty() {
                edit.set_compact_pointer(level, &self.compaction_ptrs[level]);
            }
        }

        let current = self.current.as_ref().unwrap().borrow();
        // Save files.
        for level in 0..NUM_LEVELS {
            let fs = &current.files[level];
            for f in fs {
                edit.add_file(level, f.borrow().clone());
            }
        }
        self.descriptor_log.as_mut().unwrap().add_record(&edit.encode())
    }

    /// log_and_apply merges the given edit with the current state and generates a new version. It
    /// writes the VersionEdit to the manifest.
    fn log_and_apply(&mut self, mut edit: VersionEdit) -> Result<()> {
        assert!(self.current.is_some());

        if edit.log_number.is_none() {
            edit.set_log_num(self.log_num);
        } else {
            assert!(edit.log_number.unwrap() >= self.log_num);
            assert!(edit.log_number.unwrap() < self.next_file_num);
        }
        if edit.prev_log_number.is_none() {
            edit.set_prev_log_num(self.prev_log_num);
        }
        edit.set_next_file(self.next_file_num);
        edit.set_last_seq(self.last_seq);

        let mut v = Version::new(self.cache.clone(), self.opt.cmp.clone());
        {
            let mut builder = Builder::new();
            builder.apply(&edit, &mut self.compaction_ptrs);
            builder.save_to(&self.cmp, self.current.as_ref().unwrap(), &mut v);
        }
        self.finalize(&mut v);

        if self.descriptor_log.is_none() {
            let descname = format!("{}/MANIFEST-{:06}", self.dbname, self.manifest_num);
            edit.set_next_file(self.next_file_num);
            self.descriptor_log =
                Some(LogWriter::new(self.opt.env.open_writable_file(Path::new(&descname))?));
            self.write_snapshot()?;
        }

        let encoded = edit.encode();
        if let Some(ref mut lw) = self.descriptor_log {
            lw.add_record(&encoded)?;
            lw.flush()?;
        }
        set_current_file(&self.opt.env, &self.dbname, self.manifest_num)?;

        self.add_version(v);
        // log_number was set above.
        self.log_num = edit.log_number.unwrap();
        self.prev_log_num = edit.prev_log_number.unwrap();

        // TODO: Roll back written files if something went wrong.
        Ok(())
    }

    fn finalize(&self, v: &mut Version) {
        let mut best_lvl = None;
        let mut best_score = None;

        for l in 0..NUM_LEVELS - 1 {
            let score: f64;
            if l == 0 {
                score = v.files[l].len() as f64 / 4.0;
            } else {
                let mut max_bytes = 10.0 * f64::from(1 << 20);
                for _ in 0..l - 1 {
                    max_bytes *= 10.0;
                }
                score = total_size(v.files[l].iter()) as f64 / max_bytes;
            }
            if let Some(ref mut b) = best_score {
                if *b < score {
                    *b = score;
                    best_lvl = Some(l);
                }
            } else {
                best_score = Some(score);
                best_lvl = Some(l);
            }
        }
        v.compaction_score = best_score;
        v.compaction_level = best_lvl;
    }

    /// recover recovers the state of a LevelDB instance from the files on disk. If recover()
    /// returns true, proceed with calling log_and_apply().
    fn recover(&mut self) -> Result<bool> {
        assert!(self.current.is_some());

        let mut current = String::new();
        {
            let mut f =
                self.opt.env.open_sequential_file(Path::new(&format!("{}/CURRENT", self.dbname)))?;
            f.read_to_string(&mut current)?;
        }
        if current.is_empty() || !current.ends_with('\n') {
            return err(StatusCode::Corruption,
                       "current file is empty or has no newline");
        }
        {
            let len = current.len();
            current.truncate(len - 1);
        }

        let descfilename = format!("{}/{}", self.dbname, current);
        let mut builder = Builder::new();
        {
            let mut descfile = self.opt.env.open_sequential_file(Path::new(&descfilename))?;
            let mut logreader = LogReader::new(&mut descfile,
                                               // checksum=
                                               true,
                                               // offset=
                                               0);

            let mut log_number = None;
            let mut prev_log_number = None;
            let mut next_file_number = None;
            let mut last_seq = None;

            let mut buf = Vec::new();
            while let Ok(size) = logreader.read(&mut buf) {
                if size == 0 {
                    break;
                }
                let edit = VersionEdit::decode_from(&buf)?;
                builder.apply(&edit, &mut self.compaction_ptrs);
                if let Some(ln) = edit.log_number {
                    log_number = Some(ln);
                }
                if let Some(pln) = edit.prev_log_number {
                    prev_log_number = Some(pln);
                }
                if let Some(nfn) = edit.next_file_number {
                    next_file_number = Some(nfn);
                }
                if let Some(ls) = edit.last_seq {
                    last_seq = Some(ls);
                }
            }

            if let Some(ln) = log_number {
                self.log_num = ln;
                self.mark_file_number_used(ln);
            } else {
                return err(StatusCode::Corruption,
                           "no meta-lognumber entry in descriptor");
            }
            if let Some(nfn) = next_file_number {
                self.next_file_num = nfn + 1;
            } else {
                return err(StatusCode::Corruption,
                           "no meta-next-file entry in descriptor");
            }
            if let Some(ls) = last_seq {
                self.last_seq = ls;
            } else {
                return err(StatusCode::Corruption,
                           "no last-sequence entry in descriptor");
            }

            if let Some(pln) = prev_log_number {
                self.prev_log_num = pln + 1;
                self.mark_file_number_used(pln + 1);
            } else {
                self.prev_log_num = 0;
                self.mark_file_number_used(1);
            }
        }

        let mut v = Version::new(self.cache.clone(), self.opt.cmp.clone());
        builder.save_to(&self.cmp, self.current.as_ref().unwrap(), &mut v);
        self.finalize(&mut v);
        self.add_version(v);
        self.manifest_num = self.next_file_num - 1;

        Ok(self.reuse_manifest(&descfilename, &current))
    }

    /// reuse_manifest checks whether the current manifest can be reused.
    fn reuse_manifest(&mut self, current_manifest_path: &str, current_manifest_base: &str) -> bool {
        // The original doesn't reuse manifests; we do.
        if let Ok((num, typ)) = parse_file_name(current_manifest_base) {
            if typ != FileType::Descriptor {
                return false;
            }
            if let Ok(size) = self.opt.env.size_of(Path::new(current_manifest_path)) {
                if size > self.opt.max_file_size {
                    return false;
                }
            } else {
                return false;
            }

            assert!(self.descriptor_log.is_none());
            if let Ok(f) = self.opt.env.open_appendable_file(Path::new(current_manifest_path)) {
                // TODO: Log this.
                self.descriptor_log = Some(LogWriter::new(f));
                self.manifest_num = num;
                return true;
            } else {
                // TODO: Log this.
                return false;
            }
        }
        false
    }
}

struct Builder {
    // (added, deleted) files per level.
    deleted: [Vec<FileNum>; NUM_LEVELS],
    added: [Vec<FileMetaHandle>; NUM_LEVELS],
}

impl Builder {
    fn new() -> Builder {
        Builder {
            deleted: Default::default(),
            added: Default::default(),
        }
    }

    fn apply(&mut self, edit: &VersionEdit, compaction_ptrs: &mut [Vec<u8>; NUM_LEVELS]) {
        for c in edit.compaction_ptrs.iter() {
            compaction_ptrs[c.level] = c.key.clone();
        }
        for &(level, num) in edit.deleted.iter() {
            self.deleted[level].push(num);
        }
        for &(level, ref f) in edit.new_files.iter() {
            let mut f = f.clone();
            f.allowed_seeks = f.size / 16384;
            if f.allowed_seeks < 100 {
                f.allowed_seeks = 100;
            }
            for i in 0..self.deleted[level].len() {
                if self.deleted[level][i] == f.num {
                    self.deleted[level].swap_remove(i);
                }
            }
            self.added[level].push(share(f));
        }
    }

    fn maybe_add_file(&mut self,
                      cmp: &InternalKeyCmp,
                      v: &mut Version,
                      level: usize,
                      f: FileMetaHandle) {
        // Only add file if it's not already deleted.
        if self.deleted[level].iter().any(|d| *d == f.borrow().num) {
            return;
        }
        {
            let files = &v.files[level];
            if level > 0 && !files.is_empty() {
                // File must be after last file in level.
                assert_eq!(cmp.cmp(&files[files.len() - 1].borrow().largest,
                                   &f.borrow().smallest),
                           Ordering::Less);
            }
        }
        v.files[level].push(f);
    }

    fn save_to(&mut self, cmp: &InternalKeyCmp, base: &Shared<Version>, v: &mut Version) {
        for level in 0..NUM_LEVELS {
            sort_files_by_smallest(cmp, &mut self.added[level]);
            // The base version should already have sorted files.
            sort_files_by_smallest(cmp, &mut base.borrow_mut().files[level]);

            let added = self.added[level].clone();
            let basefiles = base.borrow().files[level].clone();
            v.files[level].reserve(basefiles.len() + self.added[level].len());

            let mut iadded = added.into_iter();
            let mut ibasefiles = basefiles.into_iter();
            let merged = merge_iters(&mut iadded,
                                     &mut ibasefiles,
                                     |a, b| cmp.cmp(&a.borrow().smallest, &b.borrow().smallest));
            for m in merged {
                self.maybe_add_file(cmp, v, level, m);
            }

            // Make sure that there is no overlap in higher levels.
            if level == 0 {
                continue;
            }
            for i in 1..v.files[level].len() {
                let (prev_end, this_begin) = (&v.files[level][i - 1].borrow().largest,
                                              &v.files[level][i].borrow().smallest);
                assert!(cmp.cmp(prev_end, this_begin) >= Ordering::Equal);
            }
        }
    }
}

fn set_current_file(env: &Box<Env>, dbname: &str, manifest_file_num: FileNum) -> Result<()> {
    let manifest_base = format!("MANIFEST-{:06}", manifest_file_num);
    let tempfile = format!("{}/{}.dbtmp", dbname, manifest_file_num);

    {
        let mut f = env.open_writable_file(Path::new(&tempfile))?;
        f.write(manifest_base.as_bytes())?;
        f.write("\n".as_bytes())?;
    }
    let currentfile = format!("{}/CURRENT", dbname);
    if let Err(e) = env.rename(Path::new(&tempfile), Path::new(&currentfile)) {
        // ignore error.
        env.delete(Path::new(&tempfile)).is_ok();
        return Err(Status::from(e));
    }
    Ok(())
}

/// sort_files_by_smallest sorts the list of files by the smallest keys of the files.
fn sort_files_by_smallest<C: Cmp>(cmp: &C, files: &mut Vec<FileMetaHandle>) {
    files.sort_by(|a, b| cmp.cmp(&a.borrow().smallest, &b.borrow().smallest))
}

/// merge_iters merges and collects the items from two sorted iterators.
fn merge_iters<Item,
               C: Fn(&Item, &Item) -> Ordering,
               I: Iterator<Item = Item>,
               J: Iterator<Item = Item>>
    (iter_a: &mut I,
     iter_b: &mut J,
     cmp: C)
     -> Vec<Item> {
    let mut a = iter_a.next();
    let mut b = iter_b.next();
    let mut out = vec![];
    while a.is_some() && b.is_some() {
        let ord = cmp(a.as_ref().unwrap(), b.as_ref().unwrap());
        if ord == Ordering::Less {
            out.push(a.unwrap());
            a = iter_a.next();
        } else {
            out.push(b.unwrap());
            b = iter_b.next();
        }
    }

    // Push cached elements.
    if let Some(a_) = a {
        out.push(a_);
    }
    if let Some(b_) = b {
        out.push(b_);
    }

    // Push remaining elements from either iterator.
    for a in iter_a {
        out.push(a);
    }
    for b in iter_b {
        out.push(b);
    }
    out
}

/// get_range returns the indices of the files within files that have the smallest lower bound
/// respectively the largest upper bound.
fn get_range<'a, C: Cmp, I: Iterator<Item = &'a FileMetaHandle>>(c: &C,
                                                                 files: I)
                                                                 -> (Vec<u8>, Vec<u8>) {
    let mut smallest = None;
    let mut largest = None;
    for f in files {
        if smallest.is_none() {
            smallest = Some(f.borrow().smallest.clone());
        }
        if largest.is_none() {
            largest = Some(f.borrow().largest.clone());
        }
        let f = f.borrow();
        if c.cmp(&f.smallest, smallest.as_ref().unwrap()) == Ordering::Less {
            smallest = Some(f.smallest.clone());
        }
        if c.cmp(&f.largest, largest.as_ref().unwrap()) == Ordering::Greater {
            largest = Some(f.largest.clone());
        }
    }
    (smallest.unwrap(), largest.unwrap())
}

fn total_size<'a, I: Iterator<Item = &'a FileMetaHandle>>(files: I) -> usize {
    files.fold(0, |a, f| a + f.borrow().size)
}

#[cfg(test)]
mod tests {
    use super::*;
    use cmp::DefaultCmp;
    use mem_env::MemEnv;
    use types::FileMetaData;
    use version::testutil::make_version;

    fn example_files() -> Vec<FileMetaHandle> {
        let mut f1 = FileMetaData::default();
        f1.smallest = "f".as_bytes().to_vec();
        f1.largest = "g".as_bytes().to_vec();
        let mut f2 = FileMetaData::default();
        f2.smallest = "e".as_bytes().to_vec();
        f2.largest = "f".as_bytes().to_vec();
        let mut f3 = FileMetaData::default();
        f3.smallest = "a".as_bytes().to_vec();
        f3.largest = "b".as_bytes().to_vec();
        let mut f4 = FileMetaData::default();
        f4.smallest = "q".as_bytes().to_vec();
        f4.largest = "z".as_bytes().to_vec();
        vec![f1, f2, f3, f4].into_iter().map(share).collect()
    }

    #[test]
    fn test_version_set_total_size() {
        let mut f1 = FileMetaData::default();
        f1.size = 10;
        let mut f2 = FileMetaData::default();
        f2.size = 20;
        let mut f3 = FileMetaData::default();
        f3.size = 30;
        let files = vec![share(f1), share(f2), share(f3)];
        assert_eq!(60, total_size(files.iter()));
    }

    #[test]
    fn test_version_set_get_range() {
        let cmp = DefaultCmp;
        let fs = example_files();
        assert_eq!(("a".as_bytes().to_vec(), "z".as_bytes().to_vec()),
                   get_range(&cmp, fs.iter()));
    }

    #[test]
    fn test_version_set_log_and_apply() {
        let (_, opt) = make_version();
        let mut vs = VersionSet::new("db".to_string(),
                                     opt.clone(),
                                     share(TableCache::new("db", opt.clone(), 100)));

        assert_eq!(2, vs.new_file_number());
        // Simulate NewDB
        {
            let mut ve = VersionEdit::new();
            ve.set_comparator_name("leveldb.BytewiseComparator");
            ve.set_log_num(10);
            ve.set_next_file(20);
            ve.set_last_seq(30);

            // Write first manifest to be recovered from.
            let manifest = format!("db/MANIFEST-{:06}", 1);
            let mffile = opt.env.open_writable_file(Path::new(&manifest)).unwrap();
            let mut lw = LogWriter::new(mffile);
            lw.add_record(&ve.encode()).unwrap();
            lw.flush().unwrap();
            set_current_file(&opt.env.as_ref(), "db", 1).unwrap();
        }

        // Recover from new state.
        {
            vs.recover().unwrap();
            assert_eq!(10, vs.log_num);
            assert_eq!(21, vs.next_file_num);
            assert_eq!(30, vs.last_seq);
            assert_eq!(0, vs.current.as_ref().unwrap().borrow().files[0].len());
            assert_eq!(0, vs.current.as_ref().unwrap().borrow().files[1].len());
            assert_eq!(35, vs.write_snapshot().unwrap());
        }

        // Simulate compaction by adding a file.
        {
            let mut ve = VersionEdit::new();
            ve.set_prev_log_num(1);
            ve.set_log_num(11);
            let mut fmd = FileMetaData::default();
            fmd.num = 21;
            fmd.size = 123;
            fmd.smallest = "abc".as_bytes().to_vec();
            fmd.largest = "def".as_bytes().to_vec();
            ve.add_file(1, fmd);
            vs.log_and_apply(ve).unwrap();

            assert!(opt.env.exists(Path::new("db/CURRENT")).unwrap());
            assert!(opt.env.exists(Path::new("db/MANIFEST-000001")).unwrap());
            // next_file_num and last_seq are untouched by log_and_apply
            assert_eq!(21, vs.new_file_number());
            assert_eq!(22, vs.next_file_num);
            assert_eq!(30, vs.last_seq);
            // the following fields are touched by log_and_apply.
            assert_eq!(1, vs.prev_log_num);
            assert_eq!(11, vs.log_num);

            assert_eq!(3, vs.versions.len());
            // The previous "compaction" should have added one file to the first level in the
            // current version.
            assert_eq!(0, vs.current.as_ref().unwrap().borrow().files[0].len());
            assert_eq!(1, vs.current.as_ref().unwrap().borrow().files[1].len());
            assert_eq!(47, vs.write_snapshot().unwrap());
        }
    }

    #[test]
    fn test_version_set_utils() {
        let (v, opt) = make_version();
        let mut vs = VersionSet::new("db".to_string(),
                                     opt.clone(),
                                     share(TableCache::new("db", opt, 100)));
        vs.add_version(v);
        // live_files()
        assert_eq!(9, vs.live_files().len());
        // num_level_bytes()
        assert_eq!(434, vs.num_level_bytes(0));
        assert_eq!(651, vs.num_level_bytes(1));
        assert_eq!(434, vs.num_level_bytes(2));
        // num_level_files()
        assert_eq!(2, vs.num_level_files(0));
        assert_eq!(3, vs.num_level_files(1));
        assert_eq!(2, vs.num_level_files(2));
        // new_file_number()
        assert_eq!(2, vs.new_file_number());
        assert_eq!(3, vs.new_file_number());
    }

    #[test]
    fn test_version_set_compaction() {
        let (v, opt) = make_version();
        let mut vs = VersionSet::new("db".to_string(),
                                     opt.clone(),
                                     share(TableCache::new("db", opt, 100)));
        time_test!();
        vs.add_version(v);

        {
            // approximate_offset()
            let v = vs.current().unwrap();
            assert_eq!(0,
                       vs.approximate_offset(&v,
                                             LookupKey::new("aaa".as_bytes(), 9000)
                                                 .internal_key()));
            assert_eq!(216,
                       vs.approximate_offset(&v,
                                             LookupKey::new("bab".as_bytes(), 9000)
                                                 .internal_key()));
            assert_eq!(1085,
                       vs.approximate_offset(&v,
                                             LookupKey::new("fab".as_bytes(), 9000)
                                                 .internal_key()));
        }
        // The following tests reuse the same version set and verify that various compactions work
        // like they should.
        {
            time_test!("compaction tests");
            // compact level 0 with a partial range.
            let from = LookupKey::new("000".as_bytes(), 1000);
            let to = LookupKey::new("ab".as_bytes(), 1010);
            let c = vs.compact_range(0, from.internal_key(), to.internal_key()).unwrap();
            assert_eq!(2, c.inputs[0].len());
            assert_eq!(1, c.inputs[1].len());
            assert_eq!(1, c.grandparents.unwrap().len());

            // compact level 0, but entire range of keys in version.
            let from = LookupKey::new("000".as_bytes(), 1000);
            let to = LookupKey::new("zzz".as_bytes(), 1010);
            let c = vs.compact_range(0, from.internal_key(), to.internal_key()).unwrap();
            assert_eq!(2, c.inputs[0].len());
            assert_eq!(1, c.inputs[1].len());
            assert_eq!(1, c.grandparents.unwrap().len());

            // Expand input range on higher level.
            let from = LookupKey::new("dab".as_bytes(), 1000);
            let to = LookupKey::new("eab".as_bytes(), 1010);
            let c = vs.compact_range(1, from.internal_key(), to.internal_key()).unwrap();
            assert_eq!(3, c.inputs[0].len());
            assert_eq!(1, c.inputs[1].len());
            assert_eq!(0, c.grandparents.unwrap().len());

            // is_trivial_move
            let from = LookupKey::new("fab".as_bytes(), 1000);
            let to = LookupKey::new("fba".as_bytes(), 1010);
            let c = vs.compact_range(2, from.internal_key(), to.internal_key()).unwrap();
            assert!(c.is_trivial_move());

            // should_stop_before
            let from = LookupKey::new("000".as_bytes(), 1000);
            let to = LookupKey::new("zzz".as_bytes(), 1010);
            let mid = LookupKey::new("abc".as_bytes(), 1010);
            let mut c = vs.compact_range(0, from.internal_key(), to.internal_key()).unwrap();
            assert!(!c.should_stop_before(from.internal_key()));
            assert!(!c.should_stop_before(mid.internal_key()));
            assert!(!c.should_stop_before(to.internal_key()));

            // is_base_level_for
            let from = LookupKey::new("000".as_bytes(), 1000);
            let to = LookupKey::new("zzz".as_bytes(), 1010);
            let mut c = vs.compact_range(0, from.internal_key(), to.internal_key()).unwrap();
            assert!(c.is_base_level_for("aaa".as_bytes()));
            assert!(!c.is_base_level_for("hac".as_bytes()));

            // input/add_input_deletions
            let from = LookupKey::new("000".as_bytes(), 1000);
            let to = LookupKey::new("zzz".as_bytes(), 1010);
            let mut c = vs.compact_range(0, from.internal_key(), to.internal_key()).unwrap();
            for inp in &[(0, 0, 1), (0, 1, 2), (1, 0, 3)] {
                let f = c.input(inp.0, inp.1);
                assert_eq!(inp.2, f.borrow().num);
            }
            c.add_input_deletions();
            assert_eq!(23, c.edit.encode().len())
        }
    }
}
