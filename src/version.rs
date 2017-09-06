use cmp::{Cmp, InternalKeyCmp};
use error::Result;
use key_types::{parse_internal_key, InternalKey, LookupKey, UserKey};
use table_cache::SharedTableCache;
use table_reader::TableIterator;
use types::{MAX_SEQUENCE_NUMBER, FileMetaData, LdbIterator, Shared};
use version_set::NUM_LEVELS;

use std::cmp::Ordering;
use std::default::Default;
use std::rc::Rc;

/// FileMetaHandle is a reference-counted FileMetaData object with interior mutability. This is
/// necessary to provide a shared metadata container that can be modified while referenced by e.g.
/// multiple versions.
type FileMetaHandle = Shared<FileMetaData>;

/// Contains statistics about seeks occurred in a file.
struct GetStats {
    file: Option<FileMetaHandle>,
    level: usize,
}

struct Version {
    table_cache: SharedTableCache,
    user_cmp: Rc<Box<Cmp>>,
    files: [Vec<FileMetaHandle>; NUM_LEVELS],

    file_to_compact: Option<FileMetaHandle>,
    file_to_compact_lvl: usize,
}

impl Version {
    fn new(cache: SharedTableCache, ucmp: Rc<Box<Cmp>>) -> Version {
        Version {
            table_cache: cache,
            user_cmp: ucmp,
            files: Default::default(),
            file_to_compact: None,
            file_to_compact_lvl: 0,
        }
    }

    /// get_full_impl does the same as get(), but implements the entire logic itself instead of
    /// delegating some of it to get_overlapping().
    #[allow(unused_assignments)]
    fn get_full_impl(&mut self, key: &LookupKey) -> Result<Option<(Vec<u8>, GetStats)>> {
        let ikey = key.internal_key();
        let ukey = key.user_key();
        let icmp = InternalKeyCmp(self.user_cmp.clone());
        let mut stats = GetStats {
            file: None,
            level: 0,
        };

        // Search for key, starting at the lowest level and working upwards.
        for level in 0..NUM_LEVELS {
            let files = &self.files[level];
            let mut to_search = vec![];

            if level == 0 {
                to_search.reserve(files.len());
                for f_ in files {
                    let f = f_.borrow();
                    let (fsmallest, flargest) = (parse_internal_key(&f.smallest).2,
                                                 parse_internal_key(&f.largest).2);
                    if self.user_cmp.cmp(ukey, fsmallest) >= Ordering::Equal &&
                       self.user_cmp.cmp(ukey, flargest) <= Ordering::Equal {
                        to_search.push(f_.clone());
                    }
                }
                to_search.sort_by(|a, b| a.borrow().num.cmp(&b.borrow().num));
            } else {
                let ix = find_file(&icmp, files, ikey);
                if ix < files.len() {
                    let f = files[ix].borrow();
                    let fsmallest = parse_internal_key(&f.smallest).2;
                    if self.user_cmp.cmp(ukey, fsmallest) >= Ordering::Equal {
                        to_search.push(files[ix].clone());
                    }
                }
            }

            if files.is_empty() {
                continue;
            }

            let mut last_read = None;
            let mut last_read_level: usize = 0;
            for f in to_search {
                if last_read.is_some() && stats.file.is_none() {
                    stats.file = last_read.clone();
                    stats.level = last_read_level;
                }
                last_read_level = level;
                last_read = Some(f.clone());

                let val = self.table_cache.borrow_mut().get(f.borrow().num, ikey)?;
                return Ok(val.map(|v| (v, stats)));
            }
        }
        Ok(None)
    }

    /// get returns the value for the specified key using the persistent tables contained in this
    /// Version.
    #[allow(unused_assignments)]
    fn get(&mut self, key: &LookupKey) -> Result<Option<(Vec<u8>, GetStats)>> {
        let levels = self.get_overlapping(key);
        let ikey = key.internal_key();
        let mut stats = GetStats {
            file: None,
            level: 0,
        };

        for level in 0..levels.len() {
            let files = &levels[level];
            let mut last_read = None;
            let mut last_read_level: usize = 0;
            for f in files {
                if last_read.is_some() && stats.file.is_none() {
                    stats.file = last_read.clone();
                    stats.level = last_read_level;
                }
                last_read_level = level;
                last_read = Some(f.clone());

                let val = self.table_cache.borrow_mut().get(f.borrow().num, ikey)?;
                return Ok(val.map(|v| (v, stats)));
            }
        }
        Ok(None)
    }

    /// get_overlapping returns the files overlapping key in each level.
    fn get_overlapping(&self, key: &LookupKey) -> [Vec<FileMetaHandle>; NUM_LEVELS] {
        let mut levels: [Vec<FileMetaHandle>; NUM_LEVELS] = Default::default();
        let ikey = key.internal_key();
        let ukey = key.user_key();

        let files = &self.files[0];
        levels[0].reserve(files.len());
        for f_ in files {
            let f = f_.borrow();
            let (fsmallest, flargest) = (parse_internal_key(&f.smallest).2,
                                         parse_internal_key(&f.largest).2);
            if self.user_cmp.cmp(ukey, fsmallest) >= Ordering::Equal &&
               self.user_cmp.cmp(ukey, flargest) <= Ordering::Equal {
                levels[0].push(f_.clone());
            }
        }
        levels[0].sort_by(|a, b| a.borrow().num.cmp(&b.borrow().num));

        let icmp = InternalKeyCmp(self.user_cmp.clone());
        for level in 1..NUM_LEVELS {
            let files = &self.files[level];
            let ix = find_file(&icmp, files, ikey);
            if ix < files.len() {
                let f = files[ix].borrow();
                let fsmallest = parse_internal_key(&f.smallest).2;
                if self.user_cmp.cmp(ukey, fsmallest) >= Ordering::Equal {
                    levels[level].push(files[ix].clone());
                }
            }
        }

        levels
    }

    /// record_read_sample returns true if there is a new file to be compacted. It counts the
    /// number of files overlapping a key, and which level contains the first overlap.
    #[allow(unused_assignments)]
    fn record_read_sample(&mut self, key: &LookupKey) -> bool {
        let levels = self.get_overlapping(key);
        let mut contained_in = 0;
        let mut i = 0;
        let mut first_file = None;
        let mut first_file_level = None;
        for level in &levels {
            if !level.is_empty() {
                if first_file.is_none() && first_file_level.is_none() {
                    first_file = Some(level[0].clone());
                    first_file_level = Some(i);
                }
            }
            contained_in += level.len();
            i += 1;
        }

        if contained_in > 1 {
            self.update_stats(GetStats {
                file: first_file,
                level: first_file_level.unwrap_or(0),
            })
        } else {
            false
        }
    }

    fn update_stats(&mut self, stats: GetStats) -> bool {
        if let Some(file) = stats.file {
            {
                file.borrow_mut().allowed_seeks -= 1;
            }
            if file.borrow().allowed_seeks < 1 && self.file_to_compact.is_none() {
                self.file_to_compact = Some(file.clone());
                self.file_to_compact_lvl = stats.level;
                return true;
            }
        }
        false
    }

    /// overlap_in_level returns true if the specified level's files overlap the range [smallest;
    /// largest].
    fn overlap_in_level<'a, 'b>(&self,
                                level: usize,
                                smallest: UserKey<'a>,
                                largest: UserKey<'a>)
                                -> bool {
        assert!(level < NUM_LEVELS);
        if level == 0 {
            some_file_overlaps_range_disjoint(&InternalKeyCmp(self.user_cmp.clone()),
                                              &self.files[level],
                                              smallest,
                                              largest)
        } else {
            some_file_overlaps_range(&InternalKeyCmp(self.user_cmp.clone()),
                                     &self.files[level],
                                     smallest,
                                     largest)
        }
    }

    /// overlapping_inputs returns all files that may contain keys between begin and end.
    fn overlapping_inputs<'a, 'b>(&self,
                                  level: usize,
                                  begin: InternalKey<'a>,
                                  end: InternalKey<'b>)
                                  -> Vec<FileMetaHandle> {
        assert!(level < NUM_LEVELS);
        let (mut ubegin, mut uend) = (parse_internal_key(begin).2.to_vec(),
                                      parse_internal_key(end).2.to_vec());

        loop {
            match do_search(self, level, ubegin, uend) {
                (Some((newubegin, newuend)), _) => {
                    ubegin = newubegin;
                    uend = newuend;
                }
                (None, result) => return result,
            }
        }

        // the actual search happens in this inner function. This is done to enhance the control
        // flow. It takes the smallest and largest user keys and returns a new pair of user keys if
        // the search range should be expanded, or a list of overlapping files.
        fn do_search(myself: &Version,
                     level: usize,
                     ubegin: Vec<u8>,
                     uend: Vec<u8>)
                     -> (Option<(Vec<u8>, Vec<u8>)>, Vec<FileMetaHandle>) {
            let mut inputs = vec![];
            for f_ in myself.files[level].iter() {
                let f = f_.borrow();
                let ((_, _, fsmallest), (_, _, flargest)) = (parse_internal_key(&f.smallest),
                                                             parse_internal_key(&f.largest));
                // Skip files that are not overlapping.
                if !ubegin.is_empty() && myself.user_cmp.cmp(flargest, &ubegin) == Ordering::Less {
                    continue;
                } else if !uend.is_empty() &&
                          myself.user_cmp.cmp(fsmallest, &uend) == Ordering::Greater {
                    continue;
                } else {
                    inputs.push(f_.clone());
                    // In level 0, files may overlap each other. Check if the new file begins
                    // before ubegin or ends after uend, and expand the range, if so. Then, restart
                    // the search.
                    if level == 0 {
                        if !ubegin.is_empty() &&
                           myself.user_cmp.cmp(fsmallest, &ubegin) == Ordering::Less {
                            return (Some((fsmallest.to_vec(), uend)), inputs);
                        } else if !uend.is_empty() &&
                                  myself.user_cmp.cmp(flargest, &uend) == Ordering::Greater {
                            return (Some((ubegin, flargest.to_vec())), inputs);
                        }
                    }
                }
            }
            (None, inputs)
        }

    }

    fn new_concat_iter(&self, level: usize) -> VersionIter {
        VersionIter {
            files: self.files[level].clone(),
            cache: self.table_cache.clone(),
            cmp: InternalKeyCmp(self.user_cmp.clone()),
            current: None,
            current_ix: 0,
        }
    }
}

/// VersionIter iterates over all files belonging to a certain level.
struct VersionIter {
    // NOTE: Maybe we need to change this to Rc to support modification of the file set after
    // creation of the iterator. Versions should be immutable, though.
    files: Vec<FileMetaHandle>,
    cache: SharedTableCache,
    cmp: InternalKeyCmp,

    current: Option<TableIterator>,
    current_ix: usize,
}

impl LdbIterator for VersionIter {
    fn advance(&mut self) -> bool {
        if let Some(ref mut t) = self.current {
            if t.advance() {
                return true;
            } else if self.current_ix >= self.files.len() - 1 {
                return false;
            }
        }
        if let Ok(tbl) = self.cache
            .borrow_mut()
            .get_table(self.files[self.current_ix].borrow().num) {
            self.current = Some(tbl.iter());
        } else {
            return false;
        }
        self.current_ix += 1;
        self.advance()
    }
    fn current(&self, key: &mut Vec<u8>, val: &mut Vec<u8>) -> bool {
        if let Some(ref t) = self.current {
            t.current(key, val)
        } else {
            false
        }
    }
    fn seek(&mut self, key: &[u8]) {
        let ix = find_file(&self.cmp, &self.files, key);
        assert!(ix < self.files.len());
        if let Ok(tbl) = self.cache.borrow_mut().get_table(self.files[ix].borrow().num) {
            let mut iter = tbl.iter();
            iter.seek(key);
            if iter.valid() {
                self.current_ix = ix;
                self.current = Some(iter);
                return;
            }
        }
        self.reset();
    }
    fn reset(&mut self) {
        self.current = None;
        self.current_ix = 0;
    }
    fn valid(&self) -> bool {
        self.current.is_some()
    }
    fn prev(&mut self) -> bool {
        if let Some(ref mut t) = self.current {
            if t.prev() {
                return true;
            } else if self.current_ix > 0 {
                let f = &self.files[self.current_ix - 1];
                // Find previous table, seek to last entry.
                if let Ok(tbl) = self.cache.borrow_mut().get_table(f.borrow().num) {
                    let mut iter = tbl.iter();
                    iter.seek(&f.borrow().largest);
                    // The saved largest key must be in the table.
                    assert!(iter.valid());
                    self.current_ix -= 1;
                    return true;
                }
            }
        }
        self.reset();
        false
    }
}

/// key_is_after_file returns true if the given user key is larger than the largest key in f.
fn key_is_after_file<'a>(cmp: &InternalKeyCmp, key: UserKey<'a>, f: &FileMetaHandle) -> bool {
    let f = f.borrow();
    let ulargest = parse_internal_key(&f.largest).2;
    !key.is_empty() && cmp.cmp_inner(key, ulargest) == Ordering::Greater
}

/// key_is_before_file returns true if the given user key is larger than the largest key in f.
fn key_is_before_file<'a>(cmp: &InternalKeyCmp, key: UserKey<'a>, f: &FileMetaHandle) -> bool {
    let f = f.borrow();
    let usmallest = parse_internal_key(&f.smallest).2;
    !key.is_empty() && cmp.cmp_inner(key, usmallest) == Ordering::Less
}

/// find_file returns the index of the file in files that potentially contains the internal key
/// key. files must not overlap and be ordered ascendingly.
fn find_file<'a>(cmp: &InternalKeyCmp, files: &[FileMetaHandle], key: InternalKey<'a>) -> usize {
    let (mut left, mut right) = (0, files.len());
    while left < right {
        let mid = (left + right) / 2;
        if cmp.cmp(&files[mid].borrow().largest, key) == Ordering::Less {
            left = mid + 1;
        } else {
            right = mid;
        }
    }
    return right;
}

/// some_file_overlaps_range_disjoint returns true if any of the given disjoint files (i.e. level >
/// 1) contain keys in the range defined by the user keys [smallest; largest].
fn some_file_overlaps_range_disjoint<'a, 'b>(cmp: &InternalKeyCmp,
                                             files: &[FileMetaHandle],
                                             smallest: UserKey<'a>,
                                             largest: UserKey<'b>)
                                             -> bool {
    let ikey = LookupKey::new(smallest, MAX_SEQUENCE_NUMBER);
    let ix = find_file(cmp, files, ikey.internal_key());
    if ix < files.len() {
        !key_is_before_file(cmp, largest, &files[ix])
    } else {
        false
    }
}

/// some_file_overlaps_range returns true if any of the given possibly overlapping files contains
/// keys in the range [smallest; largest].
fn some_file_overlaps_range<'a, 'b>(cmp: &InternalKeyCmp,
                                    files: &[FileMetaHandle],
                                    smallest: UserKey<'a>,
                                    largest: UserKey<'b>)
                                    -> bool {
    for f in files {
        if !(key_is_after_file(cmp, smallest, f) || key_is_before_file(cmp, largest, f)) {
            return true;
        }
    }
    false
}

#[cfg(test)]
mod tests {
    use super::*;

    use std::default::Default;
    use std::path::Path;

    use cmp::DefaultCmp;
    use env::Env;
    use mem_env::MemEnv;
    use options::Options;
    use table_builder::TableBuilder;
    use table_cache::{table_name, TableCache};
    use types::share;

    fn new_file(num: u64, smallest: &[u8], largest: &[u8]) -> FileMetaHandle {
        share(FileMetaData {
            allowed_seeks: 10,
            size: 163840,
            num: num,
            smallest: LookupKey::new(smallest, MAX_SEQUENCE_NUMBER).internal_key().to_vec(),
            largest: LookupKey::new(largest, 0).internal_key().to_vec(),
        })
    }

    /// write_table creates a table with the given number and contents (must be sorted!) in the
    /// memenv. The sequence numbers given to keys start with startseq.
    fn write_table(me: &MemEnv,
                   contents: &[(&[u8], &[u8])],
                   startseq: u64,
                   num: u64)
                   -> FileMetaHandle {
        let dst = me.open_writable_file(Path::new(&table_name("db", num, "ldb"))).unwrap();
        let mut seq = startseq;
        let keys: Vec<Vec<u8>> = contents.iter()
            .map(|&(k, _)| {
                seq += 1;
                LookupKey::new(k, seq).internal_key().to_vec()
            })
            .collect();

        let mut tbl = TableBuilder::new(Options::default(), dst);
        for i in 0..contents.len() {
            tbl.add(&keys[i], contents[i].1);
            seq += 1;
        }

        let f = new_file(num,
                         LookupKey::new(contents[0].0, MAX_SEQUENCE_NUMBER).internal_key(),
                         LookupKey::new(contents[contents.len() - 1].0, 0).internal_key());
        f.borrow_mut().size = tbl.finish() as u64;
        f
    }

    fn make_version() -> Version {
        time_test!("make_version");
        let mut opts = Options::default();
        let env = MemEnv::new();

        // Level 0 (overlapping)
        let f1: &[(&[u8], &[u8])] = &[("aaa".as_bytes(), "val1".as_bytes()),
                                      ("aab".as_bytes(), "val2".as_bytes()),
                                      ("aba".as_bytes(), "val3".as_bytes())];
        let t1 = write_table(&env, f1, 1, 1);
        let f2: &[(&[u8], &[u8])] = &[("aax".as_bytes(), "val1".as_bytes()),
                                      ("bab".as_bytes(), "val2".as_bytes()),
                                      ("bba".as_bytes(), "val3".as_bytes())];
        let t2 = write_table(&env, f2, 4, 2);
        // Level 1
        let f3: &[(&[u8], &[u8])] = &[("caa".as_bytes(), "val1".as_bytes()),
                                      ("cab".as_bytes(), "val2".as_bytes()),
                                      ("cba".as_bytes(), "val3".as_bytes())];
        let t3 = write_table(&env, f3, 7, 3);
        let f4: &[(&[u8], &[u8])] = &[("daa".as_bytes(), "val1".as_bytes()),
                                      ("dab".as_bytes(), "val2".as_bytes()),
                                      ("dba".as_bytes(), "val3".as_bytes())];
        let t4 = write_table(&env, f4, 10, 4);
        let f5: &[(&[u8], &[u8])] = &[("eaa".as_bytes(), "val1".as_bytes()),
                                      ("eab".as_bytes(), "val2".as_bytes()),
                                      ("eba".as_bytes(), "val3".as_bytes())];
        let t5 = write_table(&env, f5, 13, 5);
        // Level 2
        let f6: &[(&[u8], &[u8])] = &[("faa".as_bytes(), "val1".as_bytes()),
                                      ("fab".as_bytes(), "val2".as_bytes()),
                                      ("fba".as_bytes(), "val3".as_bytes())];
        let t6 = write_table(&env, f6, 16, 6);
        let f7: &[(&[u8], &[u8])] = &[("gaa".as_bytes(), "val1".as_bytes()),
                                      ("gab".as_bytes(), "val2".as_bytes()),
                                      ("gba".as_bytes(), "val3".as_bytes())];
        let t7 = write_table(&env, f7, 19, 7);

        opts.set_env(Box::new(env));
        let cache = TableCache::new("db", opts, 100);
        let mut v = Version::new(share(cache), Rc::new(Box::new(DefaultCmp)));
        v.files[0] = vec![t1, t2];
        v.files[1] = vec![t3, t4, t5];
        v.files[2] = vec![t6, t7];
        v
    }

    #[test]
    fn test_version_overlapping_inputs() {
        let v = make_version();

        time_test!("overlapping-inputs");
        {
            time_test!("overlapping-inputs-1");
            // Range is expanded in overlapping level-0 files.
            let from = LookupKey::new("aab".as_bytes(), MAX_SEQUENCE_NUMBER);
            let to = LookupKey::new("aae".as_bytes(), 0);
            let r = v.overlapping_inputs(0, from.internal_key(), to.internal_key());
            assert_eq!(r.len(), 2);
            assert_eq!(r[0].borrow().num, 1);
            assert_eq!(r[1].borrow().num, 2);
        }
        {
            let from = LookupKey::new("cab".as_bytes(), MAX_SEQUENCE_NUMBER);
            let to = LookupKey::new("cbx".as_bytes(), 0);
            // expect one file.
            let r = v.overlapping_inputs(1, from.internal_key(), to.internal_key());
            assert_eq!(r.len(), 1);
            assert_eq!(r[0].borrow().num, 3);
        }
        {
            let from = LookupKey::new("cab".as_bytes(), MAX_SEQUENCE_NUMBER);
            let to = LookupKey::new("ebx".as_bytes(), 0);
            let r = v.overlapping_inputs(1, from.internal_key(), to.internal_key());
            // Assert that correct number of files and correct files were returned.
            assert_eq!(r.len(), 3);
            assert_eq!(r[0].borrow().num, 3);
            assert_eq!(r[1].borrow().num, 4);
            assert_eq!(r[2].borrow().num, 5);
        }
        {
            let from = LookupKey::new("hhh".as_bytes(), MAX_SEQUENCE_NUMBER);
            let to = LookupKey::new("ijk".as_bytes(), 0);
            let r = v.overlapping_inputs(2, from.internal_key(), to.internal_key());
            assert_eq!(r.len(), 0);
            let r = v.overlapping_inputs(1, from.internal_key(), to.internal_key());
            assert_eq!(r.len(), 0);
        }
    }

    #[test]
    fn test_version_key_ordering() {
        time_test!();
        let fmh = new_file(1, &[1, 0, 0], &[2, 0, 0]);
        let cmp = InternalKeyCmp(Rc::new(Box::new(DefaultCmp)));

        // Keys before file.
        for k in &[&[0][..], &[1], &[1, 0], &[0, 9, 9, 9]] {
            assert!(key_is_before_file(&cmp, k, &fmh));
            assert!(!key_is_after_file(&cmp, k, &fmh));
        }
        // Keys in file.
        for k in &[&[1, 0, 0][..], &[1, 0, 1], &[1, 2, 3, 4], &[1, 9, 9], &[2, 0, 0]] {
            assert!(!key_is_before_file(&cmp, k, &fmh));
            assert!(!key_is_after_file(&cmp, k, &fmh));
        }
        // Keys after file.
        for k in &[&[2, 0, 1][..], &[9, 9, 9], &[9, 9, 9, 9]] {
            assert!(!key_is_before_file(&cmp, k, &fmh));
            assert!(key_is_after_file(&cmp, k, &fmh));
        }
    }

    #[test]
    fn test_version_file_overlaps() {
        time_test!();

        let files_disjoint = [new_file(1, &[2, 0, 0], &[3, 0, 0]),
                              new_file(2, &[3, 0, 1], &[4, 0, 0]),
                              new_file(3, &[4, 0, 1], &[5, 0, 0])];
        let files_joint = [new_file(1, &[2, 0, 0], &[3, 0, 0]),
                           new_file(2, &[2, 5, 0], &[4, 0, 0]),
                           new_file(3, &[3, 5, 1], &[5, 0, 0])];
        let cmp = InternalKeyCmp(Rc::new(Box::new(DefaultCmp)));

        assert!(some_file_overlaps_range(&cmp, &files_joint, &[2, 5, 0], &[3, 1, 0]));
        assert!(some_file_overlaps_range(&cmp, &files_joint, &[2, 5, 0], &[7, 0, 0]));
        assert!(some_file_overlaps_range(&cmp, &files_joint, &[0, 0], &[2, 0, 0]));
        assert!(some_file_overlaps_range(&cmp, &files_joint, &[0, 0], &[7, 0, 0]));
        assert!(!some_file_overlaps_range(&cmp, &files_joint, &[0, 0], &[0, 5]));
        assert!(!some_file_overlaps_range(&cmp, &files_joint, &[6, 0], &[7, 5]));

        assert!(some_file_overlaps_range_disjoint(&cmp, &files_disjoint, &[2, 0, 1], &[2, 5, 0]));
        assert!(some_file_overlaps_range_disjoint(&cmp, &files_disjoint, &[3, 0, 1], &[4, 9, 0]));
        assert!(some_file_overlaps_range_disjoint(&cmp, &files_disjoint, &[2, 0, 1], &[6, 5, 0]));
        assert!(some_file_overlaps_range_disjoint(&cmp, &files_disjoint, &[0, 0, 1], &[2, 5, 0]));
        assert!(some_file_overlaps_range_disjoint(&cmp, &files_disjoint, &[0, 0, 1], &[6, 5, 0]));
        assert!(!some_file_overlaps_range_disjoint(&cmp, &files_disjoint, &[0, 0, 1], &[0, 1]));
        assert!(!some_file_overlaps_range_disjoint(&cmp, &files_disjoint, &[6, 0, 1], &[7, 0, 1]));
    }
}
