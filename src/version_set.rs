
use cmp::{Cmp, InternalKeyCmp};
use error::Result;
use key_types::{parse_internal_key, InternalKey, LookupKey, UserKey};
use table_cache::TableCache;
use table_reader::TableIterator;
use types::{MAX_SEQUENCE_NUMBER, FileMetaData, LdbIterator};

use std::cmp::Ordering;
use std::default::Default;
use std::rc::Rc;

const NUM_LEVELS: usize = 7;

/// FileMetaHandle is a reference-counted FileMetaData object.
type FileMetaHandle = Rc<FileMetaData>;

/// Contains statistics about seeks occurred in a file.
struct GetStats {
    file: Option<FileMetaHandle>,
    level: usize,
}

struct Version {
    table_cache: Rc<TableCache>,
    user_cmp: Rc<Box<Cmp>>,
    files: [Vec<FileMetaHandle>; NUM_LEVELS],

    file_to_compact: Option<FileMetaHandle>,
    file_to_compact_lvl: usize,
}

impl Version {
    fn new(cache: Rc<TableCache>, ucmp: Rc<Box<Cmp>>) -> Version {
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
                for f in files {
                    let (fsmallest, flargest) = (parse_internal_key(&(*f).smallest).2,
                                                 parse_internal_key(&(*f).largest).2);
                    if self.user_cmp.cmp(ukey, fsmallest) >= Ordering::Equal &&
                       self.user_cmp.cmp(ukey, flargest) <= Ordering::Equal {
                        to_search.push(f.clone());
                    }
                }
                to_search.sort_by(|a, b| a.num.cmp(&b.num));
            } else {
                let ix = find_file(&icmp, files, ikey);
                if ix < files.len() {
                    let fsmallest = parse_internal_key(&(*files[ix]).smallest).2;
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

                let val = Rc::get_mut(&mut self.table_cache).unwrap().get((*f).num, ikey)?;
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

                let val = Rc::get_mut(&mut self.table_cache).unwrap().get((*f).num, ikey)?;
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
        for f in files {
            let (fsmallest, flargest) = (parse_internal_key(&(*f).smallest).2,
                                         parse_internal_key(&(*f).largest).2);
            if self.user_cmp.cmp(ukey, fsmallest) >= Ordering::Equal &&
               self.user_cmp.cmp(ukey, flargest) <= Ordering::Equal {
                levels[0].push(f.clone());
            }
        }
        levels[0].sort_by(|a, b| a.num.cmp(&b.num));

        let icmp = InternalKeyCmp(self.user_cmp.clone());
        for level in 1..NUM_LEVELS {
            let files = &self.files[level];
            let ix = find_file(&icmp, files, ikey);
            if ix < files.len() {
                let fsmallest = parse_internal_key(&(*files[ix]).smallest).2;
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
        if let Some(mut file) = stats.file {
            {
                let mut f = Rc::get_mut(&mut file).unwrap();
                f.allowed_seeks -= 1;
            }
            if (*file).allowed_seeks < 1 && self.file_to_compact.is_some() {
                self.file_to_compact = Some(file.clone());
                self.file_to_compact_lvl = stats.level;
                return true;
            }
        }
        false
    }

    /// overlap_in_level returns true if the specified level's files overlap the range [smallest;
    /// largest].
    fn overlap_in_level(&self, level: usize, smallest: &[u8], largest: &[u8]) -> bool {
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
        let mut inputs = vec![];
        let (mut ubegin, mut uend) = (parse_internal_key(begin).2.to_vec(),
                                      parse_internal_key(end).2.to_vec());

        loop {
            'inner: for f in self.files[level].iter() {
                let ((_, _, fsmallest), (_, _, flargest)) = (parse_internal_key(&(*f).smallest),
                                                             parse_internal_key(&(*f).largest));
                // Skip files that are not overlapping.
                if !ubegin.is_empty() && self.user_cmp.cmp(flargest, &ubegin) == Ordering::Less {
                    continue 'inner;
                } else if !uend.is_empty() &&
                          self.user_cmp.cmp(fsmallest, &uend) == Ordering::Greater {
                    continue 'inner;
                } else {
                    inputs.push(f.clone());
                    // In level 0, files may overlap each other. Check if the new file begins
                    // before begin or ends after end, and expand the range, if so. Then, restart
                    // the search.
                    if level == 0 {
                        if !ubegin.is_empty() &&
                           self.user_cmp.cmp(fsmallest, &ubegin) == Ordering::Less {
                            ubegin = fsmallest.to_vec();
                            inputs.truncate(0);
                            break 'inner;
                        } else if !uend.is_empty() &&
                                  self.user_cmp.cmp(flargest, &uend) == Ordering::Greater {
                            uend = flargest.to_vec();
                            inputs.truncate(0);
                            break 'inner;
                        }
                    }
                }
            }
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
    cache: Rc<TableCache>,
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
        if let Ok(tbl) = Rc::get_mut(&mut self.cache)
            .unwrap()
            .get_table((*self.files[self.current_ix]).num) {
            self.current_ix += 1;
            self.current = Some(tbl.iter());
            self.advance()
        } else {
            false
        }
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
        if let Ok(tbl) = Rc::get_mut(&mut self.cache).unwrap().get_table((*self.files[ix]).num) {
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
                if let Ok(tbl) = Rc::get_mut(&mut self.cache).unwrap().get_table((*f).num) {
                    let mut iter = tbl.iter();
                    iter.seek(&(*f).largest);
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
    let ulargest = parse_internal_key(&(*f).largest).2;
    !key.is_empty() && cmp.cmp_inner(key, ulargest) == Ordering::Greater
}

/// key_is_before_file returns true if the given user key is larger than the largest key in f.
fn key_is_before_file<'a>(cmp: &InternalKeyCmp, key: UserKey<'a>, f: &FileMetaHandle) -> bool {
    let usmallest = parse_internal_key(&(*f).smallest).2;
    !key.is_empty() && cmp.cmp_inner(key, usmallest) == Ordering::Less
}

/// find_file returns the index of the file in files that potentially contains the internal key
/// key. files must not overlap and be ordered ascendingly.
fn find_file<'a>(cmp: &InternalKeyCmp, files: &[FileMetaHandle], key: InternalKey<'a>) -> usize {
    let (mut left, mut right) = (0, files.len());
    while left < right {
        let mid = (left + right) / 2;
        if cmp.cmp(&(*files[mid]).largest, key) == Ordering::Less {
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
    use cmp::DefaultCmp;

    fn new_file(num: u64, smallest: &[u8], largest: &[u8]) -> FileMetaHandle {
        Rc::new(FileMetaData {
            allowed_seeks: 10,
            size: 163840,
            num: num,
            smallest: LookupKey::new(smallest, MAX_SEQUENCE_NUMBER).internal_key().to_vec(),
            largest: LookupKey::new(largest, 0).internal_key().to_vec(),
        })
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
