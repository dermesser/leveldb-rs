
use cmp::Cmp;
use key_types::{parse_internal_key, truncate_to_userkey, InternalKey, LookupKey, ValueType};
use merging_iter::MergingIter;
use snapshot::Snapshot;
use types::{Direction, LdbIterator, Shared};
use version_set::VersionSet;

use std::cmp::Ordering;
use std::mem;
use std::rc::Rc;

use rand;

const READ_BYTES_PERIOD: isize = 1048576;

pub struct DBIterator {
    // A user comparator.
    cmp: Rc<Box<Cmp>>,
    vset: Shared<VersionSet>,
    iter: MergingIter,
    // By holding onto a snapshot, we make sure that the iterator iterates over the state at the
    // point of its creation.
    ss: Snapshot,
    dir: Direction,
    byte_count: isize,

    valid: bool,
    // temporarily stored user key.
    savedkey: Vec<u8>,
    // buffer for reading internal keys
    keybuf: Vec<u8>,
    savedval: Vec<u8>,
    valbuf: Vec<u8>,
}

impl DBIterator {
    pub fn new(cmp: Rc<Box<Cmp>>,
               vset: Shared<VersionSet>,
               iter: MergingIter,
               ss: Snapshot)
               -> DBIterator {
        DBIterator {
            cmp: cmp,
            vset: vset,
            iter: iter,
            ss: ss,
            dir: Direction::Forward,
            byte_count: random_period(),

            valid: false,
            savedkey: vec![],
            keybuf: vec![],
            savedval: vec![],
            valbuf: vec![],
        }
    }

    /// record_read_sample records a read sample using the current contents of self.keybuf, which
    /// should be an InternalKey.
    fn record_read_sample<'a>(&mut self) {
        if self.byte_count < 0 {
            let vset = self.vset.borrow().current();
            vset.borrow_mut().record_read_sample(&self.keybuf);
            self.byte_count += random_period();
        }
    }

    /// find_next_user_entry skips to the next user entry after the one saved in self.savedkey.
    fn find_next_user_entry(&mut self, mut skipping: bool) -> bool {
        assert!(self.iter.valid());
        assert!(self.dir == Direction::Forward);

        while self.iter.valid() {
            self.iter.current(&mut self.keybuf, &mut self.savedval);
            self.record_read_sample();
            let (typ, seq, ukey) = parse_internal_key(&self.keybuf);

            // Skip keys with a sequence number after our snapshot.
            if seq <= self.ss.sequence() {
                if typ == ValueType::TypeDeletion {
                    // Mark current (deleted) key to be skipped.
                    self.savedkey.clear();
                    self.savedkey.extend_from_slice(ukey);
                    skipping = true;
                } else if typ == ValueType::TypeValue {
                    if skipping && self.cmp.cmp(ukey, &self.savedkey) <= Ordering::Equal {
                        // Entry hidden, because it's smaller than the key to be skipped.
                    } else {
                        self.valid = true;
                        self.savedkey.clear();
                        return true;
                    }
                }
            }
            self.iter.advance();
        }
        self.savedkey.clear();
        self.valid = false;
        false
    }

    /// find_prev_user_entry, on a backwards-moving iterator, stores the newest non-deleted version
    /// of the entry with the key == self.savedkey that is in the current snapshot, into
    /// savedkey/savedval.
    fn find_prev_user_entry(&mut self) -> bool {
        assert!(self.dir == Direction::Reverse);
        let mut value_type = ValueType::TypeDeletion;

        // The iterator should be already set to the previous entry if this is a direction change
        // (i.e. first prev() call after advance()). savedkey is set to the key of that entry.
        //
        // We read the current entry, ignore it for comparison (because the initial value_type is
        // Deletion), assign it to savedkey and savedval and go back another step (at the end of
        // the loop).
        //
        // We repeat this until we hit the first entry with a different user key (possibly going
        // through newer versions of the same key, because the newest entry is first in order),
        // then break. The key and value of the latest entry for the desired key have been stored
        // in the previous iteration to savedkey and savedval.
        while self.iter.valid() {
            self.iter.current(&mut self.keybuf, &mut self.valbuf);
            self.record_read_sample();
            let (typ, seq, ukey) = parse_internal_key(&self.keybuf);

            if seq > 0 && seq <= self.ss.sequence() {
                if value_type != ValueType::TypeDeletion &&
                   self.cmp.cmp(ukey, &self.savedkey) == Ordering::Less {
                    // We found a non-deleted entry for a previous key (in the previous iteration)
                    break;
                }
                value_type = typ;
                if value_type == ValueType::TypeDeletion {
                    self.savedkey.clear();
                    self.savedval.clear();
                } else {
                    self.savedkey.clear();
                    self.savedkey.extend_from_slice(ukey);

                    mem::swap(&mut self.savedval, &mut self.valbuf);
                }
            }
            self.iter.prev();
        }

        if value_type == ValueType::TypeDeletion {
            self.valid = false;
            self.savedkey.clear();
            self.savedval.clear();
            self.dir = Direction::Forward;
        } else {
            self.valid = true;
        }
        true
    }
}

impl LdbIterator for DBIterator {
    fn advance(&mut self) -> bool {
        if !self.valid() {
            self.seek_to_first();
            return self.valid();
        }

        if self.dir == Direction::Reverse {
            self.dir = Direction::Forward;
            if !self.iter.valid() {
                self.iter.seek_to_first();
            } else {
                self.iter.advance();
            }
            if !self.iter.valid() {
                self.valid = false;
                self.savedkey.clear();
                return false;
            }
        } else {
            // Save current user key.
            assert!(self.iter.current(&mut self.savedkey, &mut self.savedval));
            truncate_to_userkey(&mut self.savedkey);
        }
        self.find_next_user_entry(// skipping=
                                  true)
    }
    fn current(&self, key: &mut Vec<u8>, val: &mut Vec<u8>) -> bool {
        if !self.valid() {
            return false;
        }
        // If direction is forward, savedkey and savedval are not used.
        if self.dir == Direction::Forward {
            self.iter.current(key, val);
            truncate_to_userkey(key);
            true
        } else {
            key.clear();
            key.extend_from_slice(&self.savedkey);
            val.clear();
            val.extend_from_slice(&self.savedval);
            true
        }
    }
    fn prev(&mut self) -> bool {
        if !self.valid() {
            return false;
        }

        if self.dir == Direction::Forward {
            // scan backwards until we hit a different key; then use the normal scanning procedure:
            // find_prev_user_entry() wants savedkey to be the key of the entry that is supposed to
            // be left in savedkey/savedval, which is why we have to go to the previous entry before
            // calling it.
            self.iter.current(&mut self.savedkey, &mut self.savedval);
            truncate_to_userkey(&mut self.savedkey);
            loop {
                self.iter.prev();
                if !self.iter.valid() {
                    self.valid = false;
                    self.savedkey.clear();
                    self.savedval.clear();
                    return false;
                }
                // Scan until we hit the next-smaller key.
                self.iter.current(&mut self.keybuf, &mut self.savedval);
                truncate_to_userkey(&mut self.keybuf);
                if self.cmp.cmp(&self.keybuf, &self.savedkey) == Ordering::Less {
                    println!("breaking with {:?} / {:?}", self.keybuf, self.savedval);
                    break;
                }
            }
            self.dir = Direction::Reverse;
        }
        self.find_prev_user_entry()
    }
    fn valid(&self) -> bool {
        self.valid
    }
    fn seek(&mut self, to: &[u8]) {
        self.dir = Direction::Forward;
        self.savedkey.clear();
        self.savedval.clear();
        self.savedkey.extend_from_slice(LookupKey::new(to, self.ss.sequence()).internal_key());
        self.iter.seek(&self.savedkey);
        if self.iter.valid() {
            self.find_next_user_entry(// skipping=
                                      false);
        } else {
            self.valid = false;
        }
    }
    fn seek_to_first(&mut self) {
        self.dir = Direction::Forward;
        self.savedval.clear();
        self.iter.seek_to_first();
        if self.iter.valid() {
            self.find_next_user_entry(// skipping=
                                      false);
        } else {
            self.valid = false;
        }
    }
    fn reset(&mut self) {
        self.iter.reset();
        self.valid = false;
        self.savedkey.clear();
        self.savedval.clear();
        self.keybuf.clear();
    }
}

fn random_period() -> isize {
    rand::random::<isize>() % 2 * READ_BYTES_PERIOD
}

#[cfg(test)]
mod tests {
    use super::*;
    use types::{current_key_val, Direction};
    use test_util::LdbIteratorIter;
    use db_impl::testutil::*;

    #[test]
    fn db_iter_basic_test() {
        let mut db = build_db();
        let mut iter = db.new_iter().unwrap();

        // keys and values come from make_version(); they are each the latest entry.
        let keys: &[&[u8]] = &[b"aaa", b"aab", b"aax", b"aba", b"bab", b"bba", b"cab", b"cba"];
        let vals: &[&[u8]] = &[b"val0", b"val2", b"val1", b"val3", b"val2", b"val3", b"val1",
                               b"val3"];

        for (k, v) in keys.iter().zip(vals.iter()) {
            assert!(iter.advance());
            assert_eq!((k.to_vec(), v.to_vec()), current_key_val(&iter).unwrap());
        }
    }

    #[test]
    fn db_iter_reset() {
        let mut db = build_db();
        let mut iter = db.new_iter().unwrap();

        assert!(iter.advance());
        assert!(iter.valid());
        iter.reset();
        assert!(!iter.valid());
        assert!(iter.advance());
        assert!(iter.valid());
    }

    #[test]
    fn db_iter_test_fwd_backwd() {
        let mut db = build_db();
        let mut iter = db.new_iter().unwrap();

        // keys and values come from make_version(); they are each the latest entry.
        let keys: &[&[u8]] = &[b"aaa", b"aab", b"aax", b"aba", b"bab", b"bba", b"cab", b"cba"];
        let vals: &[&[u8]] = &[b"val0", b"val2", b"val1", b"val3", b"val2", b"val3", b"val1",
                               b"val3"];

        // This specifies the direction that the iterator should move to. Based on this, an index
        // into keys/vals is incremented/decremented so that we get a nice test checking iterator
        // move correctness.
        let dirs: &[Direction] = &[Direction::Forward,
                                   Direction::Forward,
                                   Direction::Forward,
                                   Direction::Reverse,
                                   Direction::Reverse,
                                   Direction::Reverse,
                                   Direction::Forward,
                                   Direction::Forward,
                                   Direction::Reverse,
                                   Direction::Forward,
                                   Direction::Forward,
                                   Direction::Forward,
                                   Direction::Forward];
        let mut i = 0;
        iter.advance();
        for d in dirs {
            assert_eq!((keys[i].to_vec(), vals[i].to_vec()),
                       current_key_val(&iter).unwrap());
            match *d {
                Direction::Forward => {
                    assert!(iter.advance());
                    i += 1;
                }
                Direction::Reverse => {
                    assert!(iter.prev());
                    i -= 1;
                }
            }
        }
    }

    #[test]
    fn db_iter_test_seek() {
        let mut db = build_db();
        let mut iter = db.new_iter().unwrap();

        // gca is the deleted entry.
        let keys: &[&[u8]] = &[b"aab", b"aaa", b"cab", b"eaa", b"aaa", b"iba", b"fba"];
        let vals: &[&[u8]] = &[b"val2", b"val0", b"val1", b"val1", b"val0", b"val2", b"val3"];

        for (k, v) in keys.iter().zip(vals.iter()) {
            println!("{:?}", String::from_utf8(k.to_vec()).unwrap());
            iter.seek(k);
            assert_eq!((k.to_vec(), v.to_vec()), current_key_val(&iter).unwrap());
        }

        // seek past last.
        iter.seek(b"xxx");
        assert!(!iter.valid());
        iter.seek(b"aab");
        assert!(iter.valid());

        // Seek skips over deleted entry.
        iter.seek(b"gca");
        assert!(iter.valid());
        assert_eq!((b"gda".to_vec(), b"val5".to_vec()),
                   current_key_val(&iter).unwrap());
    }

    #[test]
    fn db_iter_deleted_entry_not_returned() {
        let mut db = build_db();
        let mut iter = db.new_iter().unwrap();
        let must_not_appear = b"gca";

        for (k, _) in LdbIteratorIter::wrap(&mut iter) {
            assert!(k.as_slice() != must_not_appear);
        }
    }

    #[test]
    fn db_iter_deleted_entry_not_returned_memtable() {
        let mut db = build_db();

        db.put(b"xyz", b"123").unwrap();
        db.delete(b"xyz", true).unwrap();

        let mut iter = db.new_iter().unwrap();
        let must_not_appear = b"xyz";

        for (k, _) in LdbIteratorIter::wrap(&mut iter) {
            assert!(k.as_slice() != must_not_appear);
        }
    }
}
