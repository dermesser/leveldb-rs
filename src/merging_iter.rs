use types::LdbIterator;
use types::Comparator;

use std::cmp::Ordering;

// Warning: This module is kinda messy. The original implementation is
// not that much better though :-)
//
// Issues: 1) prev() may not work correctly at the beginning of a merging
// iterator.

#[derive(PartialEq)]
enum SL {
    Smallest,
    Largest,
}

#[derive(PartialEq)]
enum Direction {
    Fwd,
    Rvrs,
}

pub struct MergingIter<'a, 'b: 'a, C: Comparator> {
    iters: Vec<&'a mut LdbIterator<Item = (&'b [u8], &'b [u8])>>,
    current: Option<usize>,
    c: C,
    direction: Direction,
}

impl<'a, 'b: 'a, C: 'b + Comparator> MergingIter<'a, 'b, C> {
    /// Construct a new merging iterator.
    pub fn new(iters: Vec<&'a mut LdbIterator<Item = (&'b [u8], &'b [u8])>>,
               c: C)
               -> MergingIter<'a, 'b, C> {
        let mi = MergingIter {
            iters: iters,
            current: None,
            direction: Direction::Fwd,
            c: c,
        };
        mi
    }

    fn init(&mut self) {
        for i in 0..self.iters.len() {
            self.iters[i].reset();
            self.iters[i].next();
            assert!(self.iters[i].valid());
        }
        self.find_smallest();
    }

    /// Adjusts the direction of the iterator depending on whether the last
    /// call was next() or prev(). This basically sets all iterators to one
    /// entry after (Fwd) or one entry before (Rvrs) the current() entry.
    fn update_direction(&mut self, d: Direction) {
        if let Some((key, _)) = self.current() {
            if let Some(current) = self.current {
                match d {
                    Direction::Fwd if self.direction == Direction::Rvrs => {
                        self.direction = Direction::Fwd;
                        for i in 0..self.iters.len() {
                            if i != current {
                                self.iters[i].seek(key);
                                if let Some((current_key, _)) = self.iters[i].current() {
                                    if C::cmp(current_key, key) == Ordering::Equal {
                                        self.iters[i].next();
                                    }
                                }
                            }
                        }
                    }
                    Direction::Rvrs if self.direction == Direction::Fwd => {
                        self.direction = Direction::Rvrs;
                        for i in 0..self.iters.len() {
                            if i != current {
                                self.iters[i].seek(key);
                                self.iters[i].prev();
                            }
                        }
                    }
                    _ => {}
                }
            }
        }
    }

    fn find_smallest(&mut self) {
        self.find(SL::Smallest)
    }
    fn find_largest(&mut self) {
        self.find(SL::Largest)
    }

    fn find(&mut self, direction: SL) {
        assert!(self.iters.len() > 0);

        let ord;

        if direction == SL::Smallest {
            ord = Ordering::Less;
        } else {
            ord = Ordering::Greater;
        }

        let mut next_ix = 0;

        for i in 1..self.iters.len() {
            if let Some(current) = self.iters[i].current() {
                if let Some(smallest) = self.iters[next_ix].current() {
                    if C::cmp(current.0, smallest.0) == ord {
                        next_ix = i;
                    }
                } else {
                    // iter at `smallest` is exhausted
                    next_ix = i;
                }
            } else {
                // smallest stays the same
            }
        }

        self.current = Some(next_ix);
    }
}

impl<'a, 'b: 'a, C: 'b + Comparator> Iterator for MergingIter<'a, 'b, C> {
    type Item = (&'b [u8], &'b [u8]);

    fn next(&mut self) -> Option<Self::Item> {
        if let Some(current) = self.current {
            self.update_direction(Direction::Fwd);
            if let None = self.iters[current].next() {
                // Take this iterator out of rotation; this will return None
                // for every call to current() and thus it will be ignored
                // from here on.
                self.iters[current].reset();
            }
            self.find_smallest();
        } else {
            self.init();
        }

        self.iters[self.current.unwrap()].current()
    }
}

impl<'a, 'b: 'a, C: 'b + Comparator> LdbIterator for MergingIter<'a, 'b, C> {
    fn valid(&self) -> bool {
        return self.current.is_some() && self.iters.iter().any(|it| it.valid());
    }
    fn seek(&mut self, key: &[u8]) {
        for i in 0..self.iters.len() {
            self.iters[i].seek(key);
        }
        self.find_smallest();
    }
    fn reset(&mut self) {
        for i in 0..self.iters.len() {
            self.iters[i].reset();
        }
    }
    fn current(&self) -> Option<Self::Item> {
        if let Some(ix) = self.current {
            self.iters[ix].current()
        } else {
            None
        }
    }
    fn prev(&mut self) -> Option<Self::Item> {
        if let Some(current) = self.current {
            if let Some((key, _)) = self.current() {
                self.update_direction(Direction::Rvrs);
                self.iters[current].prev();
                self.find_largest();
                self.current()
            } else {
                None
            }
        } else {
            None
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use test_util::TestLdbIter;
    use types::StandardComparator;
    use types::LdbIterator;
    use skipmap::tests;

    #[test]
    fn test_merging_one() {
        let skm = tests::make_skipmap();
        let mut iter = skm.iter();
        let mut iter2 = skm.iter();

        let mut miter = MergingIter::new(vec![&mut iter], StandardComparator);

        loop {
            if let Some((k, v)) = miter.next() {
                if let Some((k2, v2)) = iter2.next() {
                    assert_eq!(k, k2);
                    assert_eq!(v, v2);
                } else {
                    panic!("Expected element from iter2");
                }
            } else {
                break;
            }
        }
    }

    #[test]
    fn test_merging_two() {
        let skm = tests::make_skipmap();
        let mut iter = skm.iter();
        let mut iter2 = skm.iter();

        let mut miter = MergingIter::new(vec![&mut iter, &mut iter2], StandardComparator);

        loop {
            if let Some((k, v)) = miter.next() {
                if let Some((k2, v2)) = miter.next() {
                    assert_eq!(k, k2);
                    assert_eq!(v, v2);
                } else {
                    panic!("Odd number of elements");
                }
            } else {
                break;
            }
        }
    }

    #[test]
    fn test_merging_fwd_bckwd() {
        let skm = tests::make_skipmap();
        let mut iter = skm.iter();
        let mut iter2 = skm.iter();

        let mut miter = MergingIter::new(vec![&mut iter, &mut iter2], StandardComparator);

        let first = miter.next();
        miter.next();
        let third = miter.next();

        assert!(first != third);
        let second = miter.prev();
        assert_eq!(first, second);
    }

    fn b(s: &'static str) -> &'static [u8] {
        s.as_bytes()
    }

    #[test]
    fn test_merging_real() {
        let val = "def".as_bytes();

        let mut it1 = TestLdbIter::new(vec![(b("aba"), val), (b("abc"), val), (b("abe"), val)]);
        let mut it2 = TestLdbIter::new(vec![(b("abb"), val), (b("abd"), val)]);
        let expected = vec![b("aba"), b("abb"), b("abc"), b("abd"), b("abe")];

        let iter = MergingIter::new(vec![&mut it1, &mut it2], StandardComparator);

        let mut i = 0;
        for (k, _) in iter {
            assert_eq!(k, expected[i]);
            i += 1;
        }

    }

    #[test]
    fn test_merging_seek_reset() {
        let val = "def".as_bytes();

        let mut it1 = TestLdbIter::new(vec![(b("aba"), val), (b("abc"), val), (b("abe"), val)]);
        let mut it2 = TestLdbIter::new(vec![(b("abb"), val), (b("abd"), val)]);

        let mut iter = MergingIter::new(vec![&mut it1, &mut it2], StandardComparator);

        assert!(!iter.valid());
        iter.next();
        assert!(iter.valid());
        assert!(iter.current().is_some());

        iter.seek("abc".as_bytes());
        assert_eq!(iter.current(), Some((b("abc"), val)));
        iter.seek("ab0".as_bytes());
        assert_eq!(iter.current(), Some((b("aba"), val)));
        iter.seek("abx".as_bytes());
        assert_eq!(iter.current(), None);

        iter.reset();
        assert!(!iter.valid());
        iter.next();
        assert_eq!(iter.current(), Some((b("aba"), val)));
    }

    // #[test]
    fn test_merging_fwd_bckwd_2() {
        let val = "def".as_bytes();

        let mut it1 = TestLdbIter::new(vec![(b("aba"), val), (b("abc"), val), (b("abe"), val)]);
        let mut it2 = TestLdbIter::new(vec![(b("abb"), val), (b("abd"), val)]);

        let mut iter = MergingIter::new(vec![&mut it1, &mut it2], StandardComparator);

        iter.next();
        iter.next();
        loop {
            let a = iter.next();

            if let None = a {
                break;
            }
            let b = iter.prev();
            let c = iter.next();
            iter.next();

            println!("{:?}", (a, b, c));
        }
    }
}
