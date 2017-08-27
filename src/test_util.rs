use types::LdbIterator;
use cmp::{Cmp, DefaultCmp};
use std::cmp::Ordering;

pub struct TestLdbIter<'a> {
    v: Vec<(&'a [u8], &'a [u8])>,
    ix: usize,
    init: bool,
}

impl<'a> TestLdbIter<'a> {
    pub fn new(c: Vec<(&'a [u8], &'a [u8])>) -> TestLdbIter<'a> {
        return TestLdbIter {
            v: c,
            ix: 0,
            init: false,
        };
    }
}

impl<'a> LdbIterator for TestLdbIter<'a> {
    fn advance(&mut self) -> bool {
        if self.ix == self.v.len() {
            false
        } else if !self.init {
            self.init = true;
            true
        } else {
            self.ix += 1;
            true
        }
    }
    fn reset(&mut self) {
        self.ix = 0;
        self.init = false;
    }
    fn current(&self, key: &mut Vec<u8>, val: &mut Vec<u8>) -> bool {
        if self.init && self.ix < self.v.len() {
            key.clear();
            val.clear();
            key.extend_from_slice(self.v[self.ix].0);
            val.extend_from_slice(self.v[self.ix].1);
            true
        } else {
            false
        }
    }
    fn valid(&self) -> bool {
        self.init
    }
    fn seek(&mut self, k: &[u8]) {
        self.ix = 0;
        while self.ix < self.v.len() && DefaultCmp.cmp(self.v[self.ix].0, k) == Ordering::Less {
            self.ix += 1;
        }
    }
    fn prev(&mut self) -> bool {
        if !self.init || self.ix == 0 {
            false
        } else {
            self.ix -= 1;
            true
        }
    }
}

/// LdbIteratorIter implements std::iter::Iterator for an LdbIterator.
pub struct LdbIteratorIter<'a, It: 'a> {
    inner: &'a mut It,
}

impl<'a, It: LdbIterator> LdbIteratorIter<'a, It> {
    pub fn wrap(it: &'a mut It) -> LdbIteratorIter<'a, It> {
        LdbIteratorIter { inner: it }
    }
}

impl<'a, It: LdbIterator> Iterator for LdbIteratorIter<'a, It> {
    type Item = (Vec<u8>, Vec<u8>);
    fn next(&mut self) -> Option<Self::Item> {
        LdbIterator::next(self.inner)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_test_util() {
        let v = vec![("abc".as_bytes(), "def".as_bytes()), ("abd".as_bytes(), "deg".as_bytes())];
        let mut iter = TestLdbIter::new(v);
        assert_eq!(iter.next(),
                   Some((Vec::from("abc".as_bytes()), Vec::from("def".as_bytes()))));
    }
}
