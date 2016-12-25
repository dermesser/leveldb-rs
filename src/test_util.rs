use types::{cmp, LdbIterator};
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

impl<'a> Iterator for TestLdbIter<'a> {
    type Item = (&'a [u8], &'a [u8]);

    fn next(&mut self) -> Option<Self::Item> {
        if self.ix == self.v.len() {
            return None;
        } else if !self.init {
            self.init = true;
            Some(self.v[self.ix])
        } else {
            self.ix += 1;
            Some(self.v[self.ix - 1])
        }
    }
}

impl<'a> LdbIterator for TestLdbIter<'a> {
    fn reset(&mut self) {
        self.ix = 0;
        self.init = false;
    }
    fn current(&self) -> Option<Self::Item> {
        if self.init && self.ix < self.v.len() {
            Some(self.v[self.ix])
        } else {
            None
        }
    }
    fn valid(&self) -> bool {
        self.init
    }
    fn seek(&mut self, k: &[u8]) {
        self.ix = 0;
        while self.ix < self.v.len() && cmp(self.v[self.ix].0, k) == Ordering::Less {
            self.ix += 1;
        }
    }
    fn prev(&mut self) -> Option<Self::Item> {
        if !self.init || self.ix == 0 {
            None
        } else {
            self.ix -= 1;
            Some(self.v[self.ix])
        }
    }
}
