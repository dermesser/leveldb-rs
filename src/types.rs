
pub enum ValueType {
    TypeDeletion = 0,
    TypeValue = 1,
}

pub type SequenceNumber = u64;

#[allow(dead_code)]
pub enum Status {
    OK,
    NotFound(String),
    Corruption(String),
    NotSupported(String),
    InvalidArgument(String),
    IOError(String),
}

pub trait LdbIterator<'a>: Iterator {
    fn seek(&mut self, key: &Vec<u8>);
    fn valid(&self) -> bool;
    fn current(&'a self) -> Self::Item;
}
