
pub enum ValueType {
    TypeDeletion = 0,
    TypeValue = 1,
}

pub type SequenceNumber = u64;

pub enum Status {
    OK,
    NotFound(String),
    Corruption(String),
    NotSupported(String),
    InvalidArgument(String),
    IOError(String),
}
