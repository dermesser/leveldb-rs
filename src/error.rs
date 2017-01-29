use std::convert::From;
use std::error::Error;
use std::fmt::{self, Display, Formatter};
use std::io;
use std::result;

#[derive(Clone, Debug)]
#[allow(dead_code)]
pub enum StatusCode {
    OK,

    AlreadyExists,
    Corruption,
    IOError,
    InvalidArgument,
    InvalidData,
    NotFound,
    NotSupported,
    PermissionDenied,
    Unknown,
}

#[derive(Clone, Debug)]
pub struct Status {
    code: StatusCode,
    err: String,
}

impl Default for Status {
    fn default() -> Status {
        Status {
            code: StatusCode::OK,
            err: String::new(),
        }
    }
}

impl Display for Status {
    fn fmt(&self, fmt: &mut Formatter) -> result::Result<(), fmt::Error> {
        fmt.write_str(self.description())
    }
}

impl Error for Status {
    fn description(&self) -> &str {
        &self.err
    }
}

impl Status {
    pub fn new(code: StatusCode, msg: &str) -> Status {
        let err;
        if msg.is_empty() {
            err = format!("{:?}", code)
        } else {
            err = format!("{:?}: {}", code, msg);
        }
        return Status {
            code: code,
            err: err,
        };
    }
}

impl From<io::Error> for Status {
    fn from(e: io::Error) -> Status {
        let c = match e.kind() {
            io::ErrorKind::NotFound => StatusCode::NotFound,
            io::ErrorKind::InvalidData => StatusCode::Corruption,
            io::ErrorKind::InvalidInput => StatusCode::InvalidArgument,
            io::ErrorKind::PermissionDenied => StatusCode::PermissionDenied,
            _ => StatusCode::IOError,
        };

        Status::new(c, e.description())
    }
}

/// LevelDB's result type
pub type Result<T> = result::Result<T, Status>;

pub fn from_io_result<T>(e: io::Result<T>) -> Result<T> {
    match e {
        Ok(r) => result::Result::Ok(r),
        Err(e) => Err(Status::from(e)),
    }
}
