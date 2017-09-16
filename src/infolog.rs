
use std::io::Write;

pub struct Logger(pub Box<Write>);

#[macro_export]
macro_rules! log {
    ($l:expr) => ($l.borrow_mut().0.write("\n".as_bytes()).is_ok());
    ($l:expr, $fmt:expr) => ($l.borrow_mut().0.write(concat!($fmt, "\n").as_bytes()).is_ok());
    ($l:expr, $fmt:expr, $($arg:tt)*) => ($l.borrow_mut().0.write_fmt(format_args!(concat!($fmt, "\n"), $($arg)*)).is_ok());
}
