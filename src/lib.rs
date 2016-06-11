
#![allow(dead_code)]

extern crate crc;
extern crate rand;
extern crate integer_encoding;

pub use skipmap::Comparator;

mod log;
mod memtable;
mod skipmap;
mod types;

#[cfg(test)]
mod tests {
}
