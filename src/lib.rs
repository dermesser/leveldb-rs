extern crate rand;
extern crate integer_encoding;

pub use skipmap::Comparator;

mod memtable;
mod skipmap;
mod types;

#[cfg(test)]
mod tests {
}
