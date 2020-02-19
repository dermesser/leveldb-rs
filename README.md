# leveldb-rs

[![crates.io](https://img.shields.io/crates/v/rusty-leveldb.svg)](https://crates.io/crates/rusty-leveldb)
[![Travis
CI](https://api.travis-ci.org/dermesser/leveldb-rs.svg?branch=master)](https://travis-ci.org/dermesser/leveldb-rs)

A fully compatible implementation of LevelDB in Rust. (any incompatibility is a
bug!)

The implementation is very close to the original; often, you can see the same
algorithm translated 1:1, and class (struct) and method names are similar or
the same.

**NOTE: I do not endorse using this library for any data that you care about.**
I do care, however, about bug reports.

## Status

* User-facing methods exist: Read/Write/Delete; snapshots; iteration
* Compaction is supported, including manual ones.
* Fully synchronous: Efficiency gains by using non-atomic types, but writes may
  occasionally block during a compaction. In --release mode, an average compaction
  takes 0.2-0.5 seconds.
* Compatible with the original implementation. If it isn't (crash/read error/write error), it's a bug and needs to be fixed.
* Performance is decent; while not quite up to par with the original (we don't use multithreading, for example) it is very much usable.
* Safe: Many places use asserts though, so you may rarely see a crash -- in which case you should file a bug.

## Goals

Some of the goals of this implementation are

* As few copies of data as possible; most of the time, slices of bytes (`&[u8]`)
  are used. Owned memory is represented as `Vec<u8>` (and then possibly borrowed
  as slice). Zero-copy is not always possible, though, and sometimes simplicity is favored.
* Correctness -- self-checking implementation, good test coverage, etc. Just
  like the original implementation.
* Clarity; commented code, clear structure (hopefully doing a better job than
  the original implementation).
* Coming close-ish to the original implementation; clarifying the translation of
  typical C++ constructs to Rust.
