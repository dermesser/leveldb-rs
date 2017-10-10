# leveldb-rs

**Goal:** A fully compatible implementation of LevelDB in Rust.

The implementation is very close to the original; often, you can see the same
algorithm translated 1:1, and class (struct) and method names are similar or
the same.

## Status

* User-facing methods exist: Read/Write/Delete; snapshots; iteration
* Compaction is supported, including manual ones.
* Fully synchronous: Efficiency gains by using non-atomic types, but writes may
  occasionally block during a compaction. In --release mode, an average compaction
  takes 0.2-0.5 seconds.
* Compatibility with the original: Compression is not implemented so far; this works
  as long as compression is disabled in the original.
* Performance is usually on par with the original, except in cases where the
  original implementation makes use of multithreading (e.g., background compactions).

## Goals

Some of the goals of this implementation are

* As few copies of data as possible; most of the time, slices of bytes (`&[u8]`)
  are used. Owned memory is represented as `Vec<u8>` (and then possibly borrowed
  as slice).
* Correctness -- self-checking implementation, good test coverage, etc. Just
  like the original implementation.
* Clarity; commented code, clear structure (hopefully doing a better job than
  the original implementation).
* Coming close-ish to the original implementation; clarifying the translation of
  typical C++ constructs to Rust.
