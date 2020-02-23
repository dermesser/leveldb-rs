# kvserver

A simplistic Key/Value HTTP server using rusty-leveldb.

It is not multi-threaded, and achieves around 13'000 ops per second (both
fetching and storing keys) on my somewhat old `Intel(R) Xeon(R) CPU E5-1650 v2 @
3.50GHz` using `ab`.

For comparison, writing of random keys directly (in-process) usually happens at
300'000 keys per second.
