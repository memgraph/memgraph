# RocksDB ADR

**Author**
Marko Budiselic (github.com/gitbuda)

**Status**
ACCEPTED

**Date**
January 23, 2024

**Problem**

Interacting with data (reads and writes) on disk in a concurrent, safe, and
fast way is a challenging task. Implementing all low-level primitives to
interact with various disk hardware efficiently consumes significant
engineering people. Whenever Memgraph has to store data on disk (or any
other colder than RAM storage system), the problem is how to do that in the
least amount of development time while satisfying all functional requirements
(often performance).

**Criteria**

- working efficiently in a highly concurrent environment
- easy integration with Memgraph's C++ codebase
- providing low-level key-value API
- heavily tested in production environments
- providing abstractions for the storage hardware (even for cloud-based
  storages like S3)

**Decision**

There are a few robust key-value stores, but finding one that is
production-ready and compatible with Memgraph's C++ codebase is challenging.
**We select [RocksDB](https://github.com/facebook/rocksdb)** because it
delivers robust API to manage data on disk; it's battle-tested in many
production environments (many databases systems are embedding RocksDB), and
it's the most compatible one.
