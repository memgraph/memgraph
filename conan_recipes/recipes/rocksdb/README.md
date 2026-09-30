# RocksDB Recipe Notes

This recipe carries two Memgraph-specific patch sets:

- `all/patches/rocksdb-v8.1.1.patch`
  Existing Memgraph fixes for RocksDB `8.1.1`.

- `all/patches/rocksdb-v8.1.1-gcc15.patch`
  Compatibility fixes needed to build RocksDB `8.1.1` with the current
  server toolchain (`clang++` with GCC 15 `libstdc++`).

## Why the extra patch exists

RocksDB `8.1.1` built successfully on an older desktop toolchain, but failed on
 the server toolchain because newer `libstdc++` is stricter about some patterns
 that upstream RocksDB still uses.

The patch fixes:

- Incomplete-type ownership patterns where `std::unique_ptr<T>` was used in
  inline-defined classes before `T` was fully defined.
- A vendored `xxhash` C++23 path that included `<utility>` inside an
  `extern "C"` block, which newer headers reject.

## What was changed

The compatibility patch updates these RocksDB areas:

- `db/range_tombstone_fragmenter`
- `table/block_based/block_based_table_builder`
- `utilities/write_batch_with_index/write_batch_with_index_internal`
- `util/xxhash.h`

These changes are intended to be minimal build-compatibility fixes, not
behavioral changes to RocksDB itself.
