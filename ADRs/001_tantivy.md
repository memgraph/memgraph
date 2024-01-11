# Tantivy ADR

**Author**
Marko Budiselic (github.com/gitbuda)

**Status**
PROPOSED

**Date**
January 5, 2024

**Problem**

For some of Memgraph workloads, text search is a required feature. We don't
want to build a new text search engine because that's not Memgraph's core
value.

**Criteria**

- easy integration with our C++ codebase
- ability to operate in-memory and on-disk
- sufficient features (regex, full-text search, fuzzy search, aggregations over
  text data)
- production-ready

**Decision**

All known C++ libraries are not production-ready. Recent Rust libraries, in
particular [Tantivy](https://github.com/quickwit-oss/tantivy), seem to provide
much more features, it is production ready. The way how we'll integrate Tantivy
into the current Memgraph codebase is via
[cxx](https://github.com/dtolnay/cxx). **We select Tantivy.**
