# USearch ADR

**Author**
Marko Budiselic (github.com/gitbuda), David Ivekovic (github.com/DavIvek)

**Status**
APPROVED

**Date**
November 15, 2024

## Problem

Some Memgraph workloads require efficient vector search capabilities. Building
a new vector search engine from scratch is not aligned with Memgraph’s core
value proposition.

## Criteria

- Easy integration with our C++ codebase
- Ability to operate both in-memory and on-disk
- Thread safety
- Production-grade performance and reliability
- Sufficient feature set (filtering and predicate functions, quantization support, serialization, and serving index from disk)

## Decision

[USearch](https://github.com/unum-cloud/usearch) demonstrates the best
performance among available options, ensuring high efficiency for vector search
workloads. USearch is adopted by other major databases such as LanternDB,
ClickHouse, and YugaByte, further validating its maturity and reliability. As
an open-source project, it benefits from active community support and
transparent development. USearch is natively written in C++, making direct
integration into the Memgraph codebase straightforward, with no need for
language bridges or wrappers. **We select USearch.**
