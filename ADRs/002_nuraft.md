# NuRaft ADR

**Author**
Marko Budiselic (github.com/gitbuda)

**Status**
PROPOSED

**Date**
January 10, 2024

**Problem**

In order to enhance Memgraph to have High Availability features as requested by
customers, we want to have reliable coordinators backed by RAFT consensus algorithm. Implementing
RAFT to be correct and performant is a very challenging task. Skillful Memgraph
engineers already tried 3 times and failed to deliver in a reasonable timeframe
all three times (approximately 4 person-weeks of engineering work each time).

**Criteria**

- easy integration with our C++ codebase
- heavily tested in production environments
- implementation of performance optimizations on top of the canonical Raft
  implementation

**Decision**

There are a few, robust C++ implementations of Raft but as a part of other
projects or bigger libraries. **We select
[NuRaft](https://github.com/eBay/NuRaft)** because it focuses on delivering
Raft without bloatware, and it's used by
[Clickhouse](https://github.com/ClickHouse/ClickHouse) (an comparable peer to
Memgraph, a very well-established product).
