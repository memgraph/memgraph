# DB-Specific Memory Tracking

**Author**
Andreja Tonev (https://github.com/andrejtonev)

**Status**
ACCEPTED

**Date**
2026-04-22

## Problem

Memgraph needs per-database memory accounting and tenant-profile enforcement without losing the existing global graph,
vector-index, and query-memory accounting semantics. The implementation has to work across foreground query execution,
long-lived storage structures, background DB-owned threads, short-lived worker-pool tasks, replication, durability
recovery, and cross-thread query execution. It also has to avoid relying on jemalloc thread binding, because returning
threads to normal per-CPU arena behavior is not portable through `thread.arena`.

## Criteria

1. **Correct attribution and enforcement** (Weight: 50%): allocations must charge the owning DB and tenant limit even
   when work moves to background or replica tasks.
1. **Ownership safety** (Weight: 30%): long-lived storage objects must deallocate through the same DB arena they were
   allocated from, even when TLS is not active or the freeing thread differs from the allocating thread.
1. **Performance and maintainability** (Weight: 20%): the design should keep hot allocation paths simple, avoid broad
   refactors, and leave room for profiling-driven contention improvements.

## Decision

Keep two allocator families and use them by object lifetime:

1. `ArenaAwareAllocator` and arena-aware owners capture an explicit DB arena at construction time. They are used for
   long-lived DB-owned storage structures, WAL ownership, index containers, and objects that may outlive the thread or
   query scope that created them.
1. `DbAwareAllocator` reads the active DB arena from TLS for transient work that runs under a known `DbArenaScope`, such
   as query, trigger, stream, and scoped task execution.

DB arena selection is TLS-based. `DbArenaScope` saves and restores the previous DB arena, so nested scopes are valid and
shared worker threads do not leak DB state between tasks. Long-lived DB-owned threads acquire a per-thread DB arena once
from `DbArena::AcquireThreadArena()`. Short-lived pool tasks scope work to the owning storage's base arena. Replication
and durability paths derive the DB arena from the owning storage or stream instead of carrying transaction-local arena
state.

Memory tracker hierarchy:

1. `db_memory_tracker_` tracks graph/storage memory and rolls up to `graph_memory_tracker` and the DB total tracker.
1. `db_embedding_memory_tracker_` tracks vector-index memory and rolls up to `vector_index_memory_tracker` and the DB
   total tracker.
1. `db_query_memory_tracker_` tracks query PMR memory for tenant enforcement and rolls up only to the DB total tracker.
   Global query accounting remains on the existing query-memory path.
1. `db_total_memory_tracker_` is the tenant-profile enforcement tracker and is not itself parented to a global tracker.

Tenant profiles set the hard limit on `db_total_memory_tracker_`. Profile durability is stored in DBMS KV records under
`tenant_profile:*`; reverse DB-to-profile lookup is rebuilt from those records on startup.

## Consequences

Per-DB memory tracking works across storage, vector indexes, query PMR allocations, triggers, streams, replication,
durability recovery, GC, TTL, and async indexing without requiring every deallocation path to have active TLS. The design
intentionally accepts that many long-lived structures allocate from a DB base arena, which may concentrate some arena-bin
contention. That trade-off is safer than a full TLS-only migration because it preserves ownership correctness for
cross-thread frees and DB-lifetime structures. A future migration to route more long-lived structures through acquired
per-thread arenas should be driven by profiling that demonstrates real contention.

Raw non-PMR query allocations remain outside `db_query_memory_tracker_`; they are still governed by the existing query
memory tracker path. Text-index/Tantivy memory is not included in DB embedding memory and would need a separate
text-index accounting design if product requirements call for it.
