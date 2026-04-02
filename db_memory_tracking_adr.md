# ADR: Database Memory Tracking Hierarchy

**Status**
Accepted

**Date**
2026-04-02

## Decision

Keep query-limit tracking behavior unchanged, and move per-DB/global query usage accounting to `QueryAllocator` and `ThreadSafeQueryAllocator`.

## Final Hierarchy

| Bucket | Parent |
|---|---|
| Per-DB storage tracker | `graph_memory_tracker` |
| Per-DB embedding tracker | `vector_index_memory_tracker` |
| Per-DB query tracker | `global_query_memory_tracker` |
| Global graph / embeddings / query trackers | `total_memory_tracker` |

`DbMemoryUsage()` remains:

`DbStorageMemoryUsage() + DbEmbeddingMemoryUsage() + DbQueryMemoryUsage()`

## Query Tracking Split

| Concern | Final mechanism |
|---|---|
| DB/global query usage | `QueryAllocator` / `ThreadSafeQueryAllocator` with `TrackingMemoryResource` |
| Per-query limit enforcement | existing `QueryMemoryTracker` |

## Rationale

| Point | Reason |
|---|---|
| Safer accounting | avoids brittle query-level arena pinning |
| Better ownership | query usage is tied to `QueryExecution` lifetime |
| Preserved semantics | query-limit enforcement stays on the existing TLS path |
| Clear split | DB query usage and query limits are no longer mixed in one mechanism |

## Consequences

| Outcome | Impact |
|---|---|
| `db_query_memory_tracked` works for PMR-backed query execution memory | desired fix |
| query-limit behavior stays as before | low-risk change |
| raw non-PMR query allocations are still not counted | accepted gap |
