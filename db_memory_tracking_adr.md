# ADR: Database Memory Tracking Hierarchy

**Status**
Accepted

**Date**
2026-04-02

## Context

The `db_specific_memory_tracking` branch extends Memgraph's earlier per-database arena tracking so DB memory can be explained and reported in stable categories instead of one storage-heavy bucket.

The client requirement was to track storage and query usage similarly to the existing global limit, while preserving per-query enforcement and keeping the implementation under Memgraph's control.

Before this work:

| Area | Previous behavior |
|---|---|
| Storage | Long-lived storage allocations were attributed per DB via jemalloc arena hooks. |
| Query | Query-tracked memory was transaction-local and did not roll up per DB or into a global query bucket. |
| Embeddings | Vector index memory reported only to the global `vector_index_memory_tracker`. |
| DB totals | `DbMemoryUsage()` reflected the storage tracker only. |

## Decision

Use the existing `utils::MemoryTracker` parent chain as the single aggregation mechanism for DB and global memory reporting, but split query usage accounting from query limit enforcement.

The final hierarchy is:

| Bucket | Parent |
|---|---|
| Per-DB storage tracker | `graph_memory_tracker` |
| Per-DB embedding tracker | `vector_index_memory_tracker` |
| Per-DB query tracker | `global_query_memory_tracker` |
| Global graph / embeddings / query trackers | `total_memory_tracker` |

`DbMemoryUsage()` is defined as:

`DbStorageMemoryUsage() + DbEmbeddingMemoryUsage() + DbQueryMemoryUsage()`

`SHOW STORAGE INFO` exposes the DB split with:

| Field | Meaning |
|---|---|
| `db_storage_memory_tracked` | Per-DB long-lived storage usage |
| `db_embedding_memory_tracked` | Per-DB vector index / embedding usage |
| `db_query_memory_tracked` | Per-DB allocator-tracked query execution usage plus active TLS query-tracker rollup |
| `db_memory_tracked` | Combined DB total: storage + embeddings + query |

## Implementation Notes

| Area | Final design |
|---|---|
| Query rollup | `QueryAllocator` / `ThreadSafeQueryAllocator` now use `TrackingMemoryResource` pointed at `Database::db_query_memory_tracker_`, so PMR-backed `QueryExecution` memory rolls up by DB and then globally. |
| Query limits | `QueryMemoryTracker` remains the per-query enforcement mechanism and is started unconditionally from `PullPlan::Pull`, even for no-limit queries. |
| Embedding rollup | `Database` owns `db_embedding_memory_tracker_`, passes it through `storage::Config`, and `Indices` injects it into `VectorIndex` and `VectorEdgeIndex`. |
| Vector allocator binding | `TrackedVectorAllocator` captures the DB embedding tracker at vector-index construction time via a scoped TLS default, so later usearch allocations/free operations stay attributed to the owning DB. |
| Storage rollup | Existing per-DB jemalloc arena hooks remain the source of long-lived storage attribution. |
| Accepted query gap | Raw non-PMR allocations remain outside the allocator-based query usage signal; this is an intentional safety/coverage tradeoff. |

## Consequences

| Outcome | Impact |
|---|---|
| Better observability | DB memory is now explainable as storage, embeddings, and query components. |
| Minimal new machinery | No second aggregation system was introduced; parented `MemoryTracker`s remain the only rollup mechanism. |
| Safer query accounting | Query usage attribution is allocator-owned and avoids brittle thread-arena swapping. |
| Preserved limits | Existing per-query limit enforcement semantics remain in `QueryMemoryTracker`. |
| Accepted partial coverage | DB/global query usage intentionally undercounts raw non-PMR query allocations. |
| Stronger regression coverage | A focused invariant test now checks that `DbMemoryUsage()` always equals the sum of storage, embedding, and query subtotals. |

## Verification

The branch landed in small verified slices:

| Slice | Commit |
|---|---|
| Per-DB/global query rollup | `598d29943` |
| `SHOW STORAGE INFO` query reporting | `a9e646723` |
| Per-DB embedding tracking | `308f5d24a` |
| Combined DB totals | `14630fd46` |
| DB total invariant coverage | `59799303d` |

The query-tracking design was later refined further on the branch:

| Slice | Result |
|---|---|
| unconditional query TLS tracking | no-limit queries still participate in query limit tracking |
| DB-aware `QueryAllocator` / `ThreadSafeQueryAllocator` | PMR-backed query execution memory rolls into the DB/global query tracker |
| removal of query-level arena approach | allocator-based query usage replaced the more brittle query-arena experiment |
