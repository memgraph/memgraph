# DB Memory Tracking Project

**Status**
Planned

**Branch**
`db_specific_memory_tracking`

**Date**
2026-04-02

## Goal

Extend the current per-DB storage memory tracking so that memory usage is split into stable categories and aggregated on both the database and global levels.

The target hierarchy is:

1. Long-lived storage objects
2. Embeddings per storage
3. Database-specific query sum
4. Database-specific memory usage = `1 + 2 + 3`
5. Global graph = long-lived objects on all DBs
6. Global embeddings = embeddings on all DBs
7. Global query = query-specific usage on all DBs
8. Global usage = `5 + 6 + 7`

## Current State

| Area | Current state | Notes |
|---|---|---|
| Long-lived storage objects | Mostly implemented | Per-DB jemalloc arena + arena-aware allocators are already in place. |
| Per-DB embeddings | Missing | Vector index allocations currently report to the global vector tracker only. |
| Per-DB query aggregate | Missing | Query tracking is currently transaction-local and does not parent into a DB tracker. |
| Global graph | Implemented | `graph_memory_tracker` exists. |
| Global embeddings | Implemented | `vector_index_memory_tracker` exists. |
| Global query | Missing | No dedicated global query tracker exists yet. |
| Combined DB totals | Partial | Current `DbMemoryUsage()` is storage-oriented, not yet `storage + embeddings + query`. |

## Design Direction

Use the existing `utils::MemoryTracker` parent chaining to build the hierarchy instead of introducing a second aggregation mechanism.

### Planned tracker hierarchy

| Tracker | Parent |
|---|---|
| Per-query transaction tracker | Per-DB query tracker |
| Per-DB query tracker | Global query tracker |
| Per-DB embedding tracker | Global embeddings tracker |
| Per-DB storage tracker | Global graph tracker |
| Global graph/query/embeddings trackers | Total tracker |

### Important implementation rule

Per-query limit enforcement should remain local to the transaction/query tracker. Aggregation should happen through parent trackers only.

## Task Breakdown

| Task ID | Task | Scope | Acceptance criteria | Status |
|---|---|---|---|---|
| T1 | Add query tracker hierarchy | Add global query tracker and per-DB query tracker | Query allocations on a DB roll into DB-query and global-query buckets without breaking existing query limits | Planned |
| T2 | Thread DB query tracker into transactions | Make `QueryMemoryTracker` parent-aware and construct it from the owning DB | Existing query-tracked allocations aggregate correctly per DB | Planned |
| T3 | Expose query totals in DB/system reporting | Extend `SHOW STORAGE INFO` and any relevant internal reporting | Query subtotal and combined DB total are visible and stable | Planned |
| T4 | Add embedding tracker hierarchy | Add per-DB embedding tracker under global embeddings | Vector/embedding allocations on one DB do not inflate another DB | Planned |
| T5 | Thread embedding tracker into vector index allocators | Replace hardcoded global vector tracker path with injected DB-aware tracker | Vector index creation/recovery/search-owned buffers attribute to the correct DB and global bucket | Planned |
| T6 | Recompute combined DB totals | Make DB total reflect `storage + embeddings + query` | `DbMemoryUsage()` or equivalent combined reporting matches the intended hierarchy | Completed |
| T7 | Expand automated coverage | Add unit/e2e coverage for query and embeddings split | Each landed task has focused tests proving correctness and isolation | Planned |
| T8 | Cleanup and documentation | Update comments/ADR/project file with landed behavior | Comments and docs reflect final semantics without stale assumptions | Planned |

## Execution Order

| Phase | Tasks | Why this order |
|---|---|---|
| Phase 1 | T1, T2, T3 | Query tracking is the smallest missing piece and already has centralized hook/control points. |
| Phase 2 | T4, T5 | Embeddings are conceptually simple but require allocator plumbing and careful ownership. |
| Phase 3 | T6, T7, T8 | Final totals, hardening, and documentation after both aggregation branches exist. |

## Task Details

### T1. Add query tracker hierarchy

| Item | Description |
|---|---|
| Code areas | `src/utils/memory_tracker.*`, `src/dbms/database.*` |
| Main change | Add `global_query_memory_tracker` and per-DB query tracker in `Database` |
| Risk | Low |
| Test plan | Unit test for parent-chained query tracking and global rollup |

### T2. Thread DB query tracker into transactions

| Item | Description |
|---|---|
| Code areas | `src/utils/query_memory_tracker.*`, `src/storage/v2/transaction.hpp`, transaction construction sites |
| Main change | Make `QueryMemoryTracker` construct its transaction tracker with a parent |
| Risk | Medium |
| Test plan | Unit test that one transaction updates DB-query and global-query, and that another DB remains unchanged |

### T3. Expose query totals in reporting

| Item | Description |
|---|---|
| Code areas | `src/query/interpreter.cpp`, `src/dbms/database.hpp` and related reporting helpers |
| Main change | Report query subtotal and combined DB total |
| Risk | Low |
| Test plan | E2E `SHOW STORAGE INFO` checks for field presence, parseability, and growth |

### T4. Add embedding tracker hierarchy

| Item | Description |
|---|---|
| Code areas | `src/utils/memory_tracker.*`, `src/dbms/database.*` |
| Main change | Add per-DB embedding tracker parented to global embeddings tracker |
| Risk | Low |
| Test plan | Unit test for per-DB embedding rollup into global embeddings |

### T5. Thread embedding tracker into vector index allocators

| Item | Description |
|---|---|
| Code areas | `src/storage/v2/indices/tracked_vector_allocator.hpp`, vector index/edge index construction and recovery paths |
| Main change | Replace hardcoded use of `vector_index_memory_tracker` with injected tracker ownership |
| Risk | Medium |
| Test plan | Unit or e2e test showing vector index memory grows in the correct DB only |

### T6. Recompute combined DB totals

| Item | Description |
|---|---|
| Code areas | `src/dbms/database.*`, `src/query/interpreter.cpp` |
| Main change | Combined DB total becomes `storage + embeddings + query` |
| Risk | Low |
| Test plan | Reporting test proving sum consistency |

### T7. Expand automated coverage

| Item | Description |
|---|---|
| Code areas | `tests/unit/db_memory_tracking.cpp`, relevant e2e suites |
| Main change | Add focused tests for each completed slice |
| Risk | Low |
| Test plan | Per-task tests only, avoid giant all-in-one coverage first |

### T8. Cleanup and documentation

| Item | Description |
|---|---|
| Code areas | Comments, ADRs, project file |
| Main change | Remove stale comments and document final semantics |
| Risk | Low |
| Test plan | N/A |

## Definition Of Done Per Task

| Step | Requirement |
|---|---|
| 1 | Implement one task only |
| 2 | Add or update a focused test for that task |
| 3 | Build the smallest relevant target set |
| 4 | Run the focused test(s) |
| 5 | Sanity-check behavior and comments |
| 6 | Update this file status/progress |
| 7 | Commit only that task |

## Build And Test Strategy

| Task | Minimum build target | Minimum tests |
|---|---|---|
| T1-T3 | Relevant unit target plus any changed e2e target if reporting changes | `tests/unit/db_memory_tracking.cpp` additions and `tests/e2e/configuration/db_memory_tracking.py` if reporting changes |
| T4-T5 | Unit target covering vector index code paths | New vector/embedding tracking unit tests, plus e2e if exposed via reporting |
| T6 | Same as above | Reporting consistency checks |
| T7 | Only the newly relevant targets | Focused regression tests for the landed slice |

## Progress Tracker

| Task | Status | Tests added | Build done | Tests run | Commit |
|---|---|---|---|---|---|
| T1 | Completed | Yes | Yes | Yes | Yes (`598d29943`) |
| T2 | Completed | Yes | Yes | Yes | Yes (`598d29943`) |
| T3 | Completed | Yes | Yes | Yes | No |
| T4 | Completed | Yes | Yes | Yes | No |
| T5 | Completed | Yes | Yes | Yes | No |
| T6 | Not started | No | No | No | No |
| T7 | Not started | No | No | No | No |
| T8 | Not started | No | No | No | No |

## Notes

| Date | Note |
|---|---|
| 2026-04-02 | Initial project tracker created. Query aggregation and per-DB embeddings are the two main missing pieces relative to the target hierarchy. |
| 2026-04-02 | Started first implementation slice: added global/per-DB query tracker plumbing and focused unit coverage for rollup plus cross-DB isolation. |
| 2026-04-02 | Query aggregation slice landed in commit `598d29943` (`Add per-db and global query memory rollup`). |
| 2026-04-02 | Reporting slice verified: `SHOW STORAGE INFO` now exposes `db_storage_memory_tracked`, `db_query_memory_tracked`, `db_memory_tracked = storage + query`, and global `query_memory_tracked`; interpreter unit coverage passes for both in-memory and disk storage backends. |
| 2026-04-02 | Embedding slice verified: vector index allocators now capture a DB-specific embedding tracker at index construction time, so populated vector index creation rolls into `DbEmbeddingMemoryUsage()` for the owning DB and into the global `vector_index_memory_tracker`, with drop returning both to baseline. |
| 2026-04-02 | Combined-total slice verified: `DbMemoryUsage()` now reflects `storage + embeddings + query`, and `SHOW STORAGE INFO` exposes `db_embedding_memory_tracked` while folding embeddings into `db_memory_tracked`. |
