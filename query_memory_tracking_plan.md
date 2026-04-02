# Query Memory Tracking Plan

## Goal

Make query memory tracking correct for both:

1. database/global query usage accounting
2. per-query memory limit enforcement

These two concerns must work in parallel, but they do not need to use the same mechanism.

## Problem Summary

The current query tracking path is TLS-based and pull-scoped:

- `StartTrackingCurrentThread` is currently started from `PullPlan::Pull`
- `StopTrackingCurrentThread` clears the TLS tracker at the end of the pull scope
- `TrackFreeOnCurrentThread` only decrements the tracker if that TLS pointer is still set
- `PullPlan` builds the cursor chain before tracking starts

This creates two correctness gaps:

1. frees that happen after `StopTrackingCurrentThread` are not charged back
2. cursor/plan allocations created before `PullPlan::Pull` are not included

Because of that, the current TLS tracker is not a sound source of truth for resident query memory usage.

## Design Direction

Split responsibilities:

| Concern | Preferred mechanism | Why |
|---|---|---|
| DB/global query usage | DB-aware `QueryAllocator` / `ThreadSafeQueryAllocator` | safe, query-execution-owned, no fragile thread-arena state changes |
| Per-query limit enforcement | existing `QueryMemoryTracker` path | already integrated with limits, procedures, and user-resource checks |

This means we should treat usage accounting and limit enforcement as complementary systems:

- the query allocator answers "how much PMR-backed query execution memory is currently owned by this DB/query?"
- `QueryMemoryTracker` answers "is this query allowed to allocate more right now?"

## Target Model

### Usage accounting hierarchy

1. `QueryExecution` owns a DB-aware `QueryAllocator` / `ThreadSafeQueryAllocator`
2. allocator-backed tracking feeds a per-DB query memory tracker
3. per-DB query memory tracker feeds the global query memory tracker

### Limit hierarchy

1. per-query allocations still pass through `QueryMemoryTracker`
2. `QueryMemoryTracker` continues to enforce the query memory limit
3. procedure and user-resource tracking continues to layer on top of that mechanism

## Important Constraint

Query execution tracking must not be merged with the long-lived storage arena/bucket.

Reason:

- long-lived graph/storage objects and short-lived query objects are different buckets
- the client explicitly wants storage/query usage separated
- the DB total should be `storage + embeddings + query`, not a merged bucket

## Current Relevant Facts

| Area | Current behavior | Source |
|---|---|---|
| Query TLS tracking | allocation/free accounting depends on TLS `QueryMemoryTracker *` | `src/memory/query_memory_control.cpp` |
| Pull scope | tracker starts in `PullPlan::Pull` and stops on scope exit | `src/query/interpreter.cpp` |
| Cursor construction | cursor chain is created in `PullPlan` ctor before pull tracking starts | `src/query/interpreter.cpp` |
| Query lifetime | execution memory lives on `QueryExecution` and can outlive active pull | `src/query/interpreter.hpp` |
| Query allocator lifetime | execution memory is owned by `QueryExecution`, not the storage transaction | `src/query/interpreter.hpp` |
| Cross-thread handoff | query tracker and DB arena idx are propagated today | `src/memory/query_memory_control.cpp` |

## Implemented Architecture

### A. DB query usage via DB-aware query allocator

`QueryAllocator` / `ThreadSafeQueryAllocator` are now explicitly DB-aware:

- `utils::TrackingMemoryResource` sits in the allocator chain
- that tracking resource points at `Database::DbQueryMemoryTracker()`
- ownership stays on `QueryExecution`, not on `Database` lifetime

Properties:

- separate from storage and embeddings
- naturally decremented when query execution memory is released
- no need for fragile temporary `thread.arena` pinning

### B. Per-query limit enforcement via `QueryMemoryTracker`

The current query limit mechanism stays in place, with tracking now unconditional during query execution:

- call `StartTrackingCurrentThread` regardless of whether a limit is set
- keep `SetQueryLimit(UNLIMITED_MEMORY)` semantics for no-limit queries
- continue using procedure/user-resource tracking on top of this path

This does not make usage accounting correct by itself, but it keeps one consistent enforcement pipeline.

### C. Both systems active together

For a PMR-backed query allocation on the main query thread, we want both:

1. allocation tracked into the DB query tracker for usage attribution
2. allocation observed by `QueryMemoryTracker` for per-query limit enforcement

The same principle should hold on worker threads spawned for parallel execution.

## Coverage Summary

| Area | Status | Notes |
|---|---|---|
| PMR-backed `QueryExecution` memory | covered | primary DB/global query usage signal |
| Thread-safe PMR execution memory | covered in design | routed through `ThreadSafeQueryAllocator` |
| Trigger execution memory | covered | before/after-commit trigger allocators now point at the DB query tracker |
| Per-query limit enforcement | covered | still uses `QueryMemoryTracker` |
| Raw non-PMR allocations | not covered | accepted undercount gap for now |
| Pre-query parser/planner allocations | not covered | outside `QueryExecution` allocator ownership |

## Implementation Plan

| ID | Task | Outcome | Notes |
|---|---|---|---|
| Q1 | Document current behavior and desired split | shared design baseline | this document |
| Q2 | Make query TLS tracking unconditional | completed | keeps no-limit queries on the same enforcement/tracking entry path; does not solve resident-usage correctness alone |
| Q3 | Make `QueryAllocator` / `ThreadSafeQueryAllocator` DB-aware and tracked | completed | primary usage-accounting path |
| Q4 | Thread DB query tracker ownership through `QueryExecution` and trigger execution memory | completed | query-owned PMR allocations roll into DB/global query usage without arena pinning |
| Q5 | Validate coexistence of allocator tracking and query limits | completed | usage and limits now work in parallel |
| Q6 | Measure/document allocator coverage and accepted gaps | completed | raw non-PMR allocations remain the accepted gap |
| Q7 | Expose and verify final reporting semantics | completed | `SHOW STORAGE INFO` already reports query as a separate bucket |
| Q8 | Write ADR/final docs after code lands | in progress | final branch handoff cleanup |

## Test Plan

| ID | Scenario | Expected result |
|---|---|---|
| T1 | query without explicit memory limit | query usage still tracked at DB/global query level |
| T2 | query execution PMR memory allocation | usage appears in DB/global query bucket |
| T3 | query frees memory after pull/untracking | DB/global query usage drops correctly |
| T4 | query exceeds per-query limit | `QueryMemoryTracker` still throws/enforces correctly |
| T5 | parallel query allocates through `ThreadSafeQueryAllocator` | usage attributed to same DB query bucket and limit path still works |
| T6 | two DBs run queries concurrently | query usage remains isolated per DB and summed globally |
| T7 | DB total invariant | `db_memory_tracked = storage + embeddings + query` remains true |

## Testing Notes

| Note | Why it matters |
|---|---|
| The current unlimited-query unit regression is not sufficient on its own | it can still pass when tracking is enabled manually in the test, even if the interpreter path regresses |
| We need an interpreter/e2e-level regression for no-limit queries | the real contract is that executing a query without an explicit memory limit still updates DB/global query usage |
| Trigger handoff is not the first blocker | after-commit triggers already run on a DB-pinned thread, and before-commit triggers run on the commit thread; we can focus first on normal query execution memory |
| Temporary `thread.arena` swapping for `MakeCursor(...)` was rejected | restoring the previous jemalloc arena on reused query threads failed in practice, so this approach is too brittle to ship |
| Allocator tracking is intentionally partial | it is safe and query-owned, but it will miss raw non-PMR allocations |

## TODOs

| ID | Todo | Status | Notes |
|---|---|---|---|
| TODO-E2E-1 | Add an e2e/interpreter-level regression for unlimited-query tracking | pending | should fail if `PullPlan` stops starting query tracking for no-limit queries |
| TODO-QA-1 | Measure and document raw non-PMR query-allocation gaps | pending | allocator-based design intentionally accepts partial coverage |

## Risks

| Risk | Impact | Mitigation |
|---|---|---|
| Allocator tracking misses raw allocations | undercounted DB query usage | accept as a known gap, document it, and keep limits on `QueryMemoryTracker` |
| Limit enforcement and usage accounting diverge | confusing metrics or false assumptions | clearly define usage vs enforcement responsibilities |
| Parallel worker propagation is incomplete | missing memory from parallel query branches | ensure `ThreadSafeQueryAllocator` path is covered in tests |
| Procedures have custom tracking behavior | regressions in procedure memory limits | include procedure path in verification |

## Recommended Next Slice

Focus on coverage hardening and product semantics:

1. add the missing e2e regression for unlimited-query tracking
2. measure/document the raw non-PMR undercount boundary
3. add targeted tests for parallel `ThreadSafeQueryAllocator` attribution if not already covered elsewhere

The core design is now in place; the remaining work is mostly around coverage confidence and documentation.

## Progress

| Task | Status | Notes |
|---|---|---|
| Q1 | completed | planning document created |
| Q2 | completed | `PullPlan::Pull` now starts `QueryMemoryTracker` regardless of explicit query limit |
| Q3 | completed | `QueryAllocator` / `ThreadSafeQueryAllocator` now point at the DB query tracker directly |
| Q4 | completed | `QueryExecution` and trigger execution memory are wired to the DB query tracker |
| Q5 | completed | allocator usage tracking and `QueryMemoryTracker` limit enforcement coexist |
| Q6 | completed | accepted allocator coverage boundary documented here |
| Q7 | completed | final reporting remains `storage + embeddings + query` |
| Q8 | in progress | ADR and KG cleanup for the allocator-based final design |
