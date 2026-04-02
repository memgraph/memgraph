# Query Memory Tracking Plan

## Goal

Fix `db_query_memory_tracked` without changing query-limit behavior.

## Final Design

| Concern | Mechanism |
|---|---|
| DB/global query usage | DB-aware `QueryAllocator` / `ThreadSafeQueryAllocator` |
| Per-query limit enforcement | existing `QueryMemoryTracker` path |

## Implementation

| Area | Final behavior |
|---|---|
| Query execution memory | `QueryExecution` owns a DB-aware allocator |
| DB rollup | allocator tracking feeds `Database::db_query_memory_tracker_` |
| Global rollup | per-DB query tracker feeds `global_query_memory_tracker` |
| Query limits | `StartTrackingCurrentThread` remains conditional on query limit, as before |
| Trigger execution memory | before/after-commit trigger allocators use the DB query tracker |

## Coverage

| Area | Status |
|---|---|
| PMR-backed query execution memory | covered |
| Thread-safe PMR execution memory | covered |
| Trigger execution memory | covered |
| Raw non-PMR query allocations | not covered |

## Testing

| Done | Purpose |
|---|---|
| unit coverage for allocator-backed DB query tracking | proves `QueryAllocator` grows DB/global query memory |
| unit coverage for TLS query tracking isolation | proves `QueryMemoryTracker` no longer feeds DB/global query memory |

## Remaining Work

| Item | Notes |
|---|---|
| e2e regression for no-limit query accounting | should exercise real query execution through `QueryAllocator` |
| document raw non-PMR undercount boundary | accepted gap of the final design |
