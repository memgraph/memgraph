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

Use a single **stateless DbAwareAllocator** that reads the active DB arena from thread-local storage
(`tls_db_arena_state.arena`) at both allocation and deallocation time. Since jemalloc tracks the owning arena per-extent
in its own metadata, deallocations are correctly routed regardless of which thread performs the free.

For long-lived objects that must allocate/deallocate consistently (even when TLS may not have the arena pinned), use
**explicit arena helpers** (`ArenaAwareUniquePtr`, `MakeDbAwareUnique`) that capture the arena index at construction time
and store it for the object's lifetime.

### Arena Selection via TLS (DbArenaScope)

The primary mechanism for DB arena selection is TLS-based via `DbArenaScope`:

- `DbArenaScope` acquires an arena from `DbArena::AcquireThreadArena()` at construction and restores the previous arena
  at destruction (RAII pattern). Nested scopes are valid.
- The scope makes the arena available via `tls_db_arena_state.arena` for `DbAwareAllocator`.
- `DbAwareAllocator<T>` reads `tls_db_arena_state.arena` at both `allocate()` and `deallocate()` time.
- Short-lived pool tasks scope work to the owning storage's base arena via `ExecutionContext::db_arena_pool`.

Since the allocator is **stateless** (zero data members), it qualifies for Empty Base Optimization (EBO) in containers,
so their `sizeof` is unchanged vs. the plain `std::allocator<T>` case.

### Explicit Arena Helpers (for long-lived objects)

For objects that need to allocate/deallocate from a specific arena regardless of TLS state:

- **`ArenaAwareUniquePtr<T>`**: `std::unique_ptr` with custom deleter that calls `DbDeallocateBytes` with the captured arena
- **`ArenaAwareDeleter<T>`**: Custom deleter used by `ArenaAwareUniquePtr`
- **`MakeDbAwareUnique<T>(Args...)`**: Allocates via `DbAllocateBytes` with current TLS arena, constructs object
- **`DbAllocate<T>(n, idx)` / `DbDeallocate<T>(p, n)`**: Low-level typed allocation helpers

These are used for:
- WAL file ownership (explicit arena per WAL)
- Compressed/decompressed buffer management in compressor
- Cases where allocation must happen under one arena but deallocation may occur on a different thread

### Long-lived DB-owned threads

Background threads acquire a per-thread DB arena once via `DbAwareThread`:

- `DbAwareThread` wraps `std::jthread` and calls `DbArena::AcquireThreadArena()` at thread start
- Used for: async indexer, GC runner, TTL, and similar background tasks
- The arena remains pinned for the thread's entire lifetime via TLS

### Replication and durability

Paths derive the DB arena from the owning storage or stream instead of carrying transaction-local arena state:

- All replication handlers wrap storage access with `DbArenaScope` to ensure proper attribution
- `HeartbeatHandler`, `PrepareCommitHandler`, `FinalizeCommitHandler`, `SnapshotHandler`, `WalFilesHandler`,
  `CurrentWalHandler` all use `DbArenaScope`

### Extent Hooks (jemalloc Integration)

Custom jemalloc extent hooks (`DbArenaHooks`) are installed on DB-specific arenas:

- **db_arena_alloc**: Pre-tracks memory before base hook call; rolls back on failure; handles out-param `*commit`
- **db_arena_dalloc**: Updates tracker on deallocation via base hook
- **db_arena_commit**: Uses `OutOfMemoryExceptionBlocker` because OS already committed pages (cannot rollback)
- **db_arena_decommit**: Updates tracker on decommit
- **db_arena_destroy**: Updates tracker on extent destruction

The hooks propagate to `graph_memory_tracker` (for graph allocations) or the DB-specific tracker hierarchy.

### ArenaPool Destruction Safety

`ArenaPool::~ArenaPool()` implements a critical ordering for safe cleanup:

1. **Purge** all dirty/muzzy pages back to the OS FIRST (while custom hooks are still installed)
2. **Restore** default hooks AFTER purging
3. **Release** arena back to `GlobalArenaPool`

This ensures extent hook callbacks can safely update trackers during cleanup. Error handling logs failures but doesn't
throw (destructor is `noexcept`).

### Memory Tracker Hierarchy

Each Database has four memory trackers with specific parent relationships:

```cpp
utils::MemoryTracker db_total_memory_tracker_;                    // 0-parent (enforcement only)
utils::MemoryTracker db_memory_tracker_{&graph_memory_tracker,     // 2-parent (storage)
                                        &db_total_memory_tracker_};
utils::MemoryTracker db_embedding_memory_tracker_{&vector_index_memory_tracker,  // 2-parent (embeddings)
                                                   &db_total_memory_tracker_};
utils::MemoryTracker db_query_memory_tracker_{&db_total_memory_tracker_};        // 1-parent (query)
```

Tracker semantics:
1. **db_memory_tracker_**: Tracks graph/storage memory; rolls up to `graph_memory_tracker` (global domain) AND the DB
   total tracker (tenant enforcement).
1. **db_embedding_memory_tracker_**: Tracks vector-index memory; rolls up to `vector_index_memory_tracker` AND the DB
   total tracker.
1. **db_query_memory_tracker_**: Tracks query PMR memory for tenant enforcement ONLY (single parent to db_total).
   **Query memory is intentionally NOT aggregated to graph_memory_tracker** to avoid double-counting (query PMR bytes
   were being counted twice: once via `TrackingMemoryResource::Alloc` and once via arena hooks).
1. **db_total_memory_tracker_**: The tenant-profile enforcement tracker (hard limit); has no parent trackers.

### Database Destruction Order Safety

The Database class declares trackers BEFORE `db_arena_` to ensure safe destruction:

```cpp
// Trackers declared first (lines 220-229)
utils::MemoryTracker db_total_memory_tracker_;
utils::MemoryTracker db_memory_tracker_;
utils::MemoryTracker db_embedding_memory_tracker_;
utils::MemoryTracker db_query_memory_tracker_;
// Arena declared LAST (line 230)
std::unique_ptr<memory::ArenaPool> db_arena_;
```

C++ destroys members in reverse declaration order, so `db_arena_` is destroyed FIRST. This allows the
`ArenaPool` destructor to purge extents and call hook callbacks while all trackers are still alive, preventing
use-after-free during cleanup.

### Multi-Parent Alloc/Free Propagation

`MemoryTracker::Alloc()` propagates to parent trackers:

1. Local atomic `amount_` update first
2. Parent1 propagation (if exists)
3. Parent2 propagation (if exists) - only if parent1 succeeds
4. **Rollback on failure**: If parent2 fails, rolls back parent1 via `parent1_->Free()` and local via `fetch_sub`

`MemoryTracker::Free()` unconditionally propagates to both parents (if they exist).

### Tenant Profiles

Tenant profiles set the hard limit on `db_total_memory_tracker_`:

- **CREATE/ALTER/DROP**: Profile operations with KV store durability
- **SET ON DATABASE/REMOVE FROM DATABASE**: Associate/disassociate profiles with databases
- **Durability**: Stored in DBMS KV records under `tenant_profile:*` key prefix
- **Restore**: `RestoreTenantProfiles_()` rebuilds reverse DB-to-profile mapping on startup and applies limits
- **Replication**: `TenantProfileRpc` propagates profile changes to replicas via `TenantProfileHandler`

Profile changes use atomic batch operations (`PutMultiple`, `PutAndDeleteMultiple`) for consistency.

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

## Verified Implementation Details

### Extent Hook Correctness
- Pre-tracking before base hook call ensures quota is reserved before OS commits pages
- Rollback via `tracker->Free()` on allocation failure prevents tracker drift
- `OutOfMemoryExceptionBlocker` in `db_arena_commit` is critical because the OS has already committed pages and we
cannot "uncommit" them - the blocker prevents exceptions and ensures tracking succeeds unconditionally

### Thread Attribution Mechanisms
- **DbArenaScope**: RAII scope for main thread work (queries, replication handlers)
- **DbAwareThread**: Long-lived threads (async indexer, GC) that acquire arena once
- **CrossThreadMemoryTracking**: Captures arena at task submission, restores in worker thread
- **ConsumerThreadFactory**: Kafka/Pulsar streams use factory pattern for arena-aware consumer threads

### Allocator Pattern
- **DbAwareAllocator<T>**: Stateless allocator reading `tls_db_arena_state.arena` at allocation/deallocation time
- **Container Aliases**: `SkipListDb<TObj>` = `SkipList<TObj, DbAwareAllocator<char>>` (and similar for other containers)
- **Explicit Arena Helpers**: `ArenaAwareUniquePtr`, `MakeDbAwareUnique` for long-lived objects with captured arena
- **PageAlignedAllocator<T>**: Aligns to page boundaries; uses `DbAllocateBytes` for arena attribution
- **PageSlabMemoryResource**: PMR resource with optional upstream; defaults to `get_default_resource()`

### Query Memory Control
- `QueryMemoryTracker` now accepts optional parent `MemoryTracker*` for DB-specific tracking
- `TrackingMemoryResource` chains to DB query tracker for tenant limit enforcement
- `QueryAllocator` uses `TrackingMemoryResource` for all query PMR allocations

### Vector Index Integration
- `TrackedVectorAllocator` wraps mmap allocations with `MemoryTracker::Alloc/Free` callbacks
- Passed to `usearch` index for tracking approximate nearest neighbor memory
- Rolls up to `db_embedding_memory_tracker_` for per-DB tracking

### Compressor Integration
- `CompressedBuffer` and `DecompressedBuffer` use explicit arena allocation via `DbAwareAllocator<uint8_t>`
- RAII management with custom `Free()` method ensures proper cleanup
- Allocation happens during zlib compression/decompression operations

### Replication Safety
All replication handlers wrap storage access with `DbArenaScope`:
- `HeartbeatHandler`
- `PrepareCommitHandler`
- `FinalizeCommitHandler`
- `SnapshotHandler`
- `WalFilesHandler`
- `CurrentWalHandler`

This ensures allocations during replication processing are attributed to the correct DB.

## Future Improvements

1. **Arena Contention Profiling**: Monitor arena-bin contention under high concurrency; consider per-thread arena
   acquisition for hot paths if profiling shows contention.

2. **Query Memory Granularity**: Consider routing raw non-PMR query allocations through `db_query_memory_tracker_`
   for more accurate tenant enforcement (currently only PMR allocations are tracked per-DB).

3. **Text Index Integration**: Add text-index/Tantivy memory to DB tracking if product requirements call for it.

4. **Arena Migration**: If profiling shows contention on base arena, migrate more long-lived structures to
   per-thread acquired arenas (similar to `DbAwareThread` pattern).
