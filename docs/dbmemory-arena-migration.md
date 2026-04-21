# Memgraph DbArena → Per-Thread Arena Migration

**Scope**: Allocation routing only - keep extent hooks and tracking infrastructure.

---

## Executive Summary

Adopt the Subsystem Arenas experiment's **PerThread + Shim** allocation routing strategy while **keeping all existing tracking infrastructure** (extent hooks, MemoryTracker hierarchy). The goal is to eliminate arena-bin-lock contention and gain sized-delete optimization without changing how memory is tracked.

---

## What Stays The Same

| Component | Location | Behavior |
|-----------|----------|----------|
| Extent hooks | `db_arena.cpp:32-103` | `db_arena_alloc`, `dalloc`, `commit`, `decommit`, `purge_forced` continue firing on OS page operations |
| MemoryTracker hierarchy | `database.hpp:241-253` | 4 trackers with parent relationships unchanged |
| Tracking | Via extent hooks → `db_memory_tracker_` | Page-level tracking continues |
| TrackingMemoryResource | `memory.hpp:699-732` | Query PMR tracking unchanged |
| Tenant profile enforcement | `db_total_memory_tracker_.SetHardLimit()` | Unchanged |

---

## What Changes

| Aspect | Current | New |
|--------|---------|-----|
| **Arena model** | 1 arena per Database | N arenas per Database (one per active thread) |
| **TLS** | `unsigned tls_db_arena_idx` | `struct DbArenaTlsState { unsigned arena; unsigned tcache; }` |
| **Allocation flags** | `MALLOCX_ARENA(idx) \| MALLOCX_TCACHE_NONE` | `MALLOCX_ARENA(arena) \| MALLOCX_TCACHE(tcache)` — **speedup** |
| **Deallocation** | `JeFree` + `MALLOCX_TCACHE_NONE` | `je_sdallocx` + `MALLOCX_TCACHE_NONE` — **sized delete, tcache unchanged** |
| **Thread setup** | Bind via `je_mallctl("thread.arena")` | Acquire per-thread arena + create tcache + bind |

**Design decision**: Allocation uses tcache for speed (avoids arena-lock on alloc). Deallocation upgrades to `je_sdallocx` (sized delete) but keeps `TCACHE_NONE` — no alloc-reuse benefit on the free path, and avoids unnecessary per-thread cache pressure.

---

## Detailed Migration Table

### 1. TLS Structure Extension

| File | Current | Change |
|------|---------|--------|
| `db_aware_allocator.hpp:40` | `unsigned tls_db_arena_idx` | Replace with `DbArenaTlsState tls_db_arena_state` |

**New TLS Structure:**
```cpp
struct DbArenaTlsState {
    unsigned arena{0};           // Current thread's arena for active DB
    unsigned tcache{UINT_MAX};   // Per-(thread, DB) tcache index; UINT_MAX = uninitialized
};

// Must preserve initial-exec TLS model for hot-path performance
inline thread_local DbArenaTlsState tls_db_arena_state [[gnu::tls_model("initial-exec")]] = {};
```

> **Note:** `tcache` defaults to `UINT_MAX` (not `0`) because jemalloc tcache index 0 is a valid real cache. `UINT_MAX` is the sentinel meaning "not yet created". Callers must check before passing to `MALLOCX_TCACHE`.

---

### 2. DbArenaScope Modification

| File | Current | Change |
|------|---------|--------|
| `db_arena.hpp:203-215` | Sets/restores `tls_db_arena_idx` | Sets/restores full `DbArenaTlsState` |

**New Implementation:**
```cpp
struct DbArenaScope {
    explicit DbArenaScope(Database* db)
        : prev_(tls_db_arena_state) {
        tls_db_arena_state.arena = db->GetThreadArena();
        tls_db_arena_state.tcache = db->GetThreadTcache();
    }
    ~DbArenaScope() { tls_db_arena_state = prev_; }
private:
    DbArenaTlsState prev_;
};
```

> **Semantics note:** `AcquireThreadArena()` must use `thread_arena_map_[std::this_thread::get_id()]` as its cache — **not** TLS. TLS holds the *currently active* DB's arena; a `DbArenaScope(db2)` entered while `db1`'s scope is active must look up `db2`'s per-thread map entry, not the current TLS value. On first call from a thread, the map miss triggers arena creation (cold path, mutex held). Subsequent calls for the same (thread, DB) pair return the cached index.

---

### 3. DbArena Class Extension

| File | Current | Change |
|------|---------|--------|
| `db_arena.hpp:148-163` | Manages 1 arena | Manages per-thread arena assignment |

**New Methods:**
```cpp
class DbArena {
public:
    // Acquire (or create) the arena assigned to this thread for this DB.
    // Checks thread_arena_map_[this_thread::get_id()] under mutex.
    // Cold path (first call per thread): creates arena, installs hooks, inserts into map.
    unsigned AcquireThreadArena();

    // Get or create per-(thread, DB) tcache. Returns UINT_MAX if tcache creation fails.
    unsigned GetOrCreateTcache(unsigned arena_idx);

    // Destroy tcache for the calling thread (call on thread exit before TLS teardown).
    void DestroyThreadTcache(unsigned tcache_idx) noexcept;

private:
    std::mutex arena_map_mux_;
    // Protected by arena_map_mux_ on writes; read lock-free via TLS on hot path.
    std::unordered_map<std::thread::id, unsigned> thread_arena_map_;

    // Extent hooks installed on ALL per-thread arenas; all point to the same tracker.
    DbArenaHooks hooks_;
};
```

> **Thread safety:** `thread_arena_map_` is written under `arena_map_mux_` on the cold path only. Hot path reads `tls_db_arena_state.arena` directly (already set in TLS), never touching the map.

---

### 4. DbAllocateBytes Update

| File | Current | Change |
|------|---------|--------|
| `db_aware_allocator.hpp:44-55` | Uses `MALLOCX_TCACHE_NONE` | Uses `MALLOCX_TCACHE(tcache)` from TLS |

**New Implementation:**
```cpp
inline void* DbAllocateBytes(std::size_t bytes, unsigned idx, std::size_t alignment) {
#if USE_JEMALLOC
    if (idx != 0) {
        int flags = MALLOCX_ARENA(idx);
        const unsigned tc = tls_db_arena_state.tcache;
        flags |= (tc != UINT_MAX) ? MALLOCX_TCACHE(tc) : MALLOCX_TCACHE_NONE;
        if (alignment > alignof(std::max_align_t)) {
            flags |= MALLOCX_ALIGN(alignment);
        }
        return JeNew(bytes, flags);
    }
#endif
    return ::operator new(bytes, std::align_val_t{alignment});
}
```

---

### 5. DbDeallocateBytes - Upgrade to Sized Delete, Keep TCACHE_NONE

| File | Current | Change |
|------|---------|--------|
| `db_aware_allocator.hpp:61-71` | `JeFree` + `MALLOCX_TCACHE_NONE` | `je_sdallocx` + `MALLOCX_TCACHE_NONE` |

**Rationale**: Allocation uses tcache for speed (avoids arena lock on alloc). Deallocation keeps `TCACHE_NONE` because there is no allocation-reuse benefit to caching freed slots on the free path, and it avoids holding memory in per-thread caches unnecessarily. Note: extent hook tracking is page-level regardless of tcache setting — this choice does not affect tracking accuracy.

**Implementation (unchanged from current):**
```cpp
inline void DbDeallocateBytes(void* p, std::size_t bytes, std::size_t alignment) noexcept {
#if USE_JEMALLOC
    int flags = MALLOCX_TCACHE_NONE;
    if (alignment > alignof(std::max_align_t)) {
        flags |= MALLOCX_ALIGN(alignment);
    }
    // Sized delete: je_sdallocx skips size-class lookup inside jemalloc.
    // TCACHE_NONE on dealloc: freed objects go directly to arena bins; no allocation-reuse
    // benefit from caching on the free path, and avoids holding memory in per-thread caches.
    je_sdallocx(p, bytes, flags);
#else
    ::operator delete(p, bytes, std::align_val_t{alignment});
#endif
}
```

**Tracking behavior**: Extent hooks (`dalloc`, `decommit`, `purge_forced`) fire at **page granularity** when jemalloc returns OS pages — not on individual object frees. `TCACHE_NONE` vs tcache on dealloc does not affect tracking synchrony; both paths eventually trigger the same page-level hooks.

---

### 6. DbAwareThread Extension

| File | Current | Change |
|------|---------|--------|
| `db_arena.hpp:228-281` | Propagates arena index | Propagates arena + tcache, acquires per-thread |

**New Thread Start Logic:**
```cpp
template <typename F, typename... Args>
DbAwareThread(Database* db, F&& f, Args&&... args)
    : thread_([db, func = std::forward<F>(f)](std::stop_token st, Args... a) mutable {
        // Acquire per-thread resources for this DB
        tls_db_arena_state.arena = db->Arena().AcquireThreadArena();
        tls_db_arena_state.tcache = db->Arena().GetOrCreateTcache(tls_db_arena_state.arena);

        // Also bind for raw allocations
        unsigned arena_for_binding = tls_db_arena_state.arena;
        je_mallctl("thread.arena", nullptr, nullptr, &arena_for_binding, sizeof(unsigned));

        func(std::move(st), std::move(a)...);

        // Destroy tcache on thread exit to release jemalloc metadata.
        if (tls_db_arena_state.tcache != UINT_MAX) {
            db->Arena().DestroyThreadTcache(tls_db_arena_state.tcache);
        }
    }, std::forward<Args>(args)...) {}
```

---

### 7. GC/Snapshot/TTL Thread Setup

| Current | Change |
|---------|--------|
| `je_mallctl("thread.arena", &arena_idx)` only | Acquire per-thread arena + tcache + bind |

**New Setup:**
```cpp
// At thread start for GC/snapshot/TTL workers
auto& db_arena = database->Arena();
unsigned thread_arena = db_arena.AcquireThreadArena();
unsigned thread_tcache = db_arena.GetOrCreateTcache(thread_arena);

// Set TLS
tls_db_arena_state = {thread_arena, thread_tcache};

// Bind for raw allocations
je_mallctl("thread.arena", nullptr, nullptr, &thread_arena, sizeof(thread_arena));

// ... worker body ...

// At thread exit: destroy tcache to free jemalloc metadata
if (tls_db_arena_state.tcache != UINT_MAX) {
    db_arena.DestroyThreadTcache(tls_db_arena_state.tcache);
}
```

---

### 8. ArenaPool Extension

| File | Current | Change |
|------|---------|--------|
| `db_arena.hpp:55-111` | Global pool for all DBs | Per-DB arena assignment with thread caching |

The ArenaPool remains but `DbArena` manages which threads use which arenas:
- Each DB can have N active arenas (one per thread currently working on it)
- Extent hooks are installed on each per-thread arena
- All per-thread arenas for a DB point to the same `db_memory_tracker_`

---

### 9. Database Destructor Update

| File | Current | Change |
|------|---------|--------|
| `~DbArena()` (db_arena.cpp:152-185) | Purges 1 arena | Purges all per-thread arenas |

```cpp
DbArena::~DbArena() {
    // For each per-thread arena used by this DB:
    for (auto& [tid, arena_idx] : thread_arena_map_) {
        // Purge this arena
        je_mallctl(("arena." + std::to_string(arena_idx) + ".purge").c_str(), ...);

        // Restore default hooks
        const extent_hooks_t* base = hooks_.base_hooks;
        je_mallctl(("arena." + std::to_string(arena_idx) + ".extent_hooks").c_str(), ...);

        // Release index to pool
        ArenaPool::Instance().Release(arena_idx);
    }
}
```

---

## Naming Mapping (Subsystem Arenas → Memgraph)

| Subsystem Arenas | Memgraph | Purpose |
|------------------|----------|---------|
| `instantiate_per_thread()` | `DbArena::SetupPerThread()` | Initialize per-thread arena binding |
| `pick_arena_for_thread()` | `DbArena::AcquireThreadArena()` | Get/create per-thread arena |
| `get_or_create_tcache()` | `DbArena::GetOrCreateTcache()` | Get/create per-(thread,DB) tcache |
| `TlsState` | `DbArenaTlsState` | TLS struct with arena, tcache |
| `SubsystemGuard` | `DbArenaScope` (reuse) | RAII for arena/tcache TLS |
| `propagate_subsystem()` | `PropagateDbArena()` | Capture for async task |

---

## Key Design Points

### 1. Extent Hooks Installation

Every per-thread arena gets the **same hooks installed**:
```cpp
// In AcquireThreadArena() when creating new arena
InitDbArenaHooks(hooks_, &db_memory_tracker_, base_hooks);
// Install hooks on the new per-thread arena
je_mallctl("arena.N.extent_hooks", ..., &hooks_.hooks, ...);
```

All per-thread arenas for a DB report to the **same tracker** (`db_memory_tracker_`).

### 2. Thread Safety

- `AcquireThreadArena()` checks `thread_arena_map_` under mutex (called only at scope/thread setup, not on allocation hot path)
- Cold path creates arena via `je_mallctl("arenas.create")` and installs hooks
- No locking on allocation path — TLS is already set before any allocations occur; jemalloc arena bins are per-arena

### 3. Lifecycle Management

- Per-thread arenas are created lazily on first access
- Arenas persist until Database destruction
- `~DbArena()` iterates all per-thread arenas, purges, restores hooks, releases indices

### 4. Performance Wins

| Metric | Current | New | Gain |
|--------|---------|-----|------|
| Arena contention | Single arena per DB | Per-thread arenas | Eliminates lock cliff |
| Tcache usage (alloc) | Disabled (`TCACHE_NONE`) | Explicit per-thread tcaches | ~2.4× faster at 16 threads |
| Tcache usage (dealloc) | `TCACHE_NONE` | `TCACHE_NONE` (unchanged) | No regression; freed slots go directly to bins |
| Deallocation | Size lookup in jemalloc | Sized delete (`sdallocx`) | Skips size-class lookup |

---

## Files To Modify

1. **`src/utils/db_aware_allocator.hpp`** - Replace `tls_db_arena_idx` with `DbArenaTlsState tls_db_arena_state`; update `DbAllocateBytes`, `DbDeallocateBytes`
2. **`src/memory/db_arena.hpp`** - Add `DbArenaTlsState` struct; extend `DbArenaScope` to take `Database*`; extend `DbArena` with per-thread methods
3. **`src/memory/db_arena.cpp`** - Implement `AcquireThreadArena()`, `GetOrCreateTcache()`, `DestroyThreadTcache()`; update `~DbArena()`
4. **`src/dbms/database.hpp`** - Update `DbAwareThread` template; add `GetThreadArena()`/`GetThreadTcache()` accessors
5. **Call sites of `DbArenaScope`** - Update all callers that pass `db.ArenaIdx()` to pass `&db` (or the arena index directly if Database* is unavailable)

---

## Risk Areas

1. **Thread pool tasks** - Must propagate `DbArenaTlsState` to worker threads via `propagate_subsystem()` or equivalent wrapper
2. **Cross-thread deallocation** - GC thread freeing query-allocated objects: jemalloc derives the arena from the allocation's extent metadata, so cross-thread frees are always attributed correctly regardless of which thread's TLS is active at dealloc time.
3. **Tcache lifecycle** - Each per-(thread,DB) tcache must be explicitly destroyed via `DestroyThreadTcache()` on thread exit. Failure leaks jemalloc metadata permanently. Long-lived thread pools that serve many databases are the highest-risk scenario.
4. **`tcache{UINT_MAX}` sentinel** - Code paths that reach `DbAllocateBytes` before `tls_db_arena_state.tcache` is initialized fall back to `TCACHE_NONE`. Allocations are still attributed to the correct arena and tracked via extent hooks; only the tcache speedup is missed. Add an assertion in debug builds to catch uninitialized paths.
5. **Arena destruction order** - Must purge all per-thread arenas before releasing to pool; `~DbArena()` guarantees this ordering.
6. **Tenant limit timing** - Tracker updates occur via extent hooks at page granularity; this is unaffected by the tcache/TCACHE_NONE choice on dealloc. Hard-limit enforcement accuracy is unchanged.

---

## Testing Strategy

1. **Single-thread correctness** - Verify tracking matches current behavior exactly
2. **Multi-thread stress** - 16+ threads allocating/deallocating simultaneously
3. **Database lifecycle** - Create/destroy databases repeatedly, verify no arena leaks
4. **Tenant limits** - Verify `db_total_memory_tracker_` enforcement still works

---

*Generated: Analysis of adopting Subsystem Arenas allocation routing while preserving Memgraph's extent hook tracking.*
