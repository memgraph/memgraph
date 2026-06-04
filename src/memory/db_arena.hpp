// Copyright 2026 Memgraph Ltd.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt; by using this file, you agree to be bound by the terms of the Business Source
// License, and you may not use this file except in compliance with the Business Source License.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

#pragma once

#include <memory>
#include <mutex>
#include <stdexcept>
#include <utility>
#include <vector>

#if USE_JEMALLOC
#include <jemalloc/jemalloc.h>
#endif

#include "memory/db_arena_fwd.hpp"
#include "utils/logging.hpp"
#include "utils/memory_tracker.hpp"

namespace memgraph::memory {

#if USE_JEMALLOC

// Internal layout for per-DB extent hooks. The `hooks` field MUST be first so
// that extent hook callbacks can cast `extent_hooks_t *` → `DbArenaHooks *`.
struct DbArenaHooks {
  extent_hooks_t hooks;  // must be first
  // Per-DB tracker, fed by extent events. A plain pointer is sufficient: it is
  // written exactly once (InitDbArenaHooks) before the hooks are published to
  // jemalloc via mallctl, and is only read from extent callbacks while the
  // hooks are installed on some arena. Teardown is serialised by jemalloc
  // itself: extent_hooks_set swaps hooks under the arena's background-thread
  // mutex — the same mutex the background thread holds while invoking hooks —
  // so after a successful restore no thread can be executing these callbacks.
  utils::MemoryTracker *tracker;
  extent_hooks_t *base_hooks;  // default jemalloc hooks (called through)
};

#endif

// Owns DB-specific arenas in jemalloc builds and degrades to a no-op facade
// when jemalloc support is disabled. Higher layers should use it uniformly and
// let the implementation hide the backend split.
class ArenaPool {
 public:
  explicit ArenaPool(utils::MemoryTracker *tracker);
  ~ArenaPool() noexcept;

  ArenaPool(const ArenaPool &) = delete;
  ArenaPool &operator=(const ArenaPool &) = delete;
  ArenaPool(ArenaPool &&) = delete;
  ArenaPool &operator=(ArenaPool &&) = delete;

  // Returns the constructor-created base arena used by long-lived arena-aware
  // owners (e.g. Storage containers). Short-lived DB-scoped work should use
  // Acquire() / Release() so the pool can be shared across threads.
  unsigned idx() const noexcept;

  // Acquire an arena for DB-owned thread work.
  unsigned Acquire();

  // Return an arena acquired from this pool so a future Acquire() can reuse it.
  void Release(unsigned arena_idx) noexcept;

  // Returns true if this pool owns the arena index.
  bool Owns(unsigned arena_idx) const;

  // Decay and purge all arenas currently owned by this pool so extent hooks can
  // report released pages back to the DB memory tracker.
  void PurgeAllArenas() const;

  // Acquire an explicit tcache for DB-owned thread work.
  unsigned AcquireTcache();

  // Return a tcache acquired from this pool so a future AcquireTcache() can reuse it.
  void ReleaseTcache(unsigned tcache_id) noexcept;

  // Destroy all pooled tcaches. Must only be called when no tcaches are in use.
  void DestroyAllTcaches();

 private:
#if USE_JEMALLOC
  // Heap-allocated so its address is stable for jemalloc and, critically,
  // so that it can outlive ~ArenaPool() on the unhook-failure path. We
  // install `&hooks_->hooks` on each jemalloc arena via mallctl; jemalloc
  // can still invoke this hook from tcache flush / decay long after
  // ~ArenaPool() returns. On the happy path the unique_ptr destructs
  // normally — by then we've already restored the default extent hooks on
  // every arena, so no callback can reach this struct. On the failure path
  // (mallctl fails to swap hooks back), ~ArenaPool() calls
  // `(void)hooks_.release()` to intentionally leak this struct (~64 B per
  // failed restore) so any retained jemalloc reference stays valid for the
  // process lifetime. The arena is also abandoned (not returned to
  // GlobalArenaPool), so this leak is bounded.
  std::unique_ptr<DbArenaHooks> hooks_;

  // Protects arenas_, free_count_, and first_arena_use_count_.
  mutable std::mutex arena_mux_;
  // Arena indices owned by this database. The range [0, free_count_) is free;
  // the range [free_count_, arenas_.size()) is in use.
  std::vector<unsigned> arenas_;
  std::size_t free_count_{0};

  // The base arena created in the ArenaPool constructor. Used as the fallback
  // when Acquire() cannot create a new arena, and by long-lived owners that
  // capture an explicit arena index (e.g. Storage containers).
  unsigned first_arena_idx_{0};

  unsigned first_arena_use_count_{0};

  // Tcache pool members
  std::mutex tcache_mutex_;
  std::vector<unsigned> tcaches_;
#endif
};

#if USE_JEMALLOC

// Ensure every CPU id that sched_getcpu() can return resolves to an arena that
// is NOT a per-DB arena.
//
// Under percpu_arena, jemalloc binds a thread to arenas[sched_getcpu()] with no
// clamp (percpu_arena_choose). jemalloc sizes the automatic range from the
// number of ONLINE CPUs, but sched_getcpu() can return any id below the number
// of POSSIBLE CPUs (sparse online sets: offlined cores, cgroups, NUMA, VM
// hotplug headroom; cf. jemalloc#2054). A thread on such an overflow CPU would
// bind to whatever arena owns that index — which is exactly where per-DB
// arenas (arenas.create) live — and later free through the per-DB extent hooks
// after the owning Database is destroyed (shutdown use-after-free).
//
// Fix: before any per-DB arena is created, append plain "sacrificial" arenas
// (default hooks, never destroyed, process lifetime) until the arena index
// space covers every possible CPU id. Overflow CPUs then get a dedicated
// 1:1 arena, and per-DB arenas land at indices no CPU id can ever reach.
//
// Returns the indices of the sacrificial arenas created (empty when the
// automatic range already covers all possible CPU ids, or percpu_arena is
// disabled). Thread-safe and idempotent; the first caller does the work.
const std::vector<unsigned> &EnsureCpuArenaCoverage();

// Populate a DbArenaHooks struct so it can be installed on any jemalloc arena.
// `tracker`    – receives Alloc/Free calls for that arena.
// `base_hooks` – jemalloc's default hooks; the callbacks call through to these.
// After calling this, install with:
//   const extent_hooks_t *p = &h.hooks;
//   je_mallctl("arena.N.extent_hooks", nullptr, nullptr, static_cast<void *>(const_cast<extent_hooks_t **>(&p)),
//   sizeof(p));
void InitDbArenaHooks(DbArenaHooks &h, utils::MemoryTracker *tracker, extent_hooks_t *base_hooks);

// Singleton pool that recycles jemalloc arena indices across database ArenaPool lifetimes.
//
// These are extra explicit arenas created with arenas.create for DB-scoped
// allocations. They do not replace jemalloc's normal startup arenas configured
// by SetHooks(); non-DB malloc/new traffic continues to use jemalloc's default
// arena selection unless a DB-aware allocator passes MALLOCX_ARENA explicitly.
// Without recycling, every Database creation permanently consumes an index — in
// long-running multi-tenant processes this causes unbounded jemalloc metadata growth.
//
// Thread-safe: arena creation/destruction is not a hot path so a plain mutex is fine.
class GlobalArenaPool {
 public:
  static GlobalArenaPool &Instance() noexcept {
    static GlobalArenaPool instance;
    return instance;
  }

  // Return a free arena index — either recycled from the pool or freshly created.
  // If N threads race on an empty pool, all N will call je_mallctl("arenas.create")
  // concurrently; each gets a distinct valid index. This is benign: je_mallctl is
  // thread-safe and neither index is lost. At most N-1 extra arenas are created versus
  // serialising the slow path, which is acceptable for an infrequent operation.
  unsigned Acquire() {
    {
      std::lock_guard<std::mutex> lock(mux_);
      if (!pool_.empty()) {
        unsigned idx = pool_.back();
        pool_.pop_back();
        return idx;
      }
    }
    // Safety net: per-DB arena indices must stay above every reachable CPU id,
    // so the sacrificial coverage arenas (normally created at startup by
    // SetHooks) must exist before we append the first per-DB arena.
    EnsureCpuArenaCoverage();
    unsigned arena_idx = 0;
    size_t sz = sizeof(arena_idx);
    const int err = je_mallctl("arenas.create", &arena_idx, &sz, nullptr, 0);
    if (err != 0) {
      throw std::runtime_error(fmt::format("Failed to create jemalloc arena (err={})", err));
    }
    return arena_idx;
  }

  // Return `idx` to the pool so a future Acquire() can reuse it.
  // Must only be called after extent hooks are restored to the default and the
  // arena is fully purged (ArenaPool destructor guarantees this ordering).
  void Release(unsigned idx) {
    if (idx == 0) return;
    std::lock_guard<std::mutex> lock(mux_);
    pool_.push_back(idx);
  }

  // Discard all recycled arena indices from the free-list.
  //
  // IMPORTANT: This does NOT destroy the underlying jemalloc arenas or reclaim
  // their metadata; jemalloc retains the arena structs indefinitely once created.
  // Discarded indices can never be reused, so jemalloc's internal arena count grows
  // without bound if Drain() is called repeatedly in a long-lived process.
  //
  // Call Drain() only at process exit or in test teardown where the process is
  // discarded afterwards (e.g. to reset state between test cases without forking).
  void Drain() {
    std::lock_guard<std::mutex> lock(mux_);
    pool_.clear();
  }

 private:
  std::mutex mux_;
  std::vector<unsigned> pool_;
};

namespace testing {

enum class ArenaPoolFailureInjection {
  None,
  ConstructorPublish,
  AcquireArenaCreate,
};

void SetArenaPoolFailureInjection(ArenaPoolFailureInjection failure);

}  // namespace testing

#endif  // USE_JEMALLOC

}  // namespace memgraph::memory
