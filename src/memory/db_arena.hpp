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

#include <atomic>
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
  // Written once (relaxed) before hooks are published via mallctl; jemalloc's
  // RELEASE store of extent_hooks orders it against callback loads, so relaxed
  // suffices for the success path. Nulled (relaxed) only on the hook-restore-
  // failure path in ~ArenaPool where no jemalloc mutex serialises — atomic only
  // to keep that defensive store race-free, never a synchronisation point.
  std::atomic<utils::MemoryTracker *> tracker;
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
  [[nodiscard]] unsigned Acquire();

  // Return an arena acquired from this pool so a future Acquire() can reuse it.
  void Release(unsigned arena_idx) noexcept;

  // Returns true if this pool owns the arena index.
  bool Owns(unsigned arena_idx) const;

  // Decay and purge all arenas currently owned by this pool so extent hooks can
  // report released pages back to the DB memory tracker.
  void PurgeAllArenas() const;

  // Acquire an explicit tcache for DB-owned thread work.
  [[nodiscard]] unsigned AcquireTcache();

  // Return a tcache acquired from this pool so a future AcquireTcache() can reuse it.
  void ReleaseTcache(unsigned tcache_id) noexcept;

  // Destroy all pooled tcaches. Must only be called when no tcaches are in use.
  void DestroyAllTcaches();

 private:
#if USE_JEMALLOC
  // Heap-allocated for stable address; &hooks_->hooks is installed on jemalloc arenas via mallctl.
  // On failed hook restore, ~ArenaPool leaks this struct (~64 B) so any retained jemalloc reference
  // stays valid for the process lifetime. The arena is abandoned, so the leak is bounded.
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

// Ensure every CPU id sched_getcpu() can return resolves to an arena that is NOT a per-DB arena.
// percpu_arena binds threads to arenas[sched_getcpu()] with no clamp; sched_getcpu can return ids
// above the online-CPU-derived automatic range (jemalloc#2054). A thread on an overflow CPU would
// bind a per-DB arena → shutdown UAF. Fix: create sacrificial arenas (default hooks, process
// lifetime) until per-DB indices exceed every possible CPU id. Idempotent, thread-safe.
// Returns the sacrificial arena indices created (empty if percpu_arena is disabled or already covered).
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
  [[nodiscard]] unsigned Acquire() {
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
  ConstructorThrow,
  AcquireArenaCreate,
  DestructorRestore,
};

void SetArenaPoolFailureInjection(ArenaPoolFailureInjection failure);

}  // namespace testing

#endif  // USE_JEMALLOC

}  // namespace memgraph::memory
