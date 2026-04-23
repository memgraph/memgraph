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
  extent_hooks_t hooks;           // must be first
  utils::MemoryTracker *tracker;  // per-DB tracker, fed by extent events
  extent_hooks_t *base_hooks;     // default jemalloc hooks (called through)
  // TODO: Think about a failsafe in case unhooking failed
};

// Populate a DbArenaHooks struct so it can be installed on any jemalloc arena.
// `tracker`    – receives Alloc/Free calls for that arena.
// `base_hooks` – jemalloc's default hooks; the callbacks call through to these.
// After calling this, install with:
//   const extent_hooks_t *p = &h.hooks;
//   je_mallctl("arena.N.extent_hooks", nullptr, nullptr, &p, sizeof(p));
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

// Owns DB-specific jemalloc arenas with custom extent hooks that report
// committed OS pages to a `utils::MemoryTracker` owned by the caller.
// Active DB-owned threads can acquire additional arenas to reduce allocator
// contention. Acquired arenas are owned for the ArenaPool lifetime.
class ArenaPool {
 public:
  explicit ArenaPool(utils::MemoryTracker *tracker);
  ~ArenaPool() noexcept;

  ArenaPool(const ArenaPool &) = delete;
  ArenaPool &operator=(const ArenaPool &) = delete;
  ArenaPool(ArenaPool &&) = delete;
  ArenaPool &operator=(ArenaPool &&) = delete;

  // Returns the constructor-created base arena used by long-lived arena-aware
  // owners and short-lived DB-scoped work.
  unsigned idx() const noexcept;

  // Acquire an arena for DB-owned thread work.
  unsigned Acquire();

  // Return an arena acquired from this pool so a future Acquire() can reuse it.
  void Release(unsigned arena_idx);

  // Returns true if this pool owns the arena index.
  bool Owns(unsigned arena_idx) const;

 private:
  DbArenaHooks hooks_{};

  // Protects arenas_, free_count_, and first_arena_use_count_.
  mutable std::mutex arena_mux_;
  // Arena indices owned by this database. The range [0, free_count_) is free;
  // the range [free_count_, arenas_.size()) is in use.
  std::vector<unsigned> arenas_;
  std::size_t free_count_{0};

  // Cache of the first arena for backwards compatibility
  unsigned first_arena_idx_{0};

  unsigned first_arena_use_count_{0};
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
