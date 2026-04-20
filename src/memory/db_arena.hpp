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

#include <memory_resource>
#include <mutex>
#include <new>
#include <stdexcept>
#include <thread>
#include <unordered_map>
#include <utility>
#include <vector>

#if USE_JEMALLOC
#include <jemalloc/jemalloc.h>
#endif

#include "utils/db_aware_allocator.hpp"
#include "utils/logging.hpp"
#include "utils/memory_tracker.hpp"

// Forward declare (in dbms namespace)
namespace memgraph::dbms {
class Database;
}

namespace memgraph::memory {

#if USE_JEMALLOC

// Internal layout for per-DB extent hooks. The `hooks` field MUST be first so
// that extent hook callbacks can cast `extent_hooks_t *` → `DbArenaHooks *`.
struct DbArenaHooks {
  extent_hooks_t hooks;           // must be first
  utils::MemoryTracker *tracker;  // per-DB tracker, fed by extent events
  extent_hooks_t *base_hooks;     // default jemalloc hooks (called through)
};

// Populate a DbArenaHooks struct so it can be installed on any jemalloc arena.
// `tracker`    – receives Alloc/Free calls for that arena.
// `base_hooks` – jemalloc's default hooks; the callbacks call through to these.
// After calling this, install with:
//   const extent_hooks_t *p = &h.hooks;
//   je_mallctl("arena.N.extent_hooks", nullptr, nullptr, &p, sizeof(p));
void InitDbArenaHooks(DbArenaHooks &h, utils::MemoryTracker *tracker, extent_hooks_t *base_hooks);

// Singleton pool that recycles jemalloc arena indices across DbArena lifetimes.
// Without recycling, every Database creation permanently consumes an index — in
// long-running multi-tenant processes this causes unbounded jemalloc metadata growth.
//
// Thread-safe: arena creation/destruction is not a hot path so a plain mutex is fine.
class ArenaPool {
 public:
  static ArenaPool &Instance() noexcept {
    static ArenaPool instance;
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
  // arena is fully purged (DbArena destructor guarantees this ordering).
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

// Move-only RAII handle for a jemalloc arena index obtained from ArenaPool.
// Releases the index back to the pool on destruction. Moved-from handles hold
// the sentinel value 0 (no arena) and release nothing.
class ArenaHandle {
 public:
  ArenaHandle() noexcept = default;

  explicit ArenaHandle(unsigned idx) noexcept : idx_(idx) {}

  ~ArenaHandle() { ArenaPool::Instance().Release(idx_); }

  ArenaHandle(ArenaHandle &&o) noexcept : idx_(std::exchange(o.idx_, 0)) {}

  ArenaHandle &operator=(ArenaHandle &&o) noexcept {
    if (this != &o) {
      ArenaPool::Instance().Release(idx_);
      idx_ = std::exchange(o.idx_, 0);
    }
    return *this;
  }

  ArenaHandle(const ArenaHandle &) = delete;
  ArenaHandle &operator=(const ArenaHandle &) = delete;

  unsigned idx() const noexcept { return idx_; }

  explicit operator bool() const noexcept { return idx_ != 0; }

 private:
  unsigned idx_{0};
};

// Owns per-thread jemalloc arenas with custom extent hooks that
// report committed OS pages to a `utils::MemoryTracker` owned by the caller.
// Each thread working on this database gets its own arena to eliminate
// arena-bin-lock contention.
class DbArena {
 public:
  explicit DbArena(utils::MemoryTracker *tracker);
  ~DbArena();

  DbArena(const DbArena &) = delete;
  DbArena &operator=(const DbArena &) = delete;
  DbArena(DbArena &&) = delete;
  DbArena &operator=(DbArena &&) = delete;

  // Legacy: returns the first arena (for backwards compatibility)
  // Prefer AcquireThreadArena() for per-thread access
  unsigned idx() const noexcept;

  // Acquire (or create) the arena assigned to this thread for this DB.
  // Checks thread_arena_map_[this_thread::get_id()] under mutex.
  // Cold path (first call per thread): creates arena, installs hooks, inserts into map.
  unsigned AcquireThreadArena();

  // Get or create per-(thread, DB) tcache. Returns UINT_MAX if tcache creation fails.
  unsigned GetOrCreateTcache(unsigned arena_idx);

  // Destroy tcache for the calling thread (call on thread exit before TLS teardown).
  void DestroyThreadTcache(unsigned tcache_idx) noexcept;

 private:
  DbArenaHooks hooks_{};

  // Protects thread_arena_map_ on writes; read is lock-free via TLS on hot path
  std::mutex arena_map_mux_;
  // Maps thread_id -> arena_index for this database
  std::unordered_map<std::thread::id, unsigned> thread_arena_map_;
  // Per-thread arena handles (for cleanup in destructor)
  std::vector<ArenaHandle> arena_handles_;

  // Cache of the first arena for backwards compatibility
  unsigned first_arena_idx_{0};
};

#endif  // USE_JEMALLOC

// A std::pmr::memory_resource that routes allocations to a specific jemalloc
// arena. Used as the upstream for PageSlabMemoryResource so that Delta slab
// pages are attributed to the owning DB's MemoryTracker.
//
// arena_idx == 0 → falls back to operator new / operator delete.
// Non-jemalloc builds: always uses operator new / operator delete.
class ArenaMemoryResource final : public std::pmr::memory_resource {
 public:
  explicit ArenaMemoryResource(unsigned arena_idx) noexcept : arena_idx_(arena_idx) {}

 private:
  void *do_allocate(std::size_t bytes, std::size_t alignment) override {
    return DbAllocateBytes(bytes, arena_idx_, alignment);
  }

  void do_deallocate(void *p, std::size_t bytes, std::size_t alignment) noexcept override {
    // NOTE: jemalloc tracks the owning arena per-extent in its own metadata, so GC can safely
    // free query-thread allocations regardless of which thread calls do_deallocate.
    // MALLOCX_TCACHE_NONE matches the allocation style and lets decay=0 arenas return pages promptly.
    DbDeallocateBytes(p, bytes, alignment);
  }

  bool do_is_equal(const std::pmr::memory_resource &other) const noexcept override {
    const auto *o = dynamic_cast<const ArenaMemoryResource *>(&other);
    return o && o->arena_idx_ == arena_idx_;
  }

  unsigned arena_idx_;
};

// RAII guard: installs a DB arena and tcache for the duration of its scope and
// restores the previous values on destruction. Supports nested scopes.
//
// Usage (query threads):
//   DbArenaScope scope{&db};          // Acquire per-thread arena from Database
//   DbArenaScope scope{arena_idx};    // Legacy: direct arena index (uses TCACHE_NONE)
//   ... execute pull ...
struct DbArenaScope {
  // Acquire per-thread arena and tcache from the database.
  explicit DbArenaScope(memgraph::dbms::Database *db);

  // Legacy constructor: direct arena index (no tcache)
  explicit DbArenaScope(unsigned arena_idx) noexcept;

  ~DbArenaScope() noexcept;

  DbArenaScope(const DbArenaScope &) = delete;
  DbArenaScope &operator=(const DbArenaScope &) = delete;
  DbArenaScope(DbArenaScope &&) = delete;
  DbArenaScope &operator=(DbArenaScope &&) = delete;

 private:
  DbArenaTlsState prev_;
  // Note: db_ member removed - not needed as we store state in prev_
};

// Helper functions for setting up per-thread arena resources.
// These can be called at the start/end of thread execution.
#if USE_JEMALLOC
// Sets up per-thread arena and tcache for the given arena index.
// Returns the tcache index (UINT_MAX on failure).
unsigned SetupThreadArena(unsigned arena_idx);

// Cleans up per-thread tcache.
void CleanupThreadTcache(unsigned tcache_idx) noexcept;
#endif

// A std::jthread wrapper that sets tls_db_arena_state on the new thread so that
// allocations through DbAwareAllocator / ArenaAwareAllocator containers are
// attributed to the owning DB arena.  Does NOT redirect thread.arena via
// je_mallctl — raw allocations (std::string, operator new, …) on these threads
// go to the default jemalloc arena and are not attributed.
//
// Two construction forms:
//   DbAwareThread t{callable, args...};            // inherits tls_db_arena_state from caller
//   DbAwareThread t{arena_idx, callable, args...}; // explicit arena index
//
// Non-jemalloc builds: no-op wrapper, identical behaviour to std::jthread.
class DbAwareThread {
 public:
  // Inherit the calling thread's arena state.
  template <typename F, typename... Args>
    requires(std::is_invocable_v<std::decay_t<F>, std::decay_t<Args>...> ||
             std::is_invocable_v<std::decay_t<F>, std::stop_token, std::decay_t<Args>...>)
  explicit DbAwareThread(F &&f, Args &&...args)
      : DbAwareThread(tls_db_arena_state.arena, std::forward<F>(f), std::forward<Args>(args)...) {}

  // Explicit arena index.
  template <typename F, typename... Args>
    requires(std::is_invocable_v<std::decay_t<F>, std::decay_t<Args>...> ||
             std::is_invocable_v<std::decay_t<F>, std::stop_token, std::decay_t<Args>...>)
  DbAwareThread(unsigned arena_idx, F &&f, Args &&...args)
      : thread_(
            [arena_idx, func = std::forward<F>(f)](std::stop_token st, std::decay_t<Args>... a) mutable {
              tls_db_arena_state.arena = arena_idx;
              if constexpr (std::is_invocable_v<std::decay_t<F>, std::stop_token, std::decay_t<Args>...>) {
                func(std::move(st), std::move(a)...);
              } else {
                func(std::move(a)...);
              }
            },
            std::forward<Args>(args)...) {}

  DbAwareThread() = default;
  DbAwareThread(DbAwareThread &&) noexcept = default;
  DbAwareThread &operator=(DbAwareThread &&) noexcept = default;

  DbAwareThread(const DbAwareThread &) = delete;
  DbAwareThread &operator=(const DbAwareThread &) = delete;

  // Delegate jthread interface.
  void join() { thread_.join(); }

  void detach() { thread_.detach(); }

  [[nodiscard]] bool joinable() const noexcept { return thread_.joinable(); }

  [[nodiscard]] std::jthread::id get_id() const noexcept { return thread_.get_id(); }

  [[nodiscard]] std::stop_token get_stop_token() const noexcept { return thread_.get_stop_token(); }

  [[nodiscard]] std::stop_source get_stop_source() noexcept { return thread_.get_stop_source(); }

  void request_stop() { thread_.request_stop(); }

  void swap(DbAwareThread &other) noexcept { thread_.swap(other.thread_); }

  [[nodiscard]] std::jthread::native_handle_type native_handle() { return thread_.native_handle(); }

 private:
  std::jthread thread_;
};

}  // namespace memgraph::memory
