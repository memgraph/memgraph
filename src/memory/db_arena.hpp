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

#include <cstddef>
#include <new>
#include <thread>
#include <type_traits>
#include <utility>

#if USE_JEMALLOC
#include <jemalloc/jemalloc.h>
#endif

#include "utils/memory_tracker.hpp"

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

// Owns a dynamically-created jemalloc arena with custom extent hooks that
// report committed OS pages to a `utils::MemoryTracker` owned by the caller.
// The arena index is stable for the lifetime of this object.
class DbArena {
 public:
  explicit DbArena(utils::MemoryTracker *tracker);
  ~DbArena();

  DbArena(const DbArena &) = delete;
  DbArena &operator=(const DbArena &) = delete;
  DbArena(DbArena &&) = delete;
  DbArena &operator=(DbArena &&) = delete;

  unsigned idx() const noexcept { return arena_idx_; }

 private:
  DbArenaHooks hooks_{};
  unsigned arena_idx_{0};
};

#endif  // USE_JEMALLOC

// Thread-local arena index for the currently active database.
// 0 means "no DB arena pinned" — allocations go to jemalloc's default arena.
// Set by DbArenaScope for shared query threads and directly at thread-start
// for DB-owned background threads (GC, snapshot, TTL, async indexer, ...).
//
// DEVNOTE: initial-exec TLS model requires this to be in the main executable,
//          not a shared library. Matches the pattern used by query_memory_control.
// NOLINTNEXTLINE(cppcoreguidelines-avoid-non-const-global-variables)
inline thread_local unsigned tls_db_arena_idx [[gnu::tls_model("initial-exec")]] = 0;

// RAII guard: installs a DB arena index for the duration of its scope and
// restores the previous value on destruction. Supports nested scopes.
//
// Usage (query threads):
//   DbArenaScope scope{db.ArenaIdx()};
//   ... execute pull ...
struct DbArenaScope {
  explicit DbArenaScope(unsigned arena_idx) noexcept : prev_(tls_db_arena_idx) { tls_db_arena_idx = arena_idx; }

  ~DbArenaScope() noexcept { tls_db_arena_idx = prev_; }

  DbArenaScope(const DbArenaScope &) = delete;
  DbArenaScope &operator=(const DbArenaScope &) = delete;
  DbArenaScope(DbArenaScope &&) = delete;
  DbArenaScope &operator=(DbArenaScope &&) = delete;

 private:
  unsigned prev_;
};

// Full arena scope for shared threads (replication pool, RPC server workers).
//
// Sets both:
//   - tls_db_arena_idx  → DbAwareAllocator routes SkipList/small_vector nodes to the DB arena
//   - thread.arena      → all other allocations (std::string, std::vector, ...) also go there
//
// Both are saved and restored on destruction, so this is safe on threads that
// serve multiple databases (e.g. ReplicationClient::thread_pool_, RPC workers).
//
// Usage:
//   DbArenaFullScope scope{storage.config_.arena_idx};
//   ... do replication work ...
struct DbArenaFullScope {
  explicit DbArenaFullScope(unsigned arena_idx) noexcept : tls_scope_(arena_idx) {
#if USE_JEMALLOC
    if (arena_idx != 0) {
      size_t sz = sizeof(prev_thread_arena_);
      je_mallctl("thread.arena", &prev_thread_arena_, &sz, &arena_idx, sz);
      active_ = true;
    }
#endif
  }

  ~DbArenaFullScope() noexcept {
#if USE_JEMALLOC
    if (active_) {
      je_mallctl("thread.arena", nullptr, nullptr, &prev_thread_arena_, sizeof(prev_thread_arena_));
    }
#endif
  }

  DbArenaFullScope(const DbArenaFullScope &) = delete;
  DbArenaFullScope &operator=(const DbArenaFullScope &) = delete;
  DbArenaFullScope(DbArenaFullScope &&) = delete;
  DbArenaFullScope &operator=(DbArenaFullScope &&) = delete;

 private:
  DbArenaScope tls_scope_;
  unsigned prev_thread_arena_{0};
  bool active_{false};
};

// A std::jthread wrapper that permanently pins the new thread to a jemalloc
// arena so every allocation on that thread is attributed to the owning DB.
//
// Two construction forms:
//   DbAwareThread t{callable, args...};            // inherits tls_db_arena_idx from caller
//   DbAwareThread t{arena_idx, callable, args...}; // explicit arena index
//
// On thread start, the wrapper sets both:
//   - tls_db_arena_idx  → DbAwareAllocator routes to this arena
//   - thread.arena      → all other allocations (std::string, std::vector…) also go there
//
// No restore is needed — these are fresh, dedicated threads.
// This mirrors the static-pin pattern used by GC/snapshot/TTL threads.
//
// Non-jemalloc builds: no-op wrapper, identical behaviour to std::jthread.
class DbAwareThread {
 public:
  // Inherit the calling thread's arena index.
  template <typename F, typename... Args>
    requires(std::is_invocable_v<std::decay_t<F>, std::decay_t<Args>...> ||
             std::is_invocable_v<std::decay_t<F>, std::stop_token, std::decay_t<Args>...>)
  explicit DbAwareThread(F &&f, Args &&...args)
      : DbAwareThread(tls_db_arena_idx, std::forward<F>(f), std::forward<Args>(args)...) {}

  // Explicit arena index.
  template <typename F, typename... Args>
    requires(std::is_invocable_v<std::decay_t<F>, std::decay_t<Args>...> ||
             std::is_invocable_v<std::decay_t<F>, std::stop_token, std::decay_t<Args>...>)
  DbAwareThread(unsigned arena_idx, F &&f, Args &&...args)
      : thread_(
            [arena_idx, func = std::forward<F>(f)](std::stop_token st, std::decay_t<Args>... a) mutable {
              tls_db_arena_idx = arena_idx;
#if USE_JEMALLOC
              if (arena_idx != 0) {
                je_mallctl("thread.arena", nullptr, nullptr, &arena_idx, sizeof(arena_idx));
              }
#endif
              if constexpr (std::is_invocable_v<std::decay_t<F>, std::stop_token, std::decay_t<Args>...>) {
                func(std::move(st), std::move(a)...);
              } else {
                func(std::move(a)...);
              }
            },
            std::forward<Args>(args)...) {
  }

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

 private:
  std::jthread thread_;
};

// Stateless C++ allocator that routes allocations to the DB arena currently
// pinned on this thread (tls_db_arena_idx).  When no arena is pinned (idx==0)
// it falls back to the process-default allocator.
//
// Zero data members → qualifies for EBO in containers (e.g. small_vector,
// SkipList) so their sizeof is unchanged vs. the plain std::allocator<T> case.
//
// Accounting: per-DB arena extent hooks (installed at Database construction)
// attribute committed OS pages to the database's MemoryTracker. Individual
// allocations routed via MALLOCX_ARENA therefore show up in that tracker.
template <typename T>
struct DbAwareAllocator {
  using value_type = T;

  // Propagate on container move/swap so the allocator stays correct.
  using propagate_on_container_move_assignment = std::true_type;
  using propagate_on_container_copy_assignment = std::true_type;
  using propagate_on_container_swap = std::true_type;

  DbAwareAllocator() noexcept = default;

  template <typename U>
  explicit DbAwareAllocator(DbAwareAllocator<U> const & /*unused*/) noexcept {}

  [[nodiscard]] T *allocate(std::size_t n) {
#if USE_JEMALLOC
    const unsigned idx = tls_db_arena_idx;
    if (idx != 0) {
      int flags = MALLOCX_ARENA(idx) | MALLOCX_TCACHE_NONE;
      if constexpr (alignof(T) > alignof(std::max_align_t)) {
        flags |= MALLOCX_ALIGN(alignof(T));
      }
      void *p = je_mallocx(n * sizeof(T), flags);
      if (!p) throw std::bad_alloc{};
      return static_cast<T *>(p);
    }
#endif
    return static_cast<T *>(::operator new(n * sizeof(T), std::align_val_t{alignof(T)}));
  }

  // DEVNOTE: jemalloc tracks the owning arena per-extent in its own metadata.
  //          je_free(p) always routes to the correct arena regardless of which
  //          thread calls it, so GC can safely free query-thread allocations.
  void deallocate(T *p, [[maybe_unused]] std::size_t n) noexcept {
#if USE_JEMALLOC
    je_free(p);
#else
    ::operator delete(static_cast<void *>(p), n * sizeof(T), std::align_val_t{alignof(T)});
#endif
  }

  template <typename U>
  friend bool operator==(DbAwareAllocator<T> const & /*lhs*/, DbAwareAllocator<U> const & /*rhs*/) noexcept {
    return true;
  }

  template <typename U>
  friend bool operator!=(DbAwareAllocator<T> const &lhs, DbAwareAllocator<U> const &rhs) noexcept {
    return !(lhs == rhs);
  }
};

}  // namespace memgraph::memory
