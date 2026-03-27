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
#include <new>
#include <thread>
#include <utility>

#if USE_JEMALLOC
#include <jemalloc/jemalloc.h>
#endif

#include "utils/db_aware_allocator.hpp"
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

  void do_deallocate(void *p, [[maybe_unused]] std::size_t bytes,
                     [[maybe_unused]] std::size_t alignment) noexcept override {
#if USE_JEMALLOC
    je_free(p);
#else
    ::operator delete(p, bytes, std::align_val_t{alignment});
#endif
  }

  bool do_is_equal(const std::pmr::memory_resource &other) const noexcept override {
    const auto *o = dynamic_cast<const ArenaMemoryResource *>(&other);
    return o && o->arena_idx_ == arena_idx_;
  }

  unsigned arena_idx_;
};

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

// Alias kept for call-site compatibility. Updates only tls_db_arena_idx (TLS),
// does NOT redirect thread.arena via je_mallctl. Only allocations going through
// DbAwareAllocator / ArenaAwareAllocator containers are attributed to the DB arena.
using DbArenaFullScope = DbArenaScope;

// A std::jthread wrapper that sets tls_db_arena_idx on the new thread so that
// allocations through DbAwareAllocator / ArenaAwareAllocator containers are
// attributed to the owning DB arena.  Does NOT redirect thread.arena via
// je_mallctl — raw allocations (std::string, operator new, …) on these threads
// go to the default jemalloc arena and are not attributed.
//
// Two construction forms:
//   DbAwareThread t{callable, args...};            // inherits tls_db_arena_idx from caller
//   DbAwareThread t{arena_idx, callable, args...}; // explicit arena index
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

 private:
  std::jthread thread_;
};

}  // namespace memgraph::memory
