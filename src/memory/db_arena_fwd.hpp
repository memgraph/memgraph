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
#include <thread>
#include <type_traits>
#include <utility>

#include "utils/db_aware_allocator.hpp"

namespace memgraph::dbms {
class Database;
}

namespace memgraph::memory {

class DbArena;

// Lightweight handle for Storage to interact with Database's per-thread arena pool.
// This decouples Storage from Database while allowing Storage to:
// 1. Acquire per-thread arenas for background threads
// 2. Access the base arena index for allocator construction
//
// IMPORTANT: Database must outlive Storage (Storage holds a pointer to Database's DbArena).
// This is guaranteed by Database's member declaration order (db_arena_ declared after storage_).
class ArenaRegistration {
 public:
  ArenaRegistration() noexcept = default;

  explicit ArenaRegistration(DbArena *arena) noexcept : arena_(arena) {}

  // Get the base arena index (for backwards compatibility with allocator construction).
  // Returns 0 if no arena is registered (non-jemalloc builds or unit tests).
  unsigned BaseArenaIdx() const noexcept;

  // Acquire (or create) the arena for the current thread.
  // First call per thread creates a new arena; subsequent calls return the cached arena.
  // Returns 0 if no arena is registered.
  unsigned AcquireThreadArena() const noexcept;

  explicit operator bool() const noexcept { return arena_ != nullptr; }

 private:
  DbArena *arena_{nullptr};
};

// A std::pmr::memory_resource that routes allocations to a specific jemalloc
// arena. Used as the upstream for PageSlabMemoryResource so that Delta slab
// pages are attributed to the owning DB's MemoryTracker.
//
// arena_idx == 0 -> falls back to operator new / operator delete.
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

// RAII guard: installs a DB arena for the duration of its scope and restores
// the previous TLS value on destruction. Supports nested scopes.
struct DbArenaScope {
  // Acquire per-thread arena from the database.
  explicit DbArenaScope(memgraph::dbms::Database *db);

  // Legacy constructor: direct arena index.
  explicit DbArenaScope(unsigned arena_idx) noexcept;

  ~DbArenaScope() noexcept;

  DbArenaScope(const DbArenaScope &) = delete;
  DbArenaScope &operator=(const DbArenaScope &) = delete;
  DbArenaScope(DbArenaScope &&) = delete;
  DbArenaScope &operator=(DbArenaScope &&) = delete;

 private:
  unsigned prev_arena_{0};
};

// A std::jthread wrapper that sets tls_db_arena_state on the new thread so that
// allocations through DbAwareAllocator / ArenaAwareAllocator containers are
// attributed to the owning DB arena.
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
              const DbArenaScope db_arena_scope{arena_idx};
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
