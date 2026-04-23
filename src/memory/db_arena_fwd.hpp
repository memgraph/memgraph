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

#include "utils/allocator/page_slab_memory_resource.hpp"
#include "utils/db_aware_allocator.hpp"

namespace memgraph::dbms {
class Database;
}

namespace memgraph::memory {

class ArenaPool;

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
    DbDeallocateBytes(p, bytes, alignment);
  }

  bool do_is_equal(const std::pmr::memory_resource &other) const noexcept override {
    const auto *o = dynamic_cast<const ArenaMemoryResource *>(&other);
    return o && o->arena_idx_ == arena_idx_;
  }

  unsigned arena_idx_;
};

// Owns the optional arena-aware upstream needed by PageSlabMemoryResource.
// Storage containers use this helper instead of branching on jemalloc when
// constructing page-slab resources for DB-owned data.
class ArenaPageSlabMemoryResource {
 public:
  explicit ArenaPageSlabMemoryResource(unsigned arena_idx) noexcept
#if USE_JEMALLOC
      : arena_upstream_(arena_idx)
#endif
  {
  }

  ArenaPageSlabMemoryResource(ArenaPageSlabMemoryResource &&) noexcept = default;
  ArenaPageSlabMemoryResource &operator=(ArenaPageSlabMemoryResource &&) noexcept = default;

  ArenaPageSlabMemoryResource(const ArenaPageSlabMemoryResource &) = delete;
  ArenaPageSlabMemoryResource &operator=(const ArenaPageSlabMemoryResource &) = delete;

  friend void swap(ArenaPageSlabMemoryResource &lhs, ArenaPageSlabMemoryResource &rhs) noexcept {
#if USE_JEMALLOC
    using std::swap;
    swap(lhs.arena_upstream_, rhs.arena_upstream_);
#else
    (void)lhs;
    (void)rhs;
#endif
  }

  auto MakeResource() -> std::unique_ptr<utils::PageSlabMemoryResource> {
    return std::make_unique<utils::PageSlabMemoryResource>(Upstream());
  }

  void Rebind(utils::PageSlabMemoryResource *resource) noexcept {
    if (resource) {
      resource->set_upstream(Upstream());
    }
  }

 private:
  auto Upstream() noexcept -> std::pmr::memory_resource * {
#if USE_JEMALLOC
    return &arena_upstream_;
#else
    return std::pmr::get_default_resource();
#endif
  }

#if USE_JEMALLOC
  ArenaMemoryResource arena_upstream_{0};
#endif
};

// =============================================================================
// DB Arena Threading Policy
// =============================================================================
//
// Scoped arena attribution is TLS-based: DbArenaScope writes
// tls_db_arena_state.arena and DbAwareAllocator reads it per allocation.
// Long-lived arena-aware owners capture an explicit arena index instead.
//
// Raw jemalloc thread.arena pinning (je_mallctl "thread.arena") is NOT used as
// a general scope because restoring to automatic per-CPU arenas returns EPERM.
//
// Thread types and their correct setup:
//
//   Long-lived dedicated DB threads (TTL, GC, trigger pool):
//     Install a pool-backed DbArenaScope at the work boundary.
//     The scope acquires from the DB's ArenaPool and releases on exit.
//
//   AsyncIndexer:
//     Uses DbAwareThread with the owning storage's ArenaPool; the thread body
//     should not install an additional broad DbArenaScope.
//
//   Short-lived thread-pool tasks (replication, plan cache):
//     Use a pool-backed DbArenaScope for the task duration. Raw arena indices
//     should be reserved for low-level allocator/recovery APIs that need them.
//
//   Query execution threads (Pull, Commit, Abort):
//     DbArenaScope is placed at the entry point once the target DB is known.
//     System queries must not pin a DB arena.
//
// =============================================================================

// RAII guard: installs a DB arena for the duration of its scope and restores
// the previous TLS value on destruction.
//
// Nesting rules (pool-backed constructor):
//   - No prior DB arena in TLS → acquire from the pool; release on destruction.
//   - Prior arena belongs to the same pool → borrow it; do not release on destruction.
//   - Prior arena belongs to a different pool → DMG_ASSERT (cross-DB collision).
//
// The default constructor (no pool) is a no-op scope that resets TLS to 0 and
// asserts there was no previous arena. It exists only for non-jemalloc builds
// where all constructors compile to no-ops.
struct DbArenaScope {
  enum class Type : uint8_t {
    DBG_CHECK,
    FORCE,
  };

  // No-op scope: resets TLS arena to 0 (asserts no previous arena was set).
  DbArenaScope() noexcept;

  // Acquire per-thread arena from the database's pool.
  explicit DbArenaScope(const memgraph::dbms::Database *db, Type type = Type::DBG_CHECK);

  // Acquire from the pool (or borrow if same pool already in TLS).
  // Releases back to the pool on destruction only when this scope acquired it.
  explicit DbArenaScope(ArenaPool *arena_pool, Type type = Type::DBG_CHECK);

  ~DbArenaScope() noexcept;

  DbArenaScope(const DbArenaScope &) = delete;
  DbArenaScope &operator=(const DbArenaScope &) = delete;
  DbArenaScope(DbArenaScope &&) = delete;
  DbArenaScope &operator=(DbArenaScope &&) = delete;

 private:
  ArenaPool *arena_pool_{nullptr};
  ArenaPool *prev_arena_pool_{nullptr};
  unsigned arena_idx_{0};
  unsigned prev_arena_{0};
};

// A std::jthread wrapper that sets tls_db_arena_state on the new thread so that
// DbAwareAllocator allocations and other TLS-scoped paths are attributed to the
// owning DB arena.
//
// Non-jemalloc builds: no-op wrapper, identical behaviour to std::jthread.
class DbAwareThread {
 public:
  // Inherit the calling thread's ArenaPool, not its raw arena index.
  template <typename F, typename... Args>
    requires(std::is_invocable_v<std::decay_t<F>, std::decay_t<Args>...> ||
             std::is_invocable_v<std::decay_t<F>, std::stop_token, std::decay_t<Args>...>)
  explicit DbAwareThread(F &&f, Args &&...args)
      : DbAwareThread(tls_db_arena_state.arena_pool, std::forward<F>(f), std::forward<Args>(args)...) {}

  // The new thread acquires from the pool for the lifetime
  // of the supplied function and releases it when the function exits.
  template <typename F, typename... Args>
    requires(std::is_invocable_v<std::decay_t<F>, std::decay_t<Args>...> ||
             std::is_invocable_v<std::decay_t<F>, std::stop_token, std::decay_t<Args>...>)
  DbAwareThread(ArenaPool *arena_pool, F &&f, Args &&...args)
      : thread_(
            [arena_pool, func = std::forward<F>(f)](std::stop_token st, std::decay_t<Args>... a) mutable {
              const DbArenaScope db_arena_scope{arena_pool};
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
