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
#include <type_traits>

#if USE_JEMALLOC
#include <jemalloc/jemalloc.h>
#endif

namespace memgraph::memory {

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
