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

// Thin header intentionally placed in utils/ to break the utils <-> memory
// circular dependency.  db_arena.hpp (in memory/) includes this file and adds
// the heavier DbArena / DbArenaScope / DbAwareThread machinery.  SkipList
// (in utils/) only needs tls_db_arena_idx and DbAwareAllocator.

#include <cstddef>
#include <new>
#include <type_traits>

#if USE_JEMALLOC
#include <jemalloc/jemalloc.h>
void *JeNew(size_t size, int flags);
#endif

namespace memgraph::memory {
template <typename T>
[[nodiscard]] T *DbAllocate(std::size_t n, unsigned idx) {
#if USE_JEMALLOC
  if (idx != 0) {
    int flags = MALLOCX_ARENA(idx) | MALLOCX_TCACHE_NONE;
    if constexpr (alignof(T) > alignof(std::max_align_t)) {
      flags |= MALLOCX_ALIGN(alignof(T));
    }
    return static_cast<T *>(JeNew(n * sizeof(T), flags));
  }
#endif
  return static_cast<T *>(::operator new(n * sizeof(T), std::align_val_t{alignof(T)}));
}

// Thread-local arena index for the currently active database.
// 0 means "no DB arena pinned" — allocations go to jemalloc's default arena.
// Set by DbArenaScope for shared query threads and directly at thread-start
// for DB-owned background threads (GC, snapshot, TTL, async indexer, ...).
//
// DEVNOTE: initial-exec TLS model requires this to be in the main executable,
//          not a shared library. Matches the pattern used by query_memory_control.
// NOLINTNEXTLINE(cppcoreguidelines-avoid-non-const-global-variables)
inline thread_local unsigned tls_db_arena_idx [[gnu::tls_model("initial-exec")]] = 0;

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
    const unsigned idx = tls_db_arena_idx;
    return DbAllocate<T>(n, idx);
  }

  // NOTE: jemalloc tracks the owning arena per-extent in its own metadata.
  //          je_free(p) always routes to the correct arena regardless of which
  //          thread calls it, so GC can safely free query-thread allocations.
  void deallocate(T *p, [[maybe_unused]] std::size_t n) noexcept {
    ::operator delete(static_cast<void *>(p), n * sizeof(T), std::align_val_t{alignof(T)});
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

// Stateful C++ allocator that always routes allocations to a specific jemalloc
// arena stored at construction time.  Unlike DbAwareAllocator (which reads TLS
// at allocation time), this allocator is self-contained and correct regardless
// of which thread performs the allocation or deallocation.
//
// Use this for long-lived objects owned by a single database (e.g. SkipList
// for vertices/edges/indices) so their node allocations are attributed to the
// right DB MemoryTracker without relying on thread.arena pinning.
//
// arena_idx == 0 → falls back to operator new / operator delete.
template <typename T>
struct ArenaAwareAllocator {
  using value_type = T;

  // Stateful — propagate so containers keep the right arena on move/swap.
  using propagate_on_container_move_assignment = std::true_type;
  using propagate_on_container_copy_assignment = std::true_type;
  using propagate_on_container_swap = std::true_type;

  explicit ArenaAwareAllocator(unsigned arena_idx = 0) noexcept : arena_idx_(arena_idx) {}

  template <typename U>
  explicit ArenaAwareAllocator(ArenaAwareAllocator<U> const &other) noexcept : arena_idx_(other.arena_idx_) {}

  unsigned arena_idx() const noexcept { return arena_idx_; }

  [[nodiscard]] T *allocate(std::size_t n) { return DbAllocate<T>(n, arena_idx_); }

  // NOTE: jemalloc tracks the owning arena per-extent in its own metadata.
  //          je_free(p) always routes to the correct arena regardless of which
  //          thread calls it, so GC can safely free query-thread allocations.
  void deallocate(T *p, [[maybe_unused]] std::size_t n) noexcept {
    ::operator delete(static_cast<void *>(p), n * sizeof(T), std::align_val_t{alignof(T)});
  }

  template <typename U>
  friend bool operator==(ArenaAwareAllocator<T> const &lhs, ArenaAwareAllocator<U> const &rhs) noexcept {
    return lhs.arena_idx_ == rhs.arena_idx_;
  }

  template <typename U>
  friend bool operator!=(ArenaAwareAllocator<T> const &lhs, ArenaAwareAllocator<U> const &rhs) noexcept {
    return !(lhs == rhs);
  }

  template <typename U>
  friend struct ArenaAwareAllocator;

 private:
  unsigned arena_idx_{0};
};

}  // namespace memgraph::memory
