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

#include <algorithm>
#include <cstddef>
#include <new>

#if USE_JEMALLOC
#include <jemalloc/jemalloc.h>
#endif

namespace memgraph::utils {

// custom allocator, ensures all allocations are page aligned.
// When arena_idx != 0 (jemalloc builds only), allocations are routed to that
// jemalloc arena so they are attributed to the owning DB's MemoryTracker.
template <typename T>
struct PageAlignedAllocator {
  static constexpr std::size_t PAGE_SIZE = 4096;
  using value_type = T;

  PageAlignedAllocator() = default;

  explicit PageAlignedAllocator(unsigned arena_idx) noexcept : arena_idx_(arena_idx) {}

  template <class U>
  explicit PageAlignedAllocator(const PageAlignedAllocator<U> &other) noexcept : arena_idx_(other.arena_idx_) {}

  auto allocate(std::size_t n) -> T * {
    auto size = std::max(n * sizeof(T), PAGE_SIZE);
    // Round up to the nearest multiple of PAGE_SIZE
    size = ((size + PAGE_SIZE - 1) / PAGE_SIZE) * PAGE_SIZE;
#if USE_JEMALLOC
    if (arena_idx_ != 0) {
      // Route to the DB arena; MALLOCX_ALIGN ensures the page alignment
      // requirement is met (PAGE_SIZE > alignof(std::max_align_t)).
      int flags = MALLOCX_ARENA(arena_idx_) | MALLOCX_TCACHE_NONE | MALLOCX_ALIGN(PAGE_SIZE);
      void *ptr = je_mallocx(size, flags);
      if (!ptr) throw std::bad_alloc{};
      return static_cast<T *>(ptr);
    }
#endif
    // we must use new/delete as it will correctly throw appropriate exception
    void *ptr = operator new(size, std::align_val_t{PAGE_SIZE});
    return static_cast<T *>(ptr);
  }

  void deallocate(T *p, std::size_t) const noexcept {
#if USE_JEMALLOC
    je_free(p);
#else
    operator delete(p, std::align_val_t{PAGE_SIZE});
#endif
  }

  friend bool operator==(PageAlignedAllocator const &lhs, PageAlignedAllocator const &rhs) noexcept {
    return lhs.arena_idx_ == rhs.arena_idx_;
  }

  unsigned arena_idx_{0};
};

}  // namespace memgraph::utils
