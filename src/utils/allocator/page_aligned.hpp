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

#include "utils/db_aware_allocator.hpp"

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
    return static_cast<T *>(memory::DbAllocateBytes(size, arena_idx_, PAGE_SIZE));
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
