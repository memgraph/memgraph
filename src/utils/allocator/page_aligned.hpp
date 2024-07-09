// Copyright 2024 Memgraph Ltd.
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

namespace memgraph::utils {

// custom allocator, ensures all allocations are page aligned
template <typename T>
struct PageAlignedAllocator {
  static constexpr std::size_t PAGE_SIZE = 4096;
  using value_type = T;

  PageAlignedAllocator() = default;

  template <class U>
  explicit PageAlignedAllocator(const PageAlignedAllocator<U> &) noexcept {}

  auto allocate(std::size_t n) -> T * {
    auto size = std::max(n * sizeof(T), PAGE_SIZE);
    // Round up to the nearest multiple of PAGE_SIZE
    size = ((size + PAGE_SIZE - 1) / PAGE_SIZE) * PAGE_SIZE;
    // we must use new/delete as it will correctly throw appropriate exception
    void *ptr = operator new (size, std::align_val_t{PAGE_SIZE});
    return static_cast<T *>(ptr);
  }

  void deallocate(T *p, std::size_t) const noexcept { operator delete (p, std::align_val_t{PAGE_SIZE}); }

  constexpr friend bool operator==(PageAlignedAllocator const &, PageAlignedAllocator const &) noexcept { return true; }
};

}  // namespace memgraph::utils
