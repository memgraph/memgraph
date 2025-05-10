// Copyright 2025 Memgraph Ltd.
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
#include <cstdlib>
#include <memory_resource>

namespace memgraph::utils {

/// This is a monotonic allocator which:
/// - grabs 1 page for each slab
/// - allocations are aligned to page size
/// - allocations smaller than a page use remaining space in current slab
/// - allocations larger than a page get their own allocations
/// - deallocation is a noop
/// - all memory released on destruction
struct PageSlabMemoryResource : std::pmr::memory_resource {
  static constexpr std::size_t PAGE_SIZE = 4096;
  PageSlabMemoryResource() = default;

  PageSlabMemoryResource(PageSlabMemoryResource const &) = delete;
  PageSlabMemoryResource &operator=(PageSlabMemoryResource const &) = delete;

  PageSlabMemoryResource(PageSlabMemoryResource &&other) noexcept {
    std::swap(pages, other.pages);
    std::swap(ptr, other.ptr);
    std::swap(space, other.space);
  }
  PageSlabMemoryResource &operator=(PageSlabMemoryResource &&other) noexcept {
    if (this == &other) return *this;
    std::swap(*this, other);
    return *this;
  }

  ~PageSlabMemoryResource() override {
    auto current = pages;
    while (current) {
      auto next = current->next;
      operator delete(current, current->alignment);
      current = next;
    }
  }

 private:
  struct header {
    explicit header(header *next, std::align_val_t alignment) : next(next), alignment{alignment} {}
    header *next = nullptr;
    std::align_val_t alignment;
  };

  constexpr static size_t alignSize(size_t size, size_t alignment) { return (size + alignment - 1) & ~(alignment - 1); }

  void *do_allocate(size_t bytes, size_t alignment) final {
    // 1. could this fit inside a page slab?
    auto earliest_slab_position = alignSize(sizeof(header), alignment);
    auto max_slab_capacity = PAGE_SIZE - earliest_slab_position;
    if (max_slab_capacity < bytes) [[unlikely]] {
      auto required_bytes = bytes + earliest_slab_position;
      auto *newmem = reinterpret_cast<header *>(operator new (required_bytes, std::align_val_t{alignment}));
      // add to the allocation list
      pages = std::construct_at<header>(newmem, pages, std::align_val_t{alignment});
      return reinterpret_cast<std::byte *>(pages) + earliest_slab_position;
    }

    // 2. can it fit in existing slab?
    if (!std::align(alignment, bytes, ptr, space)) {
      auto *newmem = reinterpret_cast<header *>(operator new (PAGE_SIZE, std::align_val_t{PAGE_SIZE}));
      pages = std::construct_at<header>(newmem, pages, std::align_val_t{PAGE_SIZE});
      ptr = reinterpret_cast<std::byte *>(pages) + sizeof(header);
      space = PAGE_SIZE - sizeof(header);
      std::align(alignment, bytes, ptr, space);
    }

    // 3. use current slab
    // NOTE: ptr and space have already been via std::align, alignment is correct here
    void *res = ptr;
    ptr = reinterpret_cast<std::byte *>(ptr) + bytes;
    space -= bytes;
    return res;
  }
  void do_deallocate(void * /*p*/, size_t /*bytes*/, size_t /*alignment*/) final { /*noop*/
  }
  bool do_is_equal(memory_resource const &other) const noexcept final { return std::addressof(other) == this; }

 private:
  header *pages = nullptr;
  void *ptr = nullptr;
  size_t space = 0;
};

}  // namespace memgraph::utils
