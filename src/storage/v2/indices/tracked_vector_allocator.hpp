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

#include <atomic>
#include <cstddef>
#include <cstdint>
#include <utility>

#include "usearch/index_plugins.hpp"
#include "utils/memory_tracker.hpp"

namespace memgraph::storage {

/// Wraps usearch's memory_mapping_allocator_gt and reports all mmap
/// allocations/deallocations to the global memory tracker.
///
/// Tracks the aligned bytes actually handed to usearch per allocation, not the
/// full arena capacity. This keeps mmap_memory_tracked close to RSS (physical
/// memory written), avoiding the large discrepancy that arises when
/// total_allocated() counts the entire reserved-but-unwritten arena tail.
///
/// Drop-in replacement for memory_mapping_allocator_gt as a tape_allocator
/// or vectors_tape_allocator template parameter of index_dense_gt.
template <std::size_t alignment_ak = 1>
class TrackedVectorAllocator {
 public:
  using value_type = unum::usearch::byte_t;
  using size_type = std::size_t;
  using pointer = unum::usearch::byte_t *;
  using const_pointer = unum::usearch::byte_t const *;

  TrackedVectorAllocator() = default;

  TrackedVectorAllocator(TrackedVectorAllocator &&other) noexcept
      : inner_(std::move(other.inner_)), tracked_bytes_(other.tracked_bytes_.exchange(0, std::memory_order_relaxed)) {}

  TrackedVectorAllocator &operator=(TrackedVectorAllocator &&other) noexcept {
    if (this != &other) {
      reset();
      inner_ = std::move(other.inner_);
      tracked_bytes_.store(other.tracked_bytes_.exchange(0, std::memory_order_relaxed), std::memory_order_relaxed);
    }
    return *this;
  }

  /// Matches memory_mapping_allocator_gt's own copy semantics. The copy starts
  /// as a fresh empty allocator; the original retains its arena and tracking.
  TrackedVectorAllocator(TrackedVectorAllocator const &) noexcept {}

  /// Releases any existing arena owned by *this, then becomes an empty allocator.
  /// Matches memory_mapping_allocator_gt's own copy-assignment semantics.
  TrackedVectorAllocator &operator=(TrackedVectorAllocator const &) noexcept {
    reset();
    return *this;
  }

  ~TrackedVectorAllocator() noexcept { reset(); }

  pointer allocate(size_type count_bytes) {
    const auto extended = unum::usearch::divide_round_up<alignment_ak>(count_bytes) * alignment_ak;
    if (!utils::mmap_memory_tracker.Alloc(static_cast<int64_t>(extended))) {
      if (auto maybe_msg = utils::MemoryErrorStatus().msg(); maybe_msg) {
        throw utils::OutOfMemoryException(std::move(*maybe_msg));
      }
      throw utils::OutOfMemoryException("Failed to allocate memory for vector index.");
    }

    auto *result = inner_.allocate(count_bytes);
    if (!result) {
      utils::mmap_memory_tracker.Free(static_cast<int64_t>(extended));
      throw utils::OutOfMemoryException("Failed to allocate memory for vector index.");
    }
    tracked_bytes_.fetch_add(extended, std::memory_order_relaxed);
    return result;
  }

  /// Any call to deallocate() frees the entire arena — individual element
  /// deallocation is not supported by the underlying bump-pointer allocator.
  void deallocate(pointer /*p*/ = nullptr, size_type /*n*/ = 0) noexcept { reset(); }

  void reset() noexcept {
    auto old = tracked_bytes_.exchange(0, std::memory_order_relaxed);
    inner_.reset();
    if (old > 0) {
      utils::mmap_memory_tracker.Free(static_cast<int64_t>(old));
    }
  }

  size_type total_allocated() const noexcept { return inner_.total_allocated(); }

  size_type total_wasted() const noexcept { return inner_.total_wasted(); }

  size_type total_reserved() const noexcept { return inner_.total_reserved(); }

 private:
  unum::usearch::memory_mapping_allocator_gt<alignment_ak> inner_;
  std::atomic<size_type> tracked_bytes_{0};
};

}  // namespace memgraph::storage
