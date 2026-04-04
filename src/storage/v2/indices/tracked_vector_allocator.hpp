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
#include "utils/logging.hpp"
#include "utils/memory_tracker.hpp"

namespace memgraph::storage {

inline thread_local utils::MemoryTracker *tls_tracked_vector_allocator_memory_tracker = nullptr;

inline auto CurrentTrackedVectorAllocatorMemoryTracker() -> utils::MemoryTracker * {
  return tls_tracked_vector_allocator_memory_tracker != nullptr ? tls_tracked_vector_allocator_memory_tracker
                                                                : &utils::vector_index_memory_tracker;
}

class TrackedVectorAllocatorMemoryTrackerScope {
 public:
  explicit TrackedVectorAllocatorMemoryTrackerScope(utils::MemoryTracker *tracker)
      : previous_tracker_(tls_tracked_vector_allocator_memory_tracker) {
    tls_tracked_vector_allocator_memory_tracker = tracker != nullptr ? tracker : &utils::vector_index_memory_tracker;
  }

  TrackedVectorAllocatorMemoryTrackerScope(const TrackedVectorAllocatorMemoryTrackerScope &) = delete;
  auto operator=(const TrackedVectorAllocatorMemoryTrackerScope &)
      -> TrackedVectorAllocatorMemoryTrackerScope & = delete;

  ~TrackedVectorAllocatorMemoryTrackerScope() { tls_tracked_vector_allocator_memory_tracker = previous_tracker_; }

 private:
  utils::MemoryTracker *previous_tracker_;
};

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

  TrackedVectorAllocator() : tracker_(CurrentTrackedVectorAllocatorMemoryTracker()) {}

  TrackedVectorAllocator(TrackedVectorAllocator &&other) noexcept
      : inner_(std::move(other.inner_)),
        tracked_bytes_(other.tracked_bytes_.exchange(0, std::memory_order_relaxed)),
        tracker_(std::exchange(other.tracker_, &utils::vector_index_memory_tracker)) {}

  TrackedVectorAllocator &operator=(TrackedVectorAllocator &&other) noexcept {
    if (this != &other) {
      reset();
      inner_ = std::move(other.inner_);
      tracked_bytes_.store(other.tracked_bytes_.exchange(0, std::memory_order_relaxed), std::memory_order_relaxed);
      tracker_ = std::exchange(other.tracker_, &utils::vector_index_memory_tracker);
    }
    return *this;
  }

  /// Matches memory_mapping_allocator_gt's own copy semantics. The copy starts
  /// as a fresh empty allocator; the original retains its arena and tracking.
  TrackedVectorAllocator(TrackedVectorAllocator const &other) noexcept : tracker_(other.tracker_) {}

  /// Releases any existing arena owned by *this, then becomes an empty allocator.
  /// Matches memory_mapping_allocator_gt's own copy-assignment semantics.
  TrackedVectorAllocator &operator=(TrackedVectorAllocator const &other) noexcept {
    reset();
    tracker_ = other.tracker_;
    return *this;
  }

  ~TrackedVectorAllocator() noexcept { reset(); }

  pointer allocate(size_type count_bytes) {
    const auto extended = unum::usearch::divide_round_up<alignment_ak>(count_bytes) * alignment_ak;
    if (!tracker_->Alloc(static_cast<int64_t>(extended))) {
      auto msg = utils::MemoryErrorStatus().msg();
      DMG_ASSERT(msg, "MemoryErrorStatus should have a message when allocation fails");
      [[maybe_unused]] auto blocker = utils::MemoryTracker::OutOfMemoryExceptionBlocker{};
      throw utils::OutOfMemoryException(std::move(*msg));
    }

    auto *result = inner_.allocate(count_bytes);
    if (!result) {
      tracker_->Free(static_cast<int64_t>(extended));
      return nullptr;  // Since this is a drop-in replacement for memory_mapping_allocator_gt, we return nullptr on
                       // allocation failure as usearch expects it
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
      tracker_->Free(static_cast<int64_t>(old));
    }
  }

  size_type total_allocated() const noexcept { return inner_.total_allocated(); }

  size_type total_wasted() const noexcept { return inner_.total_wasted(); }

  size_type total_reserved() const noexcept { return inner_.total_reserved(); }

 private:
  unum::usearch::memory_mapping_allocator_gt<alignment_ak> inner_;
  std::atomic<size_type> tracked_bytes_{0};
  utils::MemoryTracker *tracker_{&utils::vector_index_memory_tracker};
};

}  // namespace memgraph::storage
