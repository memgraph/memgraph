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
#include <cstdlib>
#include <cstring>
#include <mutex>
#include <utility>
#include <vector>

#include "utils/embeddings_memory_counter.hpp"

namespace memgraph::utils {

/// Arena allocator backed by std::aligned_alloc (routed through jemalloc).
/// Drop-in replacement for usearch's memory_mapping_allocator_gt but uses the
/// process allocator instead of raw mmap, so all allocations are automatically
/// tracked by jemalloc's extent hooks and total_memory_tracker.
///
/// Additionally reports arena-level allocations/deallocations to
/// global_embeddings_memory_counter for embeddings-specific limit enforcement.
///
/// Same doubling-arena strategy: 4 MiB min, 2x growth, bump-pointer within
/// each arena, "alloc many / free at once" lifecycle.
///
/// Thread-safe except constructors and destructors (same contract as usearch).
template <std::size_t alignment_ak = 1>
class jemalloc_arena_allocator_gt {
  using byte_t = char;

  static constexpr std::size_t min_capacity() { return std::size_t{1024} * 1024 * 4; }

  static constexpr std::size_t capacity_multiplier() { return 2; }

  static constexpr std::size_t head_size() {
    return ((sizeof(byte_t *) + sizeof(std::size_t) + alignment_ak - 1) / alignment_ak) * alignment_ak;
  }

  static std::size_t divide_round_up(std::size_t x, std::size_t d) { return (x + d - 1) / d; }

  static std::size_t ceil2(std::size_t v) {
    v--;
    v |= v >> 1;
    v |= v >> 2;
    v |= v >> 4;
    v |= v >> 8;
    v |= v >> 16;
    v |= v >> 32;
    return v + 1;
  }

  std::mutex mutex_;
  byte_t *last_arena_{nullptr};
  std::size_t last_usage_{head_size()};
  std::size_t last_capacity_{min_capacity()};
  std::size_t wasted_space_{0};
  std::vector<std::pair<byte_t *, std::size_t>> arenas_;

 public:
  using value_type = byte_t;
  using size_type = std::size_t;
  using pointer = byte_t *;
  using const_pointer = byte_t const *;

  jemalloc_arena_allocator_gt() = default;

  jemalloc_arena_allocator_gt(jemalloc_arena_allocator_gt &&other) noexcept
      : last_arena_(std::exchange(other.last_arena_, nullptr)),
        last_usage_(std::exchange(other.last_usage_, head_size())),
        last_capacity_(std::exchange(other.last_capacity_, min_capacity())),
        wasted_space_(std::exchange(other.wasted_space_, 0)),
        arenas_(std::move(other.arenas_)) {}

  jemalloc_arena_allocator_gt &operator=(jemalloc_arena_allocator_gt &&other) noexcept {
    if (this != &other) {
      reset();
      last_arena_ = std::exchange(other.last_arena_, nullptr);
      last_usage_ = std::exchange(other.last_usage_, head_size());
      last_capacity_ = std::exchange(other.last_capacity_, min_capacity());
      wasted_space_ = std::exchange(other.wasted_space_, 0);
      arenas_ = std::move(other.arenas_);
    }
    return *this;
  }

  jemalloc_arena_allocator_gt(const jemalloc_arena_allocator_gt &) noexcept {}

  jemalloc_arena_allocator_gt &operator=(const jemalloc_arena_allocator_gt &other) noexcept {
    if (this != &other) {
      reset();
    }
    return *this;
  }

  ~jemalloc_arena_allocator_gt() noexcept { reset(); }

  void reset() noexcept {
    std::size_t total = 0;
    for (auto &[ptr, sz] : arenas_) {
      total += sz;
      std::free(ptr);
    }
    if (total > 0) {
      global_embeddings_memory_counter.TrackDelta(-static_cast<int64_t>(total));
    }
    arenas_.clear();
    last_arena_ = nullptr;
    last_usage_ = head_size();
    last_capacity_ = min_capacity();
    wasted_space_ = 0;
  }

  byte_t *allocate(std::size_t count_bytes) noexcept {
    std::size_t extended_bytes = divide_round_up(count_bytes, alignment_ak) * alignment_ak;
    std::unique_lock<std::mutex> lock(mutex_);
    if (!last_arena_ || (last_usage_ + extended_bytes >= last_capacity_)) {
      std::size_t new_cap = (std::max)(last_capacity_, ceil2(extended_bytes)) * capacity_multiplier();
      auto *new_arena = static_cast<byte_t *>(std::aligned_alloc(alignment_ak, new_cap));
      if (!new_arena) return nullptr;
      arenas_.emplace_back(new_arena, new_cap);
      global_embeddings_memory_counter.TrackDelta(static_cast<int64_t>(new_cap));

      if (last_arena_) {
        wasted_space_ += last_capacity_ - last_usage_;
      }
      last_arena_ = new_arena;
      last_capacity_ = new_cap;
      last_usage_ = head_size();
    }
    wasted_space_ += extended_bytes - count_bytes;
    byte_t *result = last_arena_ + last_usage_;
    last_usage_ += extended_bytes;
    return result;
  }

  void deallocate(byte_t * /*ptr*/ = nullptr, std::size_t /*count*/ = 0) noexcept { reset(); }

  std::size_t total_allocated() const noexcept {
    std::size_t total = 0;
    for (const auto &[ptr, sz] : arenas_) total += sz;
    return total;
  }

  std::size_t total_wasted() const noexcept { return wasted_space_; }

  std::size_t total_reserved() const noexcept { return last_arena_ ? last_capacity_ - last_usage_ : 0; }
};

}  // namespace memgraph::utils
