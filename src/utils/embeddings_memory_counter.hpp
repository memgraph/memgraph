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
#include <cstdint>

namespace memgraph::utils {

/// Lock-free counter for vector index embedding memory.
/// Tracks memory consumed by usearch vector storage (which uses mmap internally
/// and is invisible to jemalloc-based total_memory_tracker).
///
/// Usearch has two separate mmap-backed arena allocators per index:
///   - HNSW tape  (alignment=64) — graph nodes (key + level + neighbor lists)
///   - Vector tape (alignment=8) — raw vector data
/// We track these separately so we can predict the arena overhead accurately
/// (each allocator has its own independent doubling-arena chain).
///
/// Two independent limits can be enforced:
///   1. Embeddings-specific limit (from license) -- checked via embeddings_limit_
///   2. Overall memory limit (user-configured)   -- checked by combining this counter
///      with total_memory_tracker.Amount()
class EmbeddingsMemoryCounter final {
 public:
  EmbeddingsMemoryCounter() = default;

  EmbeddingsMemoryCounter(const EmbeddingsMemoryCounter &) = delete;
  EmbeddingsMemoryCounter &operator=(const EmbeddingsMemoryCounter &) = delete;
  EmbeddingsMemoryCounter(EmbeddingsMemoryCounter &&) = delete;
  EmbeddingsMemoryCounter &operator=(EmbeddingsMemoryCounter &&) = delete;

  void Add(int64_t hnsw_bytes, int64_t vec_bytes) {
    hnsw_data_.fetch_add(hnsw_bytes, std::memory_order_relaxed);
    vec_data_.fetch_add(vec_bytes, std::memory_order_relaxed);
  }

  void Sub(int64_t hnsw_bytes, int64_t vec_bytes) {
    hnsw_data_.fetch_sub(hnsw_bytes, std::memory_order_relaxed);
    vec_data_.fetch_sub(vec_bytes, std::memory_order_relaxed);
  }

  void Reset() {
    hnsw_data_.store(0, std::memory_order_relaxed);
    vec_data_.store(0, std::memory_order_relaxed);
  }

  /// Returns the raw data estimate (sum of per-vector HNSW + vector bytes) WITHOUT arena overhead.
  int64_t DataAmount() const {
    return hnsw_data_.load(std::memory_order_relaxed) + vec_data_.load(std::memory_order_relaxed);
  }

  /// Returns the predicted total mmap'd memory INCLUDING arena overhead.
  /// Models each allocator's doubling-arena chain independently.
  int64_t Amount() const;

  void SetEmbeddingsLimit(int64_t limit) { embeddings_limit_.store(limit, std::memory_order_relaxed); }

  int64_t EmbeddingsLimit() const { return embeddings_limit_.load(std::memory_order_relaxed); }

  void SetOverallLimit(int64_t limit) { overall_limit_.store(limit, std::memory_order_relaxed); }

  int64_t OverallLimit() const { return overall_limit_.load(std::memory_order_relaxed); }

  /// Fast pre-check before adding an embedding.
  /// Returns true if the allocation is allowed under both limits.
  /// Not perfectly linearizable (speed over precision) -- the two atomic loads
  /// are not synchronized, but that's acceptable for this use case.
  bool CanAllocate(int64_t hnsw_bytes, int64_t vec_bytes) const;

  /// Models the doubling-arena allocator used by usearch's memory_mapping_allocator_gt.
  /// Given the total data written to one allocator, returns the total mmap'd bytes
  /// (data + arena headers + reserved tail of the last arena).
  ///
  /// Arena growth: first arena = 2 * min_capacity (8 MiB), each subsequent = 2x previous.
  /// Each arena has a small header (head_size bytes) at the start.
  static int64_t PredictArenaTotalMmap(int64_t data_bytes, int64_t head_size);

 private:
  /// HNSW tape: alignment=64, head_size = ceil(16/64)*64 = 64
  static constexpr int64_t kHnswArenaHeadSize = 64;
  /// Vector tape: alignment=8, head_size = ceil(16/8)*8 = 16
  static constexpr int64_t kVecArenaHeadSize = 16;

  std::atomic<int64_t> hnsw_data_{0};
  std::atomic<int64_t> vec_data_{0};
  std::atomic<int64_t> embeddings_limit_{0};  // 0 = unlimited
  std::atomic<int64_t> overall_limit_{0};     // 0 = unlimited
};

extern EmbeddingsMemoryCounter embeddings_memory_counter;

}  // namespace memgraph::utils
