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

  void Add(int64_t bytes) { amount_.fetch_add(bytes, std::memory_order_relaxed); }

  void Sub(int64_t bytes) { amount_.fetch_sub(bytes, std::memory_order_relaxed); }

  void Reset() { amount_.store(0, std::memory_order_relaxed); }

  int64_t Amount() const { return amount_.load(std::memory_order_relaxed); }

  void SetEmbeddingsLimit(int64_t limit) { embeddings_limit_.store(limit, std::memory_order_relaxed); }

  int64_t EmbeddingsLimit() const { return embeddings_limit_.load(std::memory_order_relaxed); }

  void SetOverallLimit(int64_t limit) { overall_limit_.store(limit, std::memory_order_relaxed); }

  int64_t OverallLimit() const { return overall_limit_.load(std::memory_order_relaxed); }

  /// Fast pre-check before adding an embedding.
  /// Returns true if the allocation is allowed under both limits.
  /// Not perfectly linearizable (speed over precision) -- the two atomic loads
  /// are not synchronized, but that's acceptable for this use case.
  bool CanAllocate(int64_t bytes) const;

 private:
  std::atomic<int64_t> amount_{0};
  std::atomic<int64_t> embeddings_limit_{0};  // 0 = unlimited
  std::atomic<int64_t> overall_limit_{0};     // 0 = unlimited
};

extern EmbeddingsMemoryCounter embeddings_memory_counter;

}  // namespace memgraph::utils
