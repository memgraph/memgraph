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

/// Lightweight atomic counter for tracking embeddings memory usage.
/// Used to enforce a separate license limit on vector index embeddings.
///
/// Actual memory is also tracked by total_memory_tracker via jemalloc.
/// This counter accumulates precise deltas from usearch memory_usage()
/// before/after each operation, so no estimation is needed.
class EmbeddingsMemoryCounter {
 public:
  /// Adds a signed delta (positive = grew, negative = shrunk).
  void TrackDelta(int64_t delta) { amount_.fetch_add(delta, std::memory_order_relaxed); }

  int64_t Amount() const { return amount_.load(std::memory_order_relaxed); }

  void SetLimit(int64_t limit) { limit_.store(limit, std::memory_order_relaxed); }

  int64_t Limit() const { return limit_.load(std::memory_order_relaxed); }

 private:
  std::atomic<int64_t> amount_{0};
  std::atomic<int64_t> limit_{0};
};

extern EmbeddingsMemoryCounter global_embeddings_memory_counter;

}  // namespace memgraph::utils
