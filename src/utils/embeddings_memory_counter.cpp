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

#include "utils/embeddings_memory_counter.hpp"

#include "utils/memory_tracker.hpp"

namespace memgraph::utils {

// NOLINTNEXTLINE(cppcoreguidelines-avoid-non-const-global-variables)
EmbeddingsMemoryCounter embeddings_memory_counter{};

int64_t EmbeddingsMemoryCounter::PredictArenaTotalMmap(int64_t data_bytes, int64_t head_size) {
  if (data_bytes <= 0) return 0;

  // Models usearch memory_mapping_allocator_gt: min_capacity=4MiB, multiplier=2.
  // First arena = min_capacity * multiplier = 8 MiB. Each subsequent doubles.
  // Usable space per arena = capacity - head_size (header stores prev-pointer + capacity).
  constexpr int64_t kMinCapacity = 4 * 1024 * 1024;
  int64_t total_mmap = 0;
  int64_t filled = 0;
  int64_t arena_cap = kMinCapacity * 2;

  while (filled < data_bytes) {
    total_mmap += arena_cap;
    filled += arena_cap - head_size;
    arena_cap *= 2;
  }

  return total_mmap;
}

int64_t EmbeddingsMemoryCounter::Amount() const {
  const auto hnsw = hnsw_data_.load(std::memory_order_relaxed);
  const auto vec = vec_data_.load(std::memory_order_relaxed);
  return PredictArenaTotalMmap(hnsw, kHnswArenaHeadSize) + PredictArenaTotalMmap(vec, kVecArenaHeadSize);
}

bool EmbeddingsMemoryCounter::CanAllocate(int64_t hnsw_bytes, int64_t vec_bytes) const {
  const auto hnsw = hnsw_data_.load(std::memory_order_relaxed);
  const auto vec = vec_data_.load(std::memory_order_relaxed);
  const auto predicted_after = PredictArenaTotalMmap(hnsw + hnsw_bytes, kHnswArenaHeadSize) +
                               PredictArenaTotalMmap(vec + vec_bytes, kVecArenaHeadSize);

  const auto emb_limit = embeddings_limit_.load(std::memory_order_relaxed);
  if (emb_limit > 0 && predicted_after > emb_limit) {
    return false;
  }

  const auto ovr_limit = overall_limit_.load(std::memory_order_relaxed);
  if (ovr_limit > 0) {
    const auto tracked_memory = total_memory_tracker.Amount();
    if (tracked_memory + predicted_after > ovr_limit) {
      return false;
    }
  }

  return true;
}

}  // namespace memgraph::utils
