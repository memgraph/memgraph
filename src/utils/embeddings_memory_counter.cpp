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

// --- ArenaModel ---

void EmbeddingsMemoryCounter::ArenaModel::Add(int64_t bytes) {
  if (last_cap == 0) {
    last_cap = kMinCap * 2;  // first arena = 8 MiB
    last_used = head_size;
  }
  // Match usearch overflow condition: allocate new arena when current can't fit
  if (last_used + bytes >= last_cap) {
    total_committed += last_cap;
    last_cap *= 2;
    last_used = head_size;
  }
  last_used += bytes;
}

int64_t EmbeddingsMemoryCounter::ArenaModel::TotalMemory() const {
  if (last_cap == 0) return 0;
  const auto touched = (last_used + kPageSize - 1) / kPageSize * kPageSize;
  return total_committed + touched;
}

void EmbeddingsMemoryCounter::ArenaModel::Rebuild(int64_t total_data) {
  Reset();
  if (total_data <= 0) return;

  last_cap = kMinCap * 2;
  last_used = head_size;
  auto remaining = total_data;

  while (remaining > 0) {
    const auto space = last_cap - last_used;
    if (remaining < space) {
      last_used += remaining;
      return;
    }
    remaining -= space;
    total_committed += last_cap;
    last_cap *= 2;
    last_used = head_size;
  }
}

void EmbeddingsMemoryCounter::ArenaModel::Reset() {
  total_committed = 0;
  last_cap = 0;
  last_used = 0;
}

// --- EmbeddingsMemoryCounter ---

void EmbeddingsMemoryCounter::SyncTrackerAmount() {
  const auto current = hnsw_arena_.TotalMemory() + vec_arena_.TotalMemory();
  const auto delta = current - last_synced_amount_;
  if (delta > 0) {
    total_memory_tracker.TrackExternalAlloc(delta);
  } else if (delta < 0) {
    total_memory_tracker.TrackExternalFree(-delta);
  }
  last_synced_amount_ = current;
}

int64_t EmbeddingsMemoryCounter::Amount() const {
  std::lock_guard lock(mutex_);
  return hnsw_arena_.TotalMemory() + vec_arena_.TotalMemory();
}

void EmbeddingsMemoryCounter::Sub(int64_t hnsw_bytes, int64_t vec_bytes) {
  std::lock_guard lock(mutex_);
  hnsw_data_ -= hnsw_bytes;
  vec_data_ -= vec_bytes;
  hnsw_arena_.Rebuild(hnsw_data_);
  vec_arena_.Rebuild(vec_data_);
  SyncTrackerAmount();
}

void EmbeddingsMemoryCounter::Reset() {
  std::lock_guard lock(mutex_);
  hnsw_data_ = 0;
  vec_data_ = 0;
  hnsw_arena_.Reset();
  vec_arena_.Reset();
  SyncTrackerAmount();
}

void EmbeddingsMemoryCounter::SetOverallLimit(int64_t limit) {
  std::lock_guard lock(mutex_);
  overall_limit_ = limit;
}

int64_t EmbeddingsMemoryCounter::OverallLimit() const {
  std::lock_guard lock(mutex_);
  return overall_limit_;
}

bool EmbeddingsMemoryCounter::TryAdd(int64_t hnsw_bytes, int64_t vec_bytes) {
  std::lock_guard lock(mutex_);

  // Speculatively advance arena models to compute predicted memory
  const auto hnsw_before = hnsw_arena_;
  const auto vec_before = vec_arena_;

  hnsw_arena_.Add(hnsw_bytes);
  vec_arena_.Add(vec_bytes);

  const auto new_total = hnsw_arena_.TotalMemory() + vec_arena_.TotalMemory();
  const auto delta = new_total - last_synced_amount_;

  if (overall_limit_ > 0) {
    const auto total_after = total_memory_tracker.Amount() + delta;
    if (total_after > overall_limit_) {
      hnsw_arena_ = hnsw_before;
      vec_arena_ = vec_before;
      return false;
    }
  }

  hnsw_data_ += hnsw_bytes;
  vec_data_ += vec_bytes;
  SyncTrackerAmount();
  return true;
}

}  // namespace memgraph::utils
