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

#include <cstdint>
#include <mutex>

namespace memgraph::utils {

/// Tracks mmap-backed memory used by usearch vector indices.
///
/// Usearch allocates HNSW graph nodes and raw vector data through
/// memory_mapping_allocator_gt (a doubling-arena mmap allocator),
/// which bypasses jemalloc and total_memory_tracker. This counter
/// models each arena chain's state to predict actual RSS, and syncs
/// deltas into total_memory_tracker so the overall memory limit
/// covers both jemalloc and embedding memory.
///
/// Arena memory is never freed on individual vector removal — usearch
/// reuses freed slots on subsequent inserts. Only dropping or clearing
/// an entire index releases the arenas (via munmap). Callers must
/// reflect this: call TryAdd only for fresh inserts (not slot reuse),
/// and call Sub only on index drop/clear.
class EmbeddingsMemoryCounter final {
 public:
  EmbeddingsMemoryCounter() = default;

  EmbeddingsMemoryCounter(const EmbeddingsMemoryCounter &) = delete;
  EmbeddingsMemoryCounter &operator=(const EmbeddingsMemoryCounter &) = delete;
  EmbeddingsMemoryCounter(EmbeddingsMemoryCounter &&) = delete;
  EmbeddingsMemoryCounter &operator=(EmbeddingsMemoryCounter &&) = delete;

  /// Called on fresh vector insert (usearch allocates new arena space).
  /// Checks overall limit, advances arena models, syncs with total_memory_tracker.
  /// Do NOT call when usearch will reuse a freed slot (e.g. update of existing key).
  bool TryAdd(int64_t hnsw_bytes, int64_t vec_bytes);

  /// Called ONLY when dropping/clearing an entire index (arenas are munmap'd),
  /// or to roll back a failed TryAdd.
  /// Do NOT call on individual vector removals — arena memory is not freed.
  void Sub(int64_t hnsw_bytes, int64_t vec_bytes);

  void Reset();

  /// O(1): predicted RSS from cached arena models.
  int64_t Amount() const;

  void SetOverallLimit(int64_t limit);
  int64_t OverallLimit() const;

 private:
  /// Models one usearch memory_mapping_allocator_gt arena chain.
  /// Caches current position for O(1) per-insert advancement.
  struct ArenaModel {
    int64_t total_committed{0};  // sum of fully-used arenas (all pages resident)
    int64_t last_cap{0};         // current arena capacity (0 = no arena yet)
    int64_t last_used{0};        // bytes consumed in current arena (includes header)
    int64_t head_size{0};

    static constexpr int64_t kMinCap = int64_t{4} * 1024 * 1024;
    static constexpr int64_t kPageSize = 4096;

    explicit ArenaModel(int64_t hs) : head_size(hs) {}

    /// Advance arena chain by bytes. O(1) for normal-sized allocations.
    void Add(int64_t bytes);

    /// Predicted RSS: committed arenas + touched pages in last arena.
    int64_t TotalMemory() const;

    /// Rebuild state from total data bytes. O(log N) — only used on index drop.
    void Rebuild(int64_t total_data);

    void Reset();
  };

  void SyncTrackerAmount();

  /// HNSW tape: alignment=64, head_size = ceil(16/64)*64 = 64
  static constexpr int64_t kHnswArenaHeadSize = 64;
  /// Vector tape: alignment=8, head_size = ceil(16/8)*8 = 16
  static constexpr int64_t kVecArenaHeadSize = 16;

  mutable std::mutex mutex_;
  ArenaModel hnsw_arena_{kHnswArenaHeadSize};
  ArenaModel vec_arena_{kVecArenaHeadSize};
  int64_t hnsw_data_{0};      // total HNSW bytes ever allocated (not freed on individual removal)
  int64_t vec_data_{0};       // total vector bytes ever allocated
  int64_t overall_limit_{0};  // 0 = unlimited
  int64_t last_synced_amount_{0};
};

extern EmbeddingsMemoryCounter embeddings_memory_counter;

}  // namespace memgraph::utils
