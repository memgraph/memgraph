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
#include <chrono>
#include <cstdint>

namespace memgraph::storage {

struct SnapshotProgress {
  enum class Phase : uint8_t { IDLE, EDGES, VERTICES, INDICES, CONSTRAINTS, FINALIZING };

  std::atomic<Phase> phase{Phase::IDLE};
  std::atomic<uint64_t> items_done{0};
  std::atomic<uint64_t> items_total{0};
  std::atomic<uint64_t> start_time_us{0};  // microseconds since epoch

  void Start() {
    auto now = std::chrono::system_clock::now().time_since_epoch();
    start_time_us.store(std::chrono::duration_cast<std::chrono::microseconds>(now).count(), std::memory_order_release);
  }

  void SetPhase(Phase p, uint64_t total) {
    items_done.store(0, std::memory_order_release);
    items_total.store(total, std::memory_order_release);
    phase.store(p, std::memory_order_release);
  }

  void AddItemsDone(uint64_t n = 1) { items_done.fetch_add(n, std::memory_order_relaxed); }

  void Reset() {
    phase.store(Phase::IDLE, std::memory_order_release);
    items_done.store(0, std::memory_order_release);
    items_total.store(0, std::memory_order_release);
    start_time_us.store(0, std::memory_order_release);
  }

  static const char *PhaseToString(Phase phase);
};

struct SnapshotProgressView {
  SnapshotProgress::Phase phase;
  uint64_t items_done;
  uint64_t items_total;
  uint64_t start_time_us;
};

/// RAII helper that batches progress increments into a local counter,
/// flushing to the atomic every `kBatchSize` items. Destructor flushes
/// any remaining count, so the final total is always accurate.
class BatchedProgressCounter {
 public:
  static constexpr uint64_t kBatchSize = 64;

  explicit BatchedProgressCounter(SnapshotProgress *progress) : progress_(progress) {}

  ~BatchedProgressCounter() { Flush(); }

  BatchedProgressCounter(const BatchedProgressCounter &) = delete;
  BatchedProgressCounter &operator=(const BatchedProgressCounter &) = delete;
  BatchedProgressCounter(BatchedProgressCounter &&) = delete;
  BatchedProgressCounter &operator=(BatchedProgressCounter &&) = delete;

  void Increment() {
    ++local_count_;
    if (local_count_ >= kBatchSize) {
      Flush();
    }
  }

  void Flush() {
    if (progress_ && local_count_ > 0) {
      progress_->AddItemsDone(local_count_);
      local_count_ = 0;
    }
  }

 private:
  SnapshotProgress *progress_;
  uint64_t local_count_{0};
};

}  // namespace memgraph::storage
