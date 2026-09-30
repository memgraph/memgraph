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
#include <string_view>

namespace memgraph::storage {

struct SnapshotProgress {
  enum class Phase : uint8_t { IDLE, EDGES, VERTICES, INDICES, CONSTRAINTS, FINALIZING };

  std::atomic<Phase> phase{Phase::IDLE};
  std::atomic<uint64_t> items_done{0};
  std::atomic<uint64_t> items_total{0};
  // system_clock us since epoch (display); 0 means "not started". steady_clock ms since epoch
  // (elapsed_ms) is only meaningful when start_time_us != 0.
  std::atomic<int64_t> start_time_us{0};
  std::atomic<int64_t> start_steady_ms{0};

  void Start() {
    start_steady_ms.store(
        std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::steady_clock::now().time_since_epoch())
            .count(),
        std::memory_order_release);
    start_time_us.store(
        std::chrono::duration_cast<std::chrono::microseconds>(std::chrono::system_clock::now().time_since_epoch())
            .count(),
        std::memory_order_release);
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
    start_steady_ms.store(0, std::memory_order_release);
  }

  static std::string_view PhaseToString(Phase phase);
};

struct SnapshotProgressView {
  SnapshotProgress::Phase phase;
  uint64_t items_done;
  uint64_t items_total;
  int64_t start_time_us;
  int64_t start_steady_ms;
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
