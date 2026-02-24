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

  void IncrementDone() { items_done.fetch_add(1, std::memory_order_relaxed); }

  void Reset() {
    phase.store(Phase::IDLE, std::memory_order_release);
    items_done.store(0, std::memory_order_release);
    items_total.store(0, std::memory_order_release);
    start_time_us.store(0, std::memory_order_release);
  }

  static const char *PhaseToString(Phase phase) {
    switch (phase) {
      using enum Phase;
      case IDLE:
        return "idle";
      case EDGES:
        return "edges";
      case VERTICES:
        return "vertices";
      case INDICES:
        return "indices";
      case CONSTRAINTS:
        return "constraints";
      case FINALIZING:
        return "finalizing";
    }
    return "unknown";
  }
};

struct SnapshotProgressSnapshot {
  SnapshotProgress::Phase phase;
  uint64_t items_done;
  uint64_t items_total;
  uint64_t start_time_us;
};

}  // namespace memgraph::storage
