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
#include <optional>
#include <string_view>

namespace memgraph::storage {

enum class GcPhase : uint8_t { IDLE, UNLINK, INDEX_CLEANUP, DELETE };

// Point-in-time copy of GC run-state, for SHOW TRANSACTIONS.
struct GcRunInfoView {
  GcPhase phase;
  bool exclusive_lock;  // GC holds main_lock_ exclusively (blocks all transactions)
  bool periodic;        // periodic scheduler vs forced (FREE MEMORY / storage-mode switch)
  int64_t start_time_us;
  int64_t start_steady_ms;
};

// GC run-state for SHOW TRANSACTIONS. One writer (the GC thread, under gc_lock_),
// many readers. Mirrors SnapshotProgress: every field is release-stored /
// acquire-loaded, so each is independently synchronized, and `running` gates the
// read (see TryGetRunInfo). `running` is cleared first on Reset so readers stop
// trusting the fields before they are wiped.
struct GcProgress {
  std::atomic_bool running{false};
  std::atomic<GcPhase> phase{GcPhase::IDLE};
  std::atomic_bool exclusive_lock{false};
  std::atomic_bool periodic{false};
  // Epoch us / ms; start_time_us == 0 means "not started".
  std::atomic<int64_t> start_time_us{0};
  std::atomic<int64_t> start_steady_ms{0};

  void Start(bool is_periodic, bool is_exclusive);
  void SetPhase(GcPhase p);

  // Clears `running` first, then the rest, so readers never see half-reset fields.
  void Reset();

  // Coherent read: reads the fields, then checks `running` last, so a run ending
  // mid-read reads as not-running, never a torn row.
  std::optional<GcRunInfoView> TryGetRunInfo() const;

  static std::string_view PhaseToString(GcPhase phase);
};

}  // namespace memgraph::storage
