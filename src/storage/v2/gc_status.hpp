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
// many readers. `running` is the publish handshake: Start() fills the fields
// (relaxed) then releases `running` last, so a reader seeing running == true also
// sees coherent fields. This holds on any architecture, so the field loads stay
// relaxed; only `running` and `phase` (advanced mid-run) carry ordering.
struct GcProgress {
  std::atomic_bool running{false};
  std::atomic<GcPhase> phase{GcPhase::IDLE};
  std::atomic_bool exclusive_lock{false};
  std::atomic_bool periodic{false};
  // Epoch us / ms; start_time_us == 0 means "not started".
  std::atomic<int64_t> start_time_us{0};
  std::atomic<int64_t> start_steady_ms{0};

  bool IsRunning() const;
  void Start(bool is_periodic, bool is_exclusive);
  void SetPhase(GcPhase p);

  // Clears `running` first, then the rest, so readers never see half-reset fields.
  void Reset();

  // Coherent read: nullopt unless running. Re-checks `running` after the fields
  // so a run ending mid-read reads as not-running, never a torn row.
  std::optional<GcRunInfoView> TryGetRunInfo() const;

  static std::string_view PhaseToString(GcPhase phase);
};

}  // namespace memgraph::storage
