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

#include "storage/v2/gc_status.hpp"

#include <chrono>

namespace memgraph::storage {

void GcProgress::Start(bool is_periodic, bool is_exclusive) {
  start_steady_ms.store(
      std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::steady_clock::now().time_since_epoch())
          .count(),
      std::memory_order_release);
  start_time_us.store(
      std::chrono::duration_cast<std::chrono::microseconds>(std::chrono::system_clock::now().time_since_epoch())
          .count(),
      std::memory_order_release);
  periodic.store(is_periodic, std::memory_order_release);
  exclusive_lock.store(is_exclusive, std::memory_order_release);
  phase.store(GcPhase::UNLINK, std::memory_order_release);
  running.store(true, std::memory_order_release);
}

void GcProgress::SetPhase(GcPhase p) { phase.store(p, std::memory_order_release); }

void GcProgress::Reset() {
  // Clear `running` first so readers stop trusting the fields before they are wiped.
  running.store(false, std::memory_order_release);
  phase.store(GcPhase::IDLE, std::memory_order_release);
  exclusive_lock.store(false, std::memory_order_release);
  periodic.store(false, std::memory_order_release);
  start_time_us.store(0, std::memory_order_release);
  start_steady_ms.store(0, std::memory_order_release);
}

std::optional<GcRunInfoView> GcProgress::TryGetRunInfo() const {
  GcRunInfoView info{.phase = phase.load(std::memory_order_acquire),
                     .exclusive_lock = exclusive_lock.load(std::memory_order_acquire),
                     .periodic = periodic.load(std::memory_order_acquire),
                     .start_time_us = start_time_us.load(std::memory_order_acquire),
                     .start_steady_ms = start_steady_ms.load(std::memory_order_acquire)};
  // Check `running` after reading the fields so a run ending mid-read reads as
  // not-running, never a torn row.
  if (!running.load(std::memory_order_acquire)) return std::nullopt;
  return info;
}

std::string_view GcProgress::PhaseToString(GcPhase phase) {
  using namespace std::string_view_literals;
  switch (phase) {
    using enum GcPhase;
    case IDLE:
      return "idle"sv;
    case UNLINK:
      return "unlink"sv;
    case INDEX_CLEANUP:
      return "index_cleanup"sv;
    case DELETE:
      return "delete"sv;
  }
  return "unknown"sv;
}

}  // namespace memgraph::storage
