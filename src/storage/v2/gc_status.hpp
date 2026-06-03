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
#include <optional>

namespace memgraph::storage {

enum class GcPhase : uint8_t { IDLE, UNLINK, INDEX_CLEANUP, DELETE };

// Plain-value snapshot of a running GC for display in SHOW TRANSACTIONS.
struct GcRunInfoView {
  GcPhase phase;
  bool exclusive_lock;  // GC holds main_lock_ exclusively (blocks all transactions)
  bool periodic;        // periodic scheduler vs forced (FREE MEMORY / storage-mode switch)
  int64_t start_time_us;
  int64_t start_steady_ms;
};

// Run-state of the storage garbage collector, published for SHOW TRANSACTIONS.
//
// There is a single writer (the GC thread, which holds gc_lock_) and many
// readers. Publication is a release/acquire handshake on `running`: Start()
// writes the descriptive fields with relaxed stores and releases `running`
// last, so any reader that acquire-observes running == true is guaranteed to
// also observe coherent descriptive fields. This guarantee holds on every
// architecture by the C++ memory model — it does not rely on x86 store
// ordering. The descriptive loads can therefore stay relaxed; only `running`
// (the handshake) and `phase` (advanced mid-run) carry ordering.
struct GcProgress {
  std::atomic_bool running{false};
  std::atomic<GcPhase> phase{GcPhase::IDLE};
  std::atomic_bool exclusive_lock{false};
  std::atomic_bool periodic{false};
  // system_clock us since epoch (display); 0 means "not started". steady_clock ms
  // since epoch (for elapsed_ms) is only meaningful when start_time_us != 0.
  std::atomic<int64_t> start_time_us{0};
  std::atomic<int64_t> start_steady_ms{0};

  bool IsRunning() const { return running.load(std::memory_order_acquire); }

  // Begin a run. Descriptive fields are relaxed; `running` is released last and
  // is the single synchronization point that publishes them to readers.
  void Start(bool is_periodic, bool is_exclusive) {
    start_steady_ms.store(
        std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::steady_clock::now().time_since_epoch())
            .count(),
        std::memory_order_relaxed);
    start_time_us.store(
        std::chrono::duration_cast<std::chrono::microseconds>(std::chrono::system_clock::now().time_since_epoch())
            .count(),
        std::memory_order_relaxed);
    periodic.store(is_periodic, std::memory_order_relaxed);
    exclusive_lock.store(is_exclusive, std::memory_order_relaxed);
    phase.store(GcPhase::UNLINK, std::memory_order_release);
    running.store(true, std::memory_order_release);
  }

  // Advance the phase mid-run (release-stored so a reader re-reading sees it).
  void SetPhase(GcPhase p) { phase.store(p, std::memory_order_release); }

  // End a run. `running` is cleared first (and seen first by readers) so the
  // rest is wiped only after readers have stopped trusting the fields.
  void Reset() {
    running.store(false, std::memory_order_release);
    phase.store(GcPhase::IDLE, std::memory_order_relaxed);
    exclusive_lock.store(false, std::memory_order_relaxed);
    periodic.store(false, std::memory_order_relaxed);
    start_time_us.store(0, std::memory_order_relaxed);
    start_steady_ms.store(0, std::memory_order_relaxed);
  }

  // Coherent read for display: nullopt when no GC is running. `running` is
  // re-checked after the fields are read, so a run that ends mid-read is
  // reported as "not running" rather than as a torn / half-reset row.
  std::optional<GcRunInfoView> TryGetRunInfo() const {
    if (!running.load(std::memory_order_acquire)) return std::nullopt;
    GcRunInfoView info{.phase = phase.load(std::memory_order_acquire),
                       .exclusive_lock = exclusive_lock.load(std::memory_order_relaxed),
                       .periodic = periodic.load(std::memory_order_relaxed),
                       .start_time_us = start_time_us.load(std::memory_order_relaxed),
                       .start_steady_ms = start_steady_ms.load(std::memory_order_relaxed)};
    if (!running.load(std::memory_order_acquire)) return std::nullopt;
    return info;
  }

  static const char *PhaseToString(GcPhase phase);
};

}  // namespace memgraph::storage
