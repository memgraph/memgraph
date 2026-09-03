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
#include <cstddef>

#include "utils/scheduler.hpp"

namespace memgraph::utils {

// A point on the steady_clock timeline, as produced by CoarseSteadyNow() (and steady_clock::now()).
// Used for query-timeout deadlines; wrap in std::optional for "no deadline".
using SteadyTimePoint = std::chrono::steady_clock::time_point;

// Process-global cache of steady_clock::now(), refreshed every ~100ms by one dedicated ticker thread.
// Hot-path readers (StoppingContext::MustAbort, ~15-20k times per large read query) pay one relaxed
// atomic load (~0.2ns) instead of steady_clock::now() (~14ns) per check.
//
// The ticker is owned by a function-local static (started on first CoarseSteadyNow() call), so it
// needs no wiring and no thread-pool reference; lazy start also means the thread spawns after main()
// has set the process signal mask. ~100ms staleness is fine for a seconds-scale timeout: the cached
// value is never ahead of steady_clock::now(), so a deadline check can only fire late, never early.
//
// relaxed on both store and load is correct and optimal: the value is a standalone scalar publishing
// no other memory, so acquire/release would only add barrier cost (real on aarch64); a 64-bit atomic
// is lock-free on x86-64 and aarch64, so the load never tears. alignas(64) isolates the reader-hot /
// writer-dirtied cache line (fixed 64 = L1 line on both arches, deliberately NOT
// std::hardware_destructive_interference_size, which is 256 here and an ABI hazard).
namespace detail {
inline constexpr std::size_t kCoarseClockCacheLine = 64;

struct alignas(kCoarseClockCacheLine) CoarseSteadyClock {
  std::atomic<std::chrono::steady_clock::rep> ticks{std::chrono::steady_clock::now().time_since_epoch().count()};

  CoarseSteadyClock() {
    ticker_.SetInterval(std::chrono::milliseconds(100));
    ticker_.Run("coarse_clk", [this]() {
      ticks.store(std::chrono::steady_clock::now().time_since_epoch().count(), std::memory_order_relaxed);
    });
  }

 private:
  Scheduler ticker_;  // refreshes `ticks`; stopped by ~Scheduler at static teardown
};

inline CoarseSteadyClock &CoarseClockInstance() noexcept {
  static CoarseSteadyClock instance;
  return instance;
}
}  // namespace detail

// Cached steady_clock time (same timeline as steady_clock::now()), at most ~100ms stale and never ahead.
inline SteadyTimePoint CoarseSteadyNow() noexcept {
  return SteadyTimePoint{
      std::chrono::steady_clock::duration{detail::CoarseClockInstance().ticks.load(std::memory_order_relaxed)}};
}

}  // namespace memgraph::utils
