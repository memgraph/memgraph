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

#include <gtest/gtest.h>

#include <atomic>
#include <chrono>
#include <cstdint>
#include <thread>

namespace memgraph::tests {

// Lets worker threads run until `counter` reaches `quota`, then clears `running` for them to see.
// Call it as the last statement of the scope holding the threads, so their destructors join.
//
// The quota is what decides how much traffic the test gets; the deadline only catches a stall, and
// fails rather than letting the test pass on a fraction of the intended coverage.
//
// A run that is merely slow still meets its quota and returns as soon as it does, so the deadline
// costs nothing when nothing is wrong and only has to sit above the slowest build. That is a debug
// build, where the allocator is itself instrumented and this kind of contended traffic runs orders
// of magnitude slower than an optimised one. A stall makes no progress at all, so a deadline this
// far above the slowest healthy run still catches it.
inline void RunUntil(std::atomic<uint64_t> const &counter, uint64_t quota, std::atomic<bool> &running,
                     std::chrono::seconds deadline = std::chrono::seconds{180}) {
  auto const start = std::chrono::steady_clock::now();
  while (counter.load(std::memory_order_relaxed) < quota) {
    if (std::chrono::steady_clock::now() - start > deadline) {
      ADD_FAILURE() << "reached only " << counter.load(std::memory_order_relaxed) << " of " << quota << " in "
                    << deadline.count() << "s";
      break;
    }
    std::this_thread::sleep_for(std::chrono::milliseconds{5});
  }
  running.store(false, std::memory_order_relaxed);
}

}  // namespace memgraph::tests
