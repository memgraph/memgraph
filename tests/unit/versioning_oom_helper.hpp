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

#include <cstddef>
#include <cstdint>

#include <gtest/gtest.h>

#include "utils/memory_tracker.hpp"

// Unit tests do not run the jemalloc hooks, so put the tracker over its limit by hand: every block exit refuses.
template <typename Create>
inline size_t CountOomRefusals(uint64_t count, Create &&create) {
  using memgraph::utils::MemoryTracker;
  using memgraph::utils::total_memory_tracker;
  constexpr int64_t kDebt = 1 << 20;
  size_t refusals = 0;
  EXPECT_TRUE(total_memory_tracker.Alloc(kDebt));
  const auto previous_limit = total_memory_tracker.HardLimit();
  {
    const MemoryTracker::OutOfMemoryExceptionEnabler enabler;
    const MemoryTracker::RefusalHandledScope refusal_handled;
    total_memory_tracker.SetHardLimit(total_memory_tracker.Amount() - 1);
    for (uint64_t gid = 0; gid < count; ++gid) {
      try {
        create(gid);
      } catch (const memgraph::utils::OutOfMemoryException &) {
        ++refusals;
      }
    }
    total_memory_tracker.SetHardLimit(previous_limit);
  }
  total_memory_tracker.Free(kDebt);
  return refusals;
}
