// Copyright 2025 Memgraph Ltd.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt; by using this file, you agree to be bound by the terms of the Business Source
// License, and you may not use this file except in compliance with the Business Source License.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

#include <gtest/gtest.h>

#include "memory/global_memory_control.hpp"
#include "memory/query_memory_control.hpp"
#include "utils/query_memory_tracker.hpp"

TEST(MemoryTrackerTest, ExceptionEnabler) {
#ifdef USE_JEMALLOC
  memgraph::memory::SetHooks();
  memgraph::utils::QueryMemoryTracker qmt;
  qmt.SetQueryLimit(memgraph::memory::UNLIMITED_MEMORY);
  memgraph::memory::StartTrackingCurrentThread(&qmt);

  for (int i = 0; i < 1e6; i++) {
    std::vector<int> vi;
    vi.reserve(1);
  }

  memgraph::memory::StopTrackingCurrentThread();

  // Nothing should happend :)
  // Previously we would deadlock
#endif
}
