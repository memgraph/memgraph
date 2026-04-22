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

#include <gtest/gtest.h>

#include <cstdlib>
#include "memory/global_memory_control.hpp"
#include "memory/query_memory_control.hpp"
#include "utils/db_aware_allocator.hpp"
#include "utils/query_memory_tracker.hpp"

TEST(MemoryTrackerTest, ExceptionEnabler) {
#if USE_JEMALLOC
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

TEST(MemoryTrackerTest, CrossThreadTrackingRestoresPreviousThreadState) {
#if USE_JEMALLOC
  memgraph::memory::SetHooks();
  memgraph::utils::QueryMemoryTracker parent_tracker;
  memgraph::utils::QueryMemoryTracker worker_tracker;

  memgraph::memory::StartTrackingCurrentThread(&parent_tracker);
  auto cross_thread_tracking = memgraph::memory::CrossThreadMemoryTracking{};
  memgraph::memory::StopTrackingCurrentThread();

  memgraph::memory::StartTrackingCurrentThread(&worker_tracker);
  cross_thread_tracking.StartTracking();

  void *parent_allocation = std::malloc(4096);
  ASSERT_NE(parent_allocation, nullptr);
  EXPECT_GT(parent_tracker.Amount(), 0);
  EXPECT_EQ(worker_tracker.Amount(), 0);
  std::free(parent_allocation);

  cross_thread_tracking.StopTracking();

  void *worker_allocation = std::malloc(4096);
  ASSERT_NE(worker_allocation, nullptr);
  EXPECT_GT(worker_tracker.Amount(), 0);
  std::free(worker_allocation);

  memgraph::memory::StopTrackingCurrentThread();
#endif
}

TEST(MemoryTrackerTest, CrossThreadTrackingRestoresPreviousDbArena) {
  constexpr unsigned kParentArena = 7;
  constexpr unsigned kWorkerArena = 11;
  constexpr unsigned kCapturedArena = 23;

  memgraph::memory::tls_db_arena_state.arena = kParentArena;
  auto cross_thread_tracking = memgraph::memory::CrossThreadMemoryTracking{kCapturedArena};

  memgraph::memory::tls_db_arena_state.arena = kWorkerArena;
  cross_thread_tracking.StartTracking();
  EXPECT_EQ(memgraph::memory::tls_db_arena_state.arena, kCapturedArena);

  cross_thread_tracking.StopTracking();
  EXPECT_EQ(memgraph::memory::tls_db_arena_state.arena, kWorkerArena);

  memgraph::memory::tls_db_arena_state.arena = 0;
}

TEST(MemoryTrackerTest, CrossThreadTrackingRestoresZeroDbArena) {
  constexpr unsigned kCapturedArena = 23;

  memgraph::memory::tls_db_arena_state.arena = 0;
  auto cross_thread_tracking = memgraph::memory::CrossThreadMemoryTracking{kCapturedArena};

  cross_thread_tracking.StartTracking();
  EXPECT_EQ(memgraph::memory::tls_db_arena_state.arena, kCapturedArena);

  cross_thread_tracking.StopTracking();
  EXPECT_EQ(memgraph::memory::tls_db_arena_state.arena, 0u);
}
