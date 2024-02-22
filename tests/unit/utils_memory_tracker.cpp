// Copyright 2024 Memgraph Ltd.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt; by using this file, you agree to be bound by the terms of the Business Source
// License, and you may not use this file except in compliance with the Business Source License.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

#include <thread>

#include <gtest/gtest.h>

#include <utils/memory_tracker.hpp>
#include <utils/on_scope_exit.hpp>

TEST(MemoryTrackerTest, ExceptionEnabler) {
  memgraph::utils::MemoryTracker memory_tracker;

  static constexpr size_t hard_limit = 10;
  memory_tracker.SetHardLimit(hard_limit);

  std::atomic<bool> can_continue{false};
  std::atomic<bool> enabler_created{false};
  std::thread t1{[&] {
    // wait until the second thread creates exception enabler
    while (!enabler_created)
      ;

    // we use the OnScopeExit so the test doesn't deadlock when
    // an ASSERT fails
    memgraph::utils::OnScopeExit thread_notifier{[&] {
      // tell the second thread it can finish its test
      can_continue = true;
    }};

    ASSERT_TRUE(memory_tracker.Alloc(hard_limit + 1));
  }};

  std::thread t2{[&] {
    memgraph::utils::MemoryTracker::OutOfMemoryExceptionEnabler exception_enabler;
    enabler_created = true;
    ASSERT_FALSE(memory_tracker.Alloc(hard_limit + 1));

    // hold the enabler until the first thread finishes
    while (!can_continue)
      ;
  }};

  t1.join();
  t2.join();
}

TEST(MemoryTrackerTest, ExceptionBlocker) {
  memgraph::utils::MemoryTracker memory_tracker;

  static constexpr size_t hard_limit = 10;
  memory_tracker.SetHardLimit(hard_limit);

  memgraph::utils::MemoryTracker::OutOfMemoryExceptionEnabler exception_enabler;
  {
    memgraph::utils::MemoryTracker::OutOfMemoryExceptionBlocker exception_blocker;

    ASSERT_TRUE(memory_tracker.Alloc(hard_limit + 1));
    ASSERT_EQ(memory_tracker.Amount(), hard_limit + 1);
  }
  ASSERT_FALSE(memory_tracker.Alloc(hard_limit + 1));
}
