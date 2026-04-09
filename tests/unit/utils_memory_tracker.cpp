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

#include <thread>

#include <gtest/gtest.h>

#include <utils/memory_tracker.hpp>
#include <utils/on_scope_exit.hpp>

using memgraph::utils::MemoryTracker;
using memgraph::utils::OutOfMemoryException;

TEST(MemoryTrackerTest, ExceptionEnabler) {
  auto memory_tracker = MemoryTracker{};

  static constexpr auto hard_limit = 10;
  memory_tracker.SetHardLimit(hard_limit);

  auto can_continue = std::atomic<bool>{false};
  auto enabler_created = std::atomic<bool>{false};

  auto non_throwing_thread = std::jthread{[&] {
    while (!enabler_created);

    auto thread_notifier = memgraph::utils::OnScopeExit{[&] { can_continue = true; }};
    ASSERT_TRUE(memory_tracker.Alloc(hard_limit + 1));
  }};

  auto throwing_thread = std::jthread{[&] {
    auto exception_enabler = MemoryTracker::OutOfMemoryExceptionEnabler{};
    enabler_created = true;
    ASSERT_FALSE(memory_tracker.Alloc(hard_limit + 1));

    while (!can_continue);
  }};
}

TEST(MemoryTrackerTest, ExceptionBlocker) {
  auto memory_tracker = MemoryTracker{};

  static constexpr auto hard_limit = 10;
  memory_tracker.SetHardLimit(hard_limit);

  auto exception_enabler = MemoryTracker::OutOfMemoryExceptionEnabler{};
  {
    auto exception_blocker = MemoryTracker::OutOfMemoryExceptionBlocker{};

    ASSERT_TRUE(memory_tracker.Alloc(hard_limit + 1));
    ASSERT_EQ(memory_tracker.Amount(), hard_limit + 1);
  }
  ASSERT_FALSE(memory_tracker.Alloc(hard_limit + 1));
}

// Tests for parent-child memory tracker propagation.
// Per-DB domain trackers are children of db_total_memory_tracker_ which is a child of
// total_memory_tracker, so every byte tracked in a child is reflected in the parent aggregate.

TEST(EmbeddingTrackingTest, TwoChildrenTrackedSeparatelyParentAggregates) {
  auto parent = MemoryTracker{};
  auto malloc_tracker = MemoryTracker{&parent};
  auto mmap_tracker = MemoryTracker{&parent};

  ASSERT_TRUE(malloc_tracker.Alloc(100));
  ASSERT_TRUE(mmap_tracker.Alloc(200));

  EXPECT_EQ(malloc_tracker.Amount(), 100);
  EXPECT_EQ(mmap_tracker.Amount(), 200);
  EXPECT_EQ(parent.Amount(), 300);
}

TEST(EmbeddingTrackingTest, PartialFreeFromChildrenReflectedInParent) {
  auto parent = MemoryTracker{};
  auto malloc_tracker = MemoryTracker{&parent};
  auto mmap_tracker = MemoryTracker{&parent};

  ASSERT_TRUE(malloc_tracker.Alloc(100));
  ASSERT_TRUE(mmap_tracker.Alloc(200));

  malloc_tracker.Free(100);

  EXPECT_EQ(malloc_tracker.Amount(), 0);
  EXPECT_EQ(mmap_tracker.Amount(), 200);
  EXPECT_EQ(parent.Amount(), 200);
}

// When the parent (total) hard limit would be exceeded, child.Alloc returns false
// and rolls back the child's own amount so both trackers stay unchanged.
TEST(EmbeddingTrackingTest, ParentLimitBlocksChildAllocAndRollsBackChildAmount) {
  auto parent = MemoryTracker{};
  auto child = MemoryTracker{&parent};

  parent.SetHardLimit(100);
  auto enabler = MemoryTracker::OutOfMemoryExceptionEnabler{};

  ASSERT_TRUE(child.Alloc(50));
  ASSERT_FALSE(child.Alloc(100));

  EXPECT_EQ(child.Amount(), 50);
  EXPECT_EQ(parent.Amount(), 50);
}

TEST(EmbeddingTrackingTest, ChildOwnLimitBlocksAllocationIndependentlyOfParent) {
  auto parent = MemoryTracker{};
  auto child = MemoryTracker{&parent};

  child.SetHardLimit(100);
  auto enabler = MemoryTracker::OutOfMemoryExceptionEnabler{};

  ASSERT_TRUE(child.Alloc(50));
  ASSERT_FALSE(child.Alloc(100));

  EXPECT_EQ(child.Amount(), 50);
  EXPECT_EQ(parent.Amount(), 50);
}

// DoCheck propagates to the parent. Verify it throws when the parent's amount
// exceeds its hard limit and exception throwing is enabled.
TEST(EmbeddingTrackingTest, ExceedingOverallLimitThrowsOutOfMemoryException) {
  auto parent = MemoryTracker{};
  auto child = MemoryTracker{&parent};

  parent.SetHardLimit(100);

  // Bypass limit enforcement to simulate a committed allocation that pushes
  // the tracker over the limit (mirrors the blocker used in jemalloc hooks).
  {
    auto blocker = MemoryTracker::OutOfMemoryExceptionBlocker{};
    ASSERT_TRUE(child.Alloc(150));
  }
  EXPECT_EQ(parent.Amount(), 150);

  auto enabler = MemoryTracker::OutOfMemoryExceptionEnabler{};
  EXPECT_THROW(child.DoCheck(), OutOfMemoryException);
}
