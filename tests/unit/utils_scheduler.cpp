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

#include "gtest/gtest.h"

#include <atomic>
#include <chrono>
#include "utils/scheduler.hpp"

/**
 * Scheduler runs every 2 seconds and increases one variable. Test thread
 * increases other variable. Scheduler checks if variables have the same
 * value.
 */
TEST(Scheduler, TestFunctionExecuting) {
  std::atomic<int> x{0};
  std::function<void()> func{[&x]() { ++x; }};
  memgraph::utils::Scheduler scheduler;
  scheduler.Run("Test", std::chrono::seconds(1), func);

  EXPECT_EQ(x, 0);
  std::this_thread::sleep_for(std::chrono::milliseconds(900));
  EXPECT_EQ(x, 0);

  std::this_thread::sleep_for(std::chrono::milliseconds(200));
  EXPECT_EQ(x, 1);

  std::this_thread::sleep_for(std::chrono::milliseconds(2000));
  EXPECT_EQ(x, 3);

  std::this_thread::sleep_for(std::chrono::milliseconds(100));
  scheduler.Stop();
  EXPECT_EQ(x, 3);
}

/**
 * Test scheduler's start time feature.
 */
TEST(Scheduler, StartTime) {
  std::atomic<int> x{0};
  std::function<void()> func{[&x]() { ++x; }};
  memgraph::utils::Scheduler scheduler;

  const auto now = std::chrono::system_clock::now();
  const auto timeout1 = now + std::chrono::seconds(4);
  const auto timeout2 = now + std::chrono::seconds(8);

  // start_time in the past
  scheduler.Run("Test", std::chrono::seconds(3), func, now + std::chrono::seconds(3));

  // Should execute only after start time
  while (x == 0 && std::chrono::system_clock::now() < timeout1) {
    std::this_thread::sleep_for(std::chrono::milliseconds(100));
  }
  ASSERT_EQ(x, 1);

  // Second execution after period
  while (x == 1 && std::chrono::system_clock::now() < timeout2) {
    std::this_thread::sleep_for(std::chrono::milliseconds(100));
  }
  ASSERT_EQ(x, 2);
}

/**
 * Test scheduler's start time feature.
 */
TEST(Scheduler, StartTimeRestart) {
  std::atomic<int> x{0};
  std::function<void()> func{[&x]() { ++x; }};
  memgraph::utils::Scheduler scheduler;

  const auto now = std::chrono::system_clock::now();
  const auto timeout1 = now + std::chrono::seconds(1);
  const auto timeout2 = now + std::chrono::seconds(4);

  // start_time in the past
  scheduler.Run("Test", std::chrono::seconds(6), func, now - std::chrono::seconds(3));

  // Should execute immediately and then exactly 6s from the start time
  while (x == 0 && std::chrono::system_clock::now() < timeout1) {
    std::this_thread::sleep_for(std::chrono::milliseconds(100));
  }
  ASSERT_EQ(x, 1);

  while (x == 1 && std::chrono::system_clock::now() < timeout2) {
    std::this_thread::sleep_for(std::chrono::milliseconds(100));
  }
  ASSERT_EQ(x, 2);
}

TEST(Scheduler, IsRunningFalse) {
  memgraph::utils::Scheduler scheduler;
  EXPECT_FALSE(scheduler.IsRunning());
}

TEST(Scheduler, StopIdleScheduler) {
  memgraph::utils::Scheduler scheduler;
  ASSERT_NO_THROW(scheduler.Stop());
}

TEST(Scheduler, PauseIdleScheduler) {
  memgraph::utils::Scheduler scheduler;
  ASSERT_NO_THROW(scheduler.Pause());
  ASSERT_NO_THROW(scheduler.Resume());
}

TEST(Scheduler, RunStop) {
  std::atomic<int> x{0};
  std::function<void()> func{[&x]() { ++x; }};
  memgraph::utils::Scheduler scheduler;
  scheduler.Run("Test", std::chrono::milliseconds(100), func);
  EXPECT_TRUE(scheduler.IsRunning());
  scheduler.Stop();
  EXPECT_FALSE(scheduler.IsRunning());
}

TEST(Scheduler, StopStoppedScheduler) {
  std::atomic<int> x{0};
  std::function<void()> func{[&x]() { ++x; }};
  memgraph::utils::Scheduler scheduler;
  scheduler.Run("Test", std::chrono::milliseconds(100), func);
  ASSERT_NO_THROW({
    scheduler.Stop();
    scheduler.Stop();
  });
}

TEST(Scheduler, PauseStoppedScheduler) {
  std::atomic<int> x{0};
  std::function<void()> func{[&x]() { ++x; }};
  memgraph::utils::Scheduler scheduler;
  scheduler.Run("Test", std::chrono::milliseconds(100), func);
  EXPECT_TRUE(scheduler.IsRunning());
  scheduler.Stop();
  EXPECT_FALSE(scheduler.IsRunning());
  scheduler.Pause();
  EXPECT_FALSE(scheduler.IsRunning());
}

TEST(Scheduler, StopPausedScheduler) {
  std::atomic<int> x{0};
  std::function<void()> func{[&x]() { ++x; }};
  memgraph::utils::Scheduler scheduler;
  scheduler.Run("Test", std::chrono::milliseconds(100), func);
  EXPECT_TRUE(scheduler.IsRunning());
  scheduler.Pause();
  EXPECT_TRUE(scheduler.IsRunning());  // note pausing the scheduler doesn't cause
                                       // return false for IsRunning
  scheduler.Stop();
  EXPECT_FALSE(scheduler.IsRunning());
}

TEST(Scheduler, ConcurrentStops) {
  std::atomic<int> x{0};
  std::function<void()> func{[&x]() { ++x; }};
  memgraph::utils::Scheduler scheduler;
  scheduler.Run("Test", std::chrono::milliseconds(100), func);

  std::jthread stopper1([&scheduler]() { scheduler.Stop(); });

  std::jthread stopper2([&scheduler]() { scheduler.Stop(); });
}
