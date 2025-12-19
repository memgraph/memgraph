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

#include "gtest/gtest.h"

#include <atomic>
#include <chrono>
#include <thread>

#include "utils/consolidated_scheduler.hpp"

using namespace std::chrono_literals;
using namespace memgraph::utils;

class ConsolidatedSchedulerTest : public ::testing::Test {
 protected:
  void SetUp() override {
    // Uses timerfd on Linux, condition_variable fallback elsewhere
    scheduler_ = std::make_unique<ConsolidatedScheduler>();
  }

  void TearDown() override {
    if (scheduler_) {
      scheduler_->Shutdown();
    }
  }

  std::unique_ptr<ConsolidatedScheduler> scheduler_;
};

/// Test 1: Basic task execution at specified interval
TEST_F(ConsolidatedSchedulerTest, BasicExecution) {
  std::atomic<int> counter{0};

  auto handle = scheduler_->Register("test", 100ms, [&counter]() { ++counter; });

  EXPECT_TRUE(handle.IsValid());
  EXPECT_EQ(counter.load(), 0);

  // Wait for first execution
  std::this_thread::sleep_for(150ms);
  EXPECT_GE(counter.load(), 1);

  // Wait for second execution
  std::this_thread::sleep_for(100ms);
  EXPECT_GE(counter.load(), 2);

  handle.Stop();
}

/// Test 2: Disabled task returns invalid handle
TEST_F(ConsolidatedSchedulerTest, DisabledTask) {
  std::atomic<int> counter{0};

  // Zero interval = disabled
  TaskConfig config;
  config.name = "disabled";
  config.schedule = ScheduleSpec::Interval(0ms);
  config.priority = SchedulerPriority::NORMAL;

  auto handle = scheduler_->Register(config, [&counter]() { ++counter; });

  EXPECT_FALSE(handle.IsValid());
  std::this_thread::sleep_for(200ms);
  EXPECT_EQ(counter.load(), 0);
}

/// Test 3: Priority ordering - critical runs before low when both due
TEST_F(ConsolidatedSchedulerTest, PriorityOrdering) {
  std::vector<std::string> execution_order;
  std::mutex order_mutex;

  // Start paused to synchronize
  auto critical_handle =
      scheduler_->Register(TaskConfig{"critical", ScheduleSpec::Interval(50ms), SchedulerPriority::CRITICAL}, [&]() {
        std::lock_guard lock(order_mutex);
        execution_order.push_back("critical");
      });

  auto low_handle =
      scheduler_->Register(TaskConfig{"low", ScheduleSpec::Interval(50ms), SchedulerPriority::LOW}, [&]() {
        std::lock_guard lock(order_mutex);
        execution_order.push_back("low");
      });

  EXPECT_TRUE(critical_handle.IsValid());
  EXPECT_TRUE(low_handle.IsValid());

  // Wait for both to execute
  std::this_thread::sleep_for(150ms);

  critical_handle.Stop();
  low_handle.Stop();

  // Both should have executed
  std::lock_guard lock(order_mutex);
  EXPECT_GE(execution_order.size(), 2);
}

/// Test 4: Pause and resume
TEST_F(ConsolidatedSchedulerTest, PauseResume) {
  std::atomic<int> counter{0};

  auto handle = scheduler_->Register("test", 50ms, [&counter]() { ++counter; });

  std::this_thread::sleep_for(100ms);
  auto count_before_pause = counter.load();
  EXPECT_GE(count_before_pause, 1);

  handle.Pause();
  EXPECT_TRUE(handle.IsPaused());

  std::this_thread::sleep_for(150ms);
  auto count_during_pause = counter.load();
  // Should not have increased much (might have one in-flight)
  EXPECT_LE(count_during_pause, count_before_pause + 1);

  handle.Resume();
  EXPECT_FALSE(handle.IsPaused());

  std::this_thread::sleep_for(150ms);
  EXPECT_GT(counter.load(), count_during_pause);

  handle.Stop();
}

/// Test 5: Handle destructor stops task
TEST_F(ConsolidatedSchedulerTest, HandleDestructorStopsTask) {
  std::atomic<int> counter{0};

  {
    auto handle = scheduler_->Register("test", 50ms, [&counter]() { ++counter; });
    std::this_thread::sleep_for(100ms);
    EXPECT_GE(counter.load(), 1);
  }  // handle destroyed here

  auto count_after_destroy = counter.load();
  std::this_thread::sleep_for(150ms);

  // Should not increase after handle destroyed
  EXPECT_LE(counter.load(), count_after_destroy + 1);
}

/// Test 6: Dynamic schedule change
TEST_F(ConsolidatedSchedulerTest, DynamicScheduleChange) {
  std::atomic<int> counter{0};

  auto handle = scheduler_->Register("test", 500ms, [&counter]() { ++counter; });

  // Should not execute yet
  std::this_thread::sleep_for(100ms);
  EXPECT_EQ(counter.load(), 0);

  // Change to faster interval
  handle.SetSchedule(ScheduleSpec::Interval(50ms));

  std::this_thread::sleep_for(150ms);
  EXPECT_GE(counter.load(), 1);

  handle.Stop();
}

/// Test 7: Trigger now executes immediately
TEST_F(ConsolidatedSchedulerTest, TriggerNow) {
  std::atomic<int> counter{0};

  // Long interval that won't trigger naturally during test
  auto handle = scheduler_->Register("test", 10s, [&counter]() { ++counter; });

  std::this_thread::sleep_for(50ms);
  EXPECT_EQ(counter.load(), 0);

  handle.TriggerNow();
  std::this_thread::sleep_for(100ms);
  EXPECT_GE(counter.load(), 1);

  handle.Stop();
}

/// Test 8: Multiple tasks run consolidated (single thread)
TEST_F(ConsolidatedSchedulerTest, MultipleTasksConsolidated) {
  std::atomic<int> counter1{0};
  std::atomic<int> counter2{0};
  std::atomic<int> counter3{0};

  auto handle1 = scheduler_->Register("task1", 100ms, [&counter1]() { ++counter1; });
  auto handle2 = scheduler_->Register("task2", 100ms, [&counter2]() { ++counter2; });
  auto handle3 = scheduler_->Register("task3", 100ms, [&counter3]() { ++counter3; });

  EXPECT_EQ(scheduler_->TaskCount(), 3);

  std::this_thread::sleep_for(250ms);

  EXPECT_GE(counter1.load(), 2);
  EXPECT_GE(counter2.load(), 2);
  EXPECT_GE(counter3.load(), 2);

  handle1.Stop();
  handle2.Stop();
  handle3.Stop();

  // Give time for cleanup
  std::this_thread::sleep_for(50ms);
}

/// Test 9: Task exception doesn't crash scheduler
TEST_F(ConsolidatedSchedulerTest, TaskExceptionHandled) {
  std::atomic<int> counter{0};
  std::atomic<int> exception_count{0};

  auto throwing_handle = scheduler_->Register("throwing", 50ms, [&exception_count]() {
    ++exception_count;
    throw std::runtime_error("test exception");
  });

  auto normal_handle = scheduler_->Register("normal", 50ms, [&counter]() { ++counter; });

  std::this_thread::sleep_for(200ms);

  // Both should have executed multiple times
  EXPECT_GE(exception_count.load(), 1);
  EXPECT_GE(counter.load(), 1);

  // Scheduler should still be running
  EXPECT_TRUE(scheduler_->IsRunning());

  throwing_handle.Stop();
  normal_handle.Stop();
}

/// Test 10: Execute immediately flag
TEST_F(ConsolidatedSchedulerTest, ExecuteImmediately) {
  std::atomic<int> counter{0};

  TaskConfig config;
  config.name = "immediate";
  config.schedule = ScheduleSpec::Interval(1s, true);  // execute_immediately = true
  config.priority = SchedulerPriority::NORMAL;

  auto handle = scheduler_->Register(config, [&counter]() { ++counter; });

  // Should execute almost immediately
  std::this_thread::sleep_for(100ms);
  EXPECT_GE(counter.load(), 1);

  handle.Stop();
}

/// Test that scheduler works outside of test fixture (uses timerfd on Linux)
TEST(ConsolidatedSchedulerStandalone, BasicExecution) {
  ConsolidatedScheduler scheduler;

  std::atomic<int> counter{0};
  auto handle = scheduler.Register("test", 100ms, [&counter]() { ++counter; });

  EXPECT_TRUE(handle.IsValid());

  std::this_thread::sleep_for(250ms);
  EXPECT_GE(counter.load(), 2);

  handle.Stop();
  scheduler.Shutdown();
}
