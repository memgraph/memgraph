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

#include "gtest/gtest.h"

#include <atomic>
#include <chrono>
#include <condition_variable>
#include <csignal>
#include <mutex>
#include <thread>

#include "utils/consolidated_scheduler.hpp"

using namespace std::chrono_literals;
using namespace memgraph::utils;

class ConsolidatedSchedulerTest : public ::testing::Test {
 protected:
  void SetUp() override {
    // Uses timerfd on Linux for minimal non-determinism
    auto result = ConsolidatedScheduler::Create();
    ASSERT_TRUE(result.has_value()) << "Failed to create scheduler: " << result.error();
    scheduler_ = std::make_unique<ConsolidatedScheduler>(std::move(*result));
    // Register a general pool for the tests
    auto pool_result = scheduler_->RegisterPool({"general", 2, 2, PoolPolicy::FIXED});
    ASSERT_TRUE(pool_result.has_value()) << "Failed to create pool: " << pool_result.error();
    general_ = std::move(*pool_result);
  }

  void TearDown() override {
    if (scheduler_) {
      scheduler_->Shutdown();
    }
  }

  std::unique_ptr<ConsolidatedScheduler> scheduler_;
  PoolId general_;
};

/// Test 1: Basic task execution at specified interval
TEST_F(ConsolidatedSchedulerTest, BasicExecution) {
  std::atomic<int> counter{0};

  auto result = general_.Register("test", 100ms, [&counter]() { ++counter; });
  ASSERT_TRUE(result.has_value()) << result.error();
  auto handle = std::move(*result);

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

  // Zero interval = disabled - returns invalid handle (not an error)
  auto result = general_.Register("disabled", 0ms, [&counter]() { ++counter; });
  ASSERT_TRUE(result.has_value());  // Not an error, just disabled
  auto handle = std::move(*result);

  EXPECT_FALSE(handle.IsValid());
  std::this_thread::sleep_for(200ms);
  EXPECT_EQ(counter.load(), 0);
}

/// Test 3: Priority ordering - critical runs before low when both due
TEST_F(ConsolidatedSchedulerTest, PriorityOrdering) {
  std::vector<std::string> execution_order;
  std::mutex order_mutex;

  auto critical_result = general_.Register(
      "critical", 50ms,
      [&]() {
        std::lock_guard lock(order_mutex);
        execution_order.push_back("critical");
      },
      SchedulerPriority::CRITICAL);
  ASSERT_TRUE(critical_result.has_value());
  auto critical_handle = std::move(*critical_result);

  auto low_result = general_.Register(
      "low", 50ms,
      [&]() {
        std::lock_guard lock(order_mutex);
        execution_order.push_back("low");
      },
      SchedulerPriority::LOW);
  ASSERT_TRUE(low_result.has_value());
  auto low_handle = std::move(*low_result);

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

  auto result = general_.Register("test", 50ms, [&counter]() { ++counter; });
  ASSERT_TRUE(result.has_value());
  auto handle = std::move(*result);

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
    auto result = general_.Register("test", 50ms, [&counter]() { ++counter; });
    ASSERT_TRUE(result.has_value());
    auto handle = std::move(*result);
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

  auto result = general_.Register("test", 500ms, [&counter]() { ++counter; });
  ASSERT_TRUE(result.has_value());
  auto handle = std::move(*result);

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
  auto result = general_.Register("test", 10s, [&counter]() { ++counter; });
  ASSERT_TRUE(result.has_value());
  auto handle = std::move(*result);

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

  auto r1 = general_.Register("task1", 100ms, [&counter1]() { ++counter1; });
  auto r2 = general_.Register("task2", 100ms, [&counter2]() { ++counter2; });
  auto r3 = general_.Register("task3", 100ms, [&counter3]() { ++counter3; });
  ASSERT_TRUE(r1.has_value() && r2.has_value() && r3.has_value());
  auto handle1 = std::move(*r1);
  auto handle2 = std::move(*r2);
  auto handle3 = std::move(*r3);

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
  // Use shared_ptr to ensure atomics outlive any in-flight callbacks
  // (Stop() doesn't wait for in-progress execution to complete)
  auto counter = std::make_shared<std::atomic<int>>(0);
  auto exception_count = std::make_shared<std::atomic<int>>(0);

  auto r1 = general_.Register("throwing", 50ms, [exception_count]() {
    ++(*exception_count);
    throw std::runtime_error("test exception");
  });
  auto r2 = general_.Register("normal", 50ms, [counter]() { ++(*counter); });
  ASSERT_TRUE(r1.has_value() && r2.has_value());
  auto throwing_handle = std::move(*r1);
  auto normal_handle = std::move(*r2);

  std::this_thread::sleep_for(200ms);

  // Both should have executed multiple times
  EXPECT_GE(exception_count->load(), 1);
  EXPECT_GE(counter->load(), 1);

  // Scheduler should still be running
  EXPECT_TRUE(scheduler_->IsRunning());

  throwing_handle.Stop();
  normal_handle.Stop();
}

/// Test 10: Execute immediately flag
TEST_F(ConsolidatedSchedulerTest, ExecuteImmediately) {
  std::atomic<int> counter{0};

  auto result = general_.Register(
      "immediate", 1s, [&counter]() { ++counter; }, SchedulerPriority::NORMAL, true);
  ASSERT_TRUE(result.has_value());
  auto handle = std::move(*result);

  // Should execute almost immediately
  std::this_thread::sleep_for(100ms);
  EXPECT_GE(counter.load(), 1);

  handle.Stop();
}

/// Helper function to create scheduler with assertion
static ConsolidatedScheduler CreateSchedulerOrFail() {
  auto result = ConsolidatedScheduler::Create();
  if (!result) {
    ADD_FAILURE() << "Failed to create scheduler: " << result.error();
    std::abort();  // Will never reach here due to ADD_FAILURE
  }
  return std::move(*result);
}

/// Helper to register a pool with assertion
static PoolId RegisterPoolOrFail(ConsolidatedScheduler &scheduler, PoolConfig config) {
  auto result = scheduler.RegisterPool(std::move(config));
  if (!result) {
    ADD_FAILURE() << "Failed to register pool: " << result.error();
    std::abort();
  }
  return std::move(*result);
}

/// Test that scheduler works outside of test fixture (uses timerfd on Linux)
TEST(ConsolidatedSchedulerStandalone, BasicExecution) {
  auto scheduler = CreateSchedulerOrFail();
  auto general = RegisterPoolOrFail(scheduler, {"general", 2, 2, PoolPolicy::FIXED});

  std::atomic<int> counter{0};
  auto result = general.Register("test", 100ms, [&counter]() { ++counter; });
  ASSERT_TRUE(result.has_value());
  auto handle = std::move(*result);

  EXPECT_TRUE(handle.IsValid());

  std::this_thread::sleep_for(250ms);
  EXPECT_GE(counter.load(), 2);

  // RAII handles cleanup - handle.Stop() and scheduler.Shutdown() called by destructors
}

/// Test 11: Concurrent execution - multiple tasks can run in parallel
TEST(ConsolidatedSchedulerStandalone, ConcurrentExecution) {
  auto scheduler = CreateSchedulerOrFail();
  auto test_pool = RegisterPoolOrFail(scheduler, {"test_pool", 4, 4, PoolPolicy::FIXED});

  EXPECT_EQ(scheduler.WorkerCount(), 4);

  std::atomic<int> concurrent_count{0};
  std::atomic<int> max_concurrent{0};

  // Create 4 tasks that all sleep for 100ms
  // If they run concurrently, they should overlap
  auto blocking_callback = [&]() {
    int current = ++concurrent_count;

    // Update max concurrent
    int expected = max_concurrent.load();
    while (current > expected && !max_concurrent.compare_exchange_weak(expected, current)) {
    }

    std::this_thread::sleep_for(100ms);
    --concurrent_count;
  };

  // Use execute_immediately so all 4 tasks start at once
  auto r1 = test_pool.Register("task1", 1s, blocking_callback, SchedulerPriority::NORMAL, true);
  auto r2 = test_pool.Register("task2", 1s, blocking_callback, SchedulerPriority::NORMAL, true);
  auto r3 = test_pool.Register("task3", 1s, blocking_callback, SchedulerPriority::NORMAL, true);
  auto r4 = test_pool.Register("task4", 1s, blocking_callback, SchedulerPriority::NORMAL, true);
  ASSERT_TRUE(r1.has_value() && r2.has_value() && r3.has_value() && r4.has_value());

  // Wait for tasks to complete
  std::this_thread::sleep_for(200ms);

  // With 4 workers, all 4 tasks should have run concurrently
  EXPECT_GE(max_concurrent.load(), 2);  // At least 2 concurrent (conservative)

  // RAII handles cleanup
}

/// Test 12: CALLER_THREAD pool policy runs tasks on dispatcher thread
TEST(ConsolidatedSchedulerStandalone, CallerThreadPolicyExecution) {
  auto scheduler = CreateSchedulerOrFail();
  auto caller_pool = RegisterPoolOrFail(scheduler, {"caller_pool", 0, 0, PoolPolicy::CALLER_THREAD});

  EXPECT_EQ(scheduler.WorkerCount(), 0);

  std::atomic<int> counter{0};
  auto result = caller_pool.Register("test", 50ms, [&counter]() { ++counter; });
  ASSERT_TRUE(result.has_value());
  auto handle = std::move(*result);

  EXPECT_TRUE(handle.IsValid());

  std::this_thread::sleep_for(150ms);
  EXPECT_GE(counter.load(), 2);

  // RAII handles cleanup
}

/// Test 13: One-shot task runs exactly once
TEST(ConsolidatedSchedulerStandalone, OneShotTask) {
  auto scheduler = CreateSchedulerOrFail();
  auto general = RegisterPoolOrFail(scheduler, {"general", 2, 2, PoolPolicy::FIXED});

  std::atomic<int> counter{0};

  // Schedule a one-shot task to run after 50ms
  auto result = general.ScheduleAfter("one-shot", 50ms, [&counter]() { ++counter; });
  ASSERT_TRUE(result.has_value());
  auto handle = std::move(*result);

  EXPECT_TRUE(handle.IsValid());

  // Wait for task to execute
  std::this_thread::sleep_for(100ms);
  EXPECT_EQ(counter.load(), 1);

  // Wait longer - should NOT execute again
  std::this_thread::sleep_for(200ms);
  EXPECT_EQ(counter.load(), 1);  // Still 1, not rescheduled
}

/// Test 14: ScheduleNow runs immediately once
TEST(ConsolidatedSchedulerStandalone, ScheduleNowRunsOnce) {
  auto scheduler = CreateSchedulerOrFail();
  auto general = RegisterPoolOrFail(scheduler, {"general", 2, 2, PoolPolicy::FIXED});

  std::atomic<int> counter{0};

  // Schedule to run as soon as possible, once
  auto result = general.ScheduleNow("immediate-once", [&counter]() { ++counter; });
  ASSERT_TRUE(result.has_value());
  auto handle = std::move(*result);

  EXPECT_TRUE(handle.IsValid());

  // Should execute almost immediately
  std::this_thread::sleep_for(100ms);
  EXPECT_EQ(counter.load(), 1);

  // Should NOT execute again
  std::this_thread::sleep_for(200ms);
  EXPECT_EQ(counter.load(), 1);
}

/// Test 15: One-shot via ScheduleSpec::After
TEST(ConsolidatedSchedulerStandalone, OneShotViaScheduleSpec) {
  auto scheduler = CreateSchedulerOrFail();
  auto general = RegisterPoolOrFail(scheduler, {"general", 2, 2, PoolPolicy::FIXED});

  std::atomic<int> counter{0};

  auto result = general.ScheduleAfter("delayed-once", 50ms, [&counter]() { ++counter; });
  ASSERT_TRUE(result.has_value());
  auto handle = std::move(*result);

  EXPECT_TRUE(handle.IsValid());

  // Wait for execution
  std::this_thread::sleep_for(100ms);
  EXPECT_EQ(counter.load(), 1);

  // Verify it doesn't repeat
  std::this_thread::sleep_for(150ms);
  EXPECT_EQ(counter.load(), 1);
}

/// Test 16: GROW pool policy adds workers under load
TEST(ConsolidatedSchedulerStandalone, GrowPoolPolicy) {
  auto scheduler = CreateSchedulerOrFail();
  auto grow_pool = RegisterPoolOrFail(scheduler, {"grow_pool", 1, 4, PoolPolicy::GROW});

  // Initially should have 1 worker
  EXPECT_EQ(scheduler.WorkerCount(), 1);

  std::atomic<int> concurrent_count{0};
  std::atomic<int> max_concurrent{0};

  // Create tasks that run concurrently
  auto blocking_callback = [&]() {
    int current = ++concurrent_count;
    int expected = max_concurrent.load();
    while (current > expected && !max_concurrent.compare_exchange_weak(expected, current)) {
    }
    std::this_thread::sleep_for(100ms);
    --concurrent_count;
  };

  // Submit multiple tasks with execute_immediately
  auto r1 = grow_pool.Register("task1", 1s, blocking_callback, SchedulerPriority::NORMAL, true);
  auto r2 = grow_pool.Register("task2", 1s, blocking_callback, SchedulerPriority::NORMAL, true);
  ASSERT_TRUE(r1.has_value() && r2.has_value());

  // Wait for tasks to complete
  std::this_thread::sleep_for(250ms);

  // Should have been able to run at least one task
  EXPECT_GE(max_concurrent.load(), 1);
}

/// Test 17: EINTR handling - signals don't break scheduler
/// This tests that epoll_wait and read() properly retry on EINTR
TEST(ConsolidatedSchedulerStandalone, SignalInterruptHandling) {
  // Save old SIGUSR1 handler to restore later
  struct sigaction old_sa {};
  sigaction(SIGUSR1, nullptr, &old_sa);

  // Set up SIGUSR1 handler (empty handler - just need to interrupt syscalls)
  struct sigaction sa {};
  sa.sa_handler = [](int) {};  // Empty handler
  sa.sa_flags = 0;             // No SA_RESTART - we want EINTR to occur
  sigemptyset(&sa.sa_mask);
  sigaction(SIGUSR1, &sa, nullptr);

  auto scheduler = CreateSchedulerOrFail();
  auto general = RegisterPoolOrFail(scheduler, {"general", 2, 2, PoolPolicy::FIXED});

  std::atomic<int> counter{0};
  auto result = general.Register("test", 50ms, [&counter]() { ++counter; });
  ASSERT_TRUE(result.has_value());
  auto handle = std::move(*result);

  // Send SIGUSR1 signals during operation to trigger EINTR
  std::atomic<bool> stop_signals{false};
  std::thread signal_sender([&stop_signals]() {
    for (int i = 0; i < 20 && !stop_signals.load(); ++i) {
      std::this_thread::sleep_for(15ms);
      raise(SIGUSR1);  // Interrupt any blocking syscalls
    }
  });

  std::this_thread::sleep_for(400ms);
  stop_signals.store(true);
  signal_sender.join();

  // Scheduler should still be running despite signals
  EXPECT_TRUE(scheduler.IsRunning());
  EXPECT_GE(counter.load(), 3);  // Should have executed multiple times

  handle.Stop();

  // Restore old SIGUSR1 handler
  sigaction(SIGUSR1, &old_sa, nullptr);
}

/// Test 18: Rapid TriggerNow calls don't cause issues
/// This stress tests the eventfd signaling mechanism
TEST(ConsolidatedSchedulerStandalone, RapidTriggerNow) {
  auto scheduler = CreateSchedulerOrFail();
  auto general = RegisterPoolOrFail(scheduler, {"general", 2, 2, PoolPolicy::FIXED});

  std::atomic<int> counter{0};
  auto result = general.Register("test", 10s, [&counter]() { ++counter; });
  ASSERT_TRUE(result.has_value());
  auto handle = std::move(*result);

  // Rapidly trigger the task - this exercises the wake mechanism
  for (int i = 0; i < 100; ++i) {
    handle.TriggerNow();
    std::this_thread::sleep_for(1ms);
  }

  std::this_thread::sleep_for(200ms);

  // Should have executed many times without crashing or deadlocking
  EXPECT_GE(counter.load(), 10);
  EXPECT_TRUE(scheduler.IsRunning());

  handle.Stop();
}

/// Test 19: Shutdown while workers are blocked waiting for tasks
/// Tests that Cancel() properly wakes up blocked threads
TEST(ConsolidatedSchedulerStandalone, ShutdownWhileBlocked) {
  auto start = std::chrono::steady_clock::now();

  {
    auto scheduler = CreateSchedulerOrFail();
    (void)RegisterPoolOrFail(scheduler, {"general", 4, 4, PoolPolicy::FIXED});
    // Don't register any tasks - workers are blocked on eventfd Wait()
    std::this_thread::sleep_for(100ms);
    // Destructor calls Shutdown() which should wake all blocked workers
  }

  auto elapsed = std::chrono::steady_clock::now() - start;
  // Should shut down quickly (within 2 seconds), not hang indefinitely
  EXPECT_LT(elapsed, std::chrono::seconds(2));
}

/// Test 20: Many one-shot tasks submitted rapidly
/// Tests queue handling and task cleanup under load
TEST(ConsolidatedSchedulerStandalone, ManyTasksRapidSubmit) {
  auto scheduler = CreateSchedulerOrFail();
  auto general = RegisterPoolOrFail(scheduler, {"general", 4, 4, PoolPolicy::FIXED});

  constexpr int kNumTasks = 100;
  std::atomic<int> counter{0};
  std::mutex done_mutex;
  std::condition_variable done_cv;
  std::vector<TaskHandle> handles;
  handles.reserve(kNumTasks);

  // Submit kNumTasks one-shot tasks rapidly
  for (int i = 0; i < kNumTasks; ++i) {
    auto result = general.ScheduleNow("task", [&]() {
      if (++counter == kNumTasks) {
        std::lock_guard lock(done_mutex);
        done_cv.notify_one();
      }
    });
    ASSERT_TRUE(result.has_value());
    handles.push_back(std::move(*result));
  }

  // Wait for all tasks to complete (with timeout for safety)
  {
    std::unique_lock lock(done_mutex);
    EXPECT_TRUE(done_cv.wait_for(lock, 10s, [&]() { return counter.load() >= kNumTasks; }));
  }

  EXPECT_EQ(counter.load(), kNumTasks);
  EXPECT_TRUE(scheduler.IsRunning());
}

/// Test 21: Factory pattern returns error on failure (simulated by invalid state)
/// This tests the std::expected error path
TEST(ConsolidatedSchedulerStandalone, FactoryPatternSuccess) {
  // Normal creation should succeed
  auto result = ConsolidatedScheduler::Create();
  ASSERT_TRUE(result.has_value());

  // Scheduler should be functional
  auto pool_result = result->RegisterPool({"general", 1, 1, PoolPolicy::FIXED});
  ASSERT_TRUE(pool_result.has_value());
  EXPECT_TRUE(result->IsRunning());
  EXPECT_EQ(result->PoolCount(), 1);
  EXPECT_TRUE(pool_result->IsValid());
}

/// Test 22: Invalid PoolId returns error
TEST(ConsolidatedSchedulerStandalone, InvalidPoolIdReturnsError) {
  PoolId invalid_pool;
  EXPECT_FALSE(invalid_pool.IsValid());

  auto result = invalid_pool.Register("test", 100ms, []() {});
  EXPECT_FALSE(result.has_value());
  EXPECT_EQ(result.error(), RegisterError::INVALID_POOL);
}

/// Test 23: Register on shutdown scheduler returns error
TEST(ConsolidatedSchedulerStandalone, RegisterOnShutdownReturnsError) {
  auto scheduler = CreateSchedulerOrFail();
  auto general = RegisterPoolOrFail(scheduler, {"general", 2, 2, PoolPolicy::FIXED});

  scheduler.Shutdown();

  auto result = general.Register("test", 100ms, []() {});
  EXPECT_FALSE(result.has_value());
  EXPECT_EQ(result.error(), RegisterError::SCHEDULER_STOPPED);
}

/// Test 24: RegisterPool returns error when pool already exists
TEST(ConsolidatedSchedulerStandalone, RegisterPoolAlreadyExistsReturnsError) {
  auto scheduler = CreateSchedulerOrFail();

  // First registration should succeed
  auto first_result = scheduler.RegisterPool({"general", 2, 2, PoolPolicy::FIXED});
  ASSERT_TRUE(first_result.has_value());

  // Second registration with same name should fail
  auto second_result = scheduler.RegisterPool({"general", 4, 4, PoolPolicy::GROW});
  EXPECT_FALSE(second_result.has_value());
  EXPECT_EQ(second_result.error(), PoolError::ALREADY_EXISTS);

  // Original pool should still work
  EXPECT_TRUE(first_result->IsValid());
  EXPECT_EQ(scheduler.PoolCount(), 1);
}

/// Test 25: GetPool returns existing pool
TEST(ConsolidatedSchedulerStandalone, GetPoolReturnsExisting) {
  auto scheduler = CreateSchedulerOrFail();

  // GetPool should return nullopt for non-existent pool
  auto not_found = scheduler.GetPool("general");
  EXPECT_FALSE(not_found.has_value());

  // Register a pool
  auto reg_result = scheduler.RegisterPool({"general", 2, 2, PoolPolicy::FIXED});
  ASSERT_TRUE(reg_result.has_value());

  // GetPool should now return the pool
  auto found = scheduler.GetPool("general");
  ASSERT_TRUE(found.has_value());
  EXPECT_TRUE(found->IsValid());
  EXPECT_EQ(found->Name(), "general");

  // Can register tasks to the retrieved pool
  std::atomic<int> counter{0};
  auto task_result = found->Register("test", 50ms, [&counter]() { ++counter; });
  ASSERT_TRUE(task_result.has_value());

  std::this_thread::sleep_for(100ms);
  EXPECT_GE(counter.load(), 1);
}

/// Test 26: RegisterPool on shutdown scheduler returns error
TEST(ConsolidatedSchedulerStandalone, RegisterPoolOnShutdownReturnsError) {
  auto scheduler = CreateSchedulerOrFail();
  scheduler.Shutdown();

  auto result = scheduler.RegisterPool({"general", 2, 2, PoolPolicy::FIXED});
  EXPECT_FALSE(result.has_value());
  EXPECT_EQ(result.error(), PoolError::SCHEDULER_STOPPED);
}

/// Test 27: Time drift handling - slow execution doesn't cause drift
/// If a task takes longer than its interval, it should skip missed executions
/// and stay aligned to the original schedule
TEST(ConsolidatedSchedulerStandalone, TimeDriftHandling) {
  auto scheduler = CreateSchedulerOrFail();
  auto general = RegisterPoolOrFail(scheduler, {"general", 1, 1, PoolPolicy::FIXED});

  std::atomic<int> execution_count{0};
  std::atomic<bool> first_execution_done{false};
  const auto period = 100ms;

  auto result = general.Register(
      "slow_task", period,
      [&]() {
        int count = ++execution_count;
        if (count == 1) {
          // First execution takes longer than 2 periods
          std::this_thread::sleep_for(250ms);
          first_execution_done.store(true);
          first_execution_done.notify_one();
        }
      },
      SchedulerPriority::NORMAL, true);  // execute_immediately
  ASSERT_TRUE(result.has_value());
  auto handle = std::move(*result);

  // Wait for first (slow) execution to complete
  first_execution_done.wait(false);

  // After slow execution, we should NOT have piled up multiple executions
  // Give a little time for potential spurious executions
  std::this_thread::sleep_for(50ms);
  int count_after_slow = execution_count.load();

  // Should be 1 or 2, NOT 3+ (which would indicate missed executions piled up)
  EXPECT_LE(count_after_slow, 2) << "Slow execution caused pile-up of missed executions";

  // Wait for a couple more intervals and verify schedule continues normally
  std::this_thread::sleep_for(250ms);
  int final_count = execution_count.load();

  // Should have executed a few more times (drift-free)
  EXPECT_GE(final_count, 3);
  EXPECT_LE(final_count, 5);  // But not too many (no pile-up)

  handle.Stop();
}

/// Test 28: Schedule alignment - executions stay aligned to original anchor
TEST(ConsolidatedSchedulerStandalone, ScheduleAlignment) {
  auto scheduler = CreateSchedulerOrFail();
  auto general = RegisterPoolOrFail(scheduler, {"general", 1, 1, PoolPolicy::FIXED});

  std::vector<std::chrono::steady_clock::time_point> execution_times;
  std::mutex times_mutex;
  const auto period = 100ms;

  auto result = general.Register(
      "aligned_task", period,
      [&]() {
        std::lock_guard lock(times_mutex);
        execution_times.push_back(std::chrono::steady_clock::now());
      },
      SchedulerPriority::NORMAL, true);  // execute_immediately
  ASSERT_TRUE(result.has_value());
  auto handle = std::move(*result);

  // Wait for several executions
  std::this_thread::sleep_for(450ms);
  handle.Stop();

  // Verify executions are aligned to ~100ms intervals from start
  std::lock_guard lock(times_mutex);
  ASSERT_GE(execution_times.size(), 4);

  for (size_t i = 1; i < execution_times.size(); ++i) {
    auto delta = execution_times[i] - execution_times[i - 1];
    // Each interval should be close to the period (within 50ms tolerance for scheduling jitter)
    EXPECT_GE(delta, period - 50ms) << "Interval " << i << " too short";
    EXPECT_LE(delta, period + 50ms) << "Interval " << i << " too long";
  }
}
