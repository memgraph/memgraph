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

#include "utils/resource_lock.hpp"

#include <gtest/gtest.h>
#include <future>
#include <shared_mutex>
#include <thread>

using namespace memgraph::utils;

// Define a test fixture for the ResourceLock class
class ResourceLockTest : public testing::Test {
 protected:
  ResourceLock lock;

  void SetUp() override {
    // Setup code, if any
  }

  void TearDown() override {
    // Tear down code, if any
  }
};

TEST_F(ResourceLockTest, MultiThreadedUniqueAccess) {
  constexpr int num_threads = 10;
  int counter = 0;

  // Lambda function representing the task performed by each thread
  auto unique_task = [&]() {
    for (int i = 0; i < 100; ++i) {
      lock.lock();
      // Critical section: Increment counter safely using the lock
      ++counter;
      lock.unlock();
    }
  };

  // Create multiple threads and execute the task concurrently
  std::vector<std::thread> threads;
  for (int i = 0; i < num_threads; ++i) {
    threads.emplace_back(unique_task);
  }

  // Wait for all threads to finish
  for (auto &thread : threads) {
    thread.join();
  }

  // Assert that the counter value is as expected (total number of iterations)
  ASSERT_EQ(counter, num_threads * 100);
}

TEST_F(ResourceLockTest, MultiThreadedSharedAccess) {
  constexpr int num_threads = 10;
  int counter = 123;

  // Lambda function representing the shared task performed by each thread
  auto shared_task = [&]() {
    for (int i = 0; i < 100; ++i) {
      lock.lock_shared();
      // Read the counter value safely using shared access
      EXPECT_EQ(counter, 123);
      lock.unlock_shared();
    }
  };

  // Create multiple threads and execute the shared task concurrently
  std::vector<std::thread> threads;
  for (int i = 0; i < num_threads; ++i) {
    threads.emplace_back(shared_task);
  }

  // Wait for all threads to finish
  for (auto &thread : threads) {
    thread.join();
  }
}

TEST_F(ResourceLockTest, MultiThreadedMixedAccess) {
  constexpr int num_threads = 10;
  int counter = 0;

  // Lambda function representing the shared task performed by each thread
  auto shared_task = [&](int expecting) {
    for (int i = 0; i < 100; ++i) {
      lock.lock_shared();
      // Read the counter value safely using shared access
      EXPECT_EQ(counter, expecting);
      lock.unlock_shared();
    }
  };

  // Lambda function representing the task performed by each thread
  auto unique_task = [&]() {
    for (int i = 0; i < 100; ++i) {
      lock.lock();
      // Critical section: Increment counter safely using the lock
      ++counter;
      lock.unlock();
    }
  };

  std::vector<std::jthread> threads;

  // Unique vs shared test 1
  {
    auto l = std::unique_lock{lock};
    // Uniquely locked; spin up shared tasks and update while they are running
    for (int i = 0; i < num_threads; ++i) {
      threads.emplace_back([shared_task] { return shared_task(3456); });
    }
    // Update while still holding unique access
    std::this_thread::sleep_for(std::chrono::milliseconds(10));
    counter = 3456;
  }
  // Wait for all threads to finish
  threads.clear();

  // Unique vs shared test 2
  {
    auto l = std::shared_lock{lock};
    // Shared locked; spin up unique tasks and read while they are running
    for (int i = 0; i < num_threads; ++i) {
      threads.emplace_back(unique_task);
    }
    // Update while still holding unique access
    std::this_thread::sleep_for(std::chrono::milliseconds(10));
    EXPECT_EQ(counter, 3456);
  }
  // Wait for all threads to finish
  threads.clear();
  EXPECT_EQ(counter, 3456 + num_threads * 100);
}

TEST_F(ResourceLockTest, TryLock) {
  ASSERT_TRUE(lock.try_lock());
  ASSERT_FALSE(lock.try_lock());
  ASSERT_FALSE(lock.try_lock_shared());
  lock.unlock();
  ASSERT_TRUE(lock.try_lock_shared());
  ASSERT_TRUE(lock.try_lock_shared());
  ASSERT_FALSE(lock.try_lock());
  ASSERT_TRUE(lock.try_lock_shared());
  ASSERT_TRUE(lock.try_lock_shared());
  lock.unlock_shared();
  lock.unlock_shared();
  lock.unlock_shared();
  lock.unlock_shared();
  ASSERT_TRUE(lock.try_lock());
  lock.unlock();
}

TEST_F(ResourceLockTest, PrioritiseReadOnlyLock) {
  enum class Outcome { Nothing, ErrorAcquiredButShouldBeDefered, ErrorAcquiredButTryShouldHaveFailed, Success };

  // Pin with one write lock
  auto guard_w_1 = SharedResourceLockGuard(lock, SharedResourceLockGuard::WRITE);

  // Concurrently acquire read only lock
  auto ro_outcome = std::async([&] {
    auto guard_ro = SharedResourceLockGuard(lock, SharedResourceLockGuard::READ_ONLY, std::defer_lock);
    if (guard_ro.owns_lock()) return Outcome::ErrorAcquiredButShouldBeDefered;
    if (guard_ro.try_lock()) return Outcome::ErrorAcquiredButTryShouldHaveFailed;
    guard_ro.lock();
    return Outcome::Success;
  });

  // wait for thread to start + block on acquiring the lock
  using namespace std::chrono_literals;
  std::this_thread::sleep_for(5ms);

  // should not be able to get write lock, because ro lock is requested
  auto guard_w_2 = SharedResourceLockGuard(lock, SharedResourceLockGuard::WRITE, std::try_to_lock);
  ASSERT_FALSE(guard_w_2.owns_lock());

  // release write lock that is preventing the read only lock from being acquired
  guard_w_1.unlock();

  // wait for thread to finish, check result
  ASSERT_EQ(ro_outcome.get(), Outcome::Success);
}

TEST_F(ResourceLockTest, LockDowngrade) {
  auto guard_write_1 = SharedResourceLockGuard(lock, SharedResourceLockGuard::WRITE);
  ASSERT_TRUE(guard_write_1.owns_lock());
  guard_write_1.downgrade_to_read();

  auto guard_read_only = SharedResourceLockGuard(lock, SharedResourceLockGuard::READ_ONLY, std::defer_lock);
  ASSERT_TRUE(guard_read_only.try_lock());
  guard_read_only.downgrade_to_read();

  auto guard_write_2 = SharedResourceLockGuard(lock, SharedResourceLockGuard::WRITE, std::defer_lock);
  ASSERT_TRUE(guard_write_2.try_lock());
}