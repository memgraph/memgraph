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

#include "utils/resource_lock.hpp"

#include <gtest/gtest.h>
#include <future>
#include <latch>
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

  std::latch latch(2);

  // Concurrently acquire read only lock
  auto ro_outcome = std::async([&] {
    auto guard_ro = SharedResourceLockGuard(lock, SharedResourceLockGuard::READ_ONLY, std::defer_lock);
    if (guard_ro.owns_lock()) return Outcome::ErrorAcquiredButShouldBeDefered;
    if (guard_ro.try_lock()) return Outcome::ErrorAcquiredButTryShouldHaveFailed;
    latch.arrive_and_wait();
    guard_ro.lock();
    return Outcome::Success;
  });

  using namespace std::chrono_literals;
  // sync on before read only lock is asked for
  latch.arrive_and_wait();
  // wait for read only to `lock()` and hence blocks waiting for no writers
  std::this_thread::sleep_for(15ms);

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

TEST_F(ResourceLockTest, ReadOnlyLockTryForWillNotifyWaitingWriterUponFailure) {
  enum class Outcome { Nothing, ErrorAcquiredButTryShouldHaveFailed, Success };
  using namespace std::chrono_literals;

  // Pin with one write lock
  auto guard_write_1 = SharedResourceLockGuard(lock, SharedResourceLockGuard::WRITE);

  // Ensure both concurrent workloads start at the same time
  auto latch1 = std::latch{2};

  // Concurrently try to acquire read only lock
  auto ro_outcome = std::async([&] {
    auto guard_ro = SharedResourceLockGuard(lock, SharedResourceLockGuard::READ_ONLY, std::defer_lock);
    latch1.arrive_and_wait();
    // expect to timeout and not get lock
    return !guard_ro.try_lock_for(2ms);
  });

  std::atomic<std::chrono::milliseconds> w2_duration{0ms};
  auto w2_outcome = std::async([&] {
    auto guard_write_2 = SharedResourceLockGuard(lock, SharedResourceLockGuard::WRITE, std::defer_lock);
    latch1.arrive_and_wait();
    // wait for read only workload to start to try and get the lock
    std::this_thread::sleep_for(1ms);
    // expect to get lock
    auto start = std::chrono::steady_clock::now();
    auto result = guard_write_2.try_lock_for(100ms);
    w2_duration = std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::steady_clock::now() - start);
    return result;
  });

  latch1.wait();

  // Ensure that the all wait times will time out
  std::this_thread::sleep_for(3ms);

  // Check we got the write lock, and did not have to wait for whole 100ms
  {
    auto writer_result = w2_outcome.get();
    ASSERT_LT(w2_duration.load(), 100ms);
    ASSERT_TRUE(writer_result);
  }

  // Check we never got the read only lock
  {
    auto reader_result = ro_outcome.get();
    ASSERT_TRUE(reader_result);
  }
}

TEST_F(ResourceLockTest, GuardUniqueIsExclusive) {
  {
    auto guard = ResourceLockGuard(lock, ResourceLockGuard::UNIQUE);
    ASSERT_TRUE(guard.owns_lock());
    ASSERT_TRUE(guard.is_exclusive());
    // UNIQUE hold blocks any other acquisition.
    ASSERT_FALSE(lock.try_lock());
    ASSERT_FALSE(lock.try_lock_shared<ResourceLock::LockReq::READ>());
  }
  // Released on scope exit in UNIQUE mode.
  ASSERT_TRUE(lock.try_lock());
  lock.unlock();
}

TEST_F(ResourceLockTest, GuardSharedIsNotExclusive) {
  auto guard = ResourceLockGuard(lock, ResourceLockGuard::READ);
  ASSERT_TRUE(guard.owns_lock());
  ASSERT_FALSE(guard.is_exclusive());
  // A shared READ hold does not block another READ.
  ASSERT_TRUE(lock.try_lock_shared<ResourceLock::LockReq::READ>());
  lock.unlock_shared<ResourceLock::LockReq::READ>();
}

TEST_F(ResourceLockTest, GuardAdoptsPreLockedUnique) {
  // Caller acquires UNIQUE directly, then hands ownership to the guard.
  lock.lock();
  {
    auto guard = ResourceLockGuard(lock, ResourceLockGuard::UNIQUE, std::adopt_lock);
    ASSERT_TRUE(guard.owns_lock());
    ASSERT_TRUE(guard.is_exclusive());
  }
  // The guard released the adopted hold; the lock is free again.
  ASSERT_TRUE(lock.try_lock());
  lock.unlock();
}

TEST_F(ResourceLockTest, GuardMoveAssignSwapsReadToWriteKeepingHold) {
  // Move-assigning a WRITE guard over a held READ guard keeps a continuous hold: WRITE is acquired
  // before the READ hold is released.
  auto guard = ResourceLockGuard(lock, ResourceLockGuard::READ);
  ASSERT_EQ(guard.type(), ResourceLockGuard::READ);

  guard = ResourceLockGuard(lock, ResourceLockGuard::WRITE);
  ASSERT_TRUE(guard.owns_lock());
  ASSERT_EQ(guard.type(), ResourceLockGuard::WRITE);

  // The shared hold was never fully dropped, so UNIQUE cannot be acquired while it is held.
  ASSERT_FALSE(lock.try_lock());

  guard.unlock();
  // Both the READ and WRITE holds are gone; the lock is fully free.
  ASSERT_TRUE(lock.try_lock());
  lock.unlock();
}

TEST_F(ResourceLockTest, GuardFromOwningUniqueLockAdopts) {
  {
    auto ul = std::unique_lock{lock};  // acquires UNIQUE
    auto guard = ResourceLockGuard{std::move(ul)};
    ASSERT_TRUE(guard.owns_lock());
    ASSERT_TRUE(guard.is_exclusive());
    ASSERT_FALSE(ul.owns_lock());  // ownership transferred, no double-unlock on ul's dtor
  }
  ASSERT_TRUE(lock.try_lock());
  lock.unlock();
}

TEST_F(ResourceLockTest, GuardFromDeferredUniqueLockIsUnlockedUnique) {
  auto ul = std::unique_lock{lock, std::defer_lock};
  auto guard = ResourceLockGuard{std::move(ul)};
  ASSERT_FALSE(guard.owns_lock());
  ASSERT_TRUE(guard.is_exclusive());  // typed UNIQUE, ready to escalate
  ASSERT_TRUE(guard.try_lock());
  ASSERT_TRUE(guard.owns_lock());
}

TEST_F(ResourceLockTest, GuardMoveConstructTransfersOwnershipNoDoubleRelease) {
  auto original = ResourceLockGuard{lock, ResourceLockGuard::WRITE};
  ASSERT_TRUE(original.owns_lock());
  {
    auto moved = std::move(original);
    ASSERT_TRUE(moved.owns_lock());
    ASSERT_EQ(moved.type(), ResourceLockGuard::WRITE);
    ASSERT_FALSE(original.owns_lock());  // source disowned by the move
  }
  // `moved` released WRITE on scope exit; the moved-from `original` must not double-release.
  ASSERT_TRUE(lock.try_lock());
  lock.unlock();
}

TEST_F(ResourceLockTest, GuardTryLockUniqueFailsWhenHeld) {
  auto held = ResourceLockGuard(lock, ResourceLockGuard::READ);

  auto guard = ResourceLockGuard(lock, ResourceLockGuard::UNIQUE, std::try_to_lock);
  ASSERT_FALSE(guard.owns_lock());

  held.unlock();
  ASSERT_TRUE(guard.try_lock());
  ASSERT_TRUE(guard.is_exclusive());
}

TEST_F(ResourceLockTest, GuardDowngradeFromUniqueIsRejected) {
  auto guard = ResourceLockGuard{lock, ResourceLockGuard::UNIQUE};
  // A UNIQUE hold cannot be downgraded (downgrade_to_read only transitions the SHARED state).
  ASSERT_FALSE(guard.downgrade_to_read());
  ASSERT_TRUE(guard.is_exclusive());  // still UNIQUE, hold intact
  ASSERT_FALSE(lock.try_lock_shared<ResourceLock::LockReq::READ>());
}

TEST_F(ResourceLockTest, GuardDowngradeWriteToReadReleasesWriteExclusivity) {
  auto guard = ResourceLockGuard{lock, ResourceLockGuard::WRITE};
  // WRITE blocks a READ_ONLY acquirer (READ_ONLY needs w_count == 0).
  ASSERT_FALSE(lock.try_lock_shared<ResourceLock::LockReq::READ_ONLY>());

  ASSERT_TRUE(guard.downgrade_to_read());
  ASSERT_EQ(guard.type(), ResourceLockGuard::READ);

  // Now a READ_ONLY acquirer can proceed.
  ASSERT_TRUE(lock.try_lock_shared<ResourceLock::LockReq::READ_ONLY>());
  lock.unlock_shared<ResourceLock::LockReq::READ_ONLY>();
}

TEST_F(ResourceLockTest, GuardDowngradeReadOnlyToReadReleasesReadOnlyExclusivity) {
  auto guard = ResourceLockGuard{lock, ResourceLockGuard::READ_ONLY};
  // READ_ONLY blocks a WRITE acquirer (WRITE needs ro_count == 0).
  ASSERT_FALSE(lock.try_lock_shared<ResourceLock::LockReq::WRITE>());

  ASSERT_TRUE(guard.downgrade_to_read());
  ASSERT_EQ(guard.type(), ResourceLockGuard::READ);

  // Now a WRITE acquirer can proceed.
  ASSERT_TRUE(lock.try_lock_shared<ResourceLock::LockReq::WRITE>());
  lock.unlock_shared<ResourceLock::LockReq::WRITE>();
}

TEST_F(ResourceLockTest, GuardDeferredThenExplicitLock) {
  auto guard = ResourceLockGuard{lock, ResourceLockGuard::WRITE, std::defer_lock};
  ASSERT_FALSE(guard.owns_lock());
  guard.lock();
  ASSERT_TRUE(guard.owns_lock());
  ASSERT_EQ(guard.type(), ResourceLockGuard::WRITE);
}

TEST_F(ResourceLockTest, GuardTryLockForUniqueAcquiresWhenFree) {
  using namespace std::chrono_literals;
  auto guard = ResourceLockGuard{lock, ResourceLockGuard::UNIQUE, std::defer_lock};
  ASSERT_TRUE(guard.try_lock_for(50ms));
  ASSERT_TRUE(guard.owns_lock());
  ASSERT_TRUE(guard.is_exclusive());
}

TEST_F(ResourceLockTest, GuardTryLockForUniqueTimesOutWhenHeld) {
  using namespace std::chrono_literals;
  auto held = ResourceLockGuard{lock, ResourceLockGuard::READ};  // a shared hold blocks UNIQUE
  auto guard = ResourceLockGuard{lock, ResourceLockGuard::UNIQUE, std::defer_lock};

  auto const start = std::chrono::steady_clock::now();
  ASSERT_FALSE(guard.try_lock_for(10ms));
  ASSERT_GE(std::chrono::steady_clock::now() - start, 10ms);
  ASSERT_FALSE(guard.owns_lock());
}
