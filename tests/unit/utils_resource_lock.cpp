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
#include <atomic>
#include <chrono>
#include <future>
#include <latch>
#include <mutex>
#include <optional>
#include <random>
#include <shared_mutex>
#include <string>
#include <thread>
#include <vector>

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

// Stress tests: a varied TSan surface plus liveness probes (UNIQUE starvation vs shared holders,
// READ_ONLY latency vs new writers).

// Mutual-exclusion fuzzer. Threads hammer every entry point while atomic counters are checked
// against the lock's invariants on each acquisition (catches violations + TSan races).
TEST_F(ResourceLockTest, ConcurrentMutualExclusionInvariantsFuzz) {
  using namespace std::chrono_literals;
  constexpr int kNumThreads = 24;
  constexpr auto kDuration = 400ms;

  struct Counters {
    std::atomic<int> active_read{0};
    std::atomic<int> active_write{0};
    std::atomic<int> active_ro{0};
    std::atomic<int> active_unique{0};
  } counters;

  std::atomic<bool> invariant_violated{false};
  std::mutex violation_mutex;
  std::string violation_message;

  auto record_violation = [&](const std::string &msg) {
    bool expected = false;
    if (invariant_violated.compare_exchange_strong(expected, true)) {
      std::lock_guard<std::mutex> lg(violation_mutex);
      violation_message = msg;
    }
  };

  // Invariants (per the lock's contract):
  //  1. WRITE and READ_ONLY are never simultaneously active.
  //  2. UNIQUE active implies nothing else is active (and vice versa).
  //  3. At most one UNIQUE holder at a time.
  auto check_invariants = [&](const char *where) {
    const int aw = counters.active_write.load(std::memory_order_acquire);
    const int aro = counters.active_ro.load(std::memory_order_acquire);
    const int ar = counters.active_read.load(std::memory_order_acquire);
    const int au = counters.active_unique.load(std::memory_order_acquire);
    if (aw > 0 && aro > 0) {
      record_violation(std::string(where) + ": WRITE(" + std::to_string(aw) + ") and READ_ONLY(" + std::to_string(aro) +
                       ") concurrently active");
    }
    if (au > 0 && (aw > 0 || ar > 0 || aro > 0)) {
      record_violation(std::string(where) + ": UNIQUE(" + std::to_string(au) + ") active alongside shared holders (w=" +
                       std::to_string(aw) + ", r=" + std::to_string(ar) + ", ro=" + std::to_string(aro) + ")");
    }
    if (au > 1) {
      record_violation(std::string(where) + ": more than one UNIQUE holder concurrently (" + std::to_string(au) + ")");
    }
  };

  std::atomic<bool> stop{false};

  auto worker = [&](int seed) {
    std::mt19937 rng(static_cast<unsigned>(seed) * 2654435761u ^
                     static_cast<unsigned>(std::hash<std::thread::id>{}(std::this_thread::get_id())));
    std::uniform_int_distribution<int> op_pick(0, 5);
    std::uniform_int_distribution<int> hold_us(0, 50);
    std::uniform_int_distribution<int> downgrade_roll(0, 3);

    while (!stop.load(std::memory_order_relaxed) && !invariant_violated.load(std::memory_order_relaxed)) {
      switch (op_pick(rng)) {
        case 0: {  // READ: never conflicts with anything but UNIQUE
          lock.lock_shared<ResourceLock::LockReq::READ>();
          counters.active_read.fetch_add(1, std::memory_order_acq_rel);
          check_invariants("READ");
          std::this_thread::sleep_for(std::chrono::microseconds(hold_us(rng)));
          counters.active_read.fetch_sub(1, std::memory_order_acq_rel);
          lock.unlock_shared<ResourceLock::LockReq::READ>();
          break;
        }
        case 1: {  // WRITE (shared writer), conflicts with READ_ONLY
          lock.lock_shared<ResourceLock::LockReq::WRITE>();
          counters.active_write.fetch_add(1, std::memory_order_acq_rel);
          check_invariants("WRITE");
          std::this_thread::sleep_for(std::chrono::microseconds(hold_us(rng)));
          if (downgrade_roll(rng) == 0) {
            counters.active_write.fetch_sub(1, std::memory_order_acq_rel);
            ASSERT_TRUE(lock.downgrade_to_read<ResourceLock::LockReq::WRITE>());
            counters.active_read.fetch_add(1, std::memory_order_acq_rel);
            check_invariants("WRITE->READ downgrade");
            counters.active_read.fetch_sub(1, std::memory_order_acq_rel);
            lock.unlock_shared<ResourceLock::LockReq::READ>();
          } else {
            counters.active_write.fetch_sub(1, std::memory_order_acq_rel);
            lock.unlock_shared<ResourceLock::LockReq::WRITE>();
          }
          break;
        }
        case 2: {  // READ_ONLY, blocking, conflicts with WRITE
          lock.lock_shared<ResourceLock::LockReq::READ_ONLY>();
          counters.active_ro.fetch_add(1, std::memory_order_acq_rel);
          check_invariants("READ_ONLY(block)");
          std::this_thread::sleep_for(std::chrono::microseconds(hold_us(rng)));
          if (downgrade_roll(rng) == 0) {
            counters.active_ro.fetch_sub(1, std::memory_order_acq_rel);
            ASSERT_TRUE(lock.downgrade_to_read<ResourceLock::LockReq::READ_ONLY>());
            counters.active_read.fetch_add(1, std::memory_order_acq_rel);
            check_invariants("READ_ONLY->READ downgrade");
            counters.active_read.fetch_sub(1, std::memory_order_acq_rel);
            lock.unlock_shared<ResourceLock::LockReq::READ>();
          } else {
            counters.active_ro.fetch_sub(1, std::memory_order_acq_rel);
            lock.unlock_shared<ResourceLock::LockReq::READ_ONLY>();
          }
          break;
        }
        case 3: {  // READ_ONLY, timed try with a short timeout (frequently expected to fail)
          if (lock.try_lock_shared_for<std::chrono::microseconds::rep, std::micro, ResourceLock::LockReq::READ_ONLY>(
                  std::chrono::microseconds(200))) {
            counters.active_ro.fetch_add(1, std::memory_order_acq_rel);
            check_invariants("READ_ONLY(try_for)");
            std::this_thread::sleep_for(std::chrono::microseconds(hold_us(rng)));
            counters.active_ro.fetch_sub(1, std::memory_order_acq_rel);
            lock.unlock_shared<ResourceLock::LockReq::READ_ONLY>();
          }
          break;
        }
        case 4: {  // UNIQUE via try_lock
          if (lock.try_lock()) {
            counters.active_unique.fetch_add(1, std::memory_order_acq_rel);
            check_invariants("UNIQUE(try_lock)");
            std::this_thread::sleep_for(std::chrono::microseconds(hold_us(rng)));
            counters.active_unique.fetch_sub(1, std::memory_order_acq_rel);
            lock.unlock();
          }
          break;
        }
        case 5: {  // UNIQUE via try_lock_for
          if (lock.try_lock_for(std::chrono::microseconds(200))) {
            counters.active_unique.fetch_add(1, std::memory_order_acq_rel);
            check_invariants("UNIQUE(try_lock_for)");
            std::this_thread::sleep_for(std::chrono::microseconds(hold_us(rng)));
            counters.active_unique.fetch_sub(1, std::memory_order_acq_rel);
            lock.unlock();
          }
          break;
        }
      }
    }
  };

  std::vector<std::jthread> threads;
  threads.reserve(kNumThreads);
  for (int i = 0; i < kNumThreads; ++i) {
    threads.emplace_back(worker, i);
  }

  std::this_thread::sleep_for(kDuration);
  stop.store(true, std::memory_order_relaxed);
  threads.clear();  // joins all workers

  ASSERT_FALSE(invariant_violated.load()) << violation_message;
}

// UNIQUE-starvation probe under continuous READ churn. With writer-preference, unique_pending_
// gates new READ holders so lock() acquires within the watchdog bound; a hung acquire becomes a
// soft, logged failure instead of a hung binary.
TEST_F(ResourceLockTest, UniqueStarvationUnderContinuousReadHammer) {
  using namespace std::chrono_literals;
  constexpr int kNumHammers = 8;
  constexpr auto kWatchdogBound = 5s;

  std::atomic<bool> stop{false};
  std::vector<std::jthread> hammers;
  hammers.reserve(kNumHammers);
  for (int i = 0; i < kNumHammers; ++i) {
    hammers.emplace_back([&, i] {
      // Stagger starts so hammers don't all release in lockstep, keeping overlap continuous.
      std::this_thread::sleep_for(std::chrono::microseconds(i * 13));
      while (!stop.load(std::memory_order_relaxed)) {
        lock.lock_shared<ResourceLock::LockReq::READ>();
        std::this_thread::sleep_for(100us);
        lock.unlock_shared<ResourceLock::LockReq::READ>();
      }
    });
  }

  auto unique_future = std::async(std::launch::async, [&] {
    auto start = std::chrono::steady_clock::now();
    lock.lock();
    auto acquired = std::chrono::steady_clock::now();
    lock.unlock();
    return std::chrono::duration_cast<std::chrono::milliseconds>(acquired - start);
  });

  const bool acquired_in_time = unique_future.wait_for(kWatchdogBound) == std::future_status::ready;

  // Stop the hammers regardless of outcome so the async UNIQUE task (and this test) can finish.
  stop.store(true, std::memory_order_relaxed);
  hammers.clear();

  if (acquired_in_time) {
    const auto latency = unique_future.get();
    RecordProperty("unique_acquire_latency_ms", std::to_string(latency.count()));
  } else {
    ADD_FAILURE() << "UNIQUE lock() did not acquire within " << kWatchdogBound.count()
                  << "s under continuous READ churn from " << kNumHammers
                  << " hammer threads. lock() registers as pending and gates new shared acquirers, so this is a "
                     "regression of that gate or of the wake-up that follows a shared release, not an accepted "
                     "absence of anti-starvation.";
    unique_future.get();  // reap the async thread now that hammers have stopped
  }
}

// The same starvation probe, with WRITE the churning shared mode.
TEST_F(ResourceLockTest, UniqueStarvationUnderContinuousWriteHammer) {
  using namespace std::chrono_literals;
  constexpr int kNumHammers = 8;
  constexpr auto kWatchdogBound = 5s;

  std::atomic<bool> stop{false};
  std::vector<std::jthread> hammers;
  hammers.reserve(kNumHammers);
  for (int i = 0; i < kNumHammers; ++i) {
    hammers.emplace_back([&, i] {
      std::this_thread::sleep_for(std::chrono::microseconds(i * 13));
      while (!stop.load(std::memory_order_relaxed)) {
        lock.lock_shared<ResourceLock::LockReq::WRITE>();
        std::this_thread::sleep_for(100us);
        lock.unlock_shared<ResourceLock::LockReq::WRITE>();
      }
    });
  }

  auto unique_future = std::async(std::launch::async, [&] {
    auto start = std::chrono::steady_clock::now();
    lock.lock();
    auto acquired = std::chrono::steady_clock::now();
    lock.unlock();
    return std::chrono::duration_cast<std::chrono::milliseconds>(acquired - start);
  });

  const bool acquired_in_time = unique_future.wait_for(kWatchdogBound) == std::future_status::ready;

  stop.store(true, std::memory_order_relaxed);
  hammers.clear();

  if (acquired_in_time) {
    const auto latency = unique_future.get();
    RecordProperty("unique_acquire_latency_ms", std::to_string(latency.count()));
  } else {
    ADD_FAILURE() << "UNIQUE lock() did not acquire within " << kWatchdogBound.count()
                  << "s under continuous WRITE churn from " << kNumHammers
                  << " hammer threads. lock() registers as pending and gates new shared acquirers, so this is a "
                     "regression of that gate or of the wake-up that follows a shared release, not an accepted "
                     "absence of anti-starvation.";
    unique_future.get();
  }
}

// READ_ONLY vs WRITE priority. A mid-stream READ_ONLY registers ro_pending_count, gating
// new WRITEs, so its latency is bounded by draining in-flight writers, not by the queued burst.
// Measured with a small and a large late-writer burst; latency must not grow with burst size.
TEST_F(ResourceLockTest, ReadOnlyLatencyIndependentOfNewWriterCount) {
  using namespace std::chrono_literals;

  auto measure = [&](int num_inflight_writers, int num_late_writers) {
    std::atomic<bool> stop_inflight{false};
    std::vector<std::jthread> inflight;
    inflight.reserve(num_inflight_writers);
    for (int i = 0; i < num_inflight_writers; ++i) {
      inflight.emplace_back([&] {
        while (!stop_inflight.load(std::memory_order_relaxed)) {
          lock.lock_shared<ResourceLock::LockReq::WRITE>();
          std::this_thread::sleep_for(200us);
          lock.unlock_shared<ResourceLock::LockReq::WRITE>();
        }
      });
    }
    // Give the in-flight writers time to actually be holding the lock before RO registers.
    std::this_thread::sleep_for(2ms);

    std::atomic<bool> ro_registered{false};
    std::atomic<bool> ro_done{false};
    std::chrono::steady_clock::time_point t_start;
    std::chrono::steady_clock::time_point t_acquired;

    std::jthread ro_thread([&] {
      t_start = std::chrono::steady_clock::now();
      ro_registered.store(true, std::memory_order_release);
      lock.lock_shared<ResourceLock::LockReq::READ_ONLY>();
      t_acquired = std::chrono::steady_clock::now();
      ro_done.store(true, std::memory_order_release);
      std::this_thread::sleep_for(1ms);
      lock.unlock_shared<ResourceLock::LockReq::READ_ONLY>();
    });

    while (!ro_registered.load(std::memory_order_acquire)) {
      std::this_thread::yield();
    }
    // Small buffer so `ro_pending_count` is very likely already incremented before the burst.
    std::this_thread::sleep_for(300us);

    std::atomic<int> late_before_ro{0};
    std::atomic<int> late_after_ro{0};
    std::vector<std::jthread> late;
    late.reserve(num_late_writers);
    for (int i = 0; i < num_late_writers; ++i) {
      late.emplace_back([&] {
        lock.lock_shared<ResourceLock::LockReq::WRITE>();
        const bool already_done = ro_done.load(std::memory_order_acquire);
        std::this_thread::sleep_for(30us);
        lock.unlock_shared<ResourceLock::LockReq::WRITE>();
        if (already_done) {
          late_after_ro.fetch_add(1, std::memory_order_relaxed);
        } else {
          late_before_ro.fetch_add(1, std::memory_order_relaxed);
        }
      });
    }

    ro_thread.join();
    late.clear();
    stop_inflight.store(true, std::memory_order_relaxed);
    inflight.clear();

    EXPECT_EQ(late_before_ro.load(), 0) << "new WRITE attempts registered after READ_ONLY was pending managed to "
                                           "acquire before it: ro_pending_count priority not honoured";

    return std::chrono::duration_cast<std::chrono::microseconds>(t_acquired - t_start);
  };

  const auto latency_small_burst = measure(/*num_inflight_writers=*/2, /*num_late_writers=*/5);
  const auto latency_large_burst = measure(/*num_inflight_writers=*/2, /*num_late_writers=*/60);

  RecordProperty("ro_latency_small_burst_us", std::to_string(latency_small_burst.count()));
  RecordProperty("ro_latency_large_burst_us", std::to_string(latency_large_burst.count()));

  // The large burst queues far more new writers behind the READ_ONLY request; if priority is
  // honoured, that should barely move the READ_ONLY latency (bounded by in-flight-writer drain
  // time), not scale with the queue length.
  EXPECT_LT(latency_large_burst, latency_small_burst + 50ms)
      << "READ_ONLY latency grew with the number of new writers queued behind it (small burst="
      << latency_small_burst.count() << "us, large burst=" << latency_large_burst.count()
      << "us): ro_pending_count priority/starvation regression";
}

// UniquePendingScope / ReadOnlyPendingScope tests: a pending-scope retry loop gets writer-
// preference (acquires under continuous shared churn where a bare try_lock() loop starves), plus
// deterministic gating and mutual-exclusion invariants.

namespace {
// Spins up kNumHammers threads that continuously take/release a shared lock of type Req, staggered
// to keep shared occupancy continuous. Caller flips `stop` and lets the vector join.
template <ResourceLock::LockReq Req>
std::vector<std::jthread> SpawnHammers(ResourceLock &target_lock, std::atomic<bool> &stop, int kNumHammers) {
  std::vector<std::jthread> hammers;
  hammers.reserve(kNumHammers);
  for (int i = 0; i < kNumHammers; ++i) {
    hammers.emplace_back([&target_lock, &stop, i] {
      std::this_thread::sleep_for(std::chrono::microseconds(i * 13));
      while (!stop.load(std::memory_order_relaxed)) {
        target_lock.lock_shared<Req>();
        std::this_thread::sleep_for(std::chrono::microseconds(100));
        target_lock.unlock_shared<Req>();
      }
    });
  }
  return hammers;
}
}  // namespace

// A UniquePendingScope held across a try_acquire() retry loop should acquire within a
// bounded time under a continuous stream of new WRITE shared acquirers. A bare `lock.try_lock()`
// retry loop against the same hammer stream is expected to starve, since its probes never register
// as pending and so never gate the hammers out.
//
// Only the campaign's acquisition is asserted; the control is recorded, not asserted. The hammers
// overlap statistically rather than by construction, so "the bare loop did not get in" asserts that
// a rare event did not occur across many probes, which no choice of bound makes reliable: a longer
// bound makes it likelier to fire, a shorter one weakens it to nothing. The gate's mechanism has
// its own deterministic test, holding a shared lock and checking that new shared acquisitions fail
// while a pending scope lives and succeed once it dies.
TEST_F(ResourceLockTest, UniquePendingScopeCampaignAcquiresUnderContinuousWriteHammer) {
  using namespace std::chrono_literals;
  constexpr int kNumHammers = 8;
  constexpr auto kPendingScopeBound = 5s;
  constexpr auto kControlBound = 300ms;

  // --- Scenario 1: UniquePendingScope campaign against continuous WRITE hammers ---
  {
    std::atomic<bool> stop{false};
    auto hammers = SpawnHammers<ResourceLock::LockReq::WRITE>(lock, stop, kNumHammers);

    auto scoped_future = std::async(std::launch::async, [&] {
      auto start = std::chrono::steady_clock::now();
      UniquePendingScope scope(lock);
      std::optional<ResourceLockGuard> acquired;
      while (!acquired) {
        acquired = scope.try_acquire();
        if (!acquired) std::this_thread::sleep_for(20us);
      }
      auto latency = std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::steady_clock::now() - start);
      return latency;  // `acquired` unlocks on scope exit here
    });

    const bool acquired_in_time = scoped_future.wait_for(kPendingScopeBound) == std::future_status::ready;
    stop.store(true, std::memory_order_relaxed);
    hammers.clear();

    if (acquired_in_time) {
      const auto latency = scoped_future.get();
      RecordProperty("unique_pending_scope_latency_ms", std::to_string(latency.count()));
    } else {
      ADD_FAILURE() << "UniquePendingScope::try_acquire() campaign did not acquire within "
                    << kPendingScopeBound.count() << "s under continuous WRITE churn from " << kNumHammers
                    << " hammer threads: pending-scope writer-preference regression";
      scoped_future.get();  // reap the async thread now that hammers have stopped
    }
  }

  // --- Scenario 2 (control): bare try_lock() retry loop against the same kind of hammer stream ---
  {
    std::atomic<bool> stop{false};
    auto hammers = SpawnHammers<ResourceLock::LockReq::WRITE>(lock, stop, kNumHammers);

    auto control_future = std::async(std::launch::async, [&] {
      auto start = std::chrono::steady_clock::now();
      while (std::chrono::steady_clock::now() - start < kControlBound) {
        if (lock.try_lock()) {
          return std::optional{
              std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::steady_clock::now() - start)};
        }
        std::this_thread::sleep_for(20us);
      }
      return std::optional<std::chrono::milliseconds>{};
    });

    auto control_result = control_future.get();
    stop.store(true, std::memory_order_relaxed);
    hammers.clear();

    if (control_result) {
      lock.unlock();  // it did acquire; release so later tests in the suite start from UNLOCKED
      RecordProperty("bare_try_lock_acquired_ms", std::to_string(control_result->count()));
    } else {
      RecordProperty("bare_try_lock_acquired_ms", "starved");
    }
  }
}

// The same campaign for ReadOnlyPendingScope, against continuous WRITE hammers.
TEST_F(ResourceLockTest, ReadOnlyPendingScopeCampaignAcquiresUnderContinuousWriteHammer) {
  using namespace std::chrono_literals;
  constexpr int kNumHammers = 8;
  constexpr auto kBound = 5s;

  std::atomic<bool> stop{false};
  auto hammers = SpawnHammers<ResourceLock::LockReq::WRITE>(lock, stop, kNumHammers);

  auto scoped_future = std::async(std::launch::async, [&] {
    auto start = std::chrono::steady_clock::now();
    ReadOnlyPendingScope scope(lock);
    std::optional<ResourceLockGuard> acquired;
    while (!acquired) {
      acquired = scope.try_acquire();
      if (!acquired) std::this_thread::sleep_for(20us);
    }
    auto latency = std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::steady_clock::now() - start);
    const auto acquired_type = acquired->type();
    return std::pair{latency, acquired_type};  // guard unlocks on scope exit here
  });

  const bool acquired_in_time = scoped_future.wait_for(kBound) == std::future_status::ready;
  stop.store(true, std::memory_order_relaxed);
  hammers.clear();

  if (acquired_in_time) {
    const auto [latency, acquired_type] = scoped_future.get();
    EXPECT_EQ(acquired_type, ResourceLockGuard::READ_ONLY);
    RecordProperty("ro_pending_scope_latency_ms", std::to_string(latency.count()));
  } else {
    ADD_FAILURE() << "ReadOnlyPendingScope::try_acquire() campaign did not acquire within " << kBound.count()
                  << "s under continuous WRITE churn from " << kNumHammers
                  << " hammer threads: pending-scope priority regression";
    scoped_future.get();  // reap the async thread now that hammers have stopped
  }
}

// A live, not-yet-successful UniquePendingScope gates new shared acquisitions of every kind, and
// the gate clears the moment it is destroyed unacquired.
TEST_F(ResourceLockTest, UniquePendingScopeGatesNewSharedAcquisitionUntilDestroyed) {
  // Hold WRITE so the scope's try_acquire() can't succeed (state != UNLOCKED), keeping it pending.
  auto guard_w0 = SharedResourceLockGuard(lock, SharedResourceLockGuard::WRITE);

  {
    UniquePendingScope scope(lock);
    ASSERT_FALSE(scope.try_acquire().has_value());

    EXPECT_FALSE(lock.try_lock_shared<ResourceLock::LockReq::WRITE>());
    EXPECT_FALSE(lock.try_lock_shared<ResourceLock::LockReq::READ>());
    EXPECT_FALSE(lock.try_lock_shared<ResourceLock::LockReq::READ_ONLY>());
  }
  // scope destroyed unacquired -> unique_pending_ == 0, WRITE-compatible acquisitions succeed again.
  ASSERT_TRUE(lock.try_lock_shared<ResourceLock::LockReq::WRITE>());
  lock.unlock_shared<ResourceLock::LockReq::WRITE>();

  guard_w0.unlock();
}

// Destroying a still-pending UniquePendingScope must wake a thread genuinely blocked in
// lock_shared(), not merely let the next acquirer through: the parked thread has no other event
// coming, since deregistering is not a state change anyone else observes. A dropped notify, or a
// counter mutated off mtx, hangs the reader and fails via the bounded wait_for. The window in which
// an off-mtx notify races the waiter's enqueue is not deterministically reproducible; that rests on
// the mtx discipline and the fuzz test.
TEST_F(ResourceLockTest, UniquePendingScopeDestructionWakesBlockedSharedReader) {
  using namespace std::chrono_literals;
  // Hold WRITE so state == SHARED (never UNLOCKED): the scope stays pending, and a READ acquirer is
  // compatible with the held WRITE once the unique_pending_ gate clears.
  auto guard_w0 = SharedResourceLockGuard(lock, SharedResourceLockGuard::WRITE);

  auto scope = std::optional<UniquePendingScope>{std::in_place, lock};  // unique_pending_ == 1
  ASSERT_FALSE(scope->try_acquire().has_value());

  std::atomic<bool> reader_acquired{false};
  auto reader = std::async(std::launch::async, [&] {
    // Parks in cv.wait (unique_pending_ == 1) until the scope below is destroyed.
    lock.lock_shared<ResourceLock::LockReq::READ>();
    reader_acquired.store(true, std::memory_order_release);
    lock.unlock_shared<ResourceLock::LockReq::READ>();
  });

  // Let the reader reach cv.wait, then confirm the gate (not a scheduling delay) is holding it.
  std::this_thread::sleep_for(50ms);
  ASSERT_FALSE(reader_acquired.load(std::memory_order_acquire));

  scope.reset();  // ~UniquePendingScope: unique_pending_ 1 -> 0 under mtx, then notify_all -> wake

  // Bounded so a lost/absent wake fails the test instead of hanging the suite forever.
  const auto status = reader.wait_for(5s);
  EXPECT_EQ(status, std::future_status::ready)
      << "reader blocked in lock_shared() was not woken by ~UniquePendingScope (lost/absent wakeup)";
  if (status != std::future_status::ready) {
    // Failure path only: release WRITE to notify_all and rescue the parked reader so ~future does
    // not block forever. Doing this earlier would mask the scope's own notify under test.
    guard_w0.unlock();
    reader.wait();
    return;
  }
  reader.get();
  EXPECT_TRUE(reader_acquired.load(std::memory_order_acquire));

  guard_w0.unlock();
}

// A READ release frees the lock for a pending UNIQUE, and the UNIQUE's own release then admits a
// READ acquirer that was gated behind it. A held READ parks both: the UNIQUE on state == SHARED,
// the second READ on the pending-UNIQUE gate, so releasing the READ must set the whole chain going.
//
// Liveness smoke test rather than a tripwire for the breadth of the notify: on glibc the futex wait
// queue is effectively FIFO, so waking a single waiter would usually pick the right one anyway and
// mask a too-narrow notify. What it does catch is a release that stops notifying at all.
TEST_F(ResourceLockTest, ReadReleaseWakesBothPendingUniqueAndGatedReader) {
  using namespace std::chrono_literals;

  auto held_read = SharedResourceLockGuard(lock, SharedResourceLockGuard::READ);  // state == SHARED

  std::atomic<bool> unique_acquired{false};
  std::atomic<bool> reader_acquired{false};

  auto unique_fut = std::async(std::launch::async, [&] {
    lock.lock();  // parks (state == SHARED); registers unique_pending_
    unique_acquired.store(true, std::memory_order_release);
    lock.unlock();  // hand off to the gated reader
  });

  // Let the UNIQUE waiter register unique_pending_ before the reader parks on that gate.
  std::this_thread::sleep_for(50ms);

  auto reader_fut = std::async(std::launch::async, [&] {
    lock.lock_shared<ResourceLock::LockReq::READ>();  // parks on unique_pending_ != 0
    reader_acquired.store(true, std::memory_order_release);
    lock.unlock_shared<ResourceLock::LockReq::READ>();
  });

  // Let the reader reach cv.wait, then confirm the gate (not scheduling) is holding both.
  std::this_thread::sleep_for(50ms);
  ASSERT_FALSE(unique_acquired.load(std::memory_order_acquire));
  ASSERT_FALSE(reader_acquired.load(std::memory_order_acquire));

  held_read.unlock();  // READ release -> notify_all must wake the pending UNIQUE

  // Bounded so a lost/absent wake fails the test instead of hanging the suite forever.
  const auto u_status = unique_fut.wait_for(5s);
  const auto r_status = reader_fut.wait_for(5s);
  EXPECT_EQ(u_status, std::future_status::ready)
      << "pending UNIQUE lock() was not woken by the READ release (lost wakeup)";
  EXPECT_EQ(r_status, std::future_status::ready)
      << "READ acquirer gated on unique_pending_ was not woken after the UNIQUE drained";

  if (u_status != std::future_status::ready || r_status != std::future_status::ready) {
    // Failure path only: re-issue the notify_all that was apparently lost so the parked threads
    // finish and ~future does not block the suite forever. State is already UNLOCKED here, so this
    // unlock() only serves as a rescue notify. (On success this branch is never taken.)
    lock.unlock();
    unique_fut.wait();
    reader_fut.wait();
    return;
  }
  unique_fut.get();
  reader_fut.get();
  EXPECT_TRUE(unique_acquired.load(std::memory_order_acquire));
  EXPECT_TRUE(reader_acquired.load(std::memory_order_acquire));
}

// A downgraded hold must still free the lock when released. Downgrading rewrites the shared counts
// without consulting the admission rules, so a slip there leaves a count standing that no release
// will ever clear: the lock never returns to unlocked and a waiting UNIQUE waits forever. Both
// source modes are covered because both are downgraded in anger, WRITE by garbage collection and
// READ_ONLY at the end of index creation.
TEST_F(ResourceLockTest, DowngradedHoldStillFreesTheLockForAWaitingUnique) {
  using namespace std::chrono_literals;

  auto check = [this](SharedResourceLockGuard::Type from) {
    auto guard = SharedResourceLockGuard(lock, from);
    ASSERT_TRUE(guard.owns_lock());

    std::atomic<bool> unique_acquired{false};
    auto unique_fut = std::async(std::launch::async, [&] {
      lock.lock();  // parks: the shared hold keeps the lock from being free
      unique_acquired.store(true, std::memory_order_release);
      lock.unlock();
    });

    std::this_thread::sleep_for(50ms);
    ASSERT_FALSE(unique_acquired.load(std::memory_order_acquire));

    ASSERT_TRUE(guard.downgrade_to_read());
    // Still a shared hold, so the UNIQUE stays parked: downgrading gives up exclusivity against
    // other shared modes, not the hold itself.
    std::this_thread::sleep_for(50ms);
    ASSERT_FALSE(unique_acquired.load(std::memory_order_acquire));

    guard.unlock();  // releases in READ, the mode it now holds

    // Bounded so a stranded count fails the test instead of hanging the suite forever.
    const auto status = unique_fut.wait_for(5s);
    EXPECT_EQ(status, std::future_status::ready)
        << "UNIQUE never acquired after the downgraded hold was released: the release did not "
           "return the lock to unlocked";
    if (status != std::future_status::ready) {
      unique_fut.wait();  // failure path only: the parked thread still owns the future
      return;
    }
    unique_fut.get();
  };

  check(SharedResourceLockGuard::WRITE);
  check(SharedResourceLockGuard::READ_ONLY);
}

// A timed UNIQUE request gates new shared acquisitions for as long as it is waiting, exactly as the
// untimed one does. This is the storage access-timeout path: acquisitions that wait out a timeout
// are routine, so the registration has to behave the same as on the path that waits forever.
TEST_F(ResourceLockTest, TimedUniqueGatesNewSharedAcquisitionsWhilePending) {
  using namespace std::chrono_literals;

  // A READ hold keeps the lock SHARED so the timed UNIQUE cannot acquire and stays pending. READ is
  // otherwise compatible with READ, so a refused probe below can only be the pending-UNIQUE gate.
  auto held_read = SharedResourceLockGuard(lock, SharedResourceLockGuard::READ);

  auto timed_unique = std::async(std::launch::async, [&] { return lock.try_lock_for(1s); });

  // Let it register as pending before probing.
  std::this_thread::sleep_for(50ms);
  EXPECT_FALSE(lock.try_lock_shared<ResourceLock::LockReq::READ>());
  EXPECT_FALSE(lock.try_lock_shared<ResourceLock::LockReq::WRITE>());
  EXPECT_FALSE(lock.try_lock_shared<ResourceLock::LockReq::READ_ONLY>());

  EXPECT_FALSE(timed_unique.get()) << "timed UNIQUE acquired while a READ was held";
  held_read.unlock();
}

// A timed UNIQUE that gives up must take its gate down with it, and wake what it was gating. The
// waiting side is the whole point: a shared acquirer parked on that gate has no other event coming,
// since the timeout is not a state change anyone else observes. Leaving the registration behind
// blocks every later shared acquisition of every kind, for the lifetime of the lock.
TEST_F(ResourceLockTest, TimedUniqueTimeoutWakesSharedAcquirerItGated) {
  using namespace std::chrono_literals;

  auto held_read = SharedResourceLockGuard(lock, SharedResourceLockGuard::READ);  // keeps state SHARED

  auto timed_unique = std::async(std::launch::async, [&] { return lock.try_lock_for(200ms); });

  // Let the timed UNIQUE register before the reader parks on its gate.
  std::this_thread::sleep_for(50ms);

  std::atomic<bool> reader_acquired{false};
  auto reader = std::async(std::launch::async, [&] {
    lock.lock_shared<ResourceLock::LockReq::READ>();  // parks on the pending-UNIQUE gate
    reader_acquired.store(true, std::memory_order_release);
    lock.unlock_shared<ResourceLock::LockReq::READ>();
  });

  // Let the reader reach cv.wait, then confirm the gate (not scheduling) is holding it.
  std::this_thread::sleep_for(50ms);
  ASSERT_FALSE(reader_acquired.load(std::memory_order_acquire));

  EXPECT_FALSE(timed_unique.get()) << "timed UNIQUE acquired while a READ was held";

  // Bounded so an absent wake fails the test instead of hanging the suite forever.
  const auto status = reader.wait_for(5s);
  EXPECT_EQ(status, std::future_status::ready)
      << "READ acquirer gated by the timed UNIQUE was not woken when that UNIQUE timed out";
  if (status != std::future_status::ready) {
    held_read.unlock();  // failure path only: rescue the parked thread so ~future cannot block
    reader.wait();
    return;
  }
  reader.get();
  EXPECT_TRUE(reader_acquired.load(std::memory_order_acquire));

  held_read.unlock();
}

// A WRITE release that does not free the lock still has to notify. w_count reaching 0 admits
// READ_ONLY, which is compatible with the READ hold that keeps the lock in SHARED, so the wake-up
// cannot be deferred to whenever the lock happens to become free: the READ holder may hold for as
// long as it likes. Narrowing unlock_should_notify<WRITE> to the fully-unlocked condition (as
// unlock_should_notify<READ> legitimately is, since no acquirer is gated on r_count) hangs this.
TEST_F(ResourceLockTest, WriteReleaseWakesGatedReadOnlyWhileReadersStillHold) {
  using namespace std::chrono_literals;

  auto held_read = SharedResourceLockGuard(lock, SharedResourceLockGuard::READ);    // r_count == 1
  auto held_write = SharedResourceLockGuard(lock, SharedResourceLockGuard::WRITE);  // w_count == 1

  std::atomic<bool> ro_acquired{false};
  auto ro_fut = std::async(std::launch::async, [&] {
    lock.lock_shared<ResourceLock::LockReq::READ_ONLY>();  // parks on w_count != 0
    ro_acquired.store(true, std::memory_order_release);
    lock.unlock_shared<ResourceLock::LockReq::READ_ONLY>();
  });

  // Let the READ_ONLY acquirer reach cv.wait, then confirm the gate (not scheduling) holds it.
  std::this_thread::sleep_for(50ms);
  ASSERT_FALSE(ro_acquired.load(std::memory_order_acquire));

  held_write.unlock();  // w_count 1 -> 0; lock stays SHARED (the READ is still held)

  // Bounded so a lost/absent wake fails the test instead of hanging the suite forever.
  const auto status = ro_fut.wait_for(5s);
  EXPECT_EQ(status, std::future_status::ready)
      << "READ_ONLY acquirer gated on w_count was not woken by the WRITE release";
  if (status != std::future_status::ready) {
    held_read.unlock();  // failure path only: rescue the parked thread so ~future cannot block
    ro_fut.wait();
    return;
  }
  ro_fut.get();
  EXPECT_TRUE(ro_acquired.load(std::memory_order_acquire));

  held_read.unlock();
}

// The mirror of the above: a READ_ONLY release that does not free the lock admits WRITE (ro_count
// reaching 0), which is likewise compatible with the READ hold keeping the lock in SHARED.
TEST_F(ResourceLockTest, ReadOnlyReleaseWakesGatedWriteWhileReadersStillHold) {
  using namespace std::chrono_literals;

  auto held_read = SharedResourceLockGuard(lock, SharedResourceLockGuard::READ);            // r_count == 1
  auto held_read_only = SharedResourceLockGuard(lock, SharedResourceLockGuard::READ_ONLY);  // ro_count == 1

  std::atomic<bool> writer_acquired{false};
  auto writer_fut = std::async(std::launch::async, [&] {
    lock.lock_shared<ResourceLock::LockReq::WRITE>();  // parks on ro_count != 0
    writer_acquired.store(true, std::memory_order_release);
    lock.unlock_shared<ResourceLock::LockReq::WRITE>();
  });

  std::this_thread::sleep_for(50ms);
  ASSERT_FALSE(writer_acquired.load(std::memory_order_acquire));

  held_read_only.unlock();  // ro_count 1 -> 0; lock stays SHARED (the READ is still held)

  const auto status = writer_fut.wait_for(5s);
  EXPECT_EQ(status, std::future_status::ready)
      << "WRITE acquirer gated on ro_count was not woken by the READ_ONLY release";
  if (status != std::future_status::ready) {
    held_read.unlock();  // failure path only: rescue the parked thread so ~future cannot block
    writer_fut.wait();
    return;
  }
  writer_fut.get();
  EXPECT_TRUE(writer_acquired.load(std::memory_order_acquire));

  held_read.unlock();
}

// Mutual-exclusion fuzzer again, with UniquePendingScope / ReadOnlyPendingScope try_acquire() among
// the hammered entry points, so a scope that adopts the lock is held to the same invariants.
TEST_F(ResourceLockTest, ConcurrentMutualExclusionInvariantsFuzzWithPendingScopes) {
  using namespace std::chrono_literals;
  constexpr int kNumThreads = 24;
  constexpr auto kDuration = 400ms;

  struct Counters {
    std::atomic<int> active_read{0};
    std::atomic<int> active_write{0};
    std::atomic<int> active_ro{0};
    std::atomic<int> active_unique{0};
  } counters;

  std::atomic<bool> invariant_violated{false};
  std::mutex violation_mutex;
  std::string violation_message;

  auto record_violation = [&](const std::string &msg) {
    bool expected = false;
    if (invariant_violated.compare_exchange_strong(expected, true)) {
      std::lock_guard<std::mutex> lg(violation_mutex);
      violation_message = msg;
    }
  };

  auto check_invariants = [&](const char *where) {
    const int aw = counters.active_write.load(std::memory_order_acquire);
    const int aro = counters.active_ro.load(std::memory_order_acquire);
    const int ar = counters.active_read.load(std::memory_order_acquire);
    const int au = counters.active_unique.load(std::memory_order_acquire);
    if (aw > 0 && aro > 0) {
      record_violation(std::string(where) + ": WRITE(" + std::to_string(aw) + ") and READ_ONLY(" + std::to_string(aro) +
                       ") concurrently active");
    }
    if (au > 0 && (aw > 0 || ar > 0 || aro > 0)) {
      record_violation(std::string(where) + ": UNIQUE(" + std::to_string(au) + ") active alongside shared holders (w=" +
                       std::to_string(aw) + ", r=" + std::to_string(ar) + ", ro=" + std::to_string(aro) + ")");
    }
    if (au > 1) {
      record_violation(std::string(where) + ": more than one UNIQUE holder concurrently (" + std::to_string(au) + ")");
    }
  };

  std::atomic<bool> stop{false};

  auto worker = [&](int seed) {
    std::mt19937 rng(static_cast<unsigned>(seed) * 2654435761u ^
                     static_cast<unsigned>(std::hash<std::thread::id>{}(std::this_thread::get_id())));
    std::uniform_int_distribution<int> op_pick(0, 6);
    std::uniform_int_distribution<int> hold_us(0, 50);

    while (!stop.load(std::memory_order_relaxed) && !invariant_violated.load(std::memory_order_relaxed)) {
      switch (op_pick(rng)) {
        case 0: {  // READ
          lock.lock_shared<ResourceLock::LockReq::READ>();
          counters.active_read.fetch_add(1, std::memory_order_acq_rel);
          check_invariants("READ");
          std::this_thread::sleep_for(std::chrono::microseconds(hold_us(rng)));
          counters.active_read.fetch_sub(1, std::memory_order_acq_rel);
          lock.unlock_shared<ResourceLock::LockReq::READ>();
          break;
        }
        case 1: {  // WRITE
          lock.lock_shared<ResourceLock::LockReq::WRITE>();
          counters.active_write.fetch_add(1, std::memory_order_acq_rel);
          check_invariants("WRITE");
          std::this_thread::sleep_for(std::chrono::microseconds(hold_us(rng)));
          counters.active_write.fetch_sub(1, std::memory_order_acq_rel);
          lock.unlock_shared<ResourceLock::LockReq::WRITE>();
          break;
        }
        case 2: {  // READ_ONLY, blocking
          lock.lock_shared<ResourceLock::LockReq::READ_ONLY>();
          counters.active_ro.fetch_add(1, std::memory_order_acq_rel);
          check_invariants("READ_ONLY(block)");
          std::this_thread::sleep_for(std::chrono::microseconds(hold_us(rng)));
          counters.active_ro.fetch_sub(1, std::memory_order_acq_rel);
          lock.unlock_shared<ResourceLock::LockReq::READ_ONLY>();
          break;
        }
        case 3: {  // UNIQUE via try_lock
          if (lock.try_lock()) {
            counters.active_unique.fetch_add(1, std::memory_order_acq_rel);
            check_invariants("UNIQUE(try_lock)");
            std::this_thread::sleep_for(std::chrono::microseconds(hold_us(rng)));
            counters.active_unique.fetch_sub(1, std::memory_order_acq_rel);
            lock.unlock();
          }
          break;
        }
        case 4: {  // UNIQUE via a single-shot UniquePendingScope::try_acquire()
          UniquePendingScope scope(lock);
          if (auto acquired = scope.try_acquire()) {
            counters.active_unique.fetch_add(1, std::memory_order_acq_rel);
            check_invariants("UNIQUE(pending_scope)");
            std::this_thread::sleep_for(std::chrono::microseconds(hold_us(rng)));
            counters.active_unique.fetch_sub(1, std::memory_order_acq_rel);
            // *acquired unlocks on scope exit below (std::unique_lock<ResourceLock> dtor)
          }
          break;
        }
        case 5: {  // READ_ONLY via a single-shot ReadOnlyPendingScope::try_acquire()
          ReadOnlyPendingScope scope(lock);
          if (auto acquired = scope.try_acquire()) {
            counters.active_ro.fetch_add(1, std::memory_order_acq_rel);
            check_invariants("READ_ONLY(pending_scope)");
            std::this_thread::sleep_for(std::chrono::microseconds(hold_us(rng)));
            counters.active_ro.fetch_sub(1, std::memory_order_acq_rel);
            // *acquired unlocks on scope exit below (SharedResourceLockGuard dtor)
          }
          break;
        }
        case 6: {  // plain try_lock_shared READ_ONLY (existing entry point, for contrast)
          if (lock.try_lock_shared<ResourceLock::LockReq::READ_ONLY>()) {
            counters.active_ro.fetch_add(1, std::memory_order_acq_rel);
            check_invariants("READ_ONLY(try)");
            std::this_thread::sleep_for(std::chrono::microseconds(hold_us(rng)));
            counters.active_ro.fetch_sub(1, std::memory_order_acq_rel);
            lock.unlock_shared<ResourceLock::LockReq::READ_ONLY>();
          }
          break;
        }
      }
    }
  };

  std::vector<std::jthread> threads;
  threads.reserve(kNumThreads);
  for (int i = 0; i < kNumThreads; ++i) {
    threads.emplace_back(worker, i);
  }

  std::this_thread::sleep_for(kDuration);
  stop.store(true, std::memory_order_relaxed);
  threads.clear();

  ASSERT_FALSE(invariant_violated.load()) << violation_message;
}

// --- Notify hook: single-threaded, deterministic fire-count tests ---
//
// SetNotifyHook installs a callable fired inside maybe_notify after the internal mutex is
// unlocked and cv.notify_all() has returned, on every NotifyKind::All transition.  The hook
// is armed via an atomic pointer, so an unarmed lock (no hook) is a single null-load of overhead.
//
// There are exactly six NotifyKind::All producers:
//  (1) unlock_shared<WRITE>  when w_count reaches 0
//  (2) unlock_shared<READ>   when r==w==ro==0 (fully unlocked)
//  (3) unlock_shared<READ_ONLY> when ro_count reaches 0
//  (4) unlock()              (UNIQUE always fully frees the lock)
//  (5) downgrade_to_read<WRITE|READ_ONLY> when the source count reaches 0
//  (6) ~UniquePendingScope / ~ReadOnlyPendingScope (unregister_pending) when their pending
//      counter reaches 0
//
// Every test below uses a plain `int fire_count` captured by reference — safe in single-threaded
// context without std::atomic.

TEST_F(ResourceLockTest, NotifyHookFiresOnWriteRelease) {
  int fire_count = 0;
  lock.SetNotifyHook([&] { ++fire_count; });

  lock.lock_shared<ResourceLock::LockReq::WRITE>();
  EXPECT_EQ(fire_count, 0);  // hook fires on release, not on acquisition

  lock.unlock_shared<ResourceLock::LockReq::WRITE>();
  EXPECT_EQ(fire_count, 1);  // w_count reached 0 → NotifyKind::All
}

TEST_F(ResourceLockTest, NotifyHookFiresOnReadRelease) {
  int fire_count = 0;
  lock.SetNotifyHook([&] { ++fire_count; });

  lock.lock_shared<ResourceLock::LockReq::READ>();
  EXPECT_EQ(fire_count, 0);

  // unlock_should_notify<READ> fires only when r==w==ro==0 (fully unlocked).
  lock.unlock_shared<ResourceLock::LockReq::READ>();
  EXPECT_EQ(fire_count, 1);
}

TEST_F(ResourceLockTest, NotifyHookFiresOnReadOnlyRelease) {
  int fire_count = 0;
  lock.SetNotifyHook([&] { ++fire_count; });

  lock.lock_shared<ResourceLock::LockReq::READ_ONLY>();
  EXPECT_EQ(fire_count, 0);

  lock.unlock_shared<ResourceLock::LockReq::READ_ONLY>();
  EXPECT_EQ(fire_count, 1);  // ro_count reached 0 → NotifyKind::All
}

TEST_F(ResourceLockTest, NotifyHookFiresOnUniqueRelease) {
  int fire_count = 0;
  lock.SetNotifyHook([&] { ++fire_count; });

  lock.lock();
  EXPECT_EQ(fire_count, 0);

  lock.unlock();
  EXPECT_EQ(fire_count, 1);  // UNIQUE release always fires
}

// downgrade_to_read<WRITE> decrements w_count (0 reached → first fire); the resulting READ hold
// is then released, freeing the lock (r==w==ro==0 → second fire).
TEST_F(ResourceLockTest, NotifyHookFiresOnWriteDowngradeThenReadRelease) {
  int fire_count = 0;
  lock.SetNotifyHook([&] { ++fire_count; });

  lock.lock_shared<ResourceLock::LockReq::WRITE>();
  EXPECT_EQ(fire_count, 0);

  ASSERT_TRUE(lock.downgrade_to_read<ResourceLock::LockReq::WRITE>());
  EXPECT_EQ(fire_count, 1);  // w_count hit 0 during downgrade

  lock.unlock_shared<ResourceLock::LockReq::READ>();
  EXPECT_EQ(fire_count, 2);  // READ release: r==w==ro==0 → fully unlocked → second fire
}

// A UniquePendingScope constructed while UNIQUE cannot acquire (READ is held, state == SHARED)
// and then destroyed WITHOUT calling try_acquire fires the hook once via unregister_pending<UNIQUE>
// when unique_pending_count reaches 0.
TEST_F(ResourceLockTest, NotifyHookFiresOnUniquePendingScopeDestroyedUnacquired) {
  int fire_count = 0;
  lock.SetNotifyHook([&] { ++fire_count; });

  // Hold READ: state == SHARED, so can_acquire<UNIQUE> (needs UNLOCKED) will fail.
  lock.lock_shared<ResourceLock::LockReq::READ>();
  EXPECT_EQ(fire_count, 0);

  {
    UniquePendingScope scope(lock);
    // Confirm try_acquire fails while READ is held.
    EXPECT_FALSE(scope.try_acquire().has_value());
    EXPECT_EQ(fire_count, 0);
    // Destructor: unregister_pending<UNIQUE> → unique_pending_count reaches 0 → fires.
  }
  EXPECT_EQ(fire_count, 1);

  // Release the READ hold: r==w==ro==0 → fires again.
  lock.unlock_shared<ResourceLock::LockReq::READ>();
  EXPECT_EQ(fire_count, 2);
}

// A ReadOnlyPendingScope constructed while READ_ONLY cannot acquire (WRITE is held, w_count != 0)
// and destroyed WITHOUT acquiring fires once via unregister_pending<READ_ONLY> when
// ro_pending_count reaches 0.
TEST_F(ResourceLockTest, NotifyHookFiresOnReadOnlyPendingScopeDestroyedUnacquired) {
  int fire_count = 0;
  lock.SetNotifyHook([&] { ++fire_count; });

  // Hold WRITE: can_acquire<READ_ONLY> requires w_count == 0, so the scope cannot acquire.
  lock.lock_shared<ResourceLock::LockReq::WRITE>();
  EXPECT_EQ(fire_count, 0);

  {
    ReadOnlyPendingScope scope(lock);
    EXPECT_FALSE(scope.try_acquire().has_value());
    EXPECT_EQ(fire_count, 0);
    // Destructor: unregister_pending<READ_ONLY> → ro_pending_count reaches 0 → fires.
  }
  EXPECT_EQ(fire_count, 1);

  // Release WRITE: w_count reaches 0 → fires again.
  lock.unlock_shared<ResourceLock::LockReq::WRITE>();
  EXPECT_EQ(fire_count, 2);
}

// Two concurrent WRITE holds: releasing the first drops w_count to 1 (not 0), which is a
// NotifyKind::None transition.  The hook must NOT fire at the intermediate step.
TEST_F(ResourceLockTest, NotifyHookDoesNotFireOnPartialWriteRelease) {
  int fire_count = 0;
  lock.SetNotifyHook([&] { ++fire_count; });

  lock.lock_shared<ResourceLock::LockReq::WRITE>();  // w_count = 1
  lock.lock_shared<ResourceLock::LockReq::WRITE>();  // w_count = 2
  EXPECT_EQ(fire_count, 0);

  // First release: w_count drops to 1, still non-zero → NotifyKind::None.
  lock.unlock_shared<ResourceLock::LockReq::WRITE>();
  EXPECT_EQ(fire_count, 0);  // intermediate: no fire

  // Second release: w_count drops to 0 → NotifyKind::All.
  lock.unlock_shared<ResourceLock::LockReq::WRITE>();
  EXPECT_EQ(fire_count, 1);
}

// After ClearNotifyHook() the hook pointer is set to nullptr; the same acquire/release cycle
// that fired before must not call the hook again.
TEST_F(ResourceLockTest, NotifyHookSilentAfterClearNotifyHook) {
  int fire_count = 0;
  lock.SetNotifyHook([&] { ++fire_count; });

  lock.lock_shared<ResourceLock::LockReq::WRITE>();
  lock.unlock_shared<ResourceLock::LockReq::WRITE>();
  EXPECT_EQ(fire_count, 1);  // hook was armed and fired

  lock.ClearNotifyHook();

  lock.lock_shared<ResourceLock::LockReq::WRITE>();
  lock.unlock_shared<ResourceLock::LockReq::WRITE>();
  EXPECT_EQ(fire_count, 1);  // hook disarmed: count unchanged
}

// A lock with no hook installed must operate correctly across all six NotifyKind::All producers
// without crashing, and must never increment an external counter that was never installed as a hook.
TEST_F(ResourceLockTest, UnarmedLockIsSafeAndSilentWithoutHook) {
  ResourceLock unarmed_lock;
  int fire_count = 0;
  // Intentionally no SetNotifyHook on unarmed_lock.

  unarmed_lock.lock_shared<ResourceLock::LockReq::WRITE>();
  unarmed_lock.unlock_shared<ResourceLock::LockReq::WRITE>();
  EXPECT_EQ(fire_count, 0);

  unarmed_lock.lock_shared<ResourceLock::LockReq::READ>();
  unarmed_lock.unlock_shared<ResourceLock::LockReq::READ>();
  EXPECT_EQ(fire_count, 0);

  unarmed_lock.lock_shared<ResourceLock::LockReq::READ_ONLY>();
  unarmed_lock.unlock_shared<ResourceLock::LockReq::READ_ONLY>();
  EXPECT_EQ(fire_count, 0);

  unarmed_lock.lock();
  unarmed_lock.unlock();
  EXPECT_EQ(fire_count, 0);

  // downgrade (producer 5) with no hook
  unarmed_lock.lock_shared<ResourceLock::LockReq::WRITE>();
  ASSERT_TRUE(unarmed_lock.downgrade_to_read<ResourceLock::LockReq::WRITE>());
  unarmed_lock.unlock_shared<ResourceLock::LockReq::READ>();
  EXPECT_EQ(fire_count, 0);

  // unregister_pending (producer 6) with no hook
  unarmed_lock.lock_shared<ResourceLock::LockReq::READ>();
  {
    UniquePendingScope scope(unarmed_lock);
    EXPECT_FALSE(scope.try_acquire().has_value());
  }
  unarmed_lock.unlock_shared<ResourceLock::LockReq::READ>();
  EXPECT_EQ(fire_count, 0);
}

// A pending UNIQUE waiter must not miss its wake-up to a waiter that cannot use it.
//
// Every waiter parks on the same condition variable but each has its own predicate, so a
// notify_one can be delivered to a waiter whose predicate is still false: a shared acquirer gated
// on unique_pending_, say. It re-checks, sleeps again, and consumes the only notification, leaving
// the waiter that could have made progress asleep until its timeout.
//
// Both crowds are load-bearing: several shared acquirers parked to absorb the notification, and
// several UNIQUE waiters so the one woken is not reliably the one waiting longest. With a single
// UNIQUE waiter of either kind the interleaving cannot arise and the test asserts nothing.
TEST_F(ResourceLockTest, PendingUniqueIsWokenWhenSharedAcquirersAreAlsoParked) {
  using namespace std::chrono_literals;
  constexpr auto kUniqueTimeout = 500ms;
  constexpr auto kDuration = 2s;
  constexpr int kSharedCyclers = 4;
  constexpr int kUniqueWaiters = 4;

  std::atomic<bool> stop{false};
  std::atomic<int> unique_timeouts{0};
  std::atomic<int> unique_acquisitions{0};

  std::vector<std::jthread> threads;
  threads.reserve(kSharedCyclers + kUniqueWaiters);

  // Each cycler drives the lock down to fully-unlocked (firing the notify) and, whenever a UNIQUE
  // is pending, parks on the acquisition gated by unique_pending_ (becoming a potential absorber).
  for (int i = 0; i < kSharedCyclers; ++i) {
    threads.emplace_back([&] {
      while (!stop.load(std::memory_order_relaxed)) {
        lock.lock_shared<ResourceLock::LockReq::READ>();
        lock.unlock_shared<ResourceLock::LockReq::READ>();
        lock.lock_shared<ResourceLock::LockReq::WRITE>();
        lock.unlock_shared<ResourceLock::LockReq::WRITE>();
      }
    });
  }

  for (int i = 0; i < kUniqueWaiters; ++i) {
    threads.emplace_back([&] {
      while (!stop.load(std::memory_order_relaxed)) {
        if (lock.try_lock_for(kUniqueTimeout)) {
          unique_acquisitions.fetch_add(1, std::memory_order_relaxed);
          lock.unlock();
        } else {
          unique_timeouts.fetch_add(1, std::memory_order_relaxed);
        }
      }
    });
  }

  std::this_thread::sleep_for(kDuration);
  stop.store(true, std::memory_order_relaxed);
  threads.clear();

  EXPECT_GT(unique_acquisitions.load(), 0) << "the UNIQUE waiter never acquired; the test proved nothing";
  EXPECT_EQ(unique_timeouts.load(), 0) << "a pending UNIQUE waiter was not woken within " << kUniqueTimeout.count()
                                       << "ms while " << kSharedCyclers << " shared acquirers were parked";
}
