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

// Stress tests: a varied TSan surface plus liveness probes (UNIQUE starvation vs shared holders,
// READ_ONLY latency vs new writers).

// Test 1: mutual-exclusion fuzzer. Threads hammer every entry point while atomic counters are
// checked against the lock's invariants on each acquisition (catches violations + TSan races).
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

// Test 2a: UNIQUE-starvation probe under continuous READ churn. With writer-preference,
// unique_pending_ gates new READ holders so lock() acquires within the watchdog bound; a hung
// acquire becomes a soft, logged failure instead of a hung binary.
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
                  << " hammer threads — ResourceLock::lock() has no anti-starvation mechanism against shared "
                     "holders, this is a liveness/fairness bug, not a memory-safety one.";
    unique_future.get();  // reap the async thread now that hammers have stopped
  }
}

// Test 2b: same probe, hammering WRITE instead of READ.
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
                  << " hammer threads — ResourceLock::lock() has no anti-starvation mechanism against shared "
                     "holders, this is a liveness/fairness bug, not a memory-safety one.";
    unique_future.get();
  }
}

// Test 3: READ_ONLY vs WRITE priority. A mid-stream READ_ONLY registers ro_pending_count, gating
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
                                           "acquire before it — ro_pending_count priority not honoured";

    return std::chrono::duration_cast<std::chrono::microseconds>(t_acquired - t_start);
  };

  const auto latency_small_burst = measure(/*num_inflight_writers=*/2, /*num_late_writers=*/5);
  const auto latency_large_burst = measure(/*num_inflight_writers=*/2, /*num_late_writers=*/60);

  RecordProperty("ro_latency_small_burst_us", std::to_string(latency_small_burst.count()));
  RecordProperty("ro_latency_large_burst_us", std::to_string(latency_large_burst.count()));

  // The large burst has 12x more new writers queued behind the READ_ONLY request; if priority is
  // honoured, that should barely move the READ_ONLY latency (bounded by in-flight-writer drain
  // time), not scale with the queue length.
  EXPECT_LT(latency_large_burst, latency_small_burst + 50ms)
      << "READ_ONLY latency grew with the number of new writers queued behind it (small burst="
      << latency_small_burst.count() << "us, large burst=" << latency_large_burst.count()
      << "us) — ro_pending_count priority/starvation regression";
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

// Test 4a: a UniquePendingScope retry loop acquires within a bound under continuous WRITE churn,
// whereas a bare try_lock() loop is expected to starve (soft/environment-dependent control).
TEST_F(ResourceLockTest, UniquePendingScopeCampaignAcquiresWhileBareTryLockStarves) {
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
      std::optional<std::unique_lock<ResourceLock>> acquired;
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
                    << " hammer threads — pending-scope writer-preference regression";
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
      lock.unlock();  // release so later tests start from UNLOCKED
      // The ungated control is expected to starve; its success is timing-dependent, so log
      // (non-fatal) rather than abort.
      ADD_FAILURE() << "bare try_lock() control unexpectedly acquired within " << control_result->count()
                    << "ms under continuous WRITE churn — expected it to starve (soft/environment-dependent check)";
    }
  }
}

// Test 4b: same contrast, but ReadOnlyPendingScope against continuous WRITE hammers.
TEST_F(ResourceLockTest, ReadOnlyPendingScopeCampaignAcquiresUnderContinuousWriteHammer) {
  using namespace std::chrono_literals;
  constexpr int kNumHammers = 8;
  constexpr auto kBound = 5s;

  std::atomic<bool> stop{false};
  auto hammers = SpawnHammers<ResourceLock::LockReq::WRITE>(lock, stop, kNumHammers);

  auto scoped_future = std::async(std::launch::async, [&] {
    auto start = std::chrono::steady_clock::now();
    ReadOnlyPendingScope scope(lock);
    std::optional<SharedResourceLockGuard> acquired;
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
    EXPECT_EQ(acquired_type, SharedResourceLockGuard::READ_ONLY);
    RecordProperty("ro_pending_scope_latency_ms", std::to_string(latency.count()));
  } else {
    ADD_FAILURE() << "ReadOnlyPendingScope::try_acquire() campaign did not acquire within " << kBound.count()
                  << "s under continuous WRITE churn from " << kNumHammers
                  << " hammer threads — pending-scope priority regression";
    scoped_future.get();  // reap the async thread now that hammers have stopped
  }
}

// Test 4c: deterministic check that a live, not-yet-successful UniquePendingScope gates new shared
// acquisitions of every kind, and that the gate clears the moment it is destroyed unacquired.
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

// Test 4c-ii: destroying a still-pending UniquePendingScope must WAKE a thread genuinely blocked in
// lock_shared() (parked in cv.wait), exercising the destructor's notify path. A dropped notify or
// off-mtx counter would hang and fail via the bounded wait_for. (The narrow notify-races-enqueue
// window isn't deterministically reproducible; that rests on the mtx discipline + the fuzz test.)
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

  // Bounded so a lost/absent wake fails the test (~5s) instead of hanging the suite forever.
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

// Test 4d: mutual-exclusion fuzzer (like Test 1) with UniquePendingScope / ReadOnlyPendingScope
// try_acquire() mixed in, to cover the new code paths (mtx-protected mutation + adopt_lock).
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
