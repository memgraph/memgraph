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

#include "utils/rw_lock.hpp"
#include "utils/timer.hpp"

#include <latch>
#include <shared_mutex>
#include <thread>

using namespace std::chrono_literals;

TEST(RWLock, MultipleReaders) {
  memgraph::utils::RWLock rwlock(memgraph::utils::RWLock::Priority::READ);

  std::vector<std::jthread> threads;
  threads.reserve(3);
  memgraph::utils::Timer const timer;
  for (int i = 0; i < 3; ++i) {
    threads.emplace_back([&rwlock] {
      auto lock = std::shared_lock{rwlock};
      std::this_thread::sleep_for(100ms);
    });
  }
  auto const elapsed = timer.Elapsed();
  // If they are running in parallel, then the total running time should be < 3x100ms
  EXPECT_LE(elapsed, 300ms);
  EXPECT_GE(elapsed, 90ms);
}

TEST(RWLock, SingleWriter) {
  memgraph::utils::RWLock rwlock(memgraph::utils::RWLock::Priority::READ);
  auto count_down_start = std::latch{3};
  auto count_down_finish = std::latch{4};

  memgraph::utils::Timer timer;
  std::chrono::duration<double> start;
  std::chrono::duration<double> total_time;

  auto j1 = [&] {
    // Start only when all threads exist
    count_down_start.arrive_and_wait();

    {
      // In smallest scope possible
      auto lock = std::unique_lock{rwlock};
      std::this_thread::sleep_for(100ms);
    }

    // Signal that the thread's work has finished
    count_down_finish.count_down();
  };
  auto j2 = [&] {
    start = timer.Elapsed();  // time from here to avoid the timing cost of setting up threads
    j1();
  };

  {
    auto threads = std::vector<std::jthread>{};
    threads.emplace_back(j1);
    threads.emplace_back(j1);
    std::this_thread::sleep_for(1ms);  // Give time for other threads to have started
    threads.emplace_back(j2);
    // avoid timing cost to tear down threads
    count_down_finish.arrive_and_wait();
    total_time = timer.Elapsed() - start;
  }

  EXPECT_LE(total_time, 350ms);
  EXPECT_GE(total_time, 290ms);
}

TEST(RWLock, ReadPriority) {
  /*
   * - Main thread is holding a shared lock until T = 100ms.
   * - Thread 1 tries to acquire an unique lock at T = 30ms.
   * - Thread 2 successfuly acquires a shared lock at T = 60ms, even though
   *   there's a writer waiting.
   */
  memgraph::utils::RWLock rwlock(memgraph::utils::RWLock::Priority::READ);
  rwlock.lock_shared();
  bool first = true;

  std::thread t1([&rwlock, &first] {
    std::this_thread::sleep_for(30ms);
    auto lock = std::unique_lock{rwlock};
    EXPECT_FALSE(first);
  });

  std::thread t2([&rwlock, &first] {
    std::this_thread::sleep_for(60ms);
    auto lock = std::shared_lock{rwlock};
    EXPECT_TRUE(first);
    first = false;
  });

  std::this_thread::sleep_for(100ms);
  rwlock.unlock_shared();
  t1.join();
  t2.join();
}

TEST(RWLock, WritePriority) {
  /*
   * - Main thread is holding a shared lock until T = 100ms.
   * - Thread 1 tries to acquire an unique lock at T = 30ms.
   * - Thread 2 tries to acquire a shared lock at T = 60ms, but it is not able
   *   to because of write priority.
   */
  memgraph::utils::RWLock rwlock(memgraph::utils::RWLock::Priority::WRITE);
  rwlock.lock_shared();
  bool first = true;

  std::thread t1([&rwlock, &first] {
    std::this_thread::sleep_for(30ms);
    auto lock = std::unique_lock{rwlock};
    EXPECT_TRUE(first);
    first = false;
  });

  std::thread t2([&rwlock, &first] {
    std::this_thread::sleep_for(60ms);
    auto lock = std::shared_lock{rwlock};
    EXPECT_FALSE(first);
  });

  std::this_thread::sleep_for(100ms);
  rwlock.unlock_shared();

  t1.join();
  t2.join();
}

TEST(RWLock, TryLock) {
  memgraph::utils::RWLock rwlock(memgraph::utils::RWLock::Priority::WRITE);
  rwlock.lock();

  std::thread t1([&rwlock] { EXPECT_FALSE(rwlock.try_lock()); });
  t1.join();

  std::thread t2([&rwlock] { EXPECT_FALSE(rwlock.try_lock_shared()); });
  t2.join();

  rwlock.unlock();

  std::thread t3([&rwlock] {
    EXPECT_TRUE(rwlock.try_lock());
    rwlock.unlock();
  });
  t3.join();

  std::thread t4([&rwlock] {
    EXPECT_TRUE(rwlock.try_lock_shared());
    rwlock.unlock_shared();
  });
  t4.join();

  rwlock.lock_shared();

  std::thread t5([&rwlock] {
    EXPECT_TRUE(rwlock.try_lock_shared());
    rwlock.unlock_shared();
  });
  t5.join();
}
