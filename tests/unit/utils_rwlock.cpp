// Copyright 2022 Memgraph Ltd.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt; by using this file, you agree to be bound by the terms of the Business Source
// License, and you may not use this file except in compliance with the Business Source License.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

#include <shared_mutex>
#include <thread>

#include "gtest/gtest.h"

#include "utils/rw_lock.hpp"
#include "utils/timer.hpp"

using namespace std::chrono_literals;

TEST(RWLock, MultipleReaders) {
  memgraph::utils::RWLock rwlock(memgraph::utils::RWLock::Priority::READ);

  std::vector<std::thread> threads;
  memgraph::utils::Timer timer;
  for (int i = 0; i < 3; ++i) {
    threads.push_back(std::thread([&rwlock] {
      std::shared_lock<memgraph::utils::RWLock> lock(rwlock);
      std::this_thread::sleep_for(100ms);
    }));
  }

  for (int i = 0; i < 3; ++i) {
    threads[i].join();
  }

  EXPECT_LE(timer.Elapsed(), 150ms);
  EXPECT_GE(timer.Elapsed(), 90ms);
}

TEST(RWLock, SingleWriter) {
  memgraph::utils::RWLock rwlock(memgraph::utils::RWLock::Priority::READ);

  std::vector<std::thread> threads;
  memgraph::utils::Timer timer;
  for (int i = 0; i < 3; ++i) {
    threads.push_back(std::thread([&rwlock] {
      std::unique_lock<memgraph::utils::RWLock> lock(rwlock);
      std::this_thread::sleep_for(100ms);
    }));
  }

  for (int i = 0; i < 3; ++i) {
    threads[i].join();
  }

  EXPECT_LE(timer.Elapsed(), 350ms);
  EXPECT_GE(timer.Elapsed(), 290ms);
}

TEST(RWLock, ReadPriority) {
  /*
   * - Main thread is holding a shared lock until T = 100ms.
   * - Thread 1 tries to acquire an unique lock at T = 30ms.
   * - Thread 2 successfully acquires a shared lock at T = 60ms, even though
   *   there's a writer waiting.
   */
  memgraph::utils::RWLock rwlock(memgraph::utils::RWLock::Priority::READ);
  rwlock.lock_shared();
  bool first = true;

  std::thread t1([&rwlock, &first] {
    std::this_thread::sleep_for(30ms);
    std::unique_lock<memgraph::utils::RWLock> lock(rwlock);
    EXPECT_FALSE(first);
  });

  std::thread t2([&rwlock, &first] {
    std::this_thread::sleep_for(60ms);
    std::shared_lock<memgraph::utils::RWLock> lock(rwlock);
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
    std::unique_lock<memgraph::utils::RWLock> lock(rwlock);
    EXPECT_TRUE(first);
    first = false;
  });

  std::thread t2([&rwlock, &first] {
    std::this_thread::sleep_for(60ms);
    std::shared_lock<memgraph::utils::RWLock> lock(rwlock);
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
