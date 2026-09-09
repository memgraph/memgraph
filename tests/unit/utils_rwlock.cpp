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

#include "utils/rw_lock.hpp"
#include "utils/timer.hpp"

#include <barrier>
#include <latch>
#include <semaphore>
#include <shared_mutex>
#include <thread>

using namespace std::chrono_literals;

TEST(RWLock, MultipleReaders) {
  memgraph::utils::RWLock rwlock(memgraph::utils::RWLock::Priority::READ);
  constexpr int num_workers{3};

  std::vector<std::thread> threads;
  threads.reserve(num_workers);

  auto timer = memgraph::utils::Timer();
  auto start = std::chrono::duration<double>();

  std::barrier start_sync(num_workers, [&start, &timer]() { start = timer.Elapsed(); });
  std::barrier end_sync(num_workers, [&start, &timer]() {
    auto const elapsed = timer.Elapsed() - start;
    EXPECT_LE(elapsed, 150ms);
    EXPECT_GE(elapsed, 90ms);
  });

  for (int i = 0; i < num_workers; ++i) {
    threads.emplace_back([&]() {
      start_sync.arrive_and_wait();
      auto lock = std::shared_lock{rwlock};
      std::this_thread::sleep_for(100ms);
      end_sync.arrive_and_wait();
    });
  }

  for (auto &thread : threads) {
    thread.join();
  }
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
   * With read priority a shared lock is still granted while a writer waits for the exclusive one,
   * so a succession of readers keeps the writer out for as long as it lasts.
   */
  memgraph::utils::RWLock rwlock(memgraph::utils::RWLock::Priority::READ);

  // Held across every one of the reader's acquisitions below, so the number of readers never falls
  // back to zero between them and the waiting writer is never offered the lock in the gap.
  rwlock.lock_shared();

  constexpr int kAdmissions = 64;

  // Deliberately not atomic: the lock is the barrier under test. The reader increments this
  // holding the shared lock and the writer reads it holding the exclusive one, so a lock that
  // failed to order the two would be a data race here rather than a passing test.
  int admissions = 0;

  std::binary_semaphore writer_requesting{0};
  std::binary_semaphore reader_holds_lock{0};

  std::thread writer([&] {
    writer_requesting.release();
    auto lock = std::unique_lock{rwlock};
    EXPECT_EQ(admissions, kAdmissions) << "writer was let in ahead of readers that asked later";
  });

  // A writer waiting for the exclusive lock cannot be observed through the lock itself, so the
  // writer announces the moment before it asks for one. Its arrival can only be raced by the
  // reader's first admission, which is why the reader is admitted repeatedly rather than once. A
  // lock without read priority stops the reader at the first admission and it never signals.
  writer_requesting.acquire();

  std::thread reader([&] {
    for (int i = 0; i < kAdmissions; ++i) {
      auto lock = std::shared_lock{rwlock};
      ++admissions;
      // Signalled while this lock is held, so the main thread gives up its own shared lock only
      // once this reader is in. Releasing that on a timer instead would let a slow machine hand
      // the lock to the waiting writer first, failing the test for want of scheduling.
      if (i + 1 == kAdmissions) reader_holds_lock.release();
    }
  });

  bool const readers_were_admitted = reader_holds_lock.try_acquire_for(10s);

  // Released before asserting so both threads run to completion whatever the outcome: leaving
  // either blocked would end the test by terminate rather than by a failure.
  rwlock.unlock_shared();
  writer.join();
  reader.join();

  EXPECT_TRUE(readers_were_admitted) << "reader could not take a shared lock while a writer was waiting";
}

TEST(RWLock, WritePriority) {
  /*
   * With write priority a shared lock is refused while a writer waits for the exclusive one, so a
   * reader that asks after the writer has to be let in behind it.
   */
  memgraph::utils::RWLock rwlock(memgraph::utils::RWLock::Priority::WRITE);

  // Held until the reader is known to be behind the writer, so the writer has to queue for the
  // lock rather than walk into a free one and settle the order by itself.
  rwlock.lock_shared();

  // Deliberately not atomic: the lock is the barrier under test. The writer sets this holding the
  // exclusive lock and the reader reads it holding a shared one, so a lock that failed to order the
  // two would be a data race here rather than a passing test.
  bool writer_went_first = false;

  std::binary_semaphore writer_requesting{0};
  std::binary_semaphore reader_behind_writer{0};

  std::thread writer([&] {
    writer_requesting.release();
    auto lock = std::unique_lock{rwlock};
    writer_went_first = true;
  });

  // Announcing is not the same as having asked, and the reader needs the request itself to be
  // registered before it asks. It establishes that below rather than trusting this.
  writer_requesting.acquire();

  std::thread reader([&] {
    // A waiting writer cannot be read off the lock. On a write-priority lock it is, however,
    // exactly what refuses a shared request while another shared hold is live, so probing until one
    // is refused is how this thread learns the writer is queued ahead of it. Nothing here is timed.
    // The bound only stops a hang: giving up leaves the assertion below to fail instead.
    auto const give_up_at = std::chrono::steady_clock::now() + 10s;
    while (rwlock.try_lock_shared()) {
      rwlock.unlock_shared();
      if (std::chrono::steady_clock::now() > give_up_at) break;
      std::this_thread::yield();
    }
    reader_behind_writer.release();
    auto lock = std::shared_lock{rwlock};
    EXPECT_TRUE(writer_went_first) << "a reader that asked after a waiting writer was admitted ahead of it";
  });

  reader_behind_writer.acquire();
  // Released only now, so neither thread can have been given the lock for want of contention.
  rwlock.unlock_shared();

  writer.join();
  reader.join();
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
