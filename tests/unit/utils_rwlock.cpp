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

#include <algorithm>
#include <latch>
#include <semaphore>
#include <shared_mutex>
#include <thread>

using namespace std::chrono_literals;

TEST(RWLock, SupportsMultipleConcurrentReaders) {
  memgraph::utils::RWLock rwlock(memgraph::utils::RWLock::Priority::READ);
  constexpr int num_workers{3};

  std::counting_semaphore<> holding_lock{0};
  std::counting_semaphore<> may_release{0};

  auto readers = std::vector<std::jthread>{};
  readers.reserve(num_workers);

  for (int i = 0; i < num_workers; ++i) {
    readers.emplace_back([&] {
      auto lock = std::shared_lock{rwlock};
      // Released while the lock is still held, so an arrival means this reader is
      // inside the critical section, not merely that it got in at some point.
      holding_lock.release();
      // Timed, so a reader unwinds and gives up its lock even if the test fails
      // and the gate below is never opened.
      may_release.try_acquire_for(10s);
    });
  }

  // Every reader is holding the shared lock at the same time, or the lock is not
  // granting shared access. Timed out rather than waited on forever, so a lock
  // that never admits them all fails here instead of hanging.
  for (int i = 0; i < num_workers; ++i) {
    EXPECT_TRUE(holding_lock.try_acquire_for(10s)) << "readers did not hold the shared lock concurrently";
  }

  may_release.release(num_workers);
}

TEST(RWLock, ExclusiveLockAdmitsOneWriterAtATime) {
  memgraph::utils::RWLock rwlock(memgraph::utils::RWLock::Priority::READ);
  constexpr int num_writers{3};

  // Deliberately not atomic: the lock is the barrier under test. Every access below is made
  // holding the exclusive lock, so a lock that admitted two writers together would be a data race
  // here rather than a passing test.
  int writers_inside = 0;
  int peak_writers_inside = 0;
  int admissions = 0;

  // Every writer exists before any of them asks for the lock, so the ones that lose have to queue
  // rather than each finding a free lock in turn.
  auto all_started = std::latch{num_writers};

  {
    auto writers = std::vector<std::jthread>{};
    writers.reserve(num_writers);

    for (int i = 0; i < num_writers; ++i) {
      writers.emplace_back([&] {
        all_started.arrive_and_wait();
        auto lock = std::unique_lock{rwlock};
        ++admissions;
        ++writers_inside;
        peak_writers_inside = std::max(peak_writers_inside, writers_inside);
        // Held long enough that an overlap has time to be seen in the peak. A writer that released
        // straight away could be admitted and gone before the next one asked, and a lock granting
        // both at once would read the same as one that sequenced them.
        std::this_thread::sleep_for(50ms);
        --writers_inside;
      });
    }
  }

  EXPECT_EQ(peak_writers_inside, 1) << "two writers held the exclusive lock at the same time";
  EXPECT_EQ(admissions, num_writers) << "a writer never got the exclusive lock";
  EXPECT_EQ(writers_inside, 0);
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

  std::binary_semaphore reader_behind_writer{0};

  std::thread writer([&] {
    auto lock = std::unique_lock{rwlock};
    writer_went_first = true;
  });

  std::thread reader([&] {
    // A waiting writer cannot be read off the lock. On a write-priority lock it is, however,
    // exactly what refuses a shared request while another shared hold is live, so probing until one
    // is refused is how this thread learns the writer is queued ahead of it. The order the
    // assertion checks is decided by the lock, never by how long anything took, which is what a
    // reader starting from a sleep could not establish.
    //
    // The bound stops a lock without write priority from spinning here forever. It does not
    // guarantee a failure: a writer that arrives after the bound expires still takes the lock
    // first, and the assertion then passes without this thread having established what it set out
    // to. That needs a writer delayed by the whole bound, so it costs a missed check rather than a
    // false one.
    auto const give_up_at = std::chrono::steady_clock::now() + 10s;
    while (rwlock.try_lock_shared()) {
      rwlock.unlock_shared();
      if (std::chrono::steady_clock::now() > give_up_at) break;
      // Sleeping rather than yielding: on a lock that never refuses, yielding spins a core for the
      // whole bound, and nothing here needs to observe the refusal promptly.
      std::this_thread::sleep_for(100us);
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
