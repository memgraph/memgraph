#include <shared_mutex>
#include <thread>

#include "glog/logging.h"
#include "gtest/gtest.h"

#include "threading/sync/rwlock.hpp"
#include "utils/timer.hpp"

using namespace std::chrono_literals;

using threading::RWLock;
using threading::RWLockPriority;

TEST(RWLock, MultipleReaders) {
  RWLock rwlock(RWLockPriority::READ);

  std::vector<std::thread> threads;
  utils::Timer timer;
  for (int i = 0; i < 3; ++i) {
    threads.push_back(std::thread([&rwlock] {
      std::shared_lock<RWLock> lock(rwlock);
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
  RWLock rwlock(RWLockPriority::READ);

  std::vector<std::thread> threads;
  utils::Timer timer;
  for (int i = 0; i < 3; ++i) {
    threads.push_back(std::thread([&rwlock] {
      std::unique_lock<RWLock> lock(rwlock);
      std::this_thread::sleep_for(100ms);
    }));
  }

  for (int i = 0; i < 3; ++i) {
    threads[i].join();
  }

  EXPECT_LE(timer.Elapsed(), 350ms);
  EXPECT_GE(timer.Elapsed(), 290ms);
}

/* Next two tests demonstrate that writers are always preferred when an unique
 * lock is released. */

TEST(RWLock, WritersPreferred_1) {
  RWLock rwlock(RWLockPriority::READ);
  rwlock.lock();
  bool first = true;

  std::thread t1([&rwlock, &first] {
    std::shared_lock<RWLock> lock(rwlock);
    EXPECT_FALSE(first);
  });

  std::thread t2([&rwlock, &first] {
    std::unique_lock<RWLock> lock(rwlock);
    EXPECT_TRUE(first);
    first = false;
  });

  std::this_thread::sleep_for(100ms);
  rwlock.unlock();
  t1.join();
  t2.join();
}

TEST(RWLock, WritersPreferred_2) {
  RWLock rwlock(RWLockPriority::WRITE);
  rwlock.lock();
  bool first = true;

  std::thread t1([&rwlock, &first] {
    std::shared_lock<RWLock> lock(rwlock);
    EXPECT_FALSE(first);
  });

  std::thread t2([&rwlock, &first] {
    std::unique_lock<RWLock> lock(rwlock);
    EXPECT_TRUE(first);
    first = false;
  });

  std::this_thread::sleep_for(100ms);
  rwlock.unlock();
  t1.join();
  t2.join();
}

TEST(RWLock, ReadPriority) {
  /*
   * - Main thread is holding a shared lock until T = 100ms.
   * - Thread 1 tries to acquire an unique lock at T = 30ms.
   * - Thread 2 successfuly acquires a shared lock at T = 60ms, even though
   *   there's a writer waiting.
   */
  RWLock rwlock(RWLockPriority::READ);
  rwlock.lock_shared();
  bool first = true;

  std::thread t1([&rwlock, &first] {
    std::this_thread::sleep_for(30ms);
    std::unique_lock<RWLock> lock(rwlock);
    EXPECT_FALSE(first);
  });

  std::thread t2([&rwlock, &first] {
    std::this_thread::sleep_for(60ms);
    std::shared_lock<RWLock> lock(rwlock);
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
  RWLock rwlock(RWLockPriority::WRITE);
  rwlock.lock_shared();
  bool first = true;

  std::thread t1([&rwlock, &first] {
    std::this_thread::sleep_for(30ms);
    std::unique_lock<RWLock> lock(rwlock);
    EXPECT_TRUE(first);
    first = false;
  });

  std::thread t2([&rwlock, &first] {
    std::this_thread::sleep_for(60ms);
    std::shared_lock<RWLock> lock(rwlock);
    EXPECT_FALSE(first);
  });

  std::this_thread::sleep_for(100ms);
  rwlock.unlock_shared();

  t1.join();
  t2.join();
}

TEST(RWLock, TryLock) {
  RWLock rwlock(RWLockPriority::WRITE);
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
