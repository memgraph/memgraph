// Copyright 2024 Memgraph Ltd.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt; by using this file, you agree to be bound by the terms of the Business Source
// License, and you may not use this file except in compliance with the Business Source License.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.
#pragma once

#include <condition_variable>
#include <cstdint>
#include <mutex>

namespace memgraph::utils {

/* A type that complies to the
 * C++ named requirements `SharedLockable`
 *
 * Unlike `std::shared_mutex` it can be locked in one thread
 * and unlocked in another.
 */
struct ResourceLock {
 private:
  enum states { UNLOCKED, UNIQUE, SHARED };

 public:
  void lock() {
    auto lock = std::unique_lock{mtx};
    // block until available
    cv.wait(lock, [this] { return state == UNLOCKED; });
    state = UNIQUE;
  }
  void lock_shared() {
    auto lock = std::unique_lock{mtx};
    // block until available
    cv.wait(lock, [this] { return state != UNIQUE; });
    state = SHARED;
    ++count;
  }
  bool try_lock() {
    auto lock = std::unique_lock{mtx};
    if (state == UNLOCKED) {
      state = UNIQUE;
      return true;
    }
    return false;
  }

  template <class Rep, class Period>
  bool try_lock_for(const std::chrono::duration<Rep, Period> &timeout_duration) {
    auto lock = std::unique_lock{mtx};
    if (!cv.wait_for(lock, timeout_duration, [this] { return state == UNLOCKED; })) return false;
    state = UNIQUE;
    return true;
  }

  bool try_lock_shared() {
    auto lock = std::unique_lock{mtx};
    if (state != UNIQUE) {
      state = SHARED;
      ++count;
      return true;
    }
    return false;
  }

  template <typename Rep, typename Period>
  bool try_lock_shared_for(std::chrono::duration<Rep, Period> const &time) {
    auto lock = std::unique_lock{mtx};
    // block until available
    if (!cv.wait_for(lock, time, [this] { return state != UNIQUE; })) return false;
    state = SHARED;
    ++count;
    return true;
  }

  void unlock() {
    auto lock = std::unique_lock{mtx};
    state = UNLOCKED;
    cv.notify_all();  // multiple lock_shared maybe waiting
  }
  void unlock_shared() {
    auto lock = std::unique_lock{mtx};
    --count;
    if (count == 0) {
      state = UNLOCKED;
      cv.notify_one();  // should be 0 waiting in lock_shared, only 1 wait in lock can progress
    }
  }

  void upgrade_to_unique() {
    auto lock = std::unique_lock{mtx};
    cv.wait(lock, [this] { return count == 1; });
    state = UNIQUE;
    count = 0;
  }

  template <class Rep, class Period>
  bool try_upgrade_to_unique(const std::chrono::duration<Rep, Period> &timeout_duration) {
    auto lock = std::unique_lock{mtx};
    if (!cv.wait_for(lock, timeout_duration, [this] { return count == 1; })) return false;
    state = UNIQUE;
    count = 0;
    return true;
  }

 private:
  std::mutex mtx;
  std::condition_variable cv;
  states state = UNLOCKED;
  uint64_t count = 0;
};

struct ResourceLockGuard {
 private:
  enum states { UNIQUE, SHARED };

 public:
  explicit ResourceLockGuard(ResourceLock &thing)
      : ptr{&thing}, state{[this]() {
          ptr->lock_shared();
          return SHARED;
        }()} {}

  void upgrade_to_unique() {
    if (state == SHARED) {
      ptr->upgrade_to_unique();
      state = UNIQUE;
    }
  }

  template <class Rep, class Period>
  bool try_upgrade_to_unique(const std::chrono::duration<Rep, Period> &timeout_duration) {
    if (state != SHARED) return true;                                 // already locked
    if (!ptr->try_upgrade_to_unique(timeout_duration)) return false;  // timeout
    state = UNIQUE;
    return true;
  }

  ~ResourceLockGuard() {
    switch (state) {
      case UNIQUE:
        ptr->unlock();
        break;
      case SHARED:
        ptr->unlock_shared();
        break;
    }
  }

 private:
  ResourceLock *ptr;
  states state;
};

}  // namespace memgraph::utils
