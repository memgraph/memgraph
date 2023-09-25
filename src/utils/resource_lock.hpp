// Copyright 2023 Memgraph Ltd.
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
  bool try_lock_shared() {
    auto lock = std::unique_lock{mtx};
    if (state != UNIQUE) {
      state = SHARED;
      ++count;
      return true;
    }
    return false;
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

 private:
  std::mutex mtx;
  std::condition_variable cv;
  states state = UNLOCKED;
  uint64_t count = 0;
};

}  // namespace memgraph::utils
