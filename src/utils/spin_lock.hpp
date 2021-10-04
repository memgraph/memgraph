// Copyright 2021 Memgraph Ltd.
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

#include <pthread.h>

#include "utils/logging.hpp"

namespace utils {

/// This class is a wrapper around the `pthread_spinlock_t`. It provides a
/// generic spin lock. The lock should be used in cases where you know that the
/// lock will be contended only for short periods of time. This lock doesn't
/// make any kernel calls (like sleep, or context switching) during its wait for
/// the lock to be acquired. This property is only useful when the lock will be
/// held for short periods of time and you don't want to introduce the extra
/// delays of a sleep or context switch. On the assembly level
/// `pthread_spinlock_t` is optimized to use less power, reduce branch
/// mispredictions, etc... The explanation can be seen here:
/// https://stackoverflow.com/questions/26583433/c11-implementation-of-spinlock-using-atomic/29195378#29195378
/// https://software.intel.com/en-us/node/524249
class SpinLock {
 public:
  SpinLock() {
    // `pthread_spin_init` returns -1 only when there isn't enough memory to
    // initialize the lock. That should never occur because the
    // `pthread_spinlock_t` is an `int` and memory isn't allocated by this init.
    // The message is probably here to suit all other platforms...
    MG_ASSERT(pthread_spin_init(&lock_, PTHREAD_PROCESS_PRIVATE) == 0, "Couldn't construct utils::SpinLock!");
  }

  SpinLock(SpinLock &&other) noexcept : lock_(other.lock_) {
    MG_ASSERT(pthread_spin_init(&other.lock_, PTHREAD_PROCESS_PRIVATE) == 0, "Couldn't construct utils::SpinLock!");
  }

  SpinLock &operator=(SpinLock &&other) noexcept {
    MG_ASSERT(pthread_spin_destroy(&lock_) == 0, "Couldn't destruct utils::SpinLock!");
    lock_ = other.lock_;
    MG_ASSERT(pthread_spin_init(&other.lock_, PTHREAD_PROCESS_PRIVATE) == 0, "Couldn't construct utils::SpinLock!");
    return *this;
  }

  SpinLock(const SpinLock &) = delete;
  SpinLock &operator=(const SpinLock &) = delete;

  ~SpinLock() { MG_ASSERT(pthread_spin_destroy(&lock_) == 0, "Couldn't destruct utils::SpinLock!"); }

  void lock() {
    // `pthread_spin_lock` returns -1 only when there is a deadlock detected
    // (errno EDEADLOCK).
    MG_ASSERT(pthread_spin_lock(&lock_) == 0, "Couldn't lock utils::SpinLock!");
  }

  bool try_lock() {
    // `pthread_spin_trylock` returns -1 only when the lock is already locked
    // (errno EBUSY).
    return pthread_spin_trylock(&lock_) == 0;
  }

  void unlock() {
    // `pthread_spin_unlock` has no documented error codes that it could return,
    // so any error is a fatal error.
    MG_ASSERT(pthread_spin_unlock(&lock_) == 0, "Couldn't unlock utils::SpinLock!");
  }

 private:
  pthread_spinlock_t lock_;
};
}  // namespace utils
