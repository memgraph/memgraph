#pragma once

#include <unistd.h>
#include <atomic>

#include "threading/sync/cpu_relax.hpp"

/**
 * @class SpinLock
 *
 * @brief
 * Spinlock is used as an locking mechanism based on an atomic flag and
 * waiting loops. It uses the cpu_relax "asm pause" command to optimize wasted
 * time while the threads are waiting.
 *
 */
class SpinLock {
 public:
  void lock() {  // Before was memory_order_acquire
    while (lock_flag.test_and_set(std::memory_order_seq_cst)) cpu_relax();
  }
  // Before was memory_order_release
  void unlock() { lock_flag.clear(std::memory_order_seq_cst); }

 private:
  // guaranteed by standard to be lock free!
  mutable std::atomic_flag lock_flag = ATOMIC_FLAG_INIT;
};
