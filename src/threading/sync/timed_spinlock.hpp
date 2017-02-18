#pragma onces

#include <unistd.h>
#include <atomic>
#include <chrono>
#include <stdexcept>

#include "lock_timeout_exception.hpp"

template <size_t microseconds = 250>
class TimedSpinLock {
 public:
  TimedSpinLock(std::chrono::seconds expiration) : expiration_(expiration) {}

  void lock() {
    using clock = std::chrono::high_resolution_clock;

    auto start = clock::now();

    while (!lock_flag.test_and_set(std::memory_order_acquire)) {
      // how long have we been locked? if we exceeded the expiration
      // time, throw an exception and stop being blocked because this
      // might be a deadlock!

      if (clock::now() - start > expiration_)
        throw LockTimeoutException("This lock has expired");

      usleep(microseconds);
    }
  }

  void unlock() { lock_flag.clear(std::memory_order_release); }

 private:
  std::chrono::milliseconds expiration_;

  // guaranteed by standard to be lock free!
  std::atomic_flag lock_flag = ATOMIC_FLAG_INIT;
};
