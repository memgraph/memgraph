/// @file
#pragma once

#include <mutex>

#include "utils/exceptions.hpp"
#include "utils/spin_lock.hpp"

namespace utils {

/// Improves contention in spinlocks by hinting the processor that we're in a
/// spinlock and not doing much.
inline void CpuRelax() {
  // if IBMPower
  // HMT_very_low()
  // http://stackoverflow.com/questions/5425506/equivalent-of-x86-pause-instruction-for-ppc
  asm("PAUSE");
}

class LockTimeoutException : public BasicException {
 public:
  using BasicException::BasicException;
};

/// Lockable is used as an custom implementation of a mutex mechanism.
///
/// It is implemented as a wrapper around std::lock_guard and std::unique_guard
/// with a default lock called Spinlock.
///
/// @tparam lock_t type of lock to be used (default = Spinlock)
template <class lock_t = SpinLock>
class Lockable {
 public:
  using lock_type = lock_t;

  std::lock_guard<lock_t> acquire_guard() const {
    return std::lock_guard<lock_t>(lock);
  }

  std::unique_lock<lock_t> acquire_unique() const {
    return std::unique_lock<lock_t>(lock);
  }

  mutable lock_t lock;
};

}  // namespace utils
