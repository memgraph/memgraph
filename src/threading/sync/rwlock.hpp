/// @file
#pragma once

#include <pthread.h>

#include "glog/logging.h"

namespace threading {

/// By passing the appropriate parameter to the `RWLock` constructor, it is
/// possible to control the behavior of `RWLock` while shared lock is held. If
/// the priority is set to `READ`, new shared (read) locks can be obtained even
/// though there is a thread waiting for an exclusive (write) lock, which can
/// lead to writer starvation. If the priority is set to `WRITE`, readers will
/// be blocked from obtaining new shared locks while there are writers waiting,
/// which can lead to reader starvation.
enum RWLockPriority { READ, WRITE };

/// A wrapper around `pthread_rwlock_t`, useful because it is not possible to
/// choose read or write priority for `std::shared_mutex`.
class RWLock {
 public:
  RWLock(const RWLock &) = delete;
  RWLock &operator=(const RWLock &) = delete;
  RWLock(RWLock &&) = delete;
  RWLock &operator=(RWLock &&) = delete;

  /// Construct a RWLock object with chosen priority. See comment above
  /// `RWLockPriority` for details.
  explicit RWLock(RWLockPriority priority);

  ~RWLock();

  bool try_lock();
  void lock();
  void unlock();

  bool try_lock_shared();
  void lock_shared();
  void unlock_shared();

 private:
  pthread_rwlock_t lock_ = PTHREAD_RWLOCK_INITIALIZER;
};

}  // namespace threading
