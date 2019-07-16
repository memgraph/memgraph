/// @file
#pragma once

#include <pthread.h>
#include <unistd.h>

#include <cerrno>

#include <glog/logging.h>

namespace utils {

/// A wrapper around `pthread_rwlock_t`, useful because it is not possible to
/// choose read or write priority for `std::shared_mutex`.
class RWLock {
 public:
  /// By passing the appropriate parameter to the `RWLock` constructor, it is
  /// possible to control the behavior of `RWLock` while shared lock is held. If
  /// the priority is set to `READ`, new shared (read) locks can be obtained
  /// even though there is a thread waiting for an exclusive (write) lock, which
  /// can lead to writer starvation. If the priority is set to `WRITE`, readers
  /// will be blocked from obtaining new shared locks while there are writers
  /// waiting, which can lead to reader starvation.
  enum class Priority { READ, WRITE };

  /// Construct a RWLock object with chosen priority. See comment above
  /// `RWLockPriority` for details.
  explicit RWLock(Priority priority) {
    pthread_rwlockattr_t attr;

    CHECK(pthread_rwlockattr_init(&attr) == 0)
        << "Couldn't initialize utils::RWLock!";

    switch (priority) {
      case Priority::READ:
        pthread_rwlockattr_setkind_np(&attr, PTHREAD_RWLOCK_PREFER_READER_NP);
        break;
      case Priority::WRITE:
        // There is also `PTHREAD_RWLOCK_PREFER_WRITER_NP` but it is not
        // providing the desired behavior.
        //
        // From `man 7 pthread_rwlockattr_setkind_np`:
        // "Setting the value read-write lock kind to
        // PTHREAD_RWLOCK_PREFER_WRITER_NP results in the same behavior as
        // setting the value to PTHREAD_RWLOCK_PREFER_READER_NP. As long as a
        // reader thread holds the lock, the thread holding a write lock will be
        // starved. Setting the lock kind to
        // PTHREAD_RWLOCK_PREFER_WRITER_NONRECURSIVE_NP allows writers to run,
        // but, as the name implies a writer may not lock recursively."
        //
        // For this reason, `RWLock` should not be used recursively.
        pthread_rwlockattr_setkind_np(
            &attr, PTHREAD_RWLOCK_PREFER_WRITER_NONRECURSIVE_NP);
        break;
    }

    CHECK(pthread_rwlock_init(&lock_, &attr) == 0)
        << "Couldn't initialize utils::RWLock!";
    pthread_rwlockattr_destroy(&attr);
  }

  RWLock(const RWLock &) = delete;
  RWLock &operator=(const RWLock &) = delete;
  RWLock(RWLock &&) = delete;
  RWLock &operator=(RWLock &&) = delete;

  ~RWLock() { pthread_rwlock_destroy(&lock_); }

  void lock() {
    CHECK(pthread_rwlock_wrlock(&lock_) == 0) << "Couldn't lock utils::RWLock!";
  }

  bool try_lock() {
    int err = pthread_rwlock_trywrlock(&lock_);
    if (err == 0) return true;
    CHECK(err == EBUSY) << "Couldn't try lock utils::RWLock!";
    return false;
  }

  void unlock() {
    CHECK(pthread_rwlock_unlock(&lock_) == 0)
        << "Couldn't unlock utils::RWLock!";
  }

  void lock_shared() {
    int err;
    while (true) {
      err = pthread_rwlock_rdlock(&lock_);
      if (err == 0) {
        return;
      } else if (err == EAGAIN) {
        continue;
      } else {
        LOG(FATAL) << "Couldn't lock shared utils::RWLock!";
      }
    }
  }

  bool try_lock_shared() {
    int err;
    while (true) {
      err = pthread_rwlock_tryrdlock(&lock_);
      if (err == 0) {
        return true;
      } else if (err == EBUSY) {
        return false;
      } else if (err == EAGAIN) {
        continue;
      } else {
        LOG(FATAL) << "Couldn't try lock shared utils::RWLock!";
      }
    }
  }

  void unlock_shared() {
    CHECK(pthread_rwlock_unlock(&lock_) == 0)
        << "Couldn't unlock shared utils::RWLock!";
  }

 private:
  pthread_rwlock_t lock_ = PTHREAD_RWLOCK_INITIALIZER;
};

}  // namespace utils
