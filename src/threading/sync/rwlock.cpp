#include "threading/sync/rwlock.hpp"

namespace threading {

RWLock::RWLock(RWLockPriority priority) {
  int err;
  pthread_rwlockattr_t attr;

  err = pthread_rwlockattr_init(&attr);
  if (err != 0) {
    throw std::system_error(err, std::system_category());
  }

  switch (priority) {
    case RWLockPriority::READ:
      pthread_rwlockattr_setkind_np(&attr, PTHREAD_RWLOCK_PREFER_READER_NP);
      break;
    case RWLockPriority::WRITE:
      /* There is also `PTHREAD_RWLOCK_PREFER_WRITER_NP` but it is not
       * providing the desired behavior.
       *
       * From `man 7 pthread_rwlockattr_setkind_np`:
       * "Setting the value read-write lock kind to
       * PTHREAD_RWLOCK_PREFER_WRITER_NP results in the same behavior as
       * setting the value to PTHREAD_RWLOCK_PREFER_READER_NP. As long as a
       * reader thread holds the lock, the thread holding a write lock will be
       * starved. Setting the lock kind to
       * PTHREAD_RWLOCK_PREFER_WRITER_NONRECURSIVE_NP allows writers to run,
       * but, as the name implies a writer may not lock recursively."
       *
       * For this reason, `RWLock` should not be used recursively.
       * */
      pthread_rwlockattr_setkind_np(
          &attr, PTHREAD_RWLOCK_PREFER_WRITER_NONRECURSIVE_NP);
      break;
  }

  err = pthread_rwlock_init(&lock_, &attr);
  pthread_rwlockattr_destroy(&attr);

  if (err != 0) {
    throw std::system_error(err, std::system_category());
  }
}

RWLock::~RWLock() { pthread_rwlock_destroy(&lock_); }

void RWLock::lock() {
  int err = pthread_rwlock_wrlock(&lock_);
  if (err != 0) {
    throw std::system_error(err, std::system_category());
  }
}

bool RWLock::try_lock() {
  int err = pthread_rwlock_trywrlock(&lock_);
  if (err == 0) return true;
  if (err == EBUSY) return false;
  throw std::system_error(err, std::system_category());
}

void RWLock::unlock() {
  int err = pthread_rwlock_unlock(&lock_);
  if (err != 0) {
    throw std::system_error(err, std::system_category());
  }
}

void RWLock::lock_shared() {
  int err = pthread_rwlock_rdlock(&lock_);
  if (err != 0) {
    throw std::system_error(err, std::system_category());
  }
}

bool RWLock::try_lock_shared() {
  int err = pthread_rwlock_tryrdlock(&lock_);
  if (err == 0) return true;
  if (err == EBUSY) return false;
  throw std::system_error(err, std::system_category());
}

void RWLock::unlock_shared() {
  int err = pthread_rwlock_unlock(&lock_);
  if (err != 0) {
    throw std::system_error(err, std::system_category());
  }
}

}  // namespace threading
