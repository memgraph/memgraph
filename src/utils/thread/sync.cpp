#include "utils/thread/sync.hpp"

#include <linux/futex.h>
#include <sys/syscall.h>
#include <sys/types.h>
#include <unistd.h>

namespace sys {
inline int futex(void *addr1, int op, int val1, const struct timespec *timeout,
                 void *addr2, int val3) {
  return syscall(SYS_futex, addr1, op, val1, timeout, addr2, val3);
};

}  // namespace sys

namespace utils {

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

void Futex::lock(const struct timespec *timeout) {
  // try to fast lock a few times before going to sleep
  for (size_t i = 0; i < LOCK_RETRIES; ++i) {
    // try to lock and exit if we succeed
    if (try_lock()) return;

    // we failed, chill a bit
    relax();
  }

  // the lock is contended, go to sleep. when someone
  // wakes you up, try taking the lock again
  while (mutex.all.exchange(LOCKED_CONTENDED, std::memory_order_seq_cst) &
         LOCKED) {
    // wait in the kernel for someone to wake us up when unlocking
    auto status = futex_wait(LOCKED_CONTENDED, timeout);

    // check if we woke up because of a timeout
    if (status == -1 && errno == ETIMEDOUT)
      throw LockTimeoutException("Lock timeout");
  }
}

void Futex::unlock() {
  futex_t state = LOCKED;

  // if we're locked and uncontended, try to unlock the mutex before
  // it becomes contended
  if (mutex.all.load(std::memory_order_seq_cst) == LOCKED &&
      mutex.all.compare_exchange_strong(state, UNLOCKED,
                                        std::memory_order_seq_cst,
                                        std::memory_order_seq_cst))
    return;

  // we are contended, just release the lock
  mutex.state.locked.store(UNLOCKED, std::memory_order_seq_cst);

  // spin and hope someone takes a lock so we don't have to wake up
  // anyone because that's quite expensive
  for (size_t i = 0; i < UNLOCK_RETRIES; ++i) {
    // if someone took the lock, we're ok
    if (is_locked(std::memory_order_seq_cst)) return;

    relax();
  }

  // store that we are becoming uncontended
  mutex.state.contended.store(UNCONTENDED, std::memory_order_seq_cst);

  // we need to wake someone up
  futex_wake(LOCKED);
}

int Futex::futex_wait(int value, const struct timespec *timeout) {
  return sys::futex(&mutex.all, FUTEX_WAIT_PRIVATE, value, timeout, nullptr, 0);
}

void Futex::futex_wake(int value) {
  sys::futex(&mutex.all, FUTEX_WAKE_PRIVATE, value, nullptr, nullptr, 0);
}

}  // namespace utils
