/// @file
#pragma once

#include <pthread.h>
#include <unistd.h>

#include <atomic>
#include <cerrno>
#include <chrono>
#include <cstdint>
#include <mutex>

#include "utils/exceptions.hpp"

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

class CasLock {
 public:
  void lock() {
    bool locked = false;

    while (!lock_flag.compare_exchange_weak(
        locked, true, std::memory_order_release, std::memory_order_relaxed)) {
      usleep(250);
    }
  }

  void unlock() { lock_flag.store(0, std::memory_order_release); }

  bool locked() { return lock_flag.load(std::memory_order_relaxed); }

 private:
  std::atomic<bool> lock_flag;
};

/// Spinlock is used as a locking mechanism based on an atomic flag and waiting
/// loops.
///
/// It uses the CpuRelax "asm pause" command to optimize wasted time while the
/// threads are waiting.
class SpinLock {
 public:
  void lock() {  // Before was memory_order_acquire
    while (lock_flag_.test_and_set()) {
      CpuRelax();
    }
  }
  // Before was memory_order_release
  void unlock() { lock_flag_.clear(); }

  bool try_lock() { return !lock_flag_.test_and_set(); }

 private:
  // guaranteed by standard to be lock free!
  mutable std::atomic_flag lock_flag_ = ATOMIC_FLAG_INIT;
};

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

class Futex {
  using futex_t = uint32_t;
  using flag_t = uint8_t;

  /// Data structure for implementing fast mutexes
  ///
  /// This structure is 4B wide, as required for futex system call where
  /// the last two bytes are used for two flags - contended and locked,
  /// respectively. Memory layout for the structure looks like this:
  ///
  ///                 all
  /// |---------------------------------|
  /// 00000000 00000000 0000000C 0000000L
  ///                   |------| |------|
  ///                  contended  locked
  ///
  /// L marks the locked bit
  /// C marks the contended bit
  union mutex_t {
    std::atomic<futex_t> all{0};

    struct {
      std::atomic<flag_t> locked;
      std::atomic<flag_t> contended;
    } state;
  };

  enum Contention : futex_t { UNCONTENDED = 0x0000, CONTENDED = 0x0100 };

  enum State : futex_t {
    UNLOCKED = 0x0000,
    LOCKED = 0x0001,
    UNLOCKED_CONTENDED = UNLOCKED | CONTENDED,  // 0x0100
    LOCKED_CONTENDED = LOCKED | CONTENDED       // 0x0101
  };

  static constexpr size_t LOCK_RETRIES = 100;
  static constexpr size_t UNLOCK_RETRIES = 200;

 public:
  Futex() {
    static_assert(sizeof(mutex_t) == sizeof(futex_t),
                  "Atomic futex should be the same size as non_atomic");
  }

  bool try_lock() {
    // we took the lock if we stored the LOCKED state and previous
    // state was UNLOCKED
    return mutex.state.locked.exchange(LOCKED, std::memory_order_seq_cst) ==
           UNLOCKED;
  }

  void lock(const struct timespec *timeout = nullptr);

  void unlock();

  bool is_locked(std::memory_order order = std::memory_order_seq_cst) const {
    return mutex.state.locked.load(order);
  }

  bool is_contended(std::memory_order order = std::memory_order_seq_cst) const {
    return mutex.state.contended.load(order);
  }

 private:
  mutex_t mutex;

  int futex_wait(int value, const struct timespec *timeout = nullptr);

  void futex_wake(int value);

  void relax() { CpuRelax(); }
};

}  // namespace utils
