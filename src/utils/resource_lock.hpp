// Copyright 2025 Memgraph Ltd.
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
#include <utility>

namespace memgraph::utils {

/* A type that complies to the
 * C++ named requirements `SharedLockable`
 *
 * Unlike `std::shared_mutex` it can be locked in one thread
 * and unlocked in another.
 */

/// Priority is given to read-only locks over read and write locks
struct ResourceLock {
 private:
  enum states : uint8_t { UNLOCKED, UNIQUE, SHARED };

 public:
  void lock() {
    auto lock = std::unique_lock{mtx};
    // block until available
    cv.wait(lock, [this] { return state == UNLOCKED; });
    state = UNIQUE;
  }

  bool try_lock() {
    auto lock = std::unique_lock{mtx};
    if (state == UNLOCKED) {
      state = UNIQUE;
      return true;
    }
    return false;
  }

  template <class Rep, class Period>
  bool try_lock_for(const std::chrono::duration<Rep, Period> &timeout_duration) {
    auto lock = std::unique_lock{mtx};
    if (!cv.wait_for(lock, timeout_duration, [this] { return state == UNLOCKED; })) return false;
    state = UNIQUE;
    return true;
  }

  void unlock() {
    {
      auto lock = std::unique_lock{mtx};
      state = UNLOCKED;
    }
    cv.notify_all();  // multiple lock_shared maybe waiting
  }

  template <bool Read = false>
  void lock_shared() {
    auto lock = std::unique_lock{mtx};
    // block until available
    cv.wait(lock, [this] { return state != UNIQUE && ro_count == 0 && ro_pending_count == 0; });
    state = SHARED;
    ++w_count;
  }

  template <>
  void lock_shared<true>() {
    auto lock = std::unique_lock{mtx};
    // block until available
    cv.wait(lock, [this] { return state != UNIQUE && ro_pending_count == 0; });
    state = SHARED;
    ++r_count;
  }

  template <bool Read = false>
  bool try_lock_shared() {
    auto lock = std::unique_lock{mtx};
    if (state != UNIQUE && ro_count == 0 && ro_pending_count == 0) {
      state = SHARED;
      ++w_count;
      return true;
    }
    return false;
  }

  template <>
  bool try_lock_shared<true>() {
    auto lock = std::unique_lock{mtx};
    if (state != UNIQUE && ro_count == 0 && ro_pending_count == 0) {
      state = SHARED;
      ++r_count;
      return true;
    }
    return false;
  }

  template <typename Rep, typename Period, bool Read = false>
    requires(!Read)
  bool try_lock_shared_for(std::chrono::duration<Rep, Period> const &time) {
    auto lock = std::unique_lock{mtx};
    // block until available
    if (!cv.wait_for(lock, time, [this] { return state != UNIQUE && ro_count == 0 && ro_pending_count == 0; }))
      return false;
    state = SHARED;
    ++w_count;
    return true;
  }

  template <typename Rep, typename Period, bool Read>
    requires(Read)
  bool try_lock_shared_for(std::chrono::duration<Rep, Period> const &time) {
    auto lock = std::unique_lock{mtx};
    // block until available
    if (!cv.wait_for(lock, time, [this] { return state != UNIQUE && ro_count == 0 && ro_pending_count == 0; }))
      return false;
    state = SHARED;
    ++r_count;
    return true;
  }

  template <bool Read = false>
  void unlock_shared() {
    auto lock = std::unique_lock{mtx};
    --w_count;
    if (w_count + r_count == 0) {
      state = UNLOCKED;
      lock.unlock();
      cv.notify_all();  // Can have multiple read only waiting
    }
  }

  template <>
  void unlock_shared<true>() {
    auto lock = std::unique_lock{mtx};
    --r_count;
    if (r_count == 0 && ro_count == 0 && w_count == 0) {
      state = UNLOCKED;
      lock.unlock();
      cv.notify_one();  // Should only have unique locks waiting
    }
  }

  void lock_read_only() {
    auto lock = std::unique_lock{mtx};
    ro_pending_count++;
    // block until available
    cv.wait(lock, [this] { return state != UNIQUE && w_count == 0; });
    ro_pending_count--;
    state = SHARED;
    ++ro_count;
  }

  bool try_lock_read_only() {
    auto lock = std::unique_lock{mtx};
    if (state != UNIQUE && w_count == 0) {
      state = SHARED;
      ++ro_count;
      return true;
    }
    return false;
  }

  template <typename Rep, typename Period>
  bool try_lock_read_only_for(std::chrono::duration<Rep, Period> const &time) {
    auto lock = std::unique_lock{mtx};
    ro_pending_count++;
    // block until available
    if (!cv.wait_for(lock, time, [this] { return state != UNIQUE && w_count == 0; })) return false;
    ro_pending_count--;
    state = SHARED;
    ++ro_count;
    return true;
  }

  void unlock_read_only() {
    auto lock = std::unique_lock{mtx};
    --ro_count;
    if (ro_count == 0) {
      if (r_count == 0) state = UNLOCKED;
      lock.unlock();
      cv.notify_all();  // allow shared writes to lock
    }
  }

 private:
  std::mutex mtx;
  std::condition_variable cv;
  states state = UNLOCKED;
  uint32_t ro_count = 0;
  uint32_t ro_pending_count = 0;
  uint32_t w_count = 0;
  uint32_t r_count = 0;
};

struct SharedResourceLockGuard {
 public:
  enum Type { WRITE, READ, READ_ONLY };
  SharedResourceLockGuard(ResourceLock &l, Type type) : ptr_{&l}, type_{type} { lock(); }
  SharedResourceLockGuard(ResourceLock &l, Type type, std::defer_lock_t /*tag*/) : ptr_{&l}, type_{type} {}

  ~SharedResourceLockGuard() { unlock(); }

  SharedResourceLockGuard(const SharedResourceLockGuard &) = delete;
  SharedResourceLockGuard &operator=(const SharedResourceLockGuard &) = delete;

  SharedResourceLockGuard(SharedResourceLockGuard &&other) noexcept
      : ptr_{std::exchange(other.ptr_, nullptr)}, type_{other.type_}, locked_{std::exchange(other.locked_, false)} {}
  SharedResourceLockGuard &operator=(SharedResourceLockGuard &&other) noexcept {
    if (this != &other) {
      // First unlock if guard is protecting a resource
      if (owns_lock()) unlock();
      // Then move
      ptr_ = std::exchange(other.ptr_, nullptr);
      type_ = other.type_;
      locked_ = std::exchange(other.locked_, false);
    }
    return *this;
  }

  void lock() {
    if (ptr_ && !locked_) {
      switch (type_) {
        case WRITE:
          ptr_->lock_shared<false>();
          break;
        case READ:
          ptr_->lock_shared<true>();
          break;
        case READ_ONLY:
          ptr_->lock_read_only();
          break;
      }
      locked_ = true;
    }
  }

  bool try_lock() {
    if (ptr_ && !locked_) {
      switch (type_) {
        case WRITE:
          locked_ = ptr_->try_lock_shared<false>();
          break;
        case READ:
          locked_ = ptr_->try_lock_shared<true>();
          break;
        case READ_ONLY:
          locked_ = ptr_->try_lock_read_only();
          break;
      }
    }
    return locked_;
  }

  template <typename Rep, typename Period>
  bool try_lock_for(std::chrono::duration<Rep, Period> const &time) {
    if (ptr_ && !locked_) {
      switch (type_) {
        case WRITE:
          locked_ = ptr_->try_lock_shared_for<Rep, Period, false>(time);
          break;
        case READ:
          locked_ = ptr_->try_lock_shared_for<Rep, Period, true>(time);
          break;
        case READ_ONLY:
          locked_ = ptr_->try_lock_read_only_for(time);
          break;
      }
    }
    return locked_;
  }

  void unlock() {
    if (ptr_ && locked_) {
      switch (type_) {
        case WRITE:
          ptr_->unlock_shared<false>();
          break;
        case READ:
          ptr_->unlock_shared<true>();
          break;
        case READ_ONLY:
          ptr_->unlock_read_only();
          break;
      }
      locked_ = false;
    }
  }

  bool owns_lock() const { return ptr_ && locked_; }

  Type type() const { return type_; }

 private:
  ResourceLock *ptr_;
  Type type_;
  bool locked_{false};
};

}  // namespace memgraph::utils
