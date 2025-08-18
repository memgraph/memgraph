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
  enum class LockReq : uint8_t { READ, WRITE, READ_ONLY };

 private:
  enum states : uint8_t { UNLOCKED, UNIQUE, SHARED };

  // clang-format off
  template <LockReq Req> bool lock_guard_condition() const;
  template <> bool lock_guard_condition<LockReq::WRITE>() const     { return ro_count == 0 && ro_pending_count.load(std::memory_order_acquire) == 0; }
  template <> bool lock_guard_condition<LockReq::READ>() const      { return true; }
  template <> bool lock_guard_condition<LockReq::READ_ONLY>() const { return w_count == 0; }

  template <LockReq Req> void lock_state_updater();
  template <> void lock_state_updater<LockReq::WRITE>()     { ++w_count; };
  template <> void lock_state_updater<LockReq::READ>()      { ++r_count; };
  template <> void lock_state_updater<LockReq::READ_ONLY>() { ++ro_count; }

  template <LockReq Req> void unlock_state_updater();
  template <> void unlock_state_updater<LockReq::WRITE>()     { --w_count; };
  template <> void unlock_state_updater<LockReq::READ>()      { --r_count; };
  template <> void unlock_state_updater<LockReq::READ_ONLY>() { --ro_count; }

  template <LockReq Req> bool unlock_has_fully_unlocked() const;
  template <> bool unlock_has_fully_unlocked<LockReq::WRITE>() const     { return w_count == 0 && r_count == 0; };
  template <> bool unlock_has_fully_unlocked<LockReq::READ>() const      { return r_count == 0 && ro_count == 0 && w_count == 0; };
  template <> bool unlock_has_fully_unlocked<LockReq::READ_ONLY>() const { return ro_count == 0 && r_count == 0; };

  template <LockReq Req> void lock_pre_state_change(){}
  template <> void lock_pre_state_change<LockReq::READ_ONLY>(){ ro_pending_count.fetch_add(1,std::memory_order_acq_rel); }

  template <LockReq Req> void lock_post_state_change(){}
  template <> void lock_post_state_change<LockReq::READ_ONLY>(){ ro_pending_count.fetch_sub(1,std::memory_order_acq_rel); }

  // If upon unlock we could possible unblock another lock then
  // we would want to notify to make sure we rapidly make progress
  // WRITE -> If w_count goes down to 0, READ_ONLY and UNIQUE maybe unblocked, hence: Notify All
  // READ -> If r_count goes down to 0 (and other counts were already 0), UNIQUE maybe unblocked, hence: Notify One
  // READ_ONLY -> If ro_count goes down to 0, WRITE and  UNIQUE maybe unblocked, hence: Notify All
  enum class NotifyKind : uint8_t { None, One, All };
  template <LockReq Req> NotifyKind unlock_should_notify() const;
  template <> NotifyKind unlock_should_notify<LockReq::WRITE>() const { return w_count == 0 ? NotifyKind::All : NotifyKind::None; }
  template <> NotifyKind unlock_should_notify<LockReq::READ>() const { return (r_count == 0 && w_count == 0 && ro_count == 0) ? NotifyKind::One : NotifyKind::None; }
  template <> NotifyKind unlock_should_notify<LockReq::READ_ONLY>() const { return ro_count == 0 ? NotifyKind::All : NotifyKind::None; }

  // A READ_ONLY lock request can block a WRITE lock request, on failure we should notify if ro_pending_count is now 0
  template <LockReq Req> NotifyKind lock_failed_wait_should_notify() const;
  template <> NotifyKind lock_failed_wait_should_notify<LockReq::WRITE>() const { return NotifyKind::None; }
  template <> NotifyKind lock_failed_wait_should_notify<LockReq::READ>() const { return NotifyKind::None; }
  template <> NotifyKind lock_failed_wait_should_notify<LockReq::READ_ONLY>() const { return ro_pending_count.load(std::memory_order_acquire) == 0 ? NotifyKind::All : NotifyKind::None; }

  // clang-format on

  template <LockReq Req>
  void maybe_notify(std::unique_lock<std::mutex> &lock, NotifyKind kind) {
    lock.unlock();
    switch (kind) {
      case NotifyKind::One:
        cv.notify_one();
        break;
      case NotifyKind::All:
        cv.notify_all();
        break;
      case NotifyKind::None:
        break;
    }
  }

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
    if (!cv.wait_for(lock, timeout_duration, [this] { return state == UNLOCKED; })) {
      return false;
    }
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

  template <LockReq Req = LockReq::WRITE>
  void lock_shared() {
    auto lock = std::unique_lock{mtx};
    lock_pre_state_change<Req>();
    cv.wait(lock, [this] { return state != UNIQUE && lock_guard_condition<Req>(); });
    state = SHARED;
    lock_state_updater<Req>();
    lock_post_state_change<Req>();
  }

  template <LockReq Req = LockReq::WRITE>
  bool try_lock_shared() {
    auto lock = std::unique_lock{mtx};
    if (state != UNIQUE && lock_guard_condition<Req>()) {
      state = SHARED;
      lock_state_updater<Req>();
      return true;
    }
    return false;
  }

  template <LockReq Req>
  bool downgrade_to_read() {
    auto lock = std::unique_lock{mtx};
    if (state != SHARED) return false;
    unlock_state_updater<Req>();
    lock_state_updater<LockReq::READ>();
    maybe_notify<Req>(lock, unlock_should_notify<Req>());
    return true;
  }

  template <typename Rep, typename Period, LockReq Req = LockReq::WRITE>
  bool try_lock_shared_for(std::chrono::duration<Rep, Period> const &time) {
    auto lock = std::unique_lock{mtx};
    lock_pre_state_change<Req>();
    if (!cv.wait_for(lock, time, [this] { return state != UNIQUE && lock_guard_condition<Req>(); })) {
      lock_post_state_change<Req>();
      maybe_notify<Req>(lock, lock_failed_wait_should_notify<Req>());
      return false;
    }
    state = SHARED;
    lock_state_updater<Req>();
    lock_post_state_change<Req>();
    return true;
  }

  template <LockReq Req = LockReq::WRITE>
  void unlock_shared() {
    auto lock = std::unique_lock{mtx};
    unlock_state_updater<Req>();
    if (unlock_has_fully_unlocked<Req>()) {
      state = UNLOCKED;
    }
    maybe_notify<Req>(lock, unlock_should_notify<Req>());
  }

 private:
  std::mutex mtx;
  std::condition_variable cv;
  states state = UNLOCKED;
  uint32_t ro_count = 0;
  std::atomic<uint32_t> ro_pending_count = 0;
  uint32_t w_count = 0;
  uint32_t r_count = 0;
};

struct SharedResourceLockGuard {
 public:
  enum Type { WRITE, READ, READ_ONLY };
  SharedResourceLockGuard(ResourceLock &l, Type type) : ptr_{&l}, type_{type} { lock(); }
  SharedResourceLockGuard(ResourceLock &l, Type type, std::defer_lock_t /*tag*/) : ptr_{&l}, type_{type} {}
  SharedResourceLockGuard(ResourceLock &l, Type type, std::try_to_lock_t /*tag*/) : ptr_{&l}, type_{type} {
    try_lock();
  }

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
          ptr_->lock_shared<ResourceLock::LockReq::WRITE>();
          break;
        case READ:
          ptr_->lock_shared<ResourceLock::LockReq::READ>();
          break;
        case READ_ONLY:
          ptr_->lock_shared<ResourceLock::LockReq::READ_ONLY>();
          break;
      }
      locked_ = true;
    }
  }

  bool try_lock() {
    if (ptr_ && !locked_) {
      switch (type_) {
        case WRITE:
          locked_ = ptr_->try_lock_shared<ResourceLock::LockReq::WRITE>();
          break;
        case READ:
          locked_ = ptr_->try_lock_shared<ResourceLock::LockReq::READ>();
          break;
        case READ_ONLY:
          locked_ = ptr_->try_lock_shared<ResourceLock::LockReq::READ_ONLY>();
          ;
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
          locked_ = ptr_->try_lock_shared_for<Rep, Period, ResourceLock::LockReq::WRITE>(time);
          break;
        case READ:
          locked_ = ptr_->try_lock_shared_for<Rep, Period, ResourceLock::LockReq::READ>(time);
          break;
        case READ_ONLY:
          locked_ = ptr_->try_lock_shared_for<Rep, Period, ResourceLock::LockReq::READ_ONLY>(time);
          break;
      }
    }
    return locked_;
  }

  void unlock() {
    if (ptr_ && locked_) {
      switch (type_) {
        case WRITE:
          ptr_->unlock_shared<ResourceLock::LockReq::WRITE>();
          break;
        case READ:
          ptr_->unlock_shared<ResourceLock::LockReq::READ>();
          break;
        case READ_ONLY:
          ptr_->unlock_shared<ResourceLock::LockReq::READ_ONLY>();
          break;
      }
      locked_ = false;
    }
  }

  bool downgrade_to_read() {
    if (ptr_ && locked_) {
      switch (type_) {
        case WRITE: {
          auto res = ptr_->downgrade_to_read<ResourceLock::LockReq::WRITE>();
          if (res) type_ = READ;
          return res;
        }
        case READ:
          return true;  // can't downgrade from read to read
        case READ_ONLY: {
          auto res = ptr_->downgrade_to_read<ResourceLock::LockReq::READ_ONLY>();
          if (res) type_ = READ;
          return res;
        }
      }
    }
    return false;
  }

  bool owns_lock() const { return ptr_ && locked_; }

  Type type() const { return type_; }

 private:
  ResourceLock *ptr_;
  Type type_;
  bool locked_{false};
};

}  // namespace memgraph::utils
