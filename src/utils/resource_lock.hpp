// Copyright 2026 Memgraph Ltd.
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
#include <optional>
#include <utility>

#include "utils/on_scope_exit.hpp"

namespace memgraph::utils {

/* A type that complies to the
 * C++ named requirements `SharedLockable`
 *
 * Unlike `std::shared_mutex` it can be locked in one thread
 * and unlocked in another.
 */

// Forward declarations: RAII helpers (defined below, after SharedResourceLockGuard) that keep a
// pending registration (unique_pending_ / ro_pending_count) alive across a *campaign* of
// non-blocking probes -- see their definitions for the full rationale. They need friend access
// to touch mtx/cv/state/counters directly, the same way the member functions above do.
class UniquePendingScope;
class ReadOnlyPendingScope;

/// Priority is given to read-only locks over read and write locks
struct ResourceLock {
  friend class UniquePendingScope;
  friend class ReadOnlyPendingScope;

  enum class LockReq : uint8_t { READ, WRITE, READ_ONLY };

 private:
  enum states : uint8_t { UNLOCKED, UNIQUE, SHARED };

  // clang-format off
  // A pending (blocked-and-waiting) UNIQUE request also gates new shared acquisitions, mirroring
  // ro_pending_count's priority mechanism for READ_ONLY over WRITE. This gives UNIQUE
  // writer-preference against a continuous stream of shared (READ/WRITE/READ_ONLY) acquirers.
  template <LockReq Req> bool lock_guard_condition() const;
  template <> bool lock_guard_condition<LockReq::WRITE>() const     { return ro_count == 0 && ro_pending_count.load(std::memory_order_acquire) == 0 && unique_pending_.load(std::memory_order_acquire) == 0; }
  template <> bool lock_guard_condition<LockReq::READ>() const      { return unique_pending_.load(std::memory_order_acquire) == 0; }
  template <> bool lock_guard_condition<LockReq::READ_ONLY>() const { return w_count == 0 && unique_pending_.load(std::memory_order_acquire) == 0; }

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
    // Register as a pending UNIQUE waiter *before* blocking: this is checked by the shared
    // acquisition guard conditions (lock_guard_condition<READ|WRITE|READ_ONLY>) so that new shared
    // acquirers yield to a waiting UNIQUE instead of starving it out indefinitely (mirrors
    // ro_pending_count's writer-preference mechanism for READ_ONLY over WRITE).
    unique_pending_.fetch_add(1, std::memory_order_acq_rel);
    bool acquired = false;
    // RAII so the decrement (and, if warranted, the wake-up of shared waiters gated on
    // unique_pending_) happens on every exit path, including an exception thrown out of cv.wait.
    OnScopeExit pending_guard{[this, &acquired, &lock] {
      if (unique_pending_.fetch_sub(1, std::memory_order_acq_rel) == 1 && !acquired) {
        // We were the last pending UNIQUE waiter and did not acquire (only reachable via an
        // exception here, since lock() itself never times out) -- wake shared acquirers that were
        // gated purely on unique_pending_ == 0. Unlock first (matches maybe_notify's style): never
        // notify_all while still holding mtx.
        if (lock.owns_lock()) lock.unlock();
        cv.notify_all();
      }
    }};
    // block until available
    cv.wait(lock, [this] { return state == UNLOCKED; });
    state = UNIQUE;
    acquired = true;
  }

  bool try_lock() {
    auto lock = std::unique_lock{mtx};
    // Non-blocking: never registers as a pending waiter, it never waits so there is nothing to
    // give priority to.
    if (state == UNLOCKED) {
      state = UNIQUE;
      return true;
    }
    return false;
  }

  template <class Rep, class Period>
  bool try_lock_for(const std::chrono::duration<Rep, Period> &timeout_duration) {
    auto lock = std::unique_lock{mtx};
    // See lock() above for why this registers as a pending UNIQUE waiter.
    unique_pending_.fetch_add(1, std::memory_order_acq_rel);
    bool acquired = false;
    OnScopeExit pending_guard{[this, &acquired, &lock] {
      if (unique_pending_.fetch_sub(1, std::memory_order_acq_rel) == 1 && !acquired) {
        // Timed out (or threw) without acquiring: if we were the last pending UNIQUE waiter, wake
        // shared acquirers that were gated purely on unique_pending_ == 0 so they can re-check now.
        if (lock.owns_lock()) lock.unlock();
        cv.notify_all();
      }
    }};
    if (!cv.wait_for(lock, timeout_duration, [this] { return state == UNLOCKED; })) {
      return false;
    }
    state = UNIQUE;
    acquired = true;
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
    // RAII: guarantees lock_post_state_change runs on every exit path (including an exception
    // thrown out of cv.wait), so a pending-counter (e.g. ro_pending_count) can never leak.
    OnScopeExit post_state_change_guard{[this] { lock_post_state_change<Req>(); }};
    cv.wait(lock, [this] { return state != UNIQUE && lock_guard_condition<Req>(); });
    state = SHARED;
    lock_state_updater<Req>();
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
    bool acquired = false;
    // RAII: guarantees lock_post_state_change runs on every exit path (including an exception
    // thrown out of cv.wait_for), so a pending-counter (e.g. ro_pending_count) can never leak.
    // On the non-acquired path (timeout or exception) also fire the same notify the old explicit
    // failure branch used, so anything gated on the pending-counter reaching 0 is woken.
    OnScopeExit post_state_change_guard{[this, &acquired, &lock] {
      lock_post_state_change<Req>();
      if (!acquired) {
        maybe_notify<Req>(lock, lock_failed_wait_should_notify<Req>());
      }
    }};
    if (!cv.wait_for(lock, time, [this] { return state != UNIQUE && lock_guard_condition<Req>(); })) {
      return false;
    }
    state = SHARED;
    lock_state_updater<Req>();
    acquired = true;
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
  // Number of threads currently blocked in lock()/try_lock_for() waiting to acquire UNIQUE.
  // Gates new shared (READ/WRITE/READ_ONLY) acquisitions (see lock_guard_condition) to give a
  // waiting UNIQUE writer-preference against continuous shared load. try_lock() (non-blocking)
  // never touches this counter.
  std::atomic<uint32_t> unique_pending_ = 0;
};

struct SharedResourceLockGuard {
 public:
  enum Type { WRITE, READ, READ_ONLY };

  SharedResourceLockGuard(ResourceLock &l, Type type) : ptr_{&l}, type_{type} { lock(); }

  SharedResourceLockGuard(ResourceLock &l, Type type, std::defer_lock_t /*tag*/) : ptr_{&l}, type_{type} {}

  SharedResourceLockGuard(ResourceLock &l, Type type, std::try_to_lock_t /*tag*/) : ptr_{&l}, type_{type} {
    try_lock();
  }

  /// Adopts a shared lock of `type` on `l` that the caller has *already* acquired (e.g. via
  /// ReadOnlyPendingScope::try_acquire()). Mirrors std::unique_lock's std::adopt_lock_t
  /// constructor: no locking is attempted here, the guard simply takes ownership of an
  /// acquisition that has already happened, so unlock() runs exactly once when this guard is
  /// destroyed or explicitly unlocked.
  SharedResourceLockGuard(ResourceLock &l, Type type, std::adopt_lock_t /*tag*/)
      : ptr_{&l}, type_{type}, locked_{true} {}

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

/// RAII helper that keeps a single caller registered as a pending UNIQUE waiter across a
/// non-blocking *probe campaign*: a caller that cannot afford to block on lock()'s condition
/// variable (e.g. a coroutine that must yield back to a scheduler instead of sleeping) but still
/// wants writer-preference against a continuous stream of new shared (READ/WRITE/READ_ONLY)
/// acquirers while it repeatedly polls try_acquire().
///
/// Why this is needed: a bare try_lock() (see ResourceLock::try_lock()) never touches
/// unique_pending_ -- by design, since a single non-blocking attempt has nothing to give
/// priority to. But a *retrying* caller that loops try_lock() is therefore just another
/// unprivileged contender on every iteration and can starve forever behind back-to-back short
/// shared holders. Holding a UniquePendingScope for the duration of the whole retry loop bumps
/// unique_pending_ once, up front, which gates *new* shared acquirers exactly as a blocking
/// lock() waiter would (see lock_guard_condition<READ|WRITE|READ_ONLY>): in-flight shared
/// holders are left alone to drain, but fresh ones are blocked, so the pool of shared holders
/// shrinks to zero and the retrying UNIQUE probe eventually finds state == UNLOCKED.
///
/// Ownership on success: try_acquire() either returns std::nullopt (still not acquired -- the
/// scope remains registered as pending so the *next* try_acquire() call keeps the same
/// writer-preference gate up) or a std::unique_lock<ResourceLock> that has taken over the
/// acquired UNIQUE lock. On success the scope stops counting as pending immediately -- it now
/// HOLDS the lock, it is no longer merely waiting for one -- and becomes inert: its destructor
/// is a no-op from that point on (ownership, including the eventual unlock(), belongs solely to
/// the returned std::unique_lock, mirroring Accessor::ReleaseUniqueGuard()'s single-owner
/// hand-off). If the scope is destroyed without ever having succeeded, its destructor decrements
/// unique_pending_ and, mirroring lock()'s own pending cleanup, wakes any shared acquirers that
/// were gated purely on unique_pending_ reaching 0.
///
/// Not copyable or movable: the pending registration is tied 1:1 to this scope's lifetime.
class UniquePendingScope {
 public:
  explicit UniquePendingScope(ResourceLock &lock) : lock_{&lock} {
    auto guard = std::unique_lock{lock_->mtx};
    lock_->unique_pending_.fetch_add(1, std::memory_order_acq_rel);
  }

  ~UniquePendingScope() {
    if (lock_ == nullptr) return;  // ownership already transferred out by a successful try_acquire()
    if (lock_->unique_pending_.fetch_sub(1, std::memory_order_acq_rel) == 1) {
      // We were the last pending UNIQUE waiter and never acquired -- wake shared acquirers that
      // were gated purely on unique_pending_ == 0 so they can re-check now (same notify this
      // counter's blocking counterpart, ResourceLock::lock(), performs on its failure path).
      lock_->cv.notify_all();
    }
  }

  UniquePendingScope(const UniquePendingScope &) = delete;
  UniquePendingScope &operator=(const UniquePendingScope &) = delete;
  UniquePendingScope(UniquePendingScope &&) = delete;
  UniquePendingScope &operator=(UniquePendingScope &&) = delete;

  /// Non-blocking attempt to take UNIQUE. Succeeds only when the lock is currently UNLOCKED
  /// (existing UNIQUE/SHARED holders are never preempted -- only new shared acquirers are gated
  /// by this scope's pending registration). Safe to call repeatedly; each failed call leaves the
  /// pending registration intact for the next attempt.
  std::optional<std::unique_lock<ResourceLock>> try_acquire() {
    if (lock_ == nullptr) return std::nullopt;  // already consumed by a prior successful call
    bool acquired = false;
    {
      auto guard = std::unique_lock{lock_->mtx};
      if (lock_->state == ResourceLock::UNLOCKED) {
        lock_->state = ResourceLock::UNIQUE;
        acquired = true;
      }
    }
    if (!acquired) return std::nullopt;
    // Transition pending -> held: decrement the counter (we are no longer *waiting*) but do not
    // notify here -- exactly like ResourceLock::lock()'s success path, waking gated shared
    // acquirers is deferred until this thread eventually calls unlock() (state is still UNIQUE,
    // so there is nothing for them to do yet).
    lock_->unique_pending_.fetch_sub(1, std::memory_order_acq_rel);
    ResourceLock *acquired_lock = std::exchange(lock_, nullptr);
    return std::unique_lock<ResourceLock>{*acquired_lock, std::adopt_lock};
  }

 private:
  ResourceLock *lock_;
};

/// Same shape as UniquePendingScope, but for READ_ONLY: keeps a single caller registered as a
/// pending READ_ONLY waiter (bumping ro_pending_count, ResourceLock's existing mechanism for
/// giving READ_ONLY priority over WRITE) across a non-blocking probe campaign, so a retrying
/// try_acquire() gets the same "new WRITE acquirers are gated" priority a blocking
/// lock_shared<READ_ONLY>() call already gets.
///
/// Ownership on success mirrors UniquePendingScope: try_acquire() returns a
/// SharedResourceLockGuard that has adopted the acquired READ_ONLY lock (see
/// SharedResourceLockGuard's std::adopt_lock_t constructor), the scope stops counting as pending
/// immediately, and becomes inert (destructor is a no-op) -- the returned guard owns the
/// eventual unlock_shared() call. If destroyed without ever succeeding, the destructor
/// decrements ro_pending_count and, if it reached 0, wakes acquirers gated on that (mirrors
/// ResourceLock::lock_failed_wait_should_notify<READ_ONLY>).
class ReadOnlyPendingScope {
 public:
  explicit ReadOnlyPendingScope(ResourceLock &lock) : lock_{&lock} {
    auto guard = std::unique_lock{lock_->mtx};
    lock_->ro_pending_count.fetch_add(1, std::memory_order_acq_rel);
  }

  ~ReadOnlyPendingScope() {
    if (lock_ == nullptr) return;  // ownership already transferred out by a successful try_acquire()
    if (lock_->ro_pending_count.fetch_sub(1, std::memory_order_acq_rel) == 1) {
      lock_->cv.notify_all();
    }
  }

  ReadOnlyPendingScope(const ReadOnlyPendingScope &) = delete;
  ReadOnlyPendingScope &operator=(const ReadOnlyPendingScope &) = delete;
  ReadOnlyPendingScope(ReadOnlyPendingScope &&) = delete;
  ReadOnlyPendingScope &operator=(ReadOnlyPendingScope &&) = delete;

  /// Non-blocking attempt to take READ_ONLY. Needs w_count == 0 && unique_pending_ == 0 (same
  /// condition as ResourceLock::lock_guard_condition<READ_ONLY>): existing WRITE holders drain
  /// naturally, and a pending/held UNIQUE keeps its writer-preference over us too.
  std::optional<SharedResourceLockGuard> try_acquire() {
    if (lock_ == nullptr) return std::nullopt;  // already consumed by a prior successful call
    bool acquired = false;
    {
      auto guard = std::unique_lock{lock_->mtx};
      if (lock_->state != ResourceLock::UNIQUE && lock_->w_count == 0 &&
          lock_->unique_pending_.load(std::memory_order_acquire) == 0) {
        lock_->state = ResourceLock::SHARED;
        ++lock_->ro_count;
        acquired = true;
      }
    }
    if (!acquired) return std::nullopt;
    // Transition pending -> held: decrement without notifying, mirroring UniquePendingScope's
    // success path (nothing to unblock yet -- we now hold the resource ourselves).
    lock_->ro_pending_count.fetch_sub(1, std::memory_order_acq_rel);
    ResourceLock *acquired_lock = std::exchange(lock_, nullptr);
    return SharedResourceLockGuard{*acquired_lock, SharedResourceLockGuard::Type::READ_ONLY, std::adopt_lock};
  }

 private:
  ResourceLock *lock_;
};

}  // namespace memgraph::utils
