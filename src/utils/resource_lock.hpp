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

#include "utils/logging.hpp"
#include "utils/on_scope_exit.hpp"

namespace memgraph::utils {

/* A type that complies to the
 * C++ named requirements `SharedLockable`
 *
 * Unlike `std::shared_mutex` it can be locked in one thread
 * and unlocked in another.
 */

// RAII helpers (defined below) that keep a pending registration alive across a campaign of
// non-blocking probes; need friend access to the lock internals. See their definitions.
class UniquePendingScope;
class ReadOnlyPendingScope;

/// Priority is given to read-only locks over read and write locks
struct ResourceLock {
  friend class UniquePendingScope;
  friend class ReadOnlyPendingScope;

  // The four ways to hold the lock. UNIQUE is exclusive of all of them, including itself; the other
  // three are the shared modes, mutually compatible except that WRITE and READ_ONLY exclude each
  // other. Every acquisition and every release dispatches on this.
  enum class LockReq : uint8_t { READ, WRITE, READ_ONLY, UNIQUE };

 private:
  enum states : uint8_t { UNLOCKED, UNIQUE, SHARED };

  // Notifying one is never correct here, hence no such alternative: all waiters share one condition
  // variable but not one predicate, so notify_one can hand the wake-up to a waiter whose predicate
  // is still false (a shared acquirer gated on unique_pending_count, say); it re-checks, sleeps
  // again, and the waiter that could have made progress is never woken.
  enum class NotifyKind : uint8_t { None, All };

  // clang-format off
  // Admission rule, evaluated under mtx (as the wait predicate, and by the non-blocking probes).
  // A waiting UNIQUE (unique_pending_count) gates new shared acquisitions (writer-preference),
  // mirroring ro_pending_count's READ_ONLY-over-WRITE priority.
  template <LockReq Req> bool can_acquire() const;
  template <> bool can_acquire<LockReq::WRITE>() const      { return state != UNIQUE && ro_count == 0 && ro_pending_count == 0 && unique_pending_count == 0; }
  template <> bool can_acquire<LockReq::READ>() const       { return state != UNIQUE && unique_pending_count == 0; }
  template <> bool can_acquire<LockReq::READ_ONLY>() const  { return state != UNIQUE && w_count == 0 && unique_pending_count == 0; }
  template <> bool can_acquire<LockReq::UNIQUE>() const     { return state == UNLOCKED; }

  template <LockReq Req> void lock_state_updater();
  template <> void lock_state_updater<LockReq::WRITE>()     { ++w_count; }
  template <> void lock_state_updater<LockReq::READ>()      { ++r_count; }
  template <> void lock_state_updater<LockReq::READ_ONLY>() { ++ro_count; }
  template <> void lock_state_updater<LockReq::UNIQUE>()    { }

  // Applied once admitted; leaves the lock in the state the acquisition claims.
  template <LockReq Req> void commit_state()             { state = SHARED; lock_state_updater<Req>(); }
  template <> void commit_state<LockReq::UNIQUE>()       { state = UNIQUE; }

  // Pending registration, held for as long as a caller is waiting to acquire, so the kinds it gates
  // yield to it (see can_acquire). READ and WRITE gate nobody, so they register nothing.
  template <LockReq Req> void pending_add() {}
  template <> void pending_add<LockReq::READ_ONLY>() { ++ro_pending_count; }
  template <> void pending_add<LockReq::UNIQUE>()    { ++unique_pending_count; }

  template <LockReq Req> void pending_sub() {}
  template <> void pending_sub<LockReq::READ_ONLY>() { --ro_pending_count; }
  template <> void pending_sub<LockReq::UNIQUE>()    { --unique_pending_count; }

  // Giving up without acquiring can be the event that ungates whoever we were holding back, and
  // nothing else will wake them: their predicate came true because we deregistered.
  template <LockReq Req> NotifyKind pending_release_should_notify() const { return NotifyKind::None; }
  template <> NotifyKind pending_release_should_notify<LockReq::READ_ONLY>() const { return ro_pending_count == 0 ? NotifyKind::All : NotifyKind::None; }
  template <> NotifyKind pending_release_should_notify<LockReq::UNIQUE>() const { return unique_pending_count == 0 ? NotifyKind::All : NotifyKind::None; }

  // The counts are unsigned: an unmatched release wraps one rather than going negative, and the
  // mode it guards is then blocked for the lifetime of the process, silently and far from the
  // cause. Assert the balance at the release itself.
  template <LockReq Req> void unlock_state_updater();
  template <> void unlock_state_updater<LockReq::WRITE>()     { DMG_ASSERT(w_count > 0, "unlock_shared<WRITE> without a matching lock_shared<WRITE>"); --w_count; }
  template <> void unlock_state_updater<LockReq::READ>()      { DMG_ASSERT(r_count > 0, "unlock_shared<READ> without a matching lock_shared<READ>"); --r_count; }
  template <> void unlock_state_updater<LockReq::READ_ONLY>() { DMG_ASSERT(ro_count > 0, "unlock_shared<READ_ONLY> without a matching lock_shared<READ_ONLY>"); --ro_count; }
  template <> void unlock_state_updater<LockReq::UNIQUE>()    { }

  // WRITE omits ro_count, and READ_ONLY omits w_count, because the two are mutually exclusive by
  // admission (can_acquire<WRITE> demands ro_count == 0, can_acquire<READ_ONLY> demands
  // w_count == 0), so the omitted count is necessarily 0 whenever the other is being released.
  // Relax either admission rule without revisiting these and the lock declares itself UNLOCKED
  // while a holder is still live. UNIQUE excludes every shared mode, so no count can be standing.
  template <LockReq Req> bool unlock_has_fully_unlocked() const;
  template <> bool unlock_has_fully_unlocked<LockReq::WRITE>() const     { return w_count == 0 && r_count == 0; }
  template <> bool unlock_has_fully_unlocked<LockReq::READ>() const      { return r_count == 0 && ro_count == 0 && w_count == 0; }
  template <> bool unlock_has_fully_unlocked<LockReq::READ_ONLY>() const { return ro_count == 0 && r_count == 0; }
  template <> bool unlock_has_fully_unlocked<LockReq::UNIQUE>() const    { return true; }

  // If upon unlock we could possible unblock another lock then
  // we would want to notify to make sure we rapidly make progress
  // WRITE -> If w_count goes down to 0, READ_ONLY and UNIQUE maybe unblocked, hence: Notify All
  // READ -> If r_count goes down to 0 (and other counts were already 0), UNIQUE maybe unblocked
  // READ_ONLY -> If ro_count goes down to 0, WRITE and  UNIQUE maybe unblocked, hence: Notify All
  //
  // WRITE and READ_ONLY notify on a weaker condition than the fully-unlocked one above: dropping to
  // 0 admits READ_ONLY resp. WRITE even while a READ hold keeps the lock SHARED, and no later event
  // is guaranteed to fire (that READ may be held indefinitely). READ notifies on exactly the
  // fully-unlocked condition because no acquirer is gated on r_count, so a READ release can only
  // ever admit UNIQUE, and only by freeing the lock. Releasing UNIQUE always frees the lock, so it
  // can admit any waiter and always notifies.
  template <LockReq Req> NotifyKind unlock_should_notify() const;
  template <> NotifyKind unlock_should_notify<LockReq::WRITE>() const { return w_count == 0 ? NotifyKind::All : NotifyKind::None; }
  template <> NotifyKind unlock_should_notify<LockReq::READ>() const { return (r_count == 0 && w_count == 0 && ro_count == 0) ? NotifyKind::All : NotifyKind::None; }
  template <> NotifyKind unlock_should_notify<LockReq::READ_ONLY>() const { return ro_count == 0 ? NotifyKind::All : NotifyKind::None; }
  template <> NotifyKind unlock_should_notify<LockReq::UNIQUE>() const { return NotifyKind::All; }

  // clang-format on

  void maybe_notify(std::unique_lock<std::mutex> &lock, NotifyKind kind) {
    lock.unlock();
    if (kind == NotifyKind::All) cv.notify_all();
  }

  /// Acquires in mode `Req`, `wait` supplying the wait strategy. Requires `lock` to own mtx; leaves
  /// it owned iff the acquisition succeeded.
  ///
  /// The pending registration is dropped by RAII because it must survive a timeout and an exception
  /// out of the wait too: leak it once and every kind it gates is blocked for the lifetime of the
  /// process. The handler runs with mtx still held (cv.wait/wait_for re-acquire it on every exit
  /// path, and `lock` outlives the guard declared after it), which is what serializes the
  /// deregistration against another thread reading that same counter inside its own wait predicate:
  /// mutate it off-mtx and the decrement plus notify can land between that thread's check and its
  /// enqueue on the cv, losing the wake.
  template <LockReq Req>
  bool acquire(std::unique_lock<std::mutex> &lock, auto &&wait) {
    pending_add<Req>();
    bool acquired = false;
    auto const pending_guard = OnScopeExit{[&] {
      pending_sub<Req>();
      if (!acquired) maybe_notify(lock, pending_release_should_notify<Req>());
    }};
    if (!wait(lock, [this] { return can_acquire<Req>(); })) return false;
    commit_state<Req>();
    acquired = true;
    return true;
  }

  /// Non-blocking probe. Requires mtx. Never registers as pending: an attempt that does not wait
  /// has nothing to give priority to.
  template <LockReq Req>
  bool try_acquire() {
    if (!can_acquire<Req>()) return false;
    commit_state<Req>();
    return true;
  }

  auto blocking_wait() {
    return [this](std::unique_lock<std::mutex> &lock, auto pred) {
      cv.wait(lock, pred);
      return true;
    };
  }

  template <class Rep, class Period>
  auto timed_wait(std::chrono::duration<Rep, Period> const &time) {
    return [this, time](std::unique_lock<std::mutex> &lock, auto pred) { return cv.wait_for(lock, time, pred); };
  }

  // The pending registration acquire() holds across a wait, exposed for the pending scopes to hold
  // across a campaign of non-blocking probes instead. See UniquePendingScope.
  template <LockReq Req>
  void register_pending() {
    auto guard = std::unique_lock{mtx};
    pending_add<Req>();
  }

  template <LockReq Req>
  void unregister_pending() {
    auto lock = std::unique_lock{mtx};
    pending_sub<Req>();
    maybe_notify(lock, pending_release_should_notify<Req>());
  }

  /// Releases a hold taken in mode `Req`: drop its count, free the lock if that was the last hold,
  /// and notify whoever the release may have admitted. UNIQUE excludes every other mode, so it has
  /// no count to drop, always frees the lock, and always has someone to admit.
  template <LockReq Req>
  void release() {
    auto lock = std::unique_lock{mtx};
    // A second release of the same hold drops `state` while another thread owns it.
    [[maybe_unused]] constexpr auto expected = Req == LockReq::UNIQUE ? UNIQUE : SHARED;
    DMG_ASSERT(state == expected, "release() on a lock that is not held in the mode being released");
    unlock_state_updater<Req>();
    if (unlock_has_fully_unlocked<Req>()) {
      state = UNLOCKED;
    }
    maybe_notify(lock, unlock_should_notify<Req>());
  }

  /// Probe that also transitions pending -> held on success. Deregisters without notifying: as on
  /// acquire()'s success path, waking the kinds we gated is deferred to the eventual release, since
  /// we hold the resource now and they still cannot have it.
  template <LockReq Req>
  bool try_acquire_pending() {
    auto guard = std::unique_lock{mtx};
    if (!try_acquire<Req>()) return false;
    pending_sub<Req>();
    return true;
  }

 public:
  void lock() {
    auto lock = std::unique_lock{mtx};
    acquire<LockReq::UNIQUE>(lock, blocking_wait());
  }

  bool try_lock() {
    auto lock = std::unique_lock{mtx};
    return try_acquire<LockReq::UNIQUE>();
  }

  template <class Rep, class Period>
  bool try_lock_for(const std::chrono::duration<Rep, Period> &timeout_duration) {
    auto lock = std::unique_lock{mtx};
    return acquire<LockReq::UNIQUE>(lock, timed_wait(timeout_duration));
  }

  void unlock() { release<LockReq::UNIQUE>(); }

  template <LockReq Req = LockReq::WRITE>
    requires(Req != LockReq::UNIQUE)
  void lock_shared() {
    auto lock = std::unique_lock{mtx};
    acquire<Req>(lock, blocking_wait());
  }

  template <LockReq Req = LockReq::WRITE>
    requires(Req != LockReq::UNIQUE)
  bool try_lock_shared() {
    auto lock = std::unique_lock{mtx};
    return try_acquire<Req>();
  }

  /// Demotes a held shared mode to READ. Not an acquisition: the caller already holds the lock, so
  /// no admission rule applies and a pending UNIQUE cannot block it.
  template <LockReq Req>
    requires(Req != LockReq::UNIQUE)
  bool downgrade_to_read() {
    auto lock = std::unique_lock{mtx};
    if (state != SHARED) return false;
    unlock_state_updater<Req>();
    lock_state_updater<LockReq::READ>();
    maybe_notify(lock, unlock_should_notify<Req>());
    return true;
  }

  template <typename Rep, typename Period, LockReq Req = LockReq::WRITE>
    requires(Req != LockReq::UNIQUE)
  bool try_lock_shared_for(std::chrono::duration<Rep, Period> const &time) {
    auto lock = std::unique_lock{mtx};
    return acquire<Req>(lock, timed_wait(time));
  }

  template <LockReq Req = LockReq::WRITE>
    requires(Req != LockReq::UNIQUE)
  void unlock_shared() {
    release<Req>();
  }

 private:
  // Every member below is read and written under mtx only, the pending counters included: a reader
  // evaluates them inside its cv.wait predicate while holding mtx, so an off-mtx mutation could
  // land between that check and the reader's enqueue on the cv and lose the wake. Plain integers
  // rather than atomics, so that discipline is the only way to touch them.
  std::mutex mtx;
  std::condition_variable cv;
  states state = UNLOCKED;
  uint32_t ro_count = 0;
  uint32_t ro_pending_count = 0;
  uint32_t w_count = 0;
  uint32_t r_count = 0;
  // Callers waiting to acquire UNIQUE (blocking lock()/try_lock_for(), or a UniquePendingScope
  // campaign). Gates new shared acquisitions for writer-preference; see can_acquire.
  uint32_t unique_pending_count = 0;
};

struct SharedResourceLockGuard {
 public:
  enum Type { WRITE, READ, READ_ONLY };

  SharedResourceLockGuard(ResourceLock &l, Type type) : ptr_{&l}, type_{type} { lock(); }

  SharedResourceLockGuard(ResourceLock &l, Type type, std::defer_lock_t /*tag*/) : ptr_{&l}, type_{type} {}

  SharedResourceLockGuard(ResourceLock &l, Type type, std::try_to_lock_t /*tag*/) : ptr_{&l}, type_{type} {
    try_lock();
  }

  /// Adopts a shared lock of `type` already acquired by the caller (e.g. via
  /// ReadOnlyPendingScope::try_acquire()), like std::unique_lock's std::adopt_lock_t ctor: takes
  /// ownership without locking, so unlock() runs exactly once on destruction.
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

// RAII guard over ResourceLock spanning every hold kind: the exclusive UNIQUE hold and the three
// shared modes. The mode is a runtime tag rather than a std::variant<> because every alternative
// shares the same payload (a ResourceLock*) -- there is no per-alternative state, so a tag + switch
// is the whole of it. Move-only; releases in the correct mode on scope exit.
struct ResourceLockGuard {
 public:
  enum Type : std::uint8_t { UNIQUE, WRITE, READ, READ_ONLY };

  ResourceLockGuard(ResourceLock &l, Type type) : ptr_{&l}, type_{type} { lock(); }

  ResourceLockGuard(ResourceLock &l, Type type, std::defer_lock_t /*tag*/) : ptr_{&l}, type_{type} {}

  ResourceLockGuard(ResourceLock &l, Type type, std::try_to_lock_t /*tag*/) : ptr_{&l}, type_{type} { try_lock(); }

  // Adopt a hold the caller already owns in `type` (e.g. SetStorageMode handing its UNIQUE hold to
  // CollectGarbage, where reacquiring would deadlock). Mirrors std::adopt_lock: no acquisition, but
  // the guard now owns the release.
  ResourceLockGuard(ResourceLock &l, Type type, std::adopt_lock_t /*tag*/) : ptr_{&l}, type_{type}, locked_{true} {}

  // Take over a std::unique_lock's exclusive hold: a UNIQUE-mode guard that adopts the hold if `lock`
  // owns it, or is left unlocked (but bound to the same ResourceLock) otherwise. `lock` is
  // disassociated from its mutex either way, so ownership is never duplicated.
  explicit ResourceLockGuard(std::unique_lock<ResourceLock> &&lock)
      : ptr_{lock.mutex()}, type_{UNIQUE}, locked_{lock.owns_lock()} {
    lock.release();
  }

  ~ResourceLockGuard() { unlock(); }

  ResourceLockGuard(const ResourceLockGuard &) = delete;
  ResourceLockGuard &operator=(const ResourceLockGuard &) = delete;

  ResourceLockGuard(ResourceLockGuard &&other) noexcept
      : ptr_{std::exchange(other.ptr_, nullptr)}, type_{other.type_}, locked_{std::exchange(other.locked_, false)} {}

  ResourceLockGuard &operator=(ResourceLockGuard &&other) noexcept {
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
        case UNIQUE:
          ptr_->lock();
          break;
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
        case UNIQUE:
          locked_ = ptr_->try_lock();
          break;
        case WRITE:
          locked_ = ptr_->try_lock_shared<ResourceLock::LockReq::WRITE>();
          break;
        case READ:
          locked_ = ptr_->try_lock_shared<ResourceLock::LockReq::READ>();
          break;
        case READ_ONLY:
          locked_ = ptr_->try_lock_shared<ResourceLock::LockReq::READ_ONLY>();
          break;
      }
    }
    return locked_;
  }

  template <typename Rep, typename Period>
  bool try_lock_for(std::chrono::duration<Rep, Period> const &time) {
    if (ptr_ && !locked_) {
      switch (type_) {
        case UNIQUE:
          locked_ = ptr_->try_lock_for(time);
          break;
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
        case UNIQUE:
          ptr_->unlock();
          break;
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

  // Demote a held WRITE/READ_ONLY shared hold to READ. Not valid from UNIQUE (downgrade_to_read only
  // transitions the SHARED state); a UNIQUE guard cannot be downgraded and reports false.
  bool downgrade_to_read() {
    if (ptr_ && locked_) {
      switch (type_) {
        case UNIQUE:
          return false;
        case READ:
          return true;  // can't downgrade from read to read
        case WRITE: {
          auto res = ptr_->downgrade_to_read<ResourceLock::LockReq::WRITE>();
          if (res) type_ = READ;
          return res;
        }
        case READ_ONLY: {
          auto res = ptr_->downgrade_to_read<ResourceLock::LockReq::READ_ONLY>();
          if (res) type_ = READ;
          return res;
        }
      }
    }
    return false;
  }

  bool is_exclusive() const { return type_ == UNIQUE; }

  bool owns_lock() const { return ptr_ && locked_; }

  Type type() const { return type_; }

 private:
  ResourceLock *ptr_;
  Type type_;
  bool locked_{false};
};

/// RAII helper that keeps a caller registered as a pending UNIQUE waiter across a non-blocking
/// probe campaign, for callers that cannot block on lock()'s cv (e.g. a coroutine that must yield
/// to a scheduler) but still want writer-preference while they poll try_acquire().
///
/// A bare try_lock() loop never registers as pending, so it is just another unprivileged contender
/// each iteration and can starve behind back-to-back shared holders. This scope registers once up
/// front, gating new shared acquirers (see can_acquire) so the shared pool drains and the probe
/// eventually finds state == UNLOCKED.
///
/// Ownership: try_acquire() returns nullopt (still pending, gate stays up) or a
/// std::unique_lock<ResourceLock> owning the acquired lock. On success the scope stops counting as
/// pending and becomes inert (destructor is a no-op); the returned lock owns the unlock(). If
/// destroyed without acquiring, the destructor deregisters and wakes gated shared acquirers (as
/// acquire()'s failure path does).
///
/// Not copyable or movable: the registration is tied 1:1 to this scope's lifetime.
class UniquePendingScope {
 public:
  explicit UniquePendingScope(ResourceLock &lock) : lock_{&lock} {
    lock_->register_pending<ResourceLock::LockReq::UNIQUE>();
  }

  ~UniquePendingScope() {
    if (lock_ == nullptr) return;  // ownership transferred out by a successful try_acquire()
    lock_->unregister_pending<ResourceLock::LockReq::UNIQUE>();
  }

  UniquePendingScope(const UniquePendingScope &) = delete;
  UniquePendingScope &operator=(const UniquePendingScope &) = delete;
  UniquePendingScope(UniquePendingScope &&) = delete;
  UniquePendingScope &operator=(UniquePendingScope &&) = delete;

  /// Non-blocking attempt to take UNIQUE; succeeds only when the lock is UNLOCKED (existing holders
  /// are never preempted). Safe to call repeatedly; a failed call leaves the pending registration
  /// intact for the next attempt.
  std::optional<std::unique_lock<ResourceLock>> try_acquire() {
    if (lock_ == nullptr) return std::nullopt;  // already consumed by a prior successful call
    if (!lock_->try_acquire_pending<ResourceLock::LockReq::UNIQUE>()) return std::nullopt;
    ResourceLock *acquired_lock = std::exchange(lock_, nullptr);
    return std::unique_lock<ResourceLock>{*acquired_lock, std::adopt_lock};
  }

 private:
  ResourceLock *lock_;
};

/// Like UniquePendingScope, but for READ_ONLY: registers a pending READ_ONLY waiter (bumping
/// ro_pending_count, the existing READ_ONLY-over-WRITE priority mechanism) across a non-blocking
/// probe campaign, so a retrying try_acquire() gets the same gating a blocking
/// lock_shared<READ_ONLY>() gets. Ownership/lifetime mirror UniquePendingScope: try_acquire()
/// returns a SharedResourceLockGuard that adopted the lock; if destroyed without acquiring, the
/// destructor decrements ro_pending_count and wakes gated acquirers.
class ReadOnlyPendingScope {
 public:
  explicit ReadOnlyPendingScope(ResourceLock &lock) : lock_{&lock} {
    lock_->register_pending<ResourceLock::LockReq::READ_ONLY>();
  }

  ~ReadOnlyPendingScope() {
    if (lock_ == nullptr) return;  // ownership transferred out by a successful try_acquire()
    lock_->unregister_pending<ResourceLock::LockReq::READ_ONLY>();
  }

  ReadOnlyPendingScope(const ReadOnlyPendingScope &) = delete;
  ReadOnlyPendingScope &operator=(const ReadOnlyPendingScope &) = delete;
  ReadOnlyPendingScope(ReadOnlyPendingScope &&) = delete;
  ReadOnlyPendingScope &operator=(ReadOnlyPendingScope &&) = delete;

  /// Non-blocking attempt to take READ_ONLY, admitted by the same rule a blocking
  /// lock_shared<READ_ONLY>() waits on (see can_acquire<READ_ONLY>). Safe to call repeatedly.
  std::optional<SharedResourceLockGuard> try_acquire() {
    if (lock_ == nullptr) return std::nullopt;  // already consumed by a prior successful call
    if (!lock_->try_acquire_pending<ResourceLock::LockReq::READ_ONLY>()) return std::nullopt;
    ResourceLock *acquired_lock = std::exchange(lock_, nullptr);
    return SharedResourceLockGuard{*acquired_lock, SharedResourceLockGuard::Type::READ_ONLY, std::adopt_lock};
  }

 private:
  ResourceLock *lock_;
};

}  // namespace memgraph::utils
