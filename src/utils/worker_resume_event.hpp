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

#include <algorithm>
#include <atomic>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <mutex>
#include <vector>

#include "utils/park_state.hpp"

namespace memgraph::utils {

/// Pool-agnostic event a parked coroutine registers on, notified by a lock-releaser once the resource
/// might be free. Knows nothing of the pool or coroutines -- each waiter is a `shared_ptr<ParkState>`
/// whose closure encapsulates what waking it means.
///
/// TWO-SIDED WAKEUP PROTOCOL. The `WaitersPending()` fast path is sound ONLY if both sides keep this
/// ordering; otherwise a release races a park such that the waiter misses the epoch bump AND the
/// releaser misses the registration, hanging the query instead of surfacing its timeout.
///
///   Waiter:   capture `Epoch()` BEFORE probing -> probe -> on failure `RegisterWaiter(ps, epoch)` ->
///             RE-PROBE once more -> only then behave as parked.
///   Releaser: transition the resource to released FIRST -> read `WaitersPending()` -> if nonzero,
///             `NotifyAll()` unconditionally, without guessing which waiters need it.
///
/// A `false` from `RegisterWaiter` means retry, EXCEPT when `IsClosed()` -- then the storage is going
/// away and the caller must give up rather than spin to its deadline.
///
/// Two invariants this class upholds:
///   - `mutex_` is ALWAYS released before any `on_resume`, which runs arbitrary scheduler/session code
///     and may re-enter this object to re-park. Callers must likewise not call `NotifyAll` while still
///     holding the resource's own lock.
///   - Single-owner resume holds ACROSS registries, not just within this event: every waiter is
///     `ClaimPark`ed before delivery, and that exchange is shared with `DeadlineParkRegistry` and any
///     abandon-path claim on the same `ParkState`.
class WorkerResumeEvent {
 public:
  WorkerResumeEvent() = default;

  WorkerResumeEvent(const WorkerResumeEvent &) = delete;
  WorkerResumeEvent &operator=(const WorkerResumeEvent &) = delete;
  WorkerResumeEvent(WorkerResumeEvent &&) = delete;
  WorkerResumeEvent &operator=(WorkerResumeEvent &&) = delete;

  ~WorkerResumeEvent() = default;

  /// Current epoch. Callers MUST capture this BEFORE their non-blocking probe of the guarded
  /// resource (see the waiter-side protocol above) -- capturing it after the probe reopens the
  /// lost-wakeup race this class is designed to close.
  uint64_t Epoch() const { return epoch_.load(std::memory_order_acquire); }

  /// Registers `ps` as parked, PROVIDED the epoch has not moved since `expected_epoch` was captured.
  /// A false return means do not park -- re-probe instead (or give up, if `IsClosed()`).
  ///
  /// The epoch load is relaxed on purpose: ordering comes from `mutex_`, not the atomic. Every writer
  /// of `epoch_` takes the same mutex around its `fetch_add`, so holding it here already establishes
  /// happens-before against any prior bump.
  bool RegisterWaiter(std::shared_ptr<ParkState> ps, uint64_t expected_epoch) {
    std::lock_guard<std::mutex> lock(mutex_);
    // Refuse permanently once drained. Bumping the epoch in `Drain()` would NOT do: a refused
    // registration would just re-register under the NEW epoch on the same drained event, moving the
    // window one iteration later. Only a STICKY flag terminates it.
    if (closed_) [[unlikely]] {
      return false;
    }
    if (epoch_.load(std::memory_order_relaxed) != expected_epoch) {
      return false;
    }
    waiters_.push_back(std::move(ps));
    waiters_pending_.fetch_add(1, std::memory_order_acq_rel);
    return true;
  }

  /// Best-effort removal, for a re-probe that already WON `ClaimPark` on its own `ParkState`.
  /// Correctness does not depend on it being called promptly or at all, since `NotifyAll`/`Drain`
  /// re-check `ClaimPark` themselves.
  ///
  /// Decrements ONLY when `ps` was actually found: a blind decrement would let a concurrent
  /// `NotifyAll`/`Drain` -- which already counted this waiter in its bulk `fetch_sub` -- double-subtract
  /// and underflow the unsigned counter.
  bool RemoveWaiter(const std::shared_ptr<ParkState> &ps) {
    std::lock_guard<std::mutex> lock(mutex_);
    auto it = std::find(waiters_.begin(), waiters_.end(), ps);
    if (it == waiters_.end()) {
      return false;
    }
    waiters_.erase(it);
    waiters_pending_.fetch_sub(1, std::memory_order_acq_rel);
    return true;
  }

  /// Wakes every registered waiter: `ClaimPark` each, deliver only on a win. A waiter already claimed
  /// elsewhere (deadline sweep, shutdown drain, its own abandon-path claim) loses here and is dropped
  /// silently. Also bumps `epoch_`, so a waiter mid-registration with a now-stale epoch bails out of
  /// `RegisterWaiter` and re-probes instead of parking.
  void NotifyAll() { ResumeAll(/*bump_epoch=*/true); }

  /// Wakes every waiter and CLOSES the event: no further registration can succeed. Teardown only --
  /// process shutdown, and `StopAllBackgroundTasks()` on the DROP DATABASE path. Deliberately does NOT
  /// bump the epoch: closing is stronger than invalidating, and a bump would only make in-flight
  /// registrations retry against a storage that is going away.
  void Drain() {
    {
      std::lock_guard<std::mutex> lock(mutex_);
      closed_ = true;
    }
    ResumeAll(/*bump_epoch=*/false);
  }

  /// True once `Drain()` has run. Callers that get `false` from `RegisterWaiter` use this to tell
  /// "the epoch moved, re-probe and try again" from "this storage is gone, stop trying".
  bool IsClosed() const {
    std::lock_guard<std::mutex> lock(mutex_);
    return closed_;
  }

  /// Lock-free fast path for releasers (step 2): zero means there is nothing to wake, so `NotifyAll`
  /// and its mutex acquisition can be skipped. Sound only alongside the ordering rules above.
  size_t WaitersPending() const { return waiters_pending_.load(std::memory_order_acquire); }

 private:
  /// Shared body for `NotifyAll`/`Drain`: move every waiter out from under `mutex_`, release the lock,
  /// then claim and deliver outside it.
  void ResumeAll(bool bump_epoch) {
    std::vector<std::shared_ptr<ParkState>> local;
    {
      std::lock_guard<std::mutex> lock(mutex_);
      if (bump_epoch) {
        epoch_.fetch_add(1, std::memory_order_acq_rel);
      }
      local = std::move(waiters_);
      waiters_.clear();
      waiters_pending_.fetch_sub(local.size(), std::memory_order_acq_rel);
    }
    // mutex_ is released here -- delivery runs entirely outside the lock.
    for (auto &ps : local) {
      // Through the delivery gate, never `on_resume()` directly: winning the claim decides that WE
      // resume this waiter, not that it may be resumed YET -- the parking thread may still be inside
      // its own await_suspend/driver. A loser touches nothing; single-owner holds.
      if (ClaimPark(*ps)) RequestResume(*ps);
    }
  }

  mutable std::mutex mutex_;  // mutable: IsClosed() is a const observer that must take the lock
  /// Set once by `Drain()`, never cleared. Guarded by `mutex_` rather than atomic so it is established
  /// in the same critical section that empties `waiters_`; set outside the lock it would reopen the very
  /// race it closes.
  bool closed_{false};
  std::atomic<uint64_t> epoch_{0};
  std::atomic<size_t> waiters_pending_{0};
  std::vector<std::shared_ptr<ParkState>> waiters_;
};

}  // namespace memgraph::utils
