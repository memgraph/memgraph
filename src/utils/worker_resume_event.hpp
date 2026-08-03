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
/// might be free. Knows nothing of `PriorityThreadPool`, tasks or coroutine frames: each waiter is a
/// `shared_ptr<ParkState>` whose `on_resume` closure already encapsulates what waking it means. That
/// keeps this type -- and anything embedding it, e.g. `Storage` -- independent of both the pool and the
/// coroutine machinery.
///
/// TWO-SIDED WAKEUP PROTOCOL -- read before touching either side.
///
/// The "skip NotifyAll when nobody is parked" fast path (`WaitersPending()`) is sound ONLY if both
/// sides keep a strict register-before-recheck / release-before-check ordering. Any other way, a
/// release can race a park so that the waiter misses the epoch bump AND the releaser misses the
/// registration -- a lost wakeup that hangs the query indefinitely rather than surfacing the ~1s
/// `*AccessTimeout`, because the deadline sweep only fires for waiters that reached a registry.
///
/// Waiter side (the coroutine about to park):
///   1. Capture `epoch = Epoch()` BEFORE the non-blocking probe (e.g. `Storage::TryAccess`).
///   2. Probe. If free, done -- no park needed.
///   3. Still blocked: build a `shared_ptr<ParkState>` and `RegisterWaiter(ps, epoch)`.
///        - false => the epoch already moved (a `NotifyAll` ran since step 1), so do NOT park; loop
///          back and re-probe now, since whatever released may be gone by the time we parked. Use
///          `IsClosed()` to tell "retry" from "this storage is going away, give up".
///        - true => durably enqueued; `waiters_pending_` was incremented in the SAME critical section
///          that pushed the waiter, so any releaser seeing a nonzero `WaitersPending()` afterwards is
///          guaranteed to see this waiter in `waiters_` once it takes `mutex_`.
///   4. RE-PROBE once more: registering does not close the race against a release that happened
///      between steps 2 and 3. If this probe succeeds, `ClaimPark` our OWN `ParkState` and proceed
///      synchronously only if we WIN -- on a loss some wake source already fired and our resumption
///      is in flight, so we must behave as parked. `RemoveWaiter(ps)` is best-effort either way.
///   5. Only if the second probe still blocks (or that claim is lost) do we actually park.
///
/// Releaser side:
///   1. Transition the resource to released FIRST; this must happen-before step 2.
///   2. `WaitersPending()` (acquire load). Any waiter that finished registering is now visible; any
///      that registers after this load will re-probe per its own protocol and observe step 1.
///   3. If nonzero, call `NotifyAll()` unconditionally -- do not try to guess which waiters need it.
///
/// Why nothing is lost: a waiter leaves `waiters_` only inside a `mutex_` critical section
/// (`RemoveWaiter`/`NotifyAll`/`Drain`), and those move the ENTIRE vector out before releasing the
/// lock. So a releaser seeing `WaitersPending() > 0` races at worst a `RegisterWaiter` that has not
/// taken the lock yet -- and that waiter's own step-4 re-probe sees the released resource.
///
/// Two invariants this class exists to uphold:
///   - `mutex_` is ALWAYS released before any `on_resume` is invoked. It runs arbitrary
///     scheduler/session code and may re-enter this object to re-park, so invoking it under the lock
///     would risk deadlock. Callers likewise must not call `NotifyAll` while still holding the
///     resource's own lock.
///   - Single-owner resume holds ACROSS registries, not just within this event: every waiter is
///     `ClaimPark`ed before delivery, and that same exchange is shared with `DeadlineParkRegistry` and
///     any abandon-path claim on the same `ParkState`.
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

  /// Registers `ps` as parked, PROVIDED the epoch has not moved since the caller captured
  /// `expected_epoch`. Returns false (and does NOT enqueue) if `epoch_` has already advanced -- the
  /// caller must not park and should re-probe instead.
  ///
  /// The epoch check reads `epoch_` with relaxed order: correctness does not come from that
  /// atomic's own ordering but from `mutex_` itself -- every writer of `epoch_` (`NotifyAll`)
  /// takes the same `mutex_` around its `fetch_add`, so holding `mutex_` here already establishes
  /// happens-before against any prior epoch bump.
  bool RegisterWaiter(std::shared_ptr<ParkState> ps, uint64_t expected_epoch) {
    std::lock_guard<std::mutex> lock(mutex_);
    // Refuse permanently once drained. The window this closes was real: `Drain()` deliberately does not
    // bump the epoch, so a RegisterWaiter taking `mutex_` just after Drain's critical section saw an
    // unchanged epoch, pushed onto the just-emptied list, and parked with nobody left to wake it --
    // rescued only by the deadline sweep, which delays the DROP by up to the access timeout.
    //
    // Bumping the epoch in `Drain()` would NOT fix it: the refused registration sends the acquire loop
    // back to re-probe and re-register under the NEW epoch on the same drained event, moving the window
    // one iteration later. Only a STICKY flag terminates it -- and that is semantically right, since
    // every `Drain()` caller is tearing this storage down.
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

  /// Best-effort removal, used by the step-4 re-probe once it has already WON `ClaimPark` on its own
  /// `ParkState` and so knows no wake source will deliver for it. Correctness does not depend on this
  /// being called promptly or at all -- `NotifyAll`/`Drain` re-check `ClaimPark` themselves, so a
  /// claimed-but-unremoved entry is pruned as a no-op next time. Purely to keep `waiters_` and
  /// `waiters_pending_` accurate sooner.
  ///
  /// Returns whether `ps` was found, and decrements only then: a blind decrement would let a concurrent
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

  /// Wakes every waiter and CLOSES the event: no further registration can succeed. Each resumed frame
  /// is expected to observe `IsShuttingDown()` and bail cleanly rather than proceed into per-database
  /// work. For teardown only -- process shutdown, and `Storage::StopAllBackgroundTasks()` on the DROP
  /// DATABASE path.
  ///
  /// Unlike `NotifyAll` this does NOT bump the epoch, because closing is stronger than invalidating: an
  /// epoch bump only makes in-flight registrations retry, and retrying against a storage that is going
  /// away is exactly what must not happen.
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
