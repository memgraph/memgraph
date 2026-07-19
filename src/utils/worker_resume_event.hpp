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
#include <coroutine>
#include <cstddef>
#include <cstdint>
#include <functional>
#include <mutex>
#include <vector>

namespace memgraph::utils {

/// Pool-agnostic event primitive a parked coroutine registers on, and that a lock-releaser
/// notifies once the resource it was waiting on might be free (IP-1 §3 item 2:
/// `utils/worker_resume_event.hpp`; see opencode-work/resource-lock-starvation/coro-prepare/
/// ip1-design.md REVISION 1, sections B1/C3/C4, which are BINDING on this implementation).
///
/// This type owns no knowledge of `PriorityThreadPool`, tasks, or task-state -- the *how* of
/// getting a parked coroutine handle back onto a worker is injected by the caller as a
/// `reschedule` callback passed to `NotifyAll`. This keeps `WorkerResumeEvent` (and, in turn,
/// anything that embeds it, e.g. `Storage`) independent of the pool implementation.
///
/// ---------------------------------------------------------------------------------------------
/// BINDING two-sided wakeup protocol (R1 §B1) -- read this before touching either side.
/// ---------------------------------------------------------------------------------------------
///
/// The original design's "skip NotifyAll when nobody is parked" fast path is only sound if BOTH
/// sides follow a strict "register before recheck" / "release before check" ordering. Do it any
/// other way and a release can race a park such that the epoch bump is missed by the waiter AND
/// the waiter's registration is missed by the releaser -- a lost wakeup that (since B2's deadline
/// sweep only fires for waiters that actually made it into `waiters_`) can hang a query
/// indefinitely instead of surfacing the ~1s `*AccessTimeout`.
///
/// Waiter side (the coroutine about to park):
///   1. Capture `epoch = Epoch()` BEFORE the non-blocking probe of the underlying resource
///      (e.g. `Storage::TryAccess`). This is the epoch that must still hold for a subsequent
///      park to be safe.
///   2. Probe the resource. If it is free, done -- no park needed.
///   3. If still blocked, call `RegisterWaiter(handle, worker_id, epoch)`.
///        - If it returns false, `epoch_` has already moved (a `NotifyAll` ran between step 1 and
///          step 3) -- the caller must NOT park; it must loop back and re-probe immediately,
///          because whatever released the resource may already be gone by the time we'd have
///          parked.
///        - If it returns true, the waiter is durably enqueued (`waiters_pending_` was
///          incremented, under `mutex_`, as part of the same critical section that pushed the
///          waiter -- so any releaser observing a nonzero `WaitersPending()` after this point is
///          guaranteed to also observe this waiter in `waiters_` once it takes `mutex_`).
///   4. RE-PROBE the resource ONE MORE TIME. Registering a waiter does not itself close the
///      race against a release that happened concretely between step 2 and step 3 (the resource
///      could have been freed and re-acquired by someone else's release/acquire pair whose
///      `NotifyAll` we might have just missed by a hair, or -- more importantly -- simply because
///      the first probe result may already be stale). If this second probe now succeeds, call
///      `RemoveWaiter(handle)` to un-park (unregister) before proceeding -- do NOT suspend.
///   5. Only if the second probe still blocks does the coroutine actually suspend.
///
/// Releaser side (whoever just released a UNIQUE/READ_ONLY hold on the guarded resource):
///   1. Transition the lock/resource state to "released" first (e.g. unlock the mutex, publish
///      the released state with release-store semantics). This must happen-before step 2.
///   2. Call `WaitersPending()` (an acquire load). Because of the waiter-side ordering above, any
///      waiter that finished `RegisterWaiter` before this load is now visible; any waiter that
///      calls `RegisterWaiter` AFTER this load will, per its own protocol, re-probe the resource
///      it is trying to acquire -- and since the release in step 1 already happened, that re-probe
///      observes the released state and the waiter never needs to park (or, if a third party
///      raced in and re-acquired it, that party will eventually release and run this same
///      protocol again).
///   3. If `WaitersPending() > 0`, call `NotifyAll(reschedule)` unconditionally (do not try to be
///      cleverer about which specific waiters "need" the wakeup -- see C4 below).
///
/// Why no wakeup is lost: a waiter can only be resumed by (a) `NotifyAll` picking it up out of
/// `waiters_`, or (b) its own step-4 re-probe succeeding without ever parking. Every waiter that
/// registers under a given `epoch` is captured entirely inside one critical section of `mutex_`
/// (`RegisterWaiter`) and can only leave `waiters_` inside another critical section of `mutex_`
/// (`RemoveWaiter` or `NotifyAll`). `NotifyAll` always bumps `epoch_` and moves the ENTIRE
/// `waiters_` vector out before releasing `mutex_` -- so a releaser that observes
/// `WaitersPending() > 0` is racing, at worst, against a `RegisterWaiter` that hasn't yet taken
/// the lock; that waiter's own subsequent re-probe (step 4) will see the released resource. There
/// is no window in which a waiter is durably registered (visible via `WaitersPending()`) AND
/// permanently skipped by every subsequent `NotifyAll`.
///
/// ---------------------------------------------------------------------------------------------
/// C3 (wake ordering) / C4 (single-owner resume) -- constraints this class exists to uphold:
/// ---------------------------------------------------------------------------------------------
///   - C3: `NotifyAll`'s `epoch_` bump models "a release happened"; callers MUST NOT invoke it
///     while still holding the resource's own internal lock (`main_lock_`/`ResourceLock`
///     internals) -- see the releaser-side ordering above. This class itself never blocks inside
///     a callback: `mutex_` is released before `reschedule` is invoked for any waiter (see
///     `NotifyAll`), so `reschedule` may safely re-enter this object (e.g. to `Epoch()`/probe
///     again) without deadlocking.
///   - C4: `NotifyAll` moves `waiters_` out from under `mutex_` and clears the member before
///     releasing the lock. A concurrent second `NotifyAll` (or a shutdown drain built the same
///     way) is guaranteed to see an empty `waiters_` and therefore reschedules nothing -- each
///     registered waiter is handed to exactly one `reschedule` call, ever. There is deliberately
///     no way to resume a handle twice through this class.
class WorkerResumeEvent {
 public:
  /// A single parked coroutine: the handle to resume and the worker it was running on when it
  /// parked (so `reschedule` can pin the resumption back onto that same worker if desired).
  struct Waiter {
    std::coroutine_handle<> handle;
    size_t worker_id{0};
  };

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

  /// Registers `h` (running on `worker_id`) as parked, PROVIDED the epoch has not moved since the
  /// caller captured `expected_epoch`. Returns false (and does NOT enqueue) if `epoch_` has
  /// already advanced -- the caller must not park and should re-probe instead.
  ///
  /// The epoch check reads `epoch_` with relaxed order: correctness does not come from that
  /// atomic's own ordering but from `mutex_` itself -- every writer of `epoch_` (`NotifyAll`)
  /// takes the same `mutex_` around its `fetch_add`, so holding `mutex_` here already establishes
  /// happens-before against any prior epoch bump.
  bool RegisterWaiter(std::coroutine_handle<> h, size_t worker_id, uint64_t expected_epoch) {
    std::lock_guard<std::mutex> lock(mutex_);
    if (epoch_.load(std::memory_order_relaxed) != expected_epoch) {
      return false;
    }
    waiters_.push_back(Waiter{.handle = h, .worker_id = worker_id});
    waiters_pending_.fetch_add(1, std::memory_order_acq_rel);
    return true;
  }

  /// Un-registers `h` if it is still present (i.e. no `NotifyAll` has picked it up yet). Used by
  /// the waiter-side step-4 re-probe: if the resource turns out to be free right after
  /// registering, the coroutine must un-park itself rather than suspend. Returns whether `h` was
  /// actually found and removed; the pending counter is only decremented in that case, preserving
  /// the invariant `waiters_pending_ == waiters_.size()` under `mutex_` (a blind unconditional
  /// decrement here would let a concurrent `NotifyAll` -- which already accounted for this waiter
  /// in its own bulk `fetch_sub` -- double-subtract and underflow the unsigned counter).
  bool RemoveWaiter(std::coroutine_handle<> h) {
    std::lock_guard<std::mutex> lock(mutex_);
    auto it = std::find_if(waiters_.begin(), waiters_.end(), [&](const Waiter &w) { return w.handle == h; });
    if (it == waiters_.end()) {
      return false;
    }
    waiters_.erase(it);
    waiters_pending_.fetch_sub(1, std::memory_order_acq_rel);
    return true;
  }

  /// Wakes every currently-registered waiter exactly once (C4: single-owner resume -- the move
  /// out from under `mutex_` guarantees a racing second `NotifyAll`/drain sees an empty list) and
  /// bumps `epoch_` so any waiter mid-registration with a now-stale epoch bails out of
  /// `RegisterWaiter` and re-probes instead of parking.
  ///
  /// CRITICAL (C3): `mutex_` is released BEFORE `reschedule` is invoked for any waiter. Never
  /// hold `mutex_` across the callback -- `reschedule` may run arbitrary scheduler code (and, via
  /// the resumed coroutine, may re-enter this object), so calling it under `mutex_` risks
  /// deadlock/reentrancy hazards this class must not introduce.
  void NotifyAll(const std::function<void(std::coroutine_handle<>, size_t)> &reschedule) {
    std::vector<Waiter> local;
    {
      std::lock_guard<std::mutex> lock(mutex_);
      epoch_.fetch_add(1, std::memory_order_acq_rel);
      local = std::move(waiters_);
      waiters_.clear();
      waiters_pending_.fetch_sub(local.size(), std::memory_order_acq_rel);
    }
    // mutex_ is released here -- reschedule() runs entirely outside the lock (C3).
    for (auto &waiter : local) {
      reschedule(waiter.handle, waiter.worker_id);
    }
  }

  /// Lock-free fast-path gate for releasers (R1 §B1 releaser step 2): if this is 0, there is
  /// nothing to wake and `NotifyAll` (and its mutex acquisition) can be skipped entirely. Sound
  /// only when combined with the ordering rules documented above (release-before-check on the
  /// releaser side; register-before-recheck on the waiter side).
  size_t WaitersPending() const { return waiters_pending_.load(std::memory_order_acquire); }

 private:
  std::mutex mutex_;
  std::atomic<uint64_t> epoch_{0};
  std::atomic<size_t> waiters_pending_{0};
  std::vector<Waiter> waiters_;
};

}  // namespace memgraph::utils
