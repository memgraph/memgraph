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

/// Pool-agnostic event primitive a parked coroutine registers on, and that a lock-releaser
/// notifies once the resource it was waiting on might be free (IP-1 §3 item 2:
/// `utils/worker_resume_event.hpp`; see opencode-work/resource-lock-starvation/coro-prepare/
/// ip1-design.md REVISION 1 §§B1/C3/C4, REVISION 2's ParkState single-owner model, and REVISION 4
/// R4.2/R4.3/R4.4 -- all BINDING on this implementation).
///
/// This type owns no knowledge of `PriorityThreadPool`, tasks, coroutine frames, or task-state --
/// each waiter is a `shared_ptr<ParkState>` (see park_state.hpp) whose `on_resume` closure already
/// encapsulates however the caller wants "wake this waiter up" to happen (R4.2: in the real
/// integration, a session-aware continuation; in tests, a plain recording closure). This keeps
/// `WorkerResumeEvent` (and, in turn, anything that embeds it, e.g. `Storage`) independent of the
/// pool implementation AND of the coroutine machinery built on top of it.
///
/// ---------------------------------------------------------------------------------------------
/// BINDING two-sided wakeup protocol (R1 §B1) -- read this before touching either side.
/// ---------------------------------------------------------------------------------------------
///
/// The "skip NotifyAll when nobody is parked" fast path (`WaitersPending()`) is only sound if BOTH
/// sides follow a strict "register before recheck" / "release before check" ordering. Do it any
/// other way and a release can race a park such that the epoch bump is missed by the waiter AND
/// the waiter's registration is missed by the releaser -- a lost wakeup that (since the deadline
/// sweep only fires for waiters that actually made it into a registry) can hang a query
/// indefinitely instead of surfacing the ~1s `*AccessTimeout`.
///
/// Waiter side (the coroutine about to park):
///   1. Capture `epoch = Epoch()` BEFORE the non-blocking probe of the underlying resource
///      (e.g. `Storage::TryAccess`). This is the epoch that must still hold for a subsequent
///      park to be safe.
///   2. Probe the resource. If it is free, done -- no park needed.
///   3. If still blocked, build a `shared_ptr<ParkState>` and call `RegisterWaiter(ps, epoch)`.
///        - If it returns false, `epoch_` has already moved (a `NotifyAll` ran between step 1 and
///          step 3) -- the caller must NOT park; it must loop back and re-probe immediately,
///          because whatever released the resource may already be gone by the time we'd have
///          parked.
///        - If it returns true, the waiter is durably enqueued (`waiters_pending_` was
///          incremented, under `mutex_`, as part of the same critical section that pushed the
///          waiter -- so any releaser observing a nonzero `WaitersPending()` after this point is
///          guaranteed to also observe this waiter in `waiters_` once it takes `mutex_`).
///   4. RE-PROBE the resource ONE MORE TIME. Registering a waiter does not itself close the race
///      against a release that happened concretely between step 2 and step 3. If this second
///      probe now succeeds, the waiter is an "abandon path" participant (R4.3): it must call
///      `ClaimPark` on its OWN `ParkState` and only proceed synchronously (without suspending) if
///      it WINS that claim -- if it loses, some wake source already fired (or is about to) and the
///      caller must treat that as a real park (its resumption is already in flight). Either way
///      `RemoveWaiter(ps)` is a best-effort cleanup, never required for correctness (R4.3/R4.5).
///   5. Only if the second probe still blocks (or the abandon-path claim above is lost) does the
///      caller actually behave as parked.
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
///   3. If `WaitersPending() > 0`, call `NotifyAll()` unconditionally (do not try to be cleverer
///      about which specific waiters "need" the wakeup -- see C4 below).
///
/// Why no wakeup is lost: a waiter can only be resumed by (a) `NotifyAll`/`Drain` claiming it out
/// of `waiters_` and invoking `on_resume`, or (b) its own step-4/5 abandon-path claim winning
/// without ever truly parking. Every waiter that registers under a given `epoch` is captured
/// entirely inside one critical section of `mutex_` (`RegisterWaiter`) and can only leave
/// `waiters_` inside another critical section of `mutex_` (`RemoveWaiter`, `NotifyAll`, or
/// `Drain`). `NotifyAll`/`Drain` always bump `epoch_` (NotifyAll only) and move the ENTIRE
/// `waiters_` vector out before releasing `mutex_` -- so a releaser that observes
/// `WaitersPending() > 0` is racing, at worst, against a `RegisterWaiter` that hasn't yet taken
/// the lock; that waiter's own subsequent re-probe (step 4) will see the released resource.
///
/// ---------------------------------------------------------------------------------------------
/// C3 (wake ordering) / C4 (single-owner resume) -- constraints this class exists to uphold:
/// ---------------------------------------------------------------------------------------------
///   - C3: `NotifyAll`'s `epoch_` bump models "a release happened"; callers MUST NOT invoke it
///     while still holding the resource's own internal lock (`main_lock_`/`ResourceLock`
///     internals) -- see the releaser-side ordering above. This class itself never blocks inside
///     `on_resume`: `mutex_` is released before any waiter's `on_resume` is invoked (see
///     `NotifyAll`/`Drain`), so `on_resume` may safely re-enter this object (e.g. to
///     `Epoch()`/probe again, or to register a fresh `ParkState` for a re-park) without
///     deadlocking.
///   - C4: `NotifyAll`/`Drain` move `waiters_` out from under `mutex_` and clear the member before
///     releasing the lock. A concurrent second call is guaranteed to see an empty `waiters_` and
///     therefore invoke nothing -- combined with `ClaimPark`'s single-owner exchange (shared with
///     `DeadlineParkRegistry` and any abandon-path claim on the SAME `ParkState`), each registered
///     waiter has `on_resume` invoked by exactly one wake source, ever, across the whole system.
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
    if (epoch_.load(std::memory_order_relaxed) != expected_epoch) {
      return false;
    }
    waiters_.push_back(std::move(ps));
    waiters_pending_.fetch_add(1, std::memory_order_acq_rel);
    return true;
  }

  /// Best-effort removal of `ps` from the waiter list, used by the waiter-side step-4/5 re-probe
  /// (R4.3) once it has ALREADY won `ClaimPark` on its own `ParkState` and therefore knows no wake
  /// source will (or should) invoke `on_resume` for it. Correctness does NOT depend on this being
  /// called promptly, or at all: `NotifyAll`/`Drain` re-check `ClaimPark` themselves before
  /// invoking `on_resume`, so a claimed-but-not-yet-removed entry is simply pruned as a no-op the
  /// next time this event wakes. This is purely a cleanup optimization to keep `waiters_` and
  /// `waiters_pending_` accurate sooner. Returns whether `ps` was actually found and removed; the
  /// pending counter is only decremented in that case (a blind unconditional decrement would let a
  /// concurrent `NotifyAll`/`Drain` -- which already accounted for this waiter in its own bulk
  /// `fetch_sub` -- double-subtract and underflow the unsigned counter).
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

  /// Wakes every currently-registered waiter: for each one, attempts `ClaimPark` and, only on a
  /// win, invokes `on_resume()`. A waiter already claimed by some other wake source (the deadline
  /// sweep, a shutdown drain, or its own abandon-path claim, R4.3) simply loses the exchange here
  /// and is dropped without invoking anything -- this is what makes single-ownership hold ACROSS
  /// registries, not just within this event (R2/R4's whole point). Also bumps `epoch_` so any
  /// waiter mid-registration with a now-stale epoch bails out of `RegisterWaiter` and re-probes
  /// instead of parking.
  ///
  /// CRITICAL (C3): `mutex_` is released BEFORE any `on_resume` is invoked. Never invoke a waiter's
  /// `on_resume` while holding `mutex_` -- it may run arbitrary scheduler/session code (and, via a
  /// re-park, may re-enter this object), so calling it under `mutex_` risks deadlock/reentrancy
  /// hazards this class must not introduce.
  void NotifyAll() { ResumeAll(/*bump_epoch=*/true); }

  /// Shutdown drain (R4.4): claims and invokes `on_resume` for every currently-registered waiter,
  /// exactly like `NotifyAll`, so a pool teardown resumes every parked frame at least once (the
  /// frame's `on_resume` is expected to observe `IsShuttingDown()` and drive itself to a clean
  /// bail rather than proceeding into any per-database work). Does not bump `epoch_` -- shutdown is
  /// terminal, not a state transition waiters should re-probe against.
  void Drain() { ResumeAll(/*bump_epoch=*/false); }

  /// Lock-free fast-path gate for releasers (R1 §B1 releaser step 2): if this is 0, there is
  /// nothing to wake and `NotifyAll` (and its mutex acquisition) can be skipped entirely. Sound
  /// only when combined with the ordering rules documented above (release-before-check on the
  /// releaser side; register-before-recheck on the waiter side).
  size_t WaitersPending() const { return waiters_pending_.load(std::memory_order_acquire); }

 private:
  /// Shared body for `NotifyAll`/`Drain`: move every waiter out from under `mutex_`, release the
  /// lock, then claim+invoke each one outside it (C3).
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
    // mutex_ is released here -- on_resume() runs entirely outside the lock (C3).
    for (auto &ps : local) {
      if (ClaimPark(*ps)) {
        ps->on_resume();
      }
      // Else: some other wake source (deadline sweep, shutdown drain, or the waiter's own
      // abandon-path claim, R4.3) already won -- do not invoke on_resume, single-owner holds.
    }
  }

  std::mutex mutex_;
  std::atomic<uint64_t> epoch_{0};
  std::atomic<size_t> waiters_pending_{0};
  std::vector<std::shared_ptr<ParkState>> waiters_;
};

}  // namespace memgraph::utils
