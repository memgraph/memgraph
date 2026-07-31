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

#include <atomic>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <functional>
#include <memory>
#include <utility>

#include "utils/logging.hpp"

namespace memgraph::utils {

/// Single-owner park descriptor shared by `WorkerResumeEvent` and `DeadlineParkRegistry` (IP-1
/// design doc, opencode-work/resource-lock-starvation/coro-prepare/ip1-design.md, REVISION 2's
/// "ParkState single-owner model" and REVISION 4 R4.1/R4.2/R4.3 -- BINDING on this shape).
///
/// ---------------------------------------------------------------------------------------------
/// Why `on_resume` is a closure, not a bare `std::coroutine_handle<>` (R4.1/R4.2).
/// ---------------------------------------------------------------------------------------------
/// An earlier revision of this design stored a bare `std::coroutine_handle<> handle` here and had
/// every wake source `handle.resume()` it directly. That shape cannot express the real resume
/// contract: resuming a parked Prepare-coroutine is not just "continue this frame" -- it is a
/// SESSION-AWARE continuation that (per R4.2) must keep the owning session alive across a
/// cross-thread resume, detect pool shutdown before touching any per-database state, and, once
/// the frame completes, re-drive the connection's normal post-Execute bookkeeping. None of that
/// can live inside `WorkerResumeEvent`/`DeadlineParkRegistry` themselves (R4.1: they must stay
/// coroutine-agnostic and reusable outside the Prepare-accessor use case), so it is factored out
/// into an opaque `std::function<void()>` supplied by whoever constructs the `ParkState`. From
/// the point of view of this header and its two registries, `on_resume` is a black box: they
/// never need to know it wraps a coroutine handle at all.
///
/// Only the caller that wins `ClaimPark` may invoke `on_resume` -- exactly once, exactly one
/// winner across every wake source that ever races on this `ParkState` (lock-release
/// `WorkerResumeEvent::NotifyAll`, `DeadlineParkRegistry::Sweep`'s deadline sweep, either
/// registry's shutdown `Drain()`, and the awaitable's own abandon-path claim, R4.3). Losers must
/// not call `on_resume` and must not otherwise touch this `ParkState` beyond reading `claimed`.
///
/// ---------------------------------------------------------------------------------------------
/// Why `ParkState` is heap-allocated via `shared_ptr`, not a coroutine-frame local (R2).
/// ---------------------------------------------------------------------------------------------
/// A parked coroutine can be woken by up to three independent sources (see above). Exactly one of
/// them may ever invoke `on_resume`; the others must detect "already taken" and touch nothing
/// else. `claimed` is the single-owner flag that arbitrates this -- but for it to be observable by
/// a *losing* waker, it must remain valid storage even after the *winning* waker has invoked
/// `on_resume` (and thereby possibly driven the coroutine frame to completion and destruction).
/// If `ParkState` lived inside the coroutine frame, a losing waker could race the frame's
/// destruction and read a dangling `claimed` flag. Keeping it on its own heap allocation, kept
/// alive by `shared_ptr` refcounting from every registry it is registered in (plus, in the real
/// integration, the coroutine frame's own reference so it can find its `ParkState` again on a
/// re-probe), avoids that race entirely.

/// Delivery gate (see `RequestResume`/`ArmPark` below). Distinct from `claimed`: `claimed` decides
/// WHO may resume, `gate` decides WHEN that resume may actually be delivered.
enum class ParkGate : uint8_t {
  kParking,          // published to the registries, but the parking thread has not finished yet
  kArmed,            // the parking thread is done -- a resume may be delivered immediately
  kResumeRequested,  // the claim winner wants a resume, but arrived before kArmed
};

class ParkState {
 public:
  std::chrono::steady_clock::time_point deadline;
  std::atomic<bool> claimed{false};
  std::atomic<ParkGate> gate{ParkGate::kParking};

  /// Set once, by whoever constructs this ParkState, before it is published to any registry.
  void set_on_resume(std::function<void()> on_resume) { on_resume_ = std::move(on_resume); }

 private:
  /// Deliberately private, reachable ONLY through `RequestResume`/`ArmPark`. Both halves of the
  /// delivery gate are the whole safety argument for an un-pinned resume (see the gate discussion
  /// below): a wake source that invoked this directly would resume a coroutine frame whose parking
  /// thread may still be inside `await_suspend` -- the use-after-free that pinning used to prevent
  /// structurally. Encapsulation is what keeps that a compile error rather than a code-review note.
  std::function<void()> on_resume_;

  friend void RequestResume(ParkState &);
  friend void ArmPark(ParkState &);
};

/// Attempts to claim `ps` for exactly one wake source. Returns true to EXACTLY ONE caller across
/// all wake sources that ever race on the same `ParkState` -- every other caller, whether racing
/// concurrently or arriving after the fact, gets false. Only the caller that receives `true` may
/// ask for a resume (`RequestResume`, never `on_resume` directly -- it is private for that reason);
/// a caller that receives `false` must not, and must simply stop (R4.3: this includes the awaitable's
/// own abandon-path claim attempt, which loses exactly when some other wake source got there first
/// and is already driving -- or has already driven -- the resume to completion).
inline bool ClaimPark(ParkState &ps) { return !ps.claimed.exchange(true, std::memory_order_acq_rel); }

/// ---------------------------------------------------------------------------------------------
/// The delivery gate: why winning `ClaimPark` is NOT enough to resume (IP-1 F6, un-pinned resume).
/// ---------------------------------------------------------------------------------------------
/// A park is published to the registries from INSIDE the parking thread's `await_suspend`, and that
/// thread keeps running for a while afterwards: it finishes `await_suspend` itself (the B1
/// register-before-recheck re-probe, the shutdown self-claim), then unwinds back through
/// `coroutine_handle::resume()` into its DRIVER, which still has post-`Resume()` bookkeeping to do
/// (see communication::v2::Session::RunLoop: it must observe `!Done()` and retain the connection's
/// single in-flight slot before returning). None of that may run concurrently with a resume, which
/// re-enters the very same coroutine chain and the very same session state.
///
/// Until IP-1 F6 that exclusion came for free from PINNING the resume onto the parking worker: a
/// same-worker pinned task cannot start until that worker returns to its run loop, i.e. until the
/// whole driver call finished. Pinning conflated WHEN the resume may run with WHERE it runs, and the
/// WHERE half is what made a parked query's timeout (and its ordinary wake latency) hostage to one
/// worker's backlog -- an unrelated long-running task on that worker delays the resume arbitrarily.
///
/// This gate keeps the WHEN and drops the WHERE. It is a two-party rendezvous on a single atomic
/// between the claim winner (`RequestResume`) and the parking thread's task boundary (`ArmPark`):
/// each exchanges in its own marker and reads what the other left, so whichever arrives SECOND
/// invokes `on_resume` exactly once, and neither can invoke it early. `on_resume` may then post the
/// resume to ANY worker.
///
/// Asymmetry worth knowing: over-arming is harmless (a `ParkState` nobody ever requested a resume
/// for just moves to `kArmed` and stays there), while under-arming hangs the park forever. Arm from
/// a place that cannot be skipped -- utils::PriorityThreadPool's worker loop does it unconditionally
/// after every task, via `ArmPendingPark` below.

/// Called by the winner of `ClaimPark`. This is the ONLY way a wake source can ask for a resume --
/// `on_resume_` is private precisely so "claim, then resume" cannot be written by accident. Invokes
/// it here iff the parking thread already armed; otherwise the arming side will.
inline void RequestResume(ParkState &ps) {
  if (ps.gate.exchange(ParkGate::kResumeRequested, std::memory_order_acq_rel) == ParkGate::kArmed) {
    ps.on_resume_();
  }
}

/// Called once the parking thread has finished everything it owns (its pool task, and therefore its
/// driver's post-`Resume()` bookkeeping). Invokes `on_resume` here iff a claim winner already asked
/// for a resume while we were still parking; otherwise that winner will, if one ever shows up.
inline void ArmPark(ParkState &ps) {
  if (ps.gate.exchange(ParkGate::kArmed, std::memory_order_acq_rel) == ParkGate::kResumeRequested) {
    ps.on_resume_();
  }
}

/// The park published by THIS thread during the pool task it is currently running, awaiting its
/// `ArmPark`. At most one is ever pending: a coroutine chain suspends at exactly one point, and
/// publishing it is the last thing the parking task does before unwinding out (a re-park always
/// happens in a later, separately-armed task -- the resume is a fresh pool task).
inline thread_local std::shared_ptr<ParkState> tls_pending_park;

/// Publishes `ps` as this thread's pending-arm park. Call right after `ps` becomes visible to the
/// registries (i.e. right after a successful `WorkerResumeEvent::RegisterWaiter`), so that a wake
/// source claiming immediately afterwards finds a `ParkState` whose arming side is accounted for.
inline void PublishPendingPark(std::shared_ptr<ParkState> ps) {
  DMG_ASSERT(!tls_pending_park,
             "Two parks pending arm on one thread -- a task published a second park before the "
             "first was armed, which the single-suspend-point invariant forbids.");
  tls_pending_park = std::move(ps);
}

/// Drops this thread's pending-arm park WITHOUT arming it. Only for a parking attempt that published
/// `ps` and then self-claimed it (the abandon path / shutdown self-claim): the publisher itself is
/// the claim winner, so no resume will ever be requested and arming would be a no-op anyway --
/// clearing merely avoids holding the `ParkState` alive until this thread's next park. Takes `ps` so
/// the identity is checked: discarding somebody else's pending park would drop its arm and hang it.
inline void DiscardPendingPark(const std::shared_ptr<ParkState> &ps) {
  DMG_ASSERT(tls_pending_park == ps, "DiscardPendingPark on a park this thread did not publish.");
  tls_pending_park.reset();
}

/// Arms this thread's pending-arm park, if any. Called unconditionally at every pool-task boundary
/// (utils::PriorityThreadPool::Worker's run loop) -- see the gate discussion above for why this must
/// be somewhere impossible to skip rather than in each individual driver.
inline void ArmPendingPark() {
  if (!tls_pending_park) return;
  auto ps = std::move(tls_pending_park);  // leaves the slot empty
  ArmPark(*ps);
}

/// Scope guard form of `ArmPendingPark`, so a task that exits by exception still arms.
struct ParkArmGuard {
  ParkArmGuard() = default;
  ParkArmGuard(const ParkArmGuard &) = delete;
  ParkArmGuard &operator=(const ParkArmGuard &) = delete;
  ParkArmGuard(ParkArmGuard &&) = delete;
  ParkArmGuard &operator=(ParkArmGuard &&) = delete;

  ~ParkArmGuard() { ArmPendingPark(); }
};

}  // namespace memgraph::utils
