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
#include <exception>
#include <functional>
#include <memory>
#include <utility>
#include <vector>

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
  /// Moves `on_resume_` out and invokes the moved-out copy, so this `ParkState` stops owning whatever
  /// the closure captured the instant the resume fires.
  ///
  /// Load-bearing, not tidiness. In the real integration `on_resume_` captures a
  /// `shared_ptr<Session>` to keep the connection alive across the park, and a `ParkState` routinely
  /// outlives its own resume: `DeadlineParkRegistry::Sweep` erases the entry from its own list but
  /// nothing prunes the twin entry in `Storage::main_lock_resume_event_` until that event next
  /// notifies. Every timed-out parked query therefore left a fired `ParkState` sitting in `waiters_`
  /// still holding the Session -- and through it a `DatabaseAccess`, and through that
  /// `Gatekeeper<Database>::count_` above zero, which is what stalls DROP DATABASE. Releasing here
  /// makes the leftover entry inert regardless of when it is finally pruned.
  ///
  /// Exactly one caller ever reaches this (the gate guarantees a single delivery), so the moved-from
  /// state is never observed by a second invocation.
  void TakeAndInvokeOnResume() noexcept {
    auto on_resume = std::move(on_resume_);
    // Never propagate. Delivery allocates (it posts a task), and every caller is somewhere a throw
    // would do disproportionate damage:
    //   - ~ResourceLockGuard -> release -> the admit observer -> here, i.e. inside a destructor, where
    //     an escaping exception terminates the process;
    //   - WorkerResumeEvent::ResumeAll and DeadlineParkRegistry::Sweep/Drain loop over every claimed
    //     waiter, so an escape would abandon the REST of them un-resumed -- each one a permanently
    //     parked query, which is a far worse outcome than the one that failed;
    //   - ~ParkArmGuard, during a task's own unwinding.
    // Losing one resume strands one query (loudly logged). Losing the instance, or the other waiters,
    // is worse. This is why callers may treat RequestResume/ArmPark as non-throwing.
    try {
      on_resume();
    } catch (const std::exception &e) {
      spdlog::critical("Failed to deliver a parked query's resume: {}. That query will not make progress.", e.what());
    }
  }

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
/// for just moves to `kArmed` and stays there), while under-arming hangs the park forever -- and not
/// only for that query: the parked frame holds its campaign-long `PendingHandle`, which keeps
/// `unique_pending_count` above zero and makes `can_acquire<WRITE>`, `<READ>` and `<READ_ONLY>` all
/// false for good on that storage. So arm from somewhere that cannot be skipped: `ParkArmGuard` below,
/// wrapped around EVERY site that runs a pool task body (utils::PriorityThreadPool's three run-loop
/// sites plus `PostResumeTask`'s inline fallback).

/// Called by the winner of `ClaimPark`. This is the ONLY way a wake source can ask for a resume --
/// `on_resume_` is private precisely so "claim, then resume" cannot be written by accident. Invokes
/// it here iff the parking thread already armed; otherwise the arming side will.
inline void RequestResume(ParkState &ps) {
  if (ps.gate.exchange(ParkGate::kResumeRequested, std::memory_order_acq_rel) == ParkGate::kArmed) {
    ps.TakeAndInvokeOnResume();
  }
}

/// Called once the parking thread has finished everything it owns (its pool task, and therefore its
/// driver's post-`Resume()` bookkeeping). Invokes `on_resume` here iff a claim winner already asked
/// for a resume while we were still parking; otherwise that winner will, if one ever shows up.
inline void ArmPark(ParkState &ps) {
  if (ps.gate.exchange(ParkGate::kArmed, std::memory_order_acq_rel) == ParkGate::kResumeRequested) {
    ps.TakeAndInvokeOnResume();
  }
}

/// Parks published by THIS thread that are still awaiting their `ArmPark`, innermost last.
///
/// A STACK rather than a single slot, because task execution nests. `PostResumeTask`'s
/// all-workers-stopped fallback (utils/priority_thread_pool.cpp) runs a resume INLINE on the claiming
/// thread, and that resume re-enters the whole session chain -- `h.resume()` -> the caller's resumed
/// hook -> `Session::RunLoop` -> `Execute()` -> possibly a brand new query that parks. That nested
/// park is published while the OUTER park is still pending its own arm. With one slot the inner
/// publish silently overwrote the outer one, and an overwritten park is not merely a lost query: it
/// stays registered with `gate == kParking`, so no `RequestResume` can ever deliver it, and its
/// coroutine frame holds the campaign-long `PendingHandle` forever -- which pins
/// `unique_pending_count` above zero and makes `can_acquire<WRITE>`, `<READ>` and `<READ_ONLY>` all
/// false permanently (utils/resource_lock.hpp). Untimed acquirers (the TTL thread, replication apply)
/// then block forever and the database is bricked until the process restarts.
///
/// A stack makes that nesting REPRESENTABLE instead of fatal, which is why this is not merely an
/// assert. Depth, not identity, is what each arming site keys off -- see `ParkArmGuard`.
inline thread_local std::vector<std::shared_ptr<ParkState>> tls_pending_parks;

/// Publishes `ps` as pending-arm for this thread. Call right after `ps` becomes visible to the
/// registries (i.e. right after a successful `WorkerResumeEvent::RegisterWaiter`), so that a wake
/// source claiming immediately afterwards finds a `ParkState` whose arming side is accounted for.
inline void PublishPendingPark(std::shared_ptr<ParkState> ps) { tls_pending_parks.push_back(std::move(ps)); }

/// Drops a pending-arm park WITHOUT arming it. Only for a parking attempt that published `ps` and
/// then self-claimed it (the abandon path / shutdown self-claim): the publisher itself is the claim
/// winner, so no resume will ever be requested and arming would be a no-op anyway -- dropping it
/// merely avoids holding the `ParkState` alive until this thread's next arming point.
///
/// `ps` must be the innermost pending park, which it always is: a publisher self-claims within the
/// same `await_suspend` that published, with no nested task execution in between. MG_ASSERT rather
/// than DMG_ASSERT deliberately -- a mismatch here would drop somebody else's arm, and that is the
/// brick-the-database failure described above, which must not be a no-op in a release build.
inline void DiscardPendingPark(const std::shared_ptr<ParkState> &ps) {
  MG_ASSERT(!tls_pending_parks.empty() && tls_pending_parks.back() == ps,
            "DiscardPendingPark on a park that is not this thread's innermost pending park -- dropping it "
            "would strand the real one, which permanently blocks every acquisition on its storage.");
  tls_pending_parks.pop_back();
}

/// Arms every park published above `base_depth`, innermost first, and pops them. Each arming site
/// passes the depth it observed on entry, so a nested arming point can only ever arm the parks its own
/// nested execution published -- never an outer task's park, whose driver has not finished yet and for
/// which a delivered resume would be exactly the race the gate exists to prevent.
///
/// Loops rather than arming once: `ArmPark` may invoke `on_resume_`, which on the inline-resume path
/// runs a whole session chain that can publish yet another park before returning.
inline void ArmPendingParksAbove(size_t base_depth) {
  while (tls_pending_parks.size() > base_depth) {
    auto ps = std::move(tls_pending_parks.back());
    tls_pending_parks.pop_back();
    ArmPark(*ps);
  }
}

/// Scope guard around one unit of task execution: arms whatever that unit published, and does so even
/// if it exits by exception. MUST wrap EVERY site that invokes a pool task body -- the run loop's
/// three, and `PostResumeTask`'s inline fallback. A site without one leaves any park its task
/// published unarmed forever.
struct ParkArmGuard {
  ParkArmGuard() : base_depth_{tls_pending_parks.size()} {}

  ParkArmGuard(const ParkArmGuard &) = delete;
  ParkArmGuard &operator=(const ParkArmGuard &) = delete;
  ParkArmGuard(ParkArmGuard &&) = delete;
  ParkArmGuard &operator=(ParkArmGuard &&) = delete;

  // No try/catch needed: delivery cannot throw (ParkState::TakeAndInvokeOnResume is noexcept and
  // swallows), and popping a shared_ptr off the stack does not allocate. That matters here because
  // this destructor runs while a task unwinds, where an escaping exception would terminate.
  ~ParkArmGuard() { ArmPendingParksAbove(base_depth_); }

 private:
  size_t base_depth_;
};

}  // namespace memgraph::utils
