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

/// Single-owner park descriptor shared by `WorkerResumeEvent` and `DeadlineParkRegistry`.
///
/// `on_resume` is an opaque closure rather than a bare `std::coroutine_handle<>` because resuming a
/// parked Prepare is not just "continue this frame": it must keep the owning session alive across a
/// cross-thread resume, detect pool shutdown before touching per-database state, and re-drive the
/// connection's post-Execute bookkeeping once the frame completes. Keeping that behind a
/// `std::function` is what lets the two registries stay coroutine-agnostic and reusable.
///
/// Only the caller that wins `ClaimPark` may cause `on_resume` to run -- exactly one winner across
/// every wake source that can race here (lock-release `WorkerResumeEvent::NotifyAll`,
/// `DeadlineParkRegistry::Sweep`, either registry's shutdown `Drain()`, and the awaitable's own
/// abandon-path claim). Losers must not ask for a resume and must not touch anything but `claimed`.
///
/// "May cause to run", NOT "may invoke": the winner's only move is `RequestResume`, which may well
/// NOT deliver -- if the parking thread has not armed yet, the winner records the request and the
/// ARMING side delivers later, on another thread. Claim decides WHO, the gate decides WHEN.
///
/// Heap-allocated via `shared_ptr` rather than living in the coroutine frame because `claimed` must
/// stay readable by a LOSING waker even after the winner has driven the frame to completion and
/// destruction. Refcounting from every registry it is registered in keeps that storage valid.
class ParkState;
inline void PublishPendingPark(std::shared_ptr<ParkState> ps) noexcept;
inline void DiscardPendingPark(const std::shared_ptr<ParkState> &ps);
inline void ArmPendingParksAbove(size_t base_depth);

/// Delivery gate (see `RequestResume`/`ArmPark`). Distinct from `claimed`: `claimed` decides WHO may
/// resume, `gate` decides WHEN that resume may be delivered.
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
  /// Load-bearing, not tidiness: `on_resume_` captures a `shared_ptr<Session>`, and a `ParkState`
  /// routinely outlives its own resume (`DeadlineParkRegistry::Sweep` erases its own entry, but the
  /// twin entry in `Storage::main_lock_resume_event_` is not pruned until that event next notifies).
  /// A fired-but-unpruned `ParkState` still holding the Session keeps a `DatabaseAccess` alive, and
  /// with it `Gatekeeper<Database>::count_` above zero -- which is what stalls DROP DATABASE.
  /// Releasing here makes the leftover entry inert however late it is pruned.
  ///
  /// The gate guarantees a single delivery, so the moved-from state is never seen by a second call.
  void TakeAndInvokeOnResume() noexcept {
    auto on_resume = std::move(on_resume_);
    // Never propagate; delivery allocates (it posts a task) and every caller is somewhere a throw
    // does disproportionate damage: inside ~ResourceLockGuard (via the admit observer), inside
    // ~ParkArmGuard during unwinding, or mid-loop in ResumeAll/Sweep/Drain -- where an escape would
    // abandon the REST of the waiters, each one a permanently parked query. Stranding the single
    // query that failed, loudly, is the least-bad outcome; this is why callers may treat
    // RequestResume/ArmPark as non-throwing.
    //
    // catch(...) rather than catch(const std::exception &) because this function is noexcept: a
    // non-std exception escaping would call std::terminate, the exact outcome being prevented. The
    // log is itself wrapped for the same reason -- spdlog can throw, here on the way out of a failure.
    try {
      on_resume();
    } catch (...) {
      try {
        spdlog::critical("Failed to deliver a parked query's resume. That query will not make progress.");
      } catch (...) {  // NOLINT(bugprone-empty-catch)
      }
    }
  }

  /// Private, reachable ONLY through `RequestResume`/`ArmPark`: a wake source invoking this directly
  /// would resume a frame whose parking thread may still be inside `await_suspend` -- the
  /// use-after-free the gate exists to prevent. Encapsulation makes that a compile error.
  std::function<void()> on_resume_;

  /// Intrusive link for the publishing thread's pending-arm stack (`tls_pending_park_head` below).
  /// Lives here, in storage the publisher already owns, so publishing allocates nothing and therefore
  /// cannot throw -- load-bearing; see `PublishPendingPark`.
  ///
  /// Private (friended to the three stack functions) to enforce rather than merely document that only
  /// the publishing thread touches it: a wake source on another thread touches `claimed`, `gate` and
  /// `on_resume_`, never this. It must also never be re-linked while already on a stack -- the
  /// per-attempt `make_shared` guarantees that, and splicing two threads' chains would be silent.
  std::shared_ptr<ParkState> next_pending;

  friend void RequestResume(ParkState &);
  friend void ArmPark(ParkState &);
  friend void PublishPendingPark(std::shared_ptr<ParkState>) noexcept;
  friend void DiscardPendingPark(const std::shared_ptr<ParkState> &);
  friend void ArmPendingParksAbove(size_t);
};

/// Attempts to claim `ps` for exactly one wake source. Returns true to EXACTLY ONE caller across all
/// wake sources that ever race on the same `ParkState`; everyone else, racing or late, gets false.
/// Only the winner may ask for a resume (via `RequestResume`); a loser must simply stop. That
/// includes the awaitable's own abandon-path claim, which loses precisely when another wake source
/// got there first and is already driving the resume.
inline bool ClaimPark(ParkState &ps) { return !ps.claimed.exchange(true, std::memory_order_acq_rel); }

/// THE DELIVERY GATE: why winning `ClaimPark` is not enough to resume.
///
/// A park is published from INSIDE the parking thread's `await_suspend`, and that thread keeps running
/// afterwards: it finishes `await_suspend` (register-before-recheck re-probe, shutdown self-claim),
/// then unwinds through `coroutine_handle::resume()` into its DRIVER, which still has post-`Resume()`
/// bookkeeping to do (`Session::RunLoop` must observe `!Done()` and retain the connection's single
/// in-flight slot). None of that may run concurrently with a resume, which re-enters the same
/// coroutine chain and the same session state.
///
/// That exclusion used to come for free from PINNING the resume to the parking worker, since a
/// same-worker task cannot start until that worker returns to its run loop. But pinning conflated WHEN
/// the resume may run with WHERE, and the WHERE half made every parked query's wake latency hostage to
/// one worker's backlog. This gate keeps the WHEN and drops the WHERE: a two-party rendezvous on one
/// atomic between the claim winner (`RequestResume`) and the parking thread's task boundary
/// (`ArmPark`). Each exchanges in its marker and reads the other's, so whoever arrives SECOND delivers
/// exactly once and neither can deliver early. `on_resume` may then post to ANY worker.
///
/// The asymmetry that matters: arming a park nobody requested a resume for is harmless, while
/// UNDER-arming hangs it forever -- and not only that query. The parked frame holds its campaign-long
/// `PendingHandle`, which keeps `unique_pending_count` above zero and makes `can_acquire<WRITE>`,
/// `<READ>` and `<READ_ONLY>` all permanently false on that storage, so untimed acquirers (TTL thread,
/// replication apply) block forever and the database is bricked until restart. Hence arming lives in
/// `ParkArmGuard`, which must wrap EVERY site that runs a session/Prepare task body: the pool's three
/// run-loop sites plus `PostResumeTask`'s inline fallback. (`TaskCollection::WaitOrSteal` also runs a
/// task body inline and deliberately has no guard -- those are Pull-time parallel tasks that never
/// reach the Prepare park path, and its caller is itself inside a guarded task, so any park published
/// underneath is armed by that outer guard once the inline body returns.)
///
/// Arming an ALREADY-delivered park must not happen -- it would find `kResumeRequested` and invoke a
/// moved-from `on_resume_`. Each `ParkState` is pushed once and popped when armed, so it cannot. (That
/// mistake used to be a double `resume()`, i.e. a use-after-free; it now merely logs, which is an
/// improvement and not a licence to rely on it.)

/// Called by the winner of `ClaimPark` -- the ONLY way a wake source can ask for a resume. Delivers
/// here iff the parking thread already armed; otherwise the arming side will.
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

/// Parks published by THIS thread still awaiting their `ArmPark`. Intrusive singly-linked stack
/// (link in `ParkState::next_pending`), innermost at the head, with an explicit depth that arming
/// sites key off (see `ParkArmGuard`).
///
/// A STACK rather than a single slot, because task execution nests: `PostResumeTask`'s
/// all-workers-stopped fallback runs a resume INLINE on the claiming thread, and that resume re-enters
/// the session chain (`h.resume()` -> resumed hook -> `Session::RunLoop` -> `Execute()` -> possibly a
/// new query that parks) while the OUTER park is still pending its arm. With one slot the inner publish
/// silently overwrote the outer, leaving it registered at `gate == kParking` -- unarmable, so its frame
/// pins `unique_pending_count` forever and bricks the storage (see the gate's asymmetry note above).
/// A stack makes that nesting representable instead of fatal, which is why this is not just an assert.
///
/// INTRUSIVE specifically so publishing CANNOT THROW. As a `std::vector` this was a blocker:
/// `push_back` allocates, and a `bad_alloc` left `ps` registered on the wake event but absent from the
/// stack. A wake source claiming in that window made the publisher lose `ClaimPark`, conclude
/// "somebody else will resume us" and suspend -- but nothing can arm a park that was never published,
/// so the frame never resumed and bricked the storage exactly as above. The fix is not a bigger
/// `catch`, it is making the failure unrepresentable: `ParkState` is already heap-allocated before
/// publication, so linking costs two `shared_ptr` assignments and no allocation. Do not reintroduce a
/// container -- `reserve` still throws, and deep enough nesting still reallocates.
inline thread_local std::shared_ptr<ParkState> tls_pending_park_head;
inline thread_local size_t tls_pending_park_depth = 0;

/// Publishes `ps` as pending-arm for this thread. Call right after `ps` becomes visible to the
/// registries (i.e. right after a successful `WorkerResumeEvent::RegisterWaiter`), so that a wake
/// source claiming immediately afterwards finds a `ParkState` whose arming side is accounted for.
///
/// `noexcept` is part of the contract, not an annotation: every caller's exception-safety argument
/// depends on this call either not running yet or having fully succeeded. Static-asserted in
/// tests/unit/park_state.cpp so the guarantee cannot be lost silently.
inline void PublishPendingPark(std::shared_ptr<ParkState> ps) noexcept {
  ps->next_pending = std::move(tls_pending_park_head);
  tls_pending_park_head = std::move(ps);
  ++tls_pending_park_depth;
}

/// Drops a pending-arm park WITHOUT arming it. Only for a parking attempt that published `ps` and then
/// self-claimed it (abandon path / shutdown self-claim): the publisher is itself the claim winner, so
/// no resume will ever be requested and arming would be a no-op -- dropping it merely avoids holding
/// the `ParkState` alive until this thread's next arming point.
///
/// `ps` must be the innermost pending park. Note the reason is NOT "no nested task execution can
/// happen in between" -- nesting IS reachable here: releasing a `main_lock_` hold inside that window
/// reaches ResourceLock's admit observer, which can claim a foreign park and, with every worker
/// stopped, run its resume inline on this thread. What holds the invariant is that such a nested
/// publish is BALANCED before control returns: the inline-resume site runs under its own
/// `ParkArmGuard`, and `ArmPendingParksAbove` pops each park before arming, so the nested chain
/// consumes exactly what it published and the head is `ps` again by the time we get here. THAT is the
/// property a future edit must preserve.
///
/// MG_ASSERT, not DMG_ASSERT: a mismatch drops somebody else's arm, which is the brick-the-database
/// failure above and must not be a no-op in a release build.
inline void DiscardPendingPark(const std::shared_ptr<ParkState> &ps) {
  MG_ASSERT(tls_pending_park_head == ps,
            "DiscardPendingPark on a park that is not this thread's innermost pending park -- dropping it "
            "would strand the real one, which permanently blocks every acquisition on its storage.");
  auto next = std::move(tls_pending_park_head->next_pending);
  tls_pending_park_head = std::move(next);
  --tls_pending_park_depth;
}

/// Arms every park published above `base_depth`, innermost first, popping as it goes. Each arming site
/// passes the depth it observed on entry, so a nested arming point can only arm the parks its own
/// nested execution published -- never an outer task's, whose driver has not finished and for which a
/// delivered resume is exactly the race the gate prevents.
///
/// The loop is DEFENCE IN DEPTH; nothing currently relies on iterating more than once. `ArmPark` can
/// invoke `on_resume_`, which on the inline path runs a session chain that may publish another park --
/// but that chain has its OWN `ParkArmGuard` whose base equals ours (we pop before arming), so it
/// consumes what it published and this loop's re-test finds nothing. The loop earns its keep only if a
/// future arming site forgets its guard.
inline void ArmPendingParksAbove(size_t base_depth) {
  while (tls_pending_park_depth > base_depth) {
    auto ps = std::move(tls_pending_park_head);
    tls_pending_park_head = std::move(ps->next_pending);
    --tls_pending_park_depth;
    // Popped BEFORE arming, deliberately: `ArmPark` can invoke `on_resume_`, which on the inline path
    // re-enters a session chain that may publish a park of its own. Popping first means that nested
    // publish sees a consistent stack and its own guard's base equals ours.
    ArmPark(*ps);
  }
}

/// Scope guard around one unit of task execution: arms whatever that unit published, and does so even
/// if it exits by exception. MUST wrap EVERY site that invokes a pool task body -- the run loop's
/// three, and `PostResumeTask`'s inline fallback. A site without one leaves any park its task
/// published unarmed forever.
struct ParkArmGuard {
  ParkArmGuard() : base_depth_{tls_pending_park_depth} {}

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
