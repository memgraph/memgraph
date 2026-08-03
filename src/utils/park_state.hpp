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
/// Rationale, alternatives and failure history: specs/parkable-prepare.md.
///
/// Two independent decisions, and conflating them is the mistake this type exists to prevent:
/// `ClaimPark` decides WHO may resume a park (one winner across every wake source, ever); the gate
/// decides WHEN that resume may be delivered. A claim winner's only move is `RequestResume`, which
/// may well NOT deliver -- if the parking thread has not armed yet, the request is recorded and the
/// ARMING side delivers it later, on another thread. Losers touch nothing but `claimed`.
///
/// Must be heap-allocated, never a coroutine-frame local: `claimed` has to stay readable by a LOSING
/// waker after the winner has already driven the frame to destruction.
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
  /// INVARIANT: moves `on_resume_` OUT before invoking it, so a fired-but-not-yet-pruned `ParkState`
  /// stops owning what the closure captured. It captures a `shared_ptr<Session>`, and a `ParkState`
  /// routinely outlives its own resume, so retaining it stalls DROP DATABASE. Do not "simplify" this
  /// to a direct call. Single delivery is the gate's guarantee, so the moved-from state is never
  /// observed twice.
  void TakeAndInvokeOnResume() noexcept {
    auto on_resume = std::move(on_resume_);
    // noexcept and swallowing on purpose: callers are ~ResourceLockGuard (via the admit observer),
    // ~ParkArmGuard mid-unwind, and the ResumeAll/Sweep/Drain loops -- where an escape would abandon
    // the REST of the claimed waiters, each a permanently parked query. catch(...) not
    // catch(const std::exception &) because a non-std escape from a noexcept function terminates,
    // which is the outcome being prevented. The log is wrapped for the same reason.
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

/// THE DELIVERY GATE (specs/parkable-prepare.md). A resume must not run while the parking thread is
/// still inside its own `await_suspend` or its driver's post-`Resume()` bookkeeping, both of which
/// touch the same coroutine chain and session state. `RequestResume` and `ArmPark` are a two-party
/// rendezvous on one atomic: whoever arrives SECOND delivers, exactly once, and neither can deliver
/// early -- so the resume is free to run on ANY worker.
///
/// ASYMMETRY: over-arming is harmless; UNDER-arming bricks the whole storage, because the stranded
/// frame holds its campaign `PendingHandle` and every `can_acquire` on that storage then fails
/// forever. So every site that runs a session/Prepare task body MUST hold a `ParkArmGuard`: the pool's
/// three run-loop sites plus `PostResumeTask`'s inline fallback. (`TaskCollection::WaitOrSteal` runs a
/// task body inline with no guard on purpose -- Pull-time parallel tasks never reach the Prepare park
/// path, and its caller is itself guarded.)
///
/// Arming an ALREADY-delivered park must not happen: it would find `kResumeRequested` and invoke a
/// moved-from `on_resume_`. Each `ParkState` is pushed once and popped when armed, so it cannot.

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

/// Parks published by THIS thread still awaiting their `ArmPark`: intrusive singly-linked stack
/// (link in `ParkState::next_pending`), innermost at the head, with an explicit depth arming sites
/// key off. A STACK because task execution nests -- the inline-resume fallback re-enters the session
/// chain, which can park again while the outer park is still pending its arm.
///
/// INTRUSIVE so that publishing CANNOT THROW. Do not reintroduce a container: as a `std::vector` a
/// `bad_alloc` here left the park registered but unpublished, hence unarmable, hence a bricked
/// storage -- and `reserve` still throws while deep nesting still reallocates.
inline thread_local std::shared_ptr<ParkState> tls_pending_park_head;
inline thread_local size_t tls_pending_park_depth = 0;

/// Publishes `ps` as pending-arm for this thread. Call right after `ps` becomes visible to the
/// registries, so a wake source claiming immediately afterwards finds an arming side accounted for.
///
/// `noexcept` is a CONTRACT, not an annotation: every caller's exception-safety argument depends on
/// this either not having run or having fully succeeded. Static-asserted in tests/unit/park_state.cpp.
inline void PublishPendingPark(std::shared_ptr<ParkState> ps) noexcept {
  ps->next_pending = std::move(tls_pending_park_head);
  tls_pending_park_head = std::move(ps);
  ++tls_pending_park_depth;
}

/// Drops a pending-arm park WITHOUT arming it. Only for a parking attempt that published `ps` and then
/// self-claimed it (abandon path / shutdown self-claim), where the publisher IS the claim winner so no
/// resume can ever be requested.
///
/// `ps` must be the innermost pending park -- and NOT because nesting is impossible here; it is
/// reachable, via a `main_lock_` release reaching the admit observer. The invariant holds because any
/// nested publish is BALANCED before control returns: the inline-resume site has its own
/// `ParkArmGuard`, and `ArmPendingParksAbove` pops before arming, so the nested chain consumes exactly
/// what it published. THAT is what a future edit must preserve.
///
/// MG_ASSERT, not DMG_ASSERT: a mismatch drops somebody else's arm, i.e. bricks the storage, which
/// must not be a no-op in a release build.
inline void DiscardPendingPark(const std::shared_ptr<ParkState> &ps) {
  MG_ASSERT(tls_pending_park_head == ps,
            "DiscardPendingPark on a park that is not this thread's innermost pending park -- dropping it "
            "would strand the real one, which permanently blocks every acquisition on its storage.");
  auto next = std::move(tls_pending_park_head->next_pending);
  tls_pending_park_head = std::move(next);
  --tls_pending_park_depth;
}

/// Arms every park published above `base_depth`, innermost first, popping as it goes. Each arming site
/// passes the depth it observed on entry, so a nested arming point can only arm what its own nested
/// execution published -- never an outer task's, whose driver has not finished and for which delivery
/// is exactly the race the gate prevents.
///
/// The loop is defence in depth; nothing currently relies on iterating more than once, since a nested
/// chain has its own guard at the same base and consumes what it published. It earns its keep only if
/// a future arming site forgets its guard.
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
