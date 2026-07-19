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
#include <chrono>
#include <coroutine>
#include <cstddef>
#include <memory>
#include <mutex>
#include <vector>

namespace memgraph::utils {

/// Deadline-sweep machinery for IP-1's B2 (see opencode-work/resource-lock-starvation/
/// coro-prepare/ip1-design.md, REVISION 1 §B2 and REVISION 2's "ParkState single-owner model").
/// This header is deliberately COROUTINE-AGNOSTIC in its logic: it only ever *holds* a
/// `std::coroutine_handle<>` opaquely and hands it back to a caller-supplied `reschedule`
/// callback -- it never resumes, destroys, or inspects the frame itself. That keeps this type
/// unit-testable with `std::noop_coroutine()` (or any other handle) and independent of the
/// `Task<T>`/awaitable machinery built on top of it.
///
/// ---------------------------------------------------------------------------------------------
/// Ownership model (R2): why ParkState is a heap `shared_ptr`, not a frame local or `weak_ptr`.
/// ---------------------------------------------------------------------------------------------
/// A parked coroutine can be woken by up to three independent sources: (i) the lock-release
/// `WorkerResumeEvent::NotifyAll` path, (ii) this registry's deadline sweep, (iii) a pool shutdown
/// drain. Exactly ONE of them may ever call `handle.resume()` on a given waiter; the others must
/// detect "already taken" and touch nothing else. `ParkState::claimed` is the single-owner flag
/// that arbitrates this -- but for it to be observable by a *losing* waker, it must remain valid
/// storage even after the *winning* waker has resumed (and thereby possibly destroyed) the
/// coroutine frame. If `ParkState` lived inside the coroutine frame (a frame local) or were only
/// reachable via `weak_ptr` from the registry, a losing waker could race the frame's destruction
/// and read a dangling/freed `claimed` flag.
///
/// The fix is to give `ParkState` its own heap allocation, independent of the frame, kept alive by
/// `shared_ptr` refcounting from up to three simultaneous owners: the coroutine frame itself (one
/// ref, so the frame can find its own `ParkState` on each re-probe), the `WorkerResumeEvent`
/// waiter entry (one ref, for the lock-release wake path), and this registry's own entry (one
/// ref, for the deadline-sweep wake path). `claimed` therefore stays valid for as long as ANY of
/// those three owners is still holding a reference -- in particular, for as long as this registry
/// has not yet pruned its own entry, even if the frame that once pointed to it is long gone. This
/// is why `Register` takes a `shared_ptr` (not a `weak_ptr`): a `weak_ptr` would let the
/// registry's ref lapse the instant every OTHER owner dropped theirs, which is exactly backwards
/// -- we want the registry itself to be one of the things keeping `claimed` alive until it prunes
/// the entry on its own schedule (a claimed-but-not-yet-pruned entry must still be readable by
/// `Sweep` so it can be dropped cleanly instead of dereferencing freed memory).
///
/// Only the caller that wins the claim (see `ClaimPark`) may ever touch `handle` (read it, pass it
/// to `reschedule`, resume it, or destroy it). A losing caller must not dereference `handle` at
/// all -- it may only observe that `claimed` was already true and walk away.
struct ParkState {
  std::coroutine_handle<> handle;
  size_t worker_id{0};
  std::chrono::steady_clock::time_point deadline;
  std::atomic<bool> claimed{false};
};

/// Attempts to claim `ps` for exactly one wake source. Returns true to EXACTLY ONE caller across
/// all wake sources (the lock-release `NotifyAll` callback, `DeadlineParkRegistry::Sweep`, and any
/// shutdown drain) that ever race on the same `ParkState` -- every other caller, whether racing
/// concurrently or arriving after the fact, gets false. Only the caller that receives `true` may
/// touch `ps.handle` (resume it, hand it to a reschedule callback, or destroy it); a caller that
/// receives `false` must not read `ps.handle` and must simply stop.
inline bool ClaimPark(ParkState &ps) { return !ps.claimed.exchange(true, std::memory_order_acq_rel); }

/// Registry of parked waiters awaiting a resource, swept periodically by the pool's existing
/// monitor (`sched_mon`) so an absolute deadline is honored without a dedicated timer thread (R1
/// §B2). Entries are `shared_ptr<ParkState>` (see the ownership discussion above) so a claimed
/// entry can be pruned here safely even though its coroutine frame may already be gone.
///
/// Thread-safety: `Register`/`Deregister`/`Sweep` may all be called concurrently from different
/// threads (the pool worker registering a park, the monitor thread sweeping, another thread
/// deregistering on the lock-release wake path). `Sweep` never invokes `reschedule` while holding
/// the internal mutex -- `reschedule` may run arbitrary scheduler code (e.g. re-post the closure
/// onto a worker), so calling it under the lock would risk deadlock/reentrancy exactly like
/// `WorkerResumeEvent::NotifyAll` (see worker_resume_event.hpp's C3 discussion).
class DeadlineParkRegistry {
 public:
  DeadlineParkRegistry() = default;

  DeadlineParkRegistry(const DeadlineParkRegistry &) = delete;
  DeadlineParkRegistry &operator=(const DeadlineParkRegistry &) = delete;
  DeadlineParkRegistry(DeadlineParkRegistry &&) = delete;
  DeadlineParkRegistry &operator=(DeadlineParkRegistry &&) = delete;

  ~DeadlineParkRegistry() = default;

  /// Registers `ps` for deadline sweeping. Safe to call from any thread.
  void Register(std::shared_ptr<ParkState> ps) {
    std::lock_guard<std::mutex> lock(mutex_);
    entries_.push_back(std::move(ps));
    size_.store(entries_.size(), std::memory_order_release);
  }

  /// Best-effort removal of `ps` from the registry, used when a waiter resumes via a different
  /// wake source (e.g. the lock-release path) and no longer needs deadline tracking. Correctness
  /// does NOT depend on this being called promptly -- or at all -- for a given entry: if it is
  /// still present when `Sweep` runs, `claimed == true` makes `Sweep` prune it without
  /// rescheduling (see `Sweep`). This is purely a cleanup optimization to keep the registry small.
  void Deregister(const std::shared_ptr<ParkState> &ps) {
    std::lock_guard<std::mutex> lock(mutex_);
    auto it = std::find(entries_.begin(), entries_.end(), ps);
    if (it != entries_.end()) {
      entries_.erase(it);
      size_.store(entries_.size(), std::memory_order_release);
    }
  }

  /// Sweeps the registry once, resuming (via `reschedule`) every waiter whose deadline has passed
  /// and that this call wins the claim on. `reschedule(worker_id, handle)` is expected to hand the
  /// handle back onto its owning worker (e.g. `PriorityThreadPool::RescheduleTaskOnWorker`) to
  /// resume it there; the resumed frame is expected to re-probe its resource, see the deadline has
  /// passed, and throw its own timeout -- this function itself never inspects or resumes `handle`.
  ///
  /// Cheap when empty: the common case (nothing parked) is a single relaxed-ish atomic load with
  /// NO mutex acquisition and no allocation -- important since this runs on every tick of the
  /// pool's existing periodic monitor regardless of whether the deadline-park feature is in use.
  ///
  /// Never holds `mutex_` across `reschedule` (see class doc comment).
  template <class RescheduleFn>
  void Sweep(std::chrono::steady_clock::time_point now, RescheduleFn &&reschedule) {
    if (size_.load(std::memory_order_acquire) == 0) [[likely]] {
      return;  // Cheap-when-empty fast path: no lock, no work.
    }

    // Under the lock: partition out every entry that is either past its deadline or already
    // claimed by some other wake source (dead weight we can prune here regardless of deadline),
    // and remove those from the live list. Entries that are neither stay untouched.
    std::vector<std::shared_ptr<ParkState>> due_or_dead;
    {
      std::lock_guard<std::mutex> lock(mutex_);
      auto is_due_or_dead = [&](const std::shared_ptr<ParkState> &ps) {
        return now >= ps->deadline || ps->claimed.load(std::memory_order_acquire);
      };
      auto first_dead = std::partition(
          entries_.begin(), entries_.end(), [&](const std::shared_ptr<ParkState> &ps) { return !is_due_or_dead(ps); });
      due_or_dead.assign(std::make_move_iterator(first_dead), std::make_move_iterator(entries_.end()));
      entries_.erase(first_dead, entries_.end());
      size_.store(entries_.size(), std::memory_order_release);
    }

    // Outside the lock: for each collected entry, try to claim it. `ClaimPark` alone correctly
    // implements BOTH desired behaviors:
    //   - an entry already claimed by another wake source (lock-release NotifyAll, a concurrent
    //     Sweep, or shutdown) loses the exchange and is simply dropped here -- pruned, no
    //     reschedule, and `handle` is never touched by this (losing) call;
    //   - an entry that is due AND not yet claimed wins the exchange and is rescheduled exactly
    //     once -- no other wake source can win it afterwards.
    for (auto &ps : due_or_dead) {
      if (now >= ps->deadline && ClaimPark(*ps)) {
        reschedule(ps->worker_id, ps->handle);
      }
      // Else: pruned without rescheduling -- either it was already claimed, or (in the racing-
      // Sweep case) another concurrent caller won the claim first.
    }
  }

 private:
  mutable std::mutex mutex_;
  std::vector<std::shared_ptr<ParkState>> entries_;
  // Relaxed-ish size mirror of entries_.size(), read WITHOUT the mutex by Sweep's empty fast
  // path. A benign race (Register racing a Sweep's fast-path load) can at worst defer noticing a
  // freshly-registered entry until the next sweep tick -- acceptable since the sweep resolution is
  // already coarse (one monitor period, ~100ms) relative to the ~1s deadlines it enforces.
  std::atomic<size_t> size_{0};
};

}  // namespace memgraph::utils
