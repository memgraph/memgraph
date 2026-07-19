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
#include <cstddef>
#include <memory>
#include <mutex>
#include <vector>

#include "utils/park_state.hpp"

namespace memgraph::utils {

/// Deadline-sweep machinery for IP-1's B2 (see opencode-work/resource-lock-starvation/
/// coro-prepare/ip1-design.md, REVISION 1 §B2, REVISION 2's "ParkState single-owner model", and
/// REVISION 4 R4.4/R4.5). This header is deliberately COROUTINE-AGNOSTIC: `ParkState::on_resume`
/// (see utils/park_state.hpp) is an opaque `std::function<void()>` this registry only ever
/// invokes, never a coroutine handle it resumes/destroys/inspects itself. That keeps this type
/// unit-testable with plain recording closures and independent of the `Task<T>`/awaitable
/// machinery built on top of it.
///
/// Thread-safety: `Register`/`Deregister`/`Sweep`/`Drain` may all be called concurrently from
/// different threads (the pool worker registering a park, the monitor thread sweeping, another
/// thread deregistering on the lock-release wake path). Neither `Sweep` nor `Drain` ever invokes a
/// waiter's `on_resume` while holding the internal mutex -- `on_resume` may run arbitrary
/// scheduler/session code (e.g. re-post the closure onto a worker, or re-park), so calling it
/// under the lock would risk deadlock/reentrancy exactly like `WorkerResumeEvent::NotifyAll` (see
/// worker_resume_event.hpp's C3 discussion).
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
  /// wake source (e.g. the lock-release path, or its own abandon-path claim per R4.3) and no
  /// longer needs deadline tracking. Correctness does NOT depend on this being called promptly --
  /// or at all -- for a given entry (R4.5): if it is still present when `Sweep` runs,
  /// `claimed == true` makes `Sweep` prune it without invoking `on_resume` (see `Sweep`). This is
  /// purely a cleanup optimization to keep the registry small; the hot re-park path may skip it
  /// entirely and simply let a stale, already-claimed entry be pruned on the next sweep tick.
  void Deregister(const std::shared_ptr<ParkState> &ps) {
    std::lock_guard<std::mutex> lock(mutex_);
    auto it = std::find(entries_.begin(), entries_.end(), ps);
    if (it != entries_.end()) {
      entries_.erase(it);
      size_.store(entries_.size(), std::memory_order_release);
    }
  }

  /// Sweeps the registry once, invoking `on_resume` for every waiter whose deadline has passed and
  /// that this call wins the claim on. The invoked closure is expected to hand its frame back onto
  /// its owning worker and re-probe its resource, see the deadline has passed, and throw its own
  /// timeout -- this function itself never inspects, resumes, or reschedules anything beyond
  /// calling `on_resume()`.
  ///
  /// Cheap when empty: the common case (nothing parked) is a single relaxed-ish atomic load with
  /// NO mutex acquisition and no allocation -- important since this runs on every tick of the
  /// pool's existing periodic monitor regardless of whether the deadline-park feature is in use.
  ///
  /// Never holds `mutex_` across `on_resume` (see class doc comment).
  void Sweep(std::chrono::steady_clock::time_point now) {
    if (size_.load(std::memory_order_acquire) == 0) [[likely]] {
      return;  // Cheap-when-empty fast path: no lock, no work.
    }

    // Under the lock: partition out every entry that is either past its deadline or already
    // claimed by some other wake source (dead weight we can prune here regardless of deadline,
    // R4.5's lazy-prune), and remove those from the live list. Entries that are neither stay
    // untouched.
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
    //     Sweep, a shutdown Drain, or the waiter's own abandon-path claim) loses the exchange and
    //     is simply dropped here -- pruned, no invocation, and `on_resume` is never called by this
    //     (losing) call;
    //   - an entry that is due AND not yet claimed wins the exchange and has `on_resume` invoked
    //     exactly once -- no other wake source can win it afterwards.
    for (auto &ps : due_or_dead) {
      if (now >= ps->deadline && ClaimPark(*ps)) {
        ps->on_resume();
      }
      // Else: pruned without invoking on_resume -- either it was already claimed, or (in the
      // racing-Sweep case) another concurrent caller won the claim first.
    }
  }

  /// Shutdown drain (R4.4): claims and invokes `on_resume` for EVERY currently-registered entry,
  /// regardless of deadline, so a pool teardown resumes every parked frame at least once (the
  /// frame's `on_resume` is expected to observe shutdown and drive itself to a clean bail). Never
  /// holds `mutex_` across `on_resume` (see class doc comment).
  void Drain() {
    std::vector<std::shared_ptr<ParkState>> all;
    {
      std::lock_guard<std::mutex> lock(mutex_);
      all = std::move(entries_);
      entries_.clear();
      size_.store(0, std::memory_order_release);
    }
    for (auto &ps : all) {
      if (ClaimPark(*ps)) {
        ps->on_resume();
      }
      // Else: already claimed by some other wake source -- single-owner holds, do nothing.
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
