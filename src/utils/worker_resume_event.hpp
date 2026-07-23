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

/// Pool-agnostic event a parked coroutine registers on, notified by a lock-releaser once the
/// resource it waited on might be free. Knows nothing of the pool/tasks/coroutine frames: each
/// waiter is a `shared_ptr<ParkState>` whose `on_resume` closure encapsulates "wake this up".
///
/// Two-sided wakeup protocol (the `WaitersPending()` fast path is only sound if BOTH sides obey
/// it; any other ordering can lose a wakeup and hang a query past the ~1s `*AccessTimeout`):
///   Waiter: capture `Epoch()` BEFORE probing the resource; if still blocked, `RegisterWaiter(ps,
///   epoch)` -- false means the epoch moved (a release raced in), so re-probe instead of parking.
///   After a successful register, re-probe once more; if it now succeeds take the abandon path
///   (win `ClaimPark` on your own ParkState to proceed synchronously, else treat as truly parked).
///   Releaser: publish the released state FIRST, then if `WaitersPending() > 0` call `NotifyAll()`
///   unconditionally.
///
/// Single-owner resume: `NotifyAll`/`Drain` move `waiters_` out under `mutex_`, then `ClaimPark`
/// each outside the lock -- combined with the same exchange in DeadlineParkRegistry and the
/// abandon path, every waiter's `on_resume` fires from exactly one wake source, ever.
/// C3: `mutex_` is always released before any `on_resume` runs (it may re-enter this object), and
/// NotifyAll must not be called while holding the resource's own internal lock.
class WorkerResumeEvent {
 public:
  WorkerResumeEvent() = default;

  WorkerResumeEvent(const WorkerResumeEvent &) = delete;
  WorkerResumeEvent &operator=(const WorkerResumeEvent &) = delete;
  WorkerResumeEvent(WorkerResumeEvent &&) = delete;
  WorkerResumeEvent &operator=(WorkerResumeEvent &&) = delete;

  ~WorkerResumeEvent() = default;

  /// Current epoch. Capture BEFORE the non-blocking probe (capturing after reopens the lost-wakeup
  /// race this class closes).
  uint64_t Epoch() const { return epoch_.load(std::memory_order_acquire); }

  /// Registers `ps` as parked iff the epoch has not moved since `expected_epoch`; false means an
  /// intervening NotifyAll bumped it -- do not park, re-probe. The relaxed epoch read is safe
  /// because `mutex_` (also held by NotifyAll's bump) provides the happens-before.
  bool RegisterWaiter(std::shared_ptr<ParkState> ps, uint64_t expected_epoch) {
    std::lock_guard<std::mutex> lock(mutex_);
    if (epoch_.load(std::memory_order_relaxed) != expected_epoch) {
      return false;
    }
    waiters_.push_back(std::move(ps));
    waiters_pending_.fetch_add(1, std::memory_order_acq_rel);
    return true;
  }

  /// Best-effort cleanup by an abandon-path waiter that already won its own `ClaimPark`.
  /// Correctness never depends on it (NotifyAll/Drain re-check ClaimPark anyway); it just keeps the
  /// counters accurate sooner. The counter is decremented only when `ps` was actually found -- an
  /// unconditional decrement could double-subtract against a concurrent NotifyAll's bulk fetch_sub
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

  /// Wakes every registered waiter: `ClaimPark` each and invoke `on_resume` only on a win (a waiter
  /// already claimed elsewhere is dropped -- this is what makes single-ownership hold across
  /// registries). Bumps `epoch_` so a mid-registration waiter re-probes instead of parking.
  /// C3: `mutex_` is released before any `on_resume` runs (it may re-enter this object).
  void NotifyAll() { ResumeAll(/*bump_epoch=*/true); }

  /// Shutdown drain: like NotifyAll but without an epoch bump (shutdown is terminal, not a
  /// re-probeable transition). Every parked frame's `on_resume` is expected to observe
  /// `IsShuttingDown()` and bail cleanly.
  void Drain() { ResumeAll(/*bump_epoch=*/false); }

  /// Lock-free releaser gate: if 0, nothing to wake and NotifyAll can be skipped. Sound only with
  /// the ordering rules above.
  size_t WaitersPending() const { return waiters_pending_.load(std::memory_order_acquire); }

 private:
  /// Move all waiters out under `mutex_`, release, then claim+invoke each outside the lock (C3).
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
    // mutex_ released here -- on_resume() runs outside the lock (C3).
    for (auto &ps : local) {
      // Skip if another wake source already claimed it -- single-owner holds.
      if (ClaimPark(*ps)) {
        ps->on_resume();
      }
    }
  }

  std::mutex mutex_;
  std::atomic<uint64_t> epoch_{0};
  std::atomic<size_t> waiters_pending_{0};
  std::vector<std::shared_ptr<ParkState>> waiters_;
};

}  // namespace memgraph::utils
