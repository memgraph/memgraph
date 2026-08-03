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

/// Timeout backstop for parked waiters. Coroutine-agnostic: `on_resume` is an opaque closure this
/// registry only invokes. All four methods are safe to call concurrently.
///
/// INVARIANT: `mutex_` is never held across `on_resume` -- it runs arbitrary scheduler/session code and
/// may re-park, so invoking it under the lock risks deadlock.
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

  /// Best-effort. Correctness does NOT depend on it: a still-present entry is already `claimed`, so
  /// `Sweep` prunes it without invoking anything.
  void Deregister(const std::shared_ptr<ParkState> &ps) {
    std::lock_guard<std::mutex> lock(mutex_);
    auto it = std::find(entries_.begin(), entries_.end(), ps);
    if (it != entries_.end()) {
      entries_.erase(it);
      size_.store(entries_.size(), std::memory_order_release);
    }
  }

  /// Exposed for TESTS: the deadline half of the post-resume prune is otherwise unobservable. Same
  /// lock-free mirror as `Sweep`'s fast path, with the same benign staleness.
  size_t Size() const { return size_.load(std::memory_order_acquire); }

  /// Sweeps once, delivering to every waiter past its deadline whose claim this call wins. The closure
  /// re-probes and throws its own timeout; this never inspects or reschedules anything.
  ///
  /// Empty is ONE atomic load, no mutex -- it runs on every monitor tick whether or not the feature is
  /// in use.
  void Sweep(std::chrono::steady_clock::time_point now) {
    if (size_.load(std::memory_order_acquire) == 0) [[likely]] {
      return;  // Cheap-when-empty fast path: no lock, no work.
    }

    // Under the lock: partition out everything past its deadline OR already claimed elsewhere (dead
    // weight, prunable regardless of deadline). Entries that are neither stay untouched.
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

    // Outside the lock. `ClaimPark` alone gets both cases right: an entry already claimed elsewhere
    // loses the exchange and is dropped without invocation, while a due-and-unclaimed one wins and is
    // delivered exactly once.
    for (auto &ps : due_or_dead) {
      if (now >= ps->deadline && ClaimPark(*ps)) {
        // Via the delivery gate, not `on_resume()` directly -- the parking thread may still be
        // inside its own await_suspend/driver (park_state.hpp). A deadline that expires inside that
        // window is delivered by the arming side the moment it closes.
        RequestResume(*ps);
      }
      // Else: pruned without invoking on_resume -- either it was already claimed, or (in the
      // racing-Sweep case) another concurrent caller won the claim first.
    }
  }

  /// Claims and delivers to EVERY entry regardless of deadline, so a pool teardown resumes every
  /// parked frame at least once; each is expected to observe shutdown and bail cleanly.
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
        RequestResume(*ps);  // delivery gate, see park_state.hpp
      }
      // Else: already claimed by some other wake source -- single-owner holds, do nothing.
    }
  }

 private:
  mutable std::mutex mutex_;
  std::vector<std::shared_ptr<ParkState>> entries_;
  // Size mirror read WITHOUT the mutex by Sweep's fast path. The benign race can at worst defer
  // noticing a new entry by one ~100ms tick, against the ~1s deadlines it enforces.
  std::atomic<size_t> size_{0};
};

}  // namespace memgraph::utils
