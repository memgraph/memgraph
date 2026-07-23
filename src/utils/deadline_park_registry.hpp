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

/// Deadline-sweep machinery: parked waiters that no other wake source resumes get their
/// `on_resume` fired once their ~1s deadline passes (so a query surfaces `*AccessTimeout` instead
/// of hanging). Coroutine-agnostic like WorkerResumeEvent -- `on_resume` is an opaque closure.
///
/// Register/Deregister/Sweep/Drain are all thread-safe. As with WorkerResumeEvent (C3), neither
/// Sweep nor Drain invokes `on_resume` while holding `mutex_` (it may re-park or re-post).
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

  /// Best-effort removal when a waiter resumed via another wake source. Never required for
  /// correctness: a stale entry left behind is claimed already, so Sweep prunes it without
  /// invoking `on_resume`. Purely keeps the registry small.
  void Deregister(const std::shared_ptr<ParkState> &ps) {
    std::lock_guard<std::mutex> lock(mutex_);
    auto it = std::find(entries_.begin(), entries_.end(), ps);
    if (it != entries_.end()) {
      entries_.erase(it);
      size_.store(entries_.size(), std::memory_order_release);
    }
  }

  /// Sweeps once, invoking `on_resume` for every due waiter it wins the claim on. Cheap when empty
  /// (single atomic load, no lock/alloc) since it runs on every tick of the pool's periodic
  /// monitor. Never holds `mutex_` across `on_resume`.
  void Sweep(std::chrono::steady_clock::time_point now) {
    if (size_.load(std::memory_order_acquire) == 0) [[likely]] {
      return;
    }

    // Under the lock: partition out entries that are past their deadline OR already claimed
    // elsewhere (lazy-pruned regardless of deadline), removing them from the live list.
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

    // Outside the lock: invoke only entries that are due AND we win the claim on; anything already
    // claimed elsewhere loses the exchange and is dropped (single-owner).
    for (auto &ps : due_or_dead) {
      if (now >= ps->deadline && ClaimPark(*ps)) {
        ps->on_resume();
      }
    }
  }

  /// Shutdown drain: claims and invokes `on_resume` for every entry regardless of deadline, so a
  /// pool teardown resumes every parked frame at least once (each is expected to observe shutdown
  /// and bail). Never holds `mutex_` across `on_resume`.
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
    }
  }

 private:
  mutable std::mutex mutex_;
  std::vector<std::shared_ptr<ParkState>> entries_;
  // Lock-free size mirror read by Sweep's empty fast path. A benign race can defer noticing a new
  // entry by one sweep tick (~100ms) -- fine against the ~1s deadlines it enforces.
  std::atomic<size_t> size_{0};
};

}  // namespace memgraph::utils
