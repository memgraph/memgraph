// Copyright 2025 Memgraph Ltd.
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

#ifdef __linux__
#include <sys/epoll.h>
#include <sys/eventfd.h>
#include <sys/timerfd.h>
#include <unistd.h>
#else
#include <condition_variable>
#include <mutex>
#endif

namespace memgraph::utils {

/// Timer backend for ConsolidatedScheduler
/// Uses timerfd on Linux (lowest non-determinism events), condition_variable fallback elsewhere
class TimerBackend {
 public:
  using time_point = std::chrono::steady_clock::time_point;

  TimerBackend();
  ~TimerBackend();

  TimerBackend(const TimerBackend &) = delete;
  TimerBackend &operator=(const TimerBackend &) = delete;
  TimerBackend(TimerBackend &&) = delete;
  TimerBackend &operator=(TimerBackend &&) = delete;

  /// Wait until the specified deadline or until cancelled
  /// @param deadline The time point to wait until
  /// @return true if deadline was reached, false if cancelled
  bool WaitUntil(time_point deadline);

  /// Update the current deadline (wakes the waiting thread to re-evaluate)
  /// Call this when a new task is added with an earlier deadline
  void UpdateDeadline(time_point new_deadline);

  /// Cancel any pending wait (used for shutdown)
  void Cancel();

  /// Check if the backend has been cancelled
  bool IsCancelled() const { return cancelled_.load(std::memory_order_acquire); }

 private:
#ifdef __linux__
  void SetTimerDeadline(time_point deadline);

  int timer_fd_{-1};
  int cancel_fd_{-1};
  int epoll_fd_{-1};
#else
  mutable std::mutex mutex_;
  std::condition_variable cv_;
  bool deadline_updated_{false};
#endif
  std::atomic<bool> cancelled_{false};
};

}  // namespace memgraph::utils
