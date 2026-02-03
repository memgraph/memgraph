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

#ifndef __linux__
#error "TimerBackend requires Linux (uses timerfd/eventfd/epoll)"
#endif

#include <sys/epoll.h>
#include <sys/eventfd.h>
#include <sys/timerfd.h>
#include <unistd.h>

#include <atomic>
#include <chrono>
#include <expected>
#include <string>

namespace memgraph::utils {

/// Error information for TimerBackend creation failures
struct TimerBackendError {
  std::string message;
  int errno_value;
};

/// Timer backend for ConsolidatedScheduler
/// Uses timerfd on Linux for minimal non-determinism events
///
/// Implementation notes:
/// - Uses timerfd with CLOCK_MONOTONIC for reliable timing unaffected by system clock changes
/// - Uses eventfd for cancel/wake signaling (without EFD_SEMAPHORE - multiple writes consolidate)
/// - Uses epoll for efficient multiplexing of timer and cancel events
/// - Level-triggered epoll (default) - safer than edge-triggered, no risk of missing events
class TimerBackend {
 public:
  using time_point = std::chrono::steady_clock::time_point;

  /// Factory method - creates a TimerBackend or returns error
  /// @return TimerBackend on success, TimerBackendError on failure
  static std::expected<TimerBackend, TimerBackendError> Create();

  ~TimerBackend();

  TimerBackend(const TimerBackend &) = delete;
  TimerBackend &operator=(const TimerBackend &) = delete;
  TimerBackend(TimerBackend &&other) noexcept;
  TimerBackend &operator=(TimerBackend &&other) noexcept;

  /// Wait until the specified deadline or until cancelled
  /// @param deadline The time point to wait until
  /// @return true if deadline was reached, false if cancelled or interrupted
  bool WaitUntil(time_point deadline);

  /// Update the current deadline (wakes the waiting thread to re-evaluate)
  /// Call this when a new task is added with an earlier deadline
  void UpdateDeadline(time_point new_deadline);

  /// Cancel any pending wait (used for shutdown)
  void Cancel();

  /// Check if the backend has been cancelled
  bool IsCancelled() const { return cancelled_.load(std::memory_order_acquire); }

 private:
  TimerBackend() = default;  // Private - use Create()

  void SetTimerDeadline(time_point deadline);

  int timer_fd_{-1};
  int cancel_fd_{-1};
  int epoll_fd_{-1};
  std::atomic<bool> cancelled_{false};
};

}  // namespace memgraph::utils
