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

#include <chrono>
#include <memory>

namespace memgraph::utils {

/// Timer backend type selection
enum class TimerBackendType {
  AUTO,                ///< Auto-select best backend for platform
  TIMERFD,             ///< Linux timerfd (lowest non-determinism events)
  CONDITION_VARIABLE,  ///< Portable condition_variable fallback
};

/// Abstract interface for timer backends
/// Used by ConsolidatedScheduler to wait for the next task deadline
class ITimerBackend {
 public:
  using time_point = std::chrono::steady_clock::time_point;

  virtual ~ITimerBackend() = default;

  /// Wait until the specified deadline or until cancelled
  /// @param deadline The time point to wait until
  /// @return true if deadline was reached, false if cancelled
  virtual bool WaitUntil(time_point deadline) = 0;

  /// Update the current deadline (wakes the waiting thread to re-evaluate)
  /// Call this when a new task is added with an earlier deadline
  virtual void UpdateDeadline(time_point new_deadline) = 0;

  /// Cancel any pending wait (used for shutdown)
  virtual void Cancel() = 0;

  /// Check if the backend has been cancelled
  virtual bool IsCancelled() const = 0;
};

/// Factory function to create the appropriate timer backend
/// @param type The type of backend to create (AUTO selects timerfd on Linux)
std::unique_ptr<ITimerBackend> CreateTimerBackend(TimerBackendType type = TimerBackendType::AUTO);

}  // namespace memgraph::utils
