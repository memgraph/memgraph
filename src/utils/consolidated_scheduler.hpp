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
#include <functional>
#include <memory>
#include <optional>
#include <string>
#include <thread>

#include "utils/timer_backend.hpp"

namespace memgraph::utils {

/// Task priority levels for the consolidated scheduler
/// Lower values = higher priority (executed first when multiple tasks due)
enum class SchedulerPriority : uint8_t {
  CRITICAL = 0,  ///< Replication, license checks
  HIGH = 1,      ///< GC, snapshots, memory management
  NORMAL = 2,    ///< Telemetry, audit logging
  LOW = 3,       ///< Monitoring, statistics
};

/// Specification for when a task should run
struct ScheduleSpec {
  std::chrono::milliseconds interval{0};  ///< Fixed interval scheduling
  bool execute_immediately{false};        ///< Run task once at registration

  /// Create a fixed-interval schedule
  template <typename Rep, typename Period>
  static constexpr ScheduleSpec Interval(std::chrono::duration<Rep, Period> duration, bool immediate = false) {
    return ScheduleSpec{std::chrono::duration_cast<std::chrono::milliseconds>(duration), immediate};
  }

  /// Check if schedule is valid (non-zero interval)
  constexpr explicit operator bool() const { return interval.count() > 0; }
};

/// Configuration for a scheduled task
struct TaskConfig {
  std::string name;                                       ///< Human-readable task name (for logging)
  ScheduleSpec schedule;                                  ///< When to run
  SchedulerPriority priority{SchedulerPriority::NORMAL};  ///< Execution priority
};

/// RAII handle for managing a scheduled task's lifecycle
/// When the handle is destroyed, the task is automatically stopped
class TaskHandle {
 public:
  TaskHandle() = default;
  ~TaskHandle();

  TaskHandle(const TaskHandle &) = delete;
  TaskHandle &operator=(const TaskHandle &) = delete;
  TaskHandle(TaskHandle &&other) noexcept;
  TaskHandle &operator=(TaskHandle &&other) noexcept;

  /// Check if this handle references a valid task
  /// Returns false if the task was disabled at registration time
  [[nodiscard]] bool IsValid() const;

  /// Check if the task is currently running its callback
  [[nodiscard]] bool IsRunning() const;

  /// Check if the task is paused
  [[nodiscard]] bool IsPaused() const;

  /// Pause the task (it won't execute until resumed)
  void Pause();

  /// Resume a paused task
  void Resume();

  /// Stop the task permanently (equivalent to destroying the handle)
  void Stop();

  /// Trigger the task to run immediately (outside its normal schedule)
  void TriggerNow();

  /// Update the task's schedule
  void SetSchedule(const ScheduleSpec &new_schedule);

  /// Get the next scheduled execution time
  [[nodiscard]] std::optional<std::chrono::steady_clock::time_point> NextExecution() const;

 private:
  friend class ConsolidatedScheduler;

  explicit TaskHandle(std::weak_ptr<struct TaskState> state);

  std::weak_ptr<struct TaskState> state_;
};

/// Forward declaration
struct TaskState;

/// Consolidated scheduler that manages multiple tasks with a single thread
/// Uses timerfd on Linux for minimal non-determinism events
class ConsolidatedScheduler {
 public:
  using time_point = std::chrono::steady_clock::time_point;

  /// Get the global scheduler instance
  static ConsolidatedScheduler &Global();

  /// Create a scheduler with the specified timer backend
  explicit ConsolidatedScheduler(TimerBackendType backend_type = TimerBackendType::AUTO);
  ~ConsolidatedScheduler();

  ConsolidatedScheduler(const ConsolidatedScheduler &) = delete;
  ConsolidatedScheduler &operator=(const ConsolidatedScheduler &) = delete;
  ConsolidatedScheduler(ConsolidatedScheduler &&) = delete;
  ConsolidatedScheduler &operator=(ConsolidatedScheduler &&) = delete;

  /// Register a new task with the scheduler
  /// @param config Task configuration (name, schedule, priority)
  /// @param callback Function to call when task is due
  /// @return Handle to control the task. Invalid if schedule is disabled (interval == 0)
  [[nodiscard]] TaskHandle Register(TaskConfig config, std::function<void()> callback);

  /// Convenience overload for simple interval-based tasks
  template <typename Rep, typename Period>
  [[nodiscard]] TaskHandle Register(std::string name, std::chrono::duration<Rep, Period> interval,
                                    std::function<void()> callback,
                                    SchedulerPriority priority = SchedulerPriority::NORMAL) {
    return Register(TaskConfig{std::move(name), ScheduleSpec::Interval(interval), priority}, std::move(callback));
  }

  /// Shutdown the scheduler and stop all tasks
  void Shutdown();

  /// Check if the scheduler is running
  [[nodiscard]] bool IsRunning() const;

  /// Get the number of active tasks
  [[nodiscard]] size_t TaskCount() const;

 private:
  void DispatcherLoop();
  void RemoveTask(const std::shared_ptr<TaskState> &task);
  void WakeDispatcher();

  class Impl;
  std::unique_ptr<Impl> impl_;
};

}  // namespace memgraph::utils
