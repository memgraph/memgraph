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

namespace memgraph::utils {

// Forward declaration
class TimerBackend;

/// Task priority levels for the consolidated scheduler
/// Lower values = higher priority (executed first when multiple tasks due)
enum class SchedulerPriority : uint8_t {
  CRITICAL = 0,  ///< Replication, license checks
  HIGH = 1,      ///< GC, snapshots, memory management
  NORMAL = 2,    ///< Telemetry, audit logging
  LOW = 3,       ///< Monitoring, statistics
};

/// Policy for how a pool handles work when all workers are busy
enum class PoolPolicy {
  FIXED,   ///< Fixed worker count, tasks queue up and wait
  GROW,    ///< Grow workers as needed up to max_workers
  INLINE,  ///< Execute on dispatcher thread if no workers available (for critical tasks)
};

/// Configuration for a worker pool
struct PoolConfig {
  std::string name;                      ///< Pool name (e.g., "critical", "general", "io")
  size_t min_workers{1};                 ///< Minimum workers (always running)
  size_t max_workers{1};                 ///< Maximum workers (for GROW policy)
  PoolPolicy policy{PoolPolicy::FIXED};  ///< How to handle backpressure
};

/// Specification for when a task should run
struct ScheduleSpec {
  std::chrono::milliseconds interval{0};  ///< Fixed interval scheduling
  bool execute_immediately{false};        ///< Run task once at registration
  bool one_shot{false};                   ///< If true, task runs once then stops (no reschedule)

  /// Create a fixed-interval schedule (repeating)
  template <typename Rep, typename Period>
  static constexpr ScheduleSpec Interval(std::chrono::duration<Rep, Period> duration, bool immediate = false) {
    return ScheduleSpec{std::chrono::duration_cast<std::chrono::milliseconds>(duration), immediate, false};
  }

  /// Create a one-shot timer (runs once after delay)
  template <typename Rep, typename Period>
  static constexpr ScheduleSpec After(std::chrono::duration<Rep, Period> delay) {
    return ScheduleSpec{std::chrono::duration_cast<std::chrono::milliseconds>(delay), false, true};
  }

  /// Create an immediate one-shot (runs as soon as possible, once)
  static constexpr ScheduleSpec Once() { return ScheduleSpec{std::chrono::milliseconds{1}, true, true}; }

  /// Check if schedule is valid (non-zero interval)
  constexpr explicit operator bool() const { return interval.count() > 0; }
};

/// Configuration for a scheduled task
struct TaskConfig {
  std::string name;                                       ///< Human-readable task name (for logging)
  ScheduleSpec schedule;                                  ///< When to run
  SchedulerPriority priority{SchedulerPriority::NORMAL};  ///< Execution priority (for ordering within pool)
  std::string pool{"general"};                            ///< Target pool for execution
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

/// Consolidated scheduler that manages multiple tasks with named worker pools
/// Uses timerfd on Linux for minimal non-determinism events
/// Tasks are dispatched by a single thread and executed by pool-specific worker threads
class ConsolidatedScheduler {
 public:
  using time_point = std::chrono::steady_clock::time_point;

  /// Default pool names
  static constexpr const char *kCriticalPool = "critical";  ///< For license, replication heartbeats
  static constexpr const char *kGeneralPool = "general";    ///< Default pool for most tasks
  static constexpr const char *kIoPool = "io";              ///< For IO-bound tasks (optional)

  /// Get the global scheduler instance (pre-configured with default pools)
  static ConsolidatedScheduler &Global();

  /// Create a scheduler (call RegisterPool() to add pools before registering tasks)
  ConsolidatedScheduler();
  ~ConsolidatedScheduler();

  ConsolidatedScheduler(const ConsolidatedScheduler &) = delete;
  ConsolidatedScheduler &operator=(const ConsolidatedScheduler &) = delete;
  ConsolidatedScheduler(ConsolidatedScheduler &&) = delete;
  ConsolidatedScheduler &operator=(ConsolidatedScheduler &&) = delete;

  /// Register a worker pool (must be called before registering tasks to that pool)
  /// @param config Pool configuration
  void RegisterPool(PoolConfig config);

  /// Register a new task with the scheduler
  /// @param config Task configuration (name, schedule, priority, pool)
  /// @param callback Function to call when task is due
  /// @return Handle to control the task. Invalid if schedule is disabled (interval == 0)
  [[nodiscard]] TaskHandle Register(TaskConfig config, std::function<void()> callback);

  /// Convenience overload for simple interval-based tasks (uses "general" pool)
  template <typename Rep, typename Period>
  [[nodiscard]] TaskHandle Register(std::string name, std::chrono::duration<Rep, Period> interval,
                                    std::function<void()> callback,
                                    SchedulerPriority priority = SchedulerPriority::NORMAL,
                                    std::string pool = kGeneralPool) {
    return Register(TaskConfig{std::move(name), ScheduleSpec::Interval(interval), priority, std::move(pool)},
                    std::move(callback));
  }

  /// Schedule a one-shot task to run after a delay
  template <typename Rep, typename Period>
  [[nodiscard]] TaskHandle ScheduleAfter(std::string name, std::chrono::duration<Rep, Period> delay,
                                         std::function<void()> callback,
                                         SchedulerPriority priority = SchedulerPriority::NORMAL,
                                         std::string pool = kGeneralPool) {
    return Register(TaskConfig{std::move(name), ScheduleSpec::After(delay), priority, std::move(pool)},
                    std::move(callback));
  }

  /// Schedule a one-shot task to run as soon as possible
  [[nodiscard]] TaskHandle ScheduleNow(std::string name, std::function<void()> callback,
                                       SchedulerPriority priority = SchedulerPriority::NORMAL,
                                       std::string pool = kGeneralPool) {
    return Register(TaskConfig{std::move(name), ScheduleSpec::Once(), priority, std::move(pool)}, std::move(callback));
  }

  /// Shutdown the scheduler and stop all tasks
  void Shutdown();

  /// Check if the scheduler is running
  [[nodiscard]] bool IsRunning() const;

  /// Get the number of active tasks
  [[nodiscard]] size_t TaskCount() const;

  /// Get the total number of worker threads across all pools
  [[nodiscard]] size_t WorkerCount() const;

  /// Get the number of registered pools
  [[nodiscard]] size_t PoolCount() const;

 private:
  void DispatcherLoop();
  void PoolWorkerLoop(const std::string &pool_name);
  void WakeDispatcher();

  class Impl;
  std::unique_ptr<Impl> impl_;
};

}  // namespace memgraph::utils
