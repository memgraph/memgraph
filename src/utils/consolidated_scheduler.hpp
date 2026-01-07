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

#include <atomic>
#include <chrono>
#include <expected>
#include <functional>
#include <memory>
#include <optional>
#include <ostream>
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
  FIXED,          ///< Fixed worker count, tasks queue up and wait
  GROW,           ///< Grow workers as needed up to max_workers
  CALLER_THREAD,  ///< Execute on dispatcher thread if no workers available (for critical tasks)
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

/// Extended schedule specification with start time alignment
/// Used for tasks that need to run at specific wall-clock aligned times
struct AlignedScheduleSpec {
  std::chrono::milliseconds interval{0};                              ///< Fixed interval scheduling
  std::optional<std::chrono::steady_clock::time_point> start_time{};  ///< First execution time (nullopt = now)
  bool one_shot{false};                                               ///< If true, task runs once then stops

  /// Create a fixed-interval schedule starting at a specific time
  /// If start_time is in the past, advances to next aligned time
  template <typename Rep, typename Period>
  static AlignedScheduleSpec StartingAt(std::chrono::duration<Rep, Period> duration,
                                        std::chrono::steady_clock::time_point start) {
    return AlignedScheduleSpec{std::chrono::duration_cast<std::chrono::milliseconds>(duration), start, false};
  }

  /// Create a fixed-interval schedule starting now
  template <typename Rep, typename Period>
  static AlignedScheduleSpec Interval(std::chrono::duration<Rep, Period> duration) {
    return AlignedScheduleSpec{std::chrono::duration_cast<std::chrono::milliseconds>(duration), std::nullopt, false};
  }

  /// Convert to basic ScheduleSpec (loses start_time info, for compatibility)
  ScheduleSpec ToBasic() const { return ScheduleSpec{interval, start_time.has_value(), one_shot}; }

  /// Check if schedule is valid (non-zero interval)
  explicit operator bool() const { return interval.count() > 0; }
};

// Forward declarations
class ConsolidatedScheduler;
class TaskHandle;

/// Error codes for task registration
enum class RegisterError : uint8_t {
  INVALID_POOL,       ///< PoolId is invalid (default-constructed or moved-from)
  SCHEDULER_STOPPED,  ///< Scheduler is not running or was shut down
};

/// Convert RegisterError to string for logging/debugging
inline const char *RegisterErrorToString(RegisterError e) {
  switch (e) {
    case RegisterError::INVALID_POOL:
      return "Invalid PoolId";
    case RegisterError::SCHEDULER_STOPPED:
      return "Scheduler not running";
  }
  return "Unknown error";
}

/// Stream operator for RegisterError
inline std::ostream &operator<<(std::ostream &os, RegisterError e) { return os << RegisterErrorToString(e); }

/// Error codes for pool registration
enum class PoolError : uint8_t {
  ALREADY_EXISTS,     ///< Pool with this name already exists
  SCHEDULER_STOPPED,  ///< Scheduler is not running or was shut down
};

/// Convert PoolError to string for logging/debugging
inline const char *PoolErrorToString(PoolError e) {
  switch (e) {
    case PoolError::ALREADY_EXISTS:
      return "Pool already exists";
    case PoolError::SCHEDULER_STOPPED:
      return "Scheduler not running";
  }
  return "Unknown error";
}

/// Stream operator for PoolError
inline std::ostream &operator<<(std::ostream &os, PoolError e) { return os << PoolErrorToString(e); }

/// Handle to a registered worker pool
/// Provides task registration methods with O(1) scheduler ownership validation
class PoolId {
 public:
  PoolId() = default;
  [[nodiscard]] bool IsValid() const { return scheduler_ != nullptr; }
  [[nodiscard]] const std::string &Name() const { return name_; }

  /// Register a repeating task to this pool
  template <typename Rep, typename Period>
  [[nodiscard]] std::expected<TaskHandle, RegisterError> Register(
      std::string name, std::chrono::duration<Rep, Period> interval, std::function<void()> callback,
      SchedulerPriority priority = SchedulerPriority::NORMAL, bool execute_immediately = false);

  /// Schedule a one-shot task to run after a delay
  template <typename Rep, typename Period>
  [[nodiscard]] std::expected<TaskHandle, RegisterError> ScheduleAfter(
      std::string name, std::chrono::duration<Rep, Period> delay, std::function<void()> callback,
      SchedulerPriority priority = SchedulerPriority::NORMAL);

  /// Schedule a one-shot task to run as soon as possible
  [[nodiscard]] std::expected<TaskHandle, RegisterError> ScheduleNow(
      std::string name, std::function<void()> callback, SchedulerPriority priority = SchedulerPriority::NORMAL);

 private:
  friend class ConsolidatedScheduler;
  PoolId(ConsolidatedScheduler *scheduler, std::string name) : scheduler_(scheduler), name_(std::move(name)) {}

  ConsolidatedScheduler *scheduler_{nullptr};
  std::string name_;
};

/// Configuration for a scheduled task (internal use)
struct TaskConfig {
  std::string name;                                       ///< Human-readable task name (for logging)
  ScheduleSpec schedule;                                  ///< When to run
  SchedulerPriority priority{SchedulerPriority::NORMAL};  ///< Execution priority (for ordering within pool)
  std::string pool_name;                                  ///< Target pool name
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
  friend class PoolId;

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

  /// Factory method - creates a ConsolidatedScheduler or returns error
  /// @return ConsolidatedScheduler on success, error string on failure
  static std::expected<ConsolidatedScheduler, std::string> Create();

  /// Get the global scheduler instance (pre-configured with default pools)
  /// Note: Aborts if scheduler creation fails (global instance must exist)
  static ConsolidatedScheduler &Global();

  ~ConsolidatedScheduler();

  ConsolidatedScheduler(const ConsolidatedScheduler &) = delete;
  ConsolidatedScheduler &operator=(const ConsolidatedScheduler &) = delete;
  ConsolidatedScheduler(ConsolidatedScheduler &&other) noexcept;
  ConsolidatedScheduler &operator=(ConsolidatedScheduler &&other) noexcept;

  /// Register a new worker pool
  /// @param config Pool configuration
  /// @return PoolId handle on success, PoolError if pool already exists or scheduler stopped
  [[nodiscard]] std::expected<PoolId, PoolError> RegisterPool(PoolConfig config);

  /// Get a handle to an existing pool by name
  /// @param name Pool name
  /// @return PoolId handle if pool exists, std::nullopt otherwise
  [[nodiscard]] std::optional<PoolId> GetPool(const std::string &name);

  /// Internal: Register a task (called by PoolId)
  /// @param config Task configuration
  /// @param callback Function to call when task is due
  /// @return Handle on success, error code on failure
  [[nodiscard]] std::expected<TaskHandle, RegisterError> RegisterInternal(TaskConfig config,
                                                                          std::function<void()> callback);

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
  ConsolidatedScheduler() = default;  // Private - use Create()
  void WakeDispatcher();              // Delegates to impl_

  class Impl;
  std::unique_ptr<Impl> impl_;
};

/// Holds the default pool handles for the global scheduler instance
/// Access via ConsolidatedScheduler::Global() paired with GlobalPools()
struct GlobalPools {
  PoolId critical;  ///< For license checks, replication heartbeats (CALLER_THREAD policy)
  PoolId general;   ///< Default pool for most tasks (FIXED policy)
  PoolId io;        ///< For IO-bound tasks like RPC/network (GROW policy)
};

/// Get the global pool handles (must be called after Global() has been accessed)
/// @return Reference to the global pools struct
GlobalPools &GetGlobalPools();

// PoolId template implementations (must be after ConsolidatedScheduler is fully defined)
template <typename Rep, typename Period>
std::expected<TaskHandle, RegisterError> PoolId::Register(std::string name, std::chrono::duration<Rep, Period> interval,
                                                          std::function<void()> callback, SchedulerPriority priority,
                                                          bool execute_immediately) {
  if (!scheduler_) {
    return std::unexpected(RegisterError::INVALID_POOL);
  }
  TaskConfig config{std::move(name), ScheduleSpec::Interval(interval, execute_immediately), priority, name_};
  return scheduler_->RegisterInternal(std::move(config), std::move(callback));
}

template <typename Rep, typename Period>
std::expected<TaskHandle, RegisterError> PoolId::ScheduleAfter(std::string name,
                                                               std::chrono::duration<Rep, Period> delay,
                                                               std::function<void()> callback,
                                                               SchedulerPriority priority) {
  if (!scheduler_) {
    return std::unexpected(RegisterError::INVALID_POOL);
  }
  TaskConfig config{std::move(name), ScheduleSpec::After(delay), priority, name_};
  return scheduler_->RegisterInternal(std::move(config), std::move(callback));
}

}  // namespace memgraph::utils
