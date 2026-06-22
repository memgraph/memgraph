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
#include <condition_variable>
#include <coroutine>
#include <cstdint>
#include <functional>
#include <memory>
#include <mutex>
#include <queue>
#include <thread>

#include "utils/logging.hpp"
#include "utils/priorities.hpp"
#include "utils/worker_yield_signal.hpp"

namespace memgraph::utils {
class PriorityThreadPool;
class WorkerResumeEvent;
class TaskCollection;  // Forward declaration

// Thread-safe mask that returns the position of first set bit
class HotMask {
 public:
  static constexpr auto kMaxElements = 1024U;

  explicit HotMask(uint16_t n_elements)
      :
#ifndef NDEBUG
        n_elements_{n_elements},
#endif
        n_groups_{GetNumGroups(n_elements)} {
  }

  inline void Set(const uint64_t id) {
    DMG_ASSERT(id < n_elements_, "Trying to set out-of-bounds");
    hot_masks_[GetGroup(id)].fetch_or(GroupMask(id), std::memory_order::acq_rel);
  }

  inline void Reset(const uint64_t id) {
    DMG_ASSERT(id < n_elements_, "Trying to reset out-of-bounds");
    hot_masks_[GetGroup(id)].fetch_and(~GroupMask(id), std::memory_order::acq_rel);
  }

  // Returns the position of the first set bit and resets it
  std::optional<uint16_t> GetHotElement();

 private:
  static constexpr auto kGroupSize = sizeof(uint64_t) * 8;  // bits
  static constexpr auto kGroupMask = kGroupSize - 1;

  // Get element's group
  static constexpr uint16_t GetGroup(const uint64_t id) { return id / kGroupSize; }

  // Get number of groups
  static inline uint16_t GetNumGroups(const uint64_t n_elements) { return (n_elements - 1) / kGroupSize + 1; }

  // Mask as seen by the appropriate group
  static constexpr uint64_t GroupMask(const uint64_t id) { return 1UL << (id & kGroupMask); }

  std::array<std::atomic<uint64_t>, kMaxElements / kGroupSize> hot_masks_{};
#ifndef NDEBUG
  const uint16_t n_elements_;
#endif
  const uint16_t n_groups_;
};

using TaskSignature = std::move_only_function<void()>;

// Resumable task: returns true if it yielded and wants to be rescheduled on the
// same worker, false if it completed.
using ResumableTaskSignature = std::move_only_function<bool()>;

class TaskCollection;

// Also execute non scheduler tasks in the local thread
class TaskCollection {
 public:
  explicit TaskCollection(size_t num_tasks) { tasks_.reserve(num_tasks); }

  TaskCollection() = default;
  TaskCollection(const TaskCollection &) = delete;
  TaskCollection &operator=(const TaskCollection &) = delete;

  TaskCollection(TaskCollection &&other) noexcept {
    auto tasks_guard = std::lock_guard{other.tasks_mutex_};
    auto progress_guard = std::lock_guard{other.progress_mutex_};
    tasks_ = std::move(other.tasks_);
    progress_epoch_ = other.progress_epoch_;
    progress_waiter_task_ = std::move(other.progress_waiter_task_);
    progress_waiter_pool_ = std::exchange(other.progress_waiter_pool_, nullptr);
    progress_waiter_worker_id_ = std::exchange(other.progress_waiter_worker_id_, std::nullopt);
    progress_waiter_task_state_ = std::move(other.progress_waiter_task_state_);
  }

  TaskCollection &operator=(TaskCollection &&) = delete;

  void AddTask(TaskSignature task) {
    tasks_.emplace_back([t = std::move(task)]() mutable {
      t();
      return false;
    });
  }

  void AddResumableTask(ResumableTaskSignature task) { tasks_.emplace_back(std::move(task)); }

  class Task {
   public:
    explicit Task(ResumableTaskSignature task)
        : state_(std::make_shared<std::atomic<State>>(State::IDLE)), task_(std::move(task)) {}

    ~Task() = default;
    Task(const Task &) = delete;
    Task(Task &&) = default;
    Task &operator=(const Task &) = delete;
    Task &operator=(Task &&) = default;

    enum class State : uint8_t {
      IDLE,
      SCHEDULED,  // Claimed by pool's WrapTask closure; also "suspended between yield-resume cycles"
      PARKED,     // Suspended on external progress; an event will requeue the task
      STOLEN,     // Claimed by WaitOrSteal for direct execution on the calling thread
      FINISHED,
    };

    /// Returns true if this state represents terminal completion (no more execution possible).
    bool IsTerminal() const { return state_->load(std::memory_order_acquire) == State::FINISHED; }

    /// Returns true if this task could potentially run later (not terminal and not actively running).
    bool CanRunLater() const {
      const auto s = state_->load(std::memory_order_acquire);
      return s == State::IDLE || s == State::SCHEDULED || s == State::PARKED || s == State::STOLEN;
    }

    std::shared_ptr<std::atomic<State>> state_;
    ResumableTaskSignature task_;
  };

  Task &operator[](size_t index) { return tasks_[index]; }

  ResumableTaskSignature WrapTask(size_t index, PriorityThreadPool *pool = nullptr);

  // Wait for all tasks to finish with optional timeout (default 30s).
  // Returns true if all tasks finished, false if timeout occurred.
  bool Wait(std::chrono::milliseconds timeout = std::chrono::milliseconds(30000));

  // Wait for tasks with stealing, returns true if all finished, false on timeout
  bool WaitOrSteal(std::chrono::milliseconds timeout = std::chrono::milliseconds(30000));

  /// Try to steal and run a single IDLE task on the calling thread.
  /// Returns true if a task was claimed and executed, false otherwise.
  /// This is intended for cooperative polling loops that want to make local
  /// progress without blocking the current thread.
  /// If a pool is provided and the task yields, it will be rescheduled on the pool.
  /// If no pool is provided and the task yields, a runtime error is thrown.
  bool TryExecuteOneIdleTask(PriorityThreadPool *pool = nullptr);

  /// Wait for any task in the collection to report progress (finish or yield),
  /// or until the timeout expires. Returns true if progress was observed.
  bool WaitForProgress(std::chrono::milliseconds timeout);

  uint64_t ProgressEpoch() const;

  bool Finished() const;

  /// Returns true only when ALL tasks are in FINISHED state.
  /// This is the proper join predicate - no task can run later.
  bool AllTerminal() const;

  /// Returns true if any task could potentially run later (not terminal).
  bool HasNonTerminalTasks() const;

  size_t Size() const { return tasks_.size(); }

#ifdef NDEBUG
  /// Debug-only: verify task state invariants (no-op in release).
  void DbgVerifyState() const {}
#else
  /// Debug-only: verify that task state transitions are valid.
  void DbgVerifyState() const;
#endif

 private:
  bool RegisterProgressWaiter(TaskSignature resume_task, PriorityThreadPool *pool, uint16_t worker_id,
                              uint64_t observed_epoch, std::shared_ptr<std::atomic<Task::State>> task_state);
  void NotifyProgress();

  std::vector<Task> tasks_;
  mutable std::mutex tasks_mutex_;  // Protects tasks_ during concurrent steal/schedule operations
  mutable std::mutex progress_mutex_;
  std::condition_variable progress_cv_;
  uint64_t progress_epoch_{0};
  TaskSignature progress_waiter_task_{};  // closure -> WrapTask re-entry
  PriorityThreadPool *progress_waiter_pool_{nullptr};
  std::optional<uint16_t> progress_waiter_worker_id_;
  std::shared_ptr<std::atomic<Task::State>> progress_waiter_task_state_{};  // for the PARKED->SCHEDULED CAS

  // Lifetime barrier: number of WrapTask invocations currently executing on this collection. A worker
  // that stores the last task FINISHED (which releases Wait()) is still inside WrapTask afterwards (e.g.
  // NotifyProgress() touching progress_cv_). Wait() must not return — and the caller must not destroy
  // the collection — until this reaches 0, or progress_cv_ would be destroyed while a worker notifies on
  // it. Incremented at WrapTask entry (before any state CAS, so it is visible once a task is seen
  // FINISHED) and decremented at WrapTask exit (after NotifyProgress, on every path incl. exceptions).
  std::atomic<int> in_flight_{0};

  friend class CollectionScheduler;
};

class WorkerResumeEvent {
 public:
  WorkerResumeEvent() = default;
  WorkerResumeEvent(const WorkerResumeEvent &) = delete;
  WorkerResumeEvent &operator=(const WorkerResumeEvent &) = delete;
  WorkerResumeEvent(WorkerResumeEvent &&) = delete;
  WorkerResumeEvent &operator=(WorkerResumeEvent &&) = delete;

  uint64_t Epoch() const;

  bool RegisterWaiter(std::coroutine_handle<> handle, PriorityThreadPool *pool, std::optional<uint16_t> worker_id,
                      uint64_t observed_epoch);

  bool RegisterTaskWaiter(TaskSignature task, PriorityThreadPool *pool, std::optional<uint16_t> worker_id,
                          uint64_t observed_epoch,
                          std::shared_ptr<std::atomic<TaskCollection::Task::State>> task_state = nullptr);

  // Remove a waiter that was previously registered but should not be notified.
  // Used when a task decides not to suspend after successfully registering.
  // Returns true if the waiter was found and removed, false otherwise.
  bool RemoveWaiter(std::coroutine_handle<> handle, uint64_t observed_epoch,
                    std::shared_ptr<std::atomic<TaskCollection::Task::State>> task_state = nullptr);

  void NotifyAll();

 private:
  struct Waiter {
    std::coroutine_handle<> handle;
    TaskSignature task;
    PriorityThreadPool *pool;
    std::optional<uint16_t> worker_id;
    // Only set for task-based waiters (registered via RegisterTaskWaiter).
    // Used by RemoveWaiter to match when handle is null.
    std::shared_ptr<std::atomic<TaskCollection::Task::State>> task_state;
  };

  mutable std::mutex mutex_;
  uint64_t epoch_{0};
  std::vector<Waiter> waiters_;
};

/// Thread-local access to the currently running resumable task.
/// Allows generic external-progress awaiters to self-park the task and arrange
/// for it to be resumed later by a WorkerResumeEvent.
class CurrentResumableTask {
 public:
  static bool RegisterWaiter(WorkerResumeEvent &event, uint64_t observed_epoch);
  static std::shared_ptr<std::atomic<TaskCollection::Task::State>> GetCurrentTaskState();
  // Clears the TLS parked flag without reading it.  Call this in a shutdown-race
  // path where RegisterWaiter returned true but the task will NOT actually suspend.
  static void ClearParked();
  // Returns the resume closure registered for the current WrapTask frame (the
  // same closure that WrapTask passes to CurrentResumableTaskScope). Used by
  // CollectionScheduler::RegisterProgressWaiter to capture it as a progress waiter
  // rather than storing a raw coroutine handle.
  // Returns nullptr if not inside a WrapTask frame or if no closure was set.
  static const std::function<void()> *GetCurrentResumeTask();
  // Sets the TLS parked flag for the current WrapTask frame, causing WrapTask's
  // WasParked() check to return true and the task to suspend without rescheduling.
  static void SetParked();
};

class PriorityThreadPool {
 public:
  using TaskID = uint64_t;
  using ThreadInitCallback = std::function<void()>;

  PriorityThreadPool(uint16_t work_threads_count, ThreadInitCallback thread_init_callback = nullptr,
                     WorkerYieldRegistry *yield_registry = nullptr);

  ~PriorityThreadPool();

  PriorityThreadPool(const PriorityThreadPool &) = delete;
  PriorityThreadPool(PriorityThreadPool &&) = delete;
  PriorityThreadPool &operator=(const PriorityThreadPool &) = delete;
  PriorityThreadPool &operator=(PriorityThreadPool &&) = delete;

  void AwaitShutdown();

  void ShutDown();

  void ScheduledAddTask(TaskSignature new_task, Priority priority);

  // Schedule a resumable task. The task returns true if it yielded and wants to
  // be rescheduled on the same worker, false when it is done. The pool handles
  // all yield detection and worker-pinned rescheduling internally.
  void ScheduleResumableTask(ResumableTaskSignature task, Priority priority);

  /**
   * Schedules a task on a specific worker. Use when the task must run on that
   * worker (e.g. continuation after yield, to respect thread-local state).
   * worker_id must be in [0, GetNumMixedWorkers()).
   */
  void RescheduleTaskOnWorker(uint16_t worker_id, TaskSignature new_task);

  void ScheduledCollection(TaskCollection &collection) {
    for (size_t i = 0; i < collection.Size(); ++i) {
      ScheduleResumableTask(collection.WrapTask(i, this), Priority::LOW);
    }
  }

  uint64_t GetNumMixedWorkers() const { return workers_.size(); }

  uint64_t GetNumWorkers() const { return workers_.size(); }

  /// Returns true if the pool is shutting down (stop has been requested).
  /// Used by ResumableWrapper to detect shutdown and handle yields inline.
  bool IsShuttingDown() const { return pool_stop_source_.stop_requested(); }

  // Single worker implementation
  class Worker {
   public:
    Worker() = default;
    ~Worker() = default;

    Worker(const Worker &) = delete;
    Worker &operator=(const Worker &) = delete;
    Worker(Worker &&) = delete;
    Worker &operator=(Worker &&) = delete;

    struct Work {
      TaskID id;                   // ID used to order (issued by the pool)
      mutable TaskSignature work;  // mutable so it can be moved from the queue
      bool pinned{false};          // if true, task must run on this worker (not stealable)

      bool operator<(const Work &other) const { return id < other.id; }
    };

    void push(TaskSignature new_task, TaskID id, bool pinned = false);

    void stop();

    template <Priority ThreadPriority>
    void operator()(uint16_t worker_id, const std::vector<std::unique_ptr<Worker>> &workers_pool, HotMask &hot_threads,
                    WorkerYieldRegistry *yield_registry);

   private:
    mutable std::mutex mtx_;
    std::condition_variable cv_;
    std::priority_queue<Work> work_;         // Stealable work
    std::priority_queue<Work> work_pinned_;  // Pinned to this worker (never stolen)

    // Stats
    std::atomic_bool has_pending_work_{false};
    std::atomic_bool working_{false};
    std::atomic_bool run_{true};
    // Used by monitor to decide if worker is blocked
    std::atomic<TaskID> last_task_{0};

    // Set by operator() for LP workers; used in push() to request yield when adding HP task to busy worker
    WorkerYieldRegistry *yield_registry_{nullptr};
    uint16_t worker_id_{0};

    friend class PriorityThreadPool;
  };

 private:
  std::stop_source pool_stop_source_;

  std::vector<std::unique_ptr<Worker>> workers_;  // Mixed work threads
  HotMask hot_threads_;                           // Mask of workers waiting for new work (but still not sleeping)

  std::vector<std::jthread> pool_;  // All available threads (list so the elements are stable)

  std::atomic<TaskID> task_id_;  // Generates a unique tasks id | MSB signals high priority

  WorkerYieldRegistry *yield_registry_{nullptr};
};

class CollectionScheduler {
 public:
  class ProgressAwaitable {
   public:
    explicit ProgressAwaitable(CollectionScheduler *scheduler)
        : scheduler_(scheduler), observed_epoch_(scheduler ? scheduler->ProgressEpoch() : 0) {}

    bool await_ready() const { return !scheduler_ || scheduler_->Finished(); }

    bool await_suspend(std::coroutine_handle<> /*handle*/) const {
      // Real wait: register the WrapTask re-entry closure (captured from TLS) as the
      // progress waiter and suspend. Returns false (busy-spin fallback) when:
      //   - no scheduler / no pool
      //   - EC-2: stolen task with no pinned worker_id
      //   - not inside a WrapTask frame (no TLS resume closure)
      //   - epoch already advanced (progress already happened -> no need to wait)
      //   - collection already Finished()
      // In all those cases await_ready() will re-check on the next busy-spin iteration.
      if (!scheduler_) return false;
      return scheduler_->RegisterProgressWaiter(observed_epoch_);
    }

    void await_resume() const {}

   private:
    CollectionScheduler *scheduler_{nullptr};
    uint64_t observed_epoch_{0};
  };

  CollectionScheduler(PriorityThreadPool *pool, std::shared_ptr<TaskCollection> collection)
      : pool_{pool}, collection_{std::move(collection)} {}

  void SetPool(PriorityThreadPool *pool) { pool_ = pool; }

  void SetCollection(std::shared_ptr<TaskCollection> collection) { collection_ = std::move(collection); }

  void Trigger() {
    if (pool_ && collection_) pool_->ScheduledCollection(*collection_);
    pool_ = nullptr;
  }

  void WaitOrSteal() {
    if (collection_) collection_->WaitOrSteal();
    collection_.reset();
  }

  bool TryExecuteOneIdleTask() const { return collection_ && collection_->TryExecuteOneIdleTask(pool_); }

  bool WaitForProgress(std::chrono::milliseconds timeout) const {
    return collection_ && collection_->WaitForProgress(timeout);
  }

  auto WaitForProgressAwaitable() { return ProgressAwaitable(this); }

  bool Finished() const {
    if (collection_) return collection_->Finished();
    return true;
  }

  /// Returns true only when ALL tasks are in FINISHED state.
  bool AllTerminal() const {
    if (collection_) return collection_->AllTerminal();
    return true;
  }

  /// Returns true if any task could potentially run later (not terminal).
  bool HasNonTerminalTasks() const {
    if (collection_) return collection_->HasNonTerminalTasks();
    return false;
  }

  /// Debug-only state verification (no-op in release).
  void DbgVerifyState() const {
    if (collection_) collection_->DbgVerifyState();
  }

 private:
  bool RegisterProgressWaiter(uint64_t observed_epoch) const;
  uint64_t ProgressEpoch() const;

  PriorityThreadPool *pool_;
  std::shared_ptr<TaskCollection> collection_;
};

}  // namespace memgraph::utils
