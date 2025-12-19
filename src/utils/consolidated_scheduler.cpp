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

#include "utils/consolidated_scheduler.hpp"

#include <sys/eventfd.h>
#include <unistd.h>

#include <algorithm>
#include <mutex>
#include <queue>
#include <vector>

#include "utils/logging.hpp"
#include "utils/thread.hpp"
#include "utils/timer_backend.hpp"

namespace memgraph::utils {

/// Internal state for a scheduled task
struct TaskState {
  std::string name;
  SchedulerPriority priority;
  std::function<void()> callback;

  // Atomic state - all scheduling state packed into atomics for lock-free access
  // next_execution stored as nanoseconds since epoch
  std::atomic<int64_t> next_execution_ns{0};
  // interval stored as milliseconds
  std::atomic<int64_t> interval_ms{0};
  // one_shot flag
  std::atomic<bool> one_shot{false};

  // Control flags
  std::atomic<bool> paused{false};
  std::atomic<bool> running{false};
  std::atomic<bool> stopped{false};
  std::atomic<bool> trigger_now{false};

  // Function pointers for scheduler operations (set by Impl)
  std::function<void()> wake_dispatcher;
  std::function<void(std::shared_ptr<TaskState>)> reschedule_task;

  void WakeDispatcher() const {
    if (wake_dispatcher) {
      wake_dispatcher();
    }
  }

  void ExecuteCallback() {
    trigger_now.store(false, std::memory_order_release);
    running.store(true, std::memory_order_release);

    try {
      callback();
    } catch (const std::exception &e) {
      spdlog::warn("Scheduler task '{}' threw exception: {}", name, e.what());
    } catch (...) {
      spdlog::warn("Scheduler task '{}' threw unknown exception", name);
    }

    running.store(false, std::memory_order_release);
  }

  /// Thread-safe getter for next_execution
  std::chrono::steady_clock::time_point GetNextExecution() const {
    auto ns = next_execution_ns.load(std::memory_order_acquire);
    return std::chrono::steady_clock::time_point{std::chrono::nanoseconds{ns}};
  }

  /// Thread-safe setter for next_execution
  void SetNextExecution(std::chrono::steady_clock::time_point tp) {
    next_execution_ns.store(tp.time_since_epoch().count(), std::memory_order_release);
  }

  /// Thread-safe getter for interval
  std::chrono::milliseconds GetInterval() const {
    return std::chrono::milliseconds{interval_ms.load(std::memory_order_acquire)};
  }

  /// Thread-safe getter for one_shot
  bool IsOneShot() const { return one_shot.load(std::memory_order_acquire); }

  /// Thread-safe update of schedule and next_execution together
  void UpdateSchedule(const ScheduleSpec &new_schedule) {
    // Store interval and one_shot first, then next_execution
    // This order ensures readers see consistent state
    interval_ms.store(new_schedule.interval.count(), std::memory_order_release);
    one_shot.store(new_schedule.one_shot, std::memory_order_release);
    auto now = std::chrono::steady_clock::now();
    next_execution_ns.store((now + new_schedule.interval).time_since_epoch().count(), std::memory_order_release);
  }

  /// Initialize schedule (called during registration, single-threaded)
  void InitSchedule(const ScheduleSpec &schedule, std::chrono::steady_clock::time_point next) {
    interval_ms.store(schedule.interval.count(), std::memory_order_relaxed);
    one_shot.store(schedule.one_shot, std::memory_order_relaxed);
    next_execution_ns.store(next.time_since_epoch().count(), std::memory_order_relaxed);
  }

  /// Comparison for priority queue (min-heap: earliest time, then highest priority)
  bool operator>(const TaskState &other) const {
    auto this_next = GetNextExecution();
    auto other_next = other.GetNextExecution();
    if (this_next != other_next) {
      return this_next > other_next;
    }
    // Lower priority enum = higher priority
    return static_cast<uint8_t>(priority) > static_cast<uint8_t>(other.priority);
  }
};

/// Priority queue comparator for shared_ptr<TaskState>
struct TaskComparator {
  bool operator()(const std::shared_ptr<TaskState> &a, const std::shared_ptr<TaskState> &b) const { return *a > *b; }
};

/// Implementation details hidden from header
class ConsolidatedScheduler::Impl {
 public:
  explicit Impl(size_t worker_count) : worker_count_(worker_count) {
    // EFD_SEMAPHORE: each write increments counter, each read decrements by 1
    // This allows precise wake-one semantics and reduces futex calls vs CV
    work_eventfd_ = eventfd(0, EFD_SEMAPHORE);
  }

  ~Impl() {
    if (work_eventfd_ >= 0) {
      close(work_eventfd_);
    }
  }

  Impl(const Impl &) = delete;
  Impl &operator=(const Impl &) = delete;
  Impl(Impl &&) = delete;
  Impl &operator=(Impl &&) = delete;

  // Timer backend for dispatcher sleep
  TimerBackend timer_backend_;

  // Task scheduling
  std::priority_queue<std::shared_ptr<TaskState>, std::vector<std::shared_ptr<TaskState>>, TaskComparator> task_queue_;
  std::vector<std::shared_ptr<TaskState>> all_tasks_;  // For iteration/removal
  mutable std::mutex mutex_;

  // Worker thread pool (priority queue so highest priority tasks dispatch first)
  std::priority_queue<std::shared_ptr<TaskState>, std::vector<std::shared_ptr<TaskState>>, TaskComparator> work_queue_;
  std::mutex work_mutex_;
  int work_eventfd_{-1};  // eventfd for worker wake (reduces futex calls vs CV)
  std::vector<std::jthread> workers_;
  size_t worker_count_;

  // Control
  std::jthread dispatcher_thread_;
  std::atomic<bool> running_{false};

  /// Signal workers that work is available (write to eventfd)
  void SignalWorkers(size_t count) {
    uint64_t val = count;
    [[maybe_unused]] auto written = write(work_eventfd_, &val, sizeof(val));
  }

  /// Wait for work signal (blocking read from eventfd)
  /// Returns false if should stop
  bool WaitForWork() {
    uint64_t val;
    // Blocking read - decrements semaphore by 1
    auto bytes = read(work_eventfd_, &val, sizeof(val));
    return bytes > 0 && running_.load(std::memory_order_acquire);
  }
};

// TaskHandle implementation
TaskHandle::TaskHandle(std::weak_ptr<TaskState> state) : state_(std::move(state)) {}

TaskHandle::~TaskHandle() { Stop(); }

TaskHandle::TaskHandle(TaskHandle &&other) noexcept : state_(std::move(other.state_)) {}

TaskHandle &TaskHandle::operator=(TaskHandle &&other) noexcept {
  if (this != &other) {
    Stop();
    state_ = std::move(other.state_);
  }
  return *this;
}

bool TaskHandle::IsValid() const { return !state_.expired(); }

bool TaskHandle::IsRunning() const {
  auto state = state_.lock();
  return state && state->running.load(std::memory_order_acquire);
}

bool TaskHandle::IsPaused() const {
  auto state = state_.lock();
  return state && state->paused.load(std::memory_order_acquire);
}

void TaskHandle::Pause() {
  if (auto state = state_.lock()) {
    state->paused.store(true, std::memory_order_release);
  }
}

void TaskHandle::Resume() {
  if (auto state = state_.lock()) {
    state->paused.store(false, std::memory_order_release);
    state->WakeDispatcher();
  }
}

void TaskHandle::Stop() {
  if (auto state = state_.lock()) {
    state->stopped.store(true, std::memory_order_release);
    state_.reset();
  }
}

void TaskHandle::TriggerNow() {
  if (auto state = state_.lock()) {
    state->trigger_now.store(true, std::memory_order_release);
    state->WakeDispatcher();
  }
}

void TaskHandle::SetSchedule(const ScheduleSpec &new_schedule) {
  if (auto state = state_.lock()) {
    state->UpdateSchedule(new_schedule);
    state->WakeDispatcher();
  }
}

std::optional<std::chrono::steady_clock::time_point> TaskHandle::NextExecution() const {
  auto state = state_.lock();
  if (!state || state->stopped.load(std::memory_order_acquire) || state->paused.load(std::memory_order_acquire)) {
    return std::nullopt;
  }
  return state->GetNextExecution();
}

// ConsolidatedScheduler implementation
ConsolidatedScheduler &ConsolidatedScheduler::Global() {
  static ConsolidatedScheduler instance;
  return instance;
}

ConsolidatedScheduler::ConsolidatedScheduler(size_t worker_count) : impl_(std::make_unique<Impl>(worker_count)) {
  impl_->running_.store(true, std::memory_order_release);

  // Start worker threads
  for (size_t i = 0; i < worker_count; ++i) {
    impl_->workers_.emplace_back([this](std::stop_token token) { WorkerLoop(); });
  }

  // Start dispatcher thread
  impl_->dispatcher_thread_ = std::jthread([this](std::stop_token token) {
    utils::ThreadSetName("scheduler");
    DispatcherLoop();
  });
}

ConsolidatedScheduler::~ConsolidatedScheduler() { Shutdown(); }

TaskHandle ConsolidatedScheduler::Register(TaskConfig config, std::function<void()> callback) {
  // Return invalid handle if schedule is disabled
  if (!config.schedule) {
    return TaskHandle{};
  }

  auto task = std::make_shared<TaskState>();
  task->name = std::move(config.name);
  task->priority = config.priority;
  task->callback = std::move(callback);

  // Initialize schedule (single-threaded at this point)
  auto now = std::chrono::steady_clock::now();
  auto next_exec = config.schedule.execute_immediately ? now : (now + config.schedule.interval);
  task->InitSchedule(config.schedule, next_exec);

  // Set up wake function
  task->wake_dispatcher = [this]() { WakeDispatcher(); };

  // Set up reschedule function (captures task as weak_ptr to avoid cycle)
  std::weak_ptr<TaskState> weak_task = task;
  task->reschedule_task = [this, weak_task](std::shared_ptr<TaskState>) {
    if (auto t = weak_task.lock()) {
      if (t->stopped.load(std::memory_order_acquire)) {
        return;  // Already stopped
      }

      // One-shot tasks stop after single execution
      if (t->IsOneShot()) {
        t->stopped.store(true, std::memory_order_release);
        return;
      }

      // Reschedule periodic task
      auto reschedule_now = std::chrono::steady_clock::now();
      t->SetNextExecution(reschedule_now + t->GetInterval());
      {
        std::lock_guard lock(impl_->mutex_);
        impl_->task_queue_.push(t);
      }
      WakeDispatcher();
    }
  };

  {
    std::lock_guard lock(impl_->mutex_);
    impl_->all_tasks_.push_back(task);
    impl_->task_queue_.push(task);
  }

  WakeDispatcher();

  return TaskHandle{task};
}

void ConsolidatedScheduler::Shutdown() {
  if (!impl_->running_.exchange(false, std::memory_order_acq_rel)) {
    return;  // Already shut down
  }

  // Cancel timer to wake dispatcher
  impl_->timer_backend_.Cancel();

  // Wake all workers so they can exit
  impl_->SignalWorkers(impl_->worker_count_);

  // Join dispatcher
  if (impl_->dispatcher_thread_.joinable()) {
    impl_->dispatcher_thread_.request_stop();
    impl_->dispatcher_thread_.join();
  }

  // Join workers
  for (auto &worker : impl_->workers_) {
    if (worker.joinable()) {
      worker.request_stop();
      worker.join();
    }
  }
  impl_->workers_.clear();

  // Clear all tasks (all threads joined, safe to access without lock but use it for clarity)
  {
    std::lock_guard lock(impl_->mutex_);
    while (!impl_->task_queue_.empty()) {
      impl_->task_queue_.pop();
    }
    impl_->all_tasks_.clear();
  }

  // Clear work queue (all workers joined, safe to access)
  {
    std::lock_guard lock(impl_->work_mutex_);
    while (!impl_->work_queue_.empty()) {
      impl_->work_queue_.pop();
    }
  }
}

bool ConsolidatedScheduler::IsRunning() const { return impl_->running_.load(std::memory_order_acquire); }

size_t ConsolidatedScheduler::TaskCount() const {
  std::lock_guard lock(impl_->mutex_);
  return impl_->all_tasks_.size();
}

size_t ConsolidatedScheduler::WorkerCount() const { return impl_->worker_count_; }

void ConsolidatedScheduler::DispatcherLoop() {
  while (impl_->running_.load(std::memory_order_acquire)) {
    std::vector<std::shared_ptr<TaskState>> tasks_to_dispatch;
    time_point next_deadline = time_point::max();

    {
      std::lock_guard lock(impl_->mutex_);

      // Remove stopped tasks
      std::erase_if(impl_->all_tasks_, [](const auto &t) { return t->stopped.load(std::memory_order_acquire); });

      // Rebuild priority queue from non-stopped, non-running tasks
      std::priority_queue<std::shared_ptr<TaskState>, std::vector<std::shared_ptr<TaskState>>, TaskComparator>
          new_queue;
      for (const auto &task : impl_->all_tasks_) {
        // Only add to queue if not stopped and not currently running
        if (!task->stopped.load(std::memory_order_acquire) && !task->running.load(std::memory_order_acquire)) {
          new_queue.push(task);
        }
      }
      impl_->task_queue_ = std::move(new_queue);

      // Find all tasks that are due now
      auto now = std::chrono::steady_clock::now();
      while (!impl_->task_queue_.empty()) {
        auto top = impl_->task_queue_.top();

        if (top->stopped.load(std::memory_order_acquire)) {
          impl_->task_queue_.pop();
          continue;
        }

        // Skip if already running (shouldn't happen due to rebuild, but be safe)
        if (top->running.load(std::memory_order_acquire)) {
          impl_->task_queue_.pop();
          continue;
        }

        if (top->paused.load(std::memory_order_acquire) && !top->trigger_now.load(std::memory_order_acquire)) {
          impl_->task_queue_.pop();
          continue;
        }

        auto task_next = top->GetNextExecution();
        if (top->trigger_now.load(std::memory_order_acquire) || task_next <= now) {
          // Advance next_execution to prevent re-dispatch before execution starts
          // (task stays in all_tasks_ but won't be "due" when queue is rebuilt)
          top->SetNextExecution(now + top->GetInterval());
          tasks_to_dispatch.push_back(top);
          impl_->task_queue_.pop();
          continue;  // Keep looking for more due tasks
        }

        // This task is not due yet - it defines the next deadline
        next_deadline = task_next;
        break;
      }
    }

    // Dispatch tasks to workers (or run inline if no workers)
    if (!tasks_to_dispatch.empty()) {
      if (impl_->worker_count_ > 0) {
        // Submit to worker pool (priority queue ensures highest priority dispatched first)
        size_t num_tasks = tasks_to_dispatch.size();
        {
          std::lock_guard lock(impl_->work_mutex_);
          for (auto &task : tasks_to_dispatch) {
            impl_->work_queue_.push(std::move(task));
          }
        }
        // Signal workers via eventfd (one signal per task for precise wake)
        impl_->SignalWorkers(num_tasks);
      } else {
        // Run inline (no worker threads)
        for (auto &task : tasks_to_dispatch) {
          task->ExecuteCallback();
          // Reschedule after execution completes
          if (task->reschedule_task) {
            task->reschedule_task(task);
          }
        }
      }
      continue;  // Check for more due tasks immediately
    }

    // No task due - wait until next deadline
    if (next_deadline != time_point::max()) {
      impl_->timer_backend_.WaitUntil(next_deadline);
    } else {
      // No tasks - wait for a long time (will be woken when task added)
      impl_->timer_backend_.WaitUntil(std::chrono::steady_clock::now() + std::chrono::hours(24));
    }
  }
}

void ConsolidatedScheduler::WorkerLoop() {
  utils::ThreadSetName("sched_idle");

  while (impl_->running_.load(std::memory_order_acquire)) {
    // Wait for work signal via eventfd (blocks until signaled)
    if (!impl_->WaitForWork()) {
      break;  // Shutdown or error
    }

    std::shared_ptr<TaskState> task;
    {
      std::lock_guard lock(impl_->work_mutex_);
      if (!impl_->work_queue_.empty()) {
        // Priority queue: highest priority (lowest enum value) at top
        task = impl_->work_queue_.top();
        impl_->work_queue_.pop();
      }
    }

    if (task && !task->stopped.load(std::memory_order_acquire)) {
      // Set thread name to task name while executing
      utils::ThreadSetName(task->name);
      task->ExecuteCallback();
      // Restore idle name
      utils::ThreadSetName("sched_idle");
      // Reschedule after execution completes
      if (task->reschedule_task) {
        task->reschedule_task(task);
      }
    }
  }
}

void ConsolidatedScheduler::WakeDispatcher() { impl_->timer_backend_.UpdateDeadline(std::chrono::steady_clock::now()); }

}  // namespace memgraph::utils
