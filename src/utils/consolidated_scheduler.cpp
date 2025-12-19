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

#include <algorithm>
#include <mutex>
#include <queue>
#include <vector>

#include "utils/logging.hpp"
#include "utils/thread.hpp"

namespace memgraph::utils {

/// Internal state for a scheduled task
struct TaskState {
  std::string name;
  ScheduleSpec schedule;
  SchedulerPriority priority;
  std::function<void()> callback;

  std::chrono::steady_clock::time_point next_execution;
  std::atomic<bool> paused{false};
  std::atomic<bool> running{false};
  std::atomic<bool> stopped{false};
  std::atomic<bool> trigger_now{false};

  // Pointer to timer backend for waking the dispatcher
  ITimerBackend *timer_backend{nullptr};

  void WakeDispatcher() const {
    if (timer_backend) {
      timer_backend->UpdateDeadline(std::chrono::steady_clock::now());
    }
  }

  /// Comparison for priority queue (min-heap: earliest time, then highest priority)
  bool operator>(const TaskState &other) const {
    if (next_execution != other.next_execution) {
      return next_execution > other.next_execution;
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
  explicit Impl(TimerBackendType backend_type) : timer_backend_(CreateTimerBackend(backend_type)) {}

  std::unique_ptr<ITimerBackend> timer_backend_;
  std::priority_queue<std::shared_ptr<TaskState>, std::vector<std::shared_ptr<TaskState>>, TaskComparator> task_queue_;
  std::vector<std::shared_ptr<TaskState>> all_tasks_;  // For iteration/removal
  mutable std::mutex mutex_;
  std::jthread dispatcher_thread_;
  std::atomic<bool> running_{false};
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
    state->schedule = new_schedule;
    state->next_execution = std::chrono::steady_clock::now() + new_schedule.interval;
    state->WakeDispatcher();
  }
}

std::optional<std::chrono::steady_clock::time_point> TaskHandle::NextExecution() const {
  auto state = state_.lock();
  if (!state || state->stopped.load(std::memory_order_acquire) || state->paused.load(std::memory_order_acquire)) {
    return std::nullopt;
  }
  return state->next_execution;
}

// ConsolidatedScheduler implementation
ConsolidatedScheduler &ConsolidatedScheduler::Global() {
  static ConsolidatedScheduler instance;
  return instance;
}

ConsolidatedScheduler::ConsolidatedScheduler(TimerBackendType backend_type)
    : impl_(std::make_unique<Impl>(backend_type)) {
  impl_->running_.store(true, std::memory_order_release);
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
  task->schedule = config.schedule;
  task->priority = config.priority;
  task->callback = std::move(callback);
  task->timer_backend = impl_->timer_backend_.get();

  auto now = std::chrono::steady_clock::now();
  if (config.schedule.execute_immediately) {
    task->next_execution = now;
  } else {
    task->next_execution = now + config.schedule.interval;
  }

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

  impl_->timer_backend_->Cancel();

  if (impl_->dispatcher_thread_.joinable()) {
    impl_->dispatcher_thread_.request_stop();
    impl_->dispatcher_thread_.join();
  }

  // Clear all tasks
  {
    std::lock_guard lock(impl_->mutex_);
    while (!impl_->task_queue_.empty()) {
      impl_->task_queue_.pop();
    }
    impl_->all_tasks_.clear();
  }
}

bool ConsolidatedScheduler::IsRunning() const { return impl_->running_.load(std::memory_order_acquire); }

size_t ConsolidatedScheduler::TaskCount() const {
  std::lock_guard lock(impl_->mutex_);
  return impl_->all_tasks_.size();
}

void ConsolidatedScheduler::DispatcherLoop() {
  while (impl_->running_.load(std::memory_order_acquire)) {
    std::shared_ptr<TaskState> task_to_run;
    time_point next_deadline = time_point::max();

    {
      std::lock_guard lock(impl_->mutex_);

      // Remove stopped tasks and rebuild queue
      std::erase_if(impl_->all_tasks_, [](const auto &t) { return t->stopped.load(std::memory_order_acquire); });

      // Rebuild priority queue (simple approach - could be optimized)
      std::priority_queue<std::shared_ptr<TaskState>, std::vector<std::shared_ptr<TaskState>>, TaskComparator>
          new_queue;
      for (const auto &task : impl_->all_tasks_) {
        if (!task->stopped.load(std::memory_order_acquire)) {
          new_queue.push(task);
        }
      }
      impl_->task_queue_ = std::move(new_queue);

      // Find next task to run
      auto now = std::chrono::steady_clock::now();
      while (!impl_->task_queue_.empty()) {
        auto top = impl_->task_queue_.top();

        if (top->stopped.load(std::memory_order_acquire)) {
          impl_->task_queue_.pop();
          continue;
        }

        if (top->paused.load(std::memory_order_acquire) && !top->trigger_now.load(std::memory_order_acquire)) {
          impl_->task_queue_.pop();
          continue;
        }

        if (top->trigger_now.load(std::memory_order_acquire) || top->next_execution <= now) {
          task_to_run = top;
          impl_->task_queue_.pop();
          break;
        }

        next_deadline = top->next_execution;
        break;
      }
    }

    if (task_to_run) {
      task_to_run->trigger_now.store(false, std::memory_order_release);
      task_to_run->running.store(true, std::memory_order_release);

      try {
        task_to_run->callback();
      } catch (const std::exception &e) {
        spdlog::warn("Scheduler task '{}' threw exception: {}", task_to_run->name, e.what());
      } catch (...) {
        spdlog::warn("Scheduler task '{}' threw unknown exception", task_to_run->name);
      }

      task_to_run->running.store(false, std::memory_order_release);

      // Reschedule if not stopped
      if (!task_to_run->stopped.load(std::memory_order_acquire)) {
        auto now = std::chrono::steady_clock::now();
        task_to_run->next_execution = now + task_to_run->schedule.interval;

        std::lock_guard lock(impl_->mutex_);
        impl_->task_queue_.push(task_to_run);
      }

      continue;  // Check for more due tasks immediately
    }

    // No task due - wait until next deadline
    if (next_deadline != time_point::max()) {
      impl_->timer_backend_->WaitUntil(next_deadline);
    } else {
      // No tasks - wait for a long time (will be woken when task added)
      impl_->timer_backend_->WaitUntil(std::chrono::steady_clock::now() + std::chrono::hours(24));
    }
  }
}

void ConsolidatedScheduler::WakeDispatcher() {
  impl_->timer_backend_->UpdateDeadline(std::chrono::steady_clock::now());
}

}  // namespace memgraph::utils
