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
#include <unordered_map>
#include <vector>

#include "utils/logging.hpp"
#include "utils/thread.hpp"
#include "utils/timer_backend.hpp"

namespace memgraph::utils {

/// Internal state for a scheduled task
struct TaskState {
  std::string name;
  std::string pool_name;  ///< Target pool for execution
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

/// A named worker pool with its own queue and worker threads
struct WorkerPool {
  std::string name;
  PoolPolicy policy{PoolPolicy::FIXED};
  size_t min_workers{0};
  size_t max_workers{0};
  int eventfd_{-1};
  std::priority_queue<std::shared_ptr<TaskState>, std::vector<std::shared_ptr<TaskState>>, TaskComparator> queue;
  std::mutex mutex;
  std::vector<std::jthread> workers;
  std::atomic<size_t> active_workers{0};  ///< Currently executing workers (for GROW policy)
  std::atomic<size_t> queued_tasks{0};    ///< Tasks waiting in queue

  WorkerPool() = default;

  explicit WorkerPool(const PoolConfig &config)
      : name(config.name),
        policy(config.policy),
        min_workers(config.min_workers),
        max_workers(config.max_workers),
        eventfd_(eventfd(0, EFD_SEMAPHORE)) {}

  ~WorkerPool() {
    if (eventfd_ >= 0) {
      close(eventfd_);
      eventfd_ = -1;
    }
  }

  WorkerPool(const WorkerPool &) = delete;
  WorkerPool &operator=(const WorkerPool &) = delete;
  WorkerPool(WorkerPool &&other) noexcept
      : name(std::move(other.name)),
        policy(other.policy),
        min_workers(other.min_workers),
        max_workers(other.max_workers),
        eventfd_(std::exchange(other.eventfd_, -1)),
        queue(std::move(other.queue)),
        workers(std::move(other.workers)),
        active_workers(other.active_workers.load()),
        queued_tasks(other.queued_tasks.load()) {}
  WorkerPool &operator=(WorkerPool &&) = delete;

  void Signal(size_t count = 1) {
    if (eventfd_ >= 0) {
      uint64_t val = count;
      [[maybe_unused]] auto written = write(eventfd_, &val, sizeof(val));
    }
  }

  bool Wait(const std::atomic<bool> &running) {
    uint64_t val;
    auto bytes = read(eventfd_, &val, sizeof(val));
    return bytes > 0 && running.load(std::memory_order_acquire);
  }

  /// Check if pool should grow (for GROW policy)
  bool ShouldGrow() const {
    if (policy != PoolPolicy::GROW) return false;
    return workers.size() < max_workers && queued_tasks.load(std::memory_order_acquire) > 0;
  }

  /// Check if pool can shrink back to min_workers
  bool CanShrink() const {
    if (policy != PoolPolicy::GROW) return false;
    return workers.size() > min_workers && queued_tasks.load(std::memory_order_acquire) == 0;
  }
};

/// Implementation details hidden from header
class ConsolidatedScheduler::Impl {
 public:
  explicit Impl(size_t /* legacy_worker_count - ignored, use pools */) {}

  ~Impl() = default;

  Impl(const Impl &) = delete;
  Impl &operator=(const Impl &) = delete;
  Impl(Impl &&) = delete;
  Impl &operator=(Impl &&) = delete;

  // Timer backend for dispatcher sleep
  TimerBackend timer_backend_;

  // Task scheduling (dispatcher manages timing, pools handle execution)
  std::priority_queue<std::shared_ptr<TaskState>, std::vector<std::shared_ptr<TaskState>>, TaskComparator> task_queue_;
  std::vector<std::shared_ptr<TaskState>> all_tasks_;  // For iteration/removal
  mutable std::mutex mutex_;

  // Named worker pools
  std::unordered_map<std::string, std::unique_ptr<WorkerPool>> pools_;
  std::mutex pools_mutex_;
  std::string default_pool_name_{"general"};

  // Control
  std::jthread dispatcher_thread_;
  std::atomic<bool> running_{false};

  /// Get or create a pool (must hold pools_mutex_)
  WorkerPool *GetPool(const std::string &name) {
    auto it = pools_.find(name);
    return it != pools_.end() ? it->second.get() : nullptr;
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
  static std::once_flag init_flag;
  static ConsolidatedScheduler instance;

  // Register default pools on first access (after construction)
  std::call_once(init_flag, [&]() {
    instance.RegisterPool({kCriticalPool, 1, 1, PoolPolicy::INLINE});  // INLINE ensures critical tasks always run
    instance.RegisterPool({kGeneralPool, 4, 4, PoolPolicy::FIXED});    // General-purpose pool
    instance.RegisterPool({kIoPool, 2, 4, PoolPolicy::GROW});          // For RPC/network tasks, grows under load
  });

  return instance;
}

ConsolidatedScheduler::ConsolidatedScheduler() : impl_(std::make_unique<Impl>(0)) {
  impl_->running_.store(true, std::memory_order_release);

  // Start dispatcher thread (pools are started when registered)
  impl_->dispatcher_thread_ = std::jthread([this](std::stop_token token) {
    utils::ThreadSetName("scheduler");
    DispatcherLoop();
  });
}

void ConsolidatedScheduler::RegisterPool(PoolConfig config) {
  std::lock_guard lock(impl_->pools_mutex_);

  // Check if pool already exists
  if (impl_->pools_.find(config.name) != impl_->pools_.end()) {
    spdlog::warn("Pool '{}' already registered, ignoring", config.name);
    return;
  }

  auto pool = std::make_unique<WorkerPool>(config);

  // Start min_workers for this pool
  for (size_t i = 0; i < config.min_workers; ++i) {
    std::string pool_name = config.name;
    pool->workers.emplace_back([this, pool_name](std::stop_token token) { PoolWorkerLoop(pool_name); });
  }

  spdlog::trace("Registered pool '{}' with {} workers (policy: {})", config.name, config.min_workers,
                static_cast<int>(config.policy));

  impl_->pools_[config.name] = std::move(pool);
}

ConsolidatedScheduler::~ConsolidatedScheduler() { Shutdown(); }

TaskHandle ConsolidatedScheduler::Register(TaskConfig config, std::function<void()> callback) {
  // Return invalid handle if schedule is disabled
  if (!config.schedule) {
    return TaskHandle{};
  }

  // Validate pool exists
  {
    std::lock_guard lock(impl_->pools_mutex_);
    if (impl_->pools_.find(config.pool) == impl_->pools_.end()) {
      spdlog::error("Cannot register task '{}': pool '{}' not found", config.name, config.pool);
      return TaskHandle{};
    }
  }

  auto task = std::make_shared<TaskState>();
  task->name = std::move(config.name);
  task->pool_name = std::move(config.pool);
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

  // Signal all pool workers to exit
  {
    std::lock_guard lock(impl_->pools_mutex_);
    for (auto &[name, pool] : impl_->pools_) {
      pool->Signal(pool->workers.size());
    }
  }

  // Join dispatcher
  if (impl_->dispatcher_thread_.joinable()) {
    impl_->dispatcher_thread_.request_stop();
    impl_->dispatcher_thread_.join();
  }

  // Join all pool workers and clear pools
  {
    std::lock_guard lock(impl_->pools_mutex_);
    for (auto &[name, pool] : impl_->pools_) {
      for (auto &worker : pool->workers) {
        if (worker.joinable()) {
          worker.request_stop();
          worker.join();
        }
      }
      pool->workers.clear();
      // Clear pool queue
      std::lock_guard pool_lock(pool->mutex);
      while (!pool->queue.empty()) {
        pool->queue.pop();
      }
    }
    impl_->pools_.clear();
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

size_t ConsolidatedScheduler::WorkerCount() const {
  std::lock_guard lock(impl_->pools_mutex_);
  size_t total = 0;
  for (const auto &[name, pool] : impl_->pools_) {
    total += pool->workers.size();
  }
  return total;
}

size_t ConsolidatedScheduler::PoolCount() const {
  std::lock_guard lock(impl_->pools_mutex_);
  return impl_->pools_.size();
}

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

    // Dispatch tasks to their respective pools
    if (!tasks_to_dispatch.empty()) {
      // Group tasks by pool
      std::unordered_map<std::string, std::vector<std::shared_ptr<TaskState>>> tasks_by_pool;
      for (auto &task : tasks_to_dispatch) {
        tasks_by_pool[task->pool_name].push_back(std::move(task));
      }

      // Submit to each pool
      std::lock_guard pools_lock(impl_->pools_mutex_);
      for (auto &[pool_name, tasks] : tasks_by_pool) {
        auto *pool = impl_->GetPool(pool_name);
        if (!pool) {
          spdlog::warn("Pool '{}' not found for task dispatch, running inline", pool_name);
          for (auto &task : tasks) {
            task->ExecuteCallback();
            if (task->reschedule_task) {
              task->reschedule_task(task);
            }
          }
          continue;
        }

        // Handle based on pool policy
        if (pool->policy == PoolPolicy::INLINE || pool->workers.empty()) {
          // Run inline on dispatcher thread
          for (auto &task : tasks) {
            task->ExecuteCallback();
            if (task->reschedule_task) {
              task->reschedule_task(task);
            }
          }
        } else {
          // Submit to pool queue
          size_t num_tasks = tasks.size();
          {
            std::lock_guard pool_lock(pool->mutex);
            for (auto &task : tasks) {
              pool->queue.push(std::move(task));
              pool->queued_tasks.fetch_add(1, std::memory_order_release);
            }
          }
          pool->Signal(num_tasks);
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

void ConsolidatedScheduler::PoolWorkerLoop(const std::string &pool_name) {
  utils::ThreadSetName(pool_name);

  // Get pool reference (must exist since we're started by RegisterPool)
  WorkerPool *pool = nullptr;
  {
    std::lock_guard lock(impl_->pools_mutex_);
    pool = impl_->GetPool(pool_name);
  }

  if (!pool) {
    spdlog::error("PoolWorkerLoop: pool '{}' not found, exiting", pool_name);
    return;
  }

  while (impl_->running_.load(std::memory_order_acquire)) {
    // Wait for work signal via pool-specific eventfd (blocks until signaled)
    if (!pool->Wait(impl_->running_)) {
      break;  // Shutdown or error
    }

    std::shared_ptr<TaskState> task;
    {
      std::lock_guard lock(pool->mutex);
      if (!pool->queue.empty()) {
        // Priority queue: highest priority (lowest enum value) at top
        task = pool->queue.top();
        pool->queue.pop();
        pool->queued_tasks.fetch_sub(1, std::memory_order_release);
      }
    }

    if (task && !task->stopped.load(std::memory_order_acquire)) {
      // Set thread name to task name while executing
      utils::ThreadSetName(task->name);
      pool->active_workers.fetch_add(1, std::memory_order_release);

      task->ExecuteCallback();

      pool->active_workers.fetch_sub(1, std::memory_order_release);
      // Restore pool name
      utils::ThreadSetName(pool_name);

      // Reschedule after execution completes
      if (task->reschedule_task) {
        task->reschedule_task(task);
      }
    }
  }
}

void ConsolidatedScheduler::WakeDispatcher() { impl_->timer_backend_.UpdateDeadline(std::chrono::steady_clock::now()); }

}  // namespace memgraph::utils
