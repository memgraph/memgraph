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

#include <array>
#include <atomic>
#include <chrono>
#include <condition_variable>
#include <cstdint>
#include <deque>
#include <functional>
#include <memory>
#include <mutex>
#include <optional>
#include <queue>
#include <thread>

#include "utils/logging.hpp"
#include "utils/priorities.hpp"
#include "utils/scheduler.hpp"

namespace memgraph::utils {

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

  // Non-destructive: true iff any worker is idle (GetHotElement would consume the slot).
  bool AnySet() const noexcept;

  // Non-destructive idle-worker count; relaxed loads, so a soft pressure signal only.
  uint16_t Count() const noexcept;

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

enum class WaitResource : uint8_t { MainLock, CommitLock };

using TaskSignature = std::move_only_function<void(utils::Priority)>;

// Collection of tasks that can be executed by the thread pool
// The idea is to batch tasks and have the ability to wait on them
// Also execute non scheduler tasks in the local thread
class TaskCollection {
 public:
  explicit TaskCollection(size_t num_tasks) { tasks_.reserve(num_tasks); }

  TaskCollection() = default;

  void AddTask(TaskSignature task) { tasks_.emplace_back(std::move(task)); }

  class Task {
   public:
    explicit Task(TaskSignature task)
        : state_(std::make_shared<std::atomic<State>>(State::IDLE)), task_(std::move(task)) {}

    ~Task() = default;
    Task(const Task &) = delete;
    Task(Task &&) = default;
    Task &operator=(const Task &) = delete;
    Task &operator=(Task &&) = default;

    enum class State : uint8_t {
      IDLE,
      SCHEDULED,
      FINISHED,
    };
    std::shared_ptr<std::atomic<State>> state_;
    TaskSignature task_;
  };

  Task &operator[](size_t index) { return tasks_[index]; }

  TaskSignature WrapTask(size_t index);

  void Wait();

  void WaitOrSteal();

  size_t Size() const { return tasks_.size(); }

 private:
  std::vector<Task> tasks_;
};

class PriorityThreadPool {
 public:
  using TaskID = uint64_t;
  using ThreadInitCallback = std::function<void()>;

  PriorityThreadPool(uint16_t mixed_work_threads_count, uint16_t high_priority_threads_count,
                     ThreadInitCallback thread_init_callback = nullptr,
                     std::chrono::milliseconds monitor_interval = std::chrono::milliseconds{100});  // test seam

  ~PriorityThreadPool();

  PriorityThreadPool(const PriorityThreadPool &) = delete;
  PriorityThreadPool(PriorityThreadPool &&) = delete;
  PriorityThreadPool &operator=(const PriorityThreadPool &) = delete;
  PriorityThreadPool &operator=(PriorityThreadPool &&) = delete;

  void AwaitShutdown();

  void ShutDown();

  TaskID ScheduledAddTask(TaskSignature new_task, Priority priority, bool productive = true);

  // Place-keeping re-post under the original id (FIFO order). Non-productive: retries are not counted
  // in productive_pending_.
  void ScheduledReAddTask(TaskSignature task, TaskID id);

  void ScheduledCollection(TaskCollection &collection) {
    for (size_t i = 0; i < collection.Size(); ++i) {
      ScheduledAddTask(collection.WrapTask(i), Priority::LOW);
    }
  }

  uint64_t GetNumMixedWorkers() const { return workers_.size(); }

  uint64_t GetNumHighPriorityWorkers() const { return hp_workers_.size(); }

  uint64_t GetNumWorkers() const { return workers_.size() + hp_workers_.size(); }

  // Queued productive (non-retry) tasks; clamped at 0 because relaxed inc/dec can transiently go negative.
  uint64_t QueuedProductiveTasks() const noexcept {
    const auto v = productive_pending_.load(std::memory_order_relaxed);
    return v > 0 ? static_cast<uint64_t>(v) : 0;
  }

  uint16_t FreeWorkers() const noexcept { return hot_threads_.Count(); }

  // Timed-wait budget for an admission that would block: kTryBudgetMax with idle workers and no backlog,
  // shrinking to kTryBudgetMin as workers get scarce or the backlog grows.
  std::chrono::microseconds AdmissionTryBudget() const noexcept;

  // Re-posts an admission makes before parking via ParkAdmission; shrinks with pressure so a busy pool
  // parks sooner, and grows when idle so a cheap re-post beats a park round-trip.
  uint32_t AdmissionRescheduleCap() const noexcept;

  // Parks until WakeMatching(resource), or until the monitor sweep re-posts it at `deadline` (or as the oldest
  // parked, against starvation). Dropped if draining. Must not hold any Worker::mtx_.
  // observed_epoch: WakeEpoch(resource) sampled before the failed attempt; if it moved, re-post instead of parking.
  void ParkAdmission(TaskSignature task, TaskID id, std::chrono::steady_clock::time_point deadline,
                     WaitResource resource, std::optional<uint64_t> observed_epoch = std::nullopt);

  uint64_t WakeEpoch(WaitResource resource) const noexcept {
    return wake_epochs_[static_cast<size_t>(resource)].value.load(std::memory_order_seq_cst);
  }

  // True iff productive work is queued and no worker is idle to take it, i.e. parking frees a core for it.
  bool ShouldParkAdmission() const noexcept;

  // Re-posts every task parked on the resource (runs them inline while draining).
  // Lock order: parked_mtx_ is released before ScheduledReAddTask takes Worker::mtx_.
  void WakeMatching(WaitResource resource);

  // True once ShutDown starts draining parks; session continuations drop themselves instead of re-parking.
  bool IsDrainingAdmissions() const noexcept { return draining_admissions_.load(std::memory_order_acquire); }

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
      bool productive{true};       // false for admission retries; excluded from productive_pending_

      bool operator<(const Work &other) const { return id < other.id; }
    };

    void push(TaskSignature new_task, TaskID id, bool productive = true);

    void stop();

    template <Priority ThreadPriority>
    void operator()(uint16_t worker_id, const std::vector<std::unique_ptr<Worker>> &workers_pool, HotMask &hot_threads);

   private:
    mutable std::mutex mtx_;
    std::condition_variable cv_;
    std::priority_queue<Work> work_;

    // Stats
    std::atomic_bool has_pending_work_{false};
    std::atomic_bool working_{false};
    std::atomic_bool run_{true};
    // Used by monitor to decide if worker is blocked
    std::atomic<TaskID> last_task_{0};

    // Pool-owned counter, wired in the pool constructor.
    std::atomic<int64_t> *productive_pending_{nullptr};

    friend class PriorityThreadPool;
  };

 private:
  // Range for AdmissionTryBudget(); perf-tunable.
  static constexpr std::chrono::microseconds kTryBudgetMin{2};        // tightest budget under max pressure
  static constexpr std::chrono::microseconds kTryBudgetMax{100'000};  // 100 ms: no-pressure long wait

  // Range for AdmissionRescheduleCap(); perf-tunable.
  static constexpr uint32_t kRescheduleCapMin{1};  // park after 1 reschedule under high pressure
  static constexpr uint32_t kRescheduleCapMax{4};  // park after 4 reschedules under light pressure

  struct ParkedAdmission {
    TaskID id;
    mutable TaskSignature task;
    std::chrono::steady_clock::time_point deadline;
    WaitResource resource;
  };

  struct alignas(64) WakeEpochCounter {
    std::atomic<uint64_t> value{0};
  };

  std::array<WakeEpochCounter, 2> wake_epochs_;  // indexed by WaitResource

  std::mutex parked_mtx_;
  std::deque<ParkedAdmission> parked_admissions_;  // FIFO: front = oldest
  std::atomic_bool draining_admissions_{false};    // set by ShutDown before the drain; new parks are dropped
  // Written under parked_mtx_; true iff deque is non-empty. seq_cst Dekker pairing with the wake epoch
  // (see ParkAdmission) means WakeMatching cannot miss a park; stale-true only costs a lock.
  std::atomic<bool> has_parked_{false};

  std::stop_source pool_stop_source_;

  std::vector<std::unique_ptr<Worker>> workers_;  // Mixed work threads
  std::vector<std::unique_ptr<Worker>>
      hp_workers_;       // High priority work threads | ideally tasks yield and this isn't needed
  HotMask hot_threads_;  // Mask of workers waiting for new work (but still not sleeping)

  std::vector<std::jthread> pool_;  // All available threads (list so the elements are stable)
  utils::Scheduler monitoring_;     // Background task monitoring the overall throughput and rearranging

  std::atomic<TaskID> task_id_;     // Generates a unique tasks id | MSB signals high priority
  std::atomic<uint16_t> last_wid_;  // Used to pick next worker
  // Per-pool wake-up modulus for re-posted (parked) tasks in ScheduledReAddTask.
  const TaskID max_wakeup_thread_;

  // Queued productive tasks (excludes admission retries): +1 in push, -1 at every pop and in AwaitShutdown.
  alignas(64) std::atomic<int64_t> productive_pending_{0};
};

class CollectionScheduler {
 public:
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

 private:
  PriorityThreadPool *pool_;
  std::shared_ptr<TaskCollection> collection_;
};

}  // namespace memgraph::utils
