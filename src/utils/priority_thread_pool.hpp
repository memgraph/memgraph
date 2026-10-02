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
#include <cstdint>
#include <functional>
#include <memory>
#include <mutex>
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
    const auto bit = GroupMask(id);
    const auto prev = hot_masks_[GetGroup(id)].fetch_and(~bit, std::memory_order::acq_rel);
    if (n_groups_ == 1 && (prev & bit) != 0 && (prev & ~bit) == 0) NotifyEmpty();
  }

  // Single-waiter parking for the poller's blocking thread; only supported with one mask word (<= 64 workers).
  bool SingleWord() const { return n_groups_ == 1; }

  // Returns once the mask may be empty (or after WakeWaiter). No lost wake-up: the waiter publishes parked_ and
  // re-checks the mask after a seq_cst fence, the notifier fences after emptying the mask and then reads parked_,
  // so one side sees the other; gate_.wait(g) also returns at once if gate_ moved since g was read.
  void WaitUntilEmpty(const std::atomic_bool &stop) {
    const auto g = gate_.load(std::memory_order::acquire);
    waiter_parked_.store(true, std::memory_order::relaxed);
    std::atomic_thread_fence(std::memory_order::seq_cst);
    if (hot_masks_[0].load(std::memory_order::acquire) != 0 && !stop.load(std::memory_order::acquire)) {
      gate_.wait(g, std::memory_order::acquire);
    }
    waiter_parked_.store(false, std::memory_order::relaxed);
  }

  bool AnyHot() const { return hot_masks_[0].load(std::memory_order::acquire) != 0; }

  void WakeWaiter() {
    gate_.fetch_add(1, std::memory_order::acq_rel);
    gate_.notify_all();
  }

  // Returns the position of the first set bit and resets it
  std::optional<uint16_t> GetHotElement();

 private:
  // Called after a transition of the mask to empty.
  void NotifyEmpty() {
    std::atomic_thread_fence(std::memory_order::seq_cst);
    if (waiter_parked_.load(std::memory_order::relaxed)) WakeWaiter();
  }

  static constexpr auto kGroupSize = sizeof(uint64_t) * 8;  // bits
  static constexpr auto kGroupMask = kGroupSize - 1;

  // Get element's group
  static constexpr uint16_t GetGroup(const uint64_t id) { return id / kGroupSize; }

  // Get number of groups
  static inline uint16_t GetNumGroups(const uint64_t n_elements) { return (n_elements - 1) / kGroupSize + 1; }

  // Mask as seen by the appropriate group
  static constexpr uint64_t GroupMask(const uint64_t id) { return 1UL << (id & kGroupMask); }

  std::array<std::atomic<uint64_t>, kMaxElements / kGroupSize> hot_masks_{};
  std::atomic<uint32_t> gate_{0};
  std::atomic_bool waiter_parked_{false};
#ifndef NDEBUG
  const uint16_t n_elements_;
#endif
  const uint16_t n_groups_;
};

using TaskSignature = std::move_only_function<void(utils::Priority)>;

// A unit of work claimed from an IdlePoller; the claimant owns it and runs it on its own thread.
struct IdleRunnable {
  virtual ~IdleRunnable() = default;
  virtual void RunInline(Priority thread_priority) = 0;
};

// Event source polled by idle (spinning) mixed workers. Implemented outside utils.
struct IdlePoller {
  virtual ~IdlePoller() = default;
  // Never blocks. Returns a claimed, ready unit of work or nullptr.
  virtual std::shared_ptr<IdleRunnable> TryClaim() = 0;
};

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
                     ThreadInitCallback thread_init_callback = nullptr);

  ~PriorityThreadPool();

  PriorityThreadPool(const PriorityThreadPool &) = delete;
  PriorityThreadPool(PriorityThreadPool &&) = delete;
  PriorityThreadPool &operator=(const PriorityThreadPool &) = delete;
  PriorityThreadPool &operator=(PriorityThreadPool &&) = delete;

  void AwaitShutdown();

  void ShutDown();

  void ScheduledAddTask(TaskSignature new_task, Priority priority);

  void ScheduledCollection(TaskCollection &collection) {
    for (size_t i = 0; i < collection.Size(); ++i) {
      ScheduledAddTask(collection.WrapTask(i), Priority::LOW);
    }
  }

  HotMask &GetHotMask() { return hot_threads_; }

  uint64_t GetNumMixedWorkers() const { return workers_.size(); }

  uint64_t GetNumHighPriorityWorkers() const { return hp_workers_.size(); }

  // The poller must outlive every worker call into it: clear it (or join the pool) before destroying it.
  void SetIdlePoller(IdlePoller *poller) { idle_poller_.store(poller, std::memory_order_release); }

  uint64_t GetNumWorkers() const { return workers_.size() + hp_workers_.size(); }

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

      bool operator<(const Work &other) const { return id < other.id; }
    };

    void push(TaskSignature new_task, TaskID id);

    void stop();

    template <Priority ThreadPriority>
    void operator()(uint16_t worker_id, const std::vector<std::unique_ptr<Worker>> &workers_pool, HotMask &hot_threads,
                    const std::atomic<IdlePoller *> &idle_poller);

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

    friend class PriorityThreadPool;
  };

 private:
  std::stop_source pool_stop_source_;

  std::vector<std::unique_ptr<Worker>> workers_;  // Mixed work threads
  std::vector<std::unique_ptr<Worker>>
      hp_workers_;       // High priority work threads | ideally tasks yield and this isn't needed
  HotMask hot_threads_;  // Mask of workers waiting for new work (but still not sleeping)

  std::vector<std::jthread> pool_;  // All available threads (list so the elements are stable)
  utils::Scheduler monitoring_;     // Background task monitoring the overall throughput and rearranging

  std::atomic<IdlePoller *> idle_poller_{nullptr};
  std::atomic<TaskID> task_id_;     // Generates a unique tasks id | MSB signals high priority
  std::atomic<uint16_t> last_wid_;  // Used to pick next worker
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
