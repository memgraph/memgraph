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
#include <functional>
#include <memory>
#include <mutex>
#include <optional>
#include <queue>
#include <stop_token>
#include <thread>
#include <vector>

#include "utils/logging.hpp"
#include "utils/priorities.hpp"

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

  // Over-notifying is harmless; under-notifying is impossible: the last clear's load reads 0 or a later Set.
  inline void Reset(const uint64_t id) {
    DMG_ASSERT(id < n_elements_, "Trying to reset out-of-bounds");
    auto &word = hot_masks_[GetGroup(id)];
    word.fetch_and(~GroupMask(id), std::memory_order::acq_rel);
    if (n_groups_ == 1 && word.load(std::memory_order::relaxed) == 0) NotifyEmpty();
  }

  // Like Reset, but reports whether the bit was still set (false: a producer took it via GetHotElement).
  inline bool ResetWasSet(const uint64_t id) {
    DMG_ASSERT(id < n_elements_, "Trying to reset out-of-bounds");
    const auto bit = GroupMask(id);
    const auto prev = hot_masks_[GetGroup(id)].fetch_and(~bit, std::memory_order::acq_rel);
    if (n_groups_ == 1 && (prev & bit) != 0 && (prev & ~bit) == 0) NotifyEmpty();
    return (prev & bit) != 0;
  }

  // Single-waiter parking is only supported with one mask word (<= 64 workers).
  bool SingleWord() const { return n_groups_ == 1; }

  // Parks until the mask may be empty, WakeWaiter, stop or deadline; false only on timeout, true means "re-check".
  // No lost wake-up: waiter publishes parked_, fences, re-checks the mask; NotifyEmpty fences, then reads parked_.
  // A WakeWaiter after the gate_ read makes FUTEX_WAIT fail with EAGAIN.
  bool WaitUntilEmpty(std::chrono::steady_clock::time_point deadline, const std::stop_token &stop);

  // Must precede the first WaitUntilEmpty; until then the empty-transition path costs workers nothing. The pool calls
  // it from SetIdlePoller, before publishing the poller.
  void EnableWaiter() {
    if (!waiter_possible_.load(std::memory_order::relaxed)) waiter_possible_.store(true, std::memory_order::seq_cst);
  }

  bool AnyHot() const { return hot_masks_[0].load(std::memory_order::acquire) != 0; }

  void WakeWaiter();

  // Returns the position of the first set bit and resets it
  std::optional<uint16_t> GetHotElement();

 private:
  void NotifyEmpty() {
    if (!waiter_possible_.load(std::memory_order::relaxed)) return;
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
  // Each on its own cache line, away from hot_masks_ which every worker writes.
  alignas(64) std::atomic<uint32_t> gate_{0};
  alignas(64) std::atomic_bool waiter_parked_{false};
  alignas(64) std::atomic_bool waiter_possible_{false};
#ifndef NDEBUG
  const uint16_t n_elements_;
#endif
  const uint16_t n_groups_;
};

using TaskSignature = std::move_only_function<void(utils::Priority)>;

// A unit of work claimed from an IdlePoller; the claimant owns it and runs it on its own thread.
// Callers of RunInline/Dispatch hold their own reference across the call, so the runnable may close itself inside.
struct IdleRunnable {
  virtual ~IdleRunnable() = default;
  virtual void RunInline(Priority thread_priority) = 0;
  // Hands the claimed work to the pool instead of running it on the claimant's thread.
  virtual void Dispatch() = 0;
};

// Lifetime: the poller must outlive every session it hands out (the pool is joined first); ClearIdlePoller only
// guarantees no TryClaim or monitor call is in flight on return. RunInline/Dispatch (and ~runnable, e.g. ~Session on
// the monitor under monitor_use_mtx_) must never call SetIdlePoller/ClearIdlePoller: self-deadlock.
struct IdlePoller {
  virtual ~IdlePoller() = default;
  // Never blocks; the pool allows one caller at a time. Returns a claimed, ready unit of work or nullptr.
  virtual std::shared_ptr<IdleRunnable> TryClaim() = 0;
  // Monitor thread only. Blocks up to `max`, Dispatch()es what it claims, returns early after Wake().
  virtual void WaitAndDispatch(std::chrono::milliseconds max) = 0;
  // Any thread: interrupts a current or the next WaitAndDispatch.
  virtual void Wake() = 0;
};

struct IdlePollState {
  std::atomic<IdlePoller *> poller{nullptr};
  alignas(64) std::atomic_bool token{false};  // held by the one worker inside TryClaim
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

  // Test seam: only unit tests call this.
  HotMask &GetHotMask() { return hot_threads_; }

  uint64_t GetNumMixedWorkers() const { return workers_.size(); }

  uint64_t GetNumHighPriorityWorkers() const { return hp_workers_.size(); }

  // Attach; the poller must stay valid until ClearIdlePoller returns (see IdlePoller lifetime).
  void SetIdlePoller(IdlePoller *poller);

  // Detach. On return no worker is inside (or will enter) TryClaim and the monitor is not inside the poller.
  void ClearIdlePoller();

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
                    IdlePollState &idle_poll);

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
  void MonitorLoop(const std::stop_token &stop);
  void MonitorTick(std::array<TaskID, HotMask::kMaxElements> &last_task);
  // wake_poller=false: the monitor is in its cv wait (no poller yet), so skip the eventfd write.
  void WakeMonitor(IdlePoller *poller, bool wake_poller = true);

  std::stop_source pool_stop_source_;

  std::vector<std::unique_ptr<Worker>> workers_;  // Mixed work threads
  std::vector<std::unique_ptr<Worker>>
      hp_workers_;       // High priority work threads | ideally tasks yield and this isn't needed
  HotMask hot_threads_;  // Mask of workers waiting for new work (but still not sleeping)

  IdlePollState idle_poll_;
  std::atomic<TaskID> task_id_;     // Generates a unique tasks id | MSB signals high priority
  std::atomic<uint16_t> last_wid_;  // Used to pick next worker

  std::mutex attach_mtx_;       // serialises SetIdlePoller / ClearIdlePoller / ShutDown's read of the poller
  std::mutex monitor_use_mtx_;  // held by the monitor around its poller load + WaitAndDispatch
  std::mutex monitor_cv_mtx_;
  std::condition_variable_any monitor_cv_;  // monitor sleeps here while no poller is attached
  // Declared after everything the worker threads and the monitor touch: members die in reverse order, so the threads
  // are joined before any of that state is destroyed.
  std::vector<std::jthread> pool_;  // All available threads (list so the elements are stable)
  std::jthread monitor_;            // Throughput monitor and idle-poller driver
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
