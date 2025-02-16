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
#include <condition_variable>
#include <functional>
#include <iostream>
#include <limits>
#include <mutex>
#include <optional>
#include <queue>
#include <stack>
#include <thread>
#include "utils/priorities.hpp"
#include "utils/scheduler.hpp"
#include "utils/spin_lock.hpp"
#include "utils/thread.hpp"

namespace memgraph::utils {

class PriorityThreadPool {
 public:
  using TaskSignature = std::function<void()>;

  struct Task {
    mutable TaskSignature task;  // allows moves from priority_queue
    void operator()() const { task(); }
  };

  PriorityThreadPool(size_t mixed_work_threads_count, size_t high_priority_threads_count);

  ~PriorityThreadPool();

  PriorityThreadPool(const PriorityThreadPool &) = delete;
  PriorityThreadPool(PriorityThreadPool &&) = delete;
  PriorityThreadPool &operator=(const PriorityThreadPool &) = delete;
  PriorityThreadPool &operator=(PriorityThreadPool &&) = delete;

  void ShutDown();

  void ScheduledAddTask(TaskSignature new_task, Priority priority);

  class Worker;

  template <Priority ThreadPriority>
  std::optional<TaskSignature> GetTask(Worker *const thread) {
    auto l = std::unique_lock{pool_lock_};

    if (pool_stop_source_.stop_requested()) [[unlikely]] {
      thread->stop();
      return std::nullopt;
    }

    // All threads can service high priority tasks
    if (!high_priority_queue_.empty()) {
      auto task = std::move(high_priority_queue_.front().task);
      high_priority_queue_.pop();
      return {std::move(task)};
    }
    // Only mixed work threads can service low priority tasks
    if constexpr (ThreadPriority < Priority::HIGH) {
      if (!low_priority_queue_.empty()) {
        auto task = std::move(low_priority_queue_.front().task);
        low_priority_queue_.pop();
        return {std::move(task)};
      }
    }

    // No tasks in the queue, put the thread back in the pool
    if constexpr (ThreadPriority == Priority::HIGH) {
      high_priority_threads_.push(thread);
    } else {
      mixed_threads_.push(thread);
    }

    return std::nullopt;
  }

  class Worker {
   public:
    thread_local static Priority priority;
    struct Work {
      uint64_t id;
      mutable TaskSignature work;
      bool operator<(const Work &other) const { return id < other.id; }
    };
    std::priority_queue<Work> work_;

    void push(TaskSignature new_task, uint64_t id);

    void stop();

    template <Priority ThreadPriority>
    void operator()();

    explicit Worker(PriorityThreadPool &scheduler, int pin_core = -1) : scheduler_{scheduler}, pinned_core_(pin_core) {
      (void)scheduler_;
    }

   private:
    mutable std::mutex mtx_;
    std::atomic_bool run_{true};
    std::condition_variable cv_;
    std::optional<TaskSignature> task_{};
    PriorityThreadPool &scheduler_;

    std::atomic<uint64_t> last_task_{0};
    std::atomic_bool working_{false};
    std::atomic_bool has_pending_work_{false};

    int pinned_core_;

    friend class PriorityThreadPool;
  };

 private:
  std::vector<Worker *> work_buckets_;
  std::vector<Worker *> hp_work_buckets_;                          // TODO Unify
  std::atomic<uint64_t> id_{std::numeric_limits<int64_t>::max()};  // MSB signals high prior
  utils::Scheduler monitoring_;
  std::atomic<uint64_t> tid_{0};

  mutable std::mutex pool_lock_;
  std::stop_source pool_stop_source_;

  std::vector<std::jthread> pool_;

  std::queue<Task> high_priority_queue_;
  std::queue<Task> low_priority_queue_;

  std::stack<Worker *> mixed_threads_;
  std::stack<Worker *> high_priority_threads_;
};

}  // namespace memgraph::utils
