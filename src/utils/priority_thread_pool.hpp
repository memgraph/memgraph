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
#include <mutex>
#include <queue>
#include <thread>

#include "utils/priorities.hpp"
#include "utils/scheduler.hpp"

namespace memgraph::utils {

class PriorityThreadPool {
 public:
  using TaskSignature = std::function<void(utils::Priority)>;
  using TaskID = uint64_t;

  PriorityThreadPool(size_t mixed_work_threads_count, size_t high_priority_threads_count);

  ~PriorityThreadPool();

  PriorityThreadPool(const PriorityThreadPool &) = delete;
  PriorityThreadPool(PriorityThreadPool &&) = delete;
  PriorityThreadPool &operator=(const PriorityThreadPool &) = delete;
  PriorityThreadPool &operator=(PriorityThreadPool &&) = delete;

  void AwaitShutdown();

  void ShutDown();

  void ScheduledAddTask(TaskSignature new_task, Priority priority);

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
    void operator()(uint16_t worker_id, std::vector<Worker *> workers_pool);

   private:
    mutable std::mutex mtx_;
    std::condition_variable cv_;
    std::priority_queue<Work> work_;

    std::atomic_bool has_pending_work_{false};
    std::atomic_bool working_{false};
    std::atomic_bool run_{true};

    std::atomic<TaskID> last_task_{0};

    friend class PriorityThreadPool;
  };

 private:
  std::stop_source pool_stop_source_;
  std::vector<std::jthread> pool_;  // All available threads
  utils::Scheduler monitoring_;     // Background task monitoring the overall throughput and rearranging

  std::vector<Worker *> work_buckets_;     // Mixed work threads
  std::vector<Worker *> hp_work_buckets_;  // High priority work threads | ideally tasks yield and this isn't needed

  std::atomic<TaskID> task_id_;     // Generates a unique tasks id | MSB signals high priority
  std::atomic<uint16_t> last_wid_;  // Used to pick next worker
};

}  // namespace memgraph::utils
