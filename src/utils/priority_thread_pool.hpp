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
#include <optional>
#include <queue>
#include <stack>
#include <thread>

namespace memgraph::utils {

class PriorityThreadPool {
 public:
  enum class TaskPriority : uint8_t { LOW, HIGH };
  using TaskSignature = std::function<void()>;

  struct Task {
    mutable TaskSignature task;  // allows moves from priority_queue
    TaskPriority priority;
    bool operator<(const Task &other) const { return priority < other.priority; }
    void operator()() const { task(); }
  };

  PriorityThreadPool(size_t mixed_work_threads_count, size_t high_priority_threads_count);

  ~PriorityThreadPool();

  PriorityThreadPool(const PriorityThreadPool &) = delete;
  PriorityThreadPool(PriorityThreadPool &&) = delete;
  PriorityThreadPool &operator=(const PriorityThreadPool &) = delete;
  PriorityThreadPool &operator=(PriorityThreadPool &&) = delete;

  void ShutDown();

  void ScheduledAddTask(TaskSignature new_task, TaskPriority priority);

  class Worker;

  std::optional<TaskSignature> GetTask(Worker *thread);

  class Worker {
   public:
    void push(TaskSignature new_task);

    void stop();

    void operator()();

    Worker(PriorityThreadPool &scheduler, TaskPriority priority) : priority_{priority}, scheduler_{scheduler} {}

    const TaskPriority priority_;

   private:
    mutable std::mutex mtx_;
    std::atomic_bool run_{true};
    std::condition_variable_any cv_;
    std::optional<TaskSignature> task_{};
    PriorityThreadPool &scheduler_;
  };

 private:
  mutable std::mutex pool_lock_;
  std::stop_source pool_stop_source_;

  std::vector<std::jthread> pool_;

  std::priority_queue<Task> task_queue_;

  std::stack<Worker *> mixed_threads_;
  std::stack<Worker *> high_priority_threads_;
};

}  // namespace memgraph::utils
