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

#include "utils/priority_thread_pool.hpp"
#include <queue>
#include "utils/logging.hpp"
#include "utils/thread.hpp"

namespace memgraph::utils {

PriorityThreadPool::PriorityThreadPool(size_t mixed_work_threads_count, size_t high_priority_threads_count) {
  pool_.reserve(mixed_work_threads_count + high_priority_threads_count);
  for (size_t i = 0; i < mixed_work_threads_count; ++i) {
    pool_.emplace_back([this]() {
      Worker worker(*this, TaskPriority::LOW);
      worker();
    });
  }
  for (size_t i = 0; i < high_priority_threads_count; ++i) {
    pool_.emplace_back([this]() {
      Worker worker(*this, TaskPriority::HIGH);
      worker();
    });
  }
}

PriorityThreadPool::~PriorityThreadPool() {
  if (!pool_stop_source_.stop_requested()) {
    ShutDown();
  }
}

void PriorityThreadPool::ShutDown() {
  {
    auto guard = std::unique_lock{pool_lock_};
    pool_stop_source_.request_stop();

    // Clear the task queue
    auto empty_queue = std::priority_queue<Task>{};
    task_queue_.swap(empty_queue);

    // Stop threads waiting for work
    while (!high_priority_threads_.empty()) {
      high_priority_threads_.top()->stop();
      high_priority_threads_.pop();
    }
    while (!mixed_threads_.empty()) {
      mixed_threads_.top()->stop();
      mixed_threads_.pop();
    }
    // Other threads are working and will stop when they check for next scheduled work
  }
  pool_.clear();
}

void PriorityThreadPool::ScheduledAddTask(TaskSignature new_task, TaskPriority priority) {
  auto l = std::unique_lock{pool_lock_};

  if (pool_stop_source_.stop_requested()) [[unlikely]] {
    return;
  }

  if (!mixed_threads_.empty()) {
    auto *thread = mixed_threads_.top();
    mixed_threads_.pop();
    thread->push(std::move(new_task));
    return;
  }
  // Prefer mixed work threads
  if (priority == TaskPriority::HIGH) {
    if (!high_priority_threads_.empty()) {
      auto *thread = high_priority_threads_.top();
      high_priority_threads_.pop();
      thread->push(std::move(new_task));
      return;
    }
  }

  // No threads available, enqueue the task
  task_queue_.emplace(std::move(new_task), priority);
}

std::optional<PriorityThreadPool::TaskSignature> PriorityThreadPool::GetTask(PriorityThreadPool::Worker *const thread) {
  auto l = std::unique_lock{pool_lock_};

  if (pool_stop_source_.stop_requested()) [[unlikely]] {
    thread->stop();
    return std::nullopt;
  }

  if (!task_queue_.empty()) {
    // Queue is ordered by priority
    if (thread->priority_ <= task_queue_.top().priority) {
      auto task = std::move(task_queue_.top().task);
      task_queue_.pop();
      return {std::move(task)};
    }
  }

  // No tasks in the queue, put the thread back in the pool
  if (thread->priority_ == TaskPriority::HIGH) [[unlikely]] {
    high_priority_threads_.push(thread);
  } else {
    mixed_threads_.push(thread);
  }

  return std::nullopt;
}

void PriorityThreadPool::Worker::push(TaskSignature new_task) {
  auto l = std::unique_lock{mtx_};
  DMG_ASSERT(!task_, "Thread already has a task");
  task_.emplace(std::move(new_task));
  cv_.notify_one();
}

void PriorityThreadPool::Worker::stop() {
  auto l = std::unique_lock{mtx_};
  run_ = false;
  cv_.notify_one();
}

void PriorityThreadPool::Worker::operator()() {
  utils::ThreadSetName(priority_ == TaskPriority::HIGH ? "high prior." : "low prior.");
  while (run_) {
    // Check if there is a scheduled task
    std::optional<TaskSignature> task = scheduler_.GetTask(this);
    if (!task) {
      // Wait for a new task
      auto l = std::unique_lock{mtx_};
      cv_.wait(l, [&] { return task_ || !run_; });

      // Stop requested
      if (!run_) [[unlikely]] {
        return;
      }

      task = *std::move(task_);
      task_.reset();
    }
    // Execute the task
    task.value()();
  }
}

}  // namespace memgraph::utils
