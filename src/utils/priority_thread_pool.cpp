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

#include "utils/logging.hpp"

namespace memgraph::utils {

PriorityThreadPool::PriorityThreadPool(size_t mixed_work_threads_count, size_t high_priority_threads_count) {
  pool_.reserve(mixed_work_threads_count + high_priority_threads_count);
  for (size_t i = 0; i < mixed_work_threads_count; ++i) {
    pool_.emplace_back([this]() {
      Worker worker(*this);
      worker.operator()<Priority::LOW>();
    });
  }
  for (size_t i = 0; i < high_priority_threads_count; ++i) {
    pool_.emplace_back([this]() {
      Worker worker(*this);
      worker.operator()<Priority::HIGH>();
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
    high_priority_queue_ = {};
    low_priority_queue_ = {};

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

void PriorityThreadPool::ScheduledAddTask(TaskSignature new_task, const Priority priority) {
  auto l = std::unique_lock{pool_lock_};

  if (pool_stop_source_.stop_requested()) [[unlikely]] {
    return;
  }

  // Mixed work threads can service both high and low priority tasks
  if (!mixed_threads_.empty()) {
    auto *thread = mixed_threads_.top();
    mixed_threads_.pop();
    thread->push(std::move(new_task));
    return;
  }

  // High priority threads can only service high priority tasks
  if (priority == Priority::HIGH) {
    if (!high_priority_threads_.empty()) {
      auto *thread = high_priority_threads_.top();
      high_priority_threads_.pop();
      thread->push(std::move(new_task));
      return;
    }
  }

  // No threads available, enqueue the task
  if (priority == Priority::HIGH) {
    high_priority_queue_.emplace(std::move(new_task));
  } else {
    low_priority_queue_.emplace(std::move(new_task));
  }
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

}  // namespace memgraph::utils
