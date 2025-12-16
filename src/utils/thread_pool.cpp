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

#include "utils/thread_pool.hpp"

namespace memgraph::utils {

ThreadPool::ThreadPool(const size_t pool_size) {
  for (size_t i = 0; i < pool_size; ++i) {
    thread_pool_.emplace_back([this] { this->ThreadLoop(); });
  }
}

void ThreadPool::AddTask(TaskSignature new_task) {
  {
    auto guard = std::unique_lock{pool_lock_};
    if (pool_stop_source_.stop_requested()) {
      return;
    }
    task_queue_.emplace(std::move(new_task));
  }
  unfinished_tasks_num_.fetch_add(1);

  queue_cv_.notify_one();
}

void ThreadPool::ShutDown() {
  {
    auto guard = std::unique_lock{pool_lock_};
    pool_stop_source_.request_stop();
    auto empty_queue = std::queue<TaskSignature>{};
    task_queue_.swap(empty_queue);
  }

  queue_cv_.notify_all();
  thread_pool_.clear();
}

ThreadPool::~ThreadPool() {
  if (!pool_stop_source_.stop_requested()) {
    ShutDown();
  }
}

void ThreadPool::ThreadLoop() {
  auto const token = pool_stop_source_.get_token();
  while (true) {
    TaskSignature task;
    {
      auto guard = std::unique_lock{pool_lock_};
      queue_cv_.wait(guard, token, [&] { return !task_queue_.empty(); });
      if (token.stop_requested()) return;
      task = std::move(task_queue_.front());
      task_queue_.pop();
    }
    task();
    unfinished_tasks_num_.fetch_sub(1);
  }
}

size_t ThreadPool::UnfinishedTasksNum() const { return unfinished_tasks_num_.load(); }

}  // namespace memgraph::utils
