// Copyright 2024 Memgraph Ltd.
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

void ThreadPool::AddTask(std::function<void()> new_task) {
  {
    spdlog::trace("Trying to acquire lock when adding task to thread pool in thread {}.", std::this_thread::get_id());
    auto guard = std::unique_lock{pool_lock_};
    spdlog::trace("Acquired lock when adding task to thread pool in thread {}.", std::this_thread::get_id());
    if (pool_stop_source_.stop_requested()) {
      spdlog::trace("Released lock when adding task to thread pool in thread {}.", std::this_thread::get_id());
      return;
    }
    task_queue_.emplace(std::move(new_task));
  }
  unfinished_tasks_num_.fetch_add(1);

  queue_cv_.notify_one();
  spdlog::trace("Released lock when adding task to thread pool in thread {}.", std::this_thread::get_id());
}

void ThreadPool::ShutDown() {
  spdlog::trace("Shutting down thread pool in thread {}.", std::this_thread::get_id());
  {
    spdlog::trace("Trying to acquire lock when shutting down thread pool in thread {}.", std::this_thread::get_id());
    auto guard = std::unique_lock{pool_lock_};
    spdlog::trace("Acquired lock when shutting down thread pool in thread {}.", std::this_thread::get_id());
    pool_stop_source_.request_stop();
    auto empty_queue = std::queue<TaskSignature>{};
    task_queue_.swap(empty_queue);
    spdlog::trace("Lock released when shutting down thread pool in thread {}.", std::this_thread::get_id());
  }

  queue_cv_.notify_all();
  thread_pool_.clear();
}

ThreadPool::~ThreadPool() {
  spdlog::trace("Destroying thread pool in thread {}.", std::this_thread::get_id());
  if (!pool_stop_source_.stop_requested()) {
    spdlog::trace("Stop not yet requested when destroying thread pool, shutting it down in thread {}.",
                  std::this_thread::get_id());
    ShutDown();
    spdlog::trace("Thread pool shut down while destroying thread pool, shutting it down in thread {}.",
                  std::this_thread::get_id());
  }
}

void ThreadPool::ThreadLoop() {
  auto token = pool_stop_source_.get_token();
  while (true) {
    TaskSignature task;
    {
      spdlog::trace("Trying to acquire lock in ThreadLoop in thread {}.", std::this_thread::get_id());
      auto guard = std::unique_lock{pool_lock_};
      spdlog::trace("Lock acquired in ThreadLoop in thread {}.", std::this_thread::get_id());
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
