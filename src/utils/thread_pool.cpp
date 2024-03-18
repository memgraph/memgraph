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
#include <iostream>
namespace memgraph::utils {

ThreadPool::ThreadPool(const size_t pool_size) {
  for (size_t i = 0; i < pool_size; ++i) {
    // std::cout << "!!!!!!!!!!!!!!!!emplacing thread" << std::endl;
    thread_pool_.emplace_back(([this] { this->ThreadLoop(); }));
    // std::cout << "!!!!!!!!!!!!!!!!!!thread emplaced thread" << std::endl;
  }
}

void ThreadPool::AddTask(std::function<void()> new_task) {
  // std::cout << "!!!!!!!!!!!!!!!!!!Trying to add task...." << std::endl;
  task_queue_.WithLock([&](auto &queue) {
    queue.emplace(std::make_unique<TaskSignature>(std::move(new_task)));
    unfinished_tasks_num_.fetch_add(1);
  });
  // std::cout << "!!!!!!!!!!!!!!!!!!task added...."<< std::endl;
  std::unique_lock pool_guard(pool_lock_);
  // std::cout <<"!!!!!!!!!!!!!!!!!!took lock..."<< std::endl;
  queue_cv_.notify_one();
  // std::cout <<"!!!!!!!!!!!!!!!!!!took lock and notifyied..."<< std::endl;
}

void ThreadPool::Shutdown() {
  terminate_pool_.store(true);
  {
    std::unique_lock pool_guard(pool_lock_);
    queue_cv_.notify_all();
  }

  for (auto &thread : thread_pool_) {
    if (thread.joinable()) {
      thread.join();
    }
  }

  thread_pool_.clear();
  stopped_.store(true);
}

ThreadPool::~ThreadPool() {
  if (!stopped_.load()) {
    Shutdown();
  }
}

std::unique_ptr<ThreadPool::TaskSignature> ThreadPool::PopTask() {
  // std::cout <<"!!!!!!!!!!!!!!!!!!!pop task function called;"<< std::endl;
  return task_queue_.WithLock([](auto &queue) -> std::unique_ptr<TaskSignature> {
    if (queue.empty()) {
      return nullptr;
    }
    auto front = std::move(queue.front());
    queue.pop();
    return front;
  });
}

void ThreadPool::ThreadLoop() {
  // std::cout <<"!!!!!!!!!!!!thread loop called"<< std::endl;
  std::unique_ptr<TaskSignature> task = PopTask();
  // std::cout <<"!!!!!!!!!!!popped task before loop"<< std::endl;
  while (true) {
    while (task) {
      if (terminate_pool_.load()) {
        return;
      }
      (*task)();
      unfinished_tasks_num_.fetch_sub(1);
      task = PopTask();
    }

    std::unique_lock guard(pool_lock_);
    queue_cv_.wait(guard, [&] {
      // std::cout <<"!!!!!!!!!!!queue"<< std::endl;
      task = PopTask();
      // std::cout <<"!!!!!!!!!!!!!!!!popping task"<< std::endl;
      return task || terminate_pool_.load();
    });
    if (terminate_pool_.load()) {
      return;
    }
  }
}

size_t ThreadPool::UnfinishedTasksNum() const { return unfinished_tasks_num_.load(); }

}  // namespace memgraph::utils
