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

namespace memgraph::utils {

template <typename Func>
struct CopyMovableFunctionWrapper {
  explicit CopyMovableFunctionWrapper(Func &&func) : func_{std::make_shared<Func>(std::move(func))} {}

  void operator()() { (*func_)(); }

 private:
  std::shared_ptr<Func> func_;
};

class ThreadPool {
  using TaskSignature = std::function<void()>;

 public:
  explicit ThreadPool(size_t pool_size);

  void AddTask(std::function<void()> new_task);

  void ShutDown();

  ~ThreadPool();

  ThreadPool(const ThreadPool &) = delete;
  ThreadPool(ThreadPool &&) = delete;
  ThreadPool &operator=(const ThreadPool &) = delete;
  ThreadPool &operator=(ThreadPool &&) = delete;

  size_t UnfinishedTasksNum() const;

 private:
  void ThreadLoop();

  std::mutex pool_lock_;
  std::condition_variable_any queue_cv_;

  std::queue<TaskSignature> task_queue_;
  std::stop_source pool_stop_source_;  //<! Common stop source for all the jthreads in `thread_pool_`

  std::vector<std::jthread> thread_pool_;

  std::atomic<size_t> unfinished_tasks_num_{0};  //<! ATM only exists for testing purposes
};

}  // namespace memgraph::utils
