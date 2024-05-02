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

#pragma once
#include <atomic>
#include <condition_variable>
#include <functional>
#include <mutex>
#include <queue>
#include <thread>

#include "utils/spin_lock.hpp"
#include "utils/synchronized.hpp"
#include "utils/thread.hpp"

namespace memgraph::utils {

template <typename Func>
struct CopyMovableFunctionWrapper {
  CopyMovableFunctionWrapper(Func &&func) : func_{std::make_shared<Func>(std::move(func))} {}

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
  std::unique_ptr<TaskSignature> PopTask();

  void ThreadLoop();

  std::vector<std::thread> thread_pool_;

  std::atomic<size_t> unfinished_tasks_num_{0};
  std::atomic<bool> terminate_pool_{false};
  std::atomic<bool> stopped_{false};
  utils::Synchronized<std::queue<std::unique_ptr<TaskSignature>>, utils::SpinLock> task_queue_;
  std::mutex pool_lock_;
  std::condition_variable queue_cv_;
};

}  // namespace memgraph::utils
