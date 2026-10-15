// Copyright 2026 Memgraph Ltd.
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
#include <cstddef>
#include <functional>
#include <mutex>
#include <queue>
#include <stop_token>
#include <thread>
#include <vector>

namespace memgraph::utils {

class ThreadPool {
 public:
  using TaskSignature = std::move_only_function<void()>;
  using ThreadInitFn = std::move_only_function<TaskSignature()>;

  // Optional initializer runs once inside each worker thread before tasks start
  // and may return a cleanup callback that runs when the worker exits.
  explicit ThreadPool(size_t pool_size, ThreadInitFn thread_init = {});

  // Returns false if the pool has already been shut down, in which case the task is dropped and never runs.
  bool AddTask(TaskSignature new_task);

  // Discards queued tasks rather than draining them (a task already popped by a worker still runs to
  // completion), returning how many were discarded; draining is avoided since a queued task can block on external I/O.
  size_t ShutDown();

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
  ThreadInitFn thread_init_;

  std::vector<std::jthread> thread_pool_;

  std::atomic<size_t> unfinished_tasks_num_{0};  //<! ATM only exists for testing purposes
};

}  // namespace memgraph::utils
