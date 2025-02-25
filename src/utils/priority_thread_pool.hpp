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
#include <boost/asio/detail/event.hpp>
#include <condition_variable>
#include <functional>
#include <iostream>
#include <limits>
#include <mutex>
#include <optional>
#include <queue>
#include <stack>
#include <thread>
#include "utils/priorities.hpp"
#include "utils/scheduler.hpp"
#include "utils/spin_lock.hpp"
#include "utils/thread.hpp"

namespace memgraph::utils {

class PriorityThreadPool {
 public:
  using TaskSignature = std::function<void(utils::Priority)>;

  struct Task {
    mutable TaskSignature task;  // allows moves from priority_queue
    void operator()(utils::Priority p) const { task(p); }
  };

  PriorityThreadPool(size_t mixed_work_threads_count, size_t high_priority_threads_count);

  ~PriorityThreadPool();

  PriorityThreadPool(const PriorityThreadPool &) = delete;
  PriorityThreadPool(PriorityThreadPool &&) = delete;
  PriorityThreadPool &operator=(const PriorityThreadPool &) = delete;
  PriorityThreadPool &operator=(PriorityThreadPool &&) = delete;

  void ShutDown();

  void ScheduledAddTask(TaskSignature new_task, Priority priority);

  class Worker {
   public:
    struct Work {
      uint64_t id;
      mutable TaskSignature work;
      bool operator<(const Work &other) const { return id < other.id; }
    };

    void push(TaskSignature new_task, uint64_t id);

    void stop();

    template <Priority ThreadPriority>
    void operator()();

    explicit Worker(PriorityThreadPool &scheduler, uint16_t id) : scheduler_{scheduler}, id_{id} {}

   private:
    PriorityThreadPool &scheduler_;  // TODO Could be removed; check perf

    mutable std::mutex mtx_;
    std::condition_variable cv_;
    std::priority_queue<Work> work_;

    std::atomic_bool has_pending_work_{false};
    std::atomic_bool working_{false};
    std::atomic_bool run_{true};

    uint16_t id_;

    std::atomic<uint64_t> last_task_{0};

    friend class PriorityThreadPool;
  };

  inline void set_hot_thread(uint64_t id);
  inline void reset_hot_thread(uint64_t id);
  inline int get_hot_thread();

 private:
  std::vector<Worker *> work_buckets_;
  std::vector<Worker *> hp_work_buckets_;  // TODO Unify
  utils::Scheduler monitoring_;

  std::atomic<uint64_t> id_{std::numeric_limits<int64_t>::max()};  // MSB signals high prior

  std::atomic<uint64_t> tid_{0};
  std::atomic<uint64_t> hot_threads_{0};  // TODO Make it actually work for more than 64 t

  std::vector<std::jthread> pool_;
  std::stop_source pool_stop_source_;
};

}  // namespace memgraph::utils
