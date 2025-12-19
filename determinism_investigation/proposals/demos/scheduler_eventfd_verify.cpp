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

// Verify eventfd-based scheduler has fewer futex calls
// Build with: g++ -std=c++20 -O2 -pthread -I../../../src -L../../../build/src/utils scheduler_eventfd_verify.cpp -o
// scheduler_eventfd_verify -lmg-utils

#include <atomic>
#include <chrono>
#include <cstdio>
#include <thread>

// Simple test - just spawn scheduler with workers and run tasks
// to compare futex vs eventfd behavior

#include <sys/eventfd.h>
#include <unistd.h>
#include <condition_variable>
#include <functional>
#include <mutex>
#include <queue>
#include <vector>

using namespace std::chrono_literals;

// Minimal eventfd-based scheduler (matching new ConsolidatedScheduler design)
class EventfdScheduler {
 public:
  explicit EventfdScheduler(size_t workers) {
    work_eventfd_ = eventfd(0, EFD_SEMAPHORE);
    running_ = true;
    for (size_t i = 0; i < workers; ++i) {
      workers_.emplace_back([this] { WorkerLoop(); });
    }
  }

  ~EventfdScheduler() {
    running_ = false;
    // Signal all workers to exit
    uint64_t val = workers_.size();
    write(work_eventfd_, &val, sizeof(val));
    for (auto &w : workers_) {
      w.join();
    }
    close(work_eventfd_);
  }

  void Submit(std::function<void()> task) {
    {
      std::lock_guard lock(mutex_);
      queue_.push(std::move(task));
    }
    uint64_t val = 1;
    write(work_eventfd_, &val, sizeof(val));
  }

 private:
  void WorkerLoop() {
    while (running_) {
      uint64_t val;
      if (read(work_eventfd_, &val, sizeof(val)) <= 0) continue;
      if (!running_) break;

      std::function<void()> task;
      {
        std::lock_guard lock(mutex_);
        if (!queue_.empty()) {
          task = std::move(queue_.front());
          queue_.pop();
        }
      }
      if (task) task();
    }
  }

  int work_eventfd_;
  std::mutex mutex_;
  std::queue<std::function<void()>> queue_;
  std::vector<std::thread> workers_;
  std::atomic<bool> running_{true};
};

int main() {
  const int NUM_TASKS = 100;
  const int NUM_WORKERS = 4;

  printf("Testing eventfd-based scheduler with %d workers, %d tasks...\n", NUM_WORKERS, NUM_TASKS);

  std::atomic<int> counter{0};
  auto task = [&counter] { counter++; };

  {
    EventfdScheduler sched(NUM_WORKERS);
    for (int i = 0; i < NUM_TASKS; ++i) {
      sched.Submit(task);
    }
    std::this_thread::sleep_for(100ms);  // Wait for completion
  }

  printf("Counter: %d (expected %d)\n", counter.load(), NUM_TASKS);
  return counter.load() == NUM_TASKS ? 0 : 1;
}
