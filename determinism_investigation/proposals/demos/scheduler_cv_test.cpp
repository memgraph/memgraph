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

// Test to compare non-determinism: workers with CV vs inline execution
// Run under live-recorder and compare event counts

#include <sys/epoll.h>
#include <sys/eventfd.h>
#include <sys/timerfd.h>
#include <unistd.h>
#include <atomic>
#include <chrono>
#include <condition_variable>
#include <cstdio>
#include <deque>
#include <functional>
#include <mutex>
#include <queue>
#include <thread>
#include <vector>

using namespace std::chrono_literals;

// ============================================================================
// Approach 1: Mutex + Condition Variable (current design)
// ============================================================================
class CVScheduler {
 public:
  explicit CVScheduler(size_t workers) {
    running_ = true;
    for (size_t i = 0; i < workers; ++i) {
      workers_.emplace_back([this] { WorkerLoop(); });
    }
  }

  ~CVScheduler() {
    {
      std::lock_guard lock(mutex_);
      running_ = false;
    }
    cv_.notify_all();
    for (auto &w : workers_) {
      w.join();
    }
  }

  void Submit(std::function<void()> task) {
    {
      std::lock_guard lock(mutex_);
      queue_.push_back(std::move(task));
    }
    cv_.notify_one();
  }

 private:
  void WorkerLoop() {
    while (true) {
      std::function<void()> task;
      {
        std::unique_lock lock(mutex_);
        cv_.wait(lock, [this] { return !queue_.empty() || !running_; });
        if (!running_ && queue_.empty()) break;
        if (!queue_.empty()) {
          task = std::move(queue_.front());
          queue_.pop_front();
        }
      }
      if (task) task();
    }
  }

  std::mutex mutex_;
  std::condition_variable cv_;
  std::deque<std::function<void()>> queue_;
  std::vector<std::thread> workers_;
  bool running_{true};
};

// ============================================================================
// Approach 2: Eventfd (file descriptor based)
// ============================================================================
class EventfdScheduler {
 public:
  explicit EventfdScheduler(size_t workers) {
    event_fd_ = eventfd(0, EFD_SEMAPHORE);
    running_ = true;
    for (size_t i = 0; i < workers; ++i) {
      workers_.emplace_back([this] { WorkerLoop(); });
    }
  }

  ~EventfdScheduler() {
    running_ = false;
    // Wake all workers
    for (size_t i = 0; i < workers_.size(); ++i) {
      uint64_t val = 1;
      write(event_fd_, &val, sizeof(val));
    }
    for (auto &w : workers_) {
      w.join();
    }
    close(event_fd_);
  }

  void Submit(std::function<void()> task) {
    {
      std::lock_guard lock(mutex_);
      queue_.push_back(std::move(task));
    }
    uint64_t val = 1;
    write(event_fd_, &val, sizeof(val));
  }

 private:
  void WorkerLoop() {
    while (running_) {
      uint64_t val;
      // Blocking read on eventfd (decrements semaphore)
      if (read(event_fd_, &val, sizeof(val)) <= 0) continue;

      std::function<void()> task;
      {
        std::lock_guard lock(mutex_);
        if (!queue_.empty()) {
          task = std::move(queue_.front());
          queue_.pop_front();
        }
      }
      if (task) task();
    }
  }

  int event_fd_;
  std::mutex mutex_;  // Still need mutex for queue access
  std::deque<std::function<void()>> queue_;
  std::vector<std::thread> workers_;
  std::atomic<bool> running_{true};
};

// ============================================================================
// Approach 3: Inline (0 workers, dispatcher runs tasks)
// ============================================================================
class InlineScheduler {
 public:
  void Submit(std::function<void()> task) {
    task();  // Just run immediately
  }
};

// ============================================================================
// Test harness
// ============================================================================
int main(int argc, char **argv) {
  const int NUM_TASKS = 100;
  const int NUM_WORKERS = 4;

  std::string mode = argc > 1 ? argv[1] : "cv";

  std::atomic<int> counter{0};
  auto task = [&counter] { counter++; };

  auto start = std::chrono::steady_clock::now();

  if (mode == "cv") {
    printf("Testing CV-based scheduler with %d workers...\n", NUM_WORKERS);
    CVScheduler sched(NUM_WORKERS);
    for (int i = 0; i < NUM_TASKS; ++i) {
      sched.Submit(task);
    }
    std::this_thread::sleep_for(100ms);  // Wait for completion
  } else if (mode == "eventfd") {
    printf("Testing eventfd-based scheduler with %d workers...\n", NUM_WORKERS);
    EventfdScheduler sched(NUM_WORKERS);
    for (int i = 0; i < NUM_TASKS; ++i) {
      sched.Submit(task);
    }
    std::this_thread::sleep_for(100ms);
  } else if (mode == "inline") {
    printf("Testing inline scheduler (0 workers)...\n");
    InlineScheduler sched;
    for (int i = 0; i < NUM_TASKS; ++i) {
      sched.Submit(task);
    }
  }

  auto end = std::chrono::steady_clock::now();
  auto ms = std::chrono::duration_cast<std::chrono::milliseconds>(end - start).count();

  printf("Counter: %d, Time: %ldms\n", counter.load(), ms);
  return 0;
}
