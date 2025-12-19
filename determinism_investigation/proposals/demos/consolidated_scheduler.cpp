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

/**
 * Demo 4: Consolidated Scheduler Comparison
 *
 * Purpose: Compare event counts between:
 * 1. Multiple independent schedulers (current Memgraph approach)
 * 2. Single consolidated scheduler with priority queue
 *
 * Scenario: 3 tasks with intervals 100ms, 200ms, 500ms over 1 second
 *
 * With independent schedulers:
 *   - Task A (100ms): 10 wake-ups
 *   - Task B (200ms): 5 wake-ups
 *   - Task C (500ms): 2 wake-ups
 *   - Total: 17 wake-ups, 17 clock checks, 17 futex calls
 *
 * With consolidated scheduler:
 *   - Single thread checks clock once per dispatch
 *   - Wake-ups: 10 (at t=100,200,300,400,500,600,700,800,900,1000)
 *   - But Task B runs at t=200,400,600,800,1000 (5 times)
 *   - And Task C runs at t=500,1000 (2 times)
 *   - Total: 10 wake-ups (some dispatch multiple tasks)
 *
 * Usage:
 *   ./consolidated_scheduler independent  # 3 separate threads
 *   ./consolidated_scheduler consolidated # 1 thread with queue
 *   ./consolidated_scheduler both         # Run both for comparison
 */

#include <atomic>
#include <chrono>
#include <condition_variable>
#include <cstring>
#include <functional>
#include <iostream>
#include <mutex>
#include <queue>
#include <thread>
#include <vector>

using namespace std::chrono;
using Clock = steady_clock;

constexpr auto TEST_DURATION = milliseconds(1000);

// Task execution counter
std::atomic<int> task_a_count{0};
std::atomic<int> task_b_count{0};
std::atomic<int> task_c_count{0};
std::atomic<int> clock_calls{0};

void reset_counters() {
  task_a_count = 0;
  task_b_count = 0;
  task_c_count = 0;
  clock_calls = 0;
}

//=============================================================================
// Approach 1: Independent Schedulers (Current Memgraph-like)
//=============================================================================

class IndependentScheduler {
 public:
  void SetInterval(milliseconds interval) { interval_ = interval; }

  void Run(const char *name, std::function<void()> task) {
    name_ = name;
    task_ = std::move(task);
    thread_ = std::thread([this] { ThreadRun(); });
  }

  void Stop() {
    {
      std::lock_guard<std::mutex> lock(mtx_);
      stop_ = true;
    }
    cv_.notify_one();
    if (thread_.joinable()) thread_.join();
  }

 private:
  void ThreadRun() {
    while (true) {
      std::unique_lock<std::mutex> lock(mtx_);

      // Clock call #1: get current time
      auto now = Clock::now();
      ++clock_calls;

      auto next_run = now + interval_;

      // This causes futex wait
      cv_.wait_until(lock, next_run, [this] { return stop_; });

      if (stop_) break;

      // Execute task
      task_();
    }
  }

  std::string name_;
  std::function<void()> task_;
  milliseconds interval_{100};
  std::thread thread_;
  std::mutex mtx_;
  std::condition_variable cv_;
  bool stop_{false};
};

void test_independent_schedulers() {
  std::cout << "\n=== Independent Schedulers (3 threads) ===" << std::endl;
  reset_counters();

  IndependentScheduler sched_a, sched_b, sched_c;

  sched_a.SetInterval(milliseconds(100));
  sched_b.SetInterval(milliseconds(200));
  sched_c.SetInterval(milliseconds(500));

  auto start = Clock::now();

  sched_a.Run("TaskA", [] { ++task_a_count; });
  sched_b.Run("TaskB", [] { ++task_b_count; });
  sched_c.Run("TaskC", [] { ++task_c_count; });

  std::this_thread::sleep_for(TEST_DURATION);

  sched_a.Stop();
  sched_b.Stop();
  sched_c.Stop();

  auto elapsed = duration_cast<milliseconds>(Clock::now() - start);

  std::cout << "  Duration: " << elapsed.count() << "ms" << std::endl;
  std::cout << "  Task A (100ms) executions: " << task_a_count << std::endl;
  std::cout << "  Task B (200ms) executions: " << task_b_count << std::endl;
  std::cout << "  Task C (500ms) executions: " << task_c_count << std::endl;
  std::cout << "  Total clock_calls: " << clock_calls << std::endl;
  std::cout << "  Threads used: 3" << std::endl;
}

//=============================================================================
// Approach 2: Consolidated Scheduler (Single Thread, Priority Queue)
//=============================================================================

class ConsolidatedScheduler {
 public:
  struct ScheduledTask {
    Clock::time_point next_run;
    milliseconds interval;
    std::function<void()> task;
    std::string name;

    // Priority queue orders by next_run (soonest first)
    bool operator>(const ScheduledTask &other) const { return next_run > other.next_run; }
  };

  void AddTask(const char *name, milliseconds interval, std::function<void()> task) {
    std::lock_guard<std::mutex> lock(mtx_);

    // Clock call to get initial next_run
    auto now = Clock::now();
    ++clock_calls;

    tasks_.push({now + interval, interval, std::move(task), name});
    cv_.notify_one();
  }

  void Run() {
    thread_ = std::thread([this] { ThreadRun(); });
  }

  void Stop() {
    {
      std::lock_guard<std::mutex> lock(mtx_);
      stop_ = true;
    }
    cv_.notify_one();
    if (thread_.joinable()) thread_.join();
  }

 private:
  void ThreadRun() {
    while (true) {
      std::unique_lock<std::mutex> lock(mtx_);

      if (tasks_.empty()) {
        cv_.wait(lock, [this] { return stop_ || !tasks_.empty(); });
        if (stop_) break;
        continue;
      }

      // Get the soonest task
      auto next_task = tasks_.top();

      // Single clock call to determine wait time
      auto now = Clock::now();
      ++clock_calls;

      if (now >= next_task.next_run) {
        // Task is due - execute it
        tasks_.pop();

        // Re-schedule the task
        next_task.next_run = now + next_task.interval;
        tasks_.push(next_task);

        // Execute without lock
        lock.unlock();
        next_task.task();
        lock.lock();
      } else {
        // Wait until next task is due
        cv_.wait_until(lock, next_task.next_run, [this] { return stop_; });
        if (stop_) break;
      }
    }
  }

  std::priority_queue<ScheduledTask, std::vector<ScheduledTask>, std::greater<>> tasks_;
  std::thread thread_;
  std::mutex mtx_;
  std::condition_variable cv_;
  bool stop_{false};
};

void test_consolidated_scheduler() {
  std::cout << "\n=== Consolidated Scheduler (1 thread) ===" << std::endl;
  reset_counters();

  ConsolidatedScheduler scheduler;

  scheduler.AddTask("TaskA", milliseconds(100), [] { ++task_a_count; });
  scheduler.AddTask("TaskB", milliseconds(200), [] { ++task_b_count; });
  scheduler.AddTask("TaskC", milliseconds(500), [] { ++task_c_count; });

  auto start = Clock::now();

  scheduler.Run();

  std::this_thread::sleep_for(TEST_DURATION);

  scheduler.Stop();

  auto elapsed = duration_cast<milliseconds>(Clock::now() - start);

  std::cout << "  Duration: " << elapsed.count() << "ms" << std::endl;
  std::cout << "  Task A (100ms) executions: " << task_a_count << std::endl;
  std::cout << "  Task B (200ms) executions: " << task_b_count << std::endl;
  std::cout << "  Task C (500ms) executions: " << task_c_count << std::endl;
  std::cout << "  Total clock_calls: " << clock_calls << std::endl;
  std::cout << "  Threads used: 1" << std::endl;
}

//=============================================================================
// Main
//=============================================================================

int main(int argc, char *argv[]) {
  const char *mode = (argc > 1) ? argv[1] : "both";

  std::cout << "Consolidated Scheduler Comparison" << std::endl;
  std::cout << "==================================" << std::endl;
  std::cout << "Test duration: " << TEST_DURATION.count() << "ms" << std::endl;
  std::cout << "Tasks: A(100ms), B(200ms), C(500ms)" << std::endl;

  if (strcmp(mode, "independent") == 0) {
    test_independent_schedulers();
  } else if (strcmp(mode, "consolidated") == 0) {
    test_consolidated_scheduler();
  } else {
    test_independent_schedulers();
    test_consolidated_scheduler();
  }

  std::cout << "\n=== Analysis ===" << std::endl;
  std::cout << "Independent: 3 threads, each polling independently" << std::endl;
  std::cout << "  - Each thread calls clock on every wake-up" << std::endl;
  std::cout << "  - Total wake-ups = sum of all task frequencies" << std::endl;
  std::cout << "\nConsolidated: 1 thread, priority queue" << std::endl;
  std::cout << "  - Single thread wakes only when soonest task is due" << std::endl;
  std::cout << "  - Clock calls = number of dispatches (not tasks)" << std::endl;
  std::cout << "  - Fewer threads = fewer context switches" << std::endl;

  return 0;
}
