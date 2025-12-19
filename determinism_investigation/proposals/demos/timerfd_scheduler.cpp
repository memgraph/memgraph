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
 * Demo 5: Linux timerfd Scheduler
 *
 * Purpose: Use Linux timerfd for kernel-driven wake-ups instead of polling.
 *
 * How it works:
 * - timerfd_create() creates a timer file descriptor
 * - timerfd_settime() sets when the timer fires
 * - read() blocks until the timer fires (kernel wakes us)
 * - No clock polling needed!
 *
 * Benefits:
 * - Kernel manages timing - no userspace clock calls
 * - Precise wake-ups at exact times
 * - Lower CPU usage (no busy-waiting or polling)
 * - Fewer non-determinism events (no clock_gettime spam)
 *
 * Trade-offs:
 * - Linux-specific (not portable)
 * - Slightly more complex API
 * - Each timer is one file descriptor
 *
 * For consolidated scheduler with timerfd:
 * - Single timerfd for next scheduled task
 * - When task completes, recalculate and reset timer
 */

#include <sys/timerfd.h>
#include <unistd.h>
#include <atomic>
#include <chrono>
#include <cstdint>
#include <cstring>
#include <functional>
#include <iostream>
#include <queue>
#include <thread>
#include <vector>

using namespace std::chrono;
using Clock = steady_clock;

constexpr auto TEST_DURATION = milliseconds(1000);

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
// Helper: Convert duration to timespec
//=============================================================================

struct timespec duration_to_timespec(nanoseconds ns) {
  struct timespec ts;
  ts.tv_sec = duration_cast<seconds>(ns).count();
  ts.tv_nsec = (ns % seconds(1)).count();
  return ts;
}

//=============================================================================
// Approach 3: Single timerfd with priority queue (optimal)
//=============================================================================

class TimerfdScheduler {
 public:
  struct ScheduledTask {
    Clock::time_point next_run;
    milliseconds interval;
    std::function<void()> task;
    std::string name;

    bool operator>(const ScheduledTask &other) const { return next_run > other.next_run; }
  };

  TimerfdScheduler() {
    // Create timer using CLOCK_MONOTONIC (like steady_clock)
    tfd_ = timerfd_create(CLOCK_MONOTONIC, 0);
    if (tfd_ < 0) {
      perror("timerfd_create");
      std::abort();
    }
  }

  ~TimerfdScheduler() {
    if (tfd_ >= 0) close(tfd_);
  }

  void AddTask(const char *name, milliseconds interval, std::function<void()> task) {
    // Only clock call: get current time once when adding task
    auto now = Clock::now();
    ++clock_calls;

    tasks_.push({now + interval, interval, std::move(task), name});
  }

  void Run() {
    thread_ = std::thread([this] { ThreadRun(); });
  }

  void Stop() {
    stop_ = true;
    // Wake up the timer by setting it to fire immediately
    struct itimerspec its = {};
    its.it_value.tv_nsec = 1;  // Fire in 1 nanosecond
    timerfd_settime(tfd_, 0, &its, nullptr);
    if (thread_.joinable()) thread_.join();
  }

 private:
  void ThreadRun() {
    while (!stop_) {
      if (tasks_.empty()) break;

      // Get soonest task
      auto next_task = tasks_.top();
      tasks_.pop();

      // Calculate time until next task (only clock call per dispatch cycle)
      auto now = Clock::now();
      ++clock_calls;

      auto wait_duration = next_task.next_run - now;
      if (wait_duration > nanoseconds(0)) {
        // Set timer to fire when task is due
        struct itimerspec its = {};
        its.it_value = duration_to_timespec(wait_duration);

        if (timerfd_settime(tfd_, 0, &its, nullptr) < 0) {
          perror("timerfd_settime");
          break;
        }

        // Block until timer fires (kernel wakes us - no polling!)
        uint64_t expirations;
        ssize_t s = read(tfd_, &expirations, sizeof(expirations));
        if (s != sizeof(expirations)) {
          if (stop_) break;
          perror("read timerfd");
          break;
        }
      }

      if (stop_) break;

      // Execute task
      next_task.task();

      // Re-schedule
      // Need current time for next_run calculation
      now = Clock::now();
      ++clock_calls;
      next_task.next_run = now + next_task.interval;
      tasks_.push(next_task);
    }
  }

  int tfd_{-1};
  std::priority_queue<ScheduledTask, std::vector<ScheduledTask>, std::greater<>> tasks_;
  std::thread thread_;
  std::atomic<bool> stop_{false};
};

void test_timerfd_scheduler() {
  std::cout << "\n=== Timerfd Scheduler (1 thread, kernel-driven) ===" << std::endl;
  reset_counters();

  TimerfdScheduler scheduler;

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
// For comparison: Simple timerfd per task (like epoll-based scheduler)
//=============================================================================

class SimpleTimerfdTask {
 public:
  SimpleTimerfdTask(const char *name, milliseconds interval, std::function<void()> task)
      : name_(name), interval_(interval), task_(std::move(task)) {
    tfd_ = timerfd_create(CLOCK_MONOTONIC, 0);
    if (tfd_ < 0) {
      perror("timerfd_create");
      std::abort();
    }
  }

  ~SimpleTimerfdTask() {
    Stop();
    if (tfd_ >= 0) close(tfd_);
  }

  void Run() {
    // Set periodic timer
    struct itimerspec its = {};
    its.it_value = duration_to_timespec(interval_);
    its.it_interval = duration_to_timespec(interval_);

    if (timerfd_settime(tfd_, 0, &its, nullptr) < 0) {
      perror("timerfd_settime");
      return;
    }

    thread_ = std::thread([this] {
      while (!stop_) {
        uint64_t expirations;
        ssize_t s = read(tfd_, &expirations, sizeof(expirations));
        if (s != sizeof(expirations)) {
          if (stop_) break;
          break;
        }
        if (!stop_) task_();
      }
    });
  }

  void Stop() {
    stop_ = true;
    // Disarm timer
    struct itimerspec its = {};
    timerfd_settime(tfd_, 0, &its, nullptr);
    // Wake up blocked read by closing fd temporarily - actually let's just write to a pipe
    // For simplicity, we'll just wait
    if (thread_.joinable()) {
      // Force thread to wake by setting immediate timer
      its.it_value.tv_nsec = 1;
      timerfd_settime(tfd_, 0, &its, nullptr);
      thread_.join();
    }
  }

 private:
  std::string name_;
  milliseconds interval_;
  std::function<void()> task_;
  int tfd_{-1};
  std::thread thread_;
  std::atomic<bool> stop_{false};
};

void test_simple_timerfd() {
  std::cout << "\n=== Simple Timerfd (3 threads, 3 timerfds) ===" << std::endl;
  reset_counters();

  SimpleTimerfdTask task_a("TaskA", milliseconds(100), [] { ++task_a_count; });
  SimpleTimerfdTask task_b("TaskB", milliseconds(200), [] { ++task_b_count; });
  SimpleTimerfdTask task_c("TaskC", milliseconds(500), [] { ++task_c_count; });

  auto start = Clock::now();

  task_a.Run();
  task_b.Run();
  task_c.Run();

  std::this_thread::sleep_for(TEST_DURATION);

  task_a.Stop();
  task_b.Stop();
  task_c.Stop();

  auto elapsed = duration_cast<milliseconds>(Clock::now() - start);

  std::cout << "  Duration: " << elapsed.count() << "ms" << std::endl;
  std::cout << "  Task A (100ms) executions: " << task_a_count << std::endl;
  std::cout << "  Task B (200ms) executions: " << task_b_count << std::endl;
  std::cout << "  Task C (500ms) executions: " << task_c_count << std::endl;
  std::cout << "  Total clock_calls: " << clock_calls << " (no polling!)" << std::endl;
  std::cout << "  Threads used: 3" << std::endl;
}

//=============================================================================
// Main
//=============================================================================

int main(int argc, char *argv[]) {
  const char *mode = (argc > 1) ? argv[1] : "both";

  std::cout << "Timerfd Scheduler Demo" << std::endl;
  std::cout << "======================" << std::endl;
  std::cout << "Test duration: " << TEST_DURATION.count() << "ms" << std::endl;
  std::cout << "Tasks: A(100ms), B(200ms), C(500ms)" << std::endl;

  if (strcmp(mode, "consolidated") == 0) {
    test_timerfd_scheduler();
  } else if (strcmp(mode, "simple") == 0) {
    test_simple_timerfd();
  } else {
    test_timerfd_scheduler();
    test_simple_timerfd();
  }

  std::cout << "\n=== Key Insight ===" << std::endl;
  std::cout << "timerfd eliminates clock polling:" << std::endl;
  std::cout << "  - Kernel wakes thread at exact time" << std::endl;
  std::cout << "  - No condition_variable::wait_until polling" << std::endl;
  std::cout << "  - No futex calls for timed waits" << std::endl;
  std::cout << "  - Only read() syscall when timer fires" << std::endl;

  return 0;
}
