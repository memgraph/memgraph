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

// io_uring-based task scheduler demo
// Compare: native timeouts vs timerfd+poll vs eventfd worker dispatch vs epoll (ConsolidatedScheduler)
//
// Build:
//   g++ -std=c++20 -O2 -o io_uring_scheduler_demo io_uring_scheduler_demo.cpp -luring -pthread
//
// Run:
//   ./io_uring_scheduler_demo native      # Native io_uring timeouts
//   ./io_uring_scheduler_demo timerfd     # timerfd + io_uring poll
//   ./io_uring_scheduler_demo eventfd     # timerfd + eventfd worker dispatch
//   ./io_uring_scheduler_demo epoll       # timerfd + epoll (like ConsolidatedScheduler)
//
// Measure non-determinism:
//   strace -c ./io_uring_scheduler_demo <mode>
//   ~/Downloads/Undo-Suite-Corporate-Multiarch-9.1.0/live-record -o /tmp/uring.undo ./io_uring_scheduler_demo <mode>

#include <liburing.h>
#include <poll.h>
#include <sys/epoll.h>
#include <sys/eventfd.h>
#include <sys/timerfd.h>
#include <unistd.h>

#include <atomic>
#include <chrono>
#include <cstdio>
#include <cstring>
#include <functional>
#include <map>
#include <mutex>
#include <queue>
#include <thread>
#include <vector>

using namespace std::chrono_literals;
using Clock = std::chrono::steady_clock;
using TimePoint = Clock::time_point;
using Duration = Clock::duration;

// ============================================================================
// Task definition
// ============================================================================
struct Task {
  uint64_t id;
  TimePoint deadline;
  std::function<void()> work;

  bool operator>(const Task &other) const {
    if (deadline != other.deadline) return deadline > other.deadline;
    return id > other.id;  // Stable ordering by insertion order
  }
};

// ============================================================================
// Approach 1: Native io_uring TIMEOUT
// ============================================================================
class NativeTimeoutScheduler {
 public:
  static constexpr uint64_t TIMER_TAG = 1ULL << 63;

  NativeTimeoutScheduler() { io_uring_queue_init(256, &ring_, 0); }

  ~NativeTimeoutScheduler() { io_uring_queue_exit(&ring_); }

  void schedule(Duration delay, std::function<void()> fn) {
    uint64_t id = next_id_++;

    // Convert to kernel timespec
    auto ns = std::chrono::duration_cast<std::chrono::nanoseconds>(delay);
    __kernel_timespec ts;
    ts.tv_sec = ns.count() / 1'000'000'000;
    ts.tv_nsec = ns.count() % 1'000'000'000;

    // Store task
    {
      std::lock_guard lock(mutex_);
      tasks_[id] = Task{id, Clock::now() + delay, std::move(fn)};
    }

    // Submit timeout to io_uring
    io_uring_sqe *sqe = io_uring_get_sqe(&ring_);
    io_uring_prep_timeout(sqe, &ts, 0, 0);
    io_uring_sqe_set_data64(sqe, TIMER_TAG | id);
  }

  void run(int num_tasks) {
    int completed = 0;

    while (completed < num_tasks) {
      io_uring_submit(&ring_);

      io_uring_cqe *cqe;
      int ret = io_uring_wait_cqe(&ring_, &cqe);
      if (ret < 0) break;

      uint64_t data = io_uring_cqe_get_data64(cqe);

      if (data & TIMER_TAG) {
        uint64_t timer_id = data & ~TIMER_TAG;

        Task task;
        {
          std::lock_guard lock(mutex_);
          auto it = tasks_.find(timer_id);
          if (it != tasks_.end()) {
            task = std::move(it->second);
            tasks_.erase(it);
          }
        }

        if (task.work) {
          task.work();
          completed++;
        }
      }

      io_uring_cqe_seen(&ring_, cqe);
    }
  }

 private:
  io_uring ring_;
  std::atomic<uint64_t> next_id_{0};
  std::mutex mutex_;
  std::map<uint64_t, Task> tasks_;
};

// ============================================================================
// Approach 2: timerfd + io_uring poll + timer wheel
// ============================================================================
class TimerfdScheduler {
 public:
  static constexpr uint64_t TIMERFD_TAG = 1;

  TimerfdScheduler() {
    io_uring_queue_init(256, &ring_, 0);
    timerfd_ = timerfd_create(CLOCK_MONOTONIC, TFD_NONBLOCK | TFD_CLOEXEC);
  }

  ~TimerfdScheduler() {
    close(timerfd_);
    io_uring_queue_exit(&ring_);
  }

  void schedule(Duration delay, std::function<void()> fn) {
    uint64_t id = next_id_++;
    auto deadline = Clock::now() + delay;

    std::lock_guard lock(mutex_);
    // Insert with (deadline, id) key for deterministic ordering
    wheel_.emplace(std::make_pair(deadline, id), Task{id, deadline, std::move(fn)});

    // Update timerfd if this is the new earliest deadline
    if (wheel_.begin()->first.first == deadline) {
      arm_timerfd(delay);
    }
  }

  void run(int num_tasks) {
    int completed = 0;

    // Initial poll registration
    submit_timerfd_poll();

    while (completed < num_tasks) {
      io_uring_submit(&ring_);

      io_uring_cqe *cqe;
      int ret = io_uring_wait_cqe(&ring_, &cqe);
      if (ret < 0) break;

      uint64_t data = io_uring_cqe_get_data64(cqe);

      if (data == TIMERFD_TAG) {
        // Drain timerfd
        uint64_t expirations;
        read(timerfd_, &expirations, sizeof(expirations));

        // Collect due tasks in deterministic order
        auto now = Clock::now();
        std::vector<Task> due_tasks;

        {
          std::lock_guard lock(mutex_);
          auto it = wheel_.begin();
          while (it != wheel_.end() && it->first.first <= now) {
            due_tasks.push_back(std::move(it->second));
            it = wheel_.erase(it);
          }

          // Rearm for next deadline
          if (!wheel_.empty()) {
            auto next_deadline = wheel_.begin()->first.first;
            auto delay = next_deadline - now;
            if (delay > Duration::zero()) {
              arm_timerfd(delay);
            }
          }
        }

        // Execute in deterministic order
        for (auto &task : due_tasks) {
          task.work();
          completed++;
        }

        // Re-register poll
        submit_timerfd_poll();
      }

      io_uring_cqe_seen(&ring_, cqe);
    }
  }

 private:
  void arm_timerfd(Duration delay) {
    auto ns = std::chrono::duration_cast<std::chrono::nanoseconds>(delay);
    itimerspec spec{};
    spec.it_value.tv_sec = ns.count() / 1'000'000'000;
    spec.it_value.tv_nsec = ns.count() % 1'000'000'000;
    timerfd_settime(timerfd_, 0, &spec, nullptr);
  }

  void submit_timerfd_poll() {
    io_uring_sqe *sqe = io_uring_get_sqe(&ring_);
    io_uring_prep_poll_add(sqe, timerfd_, POLLIN);
    io_uring_sqe_set_data64(sqe, TIMERFD_TAG);
  }

  io_uring ring_;
  int timerfd_;
  std::atomic<uint64_t> next_id_{0};
  std::mutex mutex_;
  // Sorted by (deadline, insertion_id) for deterministic ordering
  std::map<std::pair<TimePoint, uint64_t>, Task> wheel_;
};

// ============================================================================
// Approach 3: timerfd + eventfd worker dispatch (like ConsolidatedScheduler)
// ============================================================================
class EventfdWorkerScheduler {
 public:
  static constexpr uint64_t TIMERFD_TAG = 1;
  static constexpr uint64_t COMPLETION_TAG = 2;
  static constexpr size_t NUM_WORKERS = 4;

  EventfdWorkerScheduler() {
    io_uring_queue_init(256, &ring_, 0);
    timerfd_ = timerfd_create(CLOCK_MONOTONIC, TFD_NONBLOCK | TFD_CLOEXEC);
    work_eventfd_ = eventfd(0, EFD_SEMAPHORE);
    completion_eventfd_ = eventfd(0, EFD_NONBLOCK);  // Workers signal completion

    // Start workers
    for (size_t i = 0; i < NUM_WORKERS; ++i) {
      workers_.emplace_back([this] { worker_loop(); });
    }
  }

  ~EventfdWorkerScheduler() {
    stopped_ = true;
    // Wake all workers
    uint64_t val = NUM_WORKERS;
    write(work_eventfd_, &val, sizeof(val));

    for (auto &w : workers_) {
      w.join();
    }

    close(timerfd_);
    close(work_eventfd_);
    close(completion_eventfd_);
    io_uring_queue_exit(&ring_);
  }

  void schedule(Duration delay, std::function<void()> fn) {
    uint64_t id = next_id_++;
    auto deadline = Clock::now() + delay;

    std::lock_guard lock(mutex_);
    wheel_.emplace(std::make_pair(deadline, id), Task{id, deadline, std::move(fn)});

    if (wheel_.begin()->first.first == deadline) {
      arm_timerfd(delay);
    }
  }

  void run(int num_tasks) {
    submit_timerfd_poll();
    submit_completion_poll();

    while (completed_.load() < num_tasks) {
      io_uring_submit(&ring_);

      io_uring_cqe *cqe;
      int ret = io_uring_wait_cqe(&ring_, &cqe);
      if (ret < 0) break;

      uint64_t data = io_uring_cqe_get_data64(cqe);

      if (data == TIMERFD_TAG) {
        uint64_t expirations;
        read(timerfd_, &expirations, sizeof(expirations));

        auto now = Clock::now();
        std::vector<Task> due_tasks;

        // First: collect due tasks under wheel lock only
        {
          std::lock_guard lock(mutex_);
          auto it = wheel_.begin();
          while (it != wheel_.end() && it->first.first <= now) {
            due_tasks.push_back(std::move(it->second));
            it = wheel_.erase(it);
          }

          if (!wheel_.empty()) {
            auto next_deadline = wheel_.begin()->first.first;
            auto delay = next_deadline - now;
            if (delay > Duration::zero()) {
              arm_timerfd(delay);
            }
          }
        }

        // Second: push to work queue under work_mutex only
        if (!due_tasks.empty()) {
          {
            std::lock_guard wlock(work_mutex_);
            for (auto &task : due_tasks) {
              work_queue_.push(std::move(task));
            }
          }

          // Signal workers via eventfd
          uint64_t val = due_tasks.size();
          write(work_eventfd_, &val, sizeof(val));
        }

        submit_timerfd_poll();
      } else if (data == COMPLETION_TAG) {
        // Drain completion eventfd
        uint64_t val;
        read(completion_eventfd_, &val, sizeof(val));
        // Re-register poll for more completions
        submit_completion_poll();
      }

      io_uring_cqe_seen(&ring_, cqe);
    }
  }

 private:
  void arm_timerfd(Duration delay) {
    auto ns = std::chrono::duration_cast<std::chrono::nanoseconds>(delay);
    itimerspec spec{};
    spec.it_value.tv_sec = ns.count() / 1'000'000'000;
    spec.it_value.tv_nsec = ns.count() % 1'000'000'000;
    timerfd_settime(timerfd_, 0, &spec, nullptr);
  }

  void submit_timerfd_poll() {
    io_uring_sqe *sqe = io_uring_get_sqe(&ring_);
    io_uring_prep_poll_add(sqe, timerfd_, POLLIN);
    io_uring_sqe_set_data64(sqe, TIMERFD_TAG);
  }

  void submit_completion_poll() {
    io_uring_sqe *sqe = io_uring_get_sqe(&ring_);
    io_uring_prep_poll_add(sqe, completion_eventfd_, POLLIN);
    io_uring_sqe_set_data64(sqe, COMPLETION_TAG);
  }

  void worker_loop() {
    while (!stopped_) {
      uint64_t val;
      if (read(work_eventfd_, &val, sizeof(val)) <= 0) continue;
      if (stopped_) break;

      Task task;
      {
        std::lock_guard lock(work_mutex_);
        if (!work_queue_.empty()) {
          task = std::move(const_cast<Task &>(work_queue_.top()));
          work_queue_.pop();
        }
      }

      if (task.work) {
        task.work();
        completed_++;
        // Signal dispatcher that a task completed
        uint64_t one = 1;
        write(completion_eventfd_, &one, sizeof(one));
      }
    }
  }

  io_uring ring_;
  int timerfd_;
  int work_eventfd_;
  int completion_eventfd_;
  std::atomic<uint64_t> next_id_{0};
  std::atomic<bool> stopped_{false};
  std::atomic<int> completed_{0};

  std::mutex mutex_;
  std::map<std::pair<TimePoint, uint64_t>, Task> wheel_;

  std::mutex work_mutex_;
  std::priority_queue<Task, std::vector<Task>, std::greater<Task>> work_queue_;
  std::vector<std::thread> workers_;
};

// ============================================================================
// Approach 4: timerfd + epoll (like ConsolidatedScheduler)
// ============================================================================
class EpollScheduler {
 public:
  EpollScheduler() {
    timerfd_ = timerfd_create(CLOCK_MONOTONIC, TFD_NONBLOCK | TFD_CLOEXEC);
    epoll_fd_ = epoll_create1(EPOLL_CLOEXEC);

    struct epoll_event ev {};
    ev.events = EPOLLIN;
    ev.data.fd = timerfd_;
    epoll_ctl(epoll_fd_, EPOLL_CTL_ADD, timerfd_, &ev);
  }

  ~EpollScheduler() {
    close(timerfd_);
    close(epoll_fd_);
  }

  void schedule(Duration delay, std::function<void()> fn) {
    uint64_t id = next_id_++;
    auto deadline = Clock::now() + delay;

    std::lock_guard lock(mutex_);
    wheel_.emplace(std::make_pair(deadline, id), Task{id, deadline, std::move(fn)});

    if (wheel_.begin()->first.first == deadline) {
      arm_timerfd(delay);
    }
  }

  void run(int num_tasks) {
    int completed = 0;

    while (completed < num_tasks) {
      struct epoll_event events[1];
      int nfds = epoll_wait(epoll_fd_, events, 1, -1);
      if (nfds < 0) break;

      if (nfds > 0 && events[0].data.fd == timerfd_) {
        // Drain timerfd
        uint64_t expirations;
        read(timerfd_, &expirations, sizeof(expirations));

        // Collect due tasks in deterministic order
        auto now = Clock::now();
        std::vector<Task> due_tasks;

        {
          std::lock_guard lock(mutex_);
          auto it = wheel_.begin();
          while (it != wheel_.end() && it->first.first <= now) {
            due_tasks.push_back(std::move(it->second));
            it = wheel_.erase(it);
          }

          // Rearm for next deadline
          if (!wheel_.empty()) {
            auto next_deadline = wheel_.begin()->first.first;
            auto delay = next_deadline - now;
            if (delay > Duration::zero()) {
              arm_timerfd(delay);
            }
          }
        }

        // Execute in deterministic order
        for (auto &task : due_tasks) {
          task.work();
          completed++;
        }
      }
    }
  }

 private:
  void arm_timerfd(Duration delay) {
    auto ns = std::chrono::duration_cast<std::chrono::nanoseconds>(delay);
    itimerspec spec{};
    spec.it_value.tv_sec = ns.count() / 1'000'000'000;
    spec.it_value.tv_nsec = ns.count() % 1'000'000'000;
    timerfd_settime(timerfd_, 0, &spec, nullptr);
  }

  int timerfd_;
  int epoll_fd_;
  std::atomic<uint64_t> next_id_{0};
  std::mutex mutex_;
  std::map<std::pair<TimePoint, uint64_t>, Task> wheel_;
};

// ============================================================================
// Test harness
// ============================================================================
int main(int argc, char **argv) {
  const int NUM_TASKS = 100;
  std::string mode = argc > 1 ? argv[1] : "native";

  std::atomic<int> counter{0};
  std::vector<int> execution_order;
  std::mutex order_mutex;

  auto make_task = [&](int task_id) {
    return [&counter, &execution_order, &order_mutex, task_id]() {
      counter++;
      std::lock_guard lock(order_mutex);
      execution_order.push_back(task_id);
    };
  };

  auto start = Clock::now();

  if (mode == "native") {
    printf("Testing native io_uring timeout scheduler...\n");
    NativeTimeoutScheduler sched;

    for (int i = 0; i < NUM_TASKS; ++i) {
      // All tasks due at ~50ms with slight variations
      sched.schedule(50ms + std::chrono::microseconds(i * 10), make_task(i));
    }
    sched.run(NUM_TASKS);

  } else if (mode == "timerfd") {
    printf("Testing timerfd + io_uring poll scheduler...\n");
    TimerfdScheduler sched;

    for (int i = 0; i < NUM_TASKS; ++i) {
      sched.schedule(50ms + std::chrono::microseconds(i * 10), make_task(i));
    }
    sched.run(NUM_TASKS);

  } else if (mode == "eventfd") {
    printf("Testing timerfd + eventfd worker dispatch scheduler...\n");
    EventfdWorkerScheduler sched;

    for (int i = 0; i < NUM_TASKS; ++i) {
      sched.schedule(50ms + std::chrono::microseconds(i * 10), make_task(i));
    }
    sched.run(NUM_TASKS);

  } else if (mode == "epoll") {
    printf("Testing timerfd + epoll scheduler (like ConsolidatedScheduler)...\n");
    EpollScheduler sched;

    for (int i = 0; i < NUM_TASKS; ++i) {
      sched.schedule(50ms + std::chrono::microseconds(i * 10), make_task(i));
    }
    sched.run(NUM_TASKS);

  } else {
    printf("Usage: %s [native|timerfd|eventfd|epoll]\n", argv[0]);
    return 1;
  }

  auto end = Clock::now();
  auto ms = std::chrono::duration_cast<std::chrono::milliseconds>(end - start).count();

  printf("Counter: %d, Time: %ldms\n", counter.load(), ms);

  // Check determinism: print first 10 execution order
  printf("Execution order (first 10): ");
  for (size_t i = 0; i < std::min(size_t(10), execution_order.size()); ++i) {
    printf("%d ", execution_order[i]);
  }
  printf("\n");

  // Check if execution order is deterministic (monotonically increasing)
  bool deterministic = true;
  for (size_t i = 1; i < execution_order.size(); ++i) {
    if (execution_order[i] < execution_order[i - 1]) {
      deterministic = false;
      break;
    }
  }
  printf("Execution order deterministic: %s\n", deterministic ? "YES" : "NO");

  return 0;
}
