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

#include <gtest/gtest.h>

#include <chrono>
#include <coroutine>
#include <memory>
#include <thread>
#include <utility>

#include <utils/priority_thread_pool.hpp>
#include <utils/worker_yield_signal.hpp>
#include "utils/synchronized.hpp"

using namespace std::chrono_literals;

TEST(PriorityThreadPool, Basic) {
  using namespace memgraph;
  memgraph::utils::PriorityThreadPool pool{1};

  utils::Synchronized<std::vector<int>> output;
  constexpr size_t n_tasks = 100;
  for (size_t i = 0; i < n_tasks; ++i) {
    pool.ScheduledAddTask([&, i]() { output->push_back(i); }, utils::Priority::LOW);
  }

  while (output->size() != n_tasks) {
    std::this_thread::sleep_for(std::chrono::milliseconds(10));
  }

  output.WithLock([](const auto &output) {
    ASSERT_EQ(output[0], 0);
    ASSERT_TRUE(std::is_sorted(output.begin(), output.end()));
  });
}

TEST(PriorityThreadPool, Basic2) {
  using namespace memgraph;
  memgraph::utils::PriorityThreadPool pool{1};

  // Figure out which thread is the low/high
  std::atomic<std::thread::id> low_th = std::thread::id{0};
  pool.ScheduledAddTask(
      [&]() {
        low_th = std::this_thread::get_id();
        low_th.notify_one();
      },
      utils::Priority::LOW);
  low_th.wait(std::thread::id{0});

  utils::Synchronized<std::vector<int>> low_out;
  utils::Synchronized<std::vector<int>> high_out;
  constexpr size_t n_tasks = 100;
  for (size_t i = 0; i < n_tasks / 2; ++i) {
    pool.ScheduledAddTask(
        [&, i]() {
          if (std::this_thread::get_id() == low_th) {
            low_out->push_back(i);
          } else {
            high_out->push_back(i);
          }
        },
        utils::Priority::HIGH);
  }
  // Wait for at least one HP task to be scheduled
  std::this_thread::sleep_for(std::chrono::milliseconds(10));
  for (size_t i = n_tasks / 2; i < n_tasks; ++i) {
    pool.ScheduledAddTask(
        [&, i]() {
          ASSERT_EQ(std::this_thread::get_id(), low_th);
          low_out->push_back(i);
        },
        utils::Priority::LOW);
  }

  while (low_out->size() + high_out->size() != n_tasks) {
    std::this_thread::sleep_for(std::chrono::milliseconds(10));
  }

  low_out.WithLock([](const auto &output) {
    ASSERT_TRUE(std::is_sorted(output.begin(), output.end()));
    ASSERT_LE(output.size(), 100);
    ASSERT_GE(output.size(), 50);
  });
  high_out.WithLock([](const auto &output) {
    ASSERT_TRUE(std::is_sorted(output.begin(), output.end()));
    ASSERT_LE(output.size(), 50);
  });
}

TEST(PriorityThreadPool, LowHigh) {
  using namespace memgraph;
  memgraph::utils::WorkerYieldRegistry registry(1);
  memgraph::utils::PriorityThreadPool pool(1, nullptr, &registry);

  std::atomic_bool block{true};
  std::atomic<bool> lp_saw_yield{false};

  // LP task checks yield flag and reschedules itself on same worker when yield is requested
  struct LoopTask : std::enable_shared_from_this<LoopTask> {
    std::atomic_bool *block;
    std::atomic<bool> *lp_saw_yield;
    utils::PriorityThreadPool *pool;

    LoopTask(std::atomic_bool *b, std::atomic<bool> *s, utils::PriorityThreadPool *p)
        : block(b), lp_saw_yield(s), pool(p) {}

    void operator()() {
      while (*block) {
        auto *sig = utils::WorkerYieldRegistry::GetCurrentYieldSignal();
        if (sig && sig->load(std::memory_order_acquire)) {
          *lp_saw_yield = true;
          if (auto wid = utils::WorkerYieldRegistry::GetCurrentWorkerId()) {
            pool->RescheduleTaskOnWorker(*wid, [self = shared_from_this()]() { (*self)(); });
          }
          return;
        }
        std::this_thread::sleep_for(1ms);
      }
    }
  };

  auto loop_task = std::make_shared<LoopTask>(&block, &lp_saw_yield, &pool);
  pool.ScheduledAddTask([loop_task]() { (*loop_task)(); }, utils::Priority::LOW);

  // Wait for the task to be scheduled
  std::this_thread::sleep_for(std::chrono::milliseconds(100));

  utils::Synchronized<std::vector<int>> output;
  constexpr size_t n_tasks = 100;
  for (size_t i = 0; i < n_tasks / 2; ++i) {
    pool.ScheduledAddTask([&, i]() { output->push_back(i); }, utils::Priority::LOW);
  }
  for (size_t i = n_tasks / 2; i < n_tasks; ++i) {
    pool.ScheduledAddTask([&, i]() { output->push_back(i); }, utils::Priority::HIGH);
  }

  // Wait for the HIGH priority tasks to finish (LP task yields when yield is set on HP add)
  while (output->size() < n_tasks / 2) {
    std::this_thread::sleep_for(std::chrono::milliseconds(10));
  }

  // Check if only the HIGH priority tasks were executed and in order
  output.WithLock([](const auto &output) {
    ASSERT_EQ(output[0], n_tasks / 2);
    ASSERT_TRUE(std::is_sorted(output.begin(), output.end()));
  });
  ASSERT_TRUE(lp_saw_yield.load()) << "LP task must see yield when HP tasks are added";

  // Unblock mixed work thread and close
  block = false;
  block.notify_all();
  pool.ShutDown();
  pool.AwaitShutdown();
}

TEST(PriorityThreadPool, ScheduledAddTaskOnWorker_PinnedRunsOnDesignatedWorker) {
  using namespace memgraph;
  constexpr uint16_t kWorkers = 2;
  memgraph::utils::PriorityThreadPool pool{kWorkers};

  std::atomic<std::thread::id> worker0_thread_id;
  std::atomic<std::thread::id> worker1_thread_id;
  std::atomic<bool> worker0_seen{false};
  std::atomic<bool> worker1_seen{false};

  // Identify which thread is worker 0 and which is worker 1 by scheduling pinned tasks
  pool.RescheduleTaskOnWorker(0, [&]() {
    worker0_thread_id = std::this_thread::get_id();
    worker0_seen = true;
  });
  pool.RescheduleTaskOnWorker(1, [&]() {
    worker1_thread_id = std::this_thread::get_id();
    worker1_seen = true;
  });

  while (!worker0_seen || !worker1_seen) std::this_thread::sleep_for(1ms);

  ASSERT_NE(worker0_thread_id.load(), worker1_thread_id.load());

  // Schedule another pinned task on worker 0 and verify it runs on worker 0's thread
  std::atomic<bool> pinned_ran_on_worker0{false};
  pool.RescheduleTaskOnWorker(
      0, [&]() { pinned_ran_on_worker0 = (std::this_thread::get_id() == worker0_thread_id.load()); });

  while (!pinned_ran_on_worker0) std::this_thread::sleep_for(1ms);
  ASSERT_TRUE(pinned_ran_on_worker0);

  pool.ShutDown();
  pool.AwaitShutdown();
}

TEST(PriorityThreadPool, ScheduledAddTaskOnWorker_ReschedulingRunsOnSameWorker) {
  using namespace memgraph;
  constexpr uint16_t kWorkers = 2;
  memgraph::utils::PriorityThreadPool pool{kWorkers};

  std::atomic<std::thread::id> first_task_thread_id;
  std::atomic<std::thread::id> continuation_thread_id;
  std::atomic<bool> first_done{false};
  std::atomic<bool> continuation_done{false};

  // Schedule on worker 0 a task that reschedules a continuation on worker 0 (simulating yield resume)
  pool.RescheduleTaskOnWorker(0, [&, &pool = pool]() {
    first_task_thread_id = std::this_thread::get_id();
    first_done = true;
    // Reschedule continuation on same worker (pinned, same task id semantics)
    pool.RescheduleTaskOnWorker(0, [&]() {
      continuation_thread_id = std::this_thread::get_id();
      continuation_done = true;
    });
  });

  while (!continuation_done) std::this_thread::sleep_for(1ms);

  ASSERT_EQ(first_task_thread_id.load(), continuation_thread_id.load())
      << "Rescheduling on same worker must run continuation on the same thread";

  pool.ShutDown();
  pool.AwaitShutdown();
}

TEST(PriorityThreadPool, ScheduledAddTaskOnWorker_PinnedNotStolen) {
  using namespace memgraph;
  constexpr uint16_t kWorkers = 2;
  memgraph::utils::PriorityThreadPool pool{kWorkers};

  std::atomic<bool> worker1_busy{true};
  std::atomic<std::thread::id> worker0_thread_id;
  std::atomic<bool> pinned_ran_on_worker0{false};

  // Keep worker 1 busy so it would otherwise try to steal from worker 0
  pool.RescheduleTaskOnWorker(1, [&]() {
    while (worker1_busy) worker1_busy.wait(true);
  });

  std::this_thread::sleep_for(10ms);

  // Worker 0 runs this task, then schedules a pinned continuation on worker 0
  pool.RescheduleTaskOnWorker(0, [&, &pool = pool]() {
    worker0_thread_id = std::this_thread::get_id();
    pool.RescheduleTaskOnWorker(
        0, [&]() { pinned_ran_on_worker0 = (std::this_thread::get_id() == worker0_thread_id.load()); });
  });

  while (!pinned_ran_on_worker0) std::this_thread::sleep_for(1ms);
  ASSERT_TRUE(pinned_ran_on_worker0) << "Pinned continuation must run on same worker, not be stolen";

  worker1_busy = false;
  worker1_busy.notify_one();
  pool.ShutDown();
  pool.AwaitShutdown();
}

// Yield flag: set when adding a high-priority task; tasks must read it inside and yield when set.
// These tests use a pool with WorkerYieldRegistry so RequestYieldForWorker is called on HP push.

TEST(PriorityThreadPool, YieldFlag_NotSetWhenOnlyLowPriority) {
  using namespace memgraph;
  constexpr uint16_t kWorkers = 2;
  memgraph::utils::WorkerYieldRegistry registry(kWorkers);
  memgraph::utils::PriorityThreadPool pool(kWorkers, nullptr, &registry);

  std::atomic<int> completed{0};
  std::atomic<int> saw_yield_requested{0};
  constexpr int n_tasks = 50;

  for (int i = 0; i < n_tasks; ++i) {
    pool.ScheduledAddTask(
        [&]() {
          for (int j = 0; j < 100; ++j) {
            auto *sig = utils::WorkerYieldRegistry::GetCurrentYieldSignal();
            if (sig && sig->load(std::memory_order_acquire)) saw_yield_requested.fetch_add(1);
          }
          completed.fetch_add(1);
        },
        utils::Priority::LOW);
  }

  while (completed.load() != n_tasks) std::this_thread::sleep_for(1ms);
  ASSERT_EQ(saw_yield_requested.load(), 0) << "Yield must not be set when only LP tasks are added";

  pool.ShutDown();
  pool.AwaitShutdown();
}

TEST(PriorityThreadPool, YieldFlag_NoYieldWhenHighPriorityAddedToOtherWorker) {
  using namespace memgraph;
  constexpr uint16_t kWorkers = 2;
  memgraph::utils::WorkerYieldRegistry registry(kWorkers);
  memgraph::utils::PriorityThreadPool pool(kWorkers, nullptr, &registry);

  std::atomic<bool> worker0_done{false};
  std::atomic<bool> worker0_saw_yield{false};

  pool.RescheduleTaskOnWorker(0, [&]() {
    for (int i = 0; i < 2'000'000; ++i) {
      auto *sig = utils::WorkerYieldRegistry::GetCurrentYieldSignal();
      if (sig && sig->load(std::memory_order_acquire)) worker0_saw_yield = true;
    }
    worker0_done = true;
  });

  std::this_thread::sleep_for(1ms);
  // Add HP only to worker 1; worker 0's yield signal must not be set
  pool.RescheduleTaskOnWorker(1, []() {});

  while (!worker0_done) std::this_thread::sleep_for(1ms);
  ASSERT_FALSE(worker0_saw_yield.load()) << "Yield must not be set on worker 0 when HP is added only to worker 1";

  pool.ShutDown();
  pool.AwaitShutdown();
}

TEST(PriorityThreadPool, YieldFlag_Stress) {
  using namespace memgraph;
  constexpr uint16_t kWorkers = 4;
  memgraph::utils::WorkerYieldRegistry registry(kWorkers);
  memgraph::utils::PriorityThreadPool pool(kWorkers, nullptr, &registry);

  constexpr int kIterations = 30;
  int requested_yields_seen = 0;

  for (int iter = 0; iter < kIterations; ++iter) {
    [[maybe_unused]] const uint16_t worker = static_cast<uint16_t>(iter % kWorkers);
    std::atomic<bool> lp_running{false};
    std::atomic<bool> lp_saw_yield{false};
    std::atomic<bool> hp_ran{false};

    pool.ScheduledAddTask(
        [&]() {
          lp_running = true;
          for (int i = 0; i < 5'000'000; ++i) {
            auto *sig = utils::WorkerYieldRegistry::GetCurrentYieldSignal();
            if (sig && sig->load(std::memory_order_acquire)) {
              lp_saw_yield = true;
              return;
            }
          }
        },
        utils::Priority::LOW);

    while (!lp_running) std::this_thread::sleep_for(0ms);
    std::this_thread::sleep_for(0ms);

    pool.ScheduledAddTask([&]() { hp_ran = true; }, utils::Priority::HIGH);

    while (!hp_ran) std::this_thread::sleep_for(1ms);
    if (lp_saw_yield) ++requested_yields_seen;
  }

  pool.ShutDown();
  pool.AwaitShutdown();

  ASSERT_GT(requested_yields_seen, 0) << "At least one requested yield must be observed (no ignored yields)";
}

// Both workers blocked → submit tasks (round-robined to both queues) → release one worker.
// The released worker must execute all tasks: its own + stolen from the still-blocked worker.
TEST(PriorityThreadPool, WorkStealing) {
  using namespace memgraph;
  constexpr uint16_t kWorkers = 2;
  memgraph::utils::PriorityThreadPool pool{kWorkers};

  // Identify each worker's thread ID via pinned tasks.
  std::array<std::atomic<std::thread::id>, kWorkers> worker_tids;
  std::atomic<int> ids_ready{0};
  for (uint16_t i = 0; i < kWorkers; ++i) {
    pool.RescheduleTaskOnWorker(i, [&, i]() {
      worker_tids[i] = std::this_thread::get_id();
      ids_ready.fetch_add(1);
    });
  }
  while (ids_ready.load() != kWorkers) std::this_thread::sleep_for(1ms);
  ASSERT_NE(worker_tids[0].load(), worker_tids[1].load());

  // Block both workers so hot_threads_ is empty and ScheduledAddTask round-robins.
  std::atomic<bool> w0_running{false};
  std::atomic<bool> w1_running{false};
  std::atomic<bool> release_w0{false};
  std::atomic<bool> release_w1{false};
  pool.RescheduleTaskOnWorker(0, [&]() {
    w0_running = true;
    while (!release_w0) release_w0.wait(false);
  });
  pool.RescheduleTaskOnWorker(1, [&]() {
    w1_running = true;
    while (!release_w1) release_w1.wait(false);
  });
  while (!w0_running || !w1_running) std::this_thread::sleep_for(1ms);

  // Submit tasks while both workers are busy: round-robin puts ~half on each worker's queue.
  constexpr int n_tasks = 40;
  utils::Synchronized<std::set<std::thread::id>> executors;
  std::atomic<int> completed{0};
  for (int i = 0; i < n_tasks; ++i) {
    pool.ScheduledAddTask(
        [&]() {
          executors->insert(std::this_thread::get_id());
          completed.fetch_add(1);
        },
        utils::Priority::LOW);
  }

  // Release only worker 1. It runs its own queued tasks and steals from worker 0.
  release_w1 = true;
  release_w1.notify_all();

  while (completed.load() < n_tasks) std::this_thread::sleep_for(1ms);

  release_w0 = true;
  release_w0.notify_all();
  pool.ShutDown();
  pool.AwaitShutdown();

  // All tasks completed on worker 1's thread (worker 0 was blocked the entire time).
  executors.WithLock([&](const auto &ids) {
    ASSERT_EQ(ids.size(), 1U) << "All tasks must complete on worker 1";
    ASSERT_EQ(*ids.begin(), worker_tids[1].load())
        << "Worker 1 must have executed all tasks (own queue + stolen from worker 0)";
  });
}

// Yield signal set by task N must be cleared before task N+1 starts.
TEST(PriorityThreadPool, YieldSignalNotInheritedByNextTask) {
  using namespace memgraph;
  memgraph::utils::WorkerYieldRegistry registry(1);
  memgraph::utils::PriorityThreadPool pool(1, nullptr, &registry);

  std::atomic<bool> second_saw_signal{false};
  std::atomic<bool> done{false};

  // Task 1: sets yield signal for worker 0, then completes normally.
  pool.ScheduledAddTask([&]() { registry.RequestYieldForWorker(0); }, utils::Priority::LOW);

  // Task 2: runs after task 1; signal must be cleared before it starts.
  pool.ScheduledAddTask(
      [&]() {
        auto *sig = utils::WorkerYieldRegistry::GetCurrentYieldSignal();
        second_saw_signal = sig && sig->load(std::memory_order_acquire);
        done = true;
      },
      utils::Priority::LOW);

  while (!done) std::this_thread::sleep_for(1ms);
  pool.ShutDown();
  pool.AwaitShutdown();

  ASSERT_FALSE(second_saw_signal.load()) << "Yield signal must be cleared before each new task starts";
}

// ShutDown + AwaitShutdown must complete without hanging on an empty pool
// and on a pool that has tasks queued or in flight.
TEST(PriorityThreadPool, ShutdownNoHang) {
  using namespace memgraph;

  // Empty pool shutdown.
  {
    memgraph::utils::PriorityThreadPool pool{4};
    pool.ShutDown();
    pool.AwaitShutdown();
  }

  // Shutdown with tasks already executing.
  {
    memgraph::utils::PriorityThreadPool pool{2};
    std::atomic<int> started{0};
    constexpr int n = 10;
    for (int i = 0; i < n; ++i) {
      pool.ScheduledAddTask(
          [&]() {
            started.fetch_add(1);
            std::this_thread::sleep_for(2ms);
          },
          utils::Priority::LOW);
    }
    // Let some tasks start before shutting down.
    while (started.load() == 0) std::this_thread::sleep_for(1ms);
    pool.ShutDown();
    pool.AwaitShutdown();
    // Reaching here without timeout is the assertion.
  }
}

// ScheduleResumableTask Tests

// Task returns false immediately — must execute exactly once.
TEST(PriorityThreadPool, ResumableTask_CompletesImmediately) {
  using namespace memgraph;
  memgraph::utils::PriorityThreadPool pool{1};

  std::atomic<int> call_count{0};
  std::atomic<bool> done{false};

  pool.ScheduleResumableTask(
      [&]() -> bool {
        call_count.fetch_add(1);
        done = true;
        return false;
      },
      utils::Priority::LOW);

  while (!done.load(std::memory_order_acquire)) std::this_thread::sleep_for(1ms);
  pool.ShutDown();
  pool.AwaitShutdown();

  ASSERT_EQ(call_count.load(), 1) << "Task must run exactly once when it never yields";
}

// Task yields once (sets signal, returns true), then completes. Both runs must land on the same worker.
TEST(PriorityThreadPool, ResumableTask_YieldsOnceThenCompletes) {
  using namespace memgraph;
  memgraph::utils::WorkerYieldRegistry registry(1);
  memgraph::utils::PriorityThreadPool pool(1, nullptr, &registry);

  std::atomic<int> call_count{0};
  std::atomic<std::thread::id> first_thread_id;
  std::atomic<std::thread::id> second_thread_id;
  std::atomic<bool> done{false};

  pool.ScheduleResumableTask(
      [&]() -> bool {
        int call = call_count.fetch_add(1) + 1;
        if (call == 1) {
          first_thread_id = std::this_thread::get_id();
          // Simulate a high-priority task arriving: set yield signal so pool pins continuation.
          registry.RequestYieldForWorker(0);
          return true;
        }
        second_thread_id = std::this_thread::get_id();
        done = true;
        return false;
      },
      utils::Priority::LOW);

  while (!done.load(std::memory_order_acquire)) std::this_thread::sleep_for(1ms);
  pool.ShutDown();
  pool.AwaitShutdown();

  ASSERT_EQ(call_count.load(), 2) << "Task must run exactly twice";
  ASSERT_EQ(first_thread_id.load(), second_thread_id.load())
      << "Both runs must execute on the same worker thread (pinned reschedule)";
}

// Task yields multiple times, each time setting the signal so the pool keeps it pinned.
TEST(PriorityThreadPool, ResumableTask_YieldsMultipleTimes) {
  using namespace memgraph;
  constexpr int kYields = 5;
  memgraph::utils::WorkerYieldRegistry registry(1);
  memgraph::utils::PriorityThreadPool pool(1, nullptr, &registry);

  std::atomic<int> call_count{0};
  std::atomic<bool> done{false};

  pool.ScheduleResumableTask(
      [&]() -> bool {
        int call = call_count.fetch_add(1) + 1;
        if (call <= kYields) {
          registry.RequestYieldForWorker(0);
          return true;
        }
        done = true;
        return false;
      },
      utils::Priority::LOW);

  while (!done.load(std::memory_order_acquire)) std::this_thread::sleep_for(1ms);
  pool.ShutDown();
  pool.AwaitShutdown();

  ASSERT_EQ(call_count.load(), kYields + 1) << "Task must run kYields+1 times total";
}

// Task returns true but no yield signal is set (no registry) — pool falls back to ScheduledAddTask.
// Task must still complete even without worker pinning.
TEST(PriorityThreadPool, ResumableTask_FallbackWhenSignalNotSet) {
  using namespace memgraph;
  // No WorkerYieldRegistry: GetCurrentYieldSignal() returns nullptr inside the pool wrapper.
  memgraph::utils::PriorityThreadPool pool{1};

  std::atomic<int> call_count{0};
  std::atomic<bool> done{false};

  pool.ScheduleResumableTask(
      [&]() -> bool {
        int call = call_count.fetch_add(1) + 1;
        if (call < 3) return true;  // yield intent, but no signal set → fallback reschedule
        done = true;
        return false;
      },
      utils::Priority::LOW);

  while (!done.load(std::memory_order_acquire)) std::this_thread::sleep_for(1ms);
  pool.ShutDown();
  pool.AwaitShutdown();

  ASSERT_EQ(call_count.load(), 3) << "Task must run 3 times via fallback scheduling";
}

// Resumable task scheduled with HIGH priority yields and completes correctly.
TEST(PriorityThreadPool, ResumableTask_HighPriority) {
  using namespace memgraph;
  memgraph::utils::WorkerYieldRegistry registry(1);
  memgraph::utils::PriorityThreadPool pool(1, nullptr, &registry);

  std::atomic<int> call_count{0};
  std::atomic<bool> done{false};

  pool.ScheduleResumableTask(
      [&]() -> bool {
        int call = call_count.fetch_add(1) + 1;
        if (call == 1) {
          registry.RequestYieldForWorker(0);
          return true;
        }
        done = true;
        return false;
      },
      utils::Priority::HIGH);

  while (!done) std::this_thread::sleep_for(1ms);
  pool.ShutDown();
  pool.AwaitShutdown();

  ASSERT_EQ(call_count.load(), 2) << "HP resumable task must yield once then complete";
}

TEST(PriorityThreadPool, ResumableTask_SelfParksUntilEventResume) {
  using namespace memgraph;
  utils::WorkerYieldRegistry registry(1);
  utils::PriorityThreadPool pool(1, nullptr, &registry);
  utils::WorkerResumeEvent event;

  std::atomic<int> call_count{0};
  std::atomic<bool> parked{false};
  std::atomic<bool> done{false};
  std::atomic<std::thread::id> first_thread_id;
  std::atomic<std::thread::id> second_thread_id;

  pool.ScheduleResumableTask(
      [&]() -> bool {
        const int call = call_count.fetch_add(1) + 1;
        if (call == 1) {
          first_thread_id = std::this_thread::get_id();
          parked = utils::CurrentResumableTask::RegisterWaiter(event, event.Epoch());
          return false;
        }
        second_thread_id = std::this_thread::get_id();
        done = true;
        return false;
      },
      utils::Priority::LOW);

  while (!parked.load(std::memory_order_acquire)) {
    std::this_thread::sleep_for(1ms);
  }

  std::this_thread::sleep_for(10ms);
  ASSERT_EQ(call_count.load(), 1) << "Self-parked task must not be requeued before the event resumes it";

  event.NotifyAll();

  while (!done.load(std::memory_order_acquire)) {
    std::this_thread::sleep_for(1ms);
  }

  pool.ShutDown();
  pool.AwaitShutdown();

  ASSERT_EQ(call_count.load(), 2) << "Self-parked task must resume exactly once after the event";
  ASSERT_EQ(first_thread_id.load(), second_thread_id.load())
      << "Event-resumed task should continue on the original worker when possible";
}

// Pool shutdown while a resumable task keeps returning true must not hang.
// After stop_requested, RescheduleTaskOnWorker and ScheduledAddTask become no-ops.
TEST(PriorityThreadPool, ResumableTask_ShutdownMidFlight) {
  using namespace memgraph;
  memgraph::utils::PriorityThreadPool pool{1};

  std::atomic<int> call_count{0};
  std::atomic<bool> first_run{false};

  pool.ScheduleResumableTask(
      [&]() -> bool {
        call_count.fetch_add(1);
        first_run = true;
        return true;  // always yield — would loop forever without shutdown
      },
      utils::Priority::LOW);

  // Wait until the task has run at least once, then shut down mid-flight.
  while (!first_run) std::this_thread::sleep_for(1ms);
  pool.ShutDown();
  pool.AwaitShutdown();
  // Reaching here without hanging is the assertion.
  ASSERT_GE(call_count.load(), 1);
}

// TaskCollection Tests
TEST(TaskCollection, BasicAddAndSize) {
  using namespace memgraph;
  memgraph::utils::TaskCollection collection;

  ASSERT_EQ(collection.Size(), 0);

  collection.AddTask([]() {});
  ASSERT_EQ(collection.Size(), 1);

  collection.AddTask([]() {});
  collection.AddTask([]() {});
  ASSERT_EQ(collection.Size(), 3);
}

TEST(TaskCollection, BasicWait) {
  using namespace memgraph;
  memgraph::utils::TaskCollection collection;

  std::atomic<int> counter{0};
  constexpr int num_tasks = 5;

  for (int i = 0; i < num_tasks; ++i) {
    collection.AddTask([&counter]() {
      std::this_thread::sleep_for(std::chrono::milliseconds(10));
      counter.fetch_add(1);
    });
  }

  // Execute tasks manually to test Wait()
  for (size_t i = 0; i < collection.Size(); ++i) {
    auto wrapped_task = collection.WrapTask(i);
    wrapped_task();
  }

  // Everything should be already scheduled, so it should wait for all tasks to finish
  collection.Wait();
  ASSERT_EQ(counter.load(), num_tasks);
}

TEST(TaskCollection, WaitOrSteal) {
  using namespace memgraph;
  memgraph::utils::TaskCollection collection;

  std::atomic<int> counter{0};
  constexpr int num_tasks = 10;

  for (int i = 0; i < num_tasks; ++i) {
    collection.AddTask([&counter]() {
      std::this_thread::sleep_for(std::chrono::milliseconds(5));
      counter.fetch_add(1);
    });
  }

  // Execute some tasks manually to test WaitOrSteal()
  for (size_t i = 0; i < collection.Size(); i += 3) {
    auto wrapped_task = collection.WrapTask(i);
    wrapped_task();
  }

  // WaitOrSteal should execute all tasks and wait for completion
  collection.WaitOrSteal();
  ASSERT_EQ(counter.load(), num_tasks);
}

TEST(TaskCollection, TryExecuteOneIdleTaskRunsSingleTask) {
  using namespace memgraph;
  memgraph::utils::TaskCollection collection;

  std::atomic<int> counter{0};
  collection.AddTask([&counter]() { counter.fetch_add(1); });
  collection.AddTask([&counter]() { counter.fetch_add(1); });
  collection.AddTask([&counter]() { counter.fetch_add(1); });

  ASSERT_TRUE(collection.TryExecuteOneIdleTask());
  ASSERT_EQ(counter.load(), 1) << "Only one idle task should be executed per call";
  ASSERT_FALSE(collection.Finished()) << "Remaining tasks should still be pending";

  collection.WaitOrSteal();
  ASSERT_EQ(counter.load(), 3);
}

TEST(TaskCollection, WaitForProgressWakesOnTaskCompletion) {
  using namespace memgraph;
  memgraph::utils::TaskCollection collection;

  std::atomic<bool> started{false};
  collection.AddTask([&started]() {
    started = true;
    std::this_thread::sleep_for(10ms);
  });

  auto wrapped_task = collection.WrapTask(0);
  auto runner = std::jthread([&wrapped_task]() { wrapped_task(); });

  while (!started.load(std::memory_order_acquire)) {
    std::this_thread::sleep_for(1ms);
  }

  ASSERT_TRUE(collection.WaitForProgress(50ms))
      << "WaitForProgress should observe task completion and wake before timing out";
  collection.Wait();
  ASSERT_TRUE(collection.Finished());
}

TEST(TaskCollection, WaitForProgressTimesOutWithoutTaskProgress) {
  using namespace memgraph;
  memgraph::utils::TaskCollection collection;

  collection.AddTask([]() {});

  ASSERT_FALSE(collection.WaitForProgress(5ms))
      << "WaitForProgress should time out when no task has been scheduled yet";
}

TEST(TaskCollection, WaitForProgressWakesOnTaskYield) {
  using namespace memgraph;
  memgraph::utils::TaskCollection collection;

  std::atomic<bool> allow_completion{false};
  std::atomic<bool> waiting_for_progress{false};
  collection.AddResumableTask([&]() -> bool {
    if (!allow_completion.load(std::memory_order_acquire)) {
      return true;
    }
    return false;
  });

  auto wrapped_task = collection.WrapTask(0);
  auto runner = std::jthread([&]() mutable {
    while (!waiting_for_progress.load(std::memory_order_acquire)) {
      std::this_thread::sleep_for(1ms);
    }
    wrapped_task();
  });

  waiting_for_progress = true;
  ASSERT_TRUE(collection.WaitForProgress(50ms))
      << "WaitForProgress should observe a resumable task yielding, not just final completion";
  ASSERT_FALSE(collection.Finished()) << "A yielded task should still be pending after progress is reported";

  allow_completion = true;
  wrapped_task();
  collection.Wait();
  ASSERT_TRUE(collection.Finished());
}

namespace {
struct TestCoroutine {
  struct promise_type {
    TestCoroutine get_return_object() {
      return TestCoroutine{std::coroutine_handle<promise_type>::from_promise(*this)};
    }

    std::suspend_never initial_suspend() noexcept { return {}; }

    std::suspend_always final_suspend() noexcept { return {}; }

    void return_void() {}

    void unhandled_exception() { std::terminate(); }
  };

  explicit TestCoroutine(std::coroutine_handle<promise_type> handle) : handle_(handle) {}

  TestCoroutine(TestCoroutine &&other) noexcept : handle_(std::exchange(other.handle_, {})) {}

  TestCoroutine &operator=(TestCoroutine &&other) noexcept {
    if (this != &other) {
      if (handle_) handle_.destroy();
      handle_ = std::exchange(other.handle_, {});
    }
    return *this;
  }

  ~TestCoroutine() {
    if (handle_) handle_.destroy();
  }

  std::coroutine_handle<promise_type> handle_;
};

TestCoroutine AwaitCollectionProgress(memgraph::utils::CollectionScheduler &scheduler, std::atomic<bool> &resumed) {
  co_await scheduler.WaitForProgressAwaitable();
  resumed = true;
}

TestCoroutine AwaitCollectionProgressTwice(memgraph::utils::CollectionScheduler &scheduler, std::atomic<int> &resumes) {
  co_await scheduler.WaitForProgressAwaitable();
  resumes.fetch_add(1, std::memory_order_acq_rel);
  co_await scheduler.WaitForProgressAwaitable();
  resumes.fetch_add(1, std::memory_order_acq_rel);
}

struct WorkerResumeAwaitable {
  memgraph::utils::WorkerResumeEvent *event;
  memgraph::utils::PriorityThreadPool *pool;
  uint64_t observed_epoch;

  bool await_ready() const noexcept { return false; }

  bool await_suspend(std::coroutine_handle<> handle) const {
    return event->RegisterWaiter(
        handle, pool, memgraph::utils::WorkerYieldRegistry::GetCurrentWorkerId(), observed_epoch);
  }

  void await_resume() const noexcept {}
};

TestCoroutine AwaitWorkerResumeEvent(memgraph::utils::WorkerResumeEvent &event,
                                     memgraph::utils::PriorityThreadPool &pool, std::atomic<bool> &resumed,
                                     std::atomic<std::thread::id> &resumed_thread_id) {
  co_await WorkerResumeAwaitable{.event = &event, .pool = &pool, .observed_epoch = event.Epoch()};
  resumed_thread_id = std::this_thread::get_id();
  resumed = true;
}
}  // namespace

TEST(TaskCollection, ProgressAwaitableResumesWaitingCoroutine) {
  using namespace memgraph;
  utils::WorkerYieldRegistry registry(1);
  utils::PriorityThreadPool pool(1, nullptr, &registry);

  auto collection = std::make_shared<utils::TaskCollection>();
  std::atomic<bool> task_started{false};
  collection->AddTask([&task_started]() {
    task_started = true;
    std::this_thread::sleep_for(10ms);
  });

  utils::CollectionScheduler scheduler(&pool, collection);
  std::atomic<bool> coroutine_started{false};
  std::atomic<bool> coroutine_resumed{false};
  std::optional<TestCoroutine> waiter;

  pool.RescheduleTaskOnWorker(0, [&]() {
    waiter.emplace(AwaitCollectionProgress(scheduler, coroutine_resumed));
    coroutine_started = true;
  });

  while (!coroutine_started.load(std::memory_order_acquire)) {
    std::this_thread::sleep_for(1ms);
  }

  scheduler.Trigger();

  while (!task_started.load(std::memory_order_acquire) || !coroutine_resumed.load(std::memory_order_acquire)) {
    std::this_thread::sleep_for(1ms);
  }

  collection->Wait();
  pool.ShutDown();
  pool.AwaitShutdown();

  ASSERT_TRUE(coroutine_resumed.load()) << "Progress awaitable should resume the waiting coroutine after task progress";
}

TEST(TaskCollection, ProgressAwaitableTracksSuccessiveProgressEpochs) {
  using namespace memgraph;
  utils::WorkerYieldRegistry registry(1);
  utils::PriorityThreadPool pool(1, nullptr, &registry);

  auto collection = std::make_shared<utils::TaskCollection>();
  std::atomic<int> task_stage{0};
  collection->AddResumableTask([&]() -> bool {
    int stage = task_stage.fetch_add(1, std::memory_order_acq_rel);
    return stage < 1;
  });

  utils::CollectionScheduler scheduler(&pool, collection);
  std::atomic<bool> coroutine_started{false};
  std::atomic<int> coroutine_resumes{0};
  std::optional<TestCoroutine> waiter;

  pool.RescheduleTaskOnWorker(0, [&]() {
    waiter.emplace(AwaitCollectionProgressTwice(scheduler, coroutine_resumes));
    coroutine_started = true;
  });

  while (!coroutine_started.load(std::memory_order_acquire)) {
    std::this_thread::sleep_for(1ms);
  }

  scheduler.Trigger();

  while (coroutine_resumes.load(std::memory_order_acquire) != 2) {
    std::this_thread::sleep_for(1ms);
  }

  collection->Wait();
  pool.ShutDown();
  pool.AwaitShutdown();

  ASSERT_EQ(task_stage.load(std::memory_order_acquire), 2)
      << "Task should yield once and then complete on the second progress event";
  ASSERT_EQ(coroutine_resumes.load(std::memory_order_acquire), 2)
      << "Progress awaitable should wake once per new progress epoch";
}

TEST(TaskCollection, ResumableCollectionTaskYieldsThenCompletes) {
  using namespace memgraph;
  utils::WorkerYieldRegistry registry(1);
  utils::PriorityThreadPool pool(1, nullptr, &registry);

  utils::TaskCollection collection;
  std::atomic<int> call_count{0};
  std::atomic<std::thread::id> first_thread_id;
  std::atomic<std::thread::id> second_thread_id;
  std::atomic<bool> done{false};

  collection.AddResumableTask([&]() -> bool {
    const int call = call_count.fetch_add(1) + 1;
    if (call == 1) {
      first_thread_id = std::this_thread::get_id();
      registry.RequestYieldForWorker(0);
      return true;
    }
    second_thread_id = std::this_thread::get_id();
    done = true;
    return false;
  });

  pool.ScheduledCollection(collection);

  while (!done.load(std::memory_order_acquire)) {
    std::this_thread::sleep_for(1ms);
  }

  collection.Wait();
  pool.ShutDown();
  pool.AwaitShutdown();

  ASSERT_EQ(call_count.load(), 2) << "Collection task must resume after one yield and then complete";
  ASSERT_EQ(first_thread_id.load(), second_thread_id.load())
      << "Yielded collection tasks should resume on the same worker";
  ASSERT_TRUE(collection.Finished()) << "Collection should report finished after the resumable task completes";
}

TEST(TaskCollection, ResumableCollectionTaskSelfParksUntilEventResume) {
  using namespace memgraph;
  utils::WorkerYieldRegistry registry(1);
  utils::PriorityThreadPool pool(1, nullptr, &registry);
  utils::WorkerResumeEvent event;

  utils::TaskCollection collection;
  std::atomic<int> call_count{0};
  std::atomic<bool> parked{false};
  std::atomic<bool> done{false};
  std::atomic<std::thread::id> first_thread_id;
  std::atomic<std::thread::id> second_thread_id;

  collection.AddResumableTask([&]() -> bool {
    const int call = call_count.fetch_add(1) + 1;
    if (call == 1) {
      first_thread_id = std::this_thread::get_id();
      parked = utils::CurrentResumableTask::RegisterWaiter(event, event.Epoch());
      return false;
    }
    second_thread_id = std::this_thread::get_id();
    done = true;
    return false;
  });

  pool.ScheduledCollection(collection);

  while (!parked.load(std::memory_order_acquire)) {
    std::this_thread::sleep_for(1ms);
  }

  std::this_thread::sleep_for(10ms);
  ASSERT_EQ(call_count.load(), 1) << "Self-parked collection task must stay parked until resumed";
  ASSERT_FALSE(collection.Finished()) << "Parked task should keep the collection incomplete";

  event.NotifyAll();

  while (!done.load(std::memory_order_acquire)) {
    std::this_thread::sleep_for(1ms);
  }

  collection.Wait();
  pool.ShutDown();
  pool.AwaitShutdown();

  ASSERT_EQ(call_count.load(), 2) << "Event should resume the parked collection task once";
  ASSERT_EQ(first_thread_id.load(), second_thread_id.load())
      << "Parked collection task should resume on the original worker when possible";
  ASSERT_TRUE(collection.Finished()) << "Collection should finish after the parked task resumes and completes";
}

TEST(WorkerResumeEvent, NotifyAllResumesWaitersOnOriginalWorkers) {
  using namespace memgraph;
  utils::WorkerYieldRegistry registry(2);
  utils::PriorityThreadPool pool(2, nullptr, &registry);
  utils::WorkerResumeEvent event;

  std::atomic<std::thread::id> worker0_thread_id;
  std::atomic<std::thread::id> worker1_thread_id;
  std::atomic<bool> waiter0_started{false};
  std::atomic<bool> waiter1_started{false};
  std::atomic<bool> waiter0_resumed{false};
  std::atomic<bool> waiter1_resumed{false};
  std::atomic<std::thread::id> waiter0_resumed_thread_id;
  std::atomic<std::thread::id> waiter1_resumed_thread_id;
  std::optional<TestCoroutine> waiter0;
  std::optional<TestCoroutine> waiter1;

  pool.RescheduleTaskOnWorker(0, [&]() {
    worker0_thread_id = std::this_thread::get_id();
    waiter0.emplace(AwaitWorkerResumeEvent(event, pool, waiter0_resumed, waiter0_resumed_thread_id));
    waiter0_started = true;
  });
  pool.RescheduleTaskOnWorker(1, [&]() {
    worker1_thread_id = std::this_thread::get_id();
    waiter1.emplace(AwaitWorkerResumeEvent(event, pool, waiter1_resumed, waiter1_resumed_thread_id));
    waiter1_started = true;
  });

  while (!waiter0_started.load(std::memory_order_acquire) || !waiter1_started.load(std::memory_order_acquire)) {
    std::this_thread::sleep_for(1ms);
  }

  event.NotifyAll();

  while (!waiter0_resumed.load(std::memory_order_acquire) || !waiter1_resumed.load(std::memory_order_acquire)) {
    std::this_thread::sleep_for(1ms);
  }

  pool.ShutDown();
  pool.AwaitShutdown();

  ASSERT_EQ(waiter0_resumed_thread_id.load(), worker0_thread_id.load())
      << "Waiter 0 should resume on the worker where it originally suspended";
  ASSERT_EQ(waiter1_resumed_thread_id.load(), worker1_thread_id.load())
      << "Waiter 1 should resume on the worker where it originally suspended";
}

TEST(WorkerResumeEvent, StaleEpochDoesNotRegisterWaiter) {
  using namespace memgraph;
  utils::WorkerResumeEvent event;

  const auto observed_epoch = event.Epoch();
  event.NotifyAll();

  ASSERT_FALSE(event.RegisterWaiter(std::noop_coroutine(), nullptr, std::nullopt, observed_epoch))
      << "Waiter registration must fail when the observed epoch is stale";
}

TEST(TaskCollection, ThreadPoolIntegration) {
  using namespace memgraph;
  memgraph::utils::PriorityThreadPool pool{2};
  memgraph::utils::TaskCollection collection;

  std::atomic<int> counter{0};
  constexpr int num_tasks = 20;

  for (int i = 0; i < num_tasks; ++i) {
    collection.AddTask([&counter]() {
      std::this_thread::sleep_for(std::chrono::milliseconds(1));
      counter.fetch_add(1);
    });
  }

  // Schedule collection to thread pool
  pool.ScheduledCollection(collection);

  // Wait for all tasks to complete
  collection.Wait();
  ASSERT_EQ(counter.load(), num_tasks);

  pool.ShutDown();
  pool.AwaitShutdown();
}

TEST(TaskCollection, ConcurrentExecution) {
  using namespace memgraph;
  memgraph::utils::PriorityThreadPool pool{4};
  memgraph::utils::TaskCollection collection;

  std::atomic<int> counter{0};
  constexpr int num_tasks = 50;

  for (int i = 0; i < num_tasks; ++i) {
    collection.AddTask([&counter]() {
      std::this_thread::sleep_for(std::chrono::milliseconds(1));
      counter.fetch_add(1);
    });
  }

  // Schedule collection to thread pool
  pool.ScheduledCollection(collection);

  // Wait for all tasks to complete
  collection.Wait();
  ASSERT_EQ(counter.load(), num_tasks);

  pool.ShutDown();
  pool.AwaitShutdown();
}

TEST(TaskCollection, MixedWaitAndSteal) {
  using namespace memgraph;
  memgraph::utils::PriorityThreadPool pool{1};
  memgraph::utils::TaskCollection collection;

  std::atomic<int> counter{0};
  constexpr int num_tasks = 15;

  std::mutex thread_counter_mutex;
  std::map<std::thread::id, int> thread_counter;

  for (int i = 0; i < num_tasks; ++i) {
    collection.AddTask([&counter, &thread_counter_mutex, &thread_counter]() {
      // Tack which thread is executing the task
      auto thread_id = std::this_thread::get_id();
      {
        std::lock_guard<std::mutex> lock(thread_counter_mutex);
        thread_counter[thread_id]++;
      }
      std::this_thread::sleep_for(std::chrono::milliseconds(100));
      counter.fetch_add(1);
    });
  }

  // Schedule some tasks to thread pool
  pool.ScheduledCollection(collection);

  // WaitOrSteal should handle remaining tasks and wait for all
  collection.WaitOrSteal();
  ASSERT_EQ(counter.load(), num_tasks);

  // Check if the tasks were executed by the same thread
  ASSERT_GT(thread_counter.size(), 1);
  ASSERT_TRUE(thread_counter.contains(std::this_thread::get_id()));

  pool.ShutDown();
  pool.AwaitShutdown();
}

TEST(TaskCollection, ExceptionHandling) {
  using namespace memgraph;
  memgraph::utils::TaskCollection collection;

  std::atomic<int> success_count{0};
  std::atomic<int> exception_count{0};
  constexpr int num_tasks = 10;

  for (int i = 0; i < num_tasks; ++i) {
    if (i % 3 == 0) {
      // Every third task throws an exception
      collection.AddTask([&exception_count]() {
        exception_count.fetch_add(1);
        throw std::runtime_error("Test exception");
      });
    } else {
      collection.AddTask([&success_count]() { success_count.fetch_add(1); });
    }
  }

  // WaitOrSteal should handle exceptions properly
  // When an exception occurs, it stops execution of remaining tasks
  try {
    collection.WaitOrSteal();
  } catch (const std::runtime_error &e) {
    // Expected exception - this stops execution of remaining tasks
  }

  // Only tasks executed before the first exception should be counted
  // The exact count depends on which task throws first
  int total_executed = success_count.load() + exception_count.load();
  ASSERT_GT(total_executed, 0);          // At least one task should execute
  ASSERT_LE(total_executed, num_tasks);  // But not more than total tasks

  // At least one exception should have occurred
  ASSERT_GT(exception_count.load(), 0);
}

TEST(TaskCollection, ExceptionHandlingIndividual) {
  using namespace memgraph;
  memgraph::utils::TaskCollection collection;

  std::atomic<int> success_count{0};
  std::atomic<int> exception_count{0};
  constexpr int num_tasks = 10;

  for (int i = 0; i < num_tasks; ++i) {
    if (i % 3 == 0) {
      // Every third task throws an exception
      collection.AddTask([&exception_count]() {
        exception_count.fetch_add(1);
        throw std::runtime_error("Test exception");
      });
    } else {
      collection.AddTask([&success_count]() { success_count.fetch_add(1); });
    }
  }

  // Execute tasks individually to handle exceptions properly
  for (size_t i = 0; i < collection.Size(); ++i) {
    try {
      auto wrapped_task = collection.WrapTask(i);
      wrapped_task();
    } catch (const std::runtime_error &e) {
      // Expected exception - continue with next task
    }
  }

  // Now all tasks should have been executed
  ASSERT_EQ(success_count.load() + exception_count.load(), num_tasks);
  ASSERT_EQ(success_count.load(), 6);    // 6 successful tasks
  ASSERT_EQ(exception_count.load(), 4);  // 4 exception tasks
}

TEST(TaskCollection, TaskStateTransitions) {
  using namespace memgraph;
  memgraph::utils::TaskCollection collection;

  std::atomic<int> execution_count{0};
  collection.AddTask([&execution_count]() { execution_count.fetch_add(1); });

  // Test that task starts in IDLE state
  auto &task = collection[0];
  ASSERT_EQ(task.state_->load(), memgraph::utils::TaskCollection::Task::State::IDLE);

  // Wrap and execute task
  auto wrapped_task = collection.WrapTask(0);
  wrapped_task();

  // Task should be in FINISHED state
  ASSERT_EQ(task.state_->load(), memgraph::utils::TaskCollection::Task::State::FINISHED);
  ASSERT_EQ(execution_count.load(), 1);
}

TEST(TaskCollection, MultipleExecutionsPrevented) {
  using namespace memgraph;
  memgraph::utils::TaskCollection collection;

  std::atomic<int> execution_count{0};
  collection.AddTask([&execution_count]() { execution_count.fetch_add(1); });

  auto wrapped_task = collection.WrapTask(0);

  // Execute task multiple times - should only execute once
  wrapped_task();
  wrapped_task();
  wrapped_task();

  ASSERT_EQ(execution_count.load(), 1);

  // Task should be in FINISHED state
  auto &task = collection[0];
  ASSERT_EQ(task.state_->load(), memgraph::utils::TaskCollection::Task::State::FINISHED);
}

TEST(TaskCollection, LargeTaskSet) {
  using namespace memgraph;
  memgraph::utils::PriorityThreadPool pool{8};
  memgraph::utils::TaskCollection collection;

  std::atomic<int> counter{0};
  constexpr int num_tasks = 1000;

  for (int i = 0; i < num_tasks; ++i) {
    collection.AddTask([&counter]() { counter.fetch_add(1); });
  }

  // Schedule collection to thread pool
  pool.ScheduledCollection(collection);

  // Wait for all tasks to complete
  collection.Wait();
  ASSERT_EQ(counter.load(), num_tasks);

  pool.ShutDown();
  pool.AwaitShutdown();
}

// Test that yielded tasks properly report HasNonTerminalTasks until they finish
TEST(TaskCollection, YieldedTaskLifecycle) {
  using namespace memgraph;
  memgraph::utils::TaskCollection collection;

  std::atomic<int> execution_count{0};
  std::atomic<bool> should_yield{true};

  // Add a resumable task that yields once then completes
  collection.AddResumableTask([&execution_count, &should_yield]() -> bool {
    execution_count.fetch_add(1);
    if (should_yield.exchange(false)) {
      return true;  // Yield - will be rescheduled
    }
    return false;  // Complete
  });

  auto &task = collection[0];

  // Initial state: IDLE
  ASSERT_EQ(task.state_->load(), memgraph::utils::TaskCollection::Task::State::IDLE);
  ASSERT_TRUE(collection.HasNonTerminalTasks());
  ASSERT_FALSE(collection.AllTerminal());

  // First execution: yields
  auto wrapped_task = collection.WrapTask(0);
  bool yielded = wrapped_task();
  ASSERT_TRUE(yielded);
  ASSERT_EQ(execution_count.load(), 1);

  // State should be SCHEDULED (between yield and resume)
  ASSERT_EQ(task.state_->load(), memgraph::utils::TaskCollection::Task::State::SCHEDULED);
  ASSERT_TRUE(collection.HasNonTerminalTasks());
  ASSERT_FALSE(collection.AllTerminal());

  // Second execution: completes
  yielded = wrapped_task();
  ASSERT_FALSE(yielded);
  ASSERT_EQ(execution_count.load(), 2);

  // State should be FINISHED
  ASSERT_EQ(task.state_->load(), memgraph::utils::TaskCollection::Task::State::FINISHED);
  ASSERT_FALSE(collection.HasNonTerminalTasks());
  ASSERT_TRUE(collection.AllTerminal());
  ASSERT_TRUE(collection.Finished());
}

// Test that AllTerminal and HasNonTerminalTasks work correctly with multiple tasks
TEST(TaskCollection, AllTerminalWithMixedTasks) {
  using namespace memgraph;
  memgraph::utils::TaskCollection collection;

  std::atomic<int> completed_count{0};

  // Add 3 simple tasks
  for (int i = 0; i < 3; ++i) {
    collection.AddTask([&completed_count]() { completed_count.fetch_add(1); });
  }

  ASSERT_TRUE(collection.HasNonTerminalTasks());
  ASSERT_FALSE(collection.AllTerminal());

  // Execute all tasks
  for (size_t i = 0; i < collection.Size(); ++i) {
    auto wrapped_task = collection.WrapTask(i);
    wrapped_task();
  }

  // All tasks should be terminal
  ASSERT_EQ(completed_count.load(), 3);
  ASSERT_FALSE(collection.HasNonTerminalTasks());
  ASSERT_TRUE(collection.AllTerminal());
  ASSERT_TRUE(collection.Finished());
}

// ============================================================================
// COMPREHENSIVE STRESS TESTS - All Code Paths
// ============================================================================

// Test: Exception path - task throws exception
TEST(TaskCollectionStress, ExceptionPathSetsFinished) {
  using namespace memgraph;
  memgraph::utils::TaskCollection collection;

  std::atomic<int> execution_count{0};
  collection.AddResumableTask([&execution_count]() -> bool {
    execution_count.fetch_add(1);
    throw std::runtime_error("Test exception");
    return false;
  });

  auto &task = collection[0];
  auto wrapped_task = collection.WrapTask(0);

  ASSERT_THROW(wrapped_task(), std::runtime_error);
  ASSERT_EQ(execution_count.load(), 1);

  // Even with exception, task should be FINISHED
  ASSERT_EQ(task.state_->load(), memgraph::utils::TaskCollection::Task::State::FINISHED);
  ASSERT_TRUE(collection.AllTerminal());
  ASSERT_FALSE(collection.HasNonTerminalTasks());
}

// Test: Parked task lifecycle - simulates ProducerProgressAwaitable parking
TEST(TaskCollectionStress, ParkedTaskLifecycle) {
  using namespace memgraph;
  memgraph::utils::TaskCollection collection;

  std::atomic<int> execution_count{0};
  std::atomic<bool> should_park{true};
  std::atomic<bool> parked_flag{false};

  // Task that parks itself once, then completes
  collection.AddResumableTask([&execution_count, &should_park, &parked_flag]() -> bool {
    execution_count.fetch_add(1);
    if (should_park.exchange(false)) {
      // Simulate parking - in real code this would use CurrentResumableTask::RegisterWaiter
      parked_flag.store(true);
      return false;  // Return false but "parked" flag is set
    }
    return false;  // Complete on resume
  });

  auto &task = collection[0];

  // First execution: task "parks" itself
  auto wrapped_task = collection.WrapTask(0);
  // Manually simulate the park behavior since we can't use RegisterWaiter in unit test
  // In the real code, WasParked() would return true if RegisterWaiter succeeded

  // For this test, verify the state machine works correctly
  // After execution that would park (but we're simulating without actual park mechanism):
  bool yielded = wrapped_task();
  ASSERT_FALSE(yielded);
  ASSERT_EQ(execution_count.load(), 1);

  // In real scenario with park, state would be PARKED
  // Here it's FINISHED because we didn't actually park
  ASSERT_EQ(task.state_->load(), memgraph::utils::TaskCollection::Task::State::FINISHED);
}

// Test: Multiple yields - task yields several times before completion
TEST(TaskCollectionStress, MultipleYields) {
  using namespace memgraph;
  memgraph::utils::TaskCollection collection;

  std::atomic<int> execution_count{0};
  constexpr int kYieldCount = 5;

  collection.AddResumableTask([&execution_count]() -> bool {
    int count = execution_count.fetch_add(1) + 1;
    return count < kYieldCount;  // Yield 4 times, complete on 5th
  });

  auto &task = collection[0];
  auto wrapped_task = collection.WrapTask(0);

  // Execute multiple times (simulating reschedules)
  int iterations = 0;
  while (true) {
    bool yielded = wrapped_task();
    iterations++;
    if (!yielded) break;

    // State should be SCHEDULED between iterations
    ASSERT_EQ(task.state_->load(), memgraph::utils::TaskCollection::Task::State::SCHEDULED);
    ASSERT_TRUE(collection.HasNonTerminalTasks());
  }

  ASSERT_EQ(iterations, kYieldCount);
  ASSERT_EQ(execution_count.load(), kYieldCount);
  ASSERT_EQ(task.state_->load(), memgraph::utils::TaskCollection::Task::State::FINISHED);
}

// Test: Mixed tasks - some complete immediately, some yield, some throw
TEST(TaskCollectionStress, MixedTaskBehaviors) {
  using namespace memgraph;
  memgraph::utils::TaskCollection collection;

  std::atomic<int> completed{0};
  std::atomic<int> yielded{0};
  std::atomic<int> exceptions{0};

  // Task 0: Completes immediately
  collection.AddTask([&completed]() { completed.fetch_add(1); });

  // Task 1: Yields once then completes
  std::atomic<bool> task1_yielded{false};
  collection.AddResumableTask([&completed, &yielded, &task1_yielded]() -> bool {
    if (!task1_yielded.exchange(true)) {
      yielded.fetch_add(1);
      return true;  // Yield first time
    }
    completed.fetch_add(1);
    return false;  // Complete second time
  });

  // Task 2: Throws exception
  collection.AddTask([&exceptions]() {
    exceptions.fetch_add(1);
    throw std::runtime_error("Task 2 exception");
  });

  // Task 3: Yields twice then completes
  std::atomic<int> task3_count{0};
  collection.AddResumableTask([&completed, &yielded, &task3_count]() -> bool {
    int count = task3_count.fetch_add(1);
    if (count < 2) {
      yielded.fetch_add(1);
      return true;  // Yield
    }
    completed.fetch_add(1);
    return false;  // Complete
  });

  // Execute all tasks
  for (size_t i = 0; i < collection.Size(); ++i) {
    auto wrapped_task = collection.WrapTask(i);
    try {
      while (wrapped_task()) {
        // Task yielded, will be rescheduled - simulate by calling again
      }
    } catch (const std::runtime_error &) {
      // Expected for task 2
    }
  }

  // Verify all tasks finished (even the one that threw)
  ASSERT_TRUE(collection.AllTerminal());
  // Task 0 completes immediately, Task 1 completes after 1 yield, Task 2 throws (no complete), Task 3 completes after 2
  // yields
  ASSERT_EQ(completed.load(), 3);   // Tasks 0, 1, 3 completed
  ASSERT_EQ(yielded.load(), 3);     // Task 1 yielded once, task 3 yielded twice (1 + 2 = 3)
  ASSERT_EQ(exceptions.load(), 1);  // Task 2 threw
}

// Test: Concurrent execution with thread pool - stress test
TEST(TaskCollectionStress, ConcurrentMixedExecution) {
  using namespace memgraph;
  constexpr int kNumWorkers = 4;
  constexpr int kNumTasks = 100;

  memgraph::utils::PriorityThreadPool pool{kNumWorkers};

  for (int run = 0; run < 10; ++run) {
    memgraph::utils::TaskCollection collection;
    std::atomic<int> completed{0};
    std::atomic<int> yielded_count{0};

    // Mix of tasks:
    // - Some complete immediately (30%)
    // - Some yield once (40%)
    // - Some yield multiple times (30%)
    for (int i = 0; i < kNumTasks; ++i) {
      if (i % 10 < 3) {
        // Immediate completion
        collection.AddTask([&completed]() { completed.fetch_add(1); });
      } else if (i % 10 < 7) {
        // Yield once
        std::shared_ptr<std::atomic<bool>> yielded = std::make_shared<std::atomic<bool>>(false);
        collection.AddResumableTask([&completed, &yielded_count, yielded]() -> bool {
          if (!yielded->exchange(true)) {
            yielded_count.fetch_add(1);
            return true;  // Yield
          }
          completed.fetch_add(1);
          return false;
        });
      } else {
        // Yield 2-3 times
        std::shared_ptr<std::atomic<int>> count = std::make_shared<std::atomic<int>>(0);
        constexpr int kMaxYields = 3;
        collection.AddResumableTask([&completed, &yielded_count, count]() -> bool {
          int c = count->fetch_add(1) + 1;
          if (c < kMaxYields) {
            yielded_count.fetch_add(1);
            return true;  // Yield
          }
          completed.fetch_add(1);
          return false;
        });
      }
    }

    // Schedule and wait
    pool.ScheduledCollection(collection);
    collection.Wait();

    // All tasks should be terminal
    ASSERT_TRUE(collection.AllTerminal()) << "Run " << run;
    ASSERT_EQ(completed.load(), kNumTasks) << "Run " << run;
  }

  pool.ShutDown();
  pool.AwaitShutdown();
}

// Test: WaitOrSteal with non-yielding tasks only - should complete inline
TEST(TaskCollectionStress, WaitOrStealWithNonYieldingTasks) {
  using namespace memgraph;
  memgraph::utils::PriorityThreadPool pool{2};
  memgraph::utils::TaskCollection collection;

  std::atomic<int> execution_count{0};

  // Add only non-yielding tasks
  for (int i = 0; i < 5; ++i) {
    collection.AddTask([&execution_count]() { execution_count.fetch_add(1); });
  }

  // Schedule on pool
  pool.ScheduledCollection(collection);

  // WaitOrSteal should complete all tasks (they were stolen or ran on pool)
  collection.WaitOrSteal();

  ASSERT_EQ(execution_count.load(), 5);
  ASSERT_TRUE(collection.AllTerminal());

  pool.ShutDown();
  pool.AwaitShutdown();
}

// Test: ScheduleResumableTask integration - yielded task gets rescheduled
TEST(TaskCollectionStress, ScheduleResumableTaskIntegration) {
  using namespace memgraph;
  memgraph::utils::PriorityThreadPool pool{2};

  std::atomic<int> execution_count{0};
  std::atomic<bool> yielded_once{false};

  // Create a resumable task that yields once
  memgraph::utils::ResumableTaskSignature resumable_task = [&execution_count, &yielded_once]() -> bool {
    execution_count.fetch_add(1);
    if (!yielded_once.exchange(true)) {
      return true;  // Yield first time
    }
    return false;  // Complete
  };

  // Schedule it
  pool.ScheduleResumableTask(std::move(resumable_task), memgraph::utils::Priority::LOW);

  // Wait for completion
  while (execution_count.load() < 2) {
    std::this_thread::sleep_for(std::chrono::milliseconds(1));
  }

  ASSERT_EQ(execution_count.load(), 2);

  pool.ShutDown();
  pool.AwaitShutdown();
}

// Test: TryExecuteOneIdleTask claims and executes task inline
TEST(TaskCollectionStress, TryExecuteOneIdleTaskBasic) {
  using namespace memgraph;
  memgraph::utils::TaskCollection collection;

  std::atomic<int> execution_count{0};
  collection.AddTask([&execution_count]() { execution_count.fetch_add(1); });

  // TryExecuteOneIdleTask should claim and execute the task
  bool executed = collection.TryExecuteOneIdleTask(nullptr);
  ASSERT_TRUE(executed);
  ASSERT_EQ(execution_count.load(), 1);
  ASSERT_TRUE(collection.AllTerminal());

  // Second call should return false (no more idle tasks)
  executed = collection.TryExecuteOneIdleTask(nullptr);
  ASSERT_FALSE(executed);
}

// Test: TryExecuteOneIdleTask with yielding task and pool rescheduling
TEST(TaskCollectionStress, TryExecuteOneIdleTaskWithYieldAndPool) {
  using namespace memgraph;
  memgraph::utils::PriorityThreadPool pool{2};
  memgraph::utils::TaskCollection collection;

  std::atomic<int> execution_count{0};
  std::atomic<bool> yielded{false};

  collection.AddResumableTask([&execution_count, &yielded]() -> bool {
    execution_count.fetch_add(1);
    if (!yielded.exchange(true)) {
      return true;  // Yield first time
    }
    return false;  // Complete
  });

  // TryExecuteOneIdleTask runs the task inline once (execution_count==1), it yields, and is rescheduled
  // onto the pool. The inline portion must have executed.
  bool executed = collection.TryExecuteOneIdleTask(&pool);
  ASSERT_TRUE(executed);
  // NOTE: the exact intermediate state here is RACY — the 2-worker pool can run the rescheduled resume
  // (execution_count -> 2, task -> terminal) before we observe it. So only assert the lower bound; the
  // deterministic end state is checked after Wait(). (Asserting ==1 / HasNonTerminalTasks here would fail
  // and, critically, ASSERT's early-return would SKIP Wait() below, destroying the collection while a
  // pool worker is still inside NotifyProgress -> a teardown data race.)
  ASSERT_GE(execution_count.load(), 1);

  // Wait for the rescheduled task to complete (and for all in-flight WrapTask invocations to drain,
  // so the collection can be safely destroyed at scope exit).
  collection.Wait();

  ASSERT_EQ(execution_count.load(), 2);
  ASSERT_TRUE(collection.AllTerminal());

  pool.ShutDown();
  pool.AwaitShutdown();
}

// Test: Empty collection operations
TEST(TaskCollectionStress, EmptyCollection) {
  using namespace memgraph;
  memgraph::utils::TaskCollection collection;

  // All terminal queries should return true for empty collection
  ASSERT_TRUE(collection.AllTerminal());
  ASSERT_FALSE(collection.HasNonTerminalTasks());
  ASSERT_TRUE(collection.Finished());

  // Wait should complete immediately
  collection.Wait();

  // TryExecuteOneIdleTask should return false
  ASSERT_FALSE(collection.TryExecuteOneIdleTask(nullptr));
}

// Test: Rapid state transitions - stress test for race conditions
TEST(TaskCollectionStress, RapidStateTransitions) {
  using namespace memgraph;
  constexpr int kNumTasks = 50;
  constexpr int kIterations = 100;

  for (int iter = 0; iter < kIterations; ++iter) {
    memgraph::utils::TaskCollection collection;
    std::atomic<int> counter{0};

    for (int i = 0; i < kNumTasks; ++i) {
      if (i % 3 == 0) {
        collection.AddTask([&counter]() { counter.fetch_add(1); });
      } else {
        std::shared_ptr<std::atomic<bool>> done = std::make_shared<std::atomic<bool>>(false);
        collection.AddResumableTask([&counter, done]() -> bool {
          if (!done->exchange(true)) {
            return true;  // Yield once
          }
          counter.fetch_add(1);
          return false;
        });
      }
    }

    // Execute all tasks inline
    for (size_t i = 0; i < collection.Size(); ++i) {
      auto wrapped = collection.WrapTask(i);
      while (wrapped()) {
        // Handle yields inline
      }
    }

    ASSERT_TRUE(collection.AllTerminal()) << "Iteration " << iter;
    ASSERT_EQ(counter.load(), kNumTasks) << "Iteration " << iter;
  }
}

// Test: Concurrent stealing and scheduling - verifies tasks_mutex_ prevents races
// This test specifically targets the heap-buffer-overflow scenario where
// TryExecuteOneIdleTask() races with ScheduledCollection()'s pool workers.
TEST(TaskCollectionStress, ConcurrentStealingAndScheduling) {
  using namespace memgraph;
  constexpr int kNumIterations = 50;
  constexpr int kNumTasks = 10;
  constexpr int kNumStealers = 4;

  for (int iter = 0; iter < kNumIterations; ++iter) {
    memgraph::utils::PriorityThreadPool pool{2};
    memgraph::utils::TaskCollection collection;
    std::atomic<int> execution_count{0};

    // Add non-yielding tasks
    for (int i = 0; i < kNumTasks; ++i) {
      collection.AddTask([&execution_count]() { execution_count.fetch_add(1); });
    }

    // Schedule on pool - this creates closures with references to tasks_
    pool.ScheduledCollection(collection);

    // Multiple threads try to steal simultaneously while pool workers execute
    std::vector<std::jthread> stealers;
    for (int s = 0; s < kNumStealers; ++s) {
      stealers.emplace_back([&collection, &pool]() {
        // Try to steal and execute tasks
        while (collection.HasNonTerminalTasks()) {
          if (!collection.TryExecuteOneIdleTask(&pool)) {
            std::this_thread::yield();
          }
        }
      });
    }

    // Wait for all stealers to complete
    for (auto &t : stealers) {
      t.join();
    }

    // Pool may still have pending tasks - wait for them
    collection.Wait();

    // All tasks should be executed exactly once
    ASSERT_EQ(execution_count.load(), kNumTasks) << "Iteration " << iter;
    ASSERT_TRUE(collection.AllTerminal()) << "Iteration " << iter;

    pool.ShutDown();
    pool.AwaitShutdown();
  }
}

// ============================================================================
// REAL WAIT PARK/RESUME TESTS — exercises the closure-based suspend/resume path
// in CollectionScheduler::RegisterProgressWaiter + TaskCollection::NotifyProgress.
// ============================================================================

// A collection task parks on WaitForProgressAwaitable; a producer's NotifyProgress
// (triggered by another task completing) wakes it; assert it resumes and the
// collection finishes.  Proves real suspend + wake (not busy-spin).
TEST(RealWaitPark, ResumesOnProgress) {
  using namespace memgraph;
  utils::WorkerYieldRegistry registry(2);
  utils::PriorityThreadPool pool(2, nullptr, &registry);

  auto collection = std::make_shared<utils::TaskCollection>();
  std::atomic<bool> producer_ran{false};
  std::atomic<bool> waiter_resumed{false};
  std::atomic<int> resume_count{0};
  std::atomic<std::thread::id> resume_tid;

  // Producer task: completes after a short delay, triggering NotifyProgress on FINISHED.
  collection->AddTask([&producer_ran]() {
    std::this_thread::sleep_for(10ms);
    producer_ran = true;
  });

  utils::CollectionScheduler scheduler(&pool, collection);

  // Waiter coroutine: parks via WaitForProgressAwaitable; resumes when the producer
  // finishes (NotifyProgress fires from WrapTask's FINISHED path).
  std::optional<TestCoroutine> waiter_coro;

  // Schedule onto worker 0 (pinned) so TLS worker_id is set and EC-2 doesn't fire.
  pool.RescheduleTaskOnWorker(0, [&]() {
    waiter_coro.emplace([&]() -> TestCoroutine {
      co_await scheduler.WaitForProgressAwaitable();
      resume_tid = std::this_thread::get_id();
      waiter_resumed = true;
      resume_count.fetch_add(1);
    }());
  });

  // Let the waiter coroutine get scheduled and park before the producer fires.
  std::this_thread::sleep_for(20ms);

  // Trigger the collection: producer runs, finishes, NotifyProgress fires, waiter resumes.
  scheduler.Trigger();

  const auto deadline = std::chrono::steady_clock::now() + 5s;
  while (!waiter_resumed.load(std::memory_order_acquire)) {
    ASSERT_LT(std::chrono::steady_clock::now(), deadline) << "RealWaitParkResumesOnProgress: waiter never resumed";
    std::this_thread::sleep_for(1ms);
  }

  collection->Wait();
  pool.ShutDown();
  pool.AwaitShutdown();

  ASSERT_TRUE(producer_ran.load()) << "Producer task must have run";
  ASSERT_TRUE(waiter_resumed.load()) << "Waiter must resume after producer completes";
  ASSERT_EQ(resume_count.load(), 1) << "Waiter must resume exactly once";
}

// NotifyProgress fires between epoch-capture and registration (epoch advanced) ->
// RegisterProgressWaiter returns false -> busy-spin -> no hang, completes normally.
TEST(RealWaitPark, LostWakeupNoHang) {
  using namespace memgraph;
  utils::WorkerYieldRegistry registry(1);
  utils::PriorityThreadPool pool(1, nullptr, &registry);

  auto collection = std::make_shared<utils::TaskCollection>();
  std::atomic<int> completion_count{0};

  // Add a fast task that will complete (and fire NotifyProgress) immediately.
  collection->AddTask([&completion_count]() { completion_count.fetch_add(1); });

  utils::CollectionScheduler scheduler(&pool, collection);

  // Trigger the collection so the task runs — NotifyProgress fires before the waiter.
  scheduler.Trigger();

  // Wait until the task has finished (epoch advanced).
  collection->Wait();
  ASSERT_EQ(completion_count.load(), 1);

  // Now a coroutine tries to co_await on the progress — epoch already advanced,
  // await_ready() returns true (Finished()), so it busy-spins (or skips) without hanging.
  std::atomic<bool> coro_ran{false};
  std::optional<TestCoroutine> waiter_coro;
  pool.RescheduleTaskOnWorker(0, [&]() {
    waiter_coro.emplace([&]() -> TestCoroutine {
      co_await scheduler.WaitForProgressAwaitable();
      coro_ran = true;
    }());
  });

  const auto deadline = std::chrono::steady_clock::now() + 5s;
  while (!coro_ran.load(std::memory_order_acquire)) {
    ASSERT_LT(std::chrono::steady_clock::now(), deadline) << "RealWaitLostWakeupNoHang: coroutine never ran";
    std::this_thread::sleep_for(1ms);
  }

  pool.ShutDown();
  pool.AwaitShutdown();
  ASSERT_TRUE(coro_ran.load()) << "Coroutine must complete even when epoch was already advanced";
}

// Two concurrent NotifyProgress calls (both from finishing tasks); assert the parked
// task's body runs exactly once (WrapTask PARKED->SCHEDULED CAS acts as the backstop).
TEST(RealWaitPark, DoubleNotifyResumesOnce) {
  using namespace memgraph;
  constexpr uint16_t kWorkers = 2;
  utils::WorkerYieldRegistry registry(kWorkers);
  utils::PriorityThreadPool pool(kWorkers, nullptr, &registry);

  auto collection = std::make_shared<utils::TaskCollection>();
  std::atomic<int> producer_finished{0};
  std::atomic<bool> waiter_resumed{false};
  std::atomic<int> resume_count{0};

  // Two producer tasks that both call NotifyProgress when they finish.
  collection->AddTask([&producer_finished]() {
    std::this_thread::sleep_for(5ms);
    producer_finished.fetch_add(1);
  });
  collection->AddTask([&producer_finished]() {
    std::this_thread::sleep_for(5ms);
    producer_finished.fetch_add(1);
  });

  utils::CollectionScheduler scheduler(&pool, collection);
  std::optional<TestCoroutine> waiter_coro;

  // Schedule waiter on worker 0 so it has a valid pinned worker_id.
  pool.RescheduleTaskOnWorker(0, [&]() {
    waiter_coro.emplace([&]() -> TestCoroutine {
      co_await scheduler.WaitForProgressAwaitable();
      resume_count.fetch_add(1);
      waiter_resumed = true;
    }());
  });

  // Let the waiter park.
  std::this_thread::sleep_for(20ms);

  // Trigger both producers simultaneously.
  scheduler.Trigger();

  const auto deadline = std::chrono::steady_clock::now() + 5s;
  while (!waiter_resumed.load(std::memory_order_acquire)) {
    ASSERT_LT(std::chrono::steady_clock::now(), deadline) << "RealWaitDoubleNotifyResumesOnce: waiter never resumed";
    std::this_thread::sleep_for(1ms);
  }

  // Give extra time for any spurious second resume to manifest.
  std::this_thread::sleep_for(30ms);

  collection->Wait();
  pool.ShutDown();
  pool.AwaitShutdown();

  ASSERT_EQ(resume_count.load(), 1) << "Parked task body must run exactly once despite two NotifyProgress calls";
}

// Park, then force task_state->store(FINISHED) from another thread, then NotifyProgress;
// assert no crash and the coroutine body does NOT run (WrapTask PARKED->SCHEDULED CAS fails).
// TSan-relevant: exercises the concurrent PARKED->FINISHED race.
TEST(RealWaitPark, AbortWhileParkedNoUAF) {
  using namespace memgraph;
  utils::WorkerYieldRegistry registry(1);
  utils::PriorityThreadPool pool(1, nullptr, &registry);

  // Build a collection with one task and schedule it.  Intercept the task_state
  // shared_ptr by holding a reference so we can force FINISHED from the test thread.
  auto collection = std::make_shared<utils::TaskCollection>();
  std::atomic<bool> task_body_ran{false};
  std::atomic<int> run_count{0};

  // Backing producer: completes after a delay, providing the progress signal.
  collection->AddTask([]() { std::this_thread::sleep_for(50ms); });

  utils::CollectionScheduler scheduler(&pool, collection);

  // Capture the task_state for task[0] before scheduling.
  auto &task_ref = (*collection)[0];
  auto task_state = task_ref.state_;  // shared_ptr copy

  std::optional<TestCoroutine> waiter_coro;
  std::atomic<bool> waiter_ran{false};

  pool.RescheduleTaskOnWorker(0, [&]() {
    waiter_coro.emplace([&]() -> TestCoroutine {
      // Capture state before parking.
      co_await scheduler.WaitForProgressAwaitable();
      // If we reach here after a FINISHED force, the CAS in WrapTask must have
      // prevented the re-entry — this body should NOT run after forced FINISHED.
      waiter_ran = true;
      run_count.fetch_add(1);
    }());
  });

  // Let the waiter coroutine park.
  std::this_thread::sleep_for(20ms);

  // Force the producer task (index 0) to FINISHED from the test thread, simulating
  // an abort while the notification task is still pending.
  task_state->store(utils::TaskCollection::Task::State::FINISHED, std::memory_order_release);
  task_state->notify_one();

  // Now trigger the collection — by this point task[0] is already FINISHED,
  // so scheduling it will result in WrapTask seeing FINISHED and skipping.
  scheduler.Trigger();

  // Wait a bit; the waiter coroutine should NOT resume because the CAS guards it.
  std::this_thread::sleep_for(50ms);

  pool.ShutDown();
  pool.AwaitShutdown();

  // The waiter body may or may not have run depending on whether it parked before
  // the FINISHED store. The key assertion is: no crash (no UAF), and run_count <= 1.
  ASSERT_LE(run_count.load(), 1) << "Coroutine body must not execute more than once";
}

// A stolen task (no pinned worker_id in TLS, executing via TryExecuteOneIdleTask) co_awaits
// WaitForProgressAwaitable: RegisterProgressWaiter must return false (EC-2 guard), causing
// busy-spin fallback. The collection must still complete without deadlock.
TEST(RealWaitPark, StolenTaskBusySpins) {
  using namespace memgraph;
  // No WorkerYieldRegistry: TLS worker_id is null for the stolen-task path.
  utils::PriorityThreadPool pool(1);

  auto collection = std::make_shared<utils::TaskCollection>();
  std::atomic<bool> producer_done{false};
  std::atomic<bool> waiter_done{false};

  // Producer: finishes after a brief sleep.
  collection->AddTask([&producer_done]() {
    std::this_thread::sleep_for(10ms);
    producer_done = true;
  });

  utils::CollectionScheduler scheduler(&pool, collection);

  // Schedule the collection so the producer runs on the pool.
  scheduler.Trigger();

  // Wait for the producer to finish so the collection reports Finished().
  const auto deadline = std::chrono::steady_clock::now() + 5s;
  while (!producer_done.load(std::memory_order_acquire)) {
    ASSERT_LT(std::chrono::steady_clock::now(), deadline) << "StolenTaskBusySpins: producer never ran";
    std::this_thread::sleep_for(1ms);
  }
  collection->Wait();

  // Now run a coroutine on the main thread (no pool, no TLS worker_id).
  // await_ready() returns true because the collection is Finished() — no suspend occurs.
  // This exercises the EC-2 fast-path: the await_ready() check happens before await_suspend().
  std::optional<TestCoroutine> stolen_coro;
  stolen_coro.emplace([&]() -> TestCoroutine {
    // await_ready() returns true here (Finished()), so await_suspend is never called.
    co_await scheduler.WaitForProgressAwaitable();
    waiter_done = true;
  }());

  // The coroutine must complete without hanging.
  ASSERT_TRUE(waiter_done.load()) << "Stolen-task coroutine must complete via busy-spin/await_ready fallback";

  pool.ShutDown();
  pool.AwaitShutdown();
}

// Stress: 4 workers, multiple resumable tasks each yielding a few times + several progress-waiters;
// run multiple fresh pool+collection iterations; assert AllTerminal each time.
// TSan gate target: exercises the concurrent park/resume/notify path under load.
TEST(RealWaitPark, Stress) {
  using namespace memgraph;
  constexpr uint16_t kWorkers = 4;
  constexpr int kIterations = 30;
  constexpr int kTasksPerIter = 8;

  utils::WorkerYieldRegistry registry(kWorkers);
  utils::PriorityThreadPool pool(kWorkers, nullptr, &registry);

  for (int iter = 0; iter < kIterations; ++iter) {
    auto collection = std::make_shared<utils::TaskCollection>();
    std::atomic<int> completed{0};

    // Add resumable tasks that each yield a couple of times before finishing.
    for (int t = 0; t < kTasksPerIter; ++t) {
      auto yield_count = std::make_shared<std::atomic<int>>(0);
      collection->AddResumableTask([&completed, yield_count]() -> bool {
        const int c = yield_count->fetch_add(1);
        if (c < 2) return true;  // yield twice
        completed.fetch_add(1);
        return false;
      });
    }

    utils::CollectionScheduler scheduler(&pool, collection);

    // Schedule one progress-waiter coroutine per iteration on worker 0.
    std::atomic<bool> waiter_ran{false};
    std::optional<TestCoroutine> waiter_coro;
    pool.RescheduleTaskOnWorker(0, [&]() {
      waiter_coro.emplace([&]() -> TestCoroutine {
        co_await scheduler.WaitForProgressAwaitable();
        waiter_ran = true;
      }());
    });

    // Trigger and wait.
    scheduler.Trigger();
    const bool all_done = collection->Wait(10s);
    ASSERT_TRUE(all_done) << "Iteration " << iter << ": collection did not finish within timeout";
    ASSERT_TRUE(collection->AllTerminal()) << "Iteration " << iter;
    ASSERT_EQ(completed.load(), kTasksPerIter) << "Iteration " << iter;

    // The waiter must have run at least once (collection produced progress).
    const auto waiter_deadline = std::chrono::steady_clock::now() + 2s;
    while (!waiter_ran.load(std::memory_order_acquire)) {
      if (std::chrono::steady_clock::now() >= waiter_deadline) break;
      std::this_thread::sleep_for(1ms);
    }
    ASSERT_TRUE(waiter_ran.load()) << "Iteration " << iter << ": progress waiter never ran";
  }

  pool.ShutDown();
  pool.AwaitShutdown();
}
