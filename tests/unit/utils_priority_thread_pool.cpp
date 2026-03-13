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
#include <memory>
#include <thread>

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
