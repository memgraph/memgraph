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
#include <map>
#include <memory>
#include <set>
#include <thread>
#include <utility>

#include <utils/priority_thread_pool.hpp>
#include <utils/worker_yield_signal.hpp>
#include "utils/synchronized.hpp"

using namespace std::chrono_literals;

// ---------------------------------------------------------------------------
// Basic pool behaviour tests (adapted from the v2 merged test)
// All closures use void(auto) / void(utils::Priority) — the merged TaskSignature.
// PriorityThreadPool ctor: (mixed_workers, hp_workers, init_cb=nullptr, registry=nullptr).
// ---------------------------------------------------------------------------

TEST(PriorityThreadPool, Basic) {
  using namespace memgraph;
  memgraph::utils::PriorityThreadPool pool{1, 1};

  utils::Synchronized<std::vector<int>> output;
  constexpr size_t n_tasks = 100;
  for (size_t i = 0; i < n_tasks; ++i) {
    pool.ScheduledAddTask([&, i](auto) { output->push_back(i); }, utils::Priority::LOW);
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
  memgraph::utils::PriorityThreadPool pool{1, 1};

  // Figure out which thread is the low/high
  std::atomic<std::thread::id> low_th = std::thread::id{0};
  pool.ScheduledAddTask(
      [&](auto) {
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
        [&, i](auto) {
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
        [&, i](auto) {
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
  memgraph::utils::PriorityThreadPool pool{1, 1};

  std::atomic_bool block{true};
  // Block mixed work thread and see if the high priority thread takes over
  pool.ScheduledAddTask(
      [&](auto) {
        while (block) block.wait(true);
      },
      utils::Priority::LOW);

  // Wait for the task to be scheduled
  std::this_thread::sleep_for(std::chrono::milliseconds(100));

  utils::Synchronized<std::vector<int>> output;
  constexpr size_t n_tasks = 100;
  for (size_t i = 0; i < n_tasks / 2; ++i) {
    pool.ScheduledAddTask([&, i](auto) { output->push_back(i); }, utils::Priority::LOW);
  }
  for (size_t i = n_tasks / 2; i < n_tasks; ++i) {
    pool.ScheduledAddTask([&, i](auto) { output->push_back(i); }, utils::Priority::HIGH);
  }

  // Wait for the HIGH priority tasks to finish
  while (output->size() < n_tasks / 2) {
    std::this_thread::sleep_for(std::chrono::milliseconds(10));
  }

  // Check if only the HIGH priority tasks were executed and in order
  output.WithLock([](const auto &output) {
    ASSERT_EQ(output[0], n_tasks / 2);
    ASSERT_TRUE(std::is_sorted(output.begin(), output.end()));
  });

  // Unblock mixed work thread and close
  block = false;
  block.notify_one();
  pool.ShutDown();
  pool.AwaitShutdown();
}

TEST(PriorityThreadPool, MultipleLow) {
  using namespace memgraph;
  constexpr auto kLP = 8;
  memgraph::utils::PriorityThreadPool pool{kLP, 1};

  std::atomic_bool block{true};
  // Block all mixed work thread and see if the high priority thread takes over
  for (int i = 0; i < kLP; ++i) {
    pool.ScheduledAddTask(
        [&](auto) {
          while (block) block.wait(true);
        },
        utils::Priority::LOW);
  }

  // Wait for the task to be scheduled
  std::this_thread::sleep_for(std::chrono::milliseconds(100));

  utils::Synchronized<std::vector<int>> output;
  constexpr size_t n_tasks = 100;
  for (size_t i = 0; i < n_tasks / 2; ++i) {
    pool.ScheduledAddTask([&, i](auto) { output->push_back(i); }, utils::Priority::LOW);
  }
  for (size_t i = n_tasks / 2; i < n_tasks; ++i) {
    pool.ScheduledAddTask([&, i](auto) { output->push_back(i); }, utils::Priority::HIGH);
  }

  // Wait for the HIGH priority tasks to finish
  while (output->size() < n_tasks / 2) {
    std::this_thread::sleep_for(std::chrono::milliseconds(10));
  }

  // Check if only the HIGH priority tasks were executed and in order
  output.WithLock([](const auto &output) {
    ASSERT_EQ(output[0], n_tasks / 2);
    ASSERT_TRUE(std::is_sorted(output.begin(), output.end()));
  });

  // Unblock mixed work thread and close
  block = false;
  block.notify_one();
  pool.ShutDown();
  pool.AwaitShutdown();
}

// ---------------------------------------------------------------------------
// TaskCollection tests — adapted to merged API.
// WrapTask returns ResumableTaskSignature = move_only_function<bool()>.
// Call it as: wrapped_task()  (no argument, returns bool).
// AddTask accepts TaskSignature = move_only_function<void(utils::Priority)>.
// ---------------------------------------------------------------------------

TEST(TaskCollection, BasicAddAndSize) {
  using namespace memgraph;
  memgraph::utils::TaskCollection collection;

  ASSERT_EQ(collection.Size(), 0);

  collection.AddTask([](auto) {});
  ASSERT_EQ(collection.Size(), 1);

  collection.AddTask([](auto) {});
  collection.AddTask([](auto) {});
  ASSERT_EQ(collection.Size(), 3);
}

TEST(TaskCollection, BasicWait) {
  using namespace memgraph;
  memgraph::utils::TaskCollection collection;

  std::atomic<int> counter{0};
  constexpr int num_tasks = 5;

  for (int i = 0; i < num_tasks; ++i) {
    collection.AddTask([&counter](auto) {
      std::this_thread::sleep_for(std::chrono::milliseconds(10));
      counter.fetch_add(1);
    });
  }

  // Execute tasks manually to test Wait().
  // WrapTask(i) returns ResumableTaskSignature = bool(); call with no args.
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
    collection.AddTask([&counter](auto) {
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
  collection.AddTask([&counter](auto) { counter.fetch_add(1); });
  collection.AddTask([&counter](auto) { counter.fetch_add(1); });
  collection.AddTask([&counter](auto) { counter.fetch_add(1); });

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
  collection.AddTask([&started](auto) {
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

  collection.AddTask([](auto) {});

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

TEST(TaskCollection, ThreadPoolIntegration) {
  using namespace memgraph;
  memgraph::utils::PriorityThreadPool pool{2, 1};
  memgraph::utils::TaskCollection collection;

  std::atomic<int> counter{0};
  constexpr int num_tasks = 20;

  for (int i = 0; i < num_tasks; ++i) {
    collection.AddTask([&counter](auto) {
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
  memgraph::utils::PriorityThreadPool pool{4, 2};
  memgraph::utils::TaskCollection collection;

  std::atomic<int> counter{0};
  constexpr int num_tasks = 50;

  for (int i = 0; i < num_tasks; ++i) {
    collection.AddTask([&counter](auto) {
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
  memgraph::utils::PriorityThreadPool pool{1, 1};
  memgraph::utils::TaskCollection collection;

  std::atomic<int> counter{0};
  constexpr int num_tasks = 15;

  std::mutex thread_counter_mutex;
  std::map<std::thread::id, int> thread_counter;

  for (int i = 0; i < num_tasks; ++i) {
    collection.AddTask([&counter, &thread_counter_mutex, &thread_counter](auto) {
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

  // Check if the tasks were executed by more than one thread (pool + caller)
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
      collection.AddTask([&exception_count](auto) {
        exception_count.fetch_add(1);
        throw std::runtime_error("Test exception");
      });
    } else {
      collection.AddTask([&success_count](auto) { success_count.fetch_add(1); });
    }
  }

  // WaitOrSteal should handle exceptions properly
  try {
    collection.WaitOrSteal();
  } catch (const std::runtime_error &) {
    // Expected exception - this stops execution of remaining tasks
  }

  int total_executed = success_count.load() + exception_count.load();
  ASSERT_GT(total_executed, 0);
  ASSERT_LE(total_executed, num_tasks);
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
      collection.AddTask([&exception_count](auto) {
        exception_count.fetch_add(1);
        throw std::runtime_error("Test exception");
      });
    } else {
      collection.AddTask([&success_count](auto) { success_count.fetch_add(1); });
    }
  }

  // Execute tasks individually to handle exceptions properly
  for (size_t i = 0; i < collection.Size(); ++i) {
    try {
      auto wrapped_task = collection.WrapTask(i);
      wrapped_task();  // ResumableTaskSignature = bool(); no args
    } catch (const std::runtime_error &) {
      // Expected exception - continue with next task
    }
  }

  ASSERT_EQ(success_count.load() + exception_count.load(), num_tasks);
  ASSERT_EQ(success_count.load(), 6);
  ASSERT_EQ(exception_count.load(), 4);
}

TEST(TaskCollection, TaskStateTransitions) {
  using namespace memgraph;
  memgraph::utils::TaskCollection collection;

  std::atomic<int> execution_count{0};
  collection.AddTask([&execution_count](auto) { execution_count.fetch_add(1); });

  // Test that task starts in IDLE state
  auto &task = collection[0];
  ASSERT_EQ(task.state_->load(), memgraph::utils::TaskCollection::Task::State::IDLE);

  // Wrap and execute task
  auto wrapped_task = collection.WrapTask(0);
  wrapped_task();  // ResumableTaskSignature = bool()

  // Task should be in FINISHED state
  ASSERT_EQ(task.state_->load(), memgraph::utils::TaskCollection::Task::State::FINISHED);
  ASSERT_EQ(execution_count.load(), 1);
}

TEST(TaskCollection, MultipleExecutionsPrevented) {
  using namespace memgraph;
  memgraph::utils::TaskCollection collection;

  std::atomic<int> execution_count{0};
  collection.AddTask([&execution_count](auto) { execution_count.fetch_add(1); });

  auto wrapped_task = collection.WrapTask(0);

  // Execute task multiple times - should only execute once due to CAS in WrapTask
  wrapped_task();
  wrapped_task();
  wrapped_task();

  ASSERT_EQ(execution_count.load(), 1);

  auto &task = collection[0];
  ASSERT_EQ(task.state_->load(), memgraph::utils::TaskCollection::Task::State::FINISHED);
}

TEST(TaskCollection, LargeTaskSet) {
  using namespace memgraph;
  memgraph::utils::PriorityThreadPool pool{8, 2};
  memgraph::utils::TaskCollection collection;

  std::atomic<int> counter{0};
  constexpr int num_tasks = 1000;

  for (int i = 0; i < num_tasks; ++i) {
    collection.AddTask([&counter](auto) { counter.fetch_add(1); });
  }

  pool.ScheduledCollection(collection);

  collection.Wait();
  ASSERT_EQ(counter.load(), num_tasks);

  pool.ShutDown();
  pool.AwaitShutdown();
}

// ---------------------------------------------------------------------------
// Yield-signal tests (adapted from p3 ref test).
// Ctor: p3 was (n, init_cb, registry); merged is (mixed, hp, init_cb, registry).
// Adapted: add hp=0 as the 2nd arg (no hp workers needed for these tests).
// Closures: p3 used void(); merged uses void(Priority) → add (auto) or (utils::Priority).
// ---------------------------------------------------------------------------

TEST(PriorityThreadPool, YieldFlag_NotSetWhenOnlyLowPriority) {
  using namespace memgraph;
  constexpr uint16_t kWorkers = 2;
  memgraph::utils::WorkerYieldRegistry registry(kWorkers);
  // merged ctor: (mixed=kWorkers, hp=0, init_cb=nullptr, registry=&registry)
  memgraph::utils::PriorityThreadPool pool(kWorkers, 1, nullptr, &registry);

  std::atomic<int> completed{0};
  std::atomic<int> saw_yield_requested{0};
  constexpr int n_tasks = 50;

  for (int i = 0; i < n_tasks; ++i) {
    pool.ScheduledAddTask(
        [&](auto) {
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
  memgraph::utils::PriorityThreadPool pool(kWorkers, 1, nullptr, &registry);

  std::atomic<bool> worker0_done{false};
  std::atomic<bool> worker0_saw_yield{false};

  pool.RescheduleTaskOnWorker(0, [&](auto) {
    for (int i = 0; i < 2'000'000; ++i) {
      auto *sig = utils::WorkerYieldRegistry::GetCurrentYieldSignal();
      if (sig && sig->load(std::memory_order_acquire)) worker0_saw_yield = true;
    }
    worker0_done = true;
  });

  std::this_thread::sleep_for(1ms);
  // Add task only to worker 1; worker 0's yield signal must not be set
  pool.RescheduleTaskOnWorker(1, [](auto) {});

  while (!worker0_done) std::this_thread::sleep_for(1ms);
  ASSERT_FALSE(worker0_saw_yield.load()) << "Yield must not be set on worker 0 when HP is added only to worker 1";

  pool.ShutDown();
  pool.AwaitShutdown();
}

TEST(PriorityThreadPool, YieldFlag_Stress) {
  using namespace memgraph;
  constexpr uint16_t kWorkers = 4;
  memgraph::utils::WorkerYieldRegistry registry(kWorkers);
  memgraph::utils::PriorityThreadPool pool(kWorkers, 1, nullptr, &registry);

  constexpr int kIterations = 30;
  int requested_yields_seen = 0;

  for (int iter = 0; iter < kIterations; ++iter) {
    std::atomic<bool> lp_running{false};
    std::atomic<bool> lp_saw_yield{false};
    std::atomic<bool> hp_ran{false};

    pool.ScheduledAddTask(
        [&](auto) {
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

    pool.ScheduledAddTask([&](auto) { hp_ran = true; }, utils::Priority::HIGH);

    while (!hp_ran) std::this_thread::sleep_for(1ms);
    if (lp_saw_yield) ++requested_yields_seen;
  }

  pool.ShutDown();
  pool.AwaitShutdown();

  ASSERT_GT(requested_yields_seen, 0) << "At least one requested yield must be observed";
}

// Both workers blocked → submit tasks → release one worker.
// The released worker must execute all tasks: its own + stolen from the still-blocked worker.
TEST(PriorityThreadPool, WorkStealing) {
  using namespace memgraph;
  constexpr uint16_t kWorkers = 2;
  memgraph::utils::PriorityThreadPool pool{kWorkers, 1};

  // Identify each worker's thread ID via pinned tasks.
  std::array<std::atomic<std::thread::id>, kWorkers> worker_tids;
  std::atomic<int> ids_ready{0};
  for (uint16_t i = 0; i < kWorkers; ++i) {
    pool.RescheduleTaskOnWorker(i, [&, i](auto) {
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
  pool.RescheduleTaskOnWorker(0, [&](auto) {
    w0_running = true;
    while (!release_w0) release_w0.wait(false);
  });
  pool.RescheduleTaskOnWorker(1, [&](auto) {
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
        [&](auto) {
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
  memgraph::utils::PriorityThreadPool pool(1, 1, nullptr, &registry);

  std::atomic<bool> second_saw_signal{false};
  std::atomic<bool> done{false};

  // Task 1: sets yield signal for worker 0, then completes normally.
  pool.ScheduledAddTask([&](auto) { registry.RequestYieldForWorker(0); }, utils::Priority::LOW);

  // Task 2: runs after task 1; signal must be cleared before it starts.
  pool.ScheduledAddTask(
      [&](auto) {
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

// ShutDown + AwaitShutdown must complete without hanging.
TEST(PriorityThreadPool, ShutdownNoHang) {
  using namespace memgraph;

  // Empty pool shutdown.
  {
    memgraph::utils::PriorityThreadPool pool{4, 1};
    pool.ShutDown();
    pool.AwaitShutdown();
  }

  // Shutdown with tasks already executing.
  {
    memgraph::utils::PriorityThreadPool pool{2, 1};
    std::atomic<int> started{0};
    constexpr int n = 10;
    for (int i = 0; i < n; ++i) {
      pool.ScheduledAddTask(
          [&](auto) {
            started.fetch_add(1);
            std::this_thread::sleep_for(2ms);
          },
          utils::Priority::LOW);
    }
    while (started.load() == 0) std::this_thread::sleep_for(1ms);
    pool.ShutDown();
    pool.AwaitShutdown();
    // Reaching here without timeout is the assertion.
  }
}

// ---------------------------------------------------------------------------
// ScheduleResumableTask tests (adapted from p3 ref test).
// ResumableTaskSignature = move_only_function<bool()> — identical in both versions.
// Only ctor adaptation needed: (n, init_cb, registry) → (n, 0, init_cb, registry).
// ---------------------------------------------------------------------------

// Task returns false immediately — must execute exactly once.
TEST(PriorityThreadPool, ResumableTask_CompletesImmediately) {
  using namespace memgraph;
  memgraph::utils::PriorityThreadPool pool{1, 1};

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

// Task yields once (sets signal, returns true), then completes.
// Both runs must land on the same worker (pinned reschedule).
TEST(PriorityThreadPool, ResumableTask_YieldsOnceThenCompletes) {
  using namespace memgraph;
  memgraph::utils::WorkerYieldRegistry registry(1);
  memgraph::utils::PriorityThreadPool pool(1, 1, nullptr, &registry);

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
  memgraph::utils::PriorityThreadPool pool(1, 1, nullptr, &registry);

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
  memgraph::utils::PriorityThreadPool pool{1, 1};

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
// NOTE: the p3 ref had a `ResumableTask_HighPriority` test. It does not apply to this merged pool:
// p3 had no HP worker threads, so HIGH there meant only priority ordering. Here, HP resumable tasks are
// FORBIDDEN (ScheduleResumableTask MG_ASSERTs Priority::LOW) — an HP worker could steal a resumable task
// and run it without a published worker id, breaking same-worker TLS-pinned resume. The yield-once-then-
// complete behavior is covered at LOW priority by ResumableTask_YieldsOnceThenCompletes.

// Resumable task self-parks via CurrentResumableTask::RegisterWaiter.
// The pool must not re-run it until the event fires.
// After the event, it should resume on the same worker (pinned).
TEST(PriorityThreadPool, ResumableTask_SelfParksUntilEventResume) {
  using namespace memgraph;
  utils::WorkerYieldRegistry registry(1);
  utils::PriorityThreadPool pool(1, 1, nullptr, &registry);
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
          return false;  // returning false signals WrapTask to check WasParked()
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
TEST(PriorityThreadPool, ResumableTask_ShutdownMidFlight) {
  using namespace memgraph;
  memgraph::utils::PriorityThreadPool pool{1, 1};

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

// ---------------------------------------------------------------------------
// RescheduleTaskOnWorker / pinned-continuation tests (adapted from p3 ref test).
// p3 used void() closures; merged uses void(Priority) → add (auto) param.
// ---------------------------------------------------------------------------

TEST(PriorityThreadPool, ScheduledAddTaskOnWorker_PinnedRunsOnDesignatedWorker) {
  using namespace memgraph;
  constexpr uint16_t kWorkers = 2;
  memgraph::utils::PriorityThreadPool pool{kWorkers, 1};

  std::array<std::atomic<std::thread::id>, kWorkers> worker_tids;
  std::atomic<int> ids_ready{0};
  for (uint16_t i = 0; i < kWorkers; ++i) {
    pool.RescheduleTaskOnWorker(i, [&, i](auto) {
      worker_tids[i] = std::this_thread::get_id();
      ids_ready.fetch_add(1);
    });
  }
  while (ids_ready.load() != kWorkers) std::this_thread::sleep_for(1ms);
  ASSERT_NE(worker_tids[0].load(), worker_tids[1].load());

  // Schedule another pinned task on worker 0 and verify it runs on worker 0's thread
  std::atomic<bool> pinned_ran_on_worker0{false};
  pool.RescheduleTaskOnWorker(
      0, [&](auto) { pinned_ran_on_worker0 = (std::this_thread::get_id() == worker_tids[0].load()); });

  while (!pinned_ran_on_worker0) std::this_thread::sleep_for(1ms);
  ASSERT_TRUE(pinned_ran_on_worker0);

  pool.ShutDown();
  pool.AwaitShutdown();
}

TEST(PriorityThreadPool, ScheduledAddTaskOnWorker_ReschedulingRunsOnSameWorker) {
  using namespace memgraph;
  constexpr uint16_t kWorkers = 2;
  memgraph::utils::PriorityThreadPool pool{kWorkers, 1};

  std::atomic<std::thread::id> first_task_thread_id;
  std::atomic<std::thread::id> continuation_thread_id;
  std::atomic<bool> continuation_done{false};

  // Schedule on worker 0 a task that reschedules a continuation on worker 0
  pool.RescheduleTaskOnWorker(0, [&, &pool = pool](auto) {
    first_task_thread_id = std::this_thread::get_id();
    pool.RescheduleTaskOnWorker(0, [&](auto) {
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
  memgraph::utils::PriorityThreadPool pool{kWorkers, 1};

  std::atomic<bool> worker1_busy{true};
  std::atomic<std::thread::id> worker0_thread_id;
  std::atomic<bool> pinned_ran_on_worker0{false};

  // Keep worker 1 busy so it would otherwise try to steal from worker 0
  pool.RescheduleTaskOnWorker(1, [&](auto) {
    while (worker1_busy) worker1_busy.wait(true);
  });

  std::this_thread::sleep_for(10ms);

  // Worker 0 runs this task, then schedules a pinned continuation on worker 0
  pool.RescheduleTaskOnWorker(0, [&, &pool = pool](auto) {
    worker0_thread_id = std::this_thread::get_id();
    pool.RescheduleTaskOnWorker(
        0, [&](auto) { pinned_ran_on_worker0 = (std::this_thread::get_id() == worker0_thread_id.load()); });
  });

  while (!pinned_ran_on_worker0) std::this_thread::sleep_for(1ms);
  ASSERT_TRUE(pinned_ran_on_worker0) << "Pinned continuation must run on same worker, not be stolen";

  worker1_busy = false;
  worker1_busy.notify_one();
  pool.ShutDown();
  pool.AwaitShutdown();
}

// ---------------------------------------------------------------------------
// TaskCollection resumable-task integration tests (adapted from p3 ref test).
// These use AddResumableTask (already bool()-returning) + pool.ScheduledCollection.
// ---------------------------------------------------------------------------

TEST(TaskCollection, ResumableCollectionTaskYieldsThenCompletes) {
  using namespace memgraph;
  utils::WorkerYieldRegistry registry(1);
  utils::PriorityThreadPool pool(1, 1, nullptr, &registry);

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
  utils::PriorityThreadPool pool(1, 1, nullptr, &registry);
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

// ---------------------------------------------------------------------------
// CollectionScheduler coroutine awaitable tests (ported from p3 ref test verbatim).
// The coroutine infra (TestCoroutine, AwaitCollectionProgress*, WorkerResumeAwaitable,
// AwaitWorkerResumeEvent) is unchanged between p3 and merged — no API delta here.
// Ctor: (n, init_cb, registry) → (n, 0, init_cb, registry).
// ---------------------------------------------------------------------------

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
  utils::PriorityThreadPool pool(1, 1, nullptr, &registry);

  auto collection = std::make_shared<utils::TaskCollection>();
  std::atomic<bool> task_started{false};
  collection->AddTask([&task_started](auto) {
    task_started = true;
    std::this_thread::sleep_for(10ms);
  });

  utils::CollectionScheduler scheduler(&pool, collection);
  std::atomic<bool> coroutine_started{false};
  std::atomic<bool> coroutine_resumed{false};
  std::optional<TestCoroutine> waiter;

  pool.RescheduleTaskOnWorker(0, [&](auto) {
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
  utils::PriorityThreadPool pool(1, 1, nullptr, &registry);

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

  pool.RescheduleTaskOnWorker(0, [&](auto) {
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

TEST(WorkerResumeEvent, NotifyAllResumesWaitersOnOriginalWorkers) {
  using namespace memgraph;
  utils::WorkerYieldRegistry registry(2);
  utils::PriorityThreadPool pool(2, 1, nullptr, &registry);
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

  pool.RescheduleTaskOnWorker(0, [&](auto) {
    worker0_thread_id = std::this_thread::get_id();
    waiter0.emplace(AwaitWorkerResumeEvent(event, pool, waiter0_resumed, waiter0_resumed_thread_id));
    waiter0_started = true;
  });
  pool.RescheduleTaskOnWorker(1, [&](auto) {
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
