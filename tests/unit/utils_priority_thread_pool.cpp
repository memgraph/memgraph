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
#include <thread>

#include <utils/priority_thread_pool.hpp>
#include "utils/synchronized.hpp"

using namespace std::chrono_literals;

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

// TaskCollection Tests
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

  // Execute tasks manually to test Wait()
  for (size_t i = 0; i < collection.Size(); ++i) {
    auto wrapped_task = collection.WrapTask(i);
    wrapped_task(utils::Priority::LOW);
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
    wrapped_task(utils::Priority::LOW);
  }

  // WaitOrSteal should execute all tasks and wait for completion
  collection.WaitOrSteal();
  ASSERT_EQ(counter.load(), num_tasks);
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
      collection.AddTask([&exception_count](auto) {
        exception_count.fetch_add(1);
        throw std::runtime_error("Test exception");
      });
    } else {
      collection.AddTask([&success_count](auto) { success_count.fetch_add(1); });
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
      wrapped_task(utils::Priority::LOW);
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
  collection.AddTask([&execution_count](auto) { execution_count.fetch_add(1); });

  // Test that task starts in IDLE state
  auto &task = collection[0];
  ASSERT_EQ(task.state_->load(), memgraph::utils::TaskCollection::Task::State::IDLE);

  // Wrap and execute task
  auto wrapped_task = collection.WrapTask(0);
  wrapped_task(utils::Priority::LOW);

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

  // Execute task multiple times - should only execute once
  wrapped_task(utils::Priority::LOW);
  wrapped_task(utils::Priority::LOW);
  wrapped_task(utils::Priority::LOW);

  ASSERT_EQ(execution_count.load(), 1);

  // Task should be in FINISHED state
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

  // Schedule collection to thread pool
  pool.ScheduledCollection(collection);

  // Wait for all tasks to complete
  collection.Wait();
  ASSERT_EQ(counter.load(), num_tasks);

  pool.ShutDown();
  pool.AwaitShutdown();
}

// Verifies the NB-3 admission-gate contract for HasPendingWork():
//   - productive=false tasks (admission re-posts) never increment productive_pending_,
//     so they cannot hold the gate open even when queued.
//   - productive=true tasks (real queries) immediately open the gate on submission.
TEST(PriorityThreadPool, HasPendingWorkCountsProductiveNotAdmissionReposts) {
  using namespace memgraph;

  // 4 mixed workers, 1 HP worker.
  constexpr int kN = 4;
  utils::PriorityThreadPool pool{kN, 1};

  std::atomic<int> occupied{0};
  std::atomic<bool> release{false};

  // Occupy all kN mixed workers with non-productive tasks (productive=false) so that
  // productive_pending_ stays at zero throughout the occupancy phase.
  // Submit ONE blocking occupier per worker, and wait (with real sleeps so the worker threads get
  // a timeslice) for it to actually start before submitting the next. Because the already-started
  // occupiers are blocked on `release`, a fresh occupier can only land on a still-free worker, so
  // this deterministically fills all kN distinct mixed workers. Bounded so a broken pool fails fast.
  for (int k = 1; k <= kN; ++k) {
    pool.ScheduledAddTask(
        [&](utils::Priority) {
          occupied.fetch_add(1, std::memory_order_acq_rel);
          while (!release.load(std::memory_order_acquire)) {
            std::this_thread::sleep_for(std::chrono::milliseconds(1));
          }
        },
        utils::Priority::LOW,
        /*productive=*/false);
    for (int waited = 0; occupied.load(std::memory_order_acquire) < k && waited < 5000; ++waited) {
      std::this_thread::sleep_for(std::chrono::milliseconds(1));
    }
    ASSERT_EQ(occupied.load(std::memory_order_acquire), k) << "worker " << k << " did not start in time";
  }

  // All kN workers are now spinning on `release`.  No productive task has ever been
  // submitted, so productive_pending_ must be zero and the gate must be closed.
  ASSERT_FALSE(pool.HasPendingWork());

  // Submit an admission re-post (productive=false).  It queues behind a busy worker
  // but must NOT open the gate — push() skips the counter increment for non-productive
  // tasks (priority_thread_pool.cpp Worker::push, guarded by `productive` flag).
  pool.ScheduledAddTask([](utils::Priority) {}, utils::Priority::LOW, /*productive=*/false);
  ASSERT_FALSE(pool.HasPendingWork());

  // Submit a real productive task (productive=true).  push() increments productive_pending_
  // before returning, so HasPendingWork() must be true immediately after this call.
  std::atomic<bool> productive_ran{false};
  pool.ScheduledAddTask([&](utils::Priority) { productive_ran.store(true, std::memory_order_release); },
                        utils::Priority::LOW,
                        /*productive=*/true);
  ASSERT_TRUE(pool.HasPendingWork());

  // Release all occupiers so the pool can drain.  With release==true, any extra
  // non-productive occupiers still in queues will also exit immediately when picked up.
  release.store(true, std::memory_order_release);

  // Wait (bounded) for the productive task to complete.  The pop_task lambda in the
  // worker decrements productive_pending_ BEFORE calling the task body, so by the
  // time we observe productive_ran==true via acquire, the decrement is already visible
  // (sequenced-before + release/acquire pairing).
  constexpr int kMaxPollIter = 100'000;
  for (int i = 0; i < kMaxPollIter && !productive_ran.load(std::memory_order_acquire); ++i) {
    std::this_thread::yield();
  }
  ASSERT_TRUE(productive_ran.load(std::memory_order_acquire)) << "productive task never ran within poll bound";
  // productive_pending_ was decremented at pop (before task body ran), so the gate
  // must be closed again now that we have the acquire visibility on productive_ran.
  ASSERT_FALSE(pool.HasPendingWork());

  pool.ShutDown();
  pool.AwaitShutdown();
}

// ---- Park subsystem tests ----

// Test 1: ParkAdmission then WakeOneParked re-injects the task and it runs on a worker.
TEST(PriorityThreadPool, ParkAdmission_WakeOneParked_Single) {
  using namespace memgraph;
  utils::PriorityThreadPool pool{2, 1};

  std::atomic<bool> ran{false};
  auto deadline = std::chrono::steady_clock::now() + std::chrono::hours(1);

  pool.ParkAdmission([&](utils::Priority) { ran.store(true, std::memory_order_release); }, 1000, deadline);

  pool.WakeOneParked();

  for (int w = 0; !ran.load(std::memory_order_acquire) && w < 5000; ++w) {
    std::this_thread::sleep_for(std::chrono::milliseconds(1));
  }
  ASSERT_TRUE(ran.load(std::memory_order_acquire));

  pool.ShutDown();
  pool.AwaitShutdown();
}

// Test 2: Monitor sweep re-injects a parked entry whose deadline is already in the past
// within approximately one monitor tick (~100 ms).
TEST(PriorityThreadPool, MonitorSweep_PastDeadlineReinjected) {
  using namespace memgraph;
  utils::PriorityThreadPool pool{2, 1};

  std::atomic<bool> ran{false};
  // Deadline already expired — the next monitor tick must pick this up.
  auto past_deadline = std::chrono::steady_clock::now() - std::chrono::milliseconds(1);

  pool.ParkAdmission([&](utils::Priority) { ran.store(true, std::memory_order_release); }, 1000, past_deadline);

  // Monitor fires every 100 ms; wait up to 400 ms (4 ticks).
  for (int w = 0; !ran.load(std::memory_order_acquire) && w < 40; ++w) {
    std::this_thread::sleep_for(std::chrono::milliseconds(10));
  }
  ASSERT_TRUE(ran.load(std::memory_order_acquire));

  pool.ShutDown();
  pool.AwaitShutdown();
}

// Test 3: Multiple parked admissions — WakeOneParked drains one per call, oldest first (FIFO).
// We wake one task, wait for it to complete, then wake the next, verifying ordering via an
// atomic sequence counter checked inside each task body.
TEST(PriorityThreadPool, WakeOneParked_FIFO) {
  using namespace memgraph;
  utils::PriorityThreadPool pool{2, 1};

  std::atomic<int> run_seq{0};
  auto deadline = std::chrono::steady_clock::now() + std::chrono::hours(1);

  // Park 3 tasks in order; the deque front is the oldest (index 0).
  for (int i = 0; i < 3; ++i) {
    pool.ParkAdmission(
        [&, expected = i](utils::Priority) {
          EXPECT_EQ(run_seq.load(std::memory_order_acquire), expected);
          run_seq.fetch_add(1, std::memory_order_release);
        },
        static_cast<utils::PriorityThreadPool::TaskID>(1000 - i),
        deadline);
  }

  // Wake one at a time, waiting for each to complete before waking the next.
  for (int i = 0; i < 3; ++i) {
    pool.WakeOneParked();
    for (int w = 0; run_seq.load(std::memory_order_acquire) < i + 1 && w < 5000; ++w) {
      std::this_thread::sleep_for(std::chrono::milliseconds(1));
    }
    ASSERT_EQ(run_seq.load(std::memory_order_acquire), i + 1);
  }

  // An extra WakeOneParked on an empty deque must be a silent no-op.
  pool.WakeOneParked();

  pool.ShutDown();
  pool.AwaitShutdown();
}

// Test 4: ShouldParkAdmission returns false when productive_pending_ == 0 (nothing to yield to).
TEST(PriorityThreadPool, ShouldParkAdmission_FalseWithNoProductiveWork) {
  using namespace memgraph;
  utils::PriorityThreadPool pool{2, 1};

  // No productive tasks submitted yet.
  ASSERT_FALSE(pool.ShouldParkAdmission());

  // Non-productive task does not increment productive_pending_ — gate must stay closed.
  pool.ScheduledAddTask([](utils::Priority) {}, utils::Priority::LOW, /*productive=*/false);
  std::this_thread::sleep_for(std::chrono::milliseconds(10));
  ASSERT_FALSE(pool.ShouldParkAdmission());

  pool.ShutDown();
  pool.AwaitShutdown();
}

// Test 5: ShutDown drain — every parked continuation runs exactly once.
TEST(PriorityThreadPool, ShutDown_DrainsParkedAdmissions) {
  using namespace memgraph;
  utils::PriorityThreadPool pool{2, 1};

  constexpr int kN = 5;
  std::atomic<int> ran{0};
  auto deadline = std::chrono::steady_clock::now() + std::chrono::hours(1);

  for (int i = 0; i < kN; ++i) {
    pool.ParkAdmission([&](utils::Priority) { ran.fetch_add(1, std::memory_order_acq_rel); },
                       static_cast<utils::PriorityThreadPool::TaskID>(1000 - i),
                       deadline);
  }

  // ShutDown runs the drain synchronously in the caller thread before stopping workers;
  // every parked continuation must have run by the time ShutDown returns.
  pool.ShutDown();

  ASSERT_EQ(ran.load(std::memory_order_acquire), kN);

  pool.AwaitShutdown();
}

// ==================================================================================
// WakeAllParked tests (Tests 6-9)
// ==================================================================================

// Helper for WakeAllParked_StillContendedReparkPattern (Test 8).
// Each instance represents one parked admission slot that re-parks itself while
// `still_contended` is true, simulating a stalled acquisition gate. `repark_count`
// guards against an infinite re-park loop: if `kMaxReparks` is reached the task
// completes regardless of the flag, so a stuck-flag bug cannot hang the suite.
//
// Memory-ordering note: `parked_back` is incremented AFTER ParkAdmission returns,
// which is sequenced-before the atomic store; the main thread's acquire on
// `parked_back` therefore happens-after the deque write, making the wait on
// `parked_back >= N` a valid gate for the subsequent WakeAllParked call.
namespace {
struct ParkReparker : std::enable_shared_from_this<ParkReparker> {
  std::atomic<bool> *still_contended{nullptr};
  std::atomic<int> *parked_back{nullptr};
  std::atomic<int> *completed{nullptr};
  memgraph::utils::PriorityThreadPool *pool{nullptr};
  memgraph::utils::PriorityThreadPool::TaskID task_id{0};
  std::chrono::steady_clock::time_point deadline{};

  static constexpr int kMaxReparks = 5;
  int repark_count{0};  // single-writer: only the task executing this slot modifies it

  memgraph::utils::TaskSignature MakeTask() {
    return [self = shared_from_this()](memgraph::utils::Priority) {
      const bool contended = self->still_contended->load(std::memory_order_acquire);
      if (contended && self->repark_count < kMaxReparks) {
        ++self->repark_count;
        // Re-park first; signal only after the entry is in the deque so the
        // main thread's parked_back wait is a true happens-before for the
        // subsequent WakeAllParked call.
        self->pool->ParkAdmission(self->MakeTask(), self->task_id, self->deadline);
        self->parked_back->fetch_add(1, std::memory_order_release);
      } else {
        self->completed->fetch_add(1, std::memory_order_acq_rel);
      }
    };
  }
};
}  // namespace

// Test 6: WakeAllParked drains every parked entry in a single call.
// Cross-worker execution order is non-deterministic (tasks distribute across workers
// via ScheduledReAddTask's hot-thread dispatch), so only the total count and the
// post-drain empty-deque property are asserted — not per-task ordering.
TEST(PriorityThreadPool, WakeAllParked_ReinjectsAllOldestFirst) {
  using namespace memgraph;
  utils::PriorityThreadPool pool{2, 1};

  constexpr int kN = 5;
  std::atomic<int> ran{0};
  auto deadline = std::chrono::steady_clock::now() + std::chrono::hours(1);

  // Park kN admissions with distinct decreasing ids (oldest = highest id = highest
  // priority in the worker's max-heap, matching pool FIFO semantics).
  for (int i = 0; i < kN; ++i) {
    pool.ParkAdmission([&](utils::Priority) { ran.fetch_add(1, std::memory_order_acq_rel); },
                       static_cast<utils::PriorityThreadPool::TaskID>(1000 - i),
                       deadline);
  }

  pool.WakeAllParked();

  // Bounded wait: all kN tasks must run within 5 s.
  for (int w = 0; ran.load(std::memory_order_acquire) < kN && w < 5000; ++w) {
    std::this_thread::sleep_for(std::chrono::milliseconds(1));
  }
  ASSERT_EQ(ran.load(std::memory_order_acquire), kN);

  // Parked deque must now be empty: additional wakes must be silent no-ops.
  // A 20 ms pause lets in-flight worker scheduling settle before the no-op calls.
  std::this_thread::sleep_for(std::chrono::milliseconds(20));
  pool.WakeAllParked();
  pool.WakeOneParked();
  std::this_thread::sleep_for(std::chrono::milliseconds(20));
  // Counter must stay at kN — the no-op wakes must not trigger additional runs.
  ASSERT_EQ(ran.load(std::memory_order_acquire), kN);

  pool.ShutDown();
  pool.AwaitShutdown();
}

// Test 7: WakeAllParked on an empty parked deque must be a safe no-op.
TEST(PriorityThreadPool, WakeAllParked_EmptyIsNoOp) {
  using namespace memgraph;
  utils::PriorityThreadPool pool{2, 1};

  // Three consecutive wakes on an empty pool must not crash, deadlock, or assert.
  pool.WakeAllParked();
  pool.WakeOneParked();
  pool.WakeAllParked();

  pool.ShutDown();
  pool.AwaitShutdown();
}

// Test 8: Re-park cycle — each task re-parks itself while `still_contended` is true
// (simulating a stalled engine_lock_ acquisition), then drains when the flag clears.
//
// Phase A: WakeAllParked() → all kN tasks run, re-park, increment `parked_back`.
//          Main thread waits for parked_back == kN (bounded), then verifies no task
//          completed prematurely.
// Phase B: `still_contended` cleared → WakeAllParked() → all kN complete.
//
// The kMaxReparks guard in ParkReparker bounds the cycle so a stuck-flag bug cannot
// hang the test; it does not fire on the happy path (tasks see contended=false in Phase B).
TEST(PriorityThreadPool, WakeAllParked_StillContendedReparkPattern) {
  using namespace memgraph;
  constexpr int kN = 3;
  constexpr int kPollMs = 3000;  // 3 s per phase

  utils::PriorityThreadPool pool{2, 1};
  std::atomic<bool> still_contended{true};
  std::atomic<int> parked_back{0};
  std::atomic<int> completed{0};
  auto deadline = std::chrono::steady_clock::now() + std::chrono::hours(1);

  for (int i = 0; i < kN; ++i) {
    auto r = std::make_shared<ParkReparker>();
    r->still_contended = &still_contended;
    r->parked_back = &parked_back;
    r->completed = &completed;
    r->pool = &pool;
    r->task_id = static_cast<utils::PriorityThreadPool::TaskID>(1000 - i);
    r->deadline = deadline;
    pool.ParkAdmission(r->MakeTask(), r->task_id, deadline);
  }

  // Phase A: wake all; still_contended=true → tasks re-park.
  pool.WakeAllParked();
  for (int w = 0; parked_back.load(std::memory_order_acquire) < kN && w < kPollMs; ++w) {
    std::this_thread::sleep_for(std::chrono::milliseconds(1));
  }
  ASSERT_EQ(parked_back.load(std::memory_order_acquire), kN) << "tasks did not re-park within timeout";
  ASSERT_EQ(completed.load(std::memory_order_acquire), 0) << "tasks must not complete while contended";

  // Phase B: clear flag → tasks complete on next wake.
  still_contended.store(false, std::memory_order_release);
  pool.WakeAllParked();
  for (int w = 0; completed.load(std::memory_order_acquire) < kN && w < kPollMs; ++w) {
    std::this_thread::sleep_for(std::chrono::milliseconds(1));
  }
  ASSERT_EQ(completed.load(std::memory_order_acquire), kN);

  pool.ShutDown();
  pool.AwaitShutdown();
}

// Test 9: ParkAdmission called after ShutDown drops the task without running it.
// ShutDown sets draining_admissions_=true before returning (see priority_thread_pool.cpp
// ShutDown steps: Stop monitor → set draining → drain existing → stop workers).
// The early-return guard in ParkAdmission then destroys any new arrival without enqueueing
// or executing it, preventing use-after-shutdown writes to worker queues.
//
// Design note: calling ParkAdmission between ShutDown and AwaitShutdown is safe because
// ParkAdmission only acquires parked_mtx_ (not any Worker::mtx_) and the task is
// destroyed on the early return before any worker thread can observe it.
TEST(PriorityThreadPool, ParkDropsWhenDraining) {
  using namespace memgraph;
  utils::PriorityThreadPool pool{2, 1};

  std::atomic<bool> task_ran{false};

  // ShutDown: sets draining_admissions_=true, drains previously-parked entries
  // (none here), requests stop on workers.  No tasks were parked before this call.
  pool.ShutDown();

  // ParkAdmission sees draining_admissions_=true and returns immediately; the task
  // lambda is destroyed without being called.
  pool.ParkAdmission([&](utils::Priority) { task_ran.store(true, std::memory_order_release); },
                     static_cast<utils::PriorityThreadPool::TaskID>(1000),
                     std::chrono::steady_clock::now() + std::chrono::hours(1));

  ASSERT_FALSE(task_ran.load(std::memory_order_acquire));

  pool.AwaitShutdown();
}

// Test 10 (DeadlineExpiredParkReinjectedByMonitor): SKIPPED.
// The existing MonitorSweep_PastDeadlineReinjected test already covers this invariant:
// it parks with deadline = now()-1ms and waits up to 400ms for the monitor sweep to
// re-inject and run the task.  Writing a second test for the same code path would be
// redundant and adds no coverage.
