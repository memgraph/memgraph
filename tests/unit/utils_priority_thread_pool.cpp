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

#include <atomic>
#include <chrono>
#include <cstddef>
#include <future>
#include <memory>
#include <thread>
#include <vector>

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

TEST(PriorityThreadPool, StartupPublishesMixedWorkers) {
  using namespace memgraph;
  constexpr uint16_t kMixed = 8;
  constexpr uint16_t kHighPriority = 4;
  constexpr size_t kPools = 200;

  // The constructor holds every thread on a latch until all of them have published their worker, so
  // a task scheduled straight afterwards must find a complete pool. Repeated because a torn startup
  // is a race, not a deterministic failure. Only the mixed slots are covered: every task, whatever
  // its priority, is pushed to a mixed worker, and nothing but the monitor reads the high priority
  // slots.
  //
  // One deadline for the whole loop rather than one per pool: a torn pool never runs its tasks, so
  // waiting longer per pool only delays the report. The bound is many times what the loop needs
  // even on an oversubscribed machine.
  auto const deadline = std::chrono::steady_clock::now() + 60s;
  for (size_t pool_num = 0; pool_num < kPools; ++pool_num) {
    std::atomic<size_t> ran{0};
    {
      utils::PriorityThreadPool pool{kMixed, kHighPriority};
      for (size_t i = 0; i < kMixed; ++i) {
        pool.ScheduledAddTask([&ran](auto) { ++ran; }, utils::Priority::LOW);
      }
      for (size_t i = 0; i < kHighPriority; ++i) {
        pool.ScheduledAddTask([&ran](auto) { ++ran; }, utils::Priority::HIGH);
      }

      // The pool drops queued work as it shuts down, so every task has to be seen to run before the
      // scope ends, not after.
      while (ran != kMixed + kHighPriority && std::chrono::steady_clock::now() < deadline) {
        std::this_thread::sleep_for(1ms);
      }
    }
    ASSERT_EQ(ran, kMixed + kHighPriority) << "pool " << pool_num << " did not run every task";
  }
}

TEST(PriorityThreadPool, StartupRunsInitCallbackOnEveryThread) {
  using namespace memgraph;
  constexpr uint16_t kMixed = 4;
  constexpr uint16_t kHighPriority = 2;

  std::atomic<size_t> initialised{0};
  std::atomic<size_t> ran{0};
  {
    utils::PriorityThreadPool pool{kMixed, kHighPriority, [&initialised]() { ++initialised; }};
    pool.ScheduledAddTask([&ran](auto) { ++ran; }, utils::Priority::LOW);

    // The pool drops queued work as it shuts down, so the task has to be seen to run before the
    // scope ends, not after.
    auto const deadline = std::chrono::steady_clock::now() + 60s;
    while ((initialised != kMixed + kHighPriority || ran != 1) && std::chrono::steady_clock::now() < deadline) {
      std::this_thread::sleep_for(1ms);
    }
  }
  EXPECT_EQ(initialised, kMixed + kHighPriority);
  EXPECT_EQ(ran, 1);
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

// ==================================================================================
// NB-3 productive_pending_ gate test
// ==================================================================================

// Verifies the NB-3 admission-gate contract for QueuedProductiveTasks():
//   - productive=false tasks (admission re-posts) never increment productive_pending_,
//     so they cannot hold the gate open even when queued.
//   - productive=true tasks (real queries) immediately open the gate on submission.
TEST(PriorityThreadPool, QueuedProductiveTasksCountsProductiveNotAdmissionReposts) {
  using namespace memgraph;

  // 4 mixed workers, 1 HP worker.
  constexpr int kN = 4;
  utils::PriorityThreadPool pool{kN, 1};

  std::atomic<int> occupied{0};
  std::atomic<bool> release{false};

  // Occupy all kN mixed workers with non-productive tasks (productive=false) so that
  // productive_pending_ stays at zero throughout the occupancy phase.
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
  ASSERT_EQ(pool.QueuedProductiveTasks(), 0U);
  ASSERT_FALSE(pool.ShouldParkAdmission());

  // Submit an admission re-post (productive=false).  It queues behind a busy worker
  // but must NOT open the gate.
  pool.ScheduledAddTask([](utils::Priority) {}, utils::Priority::LOW, /*productive=*/false);
  ASSERT_EQ(pool.QueuedProductiveTasks(), 0U);
  ASSERT_FALSE(pool.ShouldParkAdmission());

  // Submit a real productive task (productive=true).  push() increments productive_pending_
  // before returning, so QueuedProductiveTasks() must be > 0 immediately after this call.
  std::atomic<bool> productive_ran{false};
  pool.ScheduledAddTask([&](utils::Priority) { productive_ran.store(true, std::memory_order_release); },
                        utils::Priority::LOW,
                        /*productive=*/true);
  ASSERT_GT(pool.QueuedProductiveTasks(), 0U);

  // Release all occupiers so the pool can drain.
  release.store(true, std::memory_order_release);

  // Wait (bounded) for the productive task to complete.
  constexpr int kMaxPollIter = 100'000;
  for (int i = 0; i < kMaxPollIter && !productive_ran.load(std::memory_order_acquire); ++i) {
    std::this_thread::yield();
  }
  ASSERT_TRUE(productive_ran.load(std::memory_order_acquire)) << "productive task never ran within poll bound";
  ASSERT_EQ(pool.QueuedProductiveTasks(), 0U);
  ASSERT_FALSE(pool.ShouldParkAdmission());

  pool.ShutDown();
  pool.AwaitShutdown();
}

// ==================================================================================
// Park subsystem tests
// ==================================================================================

// Convenience tag values used across park tests.
namespace {
constexpr auto kMainTag = memgraph::utils::WaitResource::MainLock;
constexpr auto kCommitTag = memgraph::utils::WaitResource::CommitLock;
// Monitor sweep never fires during a test, so only WakeMatching / the epoch re-post can run a parked task.
constexpr auto kNoSweep = std::chrono::hours(1000);
}  // namespace

// A wake that lands between the epoch sample and ParkAdmission must not be lost: the task is re-posted
// immediately instead of waiting for the monitor sweep.
TEST(PriorityThreadPool, ParkAdmission_WakeAfterSampleBeforeParkRunsPromptly) {
  using namespace memgraph;
  utils::PriorityThreadPool pool{2, 1, nullptr, kNoSweep};

  std::promise<void> ran;
  auto ran_future = ran.get_future();
  const auto deadline = std::chrono::steady_clock::now() + std::chrono::hours(1);

  const auto observed = pool.WakeEpoch(utils::WaitResource::CommitLock);
  pool.WakeMatching(kCommitTag);  // release after the sample, before the park: nothing parked yet
  EXPECT_NE(pool.WakeEpoch(utils::WaitResource::CommitLock), observed);
  EXPECT_EQ(pool.WakeEpoch(utils::WaitResource::MainLock), 0U);

  pool.ParkAdmission([&](utils::Priority) { ran.set_value(); }, 1000, deadline, kCommitTag, observed);

  // The sweep is disabled, so only the epoch re-post can run the task.
  EXPECT_EQ(ran_future.wait_for(1s), std::future_status::ready);

  pool.ShutDown();
  pool.AwaitShutdown();
}

// Tasks still queued when the pool stops, and parked continuations, release their captured state by the time
// AwaitShutdown returns, while the pool object is alive.
TEST(PriorityThreadPool, AwaitShutdown_ReleasesUnrunQueuedAndParkedTasks) {
  using namespace memgraph;
  utils::PriorityThreadPool pool{1, 1};

  std::promise<void> started;
  std::promise<void> release;
  auto release_future = release.get_future().share();
  pool.ScheduledAddTask(
      [&started, release_future](utils::Priority) {
        started.set_value();
        release_future.wait();
      },
      utils::Priority::LOW);
  started.get_future().wait();  // the only mixed worker is now busy; HP workers never steal LOW work

  std::atomic<int> ran{0};
  constexpr int kQueued = 3;
  std::vector<std::weak_ptr<int>> sentinels;
  for (int i = 0; i < kQueued; ++i) {
    auto sentinel = std::make_shared<int>(i);
    sentinels.emplace_back(sentinel);
    pool.ScheduledAddTask(
        [sentinel = std::move(sentinel), &ran](utils::Priority) { ran.fetch_add(1, std::memory_order_acq_rel); },
        utils::Priority::LOW);
  }

  auto parked_sentinel = std::make_shared<int>(42);
  std::weak_ptr<int> parked_weak = parked_sentinel;
  pool.ParkAdmission([sentinel = std::move(parked_sentinel)](utils::Priority) {},
                     1000,
                     std::chrono::steady_clock::now() + std::chrono::hours(1),
                     kMainTag);

  pool.ShutDown();
  EXPECT_TRUE(parked_weak.expired());

  release.set_value();
  pool.AwaitShutdown();

  EXPECT_EQ(ran.load(std::memory_order_acquire), 0);
  for (const auto &w : sentinels) EXPECT_TRUE(w.expired());
}

// Test 4: ShutDown drain — every parked continuation runs exactly once.
TEST(PriorityThreadPool, ShutDown_DrainsParkedAdmissions) {
  using namespace memgraph;
  utils::PriorityThreadPool pool{2, 1};

  constexpr int kN = 5;
  std::atomic<int> ran{0};
  auto deadline = std::chrono::steady_clock::now() + std::chrono::hours(1);

  for (int i = 0; i < kN; ++i) {
    pool.ParkAdmission([&](utils::Priority) { ran.fetch_add(1, std::memory_order_acq_rel); },
                       static_cast<utils::PriorityThreadPool::TaskID>(1000 - i),
                       deadline,
                       kMainTag);
  }

  // ShutDown runs the drain synchronously in the caller thread before stopping workers;
  // every parked continuation must have run by the time ShutDown returns.
  pool.ShutDown();

  ASSERT_EQ(ran.load(std::memory_order_acquire), kN);

  pool.AwaitShutdown();
}

// ==================================================================================
// WakeMatching tests
// ==================================================================================

// Helper for Test 7 (re-park cycle simulation).
// Each instance represents one parked admission slot that re-parks itself while
// `still_contended` is true, simulating a stalled acquisition gate. `repark_count`
// guards against an infinite re-park loop: if kMaxReparks is reached the task
// completes regardless of the flag, so a stuck-flag bug cannot hang the suite.
namespace {
struct ParkReparker : std::enable_shared_from_this<ParkReparker> {
  std::atomic<bool> *still_contended{nullptr};
  std::atomic<int> *parked_back{nullptr};
  std::atomic<int> *completed{nullptr};
  memgraph::utils::PriorityThreadPool *pool{nullptr};
  memgraph::utils::PriorityThreadPool::TaskID task_id{0};
  std::chrono::steady_clock::time_point deadline{};
  memgraph::utils::WaitResource tag = memgraph::utils::WaitResource::MainLock;

  static constexpr int kMaxReparks = 5;
  int repark_count{0};  // single-writer: only the task executing this slot modifies it

  memgraph::utils::TaskSignature MakeTask() {
    return [self = shared_from_this()](memgraph::utils::Priority) {
      const bool contended = self->still_contended->load(std::memory_order_acquire);
      if (contended && self->repark_count < kMaxReparks) {
        ++self->repark_count;
        // Re-park first; signal only after the entry is in the deque so the
        // main thread's parked_back wait is a true happens-before for the
        // subsequent WakeMatching call.
        self->pool->ParkAdmission(self->MakeTask(), self->task_id, self->deadline, self->tag);
        self->parked_back->fetch_add(1, std::memory_order_release);
      } else {
        self->completed->fetch_add(1, std::memory_order_acq_rel);
      }
    };
  }
};
}  // namespace

// Test 7: Re-park cycle — each task re-parks itself while `still_contended` is true
// (simulating a stalled engine_lock_ acquisition), then drains when the flag clears.
TEST(PriorityThreadPool, WakeMatching_StillContendedReparkPattern) {
  using namespace memgraph;
  constexpr int kN = 3;
  constexpr int kPollMs = 3000;  // 3 s per phase

  utils::PriorityThreadPool pool{2, 1, nullptr, kNoSweep};
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
    pool.ParkAdmission(r->MakeTask(), r->task_id, deadline, kMainTag);
  }

  // Phase A: wake all MainLock; still_contended=true → tasks re-park.
  pool.WakeMatching(kMainTag);
  for (int w = 0; parked_back.load(std::memory_order_acquire) < kN && w < kPollMs; ++w) {
    std::this_thread::sleep_for(std::chrono::milliseconds(1));
  }
  ASSERT_EQ(parked_back.load(std::memory_order_acquire), kN) << "tasks did not re-park within timeout";
  ASSERT_EQ(completed.load(std::memory_order_acquire), 0) << "tasks must not complete while contended";

  // Phase B: clear flag → tasks complete on next wake.
  still_contended.store(false, std::memory_order_release);
  pool.WakeMatching(kMainTag);
  for (int w = 0; completed.load(std::memory_order_acquire) < kN && w < kPollMs; ++w) {
    std::this_thread::sleep_for(std::chrono::milliseconds(1));
  }
  ASSERT_EQ(completed.load(std::memory_order_acquire), kN);

  pool.ShutDown();
  pool.AwaitShutdown();
}

// Test 8: ParkAdmission called after ShutDown drops the task without running it.
TEST(PriorityThreadPool, ParkDropsWhenDraining) {
  using namespace memgraph;
  utils::PriorityThreadPool pool{2, 1};

  std::atomic<bool> task_ran{false};
  auto sentinel = std::make_shared<int>(0);
  std::weak_ptr<int> sentinel_weak = sentinel;

  pool.ShutDown();

  pool.ParkAdmission(
      [&task_ran, sentinel = std::move(sentinel)](utils::Priority) { task_ran.store(true, std::memory_order_release); },
      static_cast<utils::PriorityThreadPool::TaskID>(1000),
      std::chrono::steady_clock::now() + std::chrono::hours(1),
      kMainTag);

  ASSERT_TRUE(sentinel_weak.expired()) << "task parked while draining must be dropped immediately";
  ASSERT_FALSE(task_ran.load(std::memory_order_acquire));

  pool.AwaitShutdown();
}

// ==================================================================================
// Tagging extension tests (new in this port)
// ==================================================================================

// WakeMatching re-injects every entry parked on the woken resource (and only those); wakes on an empty deque
// and wakes after everything drained are silent no-ops.
TEST(PriorityThreadPool, WakeMatching_ResourceSelective) {
  using namespace memgraph;
  utils::PriorityThreadPool pool{2, 1, nullptr, kNoSweep};

  constexpr int kMain = 5;
  constexpr int kCommit = 3;
  std::atomic<int> main_ran{0};
  std::atomic<int> commit_ran{0};
  auto deadline = std::chrono::steady_clock::now() + std::chrono::hours(1);

  // Nothing parked yet: wakes must be safe no-ops.
  pool.WakeMatching(kMainTag);
  pool.WakeMatching(kCommitTag);
  pool.WakeMatching(kMainTag);

  for (int i = 0; i < kMain; ++i) {
    pool.ParkAdmission([&](utils::Priority) { main_ran.fetch_add(1, std::memory_order_acq_rel); },
                       static_cast<utils::PriorityThreadPool::TaskID>(2000 - i),
                       deadline,
                       kMainTag);
  }
  for (int i = 0; i < kCommit; ++i) {
    pool.ParkAdmission([&](utils::Priority) { commit_ran.fetch_add(1, std::memory_order_acq_rel); },
                       static_cast<utils::PriorityThreadPool::TaskID>(1000 - i),
                       deadline,
                       kCommitTag);
  }

  // Wake only MainLock: every MainLock task runs; CommitLock tasks stay parked.
  pool.WakeMatching(kMainTag);
  for (int w = 0; main_ran.load(std::memory_order_acquire) < kMain && w < 3000; ++w) {
    std::this_thread::sleep_for(std::chrono::milliseconds(1));
  }
  ASSERT_EQ(main_ran.load(std::memory_order_acquire), kMain);
  ASSERT_EQ(commit_ran.load(std::memory_order_acquire), 0) << "CommitLock tasks must not run after MainLock wake";

  // Repeat wake of the drained resource re-injects nothing.
  pool.WakeMatching(kMainTag);
  std::this_thread::sleep_for(std::chrono::milliseconds(20));
  ASSERT_EQ(main_ran.load(std::memory_order_acquire), kMain);
  ASSERT_EQ(commit_ran.load(std::memory_order_acquire), 0);

  // Now free CommitLock: CommitLock tasks run, and MainLock counts stay unchanged.
  pool.WakeMatching(kCommitTag);
  for (int w = 0; commit_ran.load(std::memory_order_acquire) < kCommit && w < 3000; ++w) {
    std::this_thread::sleep_for(std::chrono::milliseconds(1));
  }
  ASSERT_EQ(commit_ran.load(std::memory_order_acquire), kCommit);

  // Everything drained: further wakes on either resource run nothing more.
  pool.WakeMatching(kMainTag);
  pool.WakeMatching(kCommitTag);
  std::this_thread::sleep_for(std::chrono::milliseconds(20));
  ASSERT_EQ(main_ran.load(std::memory_order_acquire), kMain);
  ASSERT_EQ(commit_ran.load(std::memory_order_acquire), kCommit);

  pool.ShutDown();
  pool.AwaitShutdown();
}

// The monitor sweep re-injects every deadline-expired entry (any tag) in one tick, not only the oldest.
// The two far-deadline entries are parked first, so a one-per-tick "kick the oldest" backstop alone would run
// them before the expired ones. With the expiry branch the three expired entries run in the first tick (plus
// at most one far entry as the backstop); without it they would need five ticks and both far entries would
// have run by then.
TEST(PriorityThreadPool, MonitorSweep_DeadlineExpiredAnyTag) {
  using namespace memgraph;
  constexpr auto kTick = std::chrono::milliseconds{250};

  std::atomic<int> expired_ran{0};
  std::atomic<int> far_ran{0};
  utils::PriorityThreadPool pool{2, 1, nullptr, kTick};

  const auto now = std::chrono::steady_clock::now();
  const auto far_deadline = now + std::chrono::hours(1);
  const auto past_deadline = now - std::chrono::milliseconds(1);
  const auto count_far = [&](utils::Priority) { far_ran.fetch_add(1, std::memory_order_acq_rel); };
  const auto count_expired = [&](utils::Priority) { expired_ran.fetch_add(1, std::memory_order_acq_rel); };

  pool.ParkAdmission(count_far, 4000, far_deadline, kMainTag);
  pool.ParkAdmission(count_far, 3999, far_deadline, kCommitTag);
  pool.ParkAdmission(count_expired, 3000, past_deadline, kMainTag);
  pool.ParkAdmission(count_expired, 2000, past_deadline, kCommitTag);
  pool.ParkAdmission(count_expired, 1000, past_deadline, kMainTag);

  for (int w = 0; expired_ran.load(std::memory_order_acquire) < 3 && w < 5000; ++w) {
    std::this_thread::sleep_for(std::chrono::milliseconds(1));
  }
  ASSERT_EQ(expired_ran.load(std::memory_order_acquire), 3);
  ASSERT_LE(far_ran.load(std::memory_order_acquire), 1) << "expired entries must not need one tick each";

  pool.ShutDown();
  pool.AwaitShutdown();
}
