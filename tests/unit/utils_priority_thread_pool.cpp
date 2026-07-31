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
#include <mutex>
#include <optional>
#include <string>
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

// --- Unit 3a: must-run queue / PostResumeTask / worker-id TLS / IsShuttingDown ---

TEST(PriorityThreadPool, CurrentWorkerIdTLS) {
  using namespace memgraph;

  // Off any pool worker thread (e.g. the test's own thread), must be nullopt.
  ASSERT_FALSE(utils::GetCurrentWorkerId().has_value());

  memgraph::utils::PriorityThreadPool pool{2, 1};

  std::atomic<bool> observed_has_value{false};
  std::atomic<bool> done{false};
  pool.ScheduledAddTask(
      [&](auto) {
        observed_has_value = utils::GetCurrentWorkerId().has_value();
        done = true;
        done.notify_one();
      },
      utils::Priority::LOW);
  done.wait(false);
  ASSERT_TRUE(observed_has_value.load());

  pool.ShutDown();
  pool.AwaitShutdown();
}

TEST(PriorityThreadPool, PostResumeTaskRunsOnAnLpWorker) {
  using namespace memgraph;
  memgraph::utils::PriorityThreadPool pool{2, 1};

  std::mutex m;
  std::optional<size_t> observed_id;
  std::atomic<bool> done{false};

  pool.PostResumeTask([&] {
    {
      std::lock_guard<std::mutex> lock(m);
      observed_id = utils::GetCurrentWorkerId();
    }
    done = true;
    done.notify_one();
  });

  done.wait(false);
  {
    std::lock_guard<std::mutex> lock(m);
    // WHICH worker is deliberately unspecified post-F6; that it is an LP pool worker is not, since
    // the resumed coroutine may park again and a park requires a worker that will arm it.
    ASSERT_TRUE(observed_id.has_value());
    ASSERT_LT(*observed_id, pool.GetNumMixedWorkers());
  }

  pool.ShutDown();
  pool.AwaitShutdown();
}

// The point of un-pinning (IP-1 F6). Pre-F6 a resume was pinned to the worker that parked, so a long
// unrelated task on that worker delayed it arbitrarily -- which for a parked query means its
// storage-access timeout was not enforced on time either.
//
// Every worker is wedged before the resume is posted, so target selection cannot fall back on a hot or
// idle worker: the post necessarily lands on a BUSY worker's queue. Then exactly one worker is freed.
// What this pins is the property that matters -- a resume is not hostage to the whole backlog of
// whichever worker happened to receive it. It does not isolate WHICH mechanism delivers it (an LP
// thief's steal, or the receiving worker reaching its own task boundary), because a test cannot choose
// which worker PostResumeTask picks.
TEST(PriorityThreadPool, PostResumeTaskNotBlockedByBusyWorkers) {
  using namespace memgraph;
  constexpr size_t kWorkers = 4;
  memgraph::utils::PriorityThreadPool pool{kWorkers, 1};

  std::vector<std::unique_ptr<std::atomic<bool>>> blocks;
  blocks.reserve(kWorkers);
  for (size_t i = 0; i < kWorkers; ++i) blocks.emplace_back(std::make_unique<std::atomic<bool>>(true));

  std::atomic<int> occupied{0};
  for (size_t i = 0; i < kWorkers; ++i) {
    pool.ScheduledAddTask(
        [&, i](auto) {
          occupied.fetch_add(1);
          auto &block = *blocks[i];
          while (block.load()) {
            block.wait(true);
          }
        },
        utils::Priority::LOW);
  }
  while (occupied.load() != static_cast<int>(kWorkers)) {
    std::this_thread::sleep_for(std::chrono::milliseconds(1));
  }

  std::atomic<bool> resumed{false};
  pool.PostResumeTask([&] {
    resumed = true;
    resumed.notify_one();
  });

  // Nobody is free yet, so nothing can have run it.
  std::this_thread::sleep_for(std::chrono::milliseconds(50));
  EXPECT_FALSE(resumed.load()) << "no worker was free, so the resume cannot have run";

  // Free exactly one worker.
  *blocks[0] = false;
  blocks[0]->notify_all();

  for (int i = 0; i < 500 && !resumed.load(); ++i) {
    std::this_thread::sleep_for(std::chrono::milliseconds(1));
  }
  EXPECT_TRUE(resumed.load()) << "one free worker was not enough to service a resume queued on a busy one";

  for (auto &b : blocks) {
    *b = false;
    b->notify_all();
  }
  pool.ShutDown();
  pool.AwaitShutdown();
}

// The tail drain -- the sole basis for try_push's "accepted => eventually run" promise, and for
// PostResumeTask being allowed to ignore stop_requested(). Previously untested: the two teardown tests
// below both post AFTER ShutDown(), by which point every worker's run_ is false, so try_push refuses
// everywhere and the closure takes the inline fallback instead. This one posts INTO the window that
// actually needs the drain -- accepted while the worker is alive, serviced after it was told to stop.
TEST(PriorityThreadPool, MustRunItemAcceptedBeforeStopIsDrainedByTheWorkerTail) {
  using namespace memgraph;
  memgraph::utils::PriorityThreadPool pool{1, 1};  // one LP worker, so the target is unambiguous

  std::atomic<bool> block{true};
  std::atomic<bool> wedged{false};
  pool.ScheduledAddTask(
      [&](auto) {
        wedged = true;
        wedged.notify_one();
        while (block.load()) {
          block.wait(true);
        }
      },
      utils::Priority::LOW);
  wedged.wait(false);

  // The worker is alive but busy, so try_push accepts into its must-run queue rather than refusing.
  std::atomic<bool> ran{false};
  pool.PostResumeTask([&] { ran = true; });
  EXPECT_FALSE(ran.load()) << "the worker is still wedged; nothing should have run it yet";

  // Tell the worker to stop while it is STILL inside its task, then let it finish. It leaves the run
  // loop with run_ == false and must drain the accepted item on the way out.
  std::thread shutdown_thread([&] { pool.ShutDown(); });
  std::this_thread::sleep_for(std::chrono::milliseconds(50));
  block = false;
  block.notify_all();

  shutdown_thread.join();
  pool.AwaitShutdown();
  EXPECT_TRUE(ran.load()) << "a must-run item accepted before stop() was dropped instead of drained -- "
                             "that breaks 'accepted => eventually run', and each dropped item is a "
                             "permanently parked query";
}

// Resumes jump ordinary work, but must NOT jump a queued HIGH-priority item: before the must-run queue
// existed there was one max-heap and HP was strictly first, so demoting HP below every resume would be
// a priority inversion introduced by a flag that defaults to off.
TEST(PriorityThreadPool, HighPriorityWorkStillPrecedesAResume) {
  using namespace memgraph;
  memgraph::utils::PriorityThreadPool pool{1, 1};

  std::atomic<bool> block{true};
  std::atomic<bool> wedged{false};
  pool.ScheduledAddTask(
      [&](auto) {
        wedged = true;
        wedged.notify_one();
        while (block.load()) {
          block.wait(true);
        }
      },
      utils::Priority::LOW);
  wedged.wait(false);

  // Both queued behind the wedged task on the same (only) LP worker.
  std::mutex order_mutex;
  std::vector<std::string> order;
  auto record = [&](std::string what) {
    std::lock_guard<std::mutex> lock(order_mutex);
    order.push_back(std::move(what));
  };

  pool.PostResumeTask([&] { record("resume"); });
  pool.ScheduledAddTask([&](auto) { record("high"); }, utils::Priority::HIGH);

  block = false;
  block.notify_all();

  // Wait for both, then check which ran first. The HP task may also be stolen by the HP worker, which
  // is equally fine -- either way it must not be stuck behind the resume.
  for (int i = 0; i < 500; ++i) {
    {
      std::lock_guard<std::mutex> lock(order_mutex);
      if (order.size() == 2) break;
    }
    std::this_thread::sleep_for(std::chrono::milliseconds(1));
  }

  std::lock_guard<std::mutex> lock(order_mutex);
  ASSERT_EQ(order.size(), 2U) << "both tasks should have run";
  EXPECT_EQ(order.front(), "high") << "a queued HIGH-priority task was served after a parked-query "
                                      "resume; must-run work must not preempt HP work";

  pool.ShutDown();
  pool.AwaitShutdown();
}

// A resume posted after shutdown was requested must still run: dropping it leaks the parked
// session (and its DatabaseAccess), which stalls the very shutdown that dropped it. This is the
// window ShutDown()'s park drain posts into -- workers are stop()-ed but not yet joined.
TEST(PriorityThreadPool, PostResumeTaskDuringShutdownStillRuns) {
  using namespace memgraph;
  memgraph::utils::PriorityThreadPool pool{2, 1};

  std::atomic<bool> ran{false};
  pool.ShutDown();
  ASSERT_TRUE(pool.IsShuttingDown());
  pool.PostResumeTask([&] { ran = true; });

  pool.AwaitShutdown();  // joins; any accepted must-run item has been drained by now
  EXPECT_TRUE(ran.load()) << "a resume posted during teardown must never be dropped";
}

// Same, but from the parking side: ScheduledAddTask is refused once shutdown starts, and
// PostResumeTask deliberately is not.
TEST(PriorityThreadPool, ScheduledAddTaskRefusedAfterShutdownButResumeIsNot) {
  using namespace memgraph;
  memgraph::utils::PriorityThreadPool pool{1, 1};

  pool.ShutDown();

  std::atomic<int> ordinary{0};
  std::atomic<int> resume{0};
  pool.ScheduledAddTask([&](auto) { ordinary.fetch_add(1); }, utils::Priority::LOW);
  pool.PostResumeTask([&] { resume.fetch_add(1); });

  pool.AwaitShutdown();
  EXPECT_EQ(ordinary.load(), 0) << "ordinary work must not start once shutdown was requested";
  EXPECT_EQ(resume.load(), 1) << "a resume must run regardless";
}

TEST(PriorityThreadPool, IsShuttingDownTransitions) {
  using namespace memgraph;
  memgraph::utils::PriorityThreadPool pool{1, 1};

  ASSERT_FALSE(pool.IsShuttingDown());
  pool.ShutDown();
  ASSERT_TRUE(pool.IsShuttingDown());

  pool.AwaitShutdown();
}
