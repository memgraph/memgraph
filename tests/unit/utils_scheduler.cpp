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

#include <fmt/format.h>
#include <spdlog/sinks/base_sink.h>
#include "gtest/gtest.h"

#include <algorithm>
#include <atomic>
#include <chrono>
#include <functional>
#include <memory>
#include <mutex>
#include <string>
#include <string_view>
#include <thread>
#include <utility>
#include <vector>
#include "tests/unit/timezone_handler.hpp"
#include "utils/exceptions.hpp"
#include "utils/on_scope_exit.hpp"
#include "utils/scheduler.hpp"
#include "utils/temporal.hpp"

/**
 * Scheduler runs every 2 seconds and increases one variable. Test thread
 * increases other variable. Scheduler checks if variables have the same
 * value.
 */
TEST(Scheduler, TestFunctionExecuting) {
  std::atomic<int> x{0};
  std::function<void()> func{[&x]() { ++x; }};
  memgraph::utils::Scheduler scheduler;
  scheduler.SetInterval(std::chrono::seconds(1));
  scheduler.Run("Test", func);

  EXPECT_EQ(x, 0);
  std::this_thread::sleep_for(std::chrono::milliseconds(900));
  EXPECT_EQ(x, 0);

  std::this_thread::sleep_for(std::chrono::milliseconds(200));
  EXPECT_EQ(x, 1);

  std::this_thread::sleep_for(std::chrono::milliseconds(2000));
  EXPECT_EQ(x, 3);

  std::this_thread::sleep_for(std::chrono::milliseconds(100));
  scheduler.Stop();
  EXPECT_EQ(x, 3);
}

TEST(Scheduler, SetupAndSpinOnce) {
  std::atomic<int> x{0};
  std::function<void()> func{[&x]() { ++x; }};
  memgraph::utils::Scheduler scheduler;
  scheduler.SetInterval(std::chrono::seconds(100));
  scheduler.Run("Test", func);

  // No execution
  std::this_thread::sleep_for(std::chrono::milliseconds(1500));
  EXPECT_EQ(x, 0);

  // Setup short period, but we are stuck in the old wait
  scheduler.SetInterval(std::chrono::seconds(1));
  std::this_thread::sleep_for(std::chrono::milliseconds(1500));
  EXPECT_EQ(x, 0);

  // SpinOne to unblock, but still don't execute
  scheduler.SpinOnce();
  std::this_thread::sleep_for(std::chrono::milliseconds(1500));
  EXPECT_GT(x, 0);

  // Spin shouldn't execute, but should spin even under pause
  scheduler.Pause();
  std::this_thread::sleep_for(std::chrono::milliseconds(1500));
  const auto x_now = x.load();
  scheduler.SpinOnce();
  EXPECT_EQ(x, x_now);

  // Re-setup for long period and spin to update
  scheduler.SetInterval(std::chrono::seconds(100));
  scheduler.SpinOnce();
  scheduler.Resume();
  std::this_thread::sleep_for(std::chrono::milliseconds(1500));
  EXPECT_EQ(x, x_now);
}

/**
 * Test scheduler's start time feature.
 */
TEST(Scheduler, StartTime) {
  std::atomic<int> x{0};
  std::function<void()> func{[&x]() { ++x; }};
  memgraph::utils::Scheduler scheduler;

  const auto now = std::chrono::system_clock::now();
  const auto timeout1 = now + std::chrono::seconds(4);
  const auto timeout2 = now + std::chrono::seconds(8);

  // start_time in the past
  scheduler.SetInterval(std::chrono::seconds(3), now + std::chrono::seconds(3));
  scheduler.Run("Test", func);

  // Should execute only after start time
  while (x == 0 && std::chrono::system_clock::now() < timeout1) {
    std::this_thread::sleep_for(std::chrono::milliseconds(100));
  }
  ASSERT_EQ(x, 1);

  // Second execution after period
  while (x == 1 && std::chrono::system_clock::now() < timeout2) {
    std::this_thread::sleep_for(std::chrono::milliseconds(100));
  }
  ASSERT_EQ(x, 2);
}

/**
 * Test scheduler's start time feature.
 */
TEST(Scheduler, StartTimeRestart) {
  std::atomic<int> x{0};
  std::function<void()> func{[&x]() { ++x; }};
  memgraph::utils::Scheduler scheduler;

  const auto now = std::chrono::system_clock::now();
  const auto timeout1 = now + std::chrono::seconds(1);
  const auto timeout2 = now + std::chrono::seconds(4);

  // start_time in the past
  scheduler.SetInterval(std::chrono::seconds(5), now - std::chrono::seconds(3));
  scheduler.Run("Test", func);

  // Should execute at first possible time in the future
  while (std::chrono::system_clock::now() < timeout1) {
    std::this_thread::sleep_for(std::chrono::milliseconds(100));
  }
  ASSERT_EQ(x, 0);

  while (x == 0 && std::chrono::system_clock::now() < timeout2) {
    std::this_thread::sleep_for(std::chrono::milliseconds(100));
  }
  ASSERT_EQ(x, 1);
}

TEST(Scheduler, IsRunningFalse) {
  memgraph::utils::Scheduler scheduler;
  EXPECT_FALSE(scheduler.IsRunning());
}

TEST(Scheduler, StopIdleScheduler) {
  memgraph::utils::Scheduler scheduler;
  ASSERT_NO_THROW(scheduler.Stop());
}

TEST(Scheduler, PauseIdleScheduler) {
  memgraph::utils::Scheduler scheduler;
  ASSERT_NO_THROW(scheduler.Pause());
  ASSERT_NO_THROW(scheduler.Resume());
}

TEST(Scheduler, RunStop) {
  std::atomic<int> x{0};
  std::function<void()> func{[&x]() { ++x; }};
  memgraph::utils::Scheduler scheduler;
  scheduler.SetInterval(std::chrono::milliseconds(100));
  scheduler.Run("Test", func);
  EXPECT_TRUE(scheduler.IsRunning());
  scheduler.Stop();
  EXPECT_FALSE(scheduler.IsRunning());
}

TEST(Scheduler, StopStoppedScheduler) {
  std::atomic<int> x{0};
  std::function<void()> func{[&x]() { ++x; }};
  memgraph::utils::Scheduler scheduler;
  scheduler.SetInterval(std::chrono::milliseconds(100));
  scheduler.Run("Test", func);
  ASSERT_NO_THROW({
    scheduler.Stop();
    scheduler.Stop();
  });
}

TEST(Scheduler, PauseStoppedScheduler) {
  std::atomic<int> x{0};
  std::function<void()> func{[&x]() { ++x; }};
  memgraph::utils::Scheduler scheduler;
  scheduler.SetInterval(std::chrono::milliseconds(100));
  scheduler.Run("Test", func);
  EXPECT_TRUE(scheduler.IsRunning());
  scheduler.Stop();
  EXPECT_FALSE(scheduler.IsRunning());
  scheduler.Pause();
  EXPECT_FALSE(scheduler.IsRunning());
}

TEST(Scheduler, StopPausedScheduler) {
  std::atomic<int> x{0};
  std::function<void()> func{[&x]() { ++x; }};
  memgraph::utils::Scheduler scheduler;
  scheduler.SetInterval(std::chrono::milliseconds(100));
  scheduler.Run("Test", func);
  EXPECT_TRUE(scheduler.IsRunning());
  scheduler.Pause();
  EXPECT_TRUE(scheduler.IsRunning());  // note pausing the scheduler doesn't cause
                                       // return false for IsRunning
  scheduler.Stop();
  EXPECT_FALSE(scheduler.IsRunning());
}

TEST(Scheduler, ConcurrentStops) {
  std::atomic<int> x{0};
  std::function<void()> func{[&x]() { ++x; }};
  memgraph::utils::Scheduler scheduler;
  scheduler.SetInterval(std::chrono::milliseconds(100));
  scheduler.Run("Test", func);

  std::jthread stopper1([&scheduler]() { scheduler.Stop(); });

  std::jthread stopper2([&scheduler]() { scheduler.Stop(); });
}

#ifdef MG_ENTERPRISE
TEST(Scheduler, CronAny) {
  std::atomic<int> x{0};
  std::function<void()> func{[&x]() { ++x; }};
  memgraph::utils::Scheduler scheduler;

  const auto now = std::chrono::system_clock::now();
  const auto timeout1 = now + std::chrono::seconds(3);

  // Execute every second
  scheduler.SetInterval("* * * * * *");
  scheduler.Run("Test", func);

  while (x == 0 && std::chrono::system_clock::now() < timeout1) {
    std::this_thread::sleep_for(std::chrono::milliseconds(100));
  }
  ASSERT_GE(x, 1);
}

TEST(Scheduler, CronEveryNs) {
  std::atomic<int> x{0};
  std::function<void()> func{[&x]() { ++x; }};
  memgraph::utils::Scheduler scheduler;

  const auto start = std::chrono::system_clock::now();
  const auto timeout1 = start + std::chrono::seconds(6);

  // Execute every second
  scheduler.SetInterval("*/2 * * * * *");
  scheduler.Run("Test", func);

  while (x < 2 && std::chrono::system_clock::now() < timeout1) {
    std::this_thread::sleep_for(std::chrono::milliseconds(100));
  }
  const auto now = std::chrono::system_clock::now();
  ASSERT_GT(now - start, std::chrono::seconds{2});
  ASSERT_LT(now - start, std::chrono::seconds{5});
  ASSERT_GE(x, 2);
}

TEST(Scheduler, CronSpecificTime) {
  std::atomic<int> x{0};
  std::function<void()> func{[&x]() { ++x; }};
  memgraph::utils::Scheduler scheduler;

  const auto now = std::chrono::system_clock::now();
  const auto local_time = (memgraph::utils::LocalDateTime{now} + memgraph::utils::Duration(2e6 /*us*/)).local_time();
  const auto timeout1 = now + std::chrono::seconds(3);

  // Execute at specific time
  scheduler.SetInterval(fmt::format("{} {} {} * * *", local_time.second, local_time.minute, local_time.hour));
  scheduler.Run("Test", func);

  std::this_thread::sleep_for(std::chrono::milliseconds(1000));
  ASSERT_EQ(x, 0);

  while (x == 0 && std::chrono::system_clock::now() < timeout1) {
    std::this_thread::sleep_for(std::chrono::milliseconds(100));
  }
  ASSERT_EQ(x, 1);
}

TEST(Scheduler, CronSpecificTimeWTZ) {
  HandleTimezone htz;

  auto test = [&]() {
    std::atomic<int> x{0};
    std::function<void()> func{[&x]() { ++x; }};
    memgraph::utils::Scheduler scheduler;

    const auto now = std::chrono::system_clock::now();
    const auto local_time = (memgraph::utils::LocalDateTime{now} + memgraph::utils::Duration(2e6 /*us*/)).local_time();
    const auto timeout1 = now + std::chrono::seconds(3);

    // Execute at specific time
    scheduler.SetInterval(fmt::format("{} {} {} * * *", local_time.second, local_time.minute, local_time.hour));
    scheduler.Run("Test", func);

    std::this_thread::sleep_for(std::chrono::milliseconds(1000));
    ASSERT_EQ(x, 0);

    while (x == 0 && std::chrono::system_clock::now() < timeout1) {
      std::this_thread::sleep_for(std::chrono::milliseconds(100));
    }
    ASSERT_EQ(x, 1);
  };

  htz.Set("UTC");
  test();
  htz.Set("Europe/Rome");
  test();
  htz.Set("America/Los_Angeles");
  test();
}

TEST(Scheduler, CronSpecificDate) {
  std::atomic<int> x{0};
  std::function<void()> func{[&x]() { ++x; }};
  memgraph::utils::Scheduler scheduler;

  const auto now = std::chrono::system_clock::now();
  const auto local_date = memgraph::utils::LocalDateTime{now}.date();

  // Execute every second
  scheduler.SetInterval(fmt::format("* * * {} {} *", local_date.day, local_date.month));
  scheduler.Run("Test", func);

  std::this_thread::sleep_for(std::chrono::milliseconds(1500));
  ASSERT_GT(x, 0);
}

TEST(Scheduler, CronSpecificDateWTZ) {
  HandleTimezone htz;

  auto test = [&]() {
    std::atomic<int> x{0};
    std::function<void()> func{[&x]() { ++x; }};
    memgraph::utils::Scheduler scheduler;

    const auto now = std::chrono::system_clock::now();
    const auto local_date = memgraph::utils::LocalDateTime{now}.date();

    // Execute every second
    scheduler.SetInterval(fmt::format("* * * {} {} *", local_date.day, local_date.month));
    scheduler.Run("Test", func);

    std::this_thread::sleep_for(std::chrono::milliseconds(1500));
    ASSERT_GT(x, 0);
  };

  htz.Set("UTC");
  test();
  htz.Set("Europe/Rome");
  test();
  htz.Set("America/Los_Angeles");
  test();
}

TEST(Scheduler, CronSpecificDateTime) {
  std::atomic<int> x{0};
  std::function<void()> func{[&x]() { ++x; }};
  memgraph::utils::Scheduler scheduler;

  const auto now = std::chrono::system_clock::now();
  const auto timeout1 = now + std::chrono::seconds(3);
  const auto next = memgraph::utils::LocalDateTime{now} + memgraph::utils::Duration(2e6 /*us*/);
  const auto local_date = next.date();
  const auto local_time = next.local_time();

  // Execute every second
  scheduler.SetInterval(fmt::format(
      "{} {} {} {} {} *", local_time.second, local_time.minute, local_time.hour, local_date.day, local_date.month));
  scheduler.Run("Test", func);

  std::this_thread::sleep_for(std::chrono::milliseconds(1000));
  ASSERT_EQ(x, 0);

  while (x == 0 && std::chrono::system_clock::now() < timeout1) {
    std::this_thread::sleep_for(std::chrono::milliseconds(100));
  }
  ASSERT_EQ(x, 1);
}

TEST(Scheduler, CronSpecificDateTimeWTZ) {
  HandleTimezone htz;

  auto test = [&]() {
    std::atomic<int> x{0};
    std::function<void()> func{[&x]() { ++x; }};
    memgraph::utils::Scheduler scheduler;

    const auto now = std::chrono::system_clock::now();
    const auto timeout1 = now + std::chrono::seconds(3);
    const auto next = memgraph::utils::LocalDateTime{now} + memgraph::utils::Duration(2e6 /*us*/);
    const auto local_date = next.date();
    const auto local_time = next.local_time();

    // Execute every second
    scheduler.SetInterval(fmt::format(
        "{} {} {} {} {} *", local_time.second, local_time.minute, local_time.hour, local_date.day, local_date.month));
    scheduler.Run("Test", func);

    std::this_thread::sleep_for(std::chrono::milliseconds(1000));
    ASSERT_EQ(x, 0);

    while (x == 0 && std::chrono::system_clock::now() < timeout1) {
      std::this_thread::sleep_for(std::chrono::milliseconds(100));
    }
    ASSERT_EQ(x, 1);
  };

  htz.Set("UTC");
  test();
  htz.Set("Europe/Rome");
  test();
  htz.Set("America/Los_Angeles");
  test();
}

// Rome repeats 02:00 to 03:00 local on this date, so 02:30 names an instant already passed in the second pass.
TEST(Scheduler, CronNextExecutionIsNeverDueAcrossAFallback) {
  using namespace std::chrono;
  HandleTimezone htz;
  htz.Set("Europe/Rome");

  memgraph::utils::Scheduler scheduler;
  scheduler.SetInterval("0 30 2 * * *");

  const auto day = sys_days{2026y / October / 25};
  const auto first_pass = time_point_cast<system_clock::duration>(day + 30min);
  const auto next_day = time_point_cast<system_clock::duration>(sys_days{2026y / October / 26} + 90min);

  // Naming the instant is what distinguishes the two ways of never being due. Before the repeated
  // hour the schedule is still ahead, at its earlier reading; from inside it the answer is the
  // following day, so the hour's second 02:30 is skipped rather than run late. That skip is a
  // known limitation: recovering it needs a search over both readings, which choosing the later
  // one does not give.
  const auto expected = {std::pair{0min, first_pass},
                         std::pair{45min, next_day},
                         std::pair{65min, next_day},
                         std::pair{75min, next_day},
                         std::pair{89min, next_day},
                         std::pair{95min, next_day},
                         std::pair{125min, next_day}};

  for (const auto &[offset, want] : expected) {
    const auto now = time_point_cast<system_clock::duration>(day + offset);
    const auto next = scheduler.NextExecution(now);
    ASSERT_TRUE(next.has_value()) << "no next execution at UTC midnight +" << offset.count() << " min";
    EXPECT_GT(*next, now) << "at UTC midnight +" << offset.count() << " min";
    EXPECT_EQ(*next, want) << "at UTC midnight +" << offset.count() << " min";
  }
}

// Rome advances 02:00 to 03:00 local on this date, so the 02:30 this expression names does not
// occur. The counterpart of the fallback case: there the named time happens twice, here not at all.
TEST(Scheduler, CronNextExecutionIsNeverDueAcrossASpringForward) {
  using namespace std::chrono;
  HandleTimezone htz;
  htz.Set("Europe/Rome");

  memgraph::utils::Scheduler scheduler;
  scheduler.SetInterval("0 30 2 * * *");

  const auto day = sys_days{2026y / March / 29};
  const auto transition = time_point_cast<system_clock::duration>(day + 1h);

  for (const auto offset : {0min, 30min, 55min, 65min, 95min, 125min}) {
    const auto now = time_point_cast<system_clock::duration>(day + offset);
    const auto next = scheduler.NextExecution(now);
    ASSERT_TRUE(next.has_value()) << "no next execution at UTC midnight +" << offset.count() << " min";
    EXPECT_GT(*next, now) << "at UTC midnight +" << offset.count() << " min";
    // A day the expression cannot name costs at most that day. Which of the two answers comes back
    // depends on how far into the skipped hour the search starts, because croncpp normalises each
    // candidate through mktime, so only the bound is asserted per offset.
    EXPECT_LE(*next, now + 25h) << "at UTC midnight +" << offset.count() << " min";
  }

  // Asked before the hour goes missing, the schedule answers with the instant the clocks move,
  // which is where a calendar time inside the gap resolves to.
  EXPECT_EQ(scheduler.NextExecution(time_point_cast<system_clock::duration>(day)), transition);
}

// The 30th of February passes validation, since each field is in range on its own, and matches nothing.
TEST(Scheduler, CronThatNeverMatchesHasNoNextExecution) {
  memgraph::utils::Scheduler scheduler;
  scheduler.SetInterval("0 0 0 30 2 *");

  std::optional<memgraph::utils::Scheduler::time_point> next;
  ASSERT_NO_THROW(next = scheduler.NextExecution(std::chrono::system_clock::now()));
  EXPECT_FALSE(next.has_value());
}

#endif

TEST(Scheduler, SkipSlowExecutions) {
  std::atomic<int> x{0};
  std::atomic_bool executed{false};
  std::function<void()> func{[&x, &executed]() {
    bool first = ++x == 1;
    if (first) std::this_thread::sleep_for(std::chrono::seconds(5));
    executed.store(true);
    executed.notify_one();
  }};
  memgraph::utils::Scheduler scheduler;
  scheduler.SetInterval(std::chrono::seconds(1));
  scheduler.Run("Test", func);

  // Wait for first execution to start
  while (x < 1) std::this_thread::sleep_for(std::chrono::milliseconds(100));
  // Wait for first execution to end
  executed.wait(false);

  // Loop for a while and check that 5 missing execution are not run
  const auto test_end = std::chrono::system_clock::now() + std::chrono::seconds(1);
  while (std::chrono::system_clock::now() < test_end) {
    ASSERT_LE(x.load(), 3);
    std::this_thread::sleep_for(std::chrono::milliseconds(100));
  }

  scheduler.Stop();
}

TEST(Scheduler, TimeDrift) {
  std::atomic<int> x{0};
  const auto start_time = std::chrono::system_clock::now();
  const auto period = std::chrono::milliseconds(500);

  std::function<void()> func{[&x, first_now = std::chrono::system_clock::time_point{}, start_time, period]() mutable {
    ++x;
    const auto now = std::chrono::system_clock::now();
    if (x == 1) {
      // First - cause a time drift
      std::this_thread::sleep_for(std::chrono::seconds(2));
      first_now = std::chrono::system_clock::now();
    } else if (x == 2) {
      // Second - next execution, execute right away
      EXPECT_NEAR(
          first_now.time_since_epoch().count(),
          now.time_since_epoch().count(),
          std::chrono::duration_cast<std::chrono::system_clock::duration>(std::chrono::milliseconds(100)).count());
    } else {
      // From 3. should go back to the original time offset
      const auto delta = now - start_time;
      EXPECT_NEAR(delta / period, x.load() + 3.0 /* skipped executions */, 0.1);
    }
  }};

  memgraph::utils::Scheduler scheduler;
  scheduler.SetInterval(period, start_time);
  scheduler.Run("Test", func);

  // Wait for a few executions and check
  std::this_thread::sleep_for(std::chrono::seconds(5));
  ASSERT_GE(x.load(), 3);

  scheduler.Stop();
}

namespace {
// Keeps the payload of every message logged while it is installed. The scheduler logs from its own
// thread, so reading what it wrote needs its own lock rather than the sink's.
class CapturingSink : public spdlog::sinks::base_sink<std::mutex> {
 public:
  std::vector<std::string> Messages() const {
    auto guard = std::lock_guard{messages_mutex_};
    return messages_;
  }

 protected:
  void sink_it_(const spdlog::details::log_msg &msg) override {
    auto guard = std::lock_guard{messages_mutex_};
    messages_.emplace_back(msg.payload.data(), msg.payload.size());
  }

  void flush_() override {}

 private:
  mutable std::mutex messages_mutex_;
  std::vector<std::string> messages_;
};

// Runs a task that always throws until it has been seen to run more than once, and returns what the
// scheduler logged meanwhile. A regression aborts this binary: a death test would instead fork a
// process that has threads and cannot join them.
std::vector<std::string> LogsWhileATaskKeepsFailing(const std::string &service_name,
                                                    const std::function<void()> &throw_something) {
  auto sink = std::make_shared<CapturingSink>();
  auto previous_logger = spdlog::default_logger();
  auto logger = std::make_shared<spdlog::logger>("scheduler_test", sink);
  logger->set_level(spdlog::level::trace);
  spdlog::set_default_logger(logger);
  const memgraph::utils::OnScopeExit restore_logger(
      [&previous_logger] { spdlog::set_default_logger(previous_logger); });

  std::atomic<int> runs{0};
  memgraph::utils::Scheduler scheduler;
  scheduler.SetInterval(std::chrono::milliseconds(50));
  scheduler.Run(service_name, [&runs, &throw_something] {
    ++runs;
    throw_something();
  });

  const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(10);
  while (runs.load() <= 1 && std::chrono::steady_clock::now() < deadline) {
    std::this_thread::sleep_for(std::chrono::milliseconds(10));
  }
  scheduler.Stop();

  // A scheduler that gave up after the first failure would also have survived.
  EXPECT_GT(runs.load(), 1) << "the task ran once, so surviving says nothing about the schedule";
  return sink->Messages();
}

bool AnyMessageContains(const std::vector<std::string> &messages, std::string_view fragment) {
  return std::ranges::any_of(messages,
                             [fragment](const auto &message) { return message.find(fragment) != std::string::npos; });
}
}  // namespace

TEST(Scheduler, ThrowingTaskKeepsSchedulerRunning) {
  const auto messages =
      LogsWhileATaskKeepsFailing("Throwing", [] { throw memgraph::utils::BasicException("the task gave up"); });

  // Naming the service is the whole value of the log line: one failing schedule among several is
  // otherwise unattributable.
  EXPECT_TRUE(AnyMessageContains(messages, "Throwing")) << "nothing logged named the failing schedule";
  EXPECT_TRUE(AnyMessageContains(messages, "the task gave up")) << "nothing logged carried the reason";
}

// A task throwing something that does not derive from std::exception reaches the second handler,
// which cannot report a reason and so has to name the schedule.
TEST(Scheduler, TaskThrowingANonExceptionKeepsSchedulerRunning) {
  const auto messages =
      LogsWhileATaskKeepsFailing("Unrecognised", [] { throw 42; });  // NOLINT(hicpp-exception-baseclass)

  EXPECT_TRUE(AnyMessageContains(messages, "Unrecognised")) << "nothing logged named the failing schedule";
  EXPECT_TRUE(AnyMessageContains(messages, "unrecognised exception")) << "nothing logged reported an unknown throw";
}
