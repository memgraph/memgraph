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

#include <algorithm>
#include <array>
#include <atomic>
#include <chrono>
#include <condition_variable>
#include <cstddef>
#include <mutex>
#include <stop_token>
#include <thread>

#include <utils/priority_thread_pool.hpp>
#include "utils/synchronized.hpp"
#include "utils/tsc.hpp"

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

namespace {

struct FakeWork final : memgraph::utils::IdleRunnable {
  std::atomic<int> runs{0};
  std::atomic<int> dispatched{0};

  void RunInline(memgraph::utils::Priority /*unused*/) override { runs.fetch_add(1); }

  void Dispatch() override { dispatched.fetch_add(1); }
};

// WaitAndDispatch sleeps until `max` elapses or Wake().
struct WaitablePoller : memgraph::utils::IdlePoller {
  std::shared_ptr<memgraph::utils::IdleRunnable> TryClaim() override { return nullptr; }

  void WaitAndDispatch(std::chrono::milliseconds max) override {
    waits.fetch_add(1);
    max_requested_ms.store(std::max<int64_t>(max_requested_ms.load(), max.count()));
    auto lk = std::unique_lock{mtx};
    cv.wait_for(lk, max, [this] { return woken; });
    woken = false;
  }

  void Wake() override {
    {
      auto lk = std::unique_lock{mtx};
      woken = true;
    }
    cv.notify_all();
  }

  std::mutex mtx;
  std::condition_variable cv;
  bool woken{false};
  std::atomic<int> waits{0};
  std::atomic<int64_t> max_requested_ms{0};
};

// Only a worker that finishes a task and spins reaches the idle poller, so keep the workers cycling until `pred`.
template <typename Pred>
bool PumpUntil(memgraph::utils::PriorityThreadPool &pool, Pred pred, std::chrono::steady_clock::duration timeout,
               std::chrono::microseconds pause = 1ms) {
  const auto deadline = std::chrono::steady_clock::now() + timeout;
  while (!pred() && std::chrono::steady_clock::now() < deadline) {
    pool.ScheduledAddTask([](auto) {}, memgraph::utils::Priority::LOW);
    std::this_thread::sleep_for(pause);
  }
  return pred();
}

}  // namespace

TEST(PriorityThreadPool, IdlePollerWorkRunsInlineOnAWorkerOnce) {
  using namespace memgraph;
  if (!utils::IsAvailableTSC()) GTEST_SKIP() << "idle workers only spin (and poll) when the TSC is available";

  constexpr int kOffers = 5;

  struct Poller final : WaitablePoller {
    std::array<std::shared_ptr<FakeWork>, kOffers> works;
    std::atomic<int> calls{0};
    std::atomic<int> concurrent{0};
    std::atomic<int> max_concurrent{0};

    Poller() {
      for (auto &w : works) w = std::make_shared<FakeWork>();
    }

    std::shared_ptr<utils::IdleRunnable> TryClaim() override {
      const auto now = concurrent.fetch_add(1) + 1;
      auto seen = max_concurrent.load();
      while (now > seen && !max_concurrent.compare_exchange_weak(seen, now)) {
      }
      const auto n = calls.fetch_add(1);
      std::this_thread::yield();                      // widen the window in which a second poller could overlap
      if (n == 0) std::this_thread::sleep_for(50ms);  // hold the token while the other workers keep cycling
      concurrent.fetch_sub(1);
      const auto offer = n / 100;
      return (n % 100 == 99 && offer < kOffers) ? works[offer] : nullptr;
    }
  };

  Poller poller;  // outlives the pool
  {
    utils::PriorityThreadPool pool{3, 1};
    pool.SetIdlePoller(&poller);
    // A claim that races with queued work is legitimately handed back (Dispatch), so "handled" is runs + dispatched.
    const auto all_handled = [&] {
      return std::ranges::all_of(poller.works,
                                 [](const auto &w) { return w->runs.load() + w->dispatched.load() >= 1; });
    };
    ASSERT_TRUE(PumpUntil(pool, all_handled, 30s));
    pool.ClearIdlePoller();
  }
  int total_runs = 0;
  for (const auto &w : poller.works) {
    EXPECT_EQ(w->runs.load() + w->dispatched.load(), 1);
    total_runs += w->runs.load();
  }
  EXPECT_GE(total_runs, 1);                    // the inline path is exercised (a hand-back needs a rare producer race)
  EXPECT_EQ(poller.max_concurrent.load(), 1);  // the pool lets one worker poll at a time
}

TEST(PriorityThreadPool, ClaimAfterAProducerTookTheHotBitIsDispatchedNotRunInline) {
  using namespace memgraph;
  if (!utils::IsAvailableTSC()) GTEST_SKIP() << "idle workers only spin (and poll) when the TSC is available";

  // The producer has taken the worker's hot bit but not yet queued its task, so has_pending_work_ is still false:
  // only the cleared bit tells the worker that work is on its way (also covers work queued in the claim window).
  struct Poller final : WaitablePoller {
    utils::PriorityThreadPool *pool{nullptr};
    std::shared_ptr<FakeWork> work = std::make_shared<FakeWork>();
    std::atomic_bool go{false};
    std::atomic_bool fired{false};
    std::atomic_bool took_bit{false};
    std::atomic_bool release{false};
    std::atomic_bool task_ran{false};
    std::atomic<int> calls{0};
    std::thread producer;

    std::shared_ptr<utils::IdleRunnable> TryClaim() override {
      calls.fetch_add(1);
      if (!go.load() || fired.exchange(true)) return nullptr;
      producer = std::thread([this] {
        pool->GetHotMask().GetHotElement();
        took_bit = true;
        release.wait(false);
        pool->ScheduledAddTask([this](auto) { task_ran = true; }, utils::Priority::LOW);
      });
      const auto deadline = std::chrono::steady_clock::now() + 5s;
      while (!took_bit.load() && std::chrono::steady_clock::now() < deadline) std::this_thread::yield();
      return work;
    }
  };

  Poller poller;
  {
    utils::PriorityThreadPool pool{1, 1};
    poller.pool = &pool;
    pool.SetIdlePoller(&poller);
    ASSERT_TRUE(PumpUntil(pool, [&] { return poller.calls.load() > 0; }, 10s));
    std::this_thread::sleep_for(10ms);  // drain: nothing may be queued when the claim happens
    poller.go = true;
    pool.ScheduledAddTask([](auto) {}, utils::Priority::LOW);
    const auto claim_deadline = std::chrono::steady_clock::now() + 10s;
    while (poller.work->runs.load() + poller.work->dispatched.load() == 0 &&
           std::chrono::steady_clock::now() < claim_deadline) {
      std::this_thread::sleep_for(100us);
    }
    EXPECT_EQ(poller.work->runs.load(), 0);
    EXPECT_EQ(poller.work->dispatched.load(), 1);

    poller.release = true;
    poller.release.notify_all();
    const auto ran_deadline = std::chrono::steady_clock::now() + 5s;
    while (!poller.task_ran.load() && std::chrono::steady_clock::now() < ran_deadline) std::this_thread::sleep_for(1ms);
    EXPECT_TRUE(poller.task_ran.load());
    if (poller.producer.joinable()) poller.producer.join();
    pool.ClearIdlePoller();
  }
}

TEST(HotMask, WaitUntilEmptyWakesWhenLastBitIsCleared) {
  using memgraph::utils::HotMask;
  using Clock = std::chrono::steady_clock;
  HotMask mask{4};
  std::stop_source stop;
  // Short deadline: a missing wake-up shows up as a timeout-length latency, not just a slow test.
  const auto park = [&] { return mask.WaitUntilEmpty(Clock::now() + 200ms, stop.get_token()); };
  ASSERT_TRUE(mask.SingleWord());
  mask.EnableWaiter();
  mask.Set(1);
  mask.Set(3);

  for (int round = 0; round < 20; ++round) {
    std::atomic<Clock::rep> returned_at{0};
    std::thread waiter([&] {
      while (mask.AnyHot()) park();
      returned_at = Clock::now().time_since_epoch().count();
    });
    std::this_thread::sleep_for(5ms);
    mask.Reset(1);
    const auto reset_at = Clock::now();
    mask.Reset(3);  // transition to empty must wake the parked waiter
    waiter.join();
    ASSERT_NE(returned_at.load(), 0);
    const auto latency = Clock::time_point{Clock::duration{returned_at.load()}} - reset_at;
    EXPECT_LT(latency, 100ms) << "round " << round;
    mask.Set(1);
    mask.Set(3);
  }

  // GetHotElement emptying the mask wakes as well.
  std::atomic<Clock::rep> returned_at{0};
  std::thread waiter([&] {
    while (mask.AnyHot()) park();
    returned_at = Clock::now().time_since_epoch().count();
  });
  std::this_thread::sleep_for(5ms);
  ASSERT_TRUE(mask.GetHotElement());
  ASSERT_TRUE(mask.GetHotElement());
  const auto emptied_at = Clock::now();
  waiter.join();
  ASSERT_NE(returned_at.load(), 0);
  EXPECT_LT(Clock::time_point{Clock::duration{returned_at.load()}} - emptied_at, 100ms);

  // Stop wakes a waiter whose mask is still hot.
  mask.Set(2);
  std::atomic<Clock::rep> stopped_at{0};
  std::thread stopped([&] {
    park();
    stopped_at = Clock::now().time_since_epoch().count();
  });
  std::this_thread::sleep_for(5ms);
  stop.request_stop();
  mask.WakeWaiter();
  const auto stop_at = Clock::now();
  stopped.join();
  ASSERT_NE(stopped_at.load(), 0);
  EXPECT_LT(Clock::time_point{Clock::duration{stopped_at.load()}} - stop_at, 100ms);
}

TEST(HotMask, WaitUntilEmptyTimesOutWhileHot) {
  using memgraph::utils::HotMask;
  HotMask mask{4};
  std::stop_source stop;
  mask.Set(2);

  const auto start = std::chrono::steady_clock::now();
  const auto deadline = start + 20ms;
  EXPECT_FALSE(mask.WaitUntilEmpty(deadline, stop.get_token()));
  const auto elapsed = std::chrono::steady_clock::now() - start;
  EXPECT_GE(elapsed, 15ms);
  EXPECT_LT(elapsed, 1s);
  EXPECT_TRUE(mask.AnyHot());

  EXPECT_FALSE(mask.WaitUntilEmpty(start, stop.get_token()));
  mask.Reset(2);
  EXPECT_TRUE(mask.WaitUntilEmpty(std::chrono::steady_clock::now() + 10s, stop.get_token()));
}

TEST(HotMask, ParkRacingTheLastResetNeverLosesTheWakeup) {
  using memgraph::utils::HotMask;
  HotMask mask{2};
  mask.EnableWaiter();
  std::stop_source stop;

  // Persistent waiter; per round it announces it is about to park, so Reset lands in the park window.
  std::atomic<int> go{0};
  std::atomic<int> about_to_park{0};
  std::atomic<int> done{0};
  std::atomic_bool quit{false};
  std::thread waiter([&] {
    for (int seen = 0; !quit.load();) {
      const int round = go.load(std::memory_order_acquire);
      if (round == seen) {
        std::this_thread::yield();
        continue;
      }
      seen = round;
      about_to_park.store(round, std::memory_order_release);
      while (mask.AnyHot() && !stop.stop_requested()) {
        mask.WaitUntilEmpty(std::chrono::steady_clock::now() + 30s, stop.get_token());
      }
      done.store(round, std::memory_order_release);
    }
  });
  const auto wait_for = [](const std::atomic<int> &v, int want) {
    const auto deadline = std::chrono::steady_clock::now() + 5s;
    while (v.load(std::memory_order_acquire) != want && std::chrono::steady_clock::now() < deadline) {
      std::this_thread::yield();
    }
    return v.load(std::memory_order_acquire) == want;
  };

  constexpr int kRounds = 5000;
  bool ok = true;
  int failed_round = 0;
  for (int round = 1; round <= kRounds && ok; ++round) {
    mask.Set(0);
    go.store(round, std::memory_order_release);
    ok = wait_for(about_to_park, round);
    for (int i = 0, n = (round * 37) % 256; i < n; ++i) asm volatile("" ::: "memory");  // vary where Reset lands
    mask.Reset(0);
    ok = ok && wait_for(done, round);
    failed_round = round;
  }
  if (!ok) {  // release the stuck waiter so the test can fail instead of hanging
    stop.request_stop();
    mask.WakeWaiter();
  }
  quit = true;
  waiter.join();
  ASSERT_TRUE(ok) << "lost wake-up in round " << failed_round;
}

TEST(PriorityThreadPool, MonitorPollsWhenIdleAndShutDownWakesIt) {
  using namespace memgraph;
  WaitablePoller poller;
  std::chrono::steady_clock::duration shutdown_took{};
  {
    utils::PriorityThreadPool pool{2, 1};
    pool.SetIdlePoller(&poller);
    const auto deadline = std::chrono::steady_clock::now() + 5s;
    while (poller.waits.load() == 0 && std::chrono::steady_clock::now() < deadline) std::this_thread::sleep_for(1ms);
    ASSERT_GE(poller.waits.load(), 1);
    std::this_thread::sleep_for(2ms);  // let the monitor settle inside WaitAndDispatch
    const auto start = std::chrono::steady_clock::now();
    pool.ShutDown();
    shutdown_took = std::chrono::steady_clock::now() - start;
  }
  EXPECT_LT(shutdown_took, 70ms);                  // Wake() ended the poller wait, not the 100 ms tick
  EXPECT_LE(poller.max_requested_ms.load(), 100);  // a wait never outlasts the tick
}

TEST(PriorityThreadPool, AttachWhileMonitorIsInItsCvWaitWakesItAtOnce) {
  using namespace memgraph;
  using Clock = std::chrono::steady_clock;
  WaitablePoller poller;
  utils::PriorityThreadPool pool{2, 1};
  // The monitor ticks every 100 ms: 110 ms in, it is mid-tick (~90 ms from the next one) in its cv wait.
  std::this_thread::sleep_for(110ms);
  const auto attached = Clock::now();
  pool.SetIdlePoller(&poller);
  while (poller.waits.load() == 0 && Clock::now() < attached + 5s) std::this_thread::sleep_for(100us);
  const auto took = Clock::now() - attached;
  ASSERT_GE(poller.waits.load(), 1);
  EXPECT_LT(took, 60ms);  // not the remainder of the tick
  pool.ClearIdlePoller();
}

TEST(PriorityThreadPool, MonitorStaysOutOfThePollerWhileWorkersAreHot) {
  using namespace memgraph;
  using Clock = std::chrono::steady_clock;
  if (!utils::IsAvailableTSC()) GTEST_SKIP() << "hot workers (and the monitor's park) need the TSC";

  // A worker's hot bit stays set for its whole Phase 3 spin, which includes TryClaim: parking one worker inside
  // TryClaim holds the mask non-empty for as long as the test wants.
  struct HoldingPoller final : WaitablePoller {
    std::shared_ptr<utils::IdleRunnable> TryClaim() override {
      if (!held.exchange(true)) {
        in_claim = true;
        in_claim.notify_all();
        release.wait(false);
      }
      return nullptr;
    }

    std::atomic_bool held{false};
    std::atomic_bool in_claim{false};
    std::atomic_bool release{false};
  };

  HoldingPoller poller;
  utils::PriorityThreadPool pool{2, 1};
  pool.SetIdlePoller(&poller);
  ASSERT_TRUE(PumpUntil(pool, [&] { return poller.in_claim.load(); }, 10s));  // one worker parked in TryClaim

  std::this_thread::sleep_for(150ms);  // a WaitAndDispatch begun before the bit was set ends within one tick
  const auto before = poller.waits.load();
  std::this_thread::sleep_for(300ms);
  EXPECT_EQ(poller.waits.load(), before) << "monitor entered the poller while a worker was hot";

  poller.release = true;
  poller.release.notify_all();
  // The tick alone suffices here; the empty-transition wake is covered by the HotMask tests.
  const auto cold_deadline = Clock::now() + 2s;
  while (poller.waits.load() == before && Clock::now() < cold_deadline) std::this_thread::sleep_for(1ms);
  EXPECT_GT(poller.waits.load(), before);
  pool.ClearIdlePoller();
}

TEST(PriorityThreadPool, ClearIdlePollerWaitsForTheMonitorToLeaveThePoller) {
  using namespace memgraph;

  struct HeldPoller final : WaitablePoller {
    void WaitAndDispatch(std::chrono::milliseconds /*max*/) override {
      entered = true;
      const auto deadline = std::chrono::steady_clock::now() + 10s;
      while (!release.load() && std::chrono::steady_clock::now() < deadline) std::this_thread::sleep_for(1ms);
      returned = true;
    }

    void Wake() override {}  // deliberately does not interrupt: Clear must wait for the call itself

    std::atomic_bool entered{false};
    std::atomic_bool release{false};
    std::atomic_bool returned{false};
  };

  HeldPoller poller;
  utils::PriorityThreadPool pool{2, 1};
  pool.SetIdlePoller(&poller);
  const auto deadline = std::chrono::steady_clock::now() + 5s;
  while (!poller.entered.load() && std::chrono::steady_clock::now() < deadline) std::this_thread::sleep_for(1ms);
  ASSERT_TRUE(poller.entered.load());

  std::atomic_bool cleared{false};
  bool returned_when_cleared = false;
  std::thread clearer([&] {
    pool.ClearIdlePoller();
    returned_when_cleared = poller.returned.load();
    cleared = true;
  });
  std::this_thread::sleep_for(100ms);
  EXPECT_FALSE(cleared.load());  // still blocked behind the in-flight WaitAndDispatch
  poller.release = true;
  clearer.join();
  EXPECT_TRUE(returned_when_cleared);
  pool.ShutDown();
}

TEST(PriorityThreadPool, NoTryClaimAfterClearIdlePollerReturns) {
  using namespace memgraph;
  if (!utils::IsAvailableTSC()) GTEST_SKIP() << "idle workers only spin (and poll) when the TSC is available";

  struct Poller final : WaitablePoller {
    std::shared_ptr<utils::IdleRunnable> TryClaim() override {
      in_try.fetch_add(1);
      if (cleared.load()) late_claim = true;
      if (calls.fetch_add(1) == 0) {  // the first claim stays in flight until released
        entered = true;
        const auto deadline = std::chrono::steady_clock::now() + 10s;
        while (!release.load() && std::chrono::steady_clock::now() < deadline) std::this_thread::sleep_for(1ms);
      }
      std::this_thread::yield();
      in_try.fetch_sub(1);
      return nullptr;
    }

    std::atomic<int> in_try{0};
    std::atomic<int> calls{0};
    std::atomic_bool entered{false};
    std::atomic_bool release{false};
    std::atomic_bool cleared{false};
    std::atomic_bool late_claim{false};
  };

  Poller poller;
  utils::PriorityThreadPool pool{3, 1};
  pool.SetIdlePoller(&poller);
  ASSERT_TRUE(PumpUntil(pool, [&] { return poller.entered.load(); }, 5s, 100us));

  std::atomic_bool cleared_returned{false};
  int in_try_when_cleared = -1;
  std::thread clearer([&] {
    pool.ClearIdlePoller();
    in_try_when_cleared = poller.in_try.load();
    cleared_returned = true;
  });
  const auto hold_end = std::chrono::steady_clock::now() + 100ms;
  PumpUntil(pool, [&] { return std::chrono::steady_clock::now() >= hold_end; }, 1s, 100us);
  EXPECT_FALSE(cleared_returned.load());  // still blocked behind the in-flight TryClaim
  poller.release = true;
  clearer.join();
  EXPECT_EQ(in_try_when_cleared, 0);
  poller.cleared = true;
  const auto end = std::chrono::steady_clock::now() + 100ms;
  PumpUntil(pool, [&] { return std::chrono::steady_clock::now() >= end; }, 1s, 100us);
  EXPECT_FALSE(poller.late_claim.load());
  pool.ShutDown();
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
