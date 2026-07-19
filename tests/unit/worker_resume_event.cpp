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

#include "gtest/gtest.h"

#include <atomic>
#include <coroutine>
#include <cstddef>
#include <cstdint>
#include <thread>
#include <vector>

#include "utils/worker_resume_event.hpp"

using memgraph::utils::WorkerResumeEvent;

namespace {

// Minimal lazy coroutine type used purely to obtain real std::coroutine_handle<>s for exercising
// WorkerResumeEvent. It suspends at both initial and final suspend, so:
//   - constructing it creates a suspended frame without running the body (the body only runs on
//     the first .resume(), which is exactly the "waiter gets resumed" event we want to observe);
//   - after being resumed once, the frame stays alive (suspended at final_suspend) so tests can
//     assert `.done()` and then explicitly `.destroy()` it -- no leaks.
struct TestCoro {
  struct promise_type {
    TestCoro get_return_object() { return TestCoro{std::coroutine_handle<promise_type>::from_promise(*this)}; }

    std::suspend_always initial_suspend() noexcept { return {}; }

    std::suspend_always final_suspend() noexcept { return {}; }

    void return_void() noexcept {}

    void unhandled_exception() { std::terminate(); }
  };

  std::coroutine_handle<promise_type> handle;
};

// A coroutine body that records exactly one resume into *counter, then completes. Used both as
// "did this handle get resumed" evidence and, via fetch_add, as a double-resume detector.
TestCoro MakeRecordingCoro(std::atomic<int> *counter) {
  counter->fetch_add(1, std::memory_order_relaxed);
  co_return;
}

// No-op reschedule: does not resume anything. Used where a test only cares about NotifyAll's
// bookkeeping (epoch bump, waiters_pending_ reset) and not about actually driving the coroutine.
void NoopReschedule(std::coroutine_handle<> /*handle*/, size_t /*worker_id*/) {}

}  // namespace

// (a) RegisterWaiter with the current epoch succeeds and is reflected in WaitersPending().
TEST(WorkerResumeEvent, RegisterWaiterCurrentEpochSucceeds) {
  WorkerResumeEvent event;
  std::atomic<int> resumed{0};
  TestCoro coro = MakeRecordingCoro(&resumed);
  std::coroutine_handle<> handle = coro.handle;

  const uint64_t epoch = event.Epoch();
  EXPECT_EQ(epoch, 0u);
  EXPECT_TRUE(event.RegisterWaiter(handle, /*worker_id=*/1, epoch));
  EXPECT_EQ(event.WaitersPending(), 1u);

  // Never resumed in this test -- unregister and destroy the still-suspended frame manually.
  EXPECT_TRUE(event.RemoveWaiter(handle));
  EXPECT_EQ(event.WaitersPending(), 0u);
  handle.destroy();
  EXPECT_EQ(resumed.load(), 0);
}

// (b) RegisterWaiter with a stale epoch (captured before a NotifyAll bumped it) fails and does
// NOT enqueue the waiter.
TEST(WorkerResumeEvent, RegisterWaiterStaleEpochFails) {
  WorkerResumeEvent event;
  const uint64_t stale_epoch = event.Epoch();

  // Bump the epoch via a NotifyAll with no registered waiters.
  event.NotifyAll(NoopReschedule);
  EXPECT_NE(event.Epoch(), stale_epoch);

  std::atomic<int> resumed{0};
  TestCoro coro = MakeRecordingCoro(&resumed);
  std::coroutine_handle<> handle = coro.handle;

  EXPECT_FALSE(event.RegisterWaiter(handle, /*worker_id=*/2, stale_epoch));
  EXPECT_EQ(event.WaitersPending(), 0u);

  // Never enqueued, never resumed -- just destroy the frame.
  handle.destroy();
  EXPECT_EQ(resumed.load(), 0);
}

// (c) NotifyAll resumes every registered waiter exactly once, bumps the epoch, and resets
// WaitersPending() to 0.
TEST(WorkerResumeEvent, NotifyAllResumesEveryWaiterExactlyOnce) {
  WorkerResumeEvent event;
  constexpr int kNumWaiters = 3;
  std::vector<std::atomic<int>> resumed(kNumWaiters);
  std::vector<TestCoro> coros;
  coros.reserve(kNumWaiters);
  for (int i = 0; i < kNumWaiters; ++i) {
    coros.push_back(MakeRecordingCoro(&resumed[i]));
  }

  const uint64_t epoch = event.Epoch();
  for (int i = 0; i < kNumWaiters; ++i) {
    EXPECT_TRUE(event.RegisterWaiter(coros[i].handle, static_cast<size_t>(i), epoch));
  }
  EXPECT_EQ(event.WaitersPending(), static_cast<size_t>(kNumWaiters));

  std::vector<size_t> rescheduled_worker_ids;
  event.NotifyAll([&](std::coroutine_handle<> handle, size_t worker_id) {
    rescheduled_worker_ids.push_back(worker_id);
    handle.resume();
  });

  EXPECT_EQ(event.Epoch(), epoch + 1);
  EXPECT_EQ(event.WaitersPending(), 0u);
  ASSERT_EQ(rescheduled_worker_ids.size(), static_cast<size_t>(kNumWaiters));
  for (int i = 0; i < kNumWaiters; ++i) {
    EXPECT_EQ(resumed[i].load(), 1) << "waiter " << i << " resumed " << resumed[i].load() << " times";
    EXPECT_TRUE(coros[i].handle.done());
    coros[i].handle.destroy();
  }
}

// (d) RemoveWaiter decrements WaitersPending() and prevents that handle from being resumed by a
// later NotifyAll.
TEST(WorkerResumeEvent, RemoveWaiterPreventsLaterResume) {
  WorkerResumeEvent event;
  std::atomic<int> resumed_removed{0};
  std::atomic<int> resumed_kept{0};
  TestCoro removed_coro = MakeRecordingCoro(&resumed_removed);
  TestCoro kept_coro = MakeRecordingCoro(&resumed_kept);

  const uint64_t epoch = event.Epoch();
  EXPECT_TRUE(event.RegisterWaiter(removed_coro.handle, /*worker_id=*/10, epoch));
  EXPECT_TRUE(event.RegisterWaiter(kept_coro.handle, /*worker_id=*/11, epoch));
  EXPECT_EQ(event.WaitersPending(), 2u);

  EXPECT_TRUE(event.RemoveWaiter(removed_coro.handle));
  EXPECT_EQ(event.WaitersPending(), 1u);
  // A second removal of the same handle must fail (already gone) and must not underflow the
  // pending counter.
  EXPECT_FALSE(event.RemoveWaiter(removed_coro.handle));
  EXPECT_EQ(event.WaitersPending(), 1u);

  std::vector<size_t> rescheduled_worker_ids;
  event.NotifyAll([&](std::coroutine_handle<> handle, size_t worker_id) {
    rescheduled_worker_ids.push_back(worker_id);
    handle.resume();
  });

  EXPECT_EQ(resumed_removed.load(), 0) << "removed waiter must not be resumed by a later NotifyAll";
  EXPECT_EQ(resumed_kept.load(), 1);
  ASSERT_EQ(rescheduled_worker_ids.size(), 1u);
  EXPECT_EQ(rescheduled_worker_ids[0], 11u);

  // removed_coro was never resumed -- still suspended at initial_suspend; destroy manually.
  removed_coro.handle.destroy();
  EXPECT_TRUE(kept_coro.handle.done());
  kept_coro.handle.destroy();
}

// (e) Single-owner resume: two back-to-back NotifyAll calls resume each waiter exactly once
// total -- the second call sees an empty waiter list.
TEST(WorkerResumeEvent, BackToBackNotifyAllResumesOnce) {
  WorkerResumeEvent event;
  std::atomic<int> resumed{0};
  TestCoro coro = MakeRecordingCoro(&resumed);

  const uint64_t epoch = event.Epoch();
  EXPECT_TRUE(event.RegisterWaiter(coro.handle, /*worker_id=*/5, epoch));

  int first_call_count = 0;
  event.NotifyAll([&](std::coroutine_handle<> handle, size_t /*worker_id*/) {
    ++first_call_count;
    handle.resume();
  });
  EXPECT_EQ(first_call_count, 1);
  EXPECT_EQ(resumed.load(), 1);

  int second_call_count = 0;
  event.NotifyAll([&](std::coroutine_handle<> /*handle*/, size_t /*worker_id*/) { ++second_call_count; });
  EXPECT_EQ(second_call_count, 0) << "second NotifyAll must see an empty waiter list (single-owner resume)";
  EXPECT_EQ(resumed.load(), 1) << "waiter must not be double-resumed";

  EXPECT_TRUE(coro.handle.done());
  coro.handle.destroy();
}

// (f) Multi-threaded stress: many threads each capture the epoch and register a real coroutine
// handle while dedicated notifier threads repeatedly call NotifyAll. Asserts every handle is
// resumed exactly once and none is lost or double-resumed -- i.e. the register-before-recheck /
// release-before-check protocol documented in worker_resume_event.hpp (R1 B1) actually holds
// under contention, not just in single-threaded call order.
TEST(WorkerResumeEvent, ConcurrentRegisterAndNotifyResumesEachExactlyOnce) {
  WorkerResumeEvent event;
  constexpr int kNumWaiters = 32;
  constexpr int kNumNotifiers = 3;
  constexpr int kMaxRegisterAttempts = 2'000'000;

  std::vector<std::atomic<int>> resume_counts(kNumWaiters);
  std::vector<std::atomic<bool>> registered_ok(kNumWaiters);
  for (auto &flag : registered_ok) flag.store(false, std::memory_order_relaxed);

  std::atomic<bool> stop{false};

  auto reschedule = [](std::coroutine_handle<> handle, size_t /*worker_id*/) { handle.resume(); };

  std::vector<std::thread> notifier_threads;
  notifier_threads.reserve(kNumNotifiers);
  for (int n = 0; n < kNumNotifiers; ++n) {
    notifier_threads.emplace_back([&event, &reschedule, &stop] {
      while (!stop.load(std::memory_order_relaxed)) {
        event.NotifyAll(reschedule);
        std::this_thread::yield();
      }
      // Final drain: catch anything registered just before `stop` was observed.
      event.NotifyAll(reschedule);
    });
  }

  std::vector<std::thread> waiter_threads;
  waiter_threads.reserve(kNumWaiters);
  for (int i = 0; i < kNumWaiters; ++i) {
    waiter_threads.emplace_back([&event, &resume_counts, &registered_ok, i] {
      TestCoro coro = MakeRecordingCoro(&resume_counts[i]);
      std::coroutine_handle<> handle = coro.handle;

      bool registered = false;
      for (int attempt = 0; attempt < kMaxRegisterAttempts && !registered; ++attempt) {
        const uint64_t epoch = event.Epoch();
        registered = event.RegisterWaiter(handle, static_cast<size_t>(i), epoch);
        if (!registered) std::this_thread::yield();
      }
      registered_ok[i].store(registered, std::memory_order_relaxed);

      if (!registered) {
        // Could not register within the attempt budget -- treat as a failure to be reported on
        // the main thread; still must not leak the frame.
        handle.destroy();
        return;
      }

      // A notifier thread is looping continuously until every waiter thread (including this one)
      // has joined, so this handle is guaranteed to be picked up by some NotifyAll eventually.
      while (resume_counts[i].load(std::memory_order_acquire) == 0) {
        std::this_thread::yield();
      }
      handle.destroy();
    });
  }

  for (auto &t : waiter_threads) t.join();
  stop.store(true, std::memory_order_relaxed);
  for (auto &t : notifier_threads) t.join();

  for (int i = 0; i < kNumWaiters; ++i) {
    EXPECT_TRUE(registered_ok[i].load()) << "waiter " << i << " failed to register within the attempt budget";
    EXPECT_EQ(resume_counts[i].load(), 1) << "waiter " << i << " resumed " << resume_counts[i].load() << " times";
  }
  EXPECT_EQ(event.WaitersPending(), 0u);
}
