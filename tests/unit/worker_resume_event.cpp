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
#include <cstddef>
#include <cstdint>
#include <memory>
#include <mutex>
#include <thread>
#include <vector>

#include "utils/park_state.hpp"
#include "utils/worker_resume_event.hpp"

using memgraph::utils::ClaimPark;
using memgraph::utils::ParkState;
using memgraph::utils::WorkerResumeEvent;

namespace {

// Builds a ParkState whose on_resume increments *counter -- the recording-closure equivalent of
// the old "resume a real coroutine handle" evidence, but without needing a coroutine frame at all
// (ParkState::on_resume is now an opaque std::function<void()>, see utils/park_state.hpp).
std::shared_ptr<ParkState> MakeRecordingParkState(std::atomic<int> *counter, size_t worker_id) {
  auto ps = std::make_shared<ParkState>();
  ps->worker_id = worker_id;
  ps->on_resume = [counter] { counter->fetch_add(1, std::memory_order_relaxed); };
  return ps;
}

}  // namespace

// (a) RegisterWaiter with the current epoch succeeds and is reflected in WaitersPending().
TEST(WorkerResumeEvent, RegisterWaiterCurrentEpochSucceeds) {
  WorkerResumeEvent event;
  std::atomic<int> resumed{0};
  auto ps = MakeRecordingParkState(&resumed, /*worker_id=*/1);

  const uint64_t epoch = event.Epoch();
  EXPECT_EQ(epoch, 0u);
  EXPECT_TRUE(event.RegisterWaiter(ps, epoch));
  EXPECT_EQ(event.WaitersPending(), 1u);

  // Never resumed in this test -- best-effort unregister, exactly like the abandon path would do
  // after winning its own claim.
  EXPECT_TRUE(event.RemoveWaiter(ps));
  EXPECT_EQ(event.WaitersPending(), 0u);
  EXPECT_EQ(resumed.load(), 0);
}

// (b) RegisterWaiter with a stale epoch (captured before a NotifyAll bumped it) fails and does
// NOT enqueue the waiter.
TEST(WorkerResumeEvent, RegisterWaiterStaleEpochFails) {
  WorkerResumeEvent event;
  const uint64_t stale_epoch = event.Epoch();

  // Bump the epoch via a NotifyAll with no registered waiters.
  event.NotifyAll();
  EXPECT_NE(event.Epoch(), stale_epoch);

  std::atomic<int> resumed{0};
  auto ps = MakeRecordingParkState(&resumed, /*worker_id=*/2);

  EXPECT_FALSE(event.RegisterWaiter(ps, stale_epoch));
  EXPECT_EQ(event.WaitersPending(), 0u);
  EXPECT_EQ(resumed.load(), 0);
}

// (c) NotifyAll invokes on_resume for every registered waiter exactly once, bumps the epoch, and
// resets WaitersPending() to 0.
TEST(WorkerResumeEvent, NotifyAllResumesEveryWaiterExactlyOnce) {
  WorkerResumeEvent event;
  constexpr int kNumWaiters = 3;
  std::vector<std::atomic<int>> resumed(kNumWaiters);
  std::vector<std::shared_ptr<ParkState>> parks;
  parks.reserve(kNumWaiters);
  for (int i = 0; i < kNumWaiters; ++i) {
    parks.push_back(MakeRecordingParkState(&resumed[i], static_cast<size_t>(i)));
  }

  const uint64_t epoch = event.Epoch();
  for (auto &ps : parks) {
    EXPECT_TRUE(event.RegisterWaiter(ps, epoch));
  }
  EXPECT_EQ(event.WaitersPending(), static_cast<size_t>(kNumWaiters));

  event.NotifyAll();

  EXPECT_EQ(event.Epoch(), epoch + 1);
  EXPECT_EQ(event.WaitersPending(), 0u);
  for (int i = 0; i < kNumWaiters; ++i) {
    EXPECT_EQ(resumed[i].load(), 1) << "waiter " << i << " resumed " << resumed[i].load() << " times";
    EXPECT_TRUE(parks[i]->claimed.load(std::memory_order_acquire));
  }
}

// (d) RemoveWaiter decrements WaitersPending() and prevents that handle from being resumed by a
// later NotifyAll.
TEST(WorkerResumeEvent, RemoveWaiterPreventsLaterResume) {
  WorkerResumeEvent event;
  std::atomic<int> resumed_removed{0};
  std::atomic<int> resumed_kept{0};
  auto removed_ps = MakeRecordingParkState(&resumed_removed, /*worker_id=*/10);
  auto kept_ps = MakeRecordingParkState(&resumed_kept, /*worker_id=*/11);

  const uint64_t epoch = event.Epoch();
  EXPECT_TRUE(event.RegisterWaiter(removed_ps, epoch));
  EXPECT_TRUE(event.RegisterWaiter(kept_ps, epoch));
  EXPECT_EQ(event.WaitersPending(), 2u);

  EXPECT_TRUE(event.RemoveWaiter(removed_ps));
  EXPECT_EQ(event.WaitersPending(), 1u);
  // A second removal of the same waiter must fail (already gone) and must not underflow the
  // pending counter.
  EXPECT_FALSE(event.RemoveWaiter(removed_ps));
  EXPECT_EQ(event.WaitersPending(), 1u);

  event.NotifyAll();

  EXPECT_EQ(resumed_removed.load(), 0) << "removed waiter must not be resumed by a later NotifyAll";
  EXPECT_EQ(resumed_kept.load(), 1);
  EXPECT_FALSE(removed_ps->claimed.load(std::memory_order_acquire))
      << "a removed-but-never-claimed waiter must not read as claimed";
  EXPECT_TRUE(kept_ps->claimed.load(std::memory_order_acquire));
}

// (e) Single-owner resume: two back-to-back NotifyAll calls resume each waiter exactly once
// total -- the second call sees an empty waiter list.
TEST(WorkerResumeEvent, BackToBackNotifyAllResumesOnce) {
  WorkerResumeEvent event;
  std::atomic<int> resumed{0};
  auto ps = MakeRecordingParkState(&resumed, /*worker_id=*/5);

  const uint64_t epoch = event.Epoch();
  EXPECT_TRUE(event.RegisterWaiter(ps, epoch));

  event.NotifyAll();
  EXPECT_EQ(resumed.load(), 1);

  event.NotifyAll();
  EXPECT_EQ(resumed.load(), 1) << "waiter must not be double-resumed by a second NotifyAll";
}

// (f) Abandon-path claim (R4.3), WIN case: the awaitable's own re-probe succeeds and it wins
// ClaimPark on its own ParkState BEFORE any wake source does. on_resume must never fire -- the
// winning claimant is expected to drive its own continuation synchronously, not via on_resume --
// and a later NotifyAll on the (best-effort, not-yet-removed) entry must be a harmless no-op.
TEST(WorkerResumeEvent, AbandonPathWinPreventsLaterNotifyAllResume) {
  WorkerResumeEvent event;
  std::atomic<int> resumed{0};
  auto ps = MakeRecordingParkState(&resumed, /*worker_id=*/0);

  const uint64_t epoch = event.Epoch();
  ASSERT_TRUE(event.RegisterWaiter(ps, epoch));

  // Simulate the awaitable's own abandon-path claim winning before any wake source fires.
  EXPECT_TRUE(ClaimPark(*ps));
  EXPECT_EQ(resumed.load(), 0) << "the winning claimant runs its own continuation, not on_resume";

  // Even without a prompt RemoveWaiter, a later NotifyAll must observe already-claimed and do
  // nothing.
  event.NotifyAll();
  EXPECT_EQ(resumed.load(), 0) << "on_resume must never fire once another party already won the claim";
}

// (g) Abandon-path claim (R4.3), LOSE case: a concurrent NotifyAll wins the race first (as if a
// lock release happened between the waiter's re-probe and its own ClaimPark attempt). on_resume
// fires exactly once (via NotifyAll), and the caller's own subsequent ClaimPark attempt correctly
// observes the loss instead of double-invoking anything.
TEST(WorkerResumeEvent, AbandonPathLoseToNotifyAllResumesExactlyOnce) {
  WorkerResumeEvent event;
  std::atomic<int> resumed{0};
  auto ps = MakeRecordingParkState(&resumed, /*worker_id=*/0);

  const uint64_t epoch = event.Epoch();
  ASSERT_TRUE(event.RegisterWaiter(ps, epoch));

  // A concurrent NotifyAll (the lock-release wake path) wins first.
  event.NotifyAll();
  EXPECT_EQ(resumed.load(), 1);

  // The caller's own abandon-path claim attempt now loses -- it must not invoke on_resume itself.
  EXPECT_FALSE(ClaimPark(*ps));
  EXPECT_EQ(resumed.load(), 1) << "on_resume must be invoked exactly once total across both paths";
}

// (h) Drain() resumes (claims + invokes on_resume for) every currently-registered waiter exactly
// once, and a later NotifyAll on the now-empty event is a no-op.
TEST(WorkerResumeEvent, DrainResumesAllWaitersExactlyOnce) {
  WorkerResumeEvent event;
  constexpr int kNumWaiters = 4;
  std::vector<std::atomic<int>> resumed(kNumWaiters);
  std::vector<std::shared_ptr<ParkState>> parks;
  parks.reserve(kNumWaiters);

  const uint64_t epoch = event.Epoch();
  for (int i = 0; i < kNumWaiters; ++i) {
    auto ps = MakeRecordingParkState(&resumed[i], static_cast<size_t>(i));
    ASSERT_TRUE(event.RegisterWaiter(ps, epoch));
    parks.push_back(ps);
  }
  EXPECT_EQ(event.WaitersPending(), static_cast<size_t>(kNumWaiters));

  event.Drain();

  EXPECT_EQ(event.WaitersPending(), 0u);
  for (int i = 0; i < kNumWaiters; ++i) {
    EXPECT_EQ(resumed[i].load(), 1) << "waiter " << i << " drained " << resumed[i].load() << " times";
    EXPECT_TRUE(parks[i]->claimed.load(std::memory_order_acquire));
  }

  // Nothing left to notify -- and even if there were, a claimed entry must not be re-invoked.
  event.NotifyAll();
  for (int i = 0; i < kNumWaiters; ++i) {
    EXPECT_EQ(resumed[i].load(), 1);
  }
}

// (i) Multi-threaded stress: many threads each capture the epoch and register a ParkState while
// dedicated notifier threads repeatedly call NotifyAll. Asserts every waiter's on_resume is
// invoked exactly once and none is lost or double-invoked -- i.e. the register-before-recheck /
// release-before-check protocol documented in worker_resume_event.hpp (R1 B1) actually holds
// under contention, not just in single-threaded call order, now routed through ClaimPark on a
// shared ParkState rather than a bare handle.resume().
TEST(WorkerResumeEvent, ConcurrentRegisterAndNotifyResumesEachExactlyOnce) {
  WorkerResumeEvent event;
  constexpr int kNumWaiters = 32;
  constexpr int kNumNotifiers = 3;
  constexpr int kMaxRegisterAttempts = 2'000'000;

  std::vector<std::atomic<int>> resume_counts(kNumWaiters);
  std::vector<std::atomic<bool>> registered_ok(kNumWaiters);
  for (auto &flag : registered_ok) flag.store(false, std::memory_order_relaxed);

  std::atomic<bool> stop{false};

  std::vector<std::thread> notifier_threads;
  notifier_threads.reserve(kNumNotifiers);
  for (int n = 0; n < kNumNotifiers; ++n) {
    notifier_threads.emplace_back([&event, &stop] {
      while (!stop.load(std::memory_order_relaxed)) {
        event.NotifyAll();
        std::this_thread::yield();
      }
      // Final drain: catch anything registered just before `stop` was observed.
      event.NotifyAll();
    });
  }

  std::vector<std::thread> waiter_threads;
  waiter_threads.reserve(kNumWaiters);
  for (int i = 0; i < kNumWaiters; ++i) {
    waiter_threads.emplace_back([&event, &resume_counts, &registered_ok, i] {
      auto ps = MakeRecordingParkState(&resume_counts[i], static_cast<size_t>(i));

      bool registered = false;
      for (int attempt = 0; attempt < kMaxRegisterAttempts && !registered; ++attempt) {
        const uint64_t epoch = event.Epoch();
        registered = event.RegisterWaiter(ps, epoch);
        if (!registered) std::this_thread::yield();
      }
      registered_ok[i].store(registered, std::memory_order_relaxed);

      if (!registered) {
        // Could not register within the attempt budget -- treat as a failure to be reported on
        // the main thread.
        return;
      }

      // A notifier thread is looping continuously until every waiter thread (including this one)
      // has joined, so this ParkState is guaranteed to be claimed by some NotifyAll eventually.
      while (resume_counts[i].load(std::memory_order_acquire) == 0) {
        std::this_thread::yield();
      }
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
