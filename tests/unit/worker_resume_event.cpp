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
std::shared_ptr<ParkState> MakeRecordingParkState(std::atomic<int> *counter) {
  auto ps = std::make_shared<ParkState>();
  ps->set_on_resume([counter] { counter->fetch_add(1, std::memory_order_relaxed); });
  // Pre-armed: these tests are about WHO this event decides to resume, not about WHEN a real parking
  // thread lets that resume be delivered. A ParkState starts un-armed (kParking) so that a claim
  // taken while its owner is still parking is deferred to the owner's task boundary -- see the
  // delivery-gate discussion in utils/park_state.hpp, and tests/unit/park_state.cpp for the gate's
  // own coverage. Arming up front keeps every assertion below about WorkerResumeEvent alone.
  memgraph::utils::ArmPark(*ps);
  return ps;
}

}  // namespace

// (a) RegisterWaiter with the current epoch succeeds and is reflected in WaitersPending().
TEST(WorkerResumeEvent, RegisterWaiterCurrentEpochSucceeds) {
  WorkerResumeEvent event;
  std::atomic<int> resumed{0};
  auto ps = MakeRecordingParkState(&resumed);

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

// (b) A stale epoch (captured before a NotifyAll bumped it) fails and does NOT enqueue.
TEST(WorkerResumeEvent, RegisterWaiterStaleEpochFails) {
  WorkerResumeEvent event;
  const uint64_t stale_epoch = event.Epoch();

  // Bump the epoch via a NotifyAll with no registered waiters.
  event.NotifyAll();
  EXPECT_NE(event.Epoch(), stale_epoch);

  std::atomic<int> resumed{0};
  auto ps = MakeRecordingParkState(&resumed);

  EXPECT_FALSE(event.RegisterWaiter(ps, stale_epoch));
  EXPECT_EQ(event.WaitersPending(), 0u);
  EXPECT_EQ(resumed.load(), 0);
}

// (c) NotifyAll delivers to every waiter exactly once, bumps the epoch, and zeroes WaitersPending().
TEST(WorkerResumeEvent, NotifyAllResumesEveryWaiterExactlyOnce) {
  WorkerResumeEvent event;
  constexpr int kNumWaiters = 3;
  std::vector<std::atomic<int>> resumed(kNumWaiters);
  std::vector<std::shared_ptr<ParkState>> parks;
  parks.reserve(kNumWaiters);
  for (int i = 0; i < kNumWaiters; ++i) {
    parks.push_back(MakeRecordingParkState(&resumed[i]));
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

// (d) RemoveWaiter decrements WaitersPending() and prevents a later NotifyAll from resuming it.
TEST(WorkerResumeEvent, RemoveWaiterPreventsLaterResume) {
  WorkerResumeEvent event;
  std::atomic<int> resumed_removed{0};
  std::atomic<int> resumed_kept{0};
  auto removed_ps = MakeRecordingParkState(&resumed_removed);
  auto kept_ps = MakeRecordingParkState(&resumed_kept);

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

// (e) Single-owner resume: back-to-back NotifyAll calls deliver once in total -- the second sees an
// empty list.
TEST(WorkerResumeEvent, BackToBackNotifyAllResumesOnce) {
  WorkerResumeEvent event;
  std::atomic<int> resumed{0};
  auto ps = MakeRecordingParkState(&resumed);

  const uint64_t epoch = event.Epoch();
  EXPECT_TRUE(event.RegisterWaiter(ps, epoch));

  event.NotifyAll();
  EXPECT_EQ(resumed.load(), 1);

  event.NotifyAll();
  EXPECT_EQ(resumed.load(), 1) << "waiter must not be double-resumed by a second NotifyAll";
}

// (f) Abandon path, WIN: the re-probe wins ClaimPark before any wake source. on_resume must never fire
// (the winner drives its own continuation synchronously), and a later NotifyAll on the not-yet-removed
// entry must be a harmless no-op.
TEST(WorkerResumeEvent, AbandonPathWinPreventsLaterNotifyAllResume) {
  WorkerResumeEvent event;
  std::atomic<int> resumed{0};
  auto ps = MakeRecordingParkState(&resumed);

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

// (g) Abandon path, LOSE: a concurrent NotifyAll wins first, as if a release landed between the
// re-probe and its ClaimPark. on_resume fires exactly once, and the caller observes the loss rather
// than double-invoking.
TEST(WorkerResumeEvent, AbandonPathLoseToNotifyAllResumesExactlyOnce) {
  WorkerResumeEvent event;
  std::atomic<int> resumed{0};
  auto ps = MakeRecordingParkState(&resumed);

  const uint64_t epoch = event.Epoch();
  ASSERT_TRUE(event.RegisterWaiter(ps, epoch));

  // A concurrent NotifyAll (the lock-release wake path) wins first.
  event.NotifyAll();
  EXPECT_EQ(resumed.load(), 1);

  // The caller's own abandon-path claim attempt now loses -- it must not invoke on_resume itself.
  EXPECT_FALSE(ClaimPark(*ps));
  EXPECT_EQ(resumed.load(), 1) << "on_resume must be invoked exactly once total across both paths";
}

// (h) Drain() delivers to every registered waiter exactly once; a later NotifyAll is a no-op.
TEST(WorkerResumeEvent, DrainResumesAllWaitersExactlyOnce) {
  WorkerResumeEvent event;
  constexpr int kNumWaiters = 4;
  std::vector<std::atomic<int>> resumed(kNumWaiters);
  std::vector<std::shared_ptr<ParkState>> parks;
  parks.reserve(kNumWaiters);

  const uint64_t epoch = event.Epoch();
  for (int i = 0; i < kNumWaiters; ++i) {
    auto ps = MakeRecordingParkState(&resumed[i]);
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

// (i) Multi-threaded stress: registrars capture-and-register while notifiers repeatedly NotifyAll.
// Asserts the register-before-recheck / release-before-check protocol holds under contention, not just
// in single-threaded call order -- every waiter delivered exactly once, none lost or doubled.
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
      auto ps = MakeRecordingParkState(&resume_counts[i]);

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

// Drain() CLOSES the event: no later registration may succeed, and callers can tell that apart from an
// epoch bump. The window is real -- Drain deliberately does not bump the epoch, so a registration
// arriving just after it used to push onto the just-emptied list and park with nothing left to wake it.
// An epoch bump would not close it (the retry lands on the same drained event); only a sticky flag
// does, which is why this is a distinct state.
TEST(WorkerResumeEvent, DrainClosesTheEventAgainstLateRegistration) {
  WorkerResumeEvent event;
  std::atomic<int> resumed{0};

  auto early = std::make_shared<ParkState>();
  early->set_on_resume([&resumed] { resumed.fetch_add(1, std::memory_order_release); });
  const auto epoch = event.Epoch();
  ASSERT_TRUE(event.RegisterWaiter(early, epoch));
  ASSERT_FALSE(event.IsClosed()) << "a fresh event must not be closed";

  event.Drain();
  EXPECT_TRUE(event.IsClosed()) << "Drain must close the event, not merely empty it";
  EXPECT_EQ(event.WaitersPending(), 0U);

  // The late waiter: same epoch Drain left in place, which is exactly what used to let it through.
  auto late = std::make_shared<ParkState>();
  late->set_on_resume([&resumed] { resumed.fetch_add(1, std::memory_order_release); });
  EXPECT_FALSE(event.RegisterWaiter(late, epoch))
      << "a registration arriving after Drain() succeeded -- it would park on a drained event with no "
         "wake source left, and only the deadline sweep would rescue it";
  EXPECT_EQ(event.WaitersPending(), 0U) << "a refused registration must not be counted";

  // And refusal is permanent: re-reading the epoch and retrying (what the acquire loop does on an
  // ordinary epoch bump) must not get in either, or the caller spins to its deadline.
  EXPECT_FALSE(event.RegisterWaiter(late, event.Epoch()))
      << "refusal must be sticky -- retrying under the current epoch got in, so the acquire loop would "
         "re-probe and re-register forever instead of bailing";
}
