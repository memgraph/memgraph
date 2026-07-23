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
#include <chrono>
#include <cstddef>
#include <memory>
#include <mutex>
#include <thread>
#include <vector>

#include "utils/deadline_park_registry.hpp"
#include "utils/park_state.hpp"
#include "utils/worker_resume_event.hpp"

using memgraph::utils::ClaimPark;
using memgraph::utils::DeadlineParkRegistry;
using memgraph::utils::ParkState;
using memgraph::utils::WorkerResumeEvent;

namespace {

// Builds a ParkState whose on_resume records worker_id into *invoked_worker_ids (guarded by *mutex,
// since some tests race Sweep across threads). A plain recording closure, no coroutine frame.
std::shared_ptr<ParkState> MakeParkState(size_t worker_id, std::chrono::steady_clock::time_point deadline,
                                         std::vector<size_t> *invoked_worker_ids, std::mutex *mutex) {
  auto ps = std::make_shared<ParkState>();
  ps->worker_id = worker_id;
  ps->deadline = deadline;
  ps->on_resume = [worker_id, invoked_worker_ids, mutex] {
    std::lock_guard<std::mutex> lock(*mutex);
    invoked_worker_ids->push_back(worker_id);
  };
  return ps;
}

}  // namespace

// (a) An entry past its deadline is claimed and has on_resume invoked exactly once.
TEST(DeadlineParkRegistry, PastDeadlineEntryIsClaimedAndRescheduledOnce) {
  DeadlineParkRegistry registry;
  const auto now = std::chrono::steady_clock::now();
  std::mutex mutex;
  std::vector<size_t> invoked;
  auto ps = MakeParkState(/*worker_id=*/3, /*deadline=*/now - std::chrono::seconds(1), &invoked, &mutex);
  registry.Register(ps);

  registry.Sweep(now);

  ASSERT_EQ(invoked.size(), 1u);
  EXPECT_EQ(invoked[0], 3u);
  EXPECT_TRUE(ps->claimed.load(std::memory_order_acquire));

  // A second sweep must not find (or invoke on_resume for) the same entry again -- it was pruned
  // from the registry the first time it was collected.
  registry.Sweep(now);
  EXPECT_EQ(invoked.size(), 1u);
}

// (b) An entry before its deadline is left alone (not claimed, on_resume not invoked).
TEST(DeadlineParkRegistry, FutureDeadlineEntryIsLeftAlone) {
  DeadlineParkRegistry registry;
  const auto now = std::chrono::steady_clock::now();
  std::mutex mutex;
  std::vector<size_t> invoked;
  auto ps = MakeParkState(/*worker_id=*/7, /*deadline=*/now + std::chrono::hours(1), &invoked, &mutex);
  registry.Register(ps);

  registry.Sweep(now);

  EXPECT_TRUE(invoked.empty());
  EXPECT_FALSE(ps->claimed.load(std::memory_order_acquire));

  // Still tracked: sweeping again with the same `now` still leaves it alone.
  registry.Sweep(now);
  EXPECT_TRUE(invoked.empty());
  EXPECT_FALSE(ps->claimed.load(std::memory_order_acquire));

  // But once its deadline has actually passed, a later sweep does claim it and invoke on_resume.
  registry.Sweep(ps->deadline + std::chrono::seconds(1));
  ASSERT_EQ(invoked.size(), 1u);
  EXPECT_EQ(invoked[0], 7u);
}

// (c) An already-claimed entry (lock-release wake won first) is pruned WITHOUT on_resume, even
// though its deadline passed.
TEST(DeadlineParkRegistry, AlreadyClaimedEntryIsPrunedWithoutReschedule) {
  DeadlineParkRegistry registry;
  const auto now = std::chrono::steady_clock::now();
  std::mutex mutex;
  std::vector<size_t> invoked;
  auto ps = MakeParkState(/*worker_id=*/9, /*deadline=*/now - std::chrono::seconds(1), &invoked, &mutex);
  registry.Register(ps);

  // Simulate the lock-release path claiming it first (without invoking on_resume itself).
  EXPECT_TRUE(ClaimPark(*ps));

  registry.Sweep(now);

  EXPECT_TRUE(invoked.empty()) << "an already-claimed entry must not have on_resume invoked by Sweep";
}

// (d) Two concurrent Sweeps racing on the same due entry invoke on_resume at most once (single
// owner across concurrent sweeps).
TEST(DeadlineParkRegistry, ConcurrentSweepsRescheduleAtMostOnce) {
  DeadlineParkRegistry registry;
  const auto now = std::chrono::steady_clock::now();
  std::atomic<int> total_resumes{0};
  auto ps = std::make_shared<ParkState>();
  ps->worker_id = 1;
  ps->deadline = now - std::chrono::seconds(1);
  ps->on_resume = [&total_resumes] { total_resumes.fetch_add(1, std::memory_order_relaxed); };
  registry.Register(ps);

  constexpr int kNumSweepers = 8;
  std::vector<std::thread> sweepers;
  sweepers.reserve(kNumSweepers);
  for (int i = 0; i < kNumSweepers; ++i) {
    sweepers.emplace_back([&registry, now] { registry.Sweep(now); });
  }
  for (auto &t : sweepers) t.join();

  EXPECT_EQ(total_resumes.load(), 1) << "single-owner claim must guarantee at most one on_resume invocation";
  EXPECT_TRUE(ps->claimed.load(std::memory_order_acquire));
}

// (d-cont.) A Sweep racing a manual ClaimPark (a concurrent lock-release wake) has at most one winner.
TEST(DeadlineParkRegistry, SweepRacingManualClaimParkResolvesToOneWinner) {
  DeadlineParkRegistry registry;
  const auto now = std::chrono::steady_clock::now();
  std::atomic<int> sweep_resumes{0};
  std::atomic<bool> manual_claim_won{false};
  auto ps = std::make_shared<ParkState>();
  ps->worker_id = 2;
  ps->deadline = now - std::chrono::seconds(1);
  ps->on_resume = [&sweep_resumes] { sweep_resumes.fetch_add(1, std::memory_order_relaxed); };
  registry.Register(ps);

  std::thread sweeper([&] { registry.Sweep(now); });
  std::thread manual_claimer([&] { manual_claim_won.store(ClaimPark(*ps), std::memory_order_relaxed); });

  sweeper.join();
  manual_claimer.join();

  const int total_winners = sweep_resumes.load() + (manual_claim_won.load() ? 1 : 0);
  EXPECT_EQ(total_winners, 1) << "exactly one of {Sweep, manual ClaimPark} must win the claim";
}

// (e) Sweeping an empty registry does nothing (and is the cheap fast path -- no entries to find).
TEST(DeadlineParkRegistry, EmptyRegistrySweepDoesNothing) {
  DeadlineParkRegistry registry;
  const auto now = std::chrono::steady_clock::now();
  registry.Sweep(now);  // Must not crash / must be a true no-op.
}

// (f) Deregister removes an entry so a later Sweep ignores it entirely, even past its deadline.
TEST(DeadlineParkRegistry, DeregisterRemovesEntryFromLaterSweep) {
  DeadlineParkRegistry registry;
  const auto now = std::chrono::steady_clock::now();
  std::mutex mutex;
  std::vector<size_t> invoked;
  auto ps = MakeParkState(/*worker_id=*/4, /*deadline=*/now - std::chrono::seconds(1), &invoked, &mutex);
  registry.Register(ps);

  registry.Deregister(ps);

  registry.Sweep(now);

  EXPECT_TRUE(invoked.empty());
  EXPECT_FALSE(ps->claimed.load(std::memory_order_acquire)) << "Deregister must not itself claim the entry";
}

// Registering two entries: only the due one is claimed + has on_resume invoked, the other is left
// tracked.
TEST(DeadlineParkRegistry, MixedDueAndFutureEntriesOnlyDueOneRescheduled) {
  DeadlineParkRegistry registry;
  const auto now = std::chrono::steady_clock::now();
  std::mutex mutex;
  std::vector<size_t> invoked;
  auto due = MakeParkState(/*worker_id=*/5, /*deadline=*/now - std::chrono::milliseconds(1), &invoked, &mutex);
  auto future = MakeParkState(/*worker_id=*/6, /*deadline=*/now + std::chrono::hours(1), &invoked, &mutex);
  registry.Register(due);
  registry.Register(future);

  registry.Sweep(now);

  ASSERT_EQ(invoked.size(), 1u);
  EXPECT_EQ(invoked[0], 5u);
  EXPECT_TRUE(due->claimed.load(std::memory_order_acquire));
  EXPECT_FALSE(future->claimed.load(std::memory_order_acquire));
}

// (g) Drain() claims and invokes on_resume for every registered entry regardless of deadline, and
// each exactly once.
TEST(DeadlineParkRegistry, DrainResumesAllEntriesRegardlessOfDeadline) {
  DeadlineParkRegistry registry;
  const auto now = std::chrono::steady_clock::now();
  std::mutex mutex;
  std::vector<size_t> invoked;
  auto due = MakeParkState(/*worker_id=*/1, /*deadline=*/now - std::chrono::seconds(1), &invoked, &mutex);
  auto future = MakeParkState(/*worker_id=*/2, /*deadline=*/now + std::chrono::hours(1), &invoked, &mutex);
  registry.Register(due);
  registry.Register(future);

  registry.Drain();

  ASSERT_EQ(invoked.size(), 2u);
  EXPECT_TRUE(due->claimed.load(std::memory_order_acquire));
  EXPECT_TRUE(future->claimed.load(std::memory_order_acquire));

  // A later Sweep must see nothing left to prune or invoke.
  registry.Sweep(now + std::chrono::hours(2));
  EXPECT_EQ(invoked.size(), 2u);
}

// (h) Drain() racing a Sweep on the same due entry resolves to exactly one invocation of
// on_resume (single-owner claim, same as Sweep-vs-Sweep and Sweep-vs-manual-ClaimPark above).
TEST(DeadlineParkRegistry, DrainRacingSweepResolvesToOneWinner) {
  DeadlineParkRegistry registry;
  const auto now = std::chrono::steady_clock::now();
  std::atomic<int> total_resumes{0};
  auto ps = std::make_shared<ParkState>();
  ps->worker_id = 8;
  ps->deadline = now - std::chrono::seconds(1);
  ps->on_resume = [&total_resumes] { total_resumes.fetch_add(1, std::memory_order_relaxed); };
  registry.Register(ps);

  std::thread sweeper([&] { registry.Sweep(now); });
  std::thread drainer([&] { registry.Drain(); });
  sweeper.join();
  drainer.join();

  EXPECT_EQ(total_resumes.load(), 1) << "exactly one of {Sweep, Drain} must win the claim";
}

// (i) Cross-registry single ownership: a ParkState in BOTH a WorkerResumeEvent and a
// DeadlineParkRegistry, claimed+resumed by NotifyAll, must NOT be re-invoked by a later Sweep even
// past its deadline -- the shared ParkState::claimed flag makes the two registries agree.
TEST(DeadlineParkRegistry, EntryClaimedByWorkerResumeEventNotifyAllNotReinvokedBySweep) {
  WorkerResumeEvent event;
  DeadlineParkRegistry registry;
  const auto now = std::chrono::steady_clock::now();

  std::atomic<int> resumed{0};
  auto ps = std::make_shared<ParkState>();
  ps->worker_id = 42;
  ps->deadline = now - std::chrono::seconds(1);  // already due for the deadline sweep
  ps->on_resume = [&resumed] { resumed.fetch_add(1, std::memory_order_relaxed); };

  const uint64_t epoch = event.Epoch();
  ASSERT_TRUE(event.RegisterWaiter(ps, epoch));
  registry.Register(ps);

  // The lock-release wake path fires first and wins the claim.
  event.NotifyAll();
  EXPECT_EQ(resumed.load(), 1);

  // A later deadline sweep must see the entry already claimed and prune it without invoking
  // on_resume again.
  registry.Sweep(now);
  EXPECT_EQ(resumed.load(), 1) << "on_resume must not be invoked twice across the two registries";
}
