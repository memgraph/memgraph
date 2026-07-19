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
#include <coroutine>
#include <cstddef>
#include <memory>
#include <thread>
#include <vector>

#include "utils/deadline_park_registry.hpp"

using memgraph::utils::ClaimPark;
using memgraph::utils::DeadlineParkRegistry;
using memgraph::utils::ParkState;

namespace {

// The registry never inspects or resumes `handle` -- only the wake-source callback that wins the
// claim is allowed to touch it, and none of these tests need a real coroutine frame to exercise
// that. std::noop_coroutine() gives us a valid, distinguishable-by-identity coroutine_handle<>
// without needing an actual coroutine type.
std::shared_ptr<ParkState> MakeParkState(size_t worker_id, std::chrono::steady_clock::time_point deadline) {
  auto ps = std::make_shared<ParkState>();
  ps->handle = std::noop_coroutine();
  ps->worker_id = worker_id;
  ps->deadline = deadline;
  return ps;
}

// Records every (worker_id, handle) the sweep handed back, for assertions on call count/args.
struct RecordingReschedule {
  std::vector<size_t> worker_ids;

  void operator()(size_t worker_id, std::coroutine_handle<> handle) {
    EXPECT_EQ(handle, std::noop_coroutine());
    worker_ids.push_back(worker_id);
  }
};

}  // namespace

// (a) An entry past its deadline is claimed and rescheduled exactly once.
TEST(DeadlineParkRegistry, PastDeadlineEntryIsClaimedAndRescheduledOnce) {
  DeadlineParkRegistry registry;
  const auto now = std::chrono::steady_clock::now();
  auto ps = MakeParkState(/*worker_id=*/3, /*deadline=*/now - std::chrono::seconds(1));
  registry.Register(ps);

  RecordingReschedule reschedule;
  registry.Sweep(now, std::ref(reschedule));

  ASSERT_EQ(reschedule.worker_ids.size(), 1u);
  EXPECT_EQ(reschedule.worker_ids[0], 3u);
  EXPECT_TRUE(ps->claimed.load(std::memory_order_acquire));

  // A second sweep must not find (or reschedule) the same entry again -- it was pruned from the
  // registry the first time it was collected.
  RecordingReschedule reschedule2;
  registry.Sweep(now, std::ref(reschedule2));
  EXPECT_TRUE(reschedule2.worker_ids.empty());
}

// (b) An entry before its deadline is left alone (not claimed, not rescheduled).
TEST(DeadlineParkRegistry, FutureDeadlineEntryIsLeftAlone) {
  DeadlineParkRegistry registry;
  const auto now = std::chrono::steady_clock::now();
  auto ps = MakeParkState(/*worker_id=*/7, /*deadline=*/now + std::chrono::hours(1));
  registry.Register(ps);

  RecordingReschedule reschedule;
  registry.Sweep(now, std::ref(reschedule));

  EXPECT_TRUE(reschedule.worker_ids.empty());
  EXPECT_FALSE(ps->claimed.load(std::memory_order_acquire));

  // Still tracked: sweeping again with the same `now` still leaves it alone.
  RecordingReschedule reschedule2;
  registry.Sweep(now, std::ref(reschedule2));
  EXPECT_TRUE(reschedule2.worker_ids.empty());
  EXPECT_FALSE(ps->claimed.load(std::memory_order_acquire));

  // But once its deadline has actually passed, a later sweep does claim+reschedule it.
  RecordingReschedule reschedule3;
  registry.Sweep(ps->deadline + std::chrono::seconds(1), std::ref(reschedule3));
  ASSERT_EQ(reschedule3.worker_ids.size(), 1u);
  EXPECT_EQ(reschedule3.worker_ids[0], 7u);
}

// (c) An entry already claimed (simulating the lock-release wake path having won the race first)
// is pruned WITHOUT being rescheduled by the sweep, even though its deadline has passed.
TEST(DeadlineParkRegistry, AlreadyClaimedEntryIsPrunedWithoutReschedule) {
  DeadlineParkRegistry registry;
  const auto now = std::chrono::steady_clock::now();
  auto ps = MakeParkState(/*worker_id=*/9, /*deadline=*/now - std::chrono::seconds(1));
  registry.Register(ps);

  // Simulate the lock-release path claiming it first.
  EXPECT_TRUE(ClaimPark(*ps));

  RecordingReschedule reschedule;
  registry.Sweep(now, std::ref(reschedule));

  EXPECT_TRUE(reschedule.worker_ids.empty()) << "an already-claimed entry must not be rescheduled by Sweep";
}

// (d) Two concurrent Sweeps racing on the same due entry reschedule it at most once (single
// owner across concurrent sweeps).
TEST(DeadlineParkRegistry, ConcurrentSweepsRescheduleAtMostOnce) {
  DeadlineParkRegistry registry;
  const auto now = std::chrono::steady_clock::now();
  auto ps = MakeParkState(/*worker_id=*/1, /*deadline=*/now - std::chrono::seconds(1));
  registry.Register(ps);

  std::atomic<int> total_reschedules{0};
  auto reschedule = [&](size_t /*worker_id*/, std::coroutine_handle<> /*handle*/) {
    total_reschedules.fetch_add(1, std::memory_order_relaxed);
  };

  constexpr int kNumSweepers = 8;
  std::vector<std::thread> sweepers;
  sweepers.reserve(kNumSweepers);
  for (int i = 0; i < kNumSweepers; ++i) {
    sweepers.emplace_back([&registry, now, &reschedule] { registry.Sweep(now, reschedule); });
  }
  for (auto &t : sweepers) t.join();

  EXPECT_EQ(total_reschedules.load(), 1) << "single-owner claim must guarantee at most one reschedule";
  EXPECT_TRUE(ps->claimed.load(std::memory_order_acquire));
}

// (d-cont.) A Sweep racing a manual ClaimPark (simulating a concurrent lock-release wake) also
// resolves to at most one winner.
TEST(DeadlineParkRegistry, SweepRacingManualClaimParkResolvesToOneWinner) {
  DeadlineParkRegistry registry;
  const auto now = std::chrono::steady_clock::now();
  auto ps = MakeParkState(/*worker_id=*/2, /*deadline=*/now - std::chrono::seconds(1));
  registry.Register(ps);

  std::atomic<int> sweep_reschedules{0};
  std::atomic<bool> manual_claim_won{false};

  std::thread sweeper([&] {
    registry.Sweep(now, [&](size_t /*worker_id*/, std::coroutine_handle<> /*handle*/) {
      sweep_reschedules.fetch_add(1, std::memory_order_relaxed);
    });
  });
  std::thread manual_claimer([&] { manual_claim_won.store(ClaimPark(*ps), std::memory_order_relaxed); });

  sweeper.join();
  manual_claimer.join();

  const int total_winners = sweep_reschedules.load() + (manual_claim_won.load() ? 1 : 0);
  EXPECT_EQ(total_winners, 1) << "exactly one of {Sweep, manual ClaimPark} must win the claim";
}

// (e) Sweeping an empty registry does nothing (and is the cheap fast path -- no entries to find).
TEST(DeadlineParkRegistry, EmptyRegistrySweepDoesNothing) {
  DeadlineParkRegistry registry;
  const auto now = std::chrono::steady_clock::now();

  RecordingReschedule reschedule;
  registry.Sweep(now, std::ref(reschedule));

  EXPECT_TRUE(reschedule.worker_ids.empty());
}

// (f) Deregister removes an entry so a later Sweep ignores it entirely, even past its deadline.
TEST(DeadlineParkRegistry, DeregisterRemovesEntryFromLaterSweep) {
  DeadlineParkRegistry registry;
  const auto now = std::chrono::steady_clock::now();
  auto ps = MakeParkState(/*worker_id=*/4, /*deadline=*/now - std::chrono::seconds(1));
  registry.Register(ps);

  registry.Deregister(ps);

  RecordingReschedule reschedule;
  registry.Sweep(now, std::ref(reschedule));

  EXPECT_TRUE(reschedule.worker_ids.empty());
  EXPECT_FALSE(ps->claimed.load(std::memory_order_acquire)) << "Deregister must not itself claim the entry";
}

// Registering two entries: only the due one is claimed+rescheduled, the other is left tracked.
TEST(DeadlineParkRegistry, MixedDueAndFutureEntriesOnlyDueOneRescheduled) {
  DeadlineParkRegistry registry;
  const auto now = std::chrono::steady_clock::now();
  auto due = MakeParkState(/*worker_id=*/5, /*deadline=*/now - std::chrono::milliseconds(1));
  auto future = MakeParkState(/*worker_id=*/6, /*deadline=*/now + std::chrono::hours(1));
  registry.Register(due);
  registry.Register(future);

  RecordingReschedule reschedule;
  registry.Sweep(now, std::ref(reschedule));

  ASSERT_EQ(reschedule.worker_ids.size(), 1u);
  EXPECT_EQ(reschedule.worker_ids[0], 5u);
  EXPECT_TRUE(due->claimed.load(std::memory_order_acquire));
  EXPECT_FALSE(future->claimed.load(std::memory_order_acquire));
}
