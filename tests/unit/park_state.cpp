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

#include <atomic>
#include <thread>
#include <vector>

#include <gtest/gtest.h>

#include "utils/park_state.hpp"

using memgraph::utils::ArmPark;
using memgraph::utils::ArmPendingPark;
using memgraph::utils::ClaimPark;
using memgraph::utils::DiscardPendingPark;
using memgraph::utils::ParkArmGuard;
using memgraph::utils::ParkGate;
using memgraph::utils::ParkState;
using memgraph::utils::PublishPendingPark;
using memgraph::utils::RequestResume;

namespace {

std::shared_ptr<ParkState> MakeCounting(std::atomic<int> *counter) {
  auto ps = std::make_shared<ParkState>();
  ps->set_on_resume([counter] { counter->fetch_add(1, std::memory_order_relaxed); });
  return ps;
}

}  // namespace

// A fresh ParkState is un-armed: nothing may be delivered until its parking thread says so. This is
// the safe default -- a caller who never arms hangs its park (loud, debuggable) instead of letting a
// resume race the parking thread still inside await_suspend (silent UAF).
TEST(ParkStateGate, FreshParkStateIsNotArmed) {
  std::atomic<int> resumed{0};
  auto ps = MakeCounting(&resumed);
  EXPECT_EQ(ps->gate.load(), ParkGate::kParking);
  EXPECT_EQ(resumed.load(), 0);
}

// The ordering that motivates the gate: a wake source claims while the parking thread is still
// running. Nothing may be delivered yet; the arm delivers it.
TEST(ParkStateGate, ClaimBeforeArmDefersDeliveryToTheArm) {
  std::atomic<int> resumed{0};
  auto ps = MakeCounting(&resumed);

  ASSERT_TRUE(ClaimPark(*ps));
  RequestResume(*ps);
  EXPECT_EQ(resumed.load(), 0) << "a claim taken while the owner is still parking must not deliver";

  ArmPark(*ps);
  EXPECT_EQ(resumed.load(), 1) << "the arming side must deliver the deferred resume";
}

// The common ordering: the parking thread finished long before anything wakes the park.
TEST(ParkStateGate, ArmBeforeClaimDeliversAtClaimTime) {
  std::atomic<int> resumed{0};
  auto ps = MakeCounting(&resumed);

  ArmPark(*ps);
  EXPECT_EQ(resumed.load(), 0) << "arming alone must never resume -- nobody has claimed yet";

  ASSERT_TRUE(ClaimPark(*ps));
  RequestResume(*ps);
  EXPECT_EQ(resumed.load(), 1);
}

// A park that is never woken (the accessor was acquired on the abandon path, say) must never be
// resumed by the arm on its own.
TEST(ParkStateGate, ArmWithoutAnyClaimNeverResumes) {
  std::atomic<int> resumed{0};
  auto ps = MakeCounting(&resumed);

  ArmPark(*ps);
  ArmPark(*ps);  // over-arming is explicitly harmless
  EXPECT_EQ(resumed.load(), 0);
}

// Whichever side arrives second delivers, and only one of them ever does -- under contention, on
// every interleaving. This is the property the whole un-pinned design rests on.
TEST(ParkStateGate, ClaimAndArmRaceDeliverExactlyOnce) {
  constexpr int kRounds = 2000;
  for (int round = 0; round < kRounds; ++round) {
    std::atomic<int> resumed{0};
    auto ps = MakeCounting(&resumed);
    std::atomic<bool> go{false};

    std::thread claimer([&] {
      go.wait(false);
      if (ClaimPark(*ps)) RequestResume(*ps);
    });
    std::thread armer([&] {
      go.wait(false);
      ArmPark(*ps);
    });

    go = true;
    go.notify_all();
    claimer.join();
    armer.join();

    ASSERT_EQ(resumed.load(), 1) << "round " << round << ": exactly one side must deliver";
  }
}

// Several wake sources plus the arm: ClaimPark picks one winner, the gate picks the moment.
TEST(ParkStateGate, ManyClaimersPlusArmDeliverExactlyOnce) {
  constexpr int kRounds = 500;
  constexpr int kClaimers = 4;
  for (int round = 0; round < kRounds; ++round) {
    std::atomic<int> resumed{0};
    auto ps = MakeCounting(&resumed);
    std::atomic<bool> go{false};

    std::vector<std::thread> threads;
    threads.reserve(kClaimers + 1);
    for (int i = 0; i < kClaimers; ++i) {
      threads.emplace_back([&] {
        go.wait(false);
        if (ClaimPark(*ps)) RequestResume(*ps);
      });
    }
    threads.emplace_back([&] {
      go.wait(false);
      ArmPark(*ps);
    });

    go = true;
    go.notify_all();
    for (auto &t : threads) t.join();

    ASSERT_EQ(resumed.load(), 1) << "round " << round;
  }
}

// --- the thread-local pending-arm slot, i.e. what the pool worker loop drives ---

TEST(ParkStatePendingArm, ArmPendingParkDeliversAPublishedPark) {
  std::atomic<int> resumed{0};
  auto ps = MakeCounting(&resumed);

  PublishPendingPark(ps);
  ASSERT_TRUE(ClaimPark(*ps));
  RequestResume(*ps);
  EXPECT_EQ(resumed.load(), 0);

  ArmPendingPark();
  EXPECT_EQ(resumed.load(), 1);
}

TEST(ParkStatePendingArm, ArmPendingParkIsANoOpWithNothingPublished) {
  ArmPendingPark();  // must not crash or assert
  ArmPendingPark();
}

TEST(ParkStatePendingArm, ArmPendingParkClearsTheSlot) {
  std::atomic<int> resumed{0};
  auto ps = MakeCounting(&resumed);

  PublishPendingPark(ps);
  ASSERT_TRUE(ClaimPark(*ps));
  RequestResume(*ps);
  ArmPendingPark();
  ASSERT_EQ(resumed.load(), 1);

  // Slot cleared: a second arm must not re-deliver, and publishing again must be accepted (the
  // single-slot assert only fires on a genuine double-publish).
  ArmPendingPark();
  EXPECT_EQ(resumed.load(), 1);

  std::atomic<int> resumed2{0};
  auto ps2 = MakeCounting(&resumed2);
  PublishPendingPark(ps2);
  ASSERT_TRUE(ClaimPark(*ps2));
  RequestResume(*ps2);
  ArmPendingPark();
  EXPECT_EQ(resumed2.load(), 1);
}

// The self-claim paths (abandon win, shutdown self-claim, the unwind) discard rather than arm: the
// publisher IS the claim winner, so no resume can ever be requested.
TEST(ParkStatePendingArm, DiscardPendingParkSuppressesDelivery) {
  std::atomic<int> resumed{0};
  auto ps = MakeCounting(&resumed);

  PublishPendingPark(ps);
  ASSERT_TRUE(ClaimPark(*ps));  // we are the winner, as the abandon path would be
  DiscardPendingPark(ps);

  ArmPendingPark();
  EXPECT_EQ(resumed.load(), 0);
}

// A task that leaves by exception still arms -- the guard is why "forgot to arm" is unrepresentable
// rather than merely unlikely.
TEST(ParkStatePendingArm, ParkArmGuardArmsOnAnExceptionalExit) {
  std::atomic<int> resumed{0};
  auto ps = MakeCounting(&resumed);

  try {
    ParkArmGuard const guard;
    PublishPendingPark(ps);
    ASSERT_TRUE(ClaimPark(*ps));
    RequestResume(*ps);
    throw std::runtime_error{"task blew up"};
  } catch (const std::runtime_error &) {  // NOLINT(bugprone-empty-catch)
  }

  EXPECT_EQ(resumed.load(), 1) << "a throwing task must still arm its park";
}

// The slot is per-thread: two threads parking concurrently do not see each other's pending park.
TEST(ParkStatePendingArm, PendingSlotIsPerThread) {
  std::atomic<int> resumed_a{0};
  std::atomic<int> resumed_b{0};
  auto ps_a = MakeCounting(&resumed_a);
  auto ps_b = MakeCounting(&resumed_b);

  std::thread ta([&] {
    PublishPendingPark(ps_a);
    ASSERT_TRUE(ClaimPark(*ps_a));
    RequestResume(*ps_a);
    ArmPendingPark();
  });
  std::thread tb([&] {
    PublishPendingPark(ps_b);
    ASSERT_TRUE(ClaimPark(*ps_b));
    RequestResume(*ps_b);
    ArmPendingPark();
  });
  ta.join();
  tb.join();

  EXPECT_EQ(resumed_a.load(), 1);
  EXPECT_EQ(resumed_b.load(), 1);
}
