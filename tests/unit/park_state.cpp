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
using memgraph::utils::ArmPendingParksAbove;
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

// Delivery must never propagate an exception. Every caller is somewhere a throw does outsized damage:
// inside ~ResourceLockGuard (a destructor -- an escape terminates the process), inside
// WorkerResumeEvent::ResumeAll's and DeadlineParkRegistry::Sweep/Drain's loops over claimed waiters
// (an escape abandons the REST of them, each a permanently parked query), and inside ~ParkArmGuard
// while a task unwinds.
TEST(ParkStateGate, DeliveryDoesNotPropagateAnException) {
  auto ps = std::make_shared<ParkState>();
  ps->set_on_resume([] { throw std::runtime_error{"posting the resume failed"}; });

  ASSERT_TRUE(ClaimPark(*ps));
  RequestResume(*ps);  // deferred, gate still kParking
  EXPECT_NO_THROW(ArmPark(*ps)) << "a failed delivery must be swallowed, not propagated";
}

// The same, through the stack: one waiter whose delivery throws must not stop the others from being
// armed. This is the loop-abandonment case, which is the one that multiplies a single failure into
// several permanently parked queries.
TEST(ParkStateGate, AFailedDeliveryDoesNotStrandTheOtherPendingParks) {
  std::atomic<int> good_resumed{0};
  auto bad = std::make_shared<ParkState>();
  bad->set_on_resume([] { throw std::runtime_error{"posting the resume failed"}; });
  auto good = MakeCounting(&good_resumed);

  // `good` published first, so it is armed LAST (innermost first) -- i.e. after the failure.
  PublishPendingPark(good);
  PublishPendingPark(bad);
  ASSERT_TRUE(ClaimPark(*good));
  ASSERT_TRUE(ClaimPark(*bad));
  RequestResume(*good);
  RequestResume(*bad);

  EXPECT_NO_THROW(ArmPendingParksAbove(0));
  EXPECT_EQ(good_resumed.load(), 1) << "a failed delivery aborted the arming loop and stranded the rest";
  EXPECT_TRUE(memgraph::utils::tls_pending_park_head == nullptr);
}

// --- the pending-arm stack, i.e. what the pool worker loop drives ---

TEST(ParkStatePendingArm, ArmingDeliversAPublishedPark) {
  std::atomic<int> resumed{0};
  auto ps = MakeCounting(&resumed);

  PublishPendingPark(ps);
  ASSERT_TRUE(ClaimPark(*ps));
  RequestResume(*ps);
  EXPECT_EQ(resumed.load(), 0);

  ArmPendingParksAbove(0);
  EXPECT_EQ(resumed.load(), 1);
}

TEST(ParkStatePendingArm, ArmingIsANoOpWithNothingPublished) {
  ArmPendingParksAbove(0);  // must not crash
  ArmPendingParksAbove(0);
}

TEST(ParkStatePendingArm, ArmingPopsWhatItArmed) {
  std::atomic<int> resumed{0};
  auto ps = MakeCounting(&resumed);

  PublishPendingPark(ps);
  ASSERT_TRUE(ClaimPark(*ps));
  RequestResume(*ps);
  ArmPendingParksAbove(0);
  ASSERT_EQ(resumed.load(), 1);

  ArmPendingParksAbove(0);  // stack is empty; must not re-deliver
  EXPECT_EQ(resumed.load(), 1);
}

// The self-claim paths (abandon win, shutdown self-claim, the unwind) discard rather than arm: the
// publisher IS the claim winner, so no resume can ever be requested.
TEST(ParkStatePendingArm, DiscardSuppressesDelivery) {
  std::atomic<int> resumed{0};
  auto ps = MakeCounting(&resumed);

  PublishPendingPark(ps);
  ASSERT_TRUE(ClaimPark(*ps));  // we are the winner, as the abandon path would be
  DiscardPendingPark(ps);

  ArmPendingParksAbove(0);
  EXPECT_EQ(resumed.load(), 0);
}

// A task that leaves by exception still arms.
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

// THE REGRESSION GUARD for the review's blocker finding. Task execution nests: PostResumeTask's
// all-workers-stopped fallback runs a resume inline, that resume re-enters the session chain, and a
// brand new query can park while the OUTER park is still awaiting its own arm. With a single TLS slot
// the inner publish silently overwrote the outer park, which then stayed registered with
// gate == kParking forever -- unreachable by any wake source, holding its campaign PendingHandle, and
// so blocking every subsequent acquisition on that storage.
TEST(ParkStatePendingArm, NestedPublishDoesNotStrandTheOuterPark) {
  std::atomic<int> outer_resumed{0};
  std::atomic<int> inner_resumed{0};
  auto outer = MakeCounting(&outer_resumed);
  auto inner = MakeCounting(&inner_resumed);

  {
    ParkArmGuard const outer_guard;  // stands in for the worker's task boundary
    PublishPendingPark(outer);
    ASSERT_TRUE(ClaimPark(*outer));
    RequestResume(*outer);  // deferred: outer's task has not ended
    ASSERT_EQ(outer_resumed.load(), 0);

    {
      // The nested unit of execution: an inline resume, which parks again.
      ParkArmGuard const inner_guard;
      PublishPendingPark(inner);
      ASSERT_TRUE(ClaimPark(*inner));
      RequestResume(*inner);
      EXPECT_EQ(inner_resumed.load(), 0);
      EXPECT_EQ(outer_resumed.load(), 0) << "the nested unit must NOT arm the outer park -- its driver "
                                            "has not finished, which is the race the gate prevents";
    }
    // Inner unit ended: its own park is armed, the outer one still is not.
    EXPECT_EQ(inner_resumed.load(), 1);
    EXPECT_EQ(outer_resumed.load(), 0) << "outer park armed too early";
  }

  EXPECT_EQ(outer_resumed.load(), 1) << "outer park was stranded -- it would hang forever and block "
                                        "every later acquisition on its storage";
}

// Arming loops rather than firing once: ArmPark may invoke on_resume_, which on the inline path runs a
// chain that publishes yet another park before returning. A single-shot arm would strand that one.
TEST(ParkStatePendingArm, ArmingDrainsParksPublishedWhileArming) {
  std::atomic<int> first_resumed{0};
  std::atomic<int> late_resumed{0};
  auto first = std::make_shared<ParkState>();
  auto late = std::make_shared<ParkState>();
  late->set_on_resume([&] { late_resumed.fetch_add(1, std::memory_order_relaxed); });

  // Arming `first` publishes `late` from inside on_resume_, exactly as an inline resume would.
  first->set_on_resume([&] {
    first_resumed.fetch_add(1, std::memory_order_relaxed);
    PublishPendingPark(late);
    ASSERT_TRUE(ClaimPark(*late));
    RequestResume(*late);
  });

  PublishPendingPark(first);
  ASSERT_TRUE(ClaimPark(*first));
  RequestResume(*first);

  ArmPendingParksAbove(0);
  EXPECT_EQ(first_resumed.load(), 1);
  EXPECT_EQ(late_resumed.load(), 1) << "a park published while arming was never armed";
  EXPECT_TRUE(memgraph::utils::tls_pending_park_head == nullptr);
}

// The stack is per-thread: two threads parking concurrently do not see each other's pending parks.
TEST(ParkStatePendingArm, PendingStackIsPerThread) {
  std::atomic<int> resumed_a{0};
  std::atomic<int> resumed_b{0};
  auto ps_a = MakeCounting(&resumed_a);
  auto ps_b = MakeCounting(&resumed_b);

  std::thread ta([&] {
    PublishPendingPark(ps_a);
    ASSERT_TRUE(ClaimPark(*ps_a));
    RequestResume(*ps_a);
    ArmPendingParksAbove(0);
  });
  std::thread tb([&] {
    PublishPendingPark(ps_b);
    ASSERT_TRUE(ClaimPark(*ps_b));
    RequestResume(*ps_b);
    ArmPendingParksAbove(0);
  });
  ta.join();
  tb.join();

  EXPECT_EQ(resumed_a.load(), 1);
  EXPECT_EQ(resumed_b.load(), 1);
}

// --- Regression: publishing a pending park must be INFALLIBLE, not merely exception-safe ---
//
// The defect this guards was a BLOCKER found in review, and it is worth stating exactly, because the
// obvious-looking fix is the one that failed. `PublishPendingPark` was a `std::vector::push_back`. On
// `bad_alloc` the `ParkState` was already registered on the wake event (RegisterWaiter runs first, and
// must, for the register-before-recheck race) but was NOT on this thread's pending-arm stack. If a wake
// source claimed it in that window, `AcquireAwaitable::await_suspend`'s catch handler lost `ClaimPark`,
// concluded "somebody else will resume us" and suspended the frame -- but arming only ever walks the
// pending-arm stack, so nothing could arm a park that was never published. Gate stuck at
// kResumeRequested, `claimed` blocking every later claim, frame never resumed, and its campaign-long
// PendingHandle pinning unique_pending_count above zero for the life of the process. Not a leak: a
// bricked storage.
//
// Moving the publish inside the try (an earlier fix) only rescued the branch that UNWINDS. The branch
// that SUSPENDS still assumed an arming side was owed. So the guarantee this test pins is the stronger
// one that actually makes the suspend branch sound: the call cannot fail at all.
TEST(ParkStateGate, PublishingAPendingParkCannotThrow) {
  // Compile-time half. If someone reintroduces an allocating container here, this stops compiling
  // rather than reintroducing the blocker quietly.
  static_assert(noexcept(PublishPendingPark(std::shared_ptr<ParkState>{})),
                "PublishPendingPark must be noexcept: AcquireAwaitable's lose-ClaimPark branch suspends the "
                "frame on the assumption that an arming side is owed, which is only true if publishing "
                "either did not run or fully succeeded. A throwing publish makes that branch a permanent "
                "hang and bricks the storage.");

  // Runtime half: deep nesting must not need to grow anything. A vector would reallocate here; the
  // intrusive stack links storage the ParkStates already own.
  constexpr int kDepth = 512;
  std::vector<std::shared_ptr<ParkState>> parks;
  std::atomic<int> resumed{0};
  parks.reserve(kDepth);
  for (int i = 0; i < kDepth; ++i) {
    parks.push_back(MakeCounting(&resumed));
    PublishPendingPark(parks.back());
  }
  for (auto &ps : parks) ASSERT_TRUE(ClaimPark(*ps));
  for (auto &ps : parks) RequestResume(*ps);

  ArmPendingParksAbove(0);
  EXPECT_EQ(resumed.load(), kDepth) << "a deeply nested pending-park stack lost deliveries";
  EXPECT_TRUE(memgraph::utils::tls_pending_park_head == nullptr);
  EXPECT_EQ(memgraph::utils::tls_pending_park_depth, 0U);
}

// The half of the blocker that is about the SUSPEND branch's precondition rather than about allocation:
// a park that has been claimed by a foreign source and had RequestResume called on it is delivered by
// the arming side, and delivered EXACTLY ONCE. If the park is absent from the stack (which is what the
// throwing publish used to produce) nothing arms it -- so this test also documents, by contrast, why the
// publish must precede anything that can throw.
TEST(ParkStateGate, ForeignClaimBeforeArmingIsDeliveredByTheArmingSide) {
  std::atomic<int> resumed{0};
  auto ps = MakeCounting(&resumed);

  PublishPendingPark(ps);
  // Foreign wake source: wins the claim and asks for a resume while the "parking thread" is still
  // inside its own await_suspend, i.e. before any arming point.
  ASSERT_TRUE(ClaimPark(*ps));
  RequestResume(*ps);
  EXPECT_EQ(resumed.load(), 0) << "a resume was delivered while the parking thread was still parking";
  EXPECT_EQ(ps->gate.load(), ParkGate::kResumeRequested);

  // The parking thread reaches its task boundary: the arming side completes the rendezvous.
  ArmPendingParksAbove(0);
  EXPECT_EQ(resumed.load(), 1) << "the arming side did not deliver a resume the claim winner had requested";
  EXPECT_TRUE(memgraph::utils::tls_pending_park_head == nullptr);
}
