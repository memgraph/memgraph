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

#include "communication/v2/read_arm_state.hpp"

#include <atomic>
#include <thread>

#include <gtest/gtest.h>

using memgraph::communication::v2::ReadArmState;

// A session nobody has asked to terminate arms its reads and hands the socket back and forth
// between the thread that reads and the thread that executes, without ever being closed.
TEST(ReadArmState, ArmsRepeatedlyWhileNoTerminationIsRequested) {
  auto state = ReadArmState{};

  for (int i = 0; i < 3; ++i) {
    auto ticket = state.TryArm();
    ASSERT_TRUE(ticket.has_value());
    state.NoteReadFinished();
  }
}

// With a read armed the session is idle, so the thread asking for termination owns the socket
// and closes it itself.
TEST(ReadArmState, TerminationClosesAnIdleSessionItself) {
  auto state = ReadArmState{};
  ASSERT_TRUE(state.TryArm().has_value());

  state.RequestTermination();
  EXPECT_TRUE(state.ClaimForTermination());
}

// With no read armed a worker may be mid-request and owns the socket, so termination must leave
// the closing to whoever arms next rather than closing underneath that worker.
TEST(ReadArmState, TerminationDefersWhileAWorkerOwnsTheSocket) {
  auto state = ReadArmState{};

  state.RequestTermination();
  EXPECT_FALSE(state.ClaimForTermination());
}

// The deferral is only safe because the next arm refuses and reports that it has to close, so a
// requested termination cannot be dropped by both sides.
TEST(ReadArmState, AnArmAfterADeferredTerminationRefuses) {
  auto state = ReadArmState{};
  state.RequestTermination();
  ASSERT_FALSE(state.ClaimForTermination());

  EXPECT_FALSE(state.TryArm().has_value());
}

// A read that completes after termination was requested still leaves the closing to the next arm.
TEST(ReadArmState, ReadFinishingAfterTerminationStillDefers) {
  auto state = ReadArmState{};
  ASSERT_TRUE(state.TryArm().has_value());
  state.RequestTermination();
  state.NoteReadFinished();

  EXPECT_FALSE(state.ClaimForTermination());
  EXPECT_FALSE(state.TryArm().has_value());
}

// Once a terminated session has been claimed, a second request must not claim it again, or the
// socket would be torn down twice.
TEST(ReadArmState, OnlyOneClaimSucceedsForOneArmedRead) {
  auto state = ReadArmState{};
  ASSERT_TRUE(state.TryArm().has_value());
  state.RequestTermination();

  ASSERT_TRUE(state.ClaimForTermination());
  EXPECT_FALSE(state.ClaimForTermination());
}

// Termination is never withdrawn, so every later arm keeps refusing.
TEST(ReadArmState, TerminationIsNotForgotten) {
  auto state = ReadArmState{};
  state.RequestTermination();

  EXPECT_FALSE(state.TryArm().has_value());
  EXPECT_FALSE(state.TryArm().has_value());
}

// Holding the ticket is what keeps a termination from being decided halfway through an arm: while
// a ticket is alive the session counts as armed, so a claim attempt waits and then succeeds
// rather than seeing a half-finished state and deferring to an arm that has already happened.
TEST(ReadArmState, ClaimWaitsForAnArmInProgressAndThenSucceeds) {
  auto state = ReadArmState{};
  auto claimed = std::atomic<bool>{false};
  auto claim_returned = std::atomic<bool>{false};

  auto ticket = state.TryArm();
  ASSERT_TRUE(ticket.has_value());
  state.RequestTermination();

  auto claimer = std::thread{[&state, &claimed, &claim_returned] {
    claimed.store(state.ClaimForTermination(), std::memory_order_release);
    claim_returned.store(true, std::memory_order_release);
  }};

  // The claim cannot resolve while the arm still holds its ticket.
  std::this_thread::sleep_for(std::chrono::milliseconds{50});
  EXPECT_FALSE(claim_returned.load(std::memory_order_acquire));

  ticket.reset();
  claimer.join();

  EXPECT_TRUE(claim_returned.load(std::memory_order_acquire));
  EXPECT_TRUE(claimed.load(std::memory_order_acquire));
}

// Whichever way an arm and a termination are ordered, the session is closed exactly once: twice
// tears down a socket someone still holds, and never leaves a session that ignores the request.
// Both orderings are reachable, so both are stated here as outcomes rather than left to a race.
TEST(ReadArmState, ExactlyOneSideClosesWhicheverOrderTheyArriveIn) {
  {
    // The arm gets there first, so the read is armed and the claim finds it and closes.
    auto state = ReadArmState{};
    auto closes = 0;
    if (!state.TryArm().has_value()) ++closes;
    state.RequestTermination();
    if (state.ClaimForTermination()) ++closes;
    EXPECT_EQ(closes, 1);
  }
  {
    // The request gets there first, so the claim finds no armed read and defers, and the arm
    // that follows refuses and closes.
    auto state = ReadArmState{};
    auto closes = 0;
    state.RequestTermination();
    if (state.ClaimForTermination()) ++closes;
    if (!state.TryArm().has_value()) ++closes;
    EXPECT_EQ(closes, 1);
  }
}
