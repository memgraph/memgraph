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

#include "coordination/coordinator_rpc.hpp"
#include "rpc/utils.hpp"
#include "slk_common.hpp"

#include <gtest/gtest.h>

#ifdef MG_ENTERPRISE

using memgraph::coordination::ForwardedStatus;
using memgraph::coordination::RemoveCoordinatorInstanceStatus;
using memgraph::coordination::RemoveCoordinatorRes;
using memgraph::coordination::RemoveCoordinatorResV1;

using Reason = ForwardedStatus<RemoveCoordinatorInstanceStatus>;

// A leader declines a forwarded write for one of many reasons, and the follower can name the one it hit to the user
// only if that reason travels back with the answer.
TEST(ForwardedStatus, TheReasonALeaderDeclinedForReachesTheFollower) {
  RemoveCoordinatorRes const sent{Reason{RemoveCoordinatorInstanceStatus::RAFT_CANNOT_REMOVE_LEADER}};

  memgraph::slk::Loopback loopback;
  memgraph::slk::Save(sent, loopback.GetBuilder());
  RemoveCoordinatorRes received;
  memgraph::slk::Load(&received, loopback.GetReader());

  ASSERT_TRUE(received.arg_.has_value());
  EXPECT_EQ(*received.arg_, RemoveCoordinatorInstanceStatus::RAFT_CANNOT_REMOVE_LEADER);
}

// A coordinator that predates the reason travelling reads a single flag, so every reason other than success has to
// arrive as a failure. A leader that answered nothing at all did not succeed either.
TEST(ForwardedStatus, APeerThatReadsOnlyAFlagIsToldSuccessOnlyForSuccess) {
  EXPECT_TRUE(Reason{RemoveCoordinatorInstanceStatus::SUCCESS}.Downgrade());
  EXPECT_FALSE(Reason{RemoveCoordinatorInstanceStatus::NO_SUCH_ID}.Downgrade());
  EXPECT_FALSE(Reason{}.Downgrade());
}

// The flag is what an older peer reads off the wire, so it has to be written in the shape that peer asked for.
TEST(ForwardedStatus, APeerThatReadsOnlyAFlagGetsOneItCanParse) {
  RemoveCoordinatorRes const sent{Reason{RemoveCoordinatorInstanceStatus::NOT_LEADER}};

  memgraph::slk::Loopback loopback;
  memgraph::rpc::SaveWithDowngrade(sent, RemoveCoordinatorResV1::kVersion, loopback.GetBuilder());
  RemoveCoordinatorResV1 received;
  memgraph::slk::Load(&received, loopback.GetReader());

  EXPECT_FALSE(received.arg_);
}

#endif
