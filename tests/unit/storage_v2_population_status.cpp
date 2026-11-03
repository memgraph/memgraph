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

#include "storage/v2/population_status.hpp"
#include "storage/v2/timestamp_types.hpp"
#include "storage/v2/transaction_constants.hpp"

namespace memgraph::storage {

TEST(PopulationStatus, APopulatingObjectIsVisibleToNothing) {
  PopulationStatus status;

  ASSERT_TRUE(status.IsPopulating());
  // The sentinel sits above the whole committed range, which is what keeps a half-built index out
  // of every snapshot without the reader testing readiness separately.
  EXPECT_FALSE(status.IsVisible(CommitTimestamp::FromUint(kLargestCommittedTimestamp)));
}

TEST(PopulationStatus, ACommittedObjectIsVisibleAtAndAboveItsCommitTimestamp) {
  PopulationStatus status;
  status.Commit(CommitTimestamp::FromUint(10));

  EXPECT_TRUE(status.IsReady());
  EXPECT_FALSE(status.IsVisible(CommitTimestamp::FromUint(9)));
  EXPECT_TRUE(status.IsVisible(CommitTimestamp::FromUint(10)));
  EXPECT_TRUE(status.IsVisible(CommitTimestamp::FromUint(11)));
}

TEST(PopulationStatus, ARecoveredObjectIsVisibleToEverySnapshot) {
  PopulationStatus status;
  status.Commit(CommitTimestamp::FromUint(kTimestampInitialId));

  EXPECT_TRUE(status.IsVisible(CommitTimestamp::FromUint(kTimestampInitialId)));
  EXPECT_TRUE(status.IsVisible(CommitTimestamp::FromUint(kLargestCommittedTimestamp)));
}

}  // namespace memgraph::storage
