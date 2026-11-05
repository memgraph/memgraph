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

#include "storage/v2/commit_ts_info.hpp"

using memgraph::storage::CommitTsInfo;

// A replica's durable timestamp can run ahead of its committed count once it loads a snapshot, and
// a commit can record a count a heartbeat has not yet seen. Merging by whole value, taking the side
// whose timestamp is larger, would discard a count that had already been recorded.
TEST(CommitTsInfo, MaxTakesTheLargerOfEachFieldSeparately) {
  constexpr auto recorded = CommitTsInfo{.ldt_ = 7, .num_committed_txns_ = 2};
  constexpr auto observed = CommitTsInfo{.ldt_ = 3, .num_committed_txns_ = 9};

  const auto merged = Max(recorded, observed);

  EXPECT_EQ(merged.ldt_, 7U);
  EXPECT_EQ(merged.num_committed_txns_, 9U);
}

// What the main has recorded about a replica is folded into, never replaced by, a later reading of
// it. An observation that arrives stale leaves the record alone, which is what lets a heartbeat and
// a committing transaction update the same record in either order.
TEST(CommitTsInfo, MaxLetsNeitherFieldRegress) {
  constexpr auto recorded = CommitTsInfo{.ldt_ = 12, .num_committed_txns_ = 4};

  for (const auto &stale : {CommitTsInfo{.ldt_ = 0, .num_committed_txns_ = 0},
                            CommitTsInfo{.ldt_ = 11, .num_committed_txns_ = 4},
                            CommitTsInfo{.ldt_ = 12, .num_committed_txns_ = 3}}) {
    const auto merged = Max(recorded, stale);
    EXPECT_EQ(merged.ldt_, recorded.ldt_);
    EXPECT_EQ(merged.num_committed_txns_, recorded.num_committed_txns_);

    // Order cannot matter, because the two writers reach the record in either one.
    const auto reversed = Max(stale, recorded);
    EXPECT_EQ(reversed.ldt_, recorded.ldt_);
    EXPECT_EQ(reversed.num_committed_txns_, recorded.num_committed_txns_);
  }
}
