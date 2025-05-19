// Copyright 2025 Memgraph Ltd.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt; by using this file, you agree to be bound by the terms of the Business Source
// License, and you may not use this file except in compliance with the Business Source License.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

#include "storage/v2/commit_log.hpp"

#include "gtest/gtest.h"

namespace {
inline constexpr size_t ids_per_block = 8192 * 64;
}  // namespace

using memgraph::storage::CommitLog;

TEST(CommitLog, Simple) {
  memgraph::storage::CommitLog log;
  EXPECT_EQ(log.OldestActive(), 0);

  log.MarkFinished(1);
  EXPECT_EQ(log.OldestActive(), 0);

  log.MarkFinished(0);
  EXPECT_EQ(log.OldestActive(), 2);
}

TEST(CommitLog, Fields) {
  memgraph::storage::CommitLog log;

  for (uint64_t i = 0; i < 64; ++i) {
    log.MarkFinished(i);
    EXPECT_EQ(log.OldestActive(), i + 1);
  }

  for (uint64_t i = 128; i < 192; ++i) {
    log.MarkFinished(i);
    EXPECT_EQ(log.OldestActive(), 64);
  }

  for (uint64_t i = 64; i < 128; ++i) {
    log.MarkFinished(i);
    EXPECT_EQ(log.OldestActive(), i < 127 ? i + 1 : 192);
  }
}

TEST(CommitLog, Blocks) {
  memgraph::storage::CommitLog log;

  for (uint64_t i = 0; i < ids_per_block; ++i) {
    log.MarkFinished(i);
    EXPECT_EQ(log.OldestActive(), i + 1);
  }

  for (uint64_t i = ids_per_block * 2; i < ids_per_block * 3; ++i) {
    log.MarkFinished(i);
    EXPECT_EQ(log.OldestActive(), ids_per_block);
  }
}

TEST(CommitLog, TrackAfterInitialId) {
  const auto check_marking_ids = [](auto *log, auto current_oldest_active) {
    ASSERT_EQ(log->OldestActive(), current_oldest_active);
    log->MarkFinished(current_oldest_active);
    ++current_oldest_active;
    ASSERT_EQ(log->OldestActive(), current_oldest_active);
  };

  for (uint64_t i = 0; i < 2 * ids_per_block; ++i) {
    memgraph::storage::CommitLog log{i};
    check_marking_ids(&log, i);
  }
}

TEST(CommitLog, MarkAsFinishedTxn) {
  CommitLog log;
  log.MarkFinished(1);
  EXPECT_FALSE(log.IsFinished(0));
  EXPECT_TRUE(log.IsFinished(1));
  EXPECT_FALSE(log.IsFinished(2));
}

TEST(CommitLog, MarkSingleTransaction) {
  CommitLog log;
  log.MarkFinishedInRange(100, 100);
  EXPECT_TRUE(log.IsFinished(100));
  EXPECT_FALSE(log.IsFinished(99));
  EXPECT_FALSE(log.IsFinished(101));
}

TEST(CommitLog, MarkRangeWithinSameField) {
  CommitLog log;
  log.MarkFinishedInRange(200, 203);
  for (uint64_t id = 200; id <= 203; ++id) {
    EXPECT_TRUE(log.IsFinished(id));
  }
  EXPECT_FALSE(log.IsFinished(199));
  EXPECT_FALSE(log.IsFinished(204));
}

TEST(CommitLog, MarkRangeAcrossFieldsInSameBlock) {
  constexpr uint64_t start = 60;  // Field 0
  constexpr uint64_t end = 70;    // Field 1
  CommitLog log;
  log.MarkFinishedInRange(start, end);
  for (uint64_t id = start; id <= end; ++id) {
    EXPECT_TRUE(log.IsFinished(id));
  }
  EXPECT_FALSE(log.IsFinished(59));
  EXPECT_FALSE(log.IsFinished(71));
}

TEST(CommitLog, MarkRangeAcrossBlocks) {
  // Assuming block size = 8192 * 64 = 524288 IDs per block
  constexpr uint64_t block_size = 8192 * 64;
  constexpr uint64_t start = block_size - 2;
  constexpr uint64_t end = block_size + 2;

  CommitLog log;

  log.MarkFinishedInRange(start, end);
  for (uint64_t id = start; id <= end; ++id) {
    EXPECT_TRUE(log.IsFinished(id));
  }
  EXPECT_FALSE(log.IsFinished(start - 1));
  EXPECT_FALSE(log.IsFinished(end + 1));
}

TEST(CommitLog, MarkRangeWithEndBeforeStartIsNoOp) {
  CommitLog log;
  log.MarkFinishedInRange(1000, 999);  // Invalid range
  EXPECT_FALSE(log.IsFinished(999));
  EXPECT_FALSE(log.IsFinished(1000));
}

TEST(CommitLog, MarkZeroToMaxIntRangeIsSafe) {
  constexpr uint64_t start = 0;
  constexpr uint64_t end = start + 10000;
  CommitLog log;
  log.MarkFinishedInRange(start, end);

  for (uint64_t id = start; id <= end; ++id) {
    EXPECT_TRUE(log.IsFinished(id));
  }
  EXPECT_FALSE(log.IsFinished(end + 1));
}
