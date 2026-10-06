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

#include <unistd.h>

#include <filesystem>
#include <string>

#include <gtest/gtest.h>

#include "system/system.hpp"

namespace memgraph::system {
namespace {

struct NoopAction final : ISystemAction {
  void DoDurability() override {}

  bool ShouldReplicateInCommunity() const override { return true; }

  bool DoReplication(replication::ReplicationClient & /*client*/, const utils::UUID & /*main_uuid*/,
                     Transaction const & /*system_tx*/) const override {
    return true;
  }

  void PostReplication(replication::RoleMainData & /*main_data*/) const override {}
};

}  // namespace

TEST(SystemTimestamp, PromotedReplicaMintsAboveLastCommitted) {
  System system;
  system.CreateSystemStateAccess().SetLastCommitedTS(6);
  system.ResyncTimestampOnNextTransaction();

  {
    auto txn = system.TryCreateTransaction();
    ASSERT_TRUE(txn);
    EXPECT_EQ(txn->timestamp(), 7);
    txn->AddAction<NoopAction>();
    txn->Commit(DoNothing{});
  }
  EXPECT_EQ(system.LastCommittedSystemTimestamp(), 7);

  // Resync is one-shot; the counter keeps advancing.
  {
    auto txn = system.TryCreateTransaction();
    ASSERT_TRUE(txn);
    EXPECT_EQ(txn->timestamp(), 8);
    txn->AddAction<NoopAction>();
    txn->Commit(DoNothing{});
  }
  EXPECT_EQ(system.LastCommittedSystemTimestamp(), 8);
}

TEST(SystemTimestamp, ReplicaCounterIsIndependentOfLastCommittedWithoutPromotion) {
  System system;
  system.CreateSystemStateAccess().SetLastCommitedTS(6);

  {
    auto txn = system.TryCreateTransaction();
    ASSERT_TRUE(txn);
    EXPECT_EQ(txn->timestamp(), 1);
  }
  EXPECT_EQ(system.LastCommittedSystemTimestamp(), 6);
}

// A replica's own system txn must not move the last committed timestamp.
TEST(SystemTimestamp, ReplicaLocalCommitLeavesLastCommittedUntouched) {
  System system;
  system.CreateSystemStateAccess().SetLastCommitedTS(6);

  {
    auto txn = system.TryCreateTransaction();
    ASSERT_TRUE(txn);
    txn->AddAction<NoopAction>();
    txn->Commit(DoLocal{});
  }
  EXPECT_EQ(system.LastCommittedSystemTimestamp(), 6);
}

// The commit floor lifts a stale minted timestamp above the last committed one.
TEST(SystemTimestamp, TxnMintedBeforePromotionCommitsAboveLastCommitted) {
  System system;

  {
    auto txn = system.TryCreateTransaction();
    ASSERT_TRUE(txn);
    system.CreateSystemStateAccess().SetLastCommitedTS(6);
    txn->AddAction<NoopAction>();
    txn->Commit(DoNothing{});
    EXPECT_EQ(txn->timestamp(), 7);
  }
  EXPECT_EQ(system.LastCommittedSystemTimestamp(), 7);
}

class SystemTimestampRestart : public ::testing::Test {
 protected:
  void SetUp() override { std::filesystem::create_directories(dir_); }

  void TearDown() override { std::filesystem::remove_all(dir_); }

  std::filesystem::path dir_{std::filesystem::temp_directory_path() /
                             ("unit_system_timestamp_test_" + std::to_string(static_cast<int>(getpid())))};
};

TEST_F(SystemTimestampRestart, ReplicaLastCommittedSurvivesRestart) {
  {
    System system{dir_, /*recovery_on_startup=*/true};
    system.CreateSystemStateAccess().SetLastCommitedTS(6);
  }

  System restarted{dir_, true};
  EXPECT_EQ(restarted.LastCommittedSystemTimestamp(), 6);
}

}  // namespace memgraph::system
