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

#include <algorithm>
#include <atomic>
#include <chrono>
#include <filesystem>
#include <string>
#include <thread>
#include <vector>

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

// Replica writes race with promotion and minting; no MAIN commit may land at or below the last committed ts.
TEST(SystemTimestamp, PromotionRacingMintAndCommitNeverCommitsAtOrBelowLastCommitted) {
  constexpr uint64_t kReplicaWrites = 2000;
  constexpr int kMinters = 4;
  constexpr int kTxnsPerMinter = 200;

  struct Commit {
    uint64_t ts;
    uint64_t lcts_before;
  };

  System system;
  auto access = system.CreateSystemStateAccess();
  std::atomic_bool go{false};
  std::atomic_bool promoted{false};
  std::vector<std::vector<Commit>> commits(kMinters);

  std::thread writer([&] {
    while (!go.load(std::memory_order_acquire)) std::this_thread::yield();
    for (uint64_t i = 1; i <= kReplicaWrites; ++i) access.SetLastCommitedTS(i);
  });
  std::thread promoter([&] {
    while (!go.load(std::memory_order_acquire)) std::this_thread::yield();
    writer.join();
    system.ResyncTimestampOnNextTransaction();
    promoted.store(true, std::memory_order_release);
  });
  std::vector<std::thread> minters;
  for (int m = 0; m < kMinters; ++m) {
    minters.emplace_back([&, m] {
      while (!go.load(std::memory_order_acquire)) std::this_thread::yield();
      for (int i = 0; i < kTxnsPerMinter; ++i) {
        auto txn = system.TryCreateTransaction(std::chrono::seconds{5});
        while (!txn) txn = system.TryCreateTransaction(std::chrono::seconds{5});
        txn->AddAction<NoopAction>();
        if (!promoted.load(std::memory_order_acquire)) {
          txn->Commit(DoLocal{});
          continue;
        }
        auto const lcts_before = system.LastCommittedSystemTimestamp();
        txn->Commit(DoNothing{});
        commits[m].push_back({txn->timestamp(), lcts_before});
      }
    });
  }
  go.store(true, std::memory_order_release);
  for (auto &t : minters) t.join();
  promoter.join();

  // Guarantees the test is never vacuous.
  {
    auto txn = system.TryCreateTransaction(std::chrono::seconds{5});
    ASSERT_TRUE(txn);
    txn->AddAction<NoopAction>();
    auto const lcts_before = system.LastCommittedSystemTimestamp();
    txn->Commit(DoNothing{});
    commits[0].push_back({txn->timestamp(), lcts_before});
  }

  std::vector<uint64_t> all;
  for (auto const &per_thread : commits) {
    for (auto const &c : per_thread) {
      EXPECT_GT(c.ts, c.lcts_before);
      EXPECT_GT(c.ts, kReplicaWrites);
      all.push_back(c.ts);
    }
  }
  std::ranges::sort(all);
  EXPECT_EQ(std::ranges::adjacent_find(all), all.end());
  EXPECT_EQ(system.LastCommittedSystemTimestamp(), all.back());
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
