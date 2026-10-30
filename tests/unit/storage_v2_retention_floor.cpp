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

// Snapshot/WAL retention under a retention floor: the state as of the floor stays recoverable (bounded recovery up to
// it rebuilds it) however many snapshots retention drops.

#include <gtest/gtest.h>

#include <chrono>
#include <cstdint>
#include <filesystem>
#include <optional>
#include <string_view>

#include "storage/v2/inmemory/storage.hpp"
#include "storage/v2/view.hpp"
#include "tests/test_commit_args_helper.hpp"
#include "utils/scheduler.hpp"

namespace ms = memgraph::storage;

namespace {
const std::filesystem::path kStorageDirectory{std::filesystem::temp_directory_path() / "storage_v2_retention_floor"};

ms::Config MakeConfig(bool recover_on_startup, uint64_t snapshot_retention_count,
                      std::optional<uint64_t> retention_floor = std::nullopt,
                      std::optional<uint64_t> recovery_history_bound = std::nullopt) {
  return ms::Config{
      .durability = {.storage_directory = kStorageDirectory,
                     .recover_on_startup = recover_on_startup,
                     .snapshot_wal_mode = ms::Config::Durability::SnapshotWalMode::PERIODIC_SNAPSHOT_WITH_WAL,
                     .snapshot_interval = memgraph::utils::SchedulerInterval{std::chrono::hours(24)},
                     .snapshot_retention_count = snapshot_retention_count,
                     .recovery_history_bound = recovery_history_bound,
                     .retention_floor = retention_floor},
  };
}

void Commit(ms::Storage::Accessor &acc) {
  ASSERT_TRUE(acc.PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()).has_value());
}

void Snapshot(ms::InMemoryStorage &db) { ASSERT_TRUE(db.CreateSnapshot(/*force=*/true).has_value()); }

size_t CountFiles(std::string_view subdirectory) {
  const auto dir = kStorageDirectory / subdirectory;
  if (!std::filesystem::exists(dir)) return 0;
  size_t count = 0;
  for (auto const &entry : std::filesystem::directory_iterator(dir)) count += entry.is_regular_file() ? 1 : 0;
  return count;
}

// v1 -e1-> v2, v3, with v1.p = 1.
struct Gids {
  ms::Gid v1, v2, v3, e1, v4;
};

Gids CreateBase(ms::InMemoryStorage &db) {
  const auto p = db.NameToProperty("p");
  auto acc = db.Access(ms::WRITE);
  auto v1 = acc->CreateVertex();
  auto v2 = acc->CreateVertex();
  auto v3 = acc->CreateVertex();
  EXPECT_TRUE(v1.SetProperty(p, ms::PropertyValue(1)).has_value());
  auto e1 = acc->CreateEdge(&v1, &v2, db.NameToEdgeType("T"));
  EXPECT_TRUE(e1.has_value());
  Gids gids{v1.Gid(), v2.Gid(), v3.Gid(), e1->Gid(), ms::kInvalidGid};
  Commit(*acc);
  return gids;
}

void SetV1(ms::InMemoryStorage &db, const Gids &g, int64_t value) {
  auto acc = db.Access(ms::WRITE);
  auto v1 = acc->FindVertex(g.v1, ms::View::OLD);
  ASSERT_TRUE(v1.has_value());
  ASSERT_TRUE(v1->SetProperty(db.NameToProperty("p"), ms::PropertyValue(value)).has_value());
  Commit(*acc);
}

// update v1.p, detach-delete v2, delete v3, create v4.
Gids MoveMainOn(ms::InMemoryStorage &db, Gids g) {
  SetV1(db, g, 10);
  auto acc = db.Access(ms::WRITE);
  auto v2 = acc->FindVertex(g.v2, ms::View::OLD);
  auto v3 = acc->FindVertex(g.v3, ms::View::OLD);
  EXPECT_TRUE(v2.has_value() && v3.has_value());
  EXPECT_TRUE(acc->DetachDeleteVertex(&*v2).has_value());
  EXPECT_TRUE(acc->DeleteVertex(&*v3).has_value());
  g.v4 = acc->CreateVertex().Gid();
  Commit(*acc);
  return g;
}

// The state CreateBase left, before MoveMainOn.
void ExpectBaseState(ms::InMemoryStorage &db, const Gids &g) {
  auto acc = db.Access(ms::READ);
  auto v1 = acc->FindVertex(g.v1, ms::View::OLD);
  ASSERT_TRUE(v1.has_value());
  EXPECT_EQ(*v1->GetProperty(db.NameToProperty("p"), ms::View::OLD), ms::PropertyValue(1));
  EXPECT_TRUE(acc->FindVertex(g.v2, ms::View::OLD).has_value()) << "vertex deleted after the floor must be present";
  EXPECT_TRUE(acc->FindVertex(g.v3, ms::View::OLD).has_value());
  auto out = v1->OutEdges(ms::View::OLD);
  ASSERT_TRUE(out.has_value());
  ASSERT_EQ(out->edges.size(), 1U);
  EXPECT_EQ(out->edges[0].Gid(), g.e1);
  EXPECT_FALSE(acc->FindVertex(g.v4, ms::View::OLD).has_value()) << "vertex created after the floor must be absent";
}

// Moves main on past the floor and takes three snapshots above it under a retention count of 1.
void SnapshotPastTheFloor(ms::InMemoryStorage &db, Gids &gids, bool snapshot_below_floor) {
  gids = MoveMainOn(db, gids);
  Snapshot(db);
  SetV1(db, gids, 11);
  Snapshot(db);
  SetV1(db, gids, 12);
  Snapshot(db);
  EXPECT_EQ(CountFiles("snapshots"), snapshot_below_floor ? 2U : 1U)
      << (snapshot_below_floor ? "the newest snapshot and the one below the floor" : "only the newest snapshot");
}

class RetentionFloorTest : public ::testing::Test {
 protected:
  void SetUp() override { std::filesystem::remove_all(kStorageDirectory); }

  void TearDown() override { std::filesystem::remove_all(kStorageDirectory); }
};

// Retention keeps only the newest snapshot, yet the state at the floor must stay recoverable: the snapshot below the
// floor plus the WAL after it, or, when no snapshot exists below the floor, every WAL file back to the first one.
class RetentionFloorSnapshotTest : public RetentionFloorTest, public ::testing::WithParamInterface<bool> {};

TEST_P(RetentionFloorSnapshotTest, ForkPinFloorSurvivesSnapshotRetention) {
  const bool snapshot_below_floor = GetParam();
  Gids gids{};
  uint64_t floor = 0;
  {
    ms::InMemoryStorage db{MakeConfig(/*recover_on_startup=*/false, /*retention=*/1)};
    gids = CreateBase(db);
    if (snapshot_below_floor) Snapshot(db);
    EXPECT_FALSE(db.RetentionFloor().has_value());
    floor = db.RegisterBranchForkPin() - 1;
    EXPECT_EQ(db.RetentionFloor(), floor);
    SnapshotPastTheFloor(db, gids, snapshot_below_floor);
    // A later pin does not raise the floor.
    db.RegisterBranchForkPin();
    EXPECT_EQ(db.RetentionFloor(), floor);
  }

  ms::InMemoryStorage db{MakeConfig(/*recover_on_startup=*/true, /*retention=*/1, std::nullopt, floor)};
  EXPECT_EQ(db.RetentionFloor(), floor) << "the recovery bound seeds the floor";
  ExpectBaseState(db, gids);
}

// The floor comes from the configuration (no pin in this run): its snapshot passes must keep the base too.
TEST_P(RetentionFloorSnapshotTest, ConfiguredFloorSurvivesSnapshotRetention) {
  const bool snapshot_below_floor = GetParam();
  Gids gids{};
  uint64_t floor = 0;
  {
    ms::InMemoryStorage db{MakeConfig(/*recover_on_startup=*/false, /*retention=*/1)};
    gids = CreateBase(db);
    if (snapshot_below_floor) Snapshot(db);
    floor = db.RegisterForkPin() - 1;
  }
  {
    ms::InMemoryStorage db{MakeConfig(/*recover_on_startup=*/true, /*retention=*/1, floor)};
    EXPECT_EQ(db.RetentionFloor(), floor);
    SnapshotPastTheFloor(db, gids, snapshot_below_floor);
  }

  ms::InMemoryStorage db{MakeConfig(/*recover_on_startup=*/true, /*retention=*/1, std::nullopt, floor)};
  ExpectBaseState(db, gids);
}

INSTANTIATE_TEST_SUITE_P(Retention, RetentionFloorSnapshotTest, ::testing::Bool(), [](const auto &info) {
  return info.param ? "WithASnapshotBelowTheFloor" : "WithoutASnapshotBelowTheFloor";
});

TEST_F(RetentionFloorTest, ClearingTheFloorLetsRetentionCleanUpAgain) {
  ms::InMemoryStorage db{MakeConfig(/*recover_on_startup=*/false, /*retention=*/1)};
  auto gids = CreateBase(db);
  Snapshot(db);
  const auto fork_ts = db.RegisterBranchForkPin();
  SetV1(db, gids, 2);
  Snapshot(db);
  SetV1(db, gids, 3);
  Snapshot(db);
  ASSERT_EQ(CountFiles("snapshots"), 2U);
  EXPECT_TRUE(db.RetentionFloor().has_value());

  const auto wal_files_with_floor = CountFiles("wal");
  db.ReleaseForkPin(fork_ts);
  db.SetRetentionFloor(std::nullopt);
  SetV1(db, gids, 4);
  Snapshot(db);
  EXPECT_EQ(CountFiles("snapshots"), 1U);
  EXPECT_LE(CountFiles("wal"), wal_files_with_floor);
}

TEST_F(RetentionFloorTest, RetentionIsUnchangedWithoutAFloor) {
  ms::InMemoryStorage db{MakeConfig(/*recover_on_startup=*/false, /*retention=*/2)};
  auto gids = CreateBase(db);
  EXPECT_FALSE(db.RetentionFloor().has_value());
  for (int i = 0; i < 4; ++i) {
    Snapshot(db);
    SetV1(db, gids, i);
  }
  EXPECT_EQ(CountFiles("snapshots"), 2U);
}

}  // namespace
