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

#include <algorithm>
#include <atomic>
#include <chrono>
#include <cstdint>
#include <filesystem>
#include <optional>
#include <string>
#include <string_view>
#include <thread>
#include <tuple>

#include "dbms/database.hpp"
#include "memory/db_arena.hpp"
#include "storage/v2/durability/durability.hpp"
#include "storage/v2/durability/paths.hpp"
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
                     .wal_file_size_kibibytes = 1,
                     .wal_file_flush_every_n_tx = 1,
                     .recovery_history_bound = recovery_history_bound,
                     .retention_floor = retention_floor},
  };
}

auto *Mem(memgraph::dbms::Database &db) { return static_cast<ms::InMemoryStorage *>(db.storage()); }

void Commit(ms::Storage::Accessor &acc) {
  ASSERT_TRUE(acc.PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()).has_value());
}

void Snapshot(memgraph::dbms::Database &db) { ASSERT_TRUE(Mem(db)->CreateSnapshot(/*force=*/true).has_value()); }

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

Gids CreateBase(memgraph::dbms::Database &db) {
  const auto p = db.storage()->NameToProperty("p");
  auto acc = db.Access(ms::WRITE);
  auto v1 = acc->CreateVertex();
  auto v2 = acc->CreateVertex();
  auto v3 = acc->CreateVertex();
  EXPECT_TRUE(v1.SetProperty(p, ms::PropertyValue(1)).has_value());
  auto e1 = acc->CreateEdge(&v1, &v2, db.storage()->NameToEdgeType("T"));
  EXPECT_TRUE(e1.has_value());
  Gids gids{v1.Gid(), v2.Gid(), v3.Gid(), e1->Gid(), ms::kInvalidGid};
  Commit(*acc);
  return gids;
}

void SetV1(memgraph::dbms::Database &db, const Gids &g, int64_t value) {
  auto acc = db.Access(ms::WRITE);
  auto v1 = acc->FindVertex(g.v1, ms::View::OLD);
  ASSERT_TRUE(v1.has_value());
  ASSERT_TRUE(v1->SetProperty(db.storage()->NameToProperty("p"), ms::PropertyValue(value)).has_value());
  Commit(*acc);
}

Gids MoveMainOn(memgraph::dbms::Database &db, Gids g) {
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

void ExpectBaseState(memgraph::dbms::Database &db, const Gids &g) {
  auto acc = db.Access(ms::READ);
  auto v1 = acc->FindVertex(g.v1, ms::View::OLD);
  ASSERT_TRUE(v1.has_value());
  EXPECT_EQ(*v1->GetProperty(db.storage()->NameToProperty("p"), ms::View::OLD), ms::PropertyValue(1));
  EXPECT_TRUE(acc->FindVertex(g.v2, ms::View::OLD).has_value()) << "vertex deleted after the floor must be present";
  EXPECT_TRUE(acc->FindVertex(g.v3, ms::View::OLD).has_value());
  auto out = v1->OutEdges(ms::View::OLD);
  ASSERT_TRUE(out.has_value());
  ASSERT_EQ(out->edges.size(), 1U);
  EXPECT_EQ(out->edges[0].Gid(), g.e1);
  EXPECT_FALSE(acc->FindVertex(g.v4, ms::View::OLD).has_value()) << "vertex created after the floor must be absent";
}

// Fills several 1 KiB WAL files, so snapshot passes have WAL files to prune.
void RotateWal(memgraph::dbms::Database &db, const Gids &g, int64_t base) {
  for (int i = 0; i < 20; ++i) SetV1(db, g, base + i);
}

void SnapshotPastTheFloor(memgraph::dbms::Database &db, Gids &gids, bool snapshot_below_floor,
                          uint64_t retention_count) {
  gids = MoveMainOn(db, gids);
  RotateWal(db, gids, 200);
  Snapshot(db);
  RotateWal(db, gids, 300);
  SetV1(db, gids, 11);
  Snapshot(db);
  RotateWal(db, gids, 400);
  SetV1(db, gids, 12);
  Snapshot(db);
  EXPECT_EQ(CountFiles("snapshots"), retention_count + (snapshot_below_floor ? 1U : 0U))
      << "the newest " << retention_count << " snapshots"
      << (snapshot_below_floor ? " and the one below the floor" : "");
  if (!snapshot_below_floor) {
    // No snapshot to rebuild the floor from: the whole WAL chain back to the first file must stay.
    auto wals =
        memgraph::storage::durability::GetWalFiles(kStorageDirectory / memgraph::storage::durability::kWalDirectory);
    ASSERT_TRUE(wals.has_value());
    EXPECT_GT(wals->size(), 1U) << "the WAL must have rotated";
    EXPECT_TRUE(std::ranges::any_of(*wals, [](const auto &w) { return w.seq_num == 0; }))
        << "the first WAL file must survive";
  }
}

class RetentionFloorTest : public ::testing::Test {
 protected:
  void SetUp() override { std::filesystem::remove_all(kStorageDirectory); }

  void TearDown() override { std::filesystem::remove_all(kStorageDirectory); }
};

// Retention keeps only the newest snapshots, yet the state at the floor must stay recoverable: the snapshot below the
// floor plus the WAL after it, or, when no snapshot exists below the floor, every WAL file back to the first one.
class RetentionFloorSnapshotTest : public RetentionFloorTest,
                                   public ::testing::WithParamInterface<std::tuple<bool, uint64_t>> {};

TEST_P(RetentionFloorSnapshotTest, ForkPinFloorSurvivesSnapshotRetention) {
  const auto [snapshot_below_floor, retention_count] = GetParam();
  Gids gids{};
  uint64_t floor = 0;
  {
    memgraph::dbms::Database db{MakeConfig(/*recover_on_startup=*/false, retention_count)};
    const memgraph::memory::DbArenaScope arena_scope{&db.Arena()};
    gids = CreateBase(db);
    if (snapshot_below_floor) Snapshot(db);
    EXPECT_FALSE(Mem(db)->RetentionFloor().has_value());
    floor = Mem(db)->RegisterBranchForkPin() - 1;
    EXPECT_EQ(Mem(db)->RetentionFloor(), floor);
    SnapshotPastTheFloor(db, gids, snapshot_below_floor, retention_count);
    Mem(db)->RegisterBranchForkPin();
    EXPECT_EQ(Mem(db)->RetentionFloor(), floor);
  }

  memgraph::dbms::Database db{MakeConfig(/*recover_on_startup=*/true, retention_count, std::nullopt, floor)};
  const memgraph::memory::DbArenaScope arena_scope{&db.Arena()};
  EXPECT_EQ(Mem(db)->RetentionFloor(), floor) << "the recovery bound seeds the floor";
  ExpectBaseState(db, gids);
}

// The floor comes from the configuration (no pin in this run): its snapshot passes must keep the base too.
TEST_P(RetentionFloorSnapshotTest, ConfiguredFloorSurvivesSnapshotRetention) {
  const auto [snapshot_below_floor, retention_count] = GetParam();
  Gids gids{};
  uint64_t floor = 0;
  {
    memgraph::dbms::Database db{MakeConfig(/*recover_on_startup=*/false, retention_count)};
    const memgraph::memory::DbArenaScope arena_scope{&db.Arena()};
    gids = CreateBase(db);
    if (snapshot_below_floor) Snapshot(db);
    floor = Mem(db)->RegisterForkPin() - 1;
  }
  {
    memgraph::dbms::Database db{MakeConfig(/*recover_on_startup=*/true, retention_count, floor)};
    const memgraph::memory::DbArenaScope arena_scope{&db.Arena()};
    EXPECT_EQ(Mem(db)->RetentionFloor(), floor);
    SnapshotPastTheFloor(db, gids, snapshot_below_floor, retention_count);
  }

  memgraph::dbms::Database db{MakeConfig(/*recover_on_startup=*/true, retention_count, std::nullopt, floor)};
  const memgraph::memory::DbArenaScope arena_scope{&db.Arena()};
  ExpectBaseState(db, gids);
}

INSTANTIATE_TEST_SUITE_P(Retention, RetentionFloorSnapshotTest,
                         ::testing::Combine(::testing::Bool(), ::testing::Values(uint64_t{1}, uint64_t{2})),
                         [](const auto &info) {
                           return std::string{std::get<0>(info.param) ? "WithASnapshotBelowTheFloor"
                                                                      : "WithoutASnapshotBelowTheFloor"} +
                                  "_Retain" + std::to_string(std::get<1>(info.param));
                         });

// A fork lowering the floor while snapshot retention passes run keeps the state at the fork recoverable.
TEST_F(RetentionFloorTest, ForkDuringSnapshotRetentionKeepsItsBase) {
  for (int round = 0; round < 3; ++round) {
    SCOPED_TRACE("round " + std::to_string(round));
    std::filesystem::remove_all(kStorageDirectory);
    Gids gids{};
    uint64_t floor = 0;
    ms::PropertyValue at_fork;
    {
      memgraph::dbms::Database db{MakeConfig(/*recover_on_startup=*/false, /*retention=*/1)};
      const memgraph::memory::DbArenaScope arena_scope{&db.Arena()};
      gids = CreateBase(db);
      std::atomic<int> passes{0};
      std::jthread snapshots{[&] {
        const memgraph::memory::DbArenaScope thread_scope{&db.Arena()};
        for (int i = 0; i < 20; ++i) {
          SetV1(db, gids, 100 + i);
          Snapshot(db);
          passes.fetch_add(1, std::memory_order_release);
        }
      }};
      while (passes.load(std::memory_order_acquire) < 3 + 3 * round) std::this_thread::yield();
      auto const fork_ts = Mem(db)->RegisterBranchForkPin();
      floor = fork_ts - 1;
      auto historical = Mem(db)->HistoricalAccess(fork_ts);
      ASSERT_TRUE(historical.has_value());
      auto v1 = (*historical)->FindVertex(gids.v1, ms::View::OLD);
      ASSERT_TRUE(v1.has_value());
      at_fork = *v1->GetProperty(db.storage()->NameToProperty("p"), ms::View::OLD);
      snapshots.join();
      EXPECT_EQ(Mem(db)->RetentionFloor(), floor);
    }

    memgraph::dbms::Database db{MakeConfig(/*recover_on_startup=*/true, /*retention=*/1, std::nullopt, floor)};
    const memgraph::memory::DbArenaScope arena_scope{&db.Arena()};
    auto acc = db.Access(ms::READ);
    auto v1 = acc->FindVertex(gids.v1, ms::View::OLD);
    ASSERT_TRUE(v1.has_value());
    EXPECT_EQ(*v1->GetProperty(db.storage()->NameToProperty("p"), ms::View::OLD), at_fork);
  }
}

TEST(RetentionFloorDeathTest, BoundAndFloorAreMutuallyExclusive) {
  std::filesystem::remove_all(kStorageDirectory);
  EXPECT_DEATH(ms::InMemoryStorage{MakeConfig(/*recover_on_startup=*/false, /*retention=*/1, 5, 5)},
               "mutually exclusive");
  std::filesystem::remove_all(kStorageDirectory);
}

}  // namespace
