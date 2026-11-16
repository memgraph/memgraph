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
#include <chrono>
#include <cstdint>
#include <filesystem>
#include <memory>
#include <optional>
#include <string>
#include <string_view>
#include <tuple>
#include <utility>
#include <vector>

#include "storage/v2/durability/durability.hpp"
#include "storage/v2/durability/paths.hpp"
#include "storage/v2/inmemory/storage.hpp"
#include "storage/v2/view.hpp"
#include "tests/test_commit_args_helper.hpp"
#include "utils/scheduler.hpp"

namespace ms = memgraph::storage;
namespace md = memgraph::storage::durability;

namespace {
const std::filesystem::path kStorageDirectory{std::filesystem::temp_directory_path() / "storage_v2_retention_floor"};

ms::Config MakeConfig(bool recover_on_startup, uint64_t snapshot_retention_count) {
  return ms::Config{
      .durability = {.storage_directory = kStorageDirectory,
                     .recover_on_startup = recover_on_startup,
                     .snapshot_wal_mode = ms::Config::Durability::SnapshotWalMode::PERIODIC_SNAPSHOT_WITH_WAL,
                     .snapshot_interval = memgraph::utils::SchedulerInterval{std::chrono::hours(24)},
                     .snapshot_retention_count = snapshot_retention_count,
                     .wal_file_size_kibibytes = 1,
                     .wal_file_flush_every_n_tx = 1},
  };
}

// The storage under test, with the history bounds the branch ledger would hand it.
struct Db {
  explicit Db(ms::Config config, md::HistoryBounds bounds = {})
      : store{std::move(config),
              std::nullopt,
              std::make_unique<ms::PlanInvalidatorDefault>(),
              memgraph::metrics::DatabaseMetricHandles{},
              nullptr,
              nullptr,
              nullptr,
              bounds} {}

  ms::InMemoryStorage *storage() { return &store; }

  std::unique_ptr<ms::Storage::Accessor> Access(ms::StorageAccessType type) { return store.Access(type); }

  ms::InMemoryStorage store;
};

md::HistoryBounds Bound(uint64_t bound) { return {.recovery_bound = bound}; }

md::HistoryBounds Floor(uint64_t floor) { return {.retention_floor = floor}; }

auto *Mem(Db &db) { return db.storage(); }

void Commit(ms::Storage::Accessor &acc) {
  ASSERT_TRUE(acc.PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()).has_value());
}

void Snapshot(Db &db) { ASSERT_TRUE(Mem(db)->CreateSnapshot(/*force=*/true).has_value()); }

// Snapshot file names sort by creation time.
std::vector<std::string> SnapshotNames() {
  std::vector<std::string> names;
  for (auto const &entry : std::filesystem::directory_iterator(kStorageDirectory / md::kSnapshotDirectory)) {
    names.push_back(entry.path().filename().string());
  }
  std::ranges::sort(names);
  return names;
}

size_t CountFiles(std::string_view subdirectory) {
  const auto dir = kStorageDirectory / subdirectory;
  if (!std::filesystem::exists(dir)) return 0;
  size_t count = 0;
  for (auto const &entry : std::filesystem::directory_iterator(dir)) count += entry.is_regular_file() ? 1 : 0;
  return count;
}

// v1 -e1-> v2, v3, with v1.p = 1 as of the base.
struct Gids {
  ms::Gid v1, v2, v3, e1, v4;
};

void SetV1(Db &db, const Gids &g, int64_t value) {
  auto acc = db.Access(ms::WRITE);
  auto v1 = acc->FindVertex(g.v1, ms::View::OLD);
  ASSERT_TRUE(v1.has_value());
  ASSERT_TRUE(v1->SetProperty(db.storage()->NameToProperty("p"), ms::PropertyValue(value)).has_value());
  Commit(*acc);
}

// The graph, then (optionally) a snapshot of it, then p = 1 in a later commit: with the snapshot as base, the WAL after
// it is part of the base state.
Gids CreateBase(Db &db, bool snapshot_below_floor) {
  const auto p = db.storage()->NameToProperty("p");
  Gids gids{};
  {
    auto acc = db.Access(ms::WRITE);
    auto v1 = acc->CreateVertex();
    auto v2 = acc->CreateVertex();
    auto v3 = acc->CreateVertex();
    EXPECT_TRUE(v1.SetProperty(p, ms::PropertyValue(0)).has_value());
    auto e1 = acc->CreateEdge(&v1, &v2, db.storage()->NameToEdgeType("T"));
    EXPECT_TRUE(e1.has_value());
    gids = {v1.Gid(), v2.Gid(), v3.Gid(), e1->Gid(), ms::kInvalidGid};
    Commit(*acc);
  }
  if (snapshot_below_floor) Snapshot(db);
  SetV1(db, gids, 1);
  return gids;
}

// Changes after the floor that the recovered base state must not show.
Gids MoveMainOn(Db &db, Gids g) {
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

void ExpectBaseState(Db &db, const Gids &g) {
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

// One snapshot pass past the floor, after enough commits to rotate the 1 KiB WAL files so the pass has some to prune.
void SnapshotPass(Db &db, const Gids &g, int64_t base) {
  for (int i = 0; i < 20; ++i) SetV1(db, g, base + i);
  Snapshot(db);
}

bool HasFirstWalFile() {
  auto const wals = md::GetWalFiles(kStorageDirectory / md::kWalDirectory);
  EXPECT_TRUE(wals.has_value());
  return wals && std::ranges::any_of(*wals, [](const auto &w) { return w.seq_num == 0; });
}

class RetentionFloorTest : public ::testing::Test {
 protected:
  void SetUp() override { std::filesystem::remove_all(kStorageDirectory); }

  void TearDown() override { std::filesystem::remove_all(kStorageDirectory); }
};

// Retention keeps only the newest `retention_count` snapshots, yet the state at the floor must stay recoverable: the
// newest snapshot below the floor (kept past the count, so one more than the count) plus the WAL after it, or, when no
// snapshot is below the floor, every WAL file back to the first one. The floor comes from RegisterBranchForkPin.
class RetainedBaseTest : public RetentionFloorTest, public ::testing::WithParamInterface<std::tuple<bool, uint64_t>> {};

TEST_P(RetainedBaseTest, FloorKeepsTheBaseRecoverable) {
  const auto [snapshot_below_floor, retention_count] = GetParam();
  Gids gids{};
  uint64_t floor = 0;
  {
    Db db{MakeConfig(/*recover_on_startup=*/false, retention_count)};
    gids = CreateBase(db, snapshot_below_floor);
    EXPECT_FALSE(Mem(db)->RetentionFloor().has_value());
    floor = Mem(db)->RegisterBranchForkPin() - 1;
    EXPECT_EQ(Mem(db)->RetentionFloor(), floor);

    gids = MoveMainOn(db, gids);
    // The base is the extra file once `retention_count` snapshots are newer than it.
    for (uint64_t pass = 0; pass <= retention_count; ++pass) SnapshotPass(db, gids, 100 * (pass + 2));
    EXPECT_EQ(CountFiles("snapshots"), retention_count + (snapshot_below_floor ? 1U : 0U))
        << "the newest " << retention_count << " snapshots"
        << (snapshot_below_floor ? " and the one below the floor" : "");
    if (!snapshot_below_floor) EXPECT_TRUE(HasFirstWalFile()) << "no snapshot to rebuild the floor from";

    Mem(db)->RegisterBranchForkPin();
    EXPECT_EQ(Mem(db)->RetentionFloor(), floor) << "a later fork must not raise the floor";
  }

  Db db{MakeConfig(/*recover_on_startup=*/true, retention_count), Bound(floor)};
  ExpectBaseState(db, gids);
}

INSTANTIATE_TEST_SUITE_P(Retention, RetainedBaseTest,
                         ::testing::Combine(::testing::Bool(),
                                            ::testing::Values(uint64_t{1}, uint64_t{2}, uint64_t{3})),
                         [](const auto &info) {
                           return std::string{std::get<0>(info.param) ? "WithASnapshotBelowTheFloor"
                                                                      : "WithoutASnapshotBelowTheFloor"} +
                                  "_Retain" + std::to_string(std::get<1>(info.param));
                         });

// The base is among the newest `retention_count` old snapshots: nothing extra is kept.
TEST_F(RetentionFloorTest, BaseWithinTheCountIsNotExtra) {
  Gids gids{};
  uint64_t floor = 0;
  {
    Db db{MakeConfig(/*recover_on_startup=*/false, /*retention_count=*/2)};
    gids = CreateBase(db, /*snapshot_below_floor=*/true);
    auto const first = SnapshotNames();
    ASSERT_EQ(first.size(), 1U);
    Snapshot(db);  // the base: p = 1
    floor = Mem(db)->RegisterBranchForkPin() - 1;

    gids = MoveMainOn(db, gids);
    SnapshotPass(db, gids, 100);
    auto const names = SnapshotNames();
    EXPECT_EQ(names.size(), 2U);
    EXPECT_EQ(std::ranges::count(names, first.front()), 0) << "the older snapshot is retired by the count";
  }

  Db db{MakeConfig(/*recover_on_startup=*/true, /*retention_count=*/2), Bound(floor)};
  ExpectBaseState(db, gids);
}

// A snapshot taken at or below the floor is itself the recovery base: no older snapshot is kept as an extra.
TEST_F(RetentionFloorTest, SnapshotBelowTheFloorIsTheBase) {
  Gids gids{};
  uint64_t floor = 0;
  {
    Db db{MakeConfig(/*recover_on_startup=*/false, /*retention_count=*/1)};
    gids = CreateBase(db, /*snapshot_below_floor=*/true);
    floor = Mem(db)->RegisterBranchForkPin() - 1;
    Snapshot(db);
    EXPECT_EQ(CountFiles("snapshots"), 1U) << "the new snapshot is at the floor, so it replaces the older one";
  }

  Db db{MakeConfig(/*recover_on_startup=*/true, /*retention_count=*/1), Bound(floor)};
  ExpectBaseState(db, gids);
}

// Raising the floor (to a later point, then to none) lets the next pass delete the base kept past the count.
TEST_F(RetentionFloorTest, RaisedFloorReleasesTheExtraBase) {
  Db db{MakeConfig(/*recover_on_startup=*/false, /*retention_count=*/1)};
  auto gids = CreateBase(db, /*snapshot_below_floor=*/true);
  auto const base = SnapshotNames().front();
  Mem(db)->RegisterBranchForkPin();
  gids = MoveMainOn(db, gids);

  SnapshotPass(db, gids, 100);
  SnapshotPass(db, gids, 200);
  auto names = SnapshotNames();
  ASSERT_EQ(names.size(), 2U);
  EXPECT_EQ(names.front(), base) << "the floor keeps the base";

  Mem(db)->SetRetentionFloor(Mem(db)->RegisterForkPin() - 1);
  auto const newer_base = names.back();
  SnapshotPass(db, gids, 300);
  names = SnapshotNames();
  ASSERT_EQ(names.size(), 2U);
  EXPECT_EQ(names.front(), newer_base) << "the raised floor's base replaces the old one";

  Mem(db)->SetRetentionFloor(std::nullopt);
  SnapshotPass(db, gids, 400);
  EXPECT_EQ(SnapshotNames().size(), 1U);
}

// Clearing the floor lets the next pass prune the WAL chain it was keeping from the first file.
TEST_F(RetentionFloorTest, ClearedFloorPrunesTheWalChain) {
  Db db{MakeConfig(/*recover_on_startup=*/false, /*retention_count=*/1)};
  auto gids = CreateBase(db, /*snapshot_below_floor=*/false);
  Mem(db)->RegisterBranchForkPin();
  gids = MoveMainOn(db, gids);
  SnapshotPass(db, gids, 100);
  ASSERT_TRUE(HasFirstWalFile()) << "the floor keeps the chain";

  Mem(db)->SetRetentionFloor(std::nullopt);
  SnapshotPass(db, gids, 200);
  EXPECT_FALSE(HasFirstWalFile());
  EXPECT_EQ(CountFiles("snapshots"), 1U);
}

// Both HistoryBounds fields seed the floor of a storage start: a configured floor keeps its base through the passes of
// that run, and a recovery bound keeps the floor it recovered at.
TEST_F(RetentionFloorTest, HistoryBoundsSeedTheFloor) {
  Gids gids{};
  uint64_t floor = 0;
  {
    Db db{MakeConfig(/*recover_on_startup=*/false, /*retention_count=*/1)};
    gids = CreateBase(db, /*snapshot_below_floor=*/true);
    floor = Mem(db)->RegisterForkPin() - 1;
    gids = MoveMainOn(db, gids);
  }
  {
    Db db{MakeConfig(/*recover_on_startup=*/true, /*retention_count=*/1), Floor(floor)};
    EXPECT_EQ(Mem(db)->RetentionFloor(), floor);
    SnapshotPass(db, gids, 100);
    SnapshotPass(db, gids, 200);
    EXPECT_EQ(CountFiles("snapshots"), 2U) << "the newest snapshot and the base below the floor";
  }

  Db db{MakeConfig(/*recover_on_startup=*/true, /*retention_count=*/1), Bound(floor)};
  EXPECT_EQ(Mem(db)->RetentionFloor(), floor);
  ExpectBaseState(db, gids);
}

TEST(RetentionFloorDeathTest, BoundAndFloorAreMutuallyExclusive) {
  std::filesystem::remove_all(kStorageDirectory);
  EXPECT_DEATH((Db{MakeConfig(/*recover_on_startup=*/false, /*retention=*/1),
                   md::HistoryBounds{.recovery_bound = 5, .retention_floor = 5}}),
               "mutually exclusive");
  std::filesystem::remove_all(kStorageDirectory);
}

}  // namespace
