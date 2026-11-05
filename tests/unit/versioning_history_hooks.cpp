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

// Storage hooks for rebuilding history at startup: bounded recovery through the configuration, committing without
// appending to the WAL, and moving the transaction clock past the last durable timestamp.

#include <gtest/gtest.h>

#include <algorithm>
#include <chrono>
#include <filesystem>
#include <optional>
#include <vector>

#include "dbms/database.hpp"
#include "flags/general.hpp"
#include "memory/db_arena.hpp"
#include "storage/v2/commit_args.hpp"
#include "storage/v2/durability/durability.hpp"
#include "storage/v2/durability/paths.hpp"
#include "storage/v2/inmemory/storage.hpp"
#include "storage/v2/property_value.hpp"
#include "tests/test_commit_args_helper.hpp"
#include "utils/scheduler.hpp"

namespace ms = memgraph::storage;
namespace md = memgraph::storage::durability;

namespace {

const std::filesystem::path kRoot{std::filesystem::temp_directory_path() / "versioning_history_hooks"};

ms::Config MakeConfig(bool recover, std::optional<uint64_t> bound = std::nullopt) {
  ms::Config config;
  config.gc = {.type = ms::Config::Gc::Type::PERIODIC, .interval = std::chrono::hours(24)};
  config.durability = {.storage_directory = kRoot,
                       .recover_on_startup = recover,
                       .snapshot_wal_mode = ms::Config::Durability::SnapshotWalMode::PERIODIC_SNAPSHOT_WITH_WAL,
                       .snapshot_interval = memgraph::utils::SchedulerInterval{std::chrono::hours(24)},
                       .wal_file_flush_every_n_tx = 1,
                       .recovery_history_bound = bound};
  config.salient.items.properties_on_edges = true;
  return config;
}

std::vector<md::WalDurabilityInfo> WalFiles() {
  auto files = md::GetWalFiles(kRoot / md::kWalDirectory);
  return files ? *std::move(files) : std::vector<md::WalDurabilityInfo>{};
}

uint64_t LastDurableTs() {
  uint64_t ts = 0;
  for (auto const &info : WalFiles()) ts = std::max(ts, info.to_timestamp);
  return ts;
}

void Commit(ms::Storage::Accessor &acc) {
  ASSERT_TRUE(acc.PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()).has_value());
}

void CommitAsReplicaWrite(ms::Storage::Accessor &acc, uint64_t desired_ts) {
  ASSERT_TRUE(
      acc.PrepareForCommitPhase(ms::CommitArgs::make_replica_write(desired_ts, /*two_phase_commit=*/false, [] {}))
          .has_value());
}

}  // namespace

class VersioningHistoryHooksTest : public ::testing::Test {
 protected:
  void SetUp() override {
    FLAGS_metrics_format = "OpenMetrics";
    std::filesystem::remove_all(kRoot);
  }

  void TearDown() override { std::filesystem::remove_all(kRoot); }

  // v1.p = 1 (c1), v1.p = 2 (c2), v1.p = 3 (c3), each in its own transaction and WAL transaction.
  void WriteHistory() {
    memgraph::dbms::Database db{MakeConfig(false)};
    const memgraph::memory::DbArenaScope arena_scope{&db.Arena()};
    auto *s = static_cast<ms::InMemoryStorage *>(db.storage());
    auto const p = s->NameToProperty("p");
    {
      auto acc = db.Access(ms::WRITE);
      auto v = acc->CreateVertex();
      ASSERT_TRUE(v.SetProperty(p, ms::PropertyValue(1)).has_value());
      v1 = v.Gid();
      Commit(*acc);
      ts_c1 = LastDurableTs();
    }
    for (int value = 2; value <= 3; ++value) {
      auto acc = db.Access(ms::WRITE);
      auto v = acc->FindVertex(v1, ms::View::OLD);
      ASSERT_TRUE(v->SetProperty(p, ms::PropertyValue(value)).has_value());
      Commit(*acc);
      (value == 2 ? ts_c2 : ts_c3) = LastDurableTs();
    }
    ASSERT_LT(ts_c1, ts_c2);
    ASSERT_LT(ts_c2, ts_c3);
  }

  ms::PropertyValue ReadP(memgraph::dbms::Database &db) {
    auto *s = static_cast<ms::InMemoryStorage *>(db.storage());
    auto acc = db.Access(ms::READ);
    auto v = acc->FindVertex(v1, ms::View::OLD);
    EXPECT_TRUE(v.has_value());
    return *v->GetProperty(s->NameToProperty("p"), ms::View::OLD);
  }

  ms::Gid v1{};
  uint64_t ts_c1{}, ts_c2{}, ts_c3{};
};

TEST_F(VersioningHistoryHooksTest, BoundedConfigRecoversStateAtBoundAndReturnsTail) {
  WriteHistory();
  auto const wal_files = WalFiles();
  ASSERT_FALSE(wal_files.empty());

  {
    memgraph::dbms::Database db{MakeConfig(true, ts_c2)};
    const memgraph::memory::DbArenaScope arena_scope{&db.Arena()};
    auto *s = static_cast<ms::InMemoryStorage *>(db.storage());
    EXPECT_EQ(ReadP(db), ms::PropertyValue(2));

    auto tail = s->TakeRecoveryTail();
    ASSERT_TRUE(tail.has_value());
    EXPECT_EQ(tail->reached_timestamp, ts_c2);
    ASSERT_FALSE(tail->wal_files.empty());
    EXPECT_EQ(tail->wal_files.back(), wal_files.back().path);
    EXPECT_FALSE(s->TakeRecoveryTail().has_value()) << "the tail is moved out";
  }
  {
    memgraph::dbms::Database db{MakeConfig(true)};
    const memgraph::memory::DbArenaScope arena_scope{&db.Arena()};
    auto *s = static_cast<ms::InMemoryStorage *>(db.storage());
    EXPECT_EQ(ReadP(db), ms::PropertyValue(3));
    EXPECT_FALSE(s->TakeRecoveryTail().has_value()) << "no bound configured, no tail";
  }
}

TEST_F(VersioningHistoryHooksTest, SuppressedCommitsDoNotTouchTheWal) {
  WriteHistory();

  memgraph::dbms::Database db{MakeConfig(true, ts_c1)};
  const memgraph::memory::DbArenaScope arena_scope{&db.Arena()};
  auto *s = static_cast<ms::InMemoryStorage *>(db.storage());
  auto const p = s->NameToProperty("p");
  ASSERT_TRUE(s->TakeRecoveryTail().has_value());

  auto const before = WalFiles();
  {
    ms::InMemoryStorage::SuppressWalScope const suppress{*s};
    // Replays the tail (c2 and c3 folded together) as a replica-style commit carrying main's durable timestamp.
    auto acc = db.Access(ms::WRITE);
    auto v = acc->FindVertex(v1, ms::View::OLD);
    ASSERT_TRUE(v->SetProperty(p, ms::PropertyValue(3)).has_value());
    CommitAsReplicaWrite(*acc, ts_c3);
  }
  auto const after = WalFiles();
  ASSERT_EQ(after.size(), before.size());
  for (size_t i = 0; i < before.size(); ++i) {
    EXPECT_EQ(after[i].path, before[i].path);
    EXPECT_EQ(after[i].to_timestamp, before[i].to_timestamp);
  }
  EXPECT_EQ(ReadP(db), ms::PropertyValue(3));

  // Once the scope is gone commits append again.
  s->BumpClockPastDurable();
  {
    auto acc = db.Access(ms::WRITE);
    auto v = acc->FindVertex(v1, ms::View::OLD);
    ASSERT_TRUE(v->SetProperty(p, ms::PropertyValue(7)).has_value());
    Commit(*acc);
  }
  EXPECT_EQ(WalFiles().size(), before.size() + 1);
  EXPECT_EQ(LastDurableTs(), ts_c3 + 2);  // start ts takes ts_c3 + 1, commit ts_c3 + 2
}

TEST_F(VersioningHistoryHooksTest, ClockBumpLetsLocalCommitsAndGcProceed) {
  WriteHistory();

  memgraph::dbms::Database db{MakeConfig(true, ts_c1)};
  const memgraph::memory::DbArenaScope arena_scope{&db.Arena()};
  auto *s = static_cast<ms::InMemoryStorage *>(db.storage());
  auto const p = s->NameToProperty("p");

  constexpr uint64_t kFarAheadTs = 1'000'000;
  {
    ms::InMemoryStorage::SuppressWalScope const suppress{*s};
    auto acc = db.Access(ms::WRITE);
    auto v = acc->FindVertex(v1, ms::View::OLD);
    ASSERT_TRUE(v->SetProperty(p, ms::PropertyValue(2)).has_value());
    CommitAsReplicaWrite(*acc, kFarAheadTs);
  }
  ASSERT_LT(s->OldestActiveTimestamp(), kFarAheadTs);

  s->BumpClockPastDurable();
  EXPECT_GT(s->OldestActiveTimestamp(), kFarAheadTs) << "skipped ids must be marked finished";
  s->BumpClockPastDurable();  // idempotent once ahead

  {
    auto acc = db.Access(ms::WRITE);
    auto v = acc->FindVertex(v1, ms::View::OLD);
    ASSERT_TRUE(v->SetProperty(p, ms::PropertyValue(9)).has_value());
    Commit(*acc);
  }
  EXPECT_EQ(LastDurableTs(), kFarAheadTs + 2);  // start ts takes +1, commit +2
  EXPECT_EQ(ReadP(db), ms::PropertyValue(9));
  s->FreeMemory({}, false);
  EXPECT_GT(s->OldestActiveTimestamp(), kFarAheadTs);
}
