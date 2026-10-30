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

// Recovery bounded by a durable timestamp: the recovered state is the one as of the bound, and the WAL files that
// hold the remaining transactions are reported back.

#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include <algorithm>
#include <atomic>
#include <filesystem>
#include <fstream>
#include <initializer_list>
#include <iterator>
#include <memory>
#include <optional>
#include <string>
#include <string_view>
#include <vector>

#include "dbms/database.hpp"
#include "flags/general.hpp"
#include "memory/db_arena.hpp"
#include "storage/v2/constraints/constraints.hpp"
#include "storage/v2/durability/durability.hpp"
#include "storage/v2/durability/paths.hpp"
#include "storage/v2/indices/indices.hpp"
#include "storage/v2/inmemory/storage.hpp"
#include "storage/v2/property_value.hpp"
#include "storage/v2/replication/replication_storage_state.hpp"
#include "tests/test_commit_args_helper.hpp"
#include "tests/versioning_startup_refusal.hpp"
#include "utils/scheduler.hpp"
#include "utils/uuid.hpp"
#include "versioning/branch_ledger.hpp"
#include "versioning/version_store.hpp"

namespace ms = memgraph::storage;
namespace md = memgraph::storage::durability;

namespace {

const std::filesystem::path kRoot{std::filesystem::temp_directory_path() / "versioning_bounded_recovery"};

ms::Config MakeConfig(bool recover, bool versioning = true) {
  ms::Config config;
  config.experimental_versioning = versioning;
  config.gc = {.type = ms::Config::Gc::Type::PERIODIC, .interval = std::chrono::hours(24)};
  config.durability = {.storage_directory = kRoot,
                       .recover_on_startup = recover,
                       .snapshot_wal_mode = ms::Config::Durability::SnapshotWalMode::PERIODIC_SNAPSHOT_WITH_WAL,
                       .snapshot_interval = memgraph::utils::SchedulerInterval{std::chrono::hours(24)},
                       .wal_file_flush_every_n_tx = 1};
  config.salient.items.properties_on_edges = true;
  return config;
}

uint64_t LastDurableTs() {
  auto const wal_files = md::GetWalFiles(kRoot / md::kWalDirectory);
  uint64_t ts = 0;
  if (wal_files) {
    for (auto const &info : *wal_files) ts = std::max(ts, info.to_timestamp);
  }
  return ts;
}

void Commit(ms::Storage::Accessor &acc) {
  ASSERT_TRUE(acc.PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()).has_value());
}

// Bare containers RecoverData is driven into.
struct Recovered {
  explicit Recovered(ms::Config const &config)
      : config{config},
        indices{config, ms::StorageMode::IN_MEMORY_TRANSACTIONAL},
        constraints{config, ms::StorageMode::IN_MEMORY_TRANSACTIONAL, {}},
        ttl{nullptr, {}, {}} {}

  std::optional<md::RecoveryInfo> Recover(std::optional<uint64_t> bound, md::RecoveryTail *tail) {
    md::Recovery recovery{kRoot / md::kSnapshotDirectory, kRoot / md::kWalDirectory};
    return recovery.RecoverData(
        uuid,
        repl_storage_state,
        &vertices,
        &edges,
        nullptr /* edges_metadata */,
        &edge_count,
        &name_id_mapper,
        &indices,
        &constraints,
        config,
        nullptr /* db_arena_pool */,
        &wal_seq_num,
        &enum_store,
        nullptr /* schema_info */,
        [](auto) { return std::nullopt; },
        "memgraph",
        &ttl,
        nullptr /* description_store */,
        bound,
        tail);
  }

  std::optional<ms::PropertyValue> Property(ms::Gid gid, std::string_view name) {
    auto acc = vertices.access();
    auto it = acc.find(gid);
    if (it == acc.end()) return std::nullopt;
    return it->properties.GetProperty(ms::PropertyId::FromUint(name_id_mapper.NameToId(name)));
  }

  size_t VertexCount() { return vertices.access().size(); }

  ms::Config config;
  memgraph::utils::SkipListDb<ms::Vertex> vertices;
  memgraph::utils::SkipListDb<ms::Edge> edges;
  std::atomic<uint64_t> edge_count{0};
  ms::NameIdMapper name_id_mapper;
  uint64_t wal_seq_num{0};
  memgraph::utils::UUID uuid;
  ms::Indices indices;
  ms::Constraints constraints;
  ms::ReplicationStorageState repl_storage_state;
  ms::EnumStore enum_store;
  ms::ttl::TTL ttl;
};

}  // namespace

class VersioningBoundedRecoveryTest : public ::testing::Test {
 protected:
  void SetUp() override {
    FLAGS_metrics_format = "OpenMetrics";
    std::filesystem::remove_all(kRoot);
  }

  void TearDown() override { std::filesystem::remove_all(kRoot); }
};

TEST_F(VersioningBoundedRecoveryTest, RecoversStateAsOfBoundAndReportsTail) {
  ms::Gid v1{}, v2{}, v3{};
  uint64_t ts_c1{}, ts_c2{}, ts_c3{}, ts_c4{}, ts_c5{}, ts_c6{};
  {
    memgraph::dbms::Database db{MakeConfig(false)};
    const memgraph::memory::DbArenaScope arena_scope{&db.Arena()};
    auto *s = static_cast<ms::InMemoryStorage *>(db.storage());
    auto const l = s->NameToLabel("L");
    auto const p = s->NameToProperty("p");

    {  // c1: v1.p = 1, v2.p = 2
      auto acc = db.Access(ms::WRITE);
      auto a = acc->CreateVertex();
      auto b = acc->CreateVertex();
      ASSERT_TRUE(a.SetProperty(p, ms::PropertyValue(1)).has_value());
      ASSERT_TRUE(b.SetProperty(p, ms::PropertyValue(2)).has_value());
      v1 = a.Gid();
      v2 = b.Gid();
      Commit(*acc);
      ts_c1 = LastDurableTs();
    }
    {  // c2: v1.p = 10
      auto acc = db.Access(ms::WRITE);
      auto a = acc->FindVertex(v1, ms::View::OLD);
      ASSERT_TRUE(a->SetProperty(p, ms::PropertyValue(10)).has_value());
      Commit(*acc);
      ts_c2 = LastDurableTs();
    }

    ASSERT_TRUE(s->CreateSnapshot().has_value());

    {  // c3: v1.p = 100
      auto acc = db.Access(ms::WRITE);
      auto a = acc->FindVertex(v1, ms::View::OLD);
      ASSERT_TRUE(a->SetProperty(p, ms::PropertyValue(100)).has_value());
      Commit(*acc);
      ts_c3 = LastDurableTs();
    }
    {  // c4: label index on :L
      auto acc = db.ReadOnlyAccess();
      ASSERT_TRUE(acc->CreateIndex(l).has_value());
      Commit(*acc);
      ts_c4 = LastDurableTs();
    }
    {  // c5: v3.p = 3
      auto acc = db.Access(ms::WRITE);
      auto c = acc->CreateVertex();
      ASSERT_TRUE(c.SetProperty(p, ms::PropertyValue(3)).has_value());
      v3 = c.Gid();
      Commit(*acc);
      ts_c5 = LastDurableTs();
    }
    {  // c6: v2.p = 20
      auto acc = db.Access(ms::WRITE);
      auto b = acc->FindVertex(v2, ms::View::OLD);
      ASSERT_TRUE(b->SetProperty(p, ms::PropertyValue(20)).has_value());
      Commit(*acc);
      ts_c6 = LastDurableTs();
    }
  }

  ASSERT_LT(ts_c1, ts_c2);
  ASSERT_LT(ts_c2, ts_c3);
  ASSERT_LT(ts_c3, ts_c4);
  ASSERT_LT(ts_c4, ts_c5);
  ASSERT_LT(ts_c5, ts_c6);

  auto const snapshots = md::GetSnapshotFiles(kRoot / md::kSnapshotDirectory);
  ASSERT_TRUE(snapshots.has_value());
  ASSERT_EQ(snapshots->size(), 1U);
  auto const snapshot_ts = snapshots->front().durable_timestamp;
  ASSERT_GE(snapshot_ts, ts_c2);
  ASSERT_LT(snapshot_ts, ts_c3);

  auto const wal_files = md::GetWalFiles(kRoot / md::kWalDirectory);
  ASSERT_TRUE(wal_files.has_value());
  ASSERT_FALSE(wal_files->empty());
  ASSERT_EQ(wal_files->front().seq_num, 0U);

  auto const recover = [&](std::optional<uint64_t> bound) {
    auto r = std::make_unique<Recovered>(MakeConfig(true));
    md::RecoveryTail tail;
    auto const info = r->Recover(bound, &tail);
    EXPECT_TRUE(info.has_value());
    return std::pair{std::move(r), std::move(tail)};
  };

  {  // Between the snapshot and the end: snapshot (c1, c2) plus c3; the index and everything later are left out.
    auto [r, tail] = recover(ts_c3);
    EXPECT_EQ(r->VertexCount(), 2U);
    EXPECT_EQ(r->Property(v1, "p"), ms::PropertyValue(100));
    EXPECT_EQ(r->Property(v2, "p"), ms::PropertyValue(2));
    EXPECT_FALSE(r->Property(v3, "p").has_value());
    EXPECT_FALSE(r->indices.label_index_->GetActiveIndices()->IndexExists(
        ms::LabelId::FromUint(r->name_id_mapper.NameToId("L"))));
    EXPECT_EQ(tail.reached_timestamp, ts_c3);
    ASSERT_FALSE(tail.wal_files.empty());
    EXPECT_EQ(tail.wal_files.back(), wal_files->back().path);
  }
  {  // The index definition is at the bound.
    auto [r, tail] = recover(ts_c4);
    EXPECT_EQ(r->VertexCount(), 2U);
    EXPECT_TRUE(r->indices.label_index_->GetActiveIndices()->IndexExists(
        ms::LabelId::FromUint(r->name_id_mapper.NameToId("L"))));
    EXPECT_EQ(tail.reached_timestamp, ts_c4);
    EXPECT_EQ(tail.wal_files.back(), wal_files->back().path);
  }
  {  // Before the snapshot: it is not usable, the WAL chain from sequence 0 is.
    auto [r, tail] = recover(ts_c1);
    EXPECT_EQ(r->VertexCount(), 2U);
    EXPECT_EQ(r->Property(v1, "p"), ms::PropertyValue(1));
    EXPECT_EQ(r->Property(v2, "p"), ms::PropertyValue(2));
    EXPECT_EQ(tail.reached_timestamp, ts_c1);
    ASSERT_FALSE(tail.wal_files.empty());
    EXPECT_EQ(tail.wal_files.back(), wal_files->back().path);
  }
  {  // At the end: nothing is left to replay.
    auto [r, tail] = recover(ts_c6);
    EXPECT_EQ(r->VertexCount(), 3U);
    EXPECT_EQ(r->Property(v2, "p"), ms::PropertyValue(20));
    EXPECT_EQ(r->Property(v3, "p"), ms::PropertyValue(3));
    EXPECT_EQ(tail.reached_timestamp, ts_c6);
    EXPECT_TRUE(tail.wal_files.empty());
  }
  {  // No bound: the same as before the change, and no tail is reported.
    auto r = std::make_unique<Recovered>(MakeConfig(true));
    md::RecoveryTail tail;
    ASSERT_TRUE(r->Recover(std::nullopt, &tail).has_value());
    EXPECT_EQ(r->VertexCount(), 3U);
    EXPECT_EQ(r->Property(v1, "p"), ms::PropertyValue(100));
    EXPECT_TRUE(tail.wal_files.empty());
  }
}

// Creates a branch, then expects a restart under a condition that forbids branches to be refused with every file left
// as it was, and the branch to be back once the previous settings are used again.
void ExpectRestartRefused(ms::Config restart_config, std::initializer_list<std::string_view> fragments) {
  {
    memgraph::dbms::Database db{MakeConfig(false)};
    const memgraph::memory::DbArenaScope arena_scope{&db.Arena()};
    for (int i = 0; i < 2; ++i) {
      auto acc = db.Access(ms::WRITE);
      acc->CreateVertex();
      Commit(*acc);
    }
    ASSERT_TRUE(db.version_store()->CreateBranch("b", "main").has_value());
    {
      auto acc = db.Access(ms::WRITE);
      acc->CreateVertex();
      Commit(*acc);
    }
  }

  memgraph::tests::ExpectStartupRefused(kRoot, std::move(restart_config), fragments);

  memgraph::dbms::Database db{MakeConfig(true)};
  ASSERT_NE(db.version_store(), nullptr);
  EXPECT_TRUE(db.version_store()->Exists("b"));
}

TEST_F(VersioningBoundedRecoveryTest, AnAnalyticalStartupIsRefused) {
  auto config = MakeConfig(true);
  config.salient.storage_mode = ms::StorageMode::IN_MEMORY_ANALYTICAL;
  ExpectRestartRefused(std::move(config), {"Database '", "[b]", "IN_MEMORY_ANALYTICAL", "IN_MEMORY_TRANSACTIONAL"});
}

TEST_F(VersioningBoundedRecoveryTest, AnOnDiskStartupIsRefused) {
  auto config = MakeConfig(true);
  config.salient.storage_mode = ms::StorageMode::ON_DISK_TRANSACTIONAL;
  ExpectRestartRefused(std::move(config), {"[b]", "ON_DISK_TRANSACTIONAL"});
}

TEST_F(VersioningBoundedRecoveryTest, StartupWithCommitLockNarrowingIsRefused) {
  auto config = MakeConfig(true);
  config.experimental_commit_lock_narrowing = true;
  ExpectRestartRefused(std::move(config), {"[b]", "commit-lock-narrowing", "DROP BRANCH"});
}

std::string ReadWholeFile(const std::filesystem::path &file) {
  std::ifstream in{file, std::ios::binary};
  return {std::istreambuf_iterator<char>{in}, std::istreambuf_iterator<char>{}};
}

// Versioning off hides the branches instead of dropping them: main recovers unbounded with every commit, and the
// ledger and the branch directory stay exactly as they were.
TEST_F(VersioningBoundedRecoveryTest, StartupWithVersioningDisabledHidesTheBranches) {
  {
    memgraph::dbms::Database db{MakeConfig(false)};
    const memgraph::memory::DbArenaScope arena_scope{&db.Arena()};
    for (int i = 0; i < 2; ++i) {
      auto acc = db.Access(ms::WRITE);
      acc->CreateVertex();
      Commit(*acc);
    }
    ASSERT_TRUE(db.version_store()->CreateBranch("b", "main").has_value());
    auto acc = db.Access(ms::WRITE);
    acc->CreateVertex();
    Commit(*acc);
  }

  auto const ledger_file = kRoot / "branches" / "ledger";
  auto const ledger_before = ReadWholeFile(ledger_file);
  ASSERT_FALSE(ledger_before.empty());
  auto const read = memgraph::versioning::BranchLedger{ledger_file}.Read();
  ASSERT_TRUE(read.has_value() && read->has_value());
  ASSERT_EQ((*read)->branches.size(), 1U);
  auto const branch_directory = kRoot / "branches" / std::to_string((*read)->branches[0].info.number);
  // The branch committed nothing, so it has no files yet; stand in for its WAL.
  std::filesystem::create_directories(branch_directory / "wal");
  {
    std::ofstream{branch_directory / "wal" / "marker"} << "x";
  }

  {
    memgraph::dbms::Database db{MakeConfig(true, /*versioning=*/false)};
    const memgraph::memory::DbArenaScope arena_scope{&db.Arena()};
    EXPECT_EQ(db.Access(ms::READ)->ApproximateVertexCount(), 3U);
    auto *store = db.version_store();
    ASSERT_NE(store, nullptr);
    EXPECT_TRUE(store->List().empty());
    EXPECT_FALSE(store->Exists("b"));
    EXPECT_FALSE(store->Empty());
    EXPECT_FALSE(store->CreateBranch("b", "main").has_value()) << "a hidden branch still owns its name";
  }

  EXPECT_EQ(ReadWholeFile(ledger_file), ledger_before);
  EXPECT_TRUE(std::filesystem::exists(branch_directory / "wal" / "marker"));
}

// Branches that could not be rebuilt later are not hidden: startup is refused even while versioning is off.
TEST_F(VersioningBoundedRecoveryTest, StartupWithVersioningDisabledAndNoRecoveryIsRefused) {
  ExpectRestartRefused(MakeConfig(false, /*versioning=*/false),
                       {"[b]", "recovery on startup", "--data-recovery-on-startup=true"});
}

TEST_F(VersioningBoundedRecoveryTest, StartupWithVersioningDisabledAndNoWalIsRefused) {
  auto config = MakeConfig(true, /*versioning=*/false);
  config.durability.snapshot_wal_mode = ms::Config::Durability::SnapshotWalMode::DISABLED;
  ExpectRestartRefused(std::move(config), {"[b]", "write-ahead log", "--storage-wal-enabled=true"});
}
