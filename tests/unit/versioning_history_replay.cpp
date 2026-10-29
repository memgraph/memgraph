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

// History at a fork pin survives WAL-tail replay (ReplayWalFile) and GC; releasing the pin reclaims it.

#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include <algorithm>
#include <chrono>
#include <filesystem>
#include <optional>
#include <string>
#include <vector>

#include "dbms/database.hpp"
#include "dbms/inmemory/replication_handlers.hpp"
#include "flags/general.hpp"
#include "memory/db_arena.hpp"
#include "rpc/progress_heartbeat.hpp"
#include "storage/v2/durability/durability.hpp"
#include "storage/v2/durability/paths.hpp"
#include "storage/v2/indices/index_order.hpp"
#include "storage/v2/indices/property_path.hpp"
#include "storage/v2/inmemory/storage.hpp"
#include "storage/v2/property_value.hpp"
#include "storage/v2/view.hpp"
#include "storage_test_utils.hpp"
#include "tests/test_commit_args_helper.hpp"
#include "utils/scheduler.hpp"
#include "utils/uuid.hpp"
#include "versioning/version_store.hpp"

namespace ms = memgraph::storage;

namespace {

const std::filesystem::path kRoot{std::filesystem::temp_directory_path() / "versioning_history_replay"};
const std::filesystem::path kDirA{kRoot / "a"};
const std::filesystem::path kDirB{kRoot / "b"};

ms::Config MakeConfigA() {
  ms::Config config;
  config.gc = {.type = ms::Config::Gc::Type::PERIODIC, .interval = std::chrono::hours(24)};
  config.durability = {.storage_directory = kDirA,
                       .recover_on_startup = false,
                       .snapshot_wal_mode = ms::Config::Durability::SnapshotWalMode::PERIODIC_SNAPSHOT_WITH_WAL,
                       .snapshot_interval = memgraph::utils::SchedulerInterval{std::chrono::hours(24)},
                       .wal_file_flush_every_n_tx = 1};
  config.salient.items.properties_on_edges = true;
  return config;
}

ms::Config MakeConfigB() {
  ms::Config config;
  // Manual GC only: the test drives FreeMemory() itself.
  config.gc = {.type = ms::Config::Gc::Type::PERIODIC, .interval = std::chrono::hours(24)};
  config.durability = {.storage_directory = kDirB,
                       .recover_on_startup = true,
                       .snapshot_wal_mode = ms::Config::Durability::SnapshotWalMode::DISABLED,
                       .snapshot_interval = memgraph::utils::SchedulerInterval{std::chrono::hours(24)}};
  config.salient.items.properties_on_edges = true;
  return config;
}

const std::filesystem::path kDirMain{kRoot / "main"};
const std::filesystem::path kDirUnbounded{kRoot / "unbounded"};

ms::Config MakeDurableConfig(const std::filesystem::path &dir, bool recover_on_startup) {
  ms::Config config;
  config.gc = {.type = ms::Config::Gc::Type::PERIODIC, .interval = std::chrono::hours(24)};
  config.durability = {.storage_directory = dir,
                       .recover_on_startup = recover_on_startup,
                       .snapshot_wal_mode = ms::Config::Durability::SnapshotWalMode::PERIODIC_SNAPSHOT_WITH_WAL,
                       .snapshot_interval = memgraph::utils::SchedulerInterval{std::chrono::hours(24)},
                       .wal_file_flush_every_n_tx = 1};
  return config;
}

struct EpochState {
  std::string epoch;
  ms::EpochHistory history;
};

EpochState CaptureEpochState(memgraph::dbms::Database &db) {
  auto *storage = static_cast<ms::InMemoryStorage *>(db.storage());
  return {std::string(storage->repl_storage_state_.epoch_.id()), storage->repl_storage_state_.history};
}

struct BaseGids {
  ms::Gid v1, v2, v3, e1, e2;
};

struct TailGids {
  ms::Gid v4, e3;
};

void Commit(ms::Storage::Accessor &acc) {
  ASSERT_TRUE(acc.PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()).has_value());
}

void CommitVertex(memgraph::dbms::Database &db) {
  auto acc = db.Access(ms::WRITE);
  acc->CreateVertex();
  Commit(*acc);
}

// Same MVCC checks for both reads, parameterised on which of the two states is expected.
struct Names {
  ms::LabelId l, m;
  ms::PropertyId p, w;
};

Names ResolveNames(ms::Storage &storage) {
  return {storage.NameToLabel("L"), storage.NameToLabel("M"), storage.NameToProperty("p"), storage.NameToProperty("w")};
}

void ExpectStateAtT(ms::Storage::Accessor &acc, Names const &n, BaseGids const &g, TailGids const &t) {
  constexpr auto kView = ms::View::OLD;
  EXPECT_EQ(CountVertices(acc, kView), 3U);

  auto v1 = acc.FindVertex(g.v1, kView);
  ASSERT_TRUE(v1.has_value());
  EXPECT_EQ(*v1->GetProperty(n.p, kView), ms::PropertyValue(1));
  EXPECT_TRUE(*v1->HasLabel(n.l, kView));

  auto v2 = acc.FindVertex(g.v2, kView);
  ASSERT_TRUE(v2.has_value());
  EXPECT_TRUE(*v2->HasLabel(n.l, kView));
  EXPECT_FALSE(*v2->HasLabel(n.m, kView));
  EXPECT_EQ(*v2->GetProperty(n.p, kView), ms::PropertyValue(2));

  EXPECT_TRUE(acc.FindVertex(g.v3, kView).has_value());
  // FindEdge(gid) resolves through current adjacency (FindHeavyEdge), not MVCC, so check edges by traversal at T.
  auto const out_edge = [&](ms::VertexAccessor &from, ms::Gid gid) -> std::optional<ms::EdgeAccessor> {
    auto out = from.OutEdges(kView);
    if (!out.has_value()) return std::nullopt;
    for (auto e : out->edges) {
      if (e.Gid() == gid) return e;
    }
    return std::nullopt;
  };
  auto e1 = out_edge(*v1, g.e1);
  ASSERT_TRUE(e1.has_value());
  EXPECT_EQ(*e1->GetProperty(n.w, kView), ms::PropertyValue(1));
  EXPECT_TRUE(out_edge(*v2, g.e2).has_value());

  EXPECT_FALSE(acc.FindVertex(t.v4, kView).has_value());
  EXPECT_FALSE(acc.FindEdge(t.e3, kView).has_value());
}

void ExpectFullState(ms::Storage::Accessor &acc, Names const &n, BaseGids const &g, TailGids const &t) {
  constexpr auto kView = ms::View::OLD;
  EXPECT_EQ(CountVertices(acc, kView), 3U);  // v1, v2, v4

  auto v1 = acc.FindVertex(g.v1, kView);
  ASSERT_TRUE(v1.has_value());
  EXPECT_EQ(*v1->GetProperty(n.p, kView), ms::PropertyValue(10));

  auto v2 = acc.FindVertex(g.v2, kView);
  ASSERT_TRUE(v2.has_value());
  EXPECT_FALSE(*v2->HasLabel(n.l, kView));
  EXPECT_TRUE(*v2->HasLabel(n.m, kView));

  EXPECT_FALSE(acc.FindVertex(g.v3, kView).has_value());
  EXPECT_FALSE(acc.FindEdge(g.e1, kView).has_value());
  EXPECT_FALSE(acc.FindEdge(g.e2, kView).has_value());

  auto v4 = acc.FindVertex(t.v4, kView);
  ASSERT_TRUE(v4.has_value());
  EXPECT_EQ(*v4->GetProperty(n.p, kView), ms::PropertyValue(40));
  EXPECT_TRUE(acc.FindEdge(t.e3, kView).has_value());
}

}  // namespace

class VersioningHistoryReplayTest : public ::testing::Test {
 protected:
  void SetUp() override {
    FLAGS_metrics_format = "OpenMetrics";
    FLAGS_versioning_enabled = true;
    std::filesystem::remove_all(kRoot);
  }

  void TearDown() override {
    FLAGS_versioning_enabled = false;
    std::filesystem::remove_all(kRoot);
  }
};

TEST_F(VersioningHistoryReplayTest, HistoryAtForkPinSurvivesWalTailReplayAndGc) {
  BaseGids g{};
  TailGids t{};

  // ---- Step 1: storage A. Base state, snapshot at T, tail transactions, shutdown without snapshot. ----
  {
    memgraph::dbms::Database db_a{MakeConfigA()};
    const memgraph::memory::DbArenaScope arena_scope{&db_a.Arena()};
    auto *a = static_cast<ms::InMemoryStorage *>(db_a.storage());
    auto const l = a->NameToLabel("L");
    auto const m = a->NameToLabel("M");
    auto const p = a->NameToProperty("p");
    auto const w = a->NameToProperty("w");
    auto const etype = a->NameToEdgeType("T");

    {
      auto acc = db_a.Access(ms::WRITE);
      auto v1 = acc->CreateVertex();
      auto v2 = acc->CreateVertex();
      auto v3 = acc->CreateVertex();
      ASSERT_TRUE(v1.AddLabel(l).has_value());
      ASSERT_TRUE(v1.SetProperty(p, ms::PropertyValue(1)).has_value());
      ASSERT_TRUE(v2.AddLabel(l).has_value());
      ASSERT_TRUE(v2.SetProperty(p, ms::PropertyValue(2)).has_value());
      auto e1 = acc->CreateEdge(&v1, &v2, etype);
      ASSERT_TRUE(e1.has_value());
      ASSERT_TRUE(e1->SetProperty(w, ms::PropertyValue(1)).has_value());
      auto e2 = acc->CreateEdge(&v2, &v3, etype);
      ASSERT_TRUE(e2.has_value());
      g = {v1.Gid(), v2.Gid(), v3.Gid(), e1->Gid(), e2->Gid()};
      Commit(*acc);
    }

    ASSERT_TRUE(a->CreateSnapshot().has_value());
    ASSERT_TRUE(std::filesystem::exists(kDirA / memgraph::storage::durability::kSnapshotDirectory));
    std::filesystem::create_directories(kDirB / memgraph::storage::durability::kSnapshotDirectory);
    for (auto const &entry :
         std::filesystem::directory_iterator(kDirA / memgraph::storage::durability::kSnapshotDirectory)) {
      if (!entry.is_regular_file()) continue;
      std::filesystem::copy_file(entry.path(),
                                 kDirB / memgraph::storage::durability::kSnapshotDirectory / entry.path().filename());
    }

    // Tail. Each write is its own commit; read-only accessors in between make main's timestamps sparse.
    auto const sparse_read = [&](bool abort) {
      auto acc = db_a.Access(ms::READ);
      (void)CountVertices(*acc, ms::View::OLD);
      if (abort) {
        acc->Abort();
      } else {
        Commit(*acc);
      }
    };

    {  // v1.p = 10
      auto acc = db_a.Access(ms::WRITE);
      auto v1 = acc->FindVertex(g.v1, ms::View::OLD);
      ASSERT_TRUE(v1->SetProperty(p, ms::PropertyValue(10)).has_value());
      Commit(*acc);
    }
    sparse_read(false);
    {  // v2: -L +M
      auto acc = db_a.Access(ms::WRITE);
      auto v2 = acc->FindVertex(g.v2, ms::View::OLD);
      ASSERT_TRUE(v2->RemoveLabel(l).has_value());
      ASSERT_TRUE(v2->AddLabel(m).has_value());
      Commit(*acc);
    }
    sparse_read(true);
    sparse_read(false);
    {  // delete e1
      auto acc = db_a.Access(ms::WRITE);
      auto e1 = acc->FindEdge(g.e1, ms::View::OLD);
      ASSERT_TRUE(e1.has_value());
      ASSERT_TRUE(acc->DeleteEdge(&*e1).has_value());
      Commit(*acc);
    }
    sparse_read(true);
    {  // detach-delete v3 (takes e2 with it)
      auto acc = db_a.Access(ms::WRITE);
      auto v3 = acc->FindVertex(g.v3, ms::View::OLD);
      ASSERT_TRUE(v3.has_value());
      ASSERT_TRUE(acc->DetachDeleteVertex(&*v3).has_value());
      Commit(*acc);
    }
    sparse_read(false);
    {  // create v4 {p:4} and v1 -> v4
      auto acc = db_a.Access(ms::WRITE);
      auto v1 = acc->FindVertex(g.v1, ms::View::OLD);
      auto v4 = acc->CreateVertex();
      ASSERT_TRUE(v4.SetProperty(p, ms::PropertyValue(4)).has_value());
      auto e3 = acc->CreateEdge(&*v1, &v4, etype);
      ASSERT_TRUE(e3.has_value());
      t = {v4.Gid(), e3->Gid()};
      Commit(*acc);
    }
    sparse_read(true);
    {  // label-property index :L(p)
      auto acc = db_a.ReadOnlyAccess();
      ASSERT_TRUE(acc->CreateIndex(l, {ms::PropertyPath{p}}, ms::IndexOrder::ASC).has_value());
      Commit(*acc);
    }
    {  // v4.p = 40
      auto acc = db_a.Access(ms::WRITE);
      auto v4 = acc->FindVertex(t.v4, ms::View::OLD);
      ASSERT_TRUE(v4->SetProperty(p, ms::PropertyValue(40)).has_value());
      Commit(*acc);
    }
    // snapshot_on_exit defaults to false: the tail exists only in the WALs.
  }

  auto const wal_files =
      memgraph::storage::durability::GetWalFiles(kDirA / memgraph::storage::durability::kWalDirectory);
  ASSERT_TRUE(wal_files.has_value());
  ASSERT_FALSE(wal_files->empty());

  // ---- Step 2: storage B recovers from the copied snapshot only (T). ----
  memgraph::dbms::Database db_b{MakeConfigB()};
  const memgraph::memory::DbArenaScope arena_scope_b{&db_b.Arena()};
  auto *b = static_cast<ms::InMemoryStorage *>(db_b.storage());
  auto names = ResolveNames(*b);
  {
    auto acc = db_b.Access(ms::READ);
    ExpectStateAtT(*acc, names, g, t);
  }

  // ---- Step 3: pin T before replaying anything. ----
  uint64_t const pin = b->RegisterForkPin();

  // ---- Step 4: replay main's WAL files in sequence-number order through ReplayWalFile. ----
  memgraph::rpc::ProgressHeartbeat heartbeat;
  for (auto const &info : *wal_files) {  // GetWalFiles is ordered by sequence number
    ASSERT_TRUE(memgraph::dbms::InMemoryReplicationHandlers::ReplayWalFile(info.path, b, heartbeat))
        << "ReplayWalFile failed for " << info.path;
  }

  // Names added by the replay (e.g. M after recovery of a snapshot that only had L) must be resolved afterwards.
  names = ResolveNames(*b);

  // ---- Step 5: latest = full state, historical(pin) = state at T, before and after GC. ----
  auto const check_reads = [&] {
    {
      auto acc = db_b.Access(ms::READ);
      ExpectFullState(*acc, names, g, t);
    }
    auto historical = b->HistoricalAccess(pin);
    ASSERT_TRUE(historical.has_value());
    ExpectStateAtT(**historical, names, g, t);
  };

  check_reads();
  EXPECT_GT(db_b.metric_handles()->unreleased_delta_objects.Value(), 0)
      << "replayed transactions must leave deltas behind for the pinned history";

  for (int i = 0; i < 6; ++i) b->FreeMemory({}, false);
  check_reads();
  EXPECT_GT(db_b.metric_handles()->unreleased_delta_objects.Value(), 0) << "GC reclaimed history a pin still needs";

  // ---- Negative control: without the pin the history is reclaimable and the pin is rejected. ----
  b->ReleaseForkPin(pin);
  for (int i = 0; i < 6; ++i) b->FreeMemory({}, false);
  EXPECT_EQ(db_b.metric_handles()->unreleased_delta_objects.Value(), 0)
      << "history should be reclaimed once the pin is released";
  auto released = b->HistoricalAccess(pin);
  ASSERT_FALSE(released.has_value());
  EXPECT_EQ(released.error(), ms::InMemoryStorage::HistoricalAccessError::ForkTimestampNotPinned);
  {
    auto acc = db_b.Access(ms::READ);
    ExpectFullState(*acc, names, g, t);
  }
}

// A failover inside the replayed WAL tail must leave main with the same epoch and epoch history that an unbounded
// recovery of the same files produces; replicas compare both on reconnect.
TEST_F(VersioningHistoryReplayTest, TailReplayRestoresEpochAndHistory) {
  std::string first_epoch;
  std::string second_epoch;
  {
    memgraph::dbms::Database db{MakeDurableConfig(kDirMain, /*recover_on_startup=*/false)};
    const memgraph::memory::DbArenaScope arena_scope{&db.Arena()};
    auto *storage = static_cast<ms::InMemoryStorage *>(db.storage());
    first_epoch = std::string(storage->repl_storage_state_.epoch_.id());

    CommitVertex(db);
    ASSERT_TRUE(db.version_store()->CreateBranch("b", "main", std::nullopt).has_value());
    CommitVertex(db);
  }
  {
    // Promotion after a restart (the WAL is already finalized): remember (epoch, ldt), switch the epoch. The next
    // commit opens a WAL under the new epoch.
    memgraph::dbms::Database db{MakeDurableConfig(kDirMain, /*recover_on_startup=*/true)};
    const memgraph::memory::DbArenaScope arena_scope{&db.Arena()};
    auto *storage = static_cast<ms::InMemoryStorage *>(db.storage());
    ASSERT_EQ(db.version_store()->List().size(), 1U);
    storage->repl_storage_state_.SaveLatestHistory();
    second_epoch = std::string(memgraph::utils::UUID{});
    ASSERT_NE(first_epoch, second_epoch);
    storage->repl_storage_state_.epoch_.SetEpoch(second_epoch);
    CommitVertex(db);
    CommitVertex(db);
  }

  std::filesystem::copy(kDirMain, kDirUnbounded, std::filesystem::copy_options::recursive);
  std::filesystem::remove_all(kDirUnbounded / "branches");

  // Reference: unbounded recovery (no ledger) of the same files.
  EpochState expected;
  {
    memgraph::dbms::Database db{MakeDurableConfig(kDirUnbounded, /*recover_on_startup=*/true)};
    const memgraph::memory::DbArenaScope arena_scope{&db.Arena()};
    expected = CaptureEpochState(db);
  }
  ASSERT_EQ(expected.epoch, second_epoch);
  ASSERT_GE(expected.history.size(), 2U);
  EXPECT_EQ(expected.history.front().first, first_epoch);
  EXPECT_EQ(expected.history.back().first, second_epoch);

  // Bounded recovery with the branch: the second-epoch WAL files are replayed from the tail.
  memgraph::dbms::Database db{MakeDurableConfig(kDirMain, /*recover_on_startup=*/true)};
  const memgraph::memory::DbArenaScope arena_scope{&db.Arena()};
  ASSERT_EQ(db.version_store()->List().size(), 1U);
  ASSERT_TRUE(static_cast<ms::InMemoryStorage *>(db.storage())->OldestForkPin().has_value());

  const auto actual = CaptureEpochState(db);
  EXPECT_EQ(actual.epoch, expected.epoch);
  EXPECT_EQ(actual.history, expected.history);
}
