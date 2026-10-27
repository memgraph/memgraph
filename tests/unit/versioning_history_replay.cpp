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

// Spike: can main's WAL tail (everything after a snapshot at T) be replayed into a fresh storage that recovered
// that snapshot, as real MVCC transactions through the replica apply path (InMemoryReplicationHandlers::LoadWal),
// so that a fork pin registered BEFORE the replay + HistoricalAccess reads exactly the state at T, latest reads the
// full state, and GC keeps the history while the pin is held.

#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include <algorithm>
#include <chrono>
#include <filesystem>
#include <iostream>
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

namespace memgraph::dbms {
// Befriended by InMemoryReplicationHandlers (replication_handlers.hpp) so the spike can drive the private LoadWal.
struct HistoryReplayTestAccess {
  static std::pair<bool, uint64_t> LoadWal(std::filesystem::path const &wal_path, storage::InMemoryStorage *storage,
                                           rpc::ProgressHeartbeat &heartbeat) {
    auto const status = InMemoryReplicationHandlers::LoadWal(wal_path, storage, heartbeat);
    return {status.success, status.num_txns_committed};
  }
};
}  // namespace memgraph::dbms

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

struct BaseGids {
  ms::Gid v1, v2, v3, e1, e2;
};

struct TailGids {
  ms::Gid v4, e3;
};

void Commit(ms::Storage::Accessor &acc) {
  ASSERT_TRUE(acc.PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()).has_value());
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
    for (auto &e : out->edges) {
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
    std::filesystem::remove_all(kRoot);
  }

  void TearDown() override { std::filesystem::remove_all(kRoot); }
};

TEST_F(VersioningHistoryReplayTest, WalTailReplayIntoRecoveredSnapshotKeepsHistoryAtPin) {
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
      std::cerr << "[snapshot-copy] " << entry.path() << " regular=" << entry.is_regular_file() << " perms=0"
                << std::oct << static_cast<unsigned>(entry.status().permissions()) << std::dec << '\n';
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
  uint64_t last_durable_ts = 0;
  for (auto const &info : *wal_files) last_durable_ts = std::max(last_durable_ts, info.to_timestamp);

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

  // ---- Step 4: replay main's WAL files in sequence-number order through the replica apply path. ----
  memgraph::rpc::ProgressHeartbeat heartbeat{nullptr, std::chrono::hours{24}};
  uint64_t replayed_txns = 0;
  for (auto const &info : *wal_files) {  // GetWalFiles is ordered by sequence number
    auto const [ok, num_txns] = memgraph::dbms::HistoryReplayTestAccess::LoadWal(info.path, b, heartbeat);
    ASSERT_TRUE(ok) << "LoadWal failed for " << info.path;
    replayed_txns += num_txns;
  }
  // 7 write transactions in the tail (property, label swap, edge delete, vertex delete, create, index, property).
  EXPECT_GE(replayed_txns, 7U);

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

  // ---- Step 6: clock hazard. A new local commit gets timestamp_++ on B; the WAL path asserts (debug builds) that
  // its durable ts is >= the last durable ts replayed from main. B's timestamp_ is not directly reachable here, so a
  // fresh accessor's start timestamp (timestamp_++) stands in for it. Not exercised as a commit: it may abort. ----
  uint64_t b_next_ts = 0;
  {
    auto acc = db_b.Access(ms::READ);
    b_next_ts = acc->GetStartTimestamp().value_or(0);
  }
  std::cout << "[clock-hazard] last durable ts replayed from main (ldt) = " << last_durable_ts
            << ", B's next local timestamp = " << b_next_ts
            << (b_next_ts > last_durable_ts ? " (OK: local clock ahead of ldt)"
                                            : " (HAZARD: a local commit on B would hit 'LDT not monotonically "
                                              "increasing')")
            << std::endl;
  // Known hazard (design note): replay commits at B's local clock, so B's clock trails main's last durable ts and
  // production recovery must bump timestamp_ past it (plus MarkFinishedInRange) before any local commit.
  EXPECT_LE(b_next_ts, last_durable_ts) << "hazard no longer reproduces; revisit the clock-bump requirement";
}
