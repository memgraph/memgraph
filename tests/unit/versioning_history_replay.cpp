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

// WAL-tail replay (ReplayHistoryTail): the history at each fork pin survives replay and GC, the replayed storage
// matches an unbounded recovery, and bad input is refused without holding a pin.

#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include <algorithm>
#include <array>
#include <atomic>
#include <chrono>
#include <filesystem>
#include <optional>
#include <string>
#include <thread>
#include <utility>
#include <vector>

#include <fmt/format.h>
#include <fmt/ranges.h>

#include "dbms/database.hpp"
#include "dbms/inmemory/history_replay.hpp"
#include "flags/general.hpp"
#include "memory/db_arena.hpp"
#include "storage/v2/durability/durability.hpp"
#include "storage/v2/durability/paths.hpp"
#include "storage/v2/fmt.hpp"
#include "storage/v2/indices/index_order.hpp"
#include "storage/v2/indices/property_path.hpp"
#include "storage/v2/inmemory/storage.hpp"
#include "storage/v2/property_value.hpp"
#include "storage/v2/view.hpp"
#include "storage_test_utils.hpp"
#include "tests/test_commit_args_helper.hpp"
#include "utils/scheduler.hpp"
#include "utils/uuid.hpp"

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

ms::Config MakeDurableConfig(const std::filesystem::path &dir, bool recover_on_startup,
                             std::optional<uint64_t> recovery_history_bound = std::nullopt, bool narrowing = false) {
  ms::Config config;
  config.gc = {.type = ms::Config::Gc::Type::PERIODIC, .interval = std::chrono::hours(24)};
  config.durability = {.storage_directory = dir,
                       .recover_on_startup = recover_on_startup,
                       .snapshot_wal_mode = ms::Config::Durability::SnapshotWalMode::PERIODIC_SNAPSHOT_WITH_WAL,
                       .snapshot_interval = memgraph::utils::SchedulerInterval{std::chrono::hours(24)},
                       .wal_file_flush_every_n_tx = 1,
                       .recovery_history_bound = recovery_history_bound};
  config.experimental_commit_lock_narrowing = narrowing;
  return config;
}

uint64_t Ldt(ms::InMemoryStorage const &storage) {
  return storage.repl_storage_state_.commit_ts_info_.load(std::memory_order_acquire).ldt_;
}

// A recovered database, with the tail bounded recovery left out (if bounded).
struct Recovered {
  explicit Recovered(const std::filesystem::path &dir, std::optional<uint64_t> bound = std::nullopt,
                     bool narrowing = false)
      : db{MakeDurableConfig(dir, /*recover_on_startup=*/true, bound, narrowing)},
        scope{&db.Arena()},
        storage{*static_cast<ms::InMemoryStorage *>(db.storage())},
        tail{storage.TakeRecoveryTail()} {}

  memgraph::dbms::Database db;
  memgraph::memory::DbArenaScope scope;
  ms::InMemoryStorage &storage;
  std::optional<ms::durability::RecoveryTail> tail;
};

// The graph by names, so storages whose name ids differ compare equal.
std::string DumpGraph(ms::Storage::Accessor &acc) {
  constexpr auto kView = ms::View::OLD;
  auto const props = [&](auto const &object) {
    std::vector<std::string> out;
    for (auto const &[key, value] : *object.Properties(kView)) {
      out.push_back(fmt::format("{}={}", acc.PropertyToName(key), value));
    }
    std::ranges::sort(out);
    return fmt::format("{{{}}}", fmt::join(out, ","));
  };
  std::string dump;
  for (auto vertex : acc.Vertices(kView)) {
    std::vector<std::string> labels;
    for (auto const label : *vertex.Labels(kView)) labels.push_back(acc.LabelToName(label));
    std::ranges::sort(labels);
    std::vector<std::string> edges;
    for (auto const &edge : vertex.OutEdges(kView)->edges) {
      edges.push_back(fmt::format("-[{}:{}{}]->{}",
                                  edge.Gid().AsUint(),
                                  acc.EdgeTypeToName(edge.EdgeType()),
                                  props(edge),
                                  edge.ToVertex().Gid().AsUint()));
    }
    std::ranges::sort(edges);
    dump += fmt::format(
        "{}:{}{} {}\n", vertex.Gid().AsUint(), fmt::join(labels, ":"), props(vertex), fmt::join(edges, " "));
  }
  return dump;
}

std::string DumpAt(ms::InMemoryStorage &storage, uint64_t pin) {
  auto historical = storage.HistoricalAccess(pin);
  if (!historical) return "<no access>";
  return DumpGraph(**historical);
}

std::string DumpLatest(memgraph::dbms::Database &db) {
  auto acc = db.Access(ms::READ);
  return DumpGraph(*acc);
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

// Main history in `dir`: write i adds vertex {p: i} with an edge from the previous vertex and updates that vertex; a
// read-only commit after each write makes the durable timestamps sparse. Returns each write's durable timestamp.
std::vector<uint64_t> WriteMainHistory(const std::filesystem::path &dir, int writes) {
  memgraph::dbms::Database db{MakeDurableConfig(dir, /*recover_on_startup=*/false)};
  const memgraph::memory::DbArenaScope arena_scope{&db.Arena()};
  auto &storage = *static_cast<ms::InMemoryStorage *>(db.storage());
  auto const label = storage.NameToLabel("L");
  auto const p = storage.NameToProperty("p");
  auto const type = storage.NameToEdgeType("T");
  std::vector<uint64_t> durable;
  std::optional<ms::Gid> prev;
  for (int i = 0; i < writes; ++i) {
    {
      auto acc = db.Access(ms::WRITE);
      auto v = acc->CreateVertex();
      EXPECT_TRUE(v.AddLabel(label).has_value());
      EXPECT_TRUE(v.SetProperty(p, ms::PropertyValue(i)).has_value());
      if (prev) {
        auto from = acc->FindVertex(*prev, ms::View::NEW);
        EXPECT_TRUE(from->SetProperty(p, ms::PropertyValue(10 * i)).has_value());
        auto edge = acc->CreateEdge(&*from, &v, type);
        EXPECT_TRUE(edge->SetProperty(p, ms::PropertyValue(i)).has_value());
      }
      prev = v.Gid();
      Commit(*acc);
    }
    durable.push_back(Ldt(storage));
    auto acc = db.Access(ms::READ);
    Commit(*acc);
  }
  return durable;
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
    std::filesystem::remove_all(kRoot);
  }

  void TearDown() override { std::filesystem::remove_all(kRoot); }
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

  // ---- Step 3: replay main's WAL files over T, pinning a fork right after T. ----
  std::vector<std::filesystem::path> paths;
  for (auto const &info : *wal_files) paths.push_back(info.path);  // GetWalFiles is ordered by sequence number
  ms::durability::RecoveryTail const tail{.wal_files = std::move(paths), .reached_timestamp = Ldt(*b)};
  auto pins = memgraph::dbms::ReplayHistoryTail(*b, tail, std::array{tail.reached_timestamp + 1});
  ASSERT_TRUE(pins.has_value()) << pins.error();
  uint64_t const pin = pins->front();

  // Names added by the replay (e.g. M after recovery of a snapshot that only had L) must be resolved afterwards.
  names = ResolveNames(*b);

  // ---- Step 4: latest = full state, historical(pin) = state at T, before and after GC. ----
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
// recovery of the same files produces; replicas compare both on reconnect. The pin for a fork inside the tail must
// keep the state at the fork readable, both for a fork at the recovered state and one inside the tail.
TEST_F(VersioningHistoryReplayTest, TailReplayRestoresEpochAndHistory) {
  std::string first_epoch;
  std::string second_epoch;
  uint64_t fork_ts{0};
  uint64_t tail_fork_ts{0};
  {
    memgraph::dbms::Database db{MakeDurableConfig(kDirMain, /*recover_on_startup=*/false)};
    const memgraph::memory::DbArenaScope arena_scope{&db.Arena()};
    auto *storage = static_cast<ms::InMemoryStorage *>(db.storage());
    first_epoch = std::string(storage->repl_storage_state_.epoch_.id());

    CommitVertex(db);
    fork_ts = storage->RegisterForkPin();
    storage->ReleaseForkPin(fork_ts);
    CommitVertex(db);
  }
  {
    // Promotion after a restart (the WAL is already finalized): remember (epoch, ldt), switch the epoch. The next
    // commit opens a WAL under the new epoch.
    memgraph::dbms::Database db{MakeDurableConfig(kDirMain, /*recover_on_startup=*/true)};
    const memgraph::memory::DbArenaScope arena_scope{&db.Arena()};
    auto *storage = static_cast<ms::InMemoryStorage *>(db.storage());
    storage->repl_storage_state_.SaveLatestHistory();
    second_epoch = std::string(memgraph::utils::UUID{});
    ASSERT_NE(first_epoch, second_epoch);
    storage->repl_storage_state_.epoch_.SetEpoch(second_epoch);
    CommitVertex(db);
    tail_fork_ts = storage->RegisterForkPin();
    storage->ReleaseForkPin(tail_fork_ts);
    CommitVertex(db);
  }

  std::filesystem::copy(kDirMain, kDirUnbounded, std::filesystem::copy_options::recursive);

  // Reference: unbounded recovery of the same files.
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

  // Bounded recovery below the fork; the rest, including the second-epoch WAL files, is replayed from the tail.
  memgraph::dbms::Database db{MakeDurableConfig(kDirMain, /*recover_on_startup=*/true, fork_ts - 1)};
  const memgraph::memory::DbArenaScope arena_scope{&db.Arena()};
  auto *storage = static_cast<ms::InMemoryStorage *>(db.storage());
  auto tail = storage->TakeRecoveryTail();
  ASSERT_TRUE(tail.has_value());
  const std::array forks{fork_ts, tail_fork_ts};
  auto pins = memgraph::dbms::ReplayHistoryTail(*storage, *tail, forks);
  ASSERT_TRUE(pins.has_value()) << pins.error();
  ASSERT_EQ(pins->size(), 2U);
  EXPECT_EQ(storage->OldestForkPin(), pins->front());

  const auto actual = CaptureEpochState(db);
  EXPECT_EQ(actual.epoch, expected.epoch);
  EXPECT_EQ(actual.history, expected.history);

  {
    auto acc = db.Access(ms::READ);
    EXPECT_EQ(CountVertices(*acc, ms::View::OLD), 4U);
  }
  for (auto const [pin, visible] : {std::pair{(*pins)[0], 1U}, std::pair{(*pins)[1], 3U}}) {
    auto historical = storage->HistoricalAccess(pin);
    ASSERT_TRUE(historical.has_value());
    EXPECT_EQ(CountVertices(**historical, ms::View::OLD), visible) << "a fork sees only the commits before it";
  }
  for (auto const pin : *pins) storage->ReleaseForkPin(pin);
}

// The pin for a fork whose timestamp equals the durable timestamp of the first replayed transaction must be
// registered BEFORE that transaction is applied: the fork's snapshot-isolation read is exclusive, so a commit
// landing exactly on fork_ts is part of main but invisible to the branch, and its deltas stay pinned (a pin at P
// protects deltas with commit_ts == P). The replay runs on a WAL-enabled storage and must leave the WAL files and
// the current epoch untouched.
TEST_F(VersioningHistoryReplayTest, PinAtForkTimestampEqualsFirstReplayedCommit) {
  uint64_t fork_ts{0};
  ms::Gid v1{}, v2{};
  {
    memgraph::dbms::Database db{MakeDurableConfig(kDirMain, /*recover_on_startup=*/false)};
    const memgraph::memory::DbArenaScope arena_scope{&db.Arena()};
    auto *storage = static_cast<ms::InMemoryStorage *>(db.storage());
    auto const p = storage->NameToProperty("p");

    {
      auto acc = db.Access(ms::WRITE);
      auto v = acc->CreateVertex();
      v1 = v.Gid();
      Commit(*acc);
    }
    {
      // A transaction open across the fork capture commits exactly at fork_ts: the clock is captured without
      // incrementing, and the commit mints the post-increment value.
      auto acc = db.Access(ms::WRITE);
      auto v = acc->FindVertex(v1, ms::View::OLD);
      ASSERT_TRUE(v.has_value());
      ASSERT_TRUE(v->SetProperty(p, ms::PropertyValue(1)).has_value());
      fork_ts = storage->RegisterForkPin();
      Commit(*acc);
    }
    {
      auto acc = db.Access(ms::WRITE);
      v2 = acc->CreateVertex().Gid();
      Commit(*acc);
    }
    storage->ReleaseForkPin(fork_ts);  // the recovered storage re-registers its own pin
  }

  // Recover only through fork_ts - 1: the commit at fork_ts is the first transaction of the tail.
  memgraph::dbms::Database db{MakeDurableConfig(kDirMain, /*recover_on_startup=*/true, fork_ts - 1)};
  const memgraph::memory::DbArenaScope arena_scope{&db.Arena()};
  auto *storage = static_cast<ms::InMemoryStorage *>(db.storage());
  {
    auto acc = db.Access(ms::READ);
    EXPECT_EQ(CountVertices(*acc, ms::View::OLD), 1U) << "recovery stops below the commit at fork_ts";
    auto vv = acc->FindVertex(v1, ms::View::OLD);
    ASSERT_TRUE(vv.has_value());
    EXPECT_TRUE(vv->GetProperty(storage->NameToProperty("p"), ms::View::OLD)->IsNull())
        << "recovery stops below the commit at fork_ts";
  }
  auto tail = storage->TakeRecoveryTail();
  ASSERT_TRUE(tail.has_value());
  ASSERT_FALSE(tail->wal_files.empty());

  auto const wal_before =
      memgraph::storage::durability::GetWalFiles(kDirMain / memgraph::storage::durability::kWalDirectory);
  ASSERT_TRUE(wal_before.has_value());
  auto const epoch_before = std::string(storage->repl_storage_state_.epoch_.id());

  auto pins = memgraph::dbms::ReplayHistoryTail(*storage, *tail, std::array{fork_ts});
  ASSERT_TRUE(pins.has_value()) << pins.error();
  ASSERT_EQ(pins->size(), 1U);

  auto const wal_after =
      memgraph::storage::durability::GetWalFiles(kDirMain / memgraph::storage::durability::kWalDirectory);
  ASSERT_TRUE(wal_after.has_value());
  EXPECT_EQ(wal_after->size(), wal_before->size()) << "tail replay must not append to the WAL";
  for (size_t i = 0; i < std::min(wal_before->size(), wal_after->size()); ++i) {
    EXPECT_EQ((*wal_after)[i].path, (*wal_before)[i].path);
    EXPECT_EQ((*wal_after)[i].to_timestamp, (*wal_before)[i].to_timestamp);
  }
  EXPECT_EQ(std::string(storage->repl_storage_state_.epoch_.id()), epoch_before) << "replay must not switch the epoch";

  {
    auto acc = db.Access(ms::READ);
    EXPECT_EQ(CountVertices(*acc, ms::View::OLD), 2U);
    auto v1_acc = acc->FindVertex(v1, ms::View::OLD);
    ASSERT_TRUE(v1_acc.has_value());
    EXPECT_EQ(*v1_acc->GetProperty(storage->NameToProperty("p"), ms::View::OLD), ms::PropertyValue(1))
        << "the commit at fork_ts belongs to main";
  }
  {
    auto historical = storage->HistoricalAccess((*pins)[0]);
    ASSERT_TRUE(historical.has_value());
    auto acc = std::move(*historical);
    EXPECT_EQ(CountVertices(*acc, ms::View::OLD), 1U)
        << "a commit exactly at fork_ts is invisible to the fork (snapshot isolation is exclusive)";
    auto v1_acc = acc->FindVertex(v1, ms::View::OLD);
    ASSERT_TRUE(v1_acc.has_value());
    EXPECT_TRUE(v1_acc->GetProperty(storage->NameToProperty("p"), ms::View::OLD)->IsNull())
        << "the commit at fork_ts is invisible to the fork";
    EXPECT_FALSE(acc->FindVertex(v2, ms::View::OLD).has_value());
  }

  // The pinned deltas include the commit at fork_ts: the fast-discard gate needs oldest pin > commit_ts.
  for (int i = 0; i < 6; ++i) storage->FreeMemory({}, false);
  EXPECT_GT(db.metric_handles()->unreleased_delta_objects.Value(), 0)
      << "the pin must keep the deltas of the commit at fork_ts";
  storage->ReleaseForkPin((*pins)[0]);
  for (int i = 0; i < 6; ++i) storage->FreeMemory({}, false);
  EXPECT_EQ(db.metric_handles()->unreleased_delta_objects.Value(), 0) << "history is reclaimable without the pin";
  auto released = storage->HistoricalAccess((*pins)[0]);
  ASSERT_FALSE(released.has_value());
}

// Replay registers pins positionally as it crosses each fork timestamp, and a fork at or before the recovered state
// cannot be pinned without leaking later commits into it: both are refused, holding no pin.
TEST_F(VersioningHistoryReplayTest, RefusesUnsortedOrEarlyForkTimestamps) {
  auto const durable = WriteMainHistory(kDirMain, 3);
  Recovered r{kDirMain, durable[0]};
  ASSERT_TRUE(r.tail.has_value());
  ASSERT_EQ(r.tail->reached_timestamp, durable[0]);

  for (auto const &forks : {std::vector<uint64_t>{durable[2], durable[1]}, std::vector<uint64_t>{durable[0]}}) {
    auto pins = memgraph::dbms::ReplayHistoryTail(r.storage, *r.tail, forks);
    EXPECT_FALSE(pins.has_value());
    EXPECT_FALSE(r.storage.OldestForkPin().has_value());
  }
  EXPECT_EQ(Ldt(r.storage), durable[0]) << "nothing replayed";
}

// Duplicates each get their own pin; with no WAL to replay every pin is taken at the recovered state, and the clock
// moves to the newest fork so later commits cannot fall below any of them.
TEST_F(VersioningHistoryReplayTest, EmptyTailPinsEveryForkAndBumpsTheClock) {
  auto const durable = WriteMainHistory(kDirMain, 2);
  Recovered r{kDirMain};
  auto const reached = Ldt(r.storage);
  ASSERT_EQ(reached, durable.back());
  auto const expected = DumpLatest(r.db);

  ms::durability::RecoveryTail const tail{.wal_files = {}, .reached_timestamp = reached};
  auto const far = reached + 100;
  auto pins = memgraph::dbms::ReplayHistoryTail(r.storage, tail, std::array{reached + 1, reached + 1, far});
  ASSERT_TRUE(pins.has_value()) << pins.error();
  ASSERT_EQ(pins->size(), 3U);
  EXPECT_EQ((*pins)[0], (*pins)[1]);

  CommitVertex(r.db);
  EXPECT_GE(Ldt(r.storage), far);
  r.storage.ReleaseForkPin((*pins)[0]);
  for (auto const pin : {(*pins)[1], (*pins)[2]}) EXPECT_EQ(DumpAt(r.storage, pin), expected);
  r.storage.ReleaseForkPin((*pins)[1]);
  r.storage.ReleaseForkPin((*pins)[2]);
  EXPECT_FALSE(r.storage.OldestForkPin().has_value());
}

// Bounded recovery + tail replay ends where an unbounded recovery of the same files does, and each fork (including
// one past the end of the tail) sees exactly the commits below it.
TEST_F(VersioningHistoryReplayTest, ReplayMatchesUnboundedRecovery) {
  auto const durable = WriteMainHistory(kDirMain, 6);
  std::filesystem::copy(kDirMain, kDirUnbounded, std::filesystem::copy_options::recursive);

  std::vector<std::string> at_fork;  // at_fork[i]: the graph after writes [0, i]
  uint64_t unbounded_ldt{0};
  uint64_t unbounded_committed{0};
  uint64_t unbounded_next{0};
  for (auto const ts : durable) {
    std::filesystem::remove_all(kDirB);
    std::filesystem::copy(kDirMain, kDirB, std::filesystem::copy_options::recursive);
    Recovered bounded{kDirB, ts};
    at_fork.push_back(DumpLatest(bounded.db));
  }
  std::string unbounded_graph;
  {
    Recovered unbounded{kDirUnbounded};
    unbounded_graph = DumpLatest(unbounded.db);
    auto const info = unbounded.storage.repl_storage_state_.commit_ts_info_.load();
    unbounded_ldt = info.ldt_;
    unbounded_committed = info.num_committed_txns_;
    CommitVertex(unbounded.db);
    unbounded_next = Ldt(unbounded.storage);
  }
  ASSERT_EQ(unbounded_graph, at_fork.back());

  Recovered r{kDirMain, durable[0]};
  ASSERT_TRUE(r.tail.has_value());
  // Forks right after writes 0, 2 and 5 (the last one past the end of the tail).
  auto pins =
      memgraph::dbms::ReplayHistoryTail(r.storage, *r.tail, std::array{durable[0] + 1, durable[2] + 1, durable[5] + 1});
  ASSERT_TRUE(pins.has_value()) << pins.error();
  ASSERT_EQ(pins->size(), 3U);

  EXPECT_EQ(DumpLatest(r.db), unbounded_graph);
  auto const info = r.storage.repl_storage_state_.commit_ts_info_.load();
  EXPECT_EQ(info.ldt_, unbounded_ldt);
  EXPECT_EQ(info.num_committed_txns_, unbounded_committed);
  EXPECT_EQ(DumpAt(r.storage, (*pins)[0]), at_fork[0]);
  EXPECT_EQ(DumpAt(r.storage, (*pins)[1]), at_fork[2]);
  EXPECT_EQ(DumpAt(r.storage, (*pins)[2]), at_fork[5]);

  CommitVertex(r.db);
  EXPECT_EQ(Ldt(r.storage), unbounded_next) << "the next commit lands where it would after an unbounded recovery";
  for (auto const pin : *pins) r.storage.ReleaseForkPin(pin);
}

// The newest WAL was never finalized (main still running); replay reads it like recovery does.
TEST_F(VersioningHistoryReplayTest, ReplaysUnfinalizedCurrentWal) {
  std::string expected;
  uint64_t first{0};
  {
    memgraph::dbms::Database db{MakeDurableConfig(kDirMain, /*recover_on_startup=*/false)};
    const memgraph::memory::DbArenaScope arena_scope{&db.Arena()};
    auto &storage = *static_cast<ms::InMemoryStorage *>(db.storage());
    CommitVertex(db);
    first = Ldt(storage);
    CommitVertex(db);
    CommitVertex(db);
    expected = DumpLatest(db);
    std::filesystem::copy(kDirMain, kDirB, std::filesystem::copy_options::recursive);
  }

  Recovered r{kDirB, first};
  ASSERT_TRUE(r.tail.has_value());
  ASSERT_FALSE(r.tail->wal_files.empty());
  auto pins = memgraph::dbms::ReplayHistoryTail(r.storage, *r.tail, std::array{first + 1});
  ASSERT_TRUE(pins.has_value()) << pins.error();
  EXPECT_EQ(DumpLatest(r.db), expected);
  {
    auto historical = r.storage.HistoricalAccess(pins->front());
    ASSERT_TRUE(historical.has_value());
    EXPECT_EQ(CountVertices(**historical, ms::View::OLD), 1U);
  }
  r.storage.ReleaseForkPin(pins->front());
}

// A transaction main aborted after writing it to the WAL (2PC, commit flag false) is skipped. The pin of a fork at
// its timestamp is still taken before the next applied transaction.
TEST_F(VersioningHistoryReplayTest, AbortedWalTransactionIsSkipped) {
  uint64_t first{0};
  uint64_t aborted{0};
  uint64_t last{0};
  {
    memgraph::dbms::Database db{MakeDurableConfig(kDirMain, /*recover_on_startup=*/false)};
    const memgraph::memory::DbArenaScope arena_scope{&db.Arena()};
    auto &storage = *static_cast<ms::InMemoryStorage *>(db.storage());
    CommitVertex(db);
    first = Ldt(storage);
    aborted = first + 1;
    {
      auto acc = db.Access(ms::WRITE);
      acc->CreateVertex();
      ASSERT_TRUE(
          acc->PrepareForCommitPhase(ms::CommitArgs::make_replica_write(aborted, /*two_phase_commit=*/true, [] {}))
              .has_value());
      static_cast<ms::InMemoryStorage::InMemoryAccessor *>(acc.get())->AbortAndResetCommitTs();
    }
    CommitVertex(db);
    last = Ldt(storage);
    ASSERT_GT(last, aborted);
  }

  Recovered r{kDirMain, first};
  ASSERT_TRUE(r.tail.has_value());
  auto pins = memgraph::dbms::ReplayHistoryTail(r.storage, *r.tail, std::array{aborted, aborted + 1, last});
  ASSERT_TRUE(pins.has_value()) << pins.error();
  {
    auto acc = r.db.Access(ms::READ);
    EXPECT_EQ(CountVertices(*acc, ms::View::OLD), 2U) << "the aborted transaction is not applied";
  }
  for (auto const pin : *pins) {
    auto historical = r.storage.HistoricalAccess(pin);
    ASSERT_TRUE(historical.has_value());
    EXPECT_EQ(CountVertices(**historical, ms::View::OLD), 1U) << "every fork is before the last commit";
  }
  for (auto const pin : *pins) r.storage.ReleaseForkPin(pin);
}

// A tail WAL that cannot be replayed fails the call and releases every pin it took, including those taken before the
// failing file.
TEST_F(VersioningHistoryReplayTest, CorruptTailWalFailsWithoutPins) {
  auto const durable = WriteMainHistory(kDirMain, 3);
  Recovered r{kDirMain, durable[0]};
  ASSERT_TRUE(r.tail.has_value());
  ASSERT_FALSE(r.tail->wal_files.empty());
  auto const &wal = r.tail->wal_files.back();
  std::filesystem::resize_file(wal, std::filesystem::file_size(wal) / 2);

  auto pins = memgraph::dbms::ReplayHistoryTail(r.storage, *r.tail, std::array{durable[0] + 1, durable[2] + 1});
  EXPECT_FALSE(pins.has_value());
  EXPECT_FALSE(r.storage.OldestForkPin().has_value());
}

// The narrowed commit path publishes commits differently; replay must produce the same state and pins either way.
TEST_F(VersioningHistoryReplayTest, ReplayIsTheSameWithCommitLockNarrowing) {
  auto const durable = WriteMainHistory(kDirMain, 5);
  std::filesystem::copy(kDirMain, kDirB, std::filesystem::copy_options::recursive);
  const std::array forks{durable[0] + 1, durable[1], durable[3] + 1, durable[4] + 2};

  struct Result {
    std::vector<uint64_t> pins;
    std::vector<std::string> at_pin;
    std::string latest;
    uint64_t ldt;
  };

  auto const replay = [&](const std::filesystem::path &dir, bool narrowing) {
    Recovered r{dir, durable[0], narrowing};
    EXPECT_TRUE(r.tail.has_value());
    auto pins = memgraph::dbms::ReplayHistoryTail(r.storage, *r.tail, forks);
    EXPECT_TRUE(pins.has_value());
    Result result{.pins = *pins, .at_pin = {}, .latest = DumpLatest(r.db), .ldt = Ldt(r.storage)};
    for (auto const pin : *pins) result.at_pin.push_back(DumpAt(r.storage, pin));
    for (auto const pin : *pins) r.storage.ReleaseForkPin(pin);
    return result;
  };
  auto const off = replay(kDirMain, false);
  auto const on = replay(kDirB, true);
  EXPECT_EQ(on.pins, off.pins);
  EXPECT_EQ(on.at_pin, off.at_pin);
  EXPECT_EQ(on.latest, off.latest);
  EXPECT_EQ(on.ldt, off.ldt);
}

// After replay, readers at each pin keep seeing their fork's state while main writes and GC runs.
TEST_F(VersioningHistoryReplayTest, ForkReadersStayStableUnderMainWrites) {
  auto const durable = WriteMainHistory(kDirMain, 4);
  Recovered r{kDirMain, durable[0]};
  ASSERT_TRUE(r.tail.has_value());
  auto pins = memgraph::dbms::ReplayHistoryTail(r.storage, *r.tail, std::array{durable[1] + 1, durable[3] + 1});
  ASSERT_TRUE(pins.has_value()) << pins.error();
  std::vector<std::string> expected;
  for (auto const pin : *pins) expected.push_back(DumpAt(r.storage, pin));
  ASSERT_NE(expected[0], expected[1]);

  constexpr int kReadersPerPin = 2;
  constexpr int kReads = 100;
  constexpr int kWrites = 200;
  std::atomic<int> mismatches{0};
  std::vector<std::jthread> threads;
  for (size_t i = 0; i < pins->size(); ++i) {
    for (int t = 0; t < kReadersPerPin; ++t) {
      threads.emplace_back([&, i] {
        const memgraph::memory::DbArenaScope arena_scope{&r.db.Arena()};
        for (int n = 0; n < kReads; ++n) {
          if (DumpAt(r.storage, (*pins)[i]) != expected[i]) ++mismatches;
        }
      });
    }
  }
  threads.emplace_back([&] {
    const memgraph::memory::DbArenaScope arena_scope{&r.db.Arena()};
    for (int n = 0; n < kWrites; ++n) {
      CommitVertex(r.db);
      if (n % 20 == 0) r.storage.FreeMemory({}, false);
    }
  });
  threads.clear();

  EXPECT_EQ(mismatches.load(), 0);
  {
    auto acc = r.db.Access(ms::READ);
    EXPECT_EQ(CountVertices(*acc, ms::View::OLD), durable.size() + kWrites);
  }
  for (auto const pin : *pins) r.storage.ReleaseForkPin(pin);
}

// The clock bump marks every skipped id finished, so GC is not held back by ids no transaction ever had.
TEST_F(VersioningHistoryReplayTest, ClockBumpMarksSkippedIdsFinished) {
  memgraph::dbms::Database db{MakeDurableConfig(kDirMain, /*recover_on_startup=*/false)};
  const memgraph::memory::DbArenaScope arena_scope{&db.Arena()};
  auto &storage = *static_cast<ms::InMemoryStorage *>(db.storage());
  constexpr uint64_t kTarget = 1'000;
  storage.BumpClockPastDurable(kTarget);
  EXPECT_EQ(storage.OldestActiveTimestamp(), kTarget);
  storage.BumpClockPastDurable(kTarget);
  EXPECT_EQ(storage.OldestActiveTimestamp(), kTarget) << "idempotent once ahead";
  CommitVertex(db);
  EXPECT_GT(Ldt(storage), kTarget);
  storage.FreeMemory({}, false);
  EXPECT_GT(storage.OldestActiveTimestamp(), kTarget);
}

TEST_F(VersioningHistoryReplayTest, WalSuppressionIsNotNestable) {
  memgraph::dbms::Database db{MakeDurableConfig(kDirMain, /*recover_on_startup=*/false)};
  const memgraph::memory::DbArenaScope arena_scope{&db.Arena()};
  auto &storage = *static_cast<ms::InMemoryStorage *>(db.storage());
  const ms::InMemoryStorage::SuppressWalScope outer{storage};
  ASSERT_DEATH({ const ms::InMemoryStorage::SuppressWalScope inner{storage}; }, "not nestable");
}
