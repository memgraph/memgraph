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
#include <chrono>
#include <filesystem>
#include <optional>
#include <span>
#include <string>
#include <utility>
#include <vector>

#include <fmt/format.h>
#include <fmt/ranges.h>

#include "dbms/database.hpp"
#include "dbms/inmemory/history_replay.hpp"
#include "dbms/inmemory/replication_handlers.hpp"
#include "flags/general.hpp"
#include "memory/db_arena.hpp"
#include "metrics/prometheus_metrics.hpp"
#include "storage/v2/durability/durability.hpp"
#include "storage/v2/durability/paths.hpp"
#include "storage/v2/durability/wal.hpp"
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
const std::filesystem::path kDirMain{kRoot / "main"};
const std::filesystem::path kDirUnbounded{kRoot / "unbounded"};
const std::filesystem::path kDirB{kRoot / "b"};

ms::Config MakeDurableConfig(const std::filesystem::path &dir, bool recover_on_startup, bool narrowing = false) {
  ms::Config config;
  config.gc = {.type = ms::Config::Gc::Type::PERIODIC, .interval = std::chrono::hours(24)};
  config.durability = {.storage_directory = dir,
                       .recover_on_startup = recover_on_startup,
                       .snapshot_wal_mode = ms::Config::Durability::SnapshotWalMode::PERIODIC_SNAPSHOT_WITH_WAL,
                       .snapshot_interval = memgraph::utils::SchedulerInterval{std::chrono::hours(24)},
                       .wal_file_flush_every_n_tx = 1};
  config.salient.items.properties_on_edges = true;
  config.experimental_commit_lock_narrowing = narrowing;
  return config;
}

uint64_t Ldt(ms::InMemoryStorage const &storage) {
  return storage.repl_storage_state_.commit_ts_info_.load(std::memory_order_acquire).ldt_;
}

// A recovered database plus the tail that bounded recovery left out.
struct Recovered {
  explicit Recovered(const std::filesystem::path &dir, std::optional<uint64_t> bound = std::nullopt,
                     bool narrowing = false)
      : registration{memgraph::metrics::Metrics().AddDatabase(utils_uuid, "versioning_history_replay")},
        storage{MakeDurableConfig(dir, /*recover_on_startup=*/true, narrowing),
                std::nullopt,
                std::make_unique<ms::PlanInvalidatorDefault>(),
                registration.handles(),
                nullptr,
                nullptr,
                nullptr,
                ms::durability::HistoryBounds{.recovery_bound = bound}},
        tail{storage.TakeRecoveryTail()} {}

  double UnreleasedDeltas() { return registration.handles().unreleased_delta_objects.Value(); }

  memgraph::utils::UUID utils_uuid;
  memgraph::metrics::PrometheusMetrics::Registration registration;
  ms::InMemoryStorage storage;
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

std::string DumpLatest(ms::Storage &storage) {
  auto acc = storage.Access(ms::READ);
  return DumpGraph(*acc);
}

void Commit(ms::Storage::Accessor &acc) {
  ASSERT_TRUE(acc.PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()).has_value());
}

void CommitVertex(ms::Storage &storage) {
  auto acc = storage.Access(ms::WRITE);
  acc->CreateVertex();
  Commit(*acc);
}

// Enough GC passes to reclaim everything no pin or transaction still needs.
void Gc(ms::InMemoryStorage &storage) {
  for (int i = 0; i < 6; ++i) storage.FreeMemory({}, false);
}

void FreshCopy(const std::filesystem::path &from, const std::filesystem::path &to) {
  std::filesystem::remove_all(to);
  std::filesystem::copy(from, to, std::filesystem::copy_options::recursive);
}

// (path, to_timestamp) of each WAL file under `dir`, oldest first.
std::vector<std::pair<std::filesystem::path, uint64_t>> WalFiles(const std::filesystem::path &dir) {
  auto const files = memgraph::storage::durability::GetWalFiles(dir / memgraph::storage::durability::kWalDirectory);
  EXPECT_TRUE(files.has_value());
  std::vector<std::pair<std::filesystem::path, uint64_t>> out;
  if (files) {
    for (auto const &info : *files) out.emplace_back(info.path, info.to_timestamp);
  }
  return out;
}

// Main history in `dir`: write i adds vertex {p: i} with an edge from the previous vertex and updates that vertex; a
// read-only commit after each write makes the durable timestamps sparse. Later writes also cover the other delta kinds:
// a label swap to a name first seen in the tail (write 2), an edge delete (3), a detach delete (4) and an index DDL
// after write 3. Returns each write's durable timestamp.
std::vector<uint64_t> WriteMainHistory(const std::filesystem::path &dir, int writes) {
  memgraph::dbms::Database db{MakeDurableConfig(dir, /*recover_on_startup=*/false)};
  const memgraph::memory::DbArenaScope arena_scope{&db.Arena()};
  auto &storage = *static_cast<ms::InMemoryStorage *>(db.storage());
  auto const label = storage.NameToLabel("L");
  auto const p = storage.NameToProperty("p");
  auto const type = storage.NameToEdgeType("T");
  std::vector<uint64_t> durable;
  std::vector<ms::Gid> vertices;
  std::vector<ms::Gid> edges;
  for (int i = 0; i < writes; ++i) {
    {
      auto acc = db.Access(ms::WRITE);
      if (i == 2) {
        auto prev = acc->FindVertex(vertices.back(), ms::View::OLD);
        EXPECT_TRUE(prev->RemoveLabel(label).has_value());
        EXPECT_TRUE(prev->AddLabel(storage.NameToLabel("M")).has_value());
      }
      if (i == 3) {
        auto edge = acc->FindEdge(edges[1], ms::View::OLD);
        EXPECT_TRUE(edge.has_value());
        EXPECT_TRUE(acc->DeleteEdge(&*edge).has_value());
      }
      if (i == 4) {  // takes the remaining in-edge with it
        auto victim = acc->FindVertex(vertices[1], ms::View::OLD);
        EXPECT_TRUE(victim.has_value());
        EXPECT_TRUE(acc->DetachDeleteVertex(&*victim).has_value());
      }
      auto v = acc->CreateVertex();
      EXPECT_TRUE(v.AddLabel(label).has_value());
      EXPECT_TRUE(v.SetProperty(p, ms::PropertyValue(i)).has_value());
      if (!vertices.empty()) {
        auto from = acc->FindVertex(vertices.back(), ms::View::NEW);
        EXPECT_TRUE(from->SetProperty(p, ms::PropertyValue(10 * i)).has_value());
        auto edge = acc->CreateEdge(&*from, &v, type);
        EXPECT_TRUE(edge->SetProperty(p, ms::PropertyValue(i)).has_value());
        edges.push_back(edge->Gid());
      }
      vertices.push_back(v.Gid());
      Commit(*acc);
    }
    durable.push_back(Ldt(storage));
    if (i == 3) {
      auto acc = db.ReadOnlyAccess();
      EXPECT_TRUE(acc->CreateIndex(label, {ms::PropertyPath{p}}, ms::IndexOrder::ASC).has_value());
      Commit(*acc);
    }
    auto acc = db.Access(ms::READ);
    Commit(*acc);
  }
  return durable;
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

// A failover inside the replayed tail must leave the same epoch and epoch history as an unbounded recovery (replicas
// compare both on reconnect); pins for a fork at the recovered state and one inside the tail keep their state readable.
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

    CommitVertex(*storage);
    fork_ts = storage->RegisterForkPin();
    storage->ReleaseForkPin(fork_ts);
    CommitVertex(*storage);
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
    CommitVertex(*storage);
    tail_fork_ts = storage->RegisterForkPin();
    storage->ReleaseForkPin(tail_fork_ts);
    CommitVertex(*storage);
  }

  FreshCopy(kDirMain, kDirUnbounded);

  auto const capture = [](ms::InMemoryStorage &s) {
    return std::pair{std::string(s.repl_storage_state_.epoch_.id()), s.repl_storage_state_.history};
  };
  std::pair<std::string, ms::EpochHistory> expected;
  {
    memgraph::dbms::Database db{MakeDurableConfig(kDirUnbounded, /*recover_on_startup=*/true)};
    const memgraph::memory::DbArenaScope arena_scope{&db.Arena()};
    expected = capture(*static_cast<ms::InMemoryStorage *>(db.storage()));
  }
  ASSERT_EQ(expected.first, second_epoch);
  ASSERT_GE(expected.second.size(), 2U);
  EXPECT_EQ(expected.second.front().first, first_epoch);
  EXPECT_EQ(expected.second.back().first, second_epoch);

  // Bounded recovery below the fork; the rest, including the second-epoch WAL files, is replayed from the tail.
  Recovered r{kDirMain, fork_ts - 1};
  ASSERT_TRUE(r.tail.has_value());
  const std::array forks{fork_ts, tail_fork_ts};
  auto pins = memgraph::dbms::ReplayHistoryTail(r.storage, *r.tail, forks);
  ASSERT_TRUE(pins.has_value()) << pins.error();
  ASSERT_EQ(pins->size(), 2U);

  EXPECT_EQ(capture(r.storage), expected);

  {
    auto acc = r.storage.Access(ms::READ);
    EXPECT_EQ(CountVertices(*acc, ms::View::OLD), 4U);
  }
  for (auto const [pin, visible] : {std::pair{(*pins)[0], 1U}, std::pair{(*pins)[1], 3U}}) {
    auto historical = r.storage.HistoricalAccess(pin);
    ASSERT_TRUE(historical.has_value());
    EXPECT_EQ(CountVertices(**historical, ms::View::OLD), visible) << "a fork sees only the commits before it";
  }
  for (auto const pin : *pins) r.storage.ReleaseForkPin(pin);
}

// A fork at or before the recovered state cannot be pinned without leaking later commits into it: refused, holding no
// pin, and the tail stays replayable (an empty fork list replays all of it). Fork 0 is past an empty recovered state.
TEST_F(VersioningHistoryReplayTest, RefusesForkAtOrBeforeTheRecoveredState) {
  {
    Recovered empty{kDirB};
    ASSERT_EQ(Ldt(empty.storage), 0U);
    auto pins = memgraph::dbms::ReplayHistoryTail(
        empty.storage, {.wal_files = {}, .reached_timestamp = 0}, std::array<uint64_t, 1>{0});
    ASSERT_TRUE(pins.has_value()) << pins.error();
    CommitVertex(empty.storage);
    EXPECT_EQ(DumpAt(empty.storage, pins->front()), "") << "a fork before the first commit sees nothing";
    empty.storage.ReleaseForkPin(pins->front());
  }

  auto const durable = WriteMainHistory(kDirMain, 3);
  Recovered r{kDirMain, durable[0]};
  ASSERT_TRUE(r.tail.has_value());
  ASSERT_EQ(r.tail->reached_timestamp, durable[0]);

  auto refused = memgraph::dbms::ReplayHistoryTail(r.storage, *r.tail, std::array{durable[0]});
  EXPECT_FALSE(refused.has_value());
  EXPECT_FALSE(r.storage.OldestForkPin().has_value());
  EXPECT_EQ(Ldt(r.storage), durable[0]) << "nothing replayed";

  auto pins = memgraph::dbms::ReplayHistoryTail(r.storage, *r.tail, std::span<const uint64_t>{});
  ASSERT_TRUE(pins.has_value()) << pins.error();
  EXPECT_TRUE(pins->empty());
  EXPECT_EQ(Ldt(r.storage), durable.back());
  CommitVertex(r.storage);
  EXPECT_GT(Ldt(r.storage), durable.back());
}

// Duplicates each get their own pin; with no WAL to replay every pin is taken at the recovered state, and the clock
// moves to the newest fork so later commits cannot fall below any of them. The ids it skips are marked finished, so GC
// still reclaims once the pins are gone.
TEST_F(VersioningHistoryReplayTest, EmptyTailPinsEveryForkAndBumpsTheClock) {
  auto const durable = WriteMainHistory(kDirMain, 2);
  Recovered r{kDirMain};
  auto const reached = Ldt(r.storage);
  ASSERT_EQ(reached, durable.back());
  auto const expected = DumpLatest(r.storage);

  ms::durability::RecoveryTail const tail{.wal_files = {}, .reached_timestamp = reached};
  auto const far = reached + 100;
  auto pins = memgraph::dbms::ReplayHistoryTail(r.storage, tail, std::array{reached + 1, reached + 1, far});
  ASSERT_TRUE(pins.has_value()) << pins.error();
  ASSERT_EQ(pins->size(), 3U);
  EXPECT_EQ((*pins)[0], (*pins)[1]);
  EXPECT_EQ(r.storage.LastCommittedMvccTimestamp(), far - 1);
  r.storage.BumpClockPastDurable(10);
  EXPECT_EQ(r.storage.LastCommittedMvccTimestamp(), far - 1) << "a lower target leaves the clock alone";

  CommitVertex(r.storage);
  EXPECT_GE(Ldt(r.storage), far);
  r.storage.ReleaseForkPin((*pins)[0]);
  for (auto const pin : {(*pins)[1], (*pins)[2]}) EXPECT_EQ(DumpAt(r.storage, pin), expected);

  Gc(r.storage);
  EXPECT_GT(r.UnreleasedDeltas(), 0) << "the pins must keep the new commit's deltas";
  r.storage.ReleaseForkPin((*pins)[1]);
  r.storage.ReleaseForkPin((*pins)[2]);
  EXPECT_FALSE(r.storage.OldestForkPin().has_value());
  Gc(r.storage);
  EXPECT_EQ(r.UnreleasedDeltas(), 0) << "skipped ids must not hold GC back";
}

// Bounded recovery + tail replay ends where an unbounded recovery of the same files does, and each fork sees exactly
// the commits below it: at the recovered state, exactly at the first replayed commit, in a gap, and past the end of the
// tail, with commit lock narrowing off and on. Replay leaves the WAL files alone and appends resume afterwards.
TEST_F(VersioningHistoryReplayTest, ReplayMatchesUnboundedRecovery) {
  auto const durable = WriteMainHistory(kDirMain, 6);
  FreshCopy(kDirMain, kDirUnbounded);

  std::string unbounded_graph;
  uint64_t unbounded_ldt{0};
  uint64_t unbounded_committed{0};
  uint64_t unbounded_next{0};
  {
    Recovered unbounded{kDirUnbounded};
    unbounded_graph = DumpLatest(unbounded.storage);
    auto const info = unbounded.storage.repl_storage_state_.commit_ts_info_.load();
    unbounded_ldt = info.ldt_;
    unbounded_committed = info.num_committed_txns_;
    CommitVertex(unbounded.storage);
    unbounded_next = Ldt(unbounded.storage);
  }
  std::string at_gap;  // the graph after writes [0, 2]
  {
    FreshCopy(kDirMain, kDirB);
    Recovered bounded{kDirB, durable[2]};
    at_gap = DumpLatest(bounded.storage);
  }

  // Right after the recovered state (write 0), exactly at write 1's durable timestamp (the first replayed commit, which
  // the fork must not see), right after write 2 (a gap), and past the end of the tail.
  const std::array forks{durable[0] + 1, durable[1], durable[2] + 1, durable[5] + 1};
  for (bool const narrowing : {false, true}) {
    SCOPED_TRACE(fmt::format("commit lock narrowing: {}", narrowing));
    FreshCopy(kDirMain, kDirB);
    Recovered r{kDirB, durable[0], narrowing};
    ASSERT_TRUE(r.tail.has_value());
    // The one file straddles the bound: it holds commits on both sides of it.
    ASSERT_EQ(r.tail->wal_files.size(), 1U);
    auto const at_recovered = DumpLatest(r.storage);
    const std::array<std::string const *, 4> expected_at{&at_recovered, &at_recovered, &at_gap, &unbounded_graph};
    auto const wals_before = WalFiles(kDirB);

    auto pins = memgraph::dbms::ReplayHistoryTail(r.storage, *r.tail, forks);
    ASSERT_TRUE(pins.has_value()) << pins.error();
    ASSERT_EQ(pins->size(), forks.size());
    EXPECT_EQ(WalFiles(kDirB), wals_before) << "tail replay must not write the WAL";
    EXPECT_EQ(r.storage.LastCommittedMvccTimestamp() + 1, r.storage.timestamp_);

    EXPECT_EQ(DumpLatest(r.storage), unbounded_graph);
    auto const info = r.storage.repl_storage_state_.commit_ts_info_.load();
    EXPECT_EQ(info.ldt_, unbounded_ldt);
    EXPECT_EQ(info.num_committed_txns_, unbounded_committed);
    Gc(r.storage);
    EXPECT_GT(r.UnreleasedDeltas(), 0) << "the pins must keep the replayed history";
    for (size_t i = 0; i < forks.size(); ++i) EXPECT_EQ(DumpAt(r.storage, (*pins)[i]), *expected_at[i]) << "fork " << i;

    CommitVertex(r.storage);
    EXPECT_EQ(Ldt(r.storage), unbounded_next) << "the next commit lands where it would after an unbounded recovery";
    EXPECT_EQ(WalFiles(kDirB).back().second, Ldt(r.storage)) << "appends resume after replay";
    for (auto const pin : *pins) r.storage.ReleaseForkPin(pin);
    Gc(r.storage);
    EXPECT_EQ(r.UnreleasedDeltas(), 0) << "history is reclaimable without the pins";
  }
}

// A transaction main aborted after writing it to the WAL (2PC, commit flag false) is skipped, and the pin of a fork at
// its timestamp is still taken before the next applied transaction. The same files copied while the newest WAL was
// still unfinalized (main running) replay to the same graph.
TEST_F(VersioningHistoryReplayTest, AbortedWalTransactionIsSkipped) {
  uint64_t first{0};
  uint64_t aborted{0};
  uint64_t last{0};
  std::string expected;
  {
    memgraph::dbms::Database db{MakeDurableConfig(kDirMain, /*recover_on_startup=*/false)};
    const memgraph::memory::DbArenaScope arena_scope{&db.Arena()};
    auto &storage = *static_cast<ms::InMemoryStorage *>(db.storage());
    CommitVertex(storage);
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
    CommitVertex(storage);
    last = Ldt(storage);
    ASSERT_GT(last, aborted);
    expected = DumpLatest(storage);
    std::filesystem::copy(kDirMain, kDirB, std::filesystem::copy_options::recursive);
  }

  for (auto const &dir : {kDirMain, kDirB}) {
    SCOPED_TRACE(dir.string());
    Recovered r{dir, first};
    ASSERT_TRUE(r.tail.has_value());
    auto pins = memgraph::dbms::ReplayHistoryTail(r.storage, *r.tail, std::array{aborted, aborted + 1, last});
    ASSERT_TRUE(pins.has_value()) << pins.error();
    EXPECT_EQ(DumpLatest(r.storage), expected) << "the aborted transaction is not applied";
    for (auto const pin : *pins) {
      auto historical = r.storage.HistoricalAccess(pin);
      ASSERT_TRUE(historical.has_value());
      EXPECT_EQ(CountVertices(**historical, ms::View::OLD), 1U) << "every fork is before the last commit";
    }
    for (auto const pin : *pins) r.storage.ReleaseForkPin(pin);
  }
}

// A tail WAL that cannot be replayed fails the call and releases every pin taken so far. A delta count that does not
// match the file is rejected by ReplayWalFile; halving the newest WAL keeps its header summary, so there the failure is
// a delta decode error thrown out of replay.
TEST_F(VersioningHistoryReplayTest, CorruptTailWalFailsWithoutPins) {
  auto const durable = WriteMainHistory(kDirMain, 3);
  std::filesystem::path wal;
  {
    Recovered r{kDirMain, durable[0]};
    ASSERT_TRUE(r.tail.has_value());
    ASSERT_FALSE(r.tail->wal_files.empty());
    wal = r.tail->wal_files.back();
    auto info = ms::durability::ReadWalContents(wal);
    ASSERT_GT(info.num_deltas, 1U);
    --info.num_deltas;
    const ms::InMemoryStorage::SuppressWalScope no_wal{r.storage};
    EXPECT_FALSE(memgraph::dbms::InMemoryReplicationHandlers::ReplayWalFile(wal, info, &r.storage, [](uint64_t) {}));
  }

  // Bounded recovery reads the straddling WAL, so it runs on the intact file and only then is the tail file halved.
  Recovered r{kDirMain, durable[0]};
  ASSERT_TRUE(r.tail.has_value());
  std::filesystem::resize_file(wal, std::filesystem::file_size(wal) / 2);
  auto pins = memgraph::dbms::ReplayHistoryTail(r.storage, *r.tail, std::array{durable[0] + 1, durable[2] + 1});
  ASSERT_FALSE(pins.has_value());
  EXPECT_THAT(pins.error(), ::testing::HasSubstr("threw"));
  EXPECT_FALSE(r.storage.OldestForkPin().has_value());
}
