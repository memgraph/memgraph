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
#include <array>
#include <atomic>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <filesystem>
#include <fstream>
#include <memory>
#include <optional>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include "dbms/database.hpp"
#include "flags/general.hpp"
#include "kvstore/kvstore.hpp"
#include "memory/db_arena.hpp"
#include "storage/v2/constraints/constraints.hpp"
#include "storage/v2/durability/durability.hpp"
#include "storage/v2/durability/exceptions.hpp"
#include "storage/v2/durability/marker.hpp"
#include "storage/v2/durability/metadata.hpp"
#include "storage/v2/durability/paths.hpp"
#include "storage/v2/durability/snapshot.hpp"
#include "storage/v2/durability/wal.hpp"
#include "storage/v2/indices/indices.hpp"
#include "storage/v2/inmemory/storage.hpp"
#include "storage/v2/property_value.hpp"
#include "storage/v2/replication/replication_storage_state.hpp"
#include "tests/test_commit_args_helper.hpp"
#include "tests/versioning_durable_files.hpp"
#include "tests/versioning_ledger_kv.hpp"
#include "utils/exceptions.hpp"
#include "utils/file.hpp"
#include "utils/scheduler.hpp"
#include "utils/uuid.hpp"
#include "versioning/branch_ledger.hpp"
#include "versioning/version_store.hpp"

namespace ms = memgraph::storage;
namespace md = memgraph::storage::durability;

namespace {

const std::filesystem::path kRoot{std::filesystem::temp_directory_path() / "versioning_bounded_recovery"};
const std::filesystem::path kKvRoot{std::filesystem::temp_directory_path() / "versioning_bounded_recovery_kv"};

// The DBMS durability KVStore that holds the branch ledger, shared by the databases of one test.
const memgraph::utils::UUID kDbUuid;

memgraph::tests::LedgerKv ledger_kv{kKvRoot};

std::string ReadLedger() { return ledger_kv.Get()->Get(ledger_kv.Key(kDbUuid)).value_or(""); }

void WriteLedger(const std::string &value) { ASSERT_TRUE(ledger_kv.Get()->Put(ledger_kv.Key(kDbUuid), value)); }

// Small WAL files rotate every few commits, so a history spans several of them.
constexpr uint64_t kSmallWalKiB = 1;
constexpr uint64_t kDefaultWalKiB = 20 * 1024;

ms::Config MakeConfig(bool recover, bool versioning = true, uint64_t wal_kib = kDefaultWalKiB) {
  ms::Config config;
  config.experimental_versioning = versioning;
  config.gc = {.type = ms::Config::Gc::Type::PERIODIC, .interval = std::chrono::hours(24)};
  config.durability = {.storage_directory = kRoot,
                       .recover_on_startup = recover,
                       .snapshot_wal_mode = ms::Config::Durability::SnapshotWalMode::PERIODIC_SNAPSHOT_WITH_WAL,
                       .snapshot_interval = memgraph::utils::SchedulerInterval{std::chrono::hours(24)},
                       .snapshot_retention_count = 100,
                       .wal_file_size_kibibytes = wal_kib,
                       .wal_file_flush_every_n_tx = 1};
  config.salient.items.properties_on_edges = true;
  config.salient.uuid = kDbUuid;
  return config;
}

// A Database wired to the test's ledger store.
struct TestDatabase : memgraph::dbms::Database {
  explicit TestDatabase(ms::Config config) : memgraph::dbms::Database(std::move(config), ledger_kv.Get()) {}
};

// Makes the vertex section undecodable while the header still reads, so the file is listed and then fails to load.
void CorruptSnapshot(std::filesystem::path const &path) {
  auto const info = md::ReadSnapshotInfo(path);
  memgraph::utils::OutputFile file;
  ASSERT_TRUE(file.Open(path, memgraph::utils::OutputFile::Mode::OVERWRITE_EXISTING));
  file.SetPosition(memgraph::utils::OutputFile::Position::SET, info.offset_vertices);
  auto const value = static_cast<uint8_t>(md::Marker::TYPE_MAP);
  ASSERT_TRUE(file.Write(&value, sizeof(value)));
  ASSERT_TRUE(file.Sync());
  file.Close();
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

  // Replays the tail resuming from the recovered durable timestamp (deltas at or below it are skipped, so a
  // straddling first file is safe). Index/constraint metadata is accumulated but not materialized.
  void ReplayTail(md::RecoveryTail const &tail) {
    std::optional<uint64_t> last_loaded = tail.reached_timestamp;
    for (auto const &path : tail.wal_files) {
      auto info = md::LoadWal(
          path,
          &tail_metadata_,
          last_loaded,
          &vertices,
          &edges,
          &name_id_mapper,
          &edge_count,
          config.salient.items,
          &enum_store,
          nullptr /* schema_info */,
          [](auto) { return std::nullopt; },
          &ttl,
          nullptr /* description_store */);
      EXPECT_TRUE(info.has_value()) << "tail WAL file " << path << " failed to replay";
      if (info) last_loaded = info->last_durable_timestamp;
    }
  }

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
  md::RecoveredIndicesAndConstraints tail_metadata_;
};

// kCommits transactions, each creating vertex i with p = i and setting vertex 1's p to i, plus a label index on :L
// after commit kIndexAfter. Vertex i is therefore visible as of a bound iff its commit is at or below it, and vertex
// 1's p says how many commits are.
struct History {
  static constexpr size_t kCommits = 16;
  static constexpr size_t kIndexAfter = 4;

  std::array<ms::Gid, kCommits> vertex{};
  std::array<uint64_t, kCommits> commit_ts{};
  uint64_t index_ts{};
  std::vector<uint64_t> snapshot_ts;
  uint64_t end_ts{};
};

// `snapshot_after` lists the commit numbers (1-based) after which a snapshot is taken.
void BuildHistory(History &h, std::vector<size_t> const &snapshot_after, uint64_t wal_kib) {
  TestDatabase db{MakeConfig(false, true, wal_kib)};
  const memgraph::memory::DbArenaScope arena_scope{&db.Arena()};
  auto *s = static_cast<ms::InMemoryStorage *>(db.storage());
  auto const p = s->NameToProperty("p");
  auto const pad = s->NameToProperty("pad");
  auto const l = s->NameToLabel("L");

  for (size_t i = 1; i <= History::kCommits; ++i) {
    {
      auto acc = db.Access(ms::WRITE);
      auto v = acc->CreateVertex();
      ASSERT_TRUE(v.SetProperty(p, ms::PropertyValue(static_cast<int64_t>(i))).has_value());
      ASSERT_TRUE(v.SetProperty(pad, ms::PropertyValue(std::string(300, 'x'))).has_value());
      h.vertex[i - 1] = v.Gid();
      if (i > 1) {
        auto v1 = acc->FindVertex(h.vertex[0], ms::View::OLD);
        ASSERT_TRUE(v1.has_value());
        ASSERT_TRUE(v1->SetProperty(p, ms::PropertyValue(static_cast<int64_t>(i))).has_value());
      }
      Commit(*acc);
      h.commit_ts[i - 1] = LastDurableTs();
    }
    if (i == History::kIndexAfter) {
      auto acc = db.ReadOnlyAccess();
      ASSERT_TRUE(acc->CreateIndex(l).has_value());
      Commit(*acc);
      h.index_ts = LastDurableTs();
    }
    if (std::ranges::find(snapshot_after, i) != snapshot_after.end()) {
      ASSERT_TRUE(s->CreateSnapshot().has_value());
    }
  }
  h.end_ts = std::max(h.commit_ts.back(), h.index_ts);

  auto const snapshots = md::GetSnapshotFiles(kRoot / md::kSnapshotDirectory);
  ASSERT_TRUE(snapshots.has_value());
  ASSERT_EQ(snapshots->size(), snapshot_after.size());
  for (auto const &info : *snapshots) h.snapshot_ts.push_back(info.durable_timestamp);
  std::ranges::sort(h.snapshot_ts);
}

// The WAL files on disk, in sequence order.
std::vector<md::WalDurabilityInfo> Wals() {
  auto wals = md::GetWalFiles(kRoot / md::kWalDirectory);
  EXPECT_TRUE(wals.has_value());
  if (!wals) return {};
  std::ranges::sort(*wals, {}, &md::WalDurabilityInfo::seq_num);
  return std::move(*wals);
}

// Removes the WAL files that recovery from the snapshot at `snapshot_ts` does not need (all but the last one starting
// at or before it, and the ones after), so that only a recovery that really starts from that snapshot still works.
void DropWalPrefixCoveredBy(uint64_t snapshot_ts) {
  auto const wals = Wals();
  auto const past = std::ranges::find_if(wals, [&](auto const &w) { return w.from_timestamp > snapshot_ts; });
  auto const dropped = static_cast<size_t>(std::max<std::ptrdiff_t>(past - wals.begin() - 1, 0));
  ASSERT_GT(dropped, 0U) << "the history must have WAL files before the snapshot";
  for (size_t i = 0; i < dropped; ++i) ASSERT_TRUE(std::filesystem::remove(wals[i].path));
}

// Vertex and property reads of a recovered state, so one check serves both raw recovery and a recovered storage.
struct StorageReader {
  ms::Storage &storage;

  size_t VertexCount() {
    auto acc = storage.Access(ms::READ);
    size_t count = 0;
    for ([[maybe_unused]] auto v : acc->Vertices(ms::View::OLD)) ++count;
    return count;
  }

  std::optional<ms::PropertyValue> Property(ms::Gid gid, std::string_view name) {
    auto acc = storage.Access(ms::READ);
    auto v = acc->FindVertex(gid, ms::View::OLD);
    if (!v) return std::nullopt;
    auto value = v->GetProperty(storage.NameToProperty(name), ms::View::OLD);
    if (!value) return std::nullopt;
    return *value;
  }
};

// The recovered graph is exactly the history as of `bound`.
template <typename Reader>
void ExpectStateAsOf(Reader &r, History const &h, uint64_t bound) {
  auto const visible = static_cast<size_t>(std::ranges::count_if(h.commit_ts, [&](auto ts) { return ts <= bound; }));
  EXPECT_EQ(r.VertexCount(), visible) << "bound " << bound;
  for (size_t i = 0; i < h.vertex.size(); ++i) {
    std::optional<ms::PropertyValue> expected;
    if (i < visible) expected = ms::PropertyValue(static_cast<int64_t>(i == 0 ? visible : i + 1));
    EXPECT_EQ(r.Property(h.vertex[i], "p"), expected) << "vertex " << i + 1 << ", bound " << bound;
  }
}

// The newest durable timestamp in the history at or below `bound`: what recovery has reached.
uint64_t ReachedAsOf(History const &h, uint64_t bound) {
  uint64_t reached = 0;
  auto const note = [&](uint64_t ts) {
    if (ts <= bound) reached = std::max(reached, ts);
  };
  std::ranges::for_each(h.commit_ts, note);
  std::ranges::for_each(h.snapshot_ts, note);
  note(h.index_ts);
  return reached;
}

// Recovers with `bound` from the files on disk and checks everything it promises: the state as of the bound, the
// index only if its definition is at or below it, the tail being exactly the WAL files with a delta above the bound,
// and replaying that tail landing on the full history. Returns the tail for the caller's own checks.
md::RecoveryTail RecoverAndCheck(History const &h, uint64_t bound) {
  SCOPED_TRACE("bound " + std::to_string(bound));
  Recovered r{MakeConfig(true)};
  md::RecoveryTail tail;
  EXPECT_TRUE(r.Recover(bound, &tail).has_value());

  ExpectStateAsOf(r, h, bound);
  EXPECT_EQ(
      r.indices.label_index_->GetActiveIndices()->IndexExists(ms::LabelId::FromUint(r.name_id_mapper.NameToId("L"))),
      h.index_ts <= bound);

  std::vector<std::filesystem::path> expected_tail;
  for (auto const &wal : Wals()) {
    if (wal.to_timestamp > bound) expected_tail.push_back(wal.path);
  }
  EXPECT_EQ(tail.wal_files, expected_tail);
  EXPECT_EQ(tail.reached_timestamp, ReachedAsOf(h, bound));

  r.ReplayTail(tail);
  ExpectStateAsOf(r, h, h.end_ts);
  return tail;
}

void ExpectRecoveryFailure(std::optional<uint64_t> bound, std::string_view message_part) {
  Recovered r{MakeConfig(true)};
  md::RecoveryTail tail;
  try {
    static_cast<void>(r.Recover(bound, &tail));
    ADD_FAILURE() << "expected RecoveryFailure";
  } catch (const md::RecoveryFailure &e) {
    EXPECT_NE(std::string_view{e.what()}.find(message_part), std::string_view::npos) << e.what();
  }
}

}  // namespace

class VersioningBoundedRecoveryTest : public ::testing::Test {
 protected:
  void SetUp() override {
    FLAGS_metrics_format = "OpenMetrics";
    ledger_kv.Reset();
    std::filesystem::remove_all(kRoot);
  }

  void TearDown() override {
    ledger_kv.Reset();
    std::filesystem::remove_all(kRoot);
  }

  // Two snapshots among sixteen commits, in WAL files small enough to rotate every few of them.
  static void BuildWithSnapshots(History &h) { BuildHistory(h, {5, 12}, kSmallWalKiB); }
};

// No snapshot at all: the WAL chain from sequence 0 rebuilds the state. The bound is the index definition itself.
TEST_F(VersioningBoundedRecoveryTest, NoSnapshotRecoversFromTheWalChain) {
  History h;
  ASSERT_NO_FATAL_FAILURE(BuildHistory(h, {}, kSmallWalKiB));
  ASSERT_GE(Wals().size(), 4U);
  RecoverAndCheck(h, h.index_ts);
}

// The newest snapshot at or below the bound is the base: with the WAL before it gone, only that snapshot (not an
// older one, not the WAL chain) can serve.
TEST_F(VersioningBoundedRecoveryTest, NewestSnapshotBelowTheBoundIsTheBase) {
  History h;
  ASSERT_NO_FATAL_FAILURE(BuildWithSnapshots(h));
  ASSERT_EQ(h.snapshot_ts.size(), 2U);
  ASSERT_NO_FATAL_FAILURE(DropWalPrefixCoveredBy(h.snapshot_ts[1]));
  RecoverAndCheck(h, h.commit_ts[13]);
}

// Every snapshot is past the bound: the WAL chain from sequence 0 rebuilds the state.
TEST_F(VersioningBoundedRecoveryTest, AllSnapshotsPastTheBoundRecoverFromTheWalChain) {
  History h;
  ASSERT_NO_FATAL_FAILURE(BuildWithSnapshots(h));
  RecoverAndCheck(h, h.commit_ts[1]);
}

// Every snapshot is past the bound and there is no WAL to rebuild from: recovery fails instead of coming up empty.
TEST_F(VersioningBoundedRecoveryTest, AllSnapshotsPastTheBoundWithoutWalFail) {
  {
    auto config = MakeConfig(false);
    config.durability.snapshot_wal_mode = ms::Config::Durability::SnapshotWalMode::PERIODIC_SNAPSHOT;
    TestDatabase db{config};
    const memgraph::memory::DbArenaScope arena_scope{&db.Arena()};
    auto acc = db.Access(ms::WRITE);
    acc->CreateVertex();
    Commit(*acc);
    ASSERT_TRUE(static_cast<ms::InMemoryStorage *>(db.storage())->CreateSnapshot().has_value());
  }
  auto const snapshots = md::GetSnapshotFiles(kRoot / md::kSnapshotDirectory);
  ASSERT_TRUE(snapshots.has_value() && snapshots->size() == 1U);

  ExpectRecoveryFailure(snapshots->front().durable_timestamp - 1,
                        "No snapshot at or below the recovery bound and no WAL files to recover from!");
}

// Every snapshot is past the bound and the WAL chain does not start at sequence 0: the existing missing-prefix
// failure, not a partial recovery.
TEST_F(VersioningBoundedRecoveryTest, AllSnapshotsPastTheBoundWithMissingWalPrefixFail) {
  History h;
  ASSERT_NO_FATAL_FAILURE(BuildWithSnapshots(h));
  auto const wals = Wals();
  ASSERT_GE(wals.size(), 2U);
  ASSERT_EQ(wals.front().seq_num, 0U);
  ASSERT_TRUE(std::filesystem::remove(wals.front().path));
  ExpectRecoveryFailure(h.commit_ts[1], "There are missing prefix WAL files");
}

// A WAL file with deltas on both sides of the bound is loaded up to it and also tailed first; replaying the tail from
// the reached timestamp (RecoverAndCheck) completes it without double-applying.
TEST_F(VersioningBoundedRecoveryTest, WalFileStraddlingTheBoundIsLoadedAndTailed) {
  History h;
  ASSERT_NO_FATAL_FAILURE(BuildHistory(h, {}, kSmallWalKiB));
  auto const wals = Wals();
  ASSERT_GE(wals.size(), 3U);
  auto const &straddling = wals[1];
  ASSERT_LT(straddling.from_timestamp, straddling.to_timestamp);

  auto const tail = RecoverAndCheck(h, straddling.from_timestamp);
  ASSERT_FALSE(tail.wal_files.empty());
  EXPECT_EQ(tail.wal_files.front(), straddling.path);
}

// The bound is exactly a snapshot's durable timestamp: that snapshot is eligible (with the WAL before it gone, an
// older snapshot could not serve).
TEST_F(VersioningBoundedRecoveryTest, BoundEqualToSnapshotTimestampUsesThatSnapshot) {
  History h;
  ASSERT_NO_FATAL_FAILURE(BuildWithSnapshots(h));
  ASSERT_NO_FATAL_FAILURE(DropWalPrefixCoveredBy(h.snapshot_ts[1]));
  RecoverAndCheck(h, h.snapshot_ts[1]);
}

// The bound is exactly the last delta of a WAL file: that file is complete in the recovered state and not tailed, the
// next one is tail-only.
TEST_F(VersioningBoundedRecoveryTest, BoundEqualToWalFileEndLeavesTheNextFileAsTheTail) {
  History h;
  ASSERT_NO_FATAL_FAILURE(BuildHistory(h, {}, kSmallWalKiB));
  auto const wals = Wals();
  ASSERT_GE(wals.size(), 3U);
  ASSERT_GT(wals[2].from_timestamp, wals[1].to_timestamp);

  auto const tail = RecoverAndCheck(h, wals[1].to_timestamp);
  ASSERT_FALSE(tail.wal_files.empty());
  EXPECT_EQ(tail.wal_files.front(), wals[2].path);
}

// A bound at or past the end of the history recovers everything and leaves nothing to replay; the tail reports what was
// recovered, not the bound.
TEST_F(VersioningBoundedRecoveryTest, BoundAtOrPastTheEndLeavesNoTail) {
  History h;
  ASSERT_NO_FATAL_FAILURE(BuildWithSnapshots(h));
  for (auto const bound : {h.end_ts, h.end_ts + 100}) {
    auto const tail = RecoverAndCheck(h, bound);
    EXPECT_TRUE(tail.wal_files.empty());
    EXPECT_EQ(tail.reached_timestamp, h.end_ts);
  }
}

// A missing file inside the WAL chain is fatal even though a bound is set.
TEST_F(VersioningBoundedRecoveryTest, GapInTheWalSequenceFails) {
  History h;
  ASSERT_NO_FATAL_FAILURE(BuildHistory(h, {}, kSmallWalKiB));
  auto const wals = Wals();
  ASSERT_GE(wals.size(), 3U);
  ASSERT_TRUE(std::filesystem::remove(wals[1].path));
  ExpectRecoveryFailure(h.end_ts,
                        "You are missing a WAL file with the sequence number " + std::to_string(wals[1].seq_num));
}

// An eligible snapshot that fails to load is fatal, not a reason to fall back to the WAL chain; one past the bound is
// never read.
TEST_F(VersioningBoundedRecoveryTest, CorruptSnapshotIsFatalOnlyWhenEligible) {
  History h;
  ASSERT_NO_FATAL_FAILURE(BuildHistory(h, {8}, kSmallWalKiB));
  auto const snapshots = md::GetSnapshotFiles(kRoot / md::kSnapshotDirectory);
  ASSERT_TRUE(snapshots.has_value() && snapshots->size() == 1U);
  ASSERT_NO_FATAL_FAILURE(CorruptSnapshot(snapshots->front().path));

  for (auto const bound : {h.snapshot_ts.front(), h.end_ts}) {
    Recovered r{MakeConfig(true)};
    md::RecoveryTail tail;
    EXPECT_THROW(r.Recover(bound, &tail), md::RecoveryFailure) << "bound " << bound;
  }
  RecoverAndCheck(h, h.commit_ts[1]);
}

// The storage configuration drives bounded recovery and hands the tail out exactly once; a start without a bound
// recovers everything and has no tail.
TEST_F(VersioningBoundedRecoveryTest, ConfiguredBoundRecoversStateAtBoundAndHandsOutTail) {
  History h;
  ASSERT_NO_FATAL_FAILURE(BuildHistory(h, {}, kSmallWalKiB));
  auto const bound = h.commit_ts[9];
  std::vector<std::filesystem::path> expected_tail;
  for (auto const &wal : Wals()) {
    if (wal.to_timestamp > bound) expected_tail.push_back(wal.path);
  }
  ASSERT_FALSE(expected_tail.empty());

  {
    ms::InMemoryStorage s{MakeConfig(true),
                          std::nullopt,
                          std::make_unique<ms::PlanInvalidatorDefault>(),
                          memgraph::metrics::DatabaseMetricHandles{},
                          nullptr,
                          nullptr,
                          nullptr,
                          nullptr,
                          md::HistoryBounds{.floor = bound, .recover_only_to_floor = true}};
    StorageReader reader{s};
    ExpectStateAsOf(reader, h, bound);
    auto tail = s.TakeRecoveryTail();
    ASSERT_TRUE(tail.has_value());
    EXPECT_EQ(tail->reached_timestamp, bound);
    EXPECT_EQ(tail->wal_files, expected_tail);
    EXPECT_FALSE(s.TakeRecoveryTail().has_value()) << "the tail is moved out";
  }
  {
    TestDatabase db{MakeConfig(true)};
    const memgraph::memory::DbArenaScope arena_scope{&db.Arena()};
    StorageReader reader{*db.storage()};
    ExpectStateAsOf(reader, h, h.end_ts);
    EXPECT_FALSE(static_cast<ms::InMemoryStorage *>(db.storage())->TakeRecoveryTail().has_value());
  }
}

namespace {
// Main with three vertices and a branch "b" forked after the second; the branch has one file, standing in for its WAL.
std::filesystem::path CreateBranchedDatabase() {
  {
    TestDatabase db{MakeConfig(false)};
    const memgraph::memory::DbArenaScope arena_scope{&db.Arena()};
    for (int i = 0; i < 2; ++i) {
      auto acc = db.Access(ms::WRITE);
      acc->CreateVertex();
      Commit(*acc);
    }
    EXPECT_TRUE(db.version_store()->CreateBranch("b").has_value());
    auto acc = db.Access(ms::WRITE);
    acc->CreateVertex();
    Commit(*acc);
  }
  const auto read = memgraph::versioning::BranchLedger{*ledger_kv.Get(), kDbUuid}.Read();
  EXPECT_TRUE(read.has_value() && read->has_value());
  const auto marker = kRoot / "branches" / std::to_string((*read)->branches.at(0).info.id) / "wal" / "marker";
  std::filesystem::create_directories(marker.parent_path());
  std::ofstream{marker} << "x";
  return marker;
}
}  // namespace

// Versioning on in a storage mode that cannot host branches: main starts, the branches stay on disk untouched, and the
// next start in the supported mode finds them again.
TEST_F(VersioningBoundedRecoveryTest, AnAnalyticalStartupStartsAndLeavesTheBranchesOnDisk) {
  CreateBranchedDatabase();
  const auto files_before = memgraph::tests::ImageDurableFiles(kRoot);
  const auto ledger_before = ReadLedger();
  {
    auto config = MakeConfig(true);
    config.salient.storage_mode = ms::StorageMode::IN_MEMORY_ANALYTICAL;
    TestDatabase db{std::move(config)};
    EXPECT_EQ(db.version_store(), nullptr);
  }
  EXPECT_EQ(ReadLedger(), ledger_before);
  EXPECT_EQ(memgraph::tests::ImageDurableFiles(kRoot), files_before);

  TestDatabase db{MakeConfig(true)};
  ASSERT_NE(db.version_store(), nullptr);
  EXPECT_TRUE(db.version_store()->Exists("b"));
}

// Versioning off: the ledger is not looked at, as on master. No registry, no retention, no message; the files stay.
TEST_F(VersioningBoundedRecoveryTest, WithVersioningOffALedgerIsIgnored) {
  const auto marker = CreateBranchedDatabase();
  const auto ledger_before = ReadLedger();
  ASSERT_FALSE(ledger_before.empty());

  for (const bool readable : {true, false}) {
    if (!readable) WriteLedger("not a ledger");
    TestDatabase db{MakeConfig(true, /*versioning=*/false)};
    const memgraph::memory::DbArenaScope arena_scope{&db.Arena()};
    EXPECT_EQ(db.Access(ms::READ)->ApproximateVertexCount(), 3U);
    EXPECT_EQ(db.version_store(), nullptr);
    EXPECT_FALSE(static_cast<ms::InMemoryStorage *>(db.storage())->TakeRecoveryTail().has_value());
    EXPECT_EQ(db.storage()->RetentionFloor(), ms::Storage::kNoRetentionFloor);
  }
  EXPECT_EQ(ReadLedger(), "not a ledger");
  EXPECT_TRUE(std::filesystem::exists(marker));
}

// An unreadable ledger stops the database from starting, naming the key and the store; the ledger and the branch
// files are left as they are, and the restored ledger brings the branch back.
TEST_F(VersioningBoundedRecoveryTest, AnUnreadableLedgerStopsTheDatabaseAndLeavesItUntouched) {
  const auto marker = CreateBranchedDatabase();
  const auto good_ledger = ReadLedger();
  WriteLedger("not a ledger");

  auto config = MakeConfig(true);
  config.salient.name = std::string{"tenant1"};
  try {
    TestDatabase db{std::move(config)};
    FAIL() << "the database started with an unreadable ledger";
  } catch (const memgraph::utils::BasicException &e) {
    EXPECT_THAT(e.what(), ::testing::HasSubstr(ledger_kv.Key(kDbUuid)));
    EXPECT_THAT(e.what(), ::testing::HasSubstr("databases/.durability"));
    EXPECT_THAT(e.what(), ::testing::HasSubstr("tenant1"));
  }
  EXPECT_EQ(ReadLedger(), "not a ledger");
  EXPECT_TRUE(std::filesystem::exists(marker));

  WriteLedger(good_ledger);
  TestDatabase db{MakeConfig(true)};
  ASSERT_NE(db.version_store(), nullptr);
  EXPECT_TRUE(db.version_store()->Exists("b")) << "the restored ledger brings the branch back";
}
