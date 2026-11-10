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
#include <spdlog/sinks/base_sink.h>
#include <spdlog/spdlog.h>

#include <algorithm>
#include <atomic>
#include <chrono>
#include <cstdint>
#include <filesystem>
#include <fstream>
#include <functional>
#include <initializer_list>
#include <iterator>
#include <memory>
#include <optional>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include "dbms/database.hpp"
#include "flags/general.hpp"
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
#include "tests/versioning_startup_refusal.hpp"
#include "utils/file.hpp"
#include "utils/scheduler.hpp"
#include "utils/uuid.hpp"
#include "versioning/branch_ledger.hpp"
#include "versioning/version_store.hpp"

namespace ms = memgraph::storage;
namespace md = memgraph::storage::durability;

namespace {

const std::filesystem::path kRoot{std::filesystem::temp_directory_path() / "versioning_bounded_recovery"};

ms::Config MakeConfig(bool recover, bool versioning = true, std::optional<uint64_t> bound = std::nullopt) {
  ms::Config config;
  config.experimental_versioning = versioning;
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
  {  // Past the last commit: the tail reports what was recovered, not the bound.
    auto [r, tail] = recover(ts_c6 + 100);
    EXPECT_EQ(r->VertexCount(), 3U);
    EXPECT_EQ(tail.reached_timestamp, ts_c6);
    EXPECT_TRUE(tail.wal_files.empty());
  }
  {  // No bound: full recovery, and no tail is reported.
    auto r = std::make_unique<Recovered>(MakeConfig(true));
    md::RecoveryTail tail;
    ASSERT_TRUE(r->Recover(std::nullopt, &tail).has_value());
    EXPECT_EQ(r->VertexCount(), 3U);
    EXPECT_EQ(r->Property(v1, "p"), ms::PropertyValue(100));
    EXPECT_TRUE(tail.wal_files.empty());
  }
  {  // Replaying the returned tail must land on the state an unbounded recovery produces.
    auto [r, tail] = recover(ts_c3);
    ASSERT_FALSE(tail.wal_files.empty());
    r->ReplayTail(tail);
    EXPECT_EQ(r->VertexCount(), 3U);
    EXPECT_EQ(r->Property(v1, "p"), ms::PropertyValue(100));
    EXPECT_EQ(r->Property(v2, "p"), ms::PropertyValue(20));
    EXPECT_EQ(r->Property(v3, "p"), ms::PropertyValue(3));
  }
  {  // Exactly at the snapshot: the snapshot is the base and no WAL delta is applied on top of it.
    auto [r, tail] = recover(snapshot_ts);
    EXPECT_EQ(r->VertexCount(), 2U);
    EXPECT_EQ(r->Property(v1, "p"), ms::PropertyValue(10));
    EXPECT_EQ(r->Property(v2, "p"), ms::PropertyValue(2));
    EXPECT_EQ(tail.reached_timestamp, snapshot_ts);
    r->ReplayTail(tail);
    EXPECT_EQ(r->VertexCount(), 3U);
    EXPECT_EQ(r->Property(v1, "p"), ms::PropertyValue(100));
    EXPECT_EQ(r->Property(v2, "p"), ms::PropertyValue(20));
  }

  // An eligible snapshot that fails to load is fatal, not a reason to fall back to the WAL chain; a snapshot past
  // the bound is never read (last block).
  CorruptSnapshot(snapshots->front().path);
  for (auto const bound : {snapshot_ts, ts_c6}) {
    Recovered r{MakeConfig(true)};
    md::RecoveryTail tail;
    EXPECT_THROW(r.Recover(bound, &tail), md::RecoveryFailure) << "bound " << bound;
  }
  {
    auto [r, tail] = recover(ts_c1);
    EXPECT_EQ(r->Property(v1, "p"), ms::PropertyValue(1));
  }
}

// The storage configuration drives bounded recovery and hands the tail out exactly once.
TEST_F(VersioningBoundedRecoveryTest, ConfiguredBoundRecoversStateAtBoundAndHandsOutTail) {
  ms::Gid v1{};
  uint64_t ts_c2{};
  {
    memgraph::dbms::Database db{MakeConfig(false)};
    const memgraph::memory::DbArenaScope arena_scope{&db.Arena()};
    auto const p = db.storage()->NameToProperty("p");
    for (int value = 1; value <= 3; ++value) {
      auto acc = db.Access(ms::WRITE);
      auto v = value == 1 ? acc->CreateVertex() : *acc->FindVertex(v1, ms::View::OLD);
      ASSERT_TRUE(v.SetProperty(p, ms::PropertyValue(value)).has_value());
      v1 = v.Gid();
      Commit(*acc);
      if (value == 2) ts_c2 = LastDurableTs();
    }
  }
  auto const read_p = [&](memgraph::dbms::Database &db) {
    auto acc = db.Access(ms::READ);
    auto v = acc->FindVertex(v1, ms::View::OLD);
    EXPECT_TRUE(v.has_value());
    return *v->GetProperty(db.storage()->NameToProperty("p"), ms::View::OLD);
  };
  auto const wal_files = md::GetWalFiles(kRoot / md::kWalDirectory);
  ASSERT_TRUE(wal_files.has_value() && !wal_files->empty());

  {
    memgraph::dbms::Database db{MakeConfig(true, /*versioning=*/true, ts_c2)};
    const memgraph::memory::DbArenaScope arena_scope{&db.Arena()};
    auto *s = static_cast<ms::InMemoryStorage *>(db.storage());
    EXPECT_EQ(read_p(db), ms::PropertyValue(2));
    auto tail = s->TakeRecoveryTail();
    ASSERT_TRUE(tail.has_value());
    EXPECT_EQ(tail->reached_timestamp, ts_c2);
    ASSERT_FALSE(tail->wal_files.empty());
    EXPECT_EQ(tail->wal_files.back(), wal_files->back().path);
    EXPECT_FALSE(s->TakeRecoveryTail().has_value()) << "the tail is moved out";
  }
  {
    memgraph::dbms::Database db{MakeConfig(true)};
    const memgraph::memory::DbArenaScope arena_scope{&db.Arena()};
    EXPECT_EQ(read_p(db), ms::PropertyValue(3));
    EXPECT_FALSE(static_cast<ms::InMemoryStorage *>(db.storage())->TakeRecoveryTail().has_value());
  }
}

// Every snapshot past the bound and no WAL to rebuild from: recovery fails instead of coming up empty.
TEST_F(VersioningBoundedRecoveryTest, SnapshotsPastTheBoundWithoutWalFail) {
  {
    auto config = MakeConfig(false);
    config.durability.snapshot_wal_mode = ms::Config::Durability::SnapshotWalMode::PERIODIC_SNAPSHOT;
    memgraph::dbms::Database db{config};
    const memgraph::memory::DbArenaScope arena_scope{&db.Arena()};
    auto acc = db.Access(ms::WRITE);
    acc->CreateVertex();
    Commit(*acc);
    ASSERT_TRUE(static_cast<ms::InMemoryStorage *>(db.storage())->CreateSnapshot().has_value());
  }
  auto const snapshots = md::GetSnapshotFiles(kRoot / md::kSnapshotDirectory);
  ASSERT_TRUE(snapshots.has_value() && snapshots->size() == 1U);

  Recovered r{MakeConfig(true)};
  md::RecoveryTail tail;
  try {
    r.Recover(snapshots->front().durable_timestamp - 1, &tail);
    ADD_FAILURE() << "expected RecoveryFailure";
  } catch (const md::RecoveryFailure &e) {
    EXPECT_NE(std::string_view{e.what()}.find("No snapshot at or below the recovery bound"), std::string_view::npos)
        << e.what();
  }
}

namespace {
std::string ReadWholeFile(const std::filesystem::path &file) {
  std::ifstream in{file, std::ios::binary};
  return {std::istreambuf_iterator<char>{in}, std::istreambuf_iterator<char>{}};
}

class LogCapture {
 public:
  LogCapture() : sink_(std::make_shared<Sink>()), previous_(spdlog::default_logger()) {
    auto logger = std::make_shared<spdlog::logger>("bounded_recovery_test", sink_);
    logger->set_level(spdlog::level::info);
    spdlog::set_default_logger(std::move(logger));
  }

  ~LogCapture() { spdlog::set_default_logger(previous_); }

  LogCapture(const LogCapture &) = delete;
  LogCapture &operator=(const LogCapture &) = delete;

  std::string Text() const {
    std::string out;
    for (const auto &message : sink_->messages) out += message + "\n";
    return out;
  }

 private:
  struct Sink : spdlog::sinks::base_sink<std::mutex> {
    std::vector<std::string> messages;

   protected:
    void sink_it_(const spdlog::details::log_msg &msg) override {
      messages.emplace_back(msg.payload.data(), msg.payload.size());
    }

    void flush_() override {}
  };

  std::shared_ptr<Sink> sink_;
  std::shared_ptr<spdlog::logger> previous_;
};

void CreateBranchedDatabase() {
  memgraph::dbms::Database db{MakeConfig(false)};
  const memgraph::memory::DbArenaScope arena_scope{&db.Arena()};
  for (int i = 0; i < 2; ++i) {
    auto acc = db.Access(ms::WRITE);
    acc->CreateVertex();
    Commit(*acc);
  }
  ASSERT_TRUE(db.version_store()->CreateBranch("b").has_value());
  auto acc = db.Access(ms::WRITE);
  acc->CreateVertex();
  Commit(*acc);
}
}  // namespace

void ExpectRestartRefused(ms::Config restart_config, std::initializer_list<std::string_view> fragments,
                          std::function<void()> before_refused = {}, std::function<void()> after_refused = {}) {
  CreateBranchedDatabase();

  if (before_refused) before_refused();
  memgraph::tests::ExpectStartupRefused(kRoot, std::move(restart_config), fragments);
  if (after_refused) after_refused();

  memgraph::dbms::Database db{MakeConfig(true)};
  ASSERT_NE(db.version_store(), nullptr);
  EXPECT_EQ(db.version_store()->List().size(), 1U);
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

TEST_F(VersioningBoundedRecoveryTest, ALeftoverOnDiskDirectoryRefusesAnInMemoryStartup) {
  auto const leftover = kRoot / "rocksdb_main_storage";
  auto config = MakeConfig(true);
  config.disk.main_storage_directory = leftover;
  ExpectRestartRefused(
      std::move(config), {"[b]", "ON_DISK_TRANSACTIONAL"},
      [&] { std::filesystem::create_directories(leftover); },
      [&] { std::filesystem::remove_all(leftover); });
}

TEST_F(VersioningBoundedRecoveryTest, StartupWithVersioningDisabledHidesTheBranches) {
  CreateBranchedDatabase();

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

  LogCapture log;
  {
    memgraph::dbms::Database db{MakeConfig(true, /*versioning=*/false)};
    const memgraph::memory::DbArenaScope arena_scope{&db.Arena()};
    EXPECT_EQ(db.Access(ms::READ)->ApproximateVertexCount(), 3U);
    auto *store = db.version_store();
    ASSERT_NE(store, nullptr);
    EXPECT_TRUE(store->List().empty());
    EXPECT_FALSE(store->Exists("b"));
    EXPECT_FALSE(store->Empty());
    EXPECT_FALSE(store->CreateBranch("b").has_value()) << "a hidden branch still owns its name";
  }

  EXPECT_EQ(ReadWholeFile(ledger_file), ledger_before);
  EXPECT_TRUE(std::filesystem::exists(branch_directory / "wal" / "marker"));
  // The hidden branches survive a REPLICA's deletion only while this instance is MAIN: the log must not promise more.
  EXPECT_THAT(log.Text(), ::testing::HasSubstr("[b]"));
  EXPECT_THAT(log.Text(), ::testing::HasSubstr("REPLICA"));
}

namespace {
void ExpectHiddenStart(ms::Config restart_config, bool expect_store, std::initializer_list<std::string_view> warned) {
  CreateBranchedDatabase();
  const auto before = memgraph::tests::ImageDurableFiles(kRoot);
  const auto ledger_before = ReadWholeFile(kRoot / "branches" / "ledger");

  LogCapture log;
  {
    memgraph::dbms::Database db{std::move(restart_config)};
    const memgraph::memory::DbArenaScope arena_scope{&db.Arena()};
    if (expect_store) {
      ASSERT_NE(db.version_store(), nullptr);
      EXPECT_FALSE(db.version_store()->Empty()) << "the branch is hidden, not gone";
      EXPECT_TRUE(db.version_store()->List().empty());
    }
  }
  const auto text = log.Text();
  for (const auto fragment : warned) EXPECT_THAT(text, ::testing::HasSubstr(std::string{fragment}));
  EXPECT_EQ(ReadWholeFile(kRoot / "branches" / "ledger"), ledger_before);
  for (const auto &[file, content] : before) {
    if (file.starts_with("branches/")) EXPECT_EQ(memgraph::tests::ImageDurableFiles(kRoot)[file], content) << file;
  }
}
}  // namespace

TEST_F(VersioningBoundedRecoveryTest, StartupWithVersioningDisabledAndNoRecoveryHidesTheBranchesAndWarns) {
  ExpectHiddenStart(MakeConfig(false, /*versioning=*/false), true, {"[b]", "recovery on startup"});
}

TEST_F(VersioningBoundedRecoveryTest, StartupWithVersioningDisabledAndNoWalHidesTheBranchesAndWarns) {
  auto config = MakeConfig(true, /*versioning=*/false);
  config.durability.snapshot_wal_mode = ms::Config::Durability::SnapshotWalMode::DISABLED;
  ExpectHiddenStart(std::move(config), true, {"[b]", "write-ahead log"});
}

TEST_F(VersioningBoundedRecoveryTest, AnAnalyticalStartupWithVersioningDisabledGoesAheadAndWarns) {
  auto config = MakeConfig(true, /*versioning=*/false);
  config.salient.storage_mode = ms::StorageMode::IN_MEMORY_ANALYTICAL;
  ExpectHiddenStart(std::move(config), false, {"[b]", "IN_MEMORY_ANALYTICAL"});
}

TEST_F(VersioningBoundedRecoveryTest, AnUnreadableLedgerStartsMainAndMakesEveryBranchOperationSayWhy) {
  CreateBranchedDatabase();
  const auto ledger_file = kRoot / "branches" / "ledger";
  const auto good_ledger = ReadWholeFile(ledger_file);
  {
    std::ofstream out{ledger_file, std::ios::binary | std::ios::trunc};
    out << "not a ledger";
  }
  const auto corrupt = ReadWholeFile(ledger_file);
  const auto before = memgraph::tests::ImageDurableFiles(kRoot);

  {
    LogCapture log;
    memgraph::dbms::Database db{MakeConfig(true)};
    const memgraph::memory::DbArenaScope arena_scope{&db.Arena()};
    EXPECT_EQ(db.Access(ms::READ)->ApproximateVertexCount(), 3U) << "main recovers whole";
    auto *store = db.version_store();
    ASSERT_NE(store, nullptr);
    const auto message = store->LedgerUnreadable();
    ASSERT_TRUE(message.has_value());
    EXPECT_THAT(*message, ::testing::HasSubstr("branch ledger"));
    EXPECT_THAT(*message, ::testing::HasSubstr("cannot be read"));
    EXPECT_THAT(*message, ::testing::HasSubstr(ledger_file.string()));
    EXPECT_THAT(*message, ::testing::HasSubstr("restart Memgraph"));
    EXPECT_EQ(store->CreateBranch("x").error(), *message);
    EXPECT_EQ(store->DropBranch("b").error(), *message);
    EXPECT_EQ(store->TryAcquireCheckout("b").error(), *message);
    EXPECT_TRUE(db.storage()->config_.durability.retain_all_files) << "no WAL file of main may be deleted";
    EXPECT_EQ(db.storage()->RetentionFloor(), std::nullopt) << "a floor of 0 would be a known base of nothing";
    EXPECT_THAT(log.Text(), ::testing::HasSubstr("cannot be read"));
    EXPECT_THAT(log.Text(), ::testing::HasSubstr("snapshots and write-ahead log files are all kept"));
  }
  EXPECT_EQ(ReadWholeFile(ledger_file), corrupt) << "the ledger is left for the administrator";
  const auto after = memgraph::tests::ImageDurableFiles(kRoot);
  for (const auto &[file, content] : before) {
    if (file.starts_with("branches/")) EXPECT_EQ(after.at(file), content) << file;
  }

  {
    LogCapture log;
    memgraph::dbms::Database db{MakeConfig(true, /*versioning=*/false)};
    const memgraph::memory::DbArenaScope arena_scope{&db.Arena()};
    EXPECT_EQ(db.Access(ms::READ)->ApproximateVertexCount(), 3U) << "main recovers whole";
    EXPECT_EQ(db.version_store(), nullptr);
    EXPECT_FALSE(db.storage()->RetentionFloor().has_value());
    EXPECT_THAT(log.Text(), ::testing::HasSubstr("cannot be read"));
  }
  EXPECT_EQ(ReadWholeFile(ledger_file), corrupt);

  {
    std::ofstream out{ledger_file, std::ios::binary | std::ios::trunc};
    out << good_ledger;
  }
  memgraph::dbms::Database db{MakeConfig(true)};
  ASSERT_NE(db.version_store(), nullptr);
  EXPECT_FALSE(db.version_store()->LedgerUnreadable().has_value());
  EXPECT_TRUE(db.version_store()->Exists("b")) << "the restored ledger brings the branch back";
}

// Fork points are unknown while the ledger is unreadable, so a snapshot a branch may need as its base is kept.
TEST_F(VersioningBoundedRecoveryTest, AnUnreadableLedgerPausesSnapshotDeletionUntilItIsFixed) {
  constexpr uint64_t kKeep = 2;
  constexpr int kSnapshots = 5;
  CreateBranchedDatabase();
  const auto ledger_file = kRoot / "branches" / "ledger";
  const auto good_ledger = ReadWholeFile(ledger_file);
  auto const snapshot_count = [] {
    auto const snapshots = md::GetSnapshotFiles(kRoot / md::kSnapshotDirectory);
    return snapshots ? snapshots->size() : size_t{0};
  };
  auto const take_snapshots = [&](int count) {
    memgraph::dbms::Database db{[] {
      auto config = MakeConfig(true);
      config.durability.snapshot_retention_count = kKeep;
      return config;
    }()};
    const memgraph::memory::DbArenaScope arena_scope{&db.Arena()};
    for (int i = 0; i < count; ++i) {
      auto acc = db.Access(ms::WRITE);
      acc->CreateVertex();
      Commit(*acc);
      ASSERT_TRUE(static_cast<ms::InMemoryStorage *>(db.storage())->CreateSnapshot().has_value());
    }
  };
  {
    std::ofstream out{ledger_file, std::ios::binary | std::ios::trunc};
    out << "not a ledger";
  }
  take_snapshots(kSnapshots);
  EXPECT_GE(snapshot_count(), static_cast<size_t>(kSnapshots)) << "no snapshot goes while the ledger is unreadable";

  {
    std::ofstream out{ledger_file, std::ios::binary | std::ios::trunc};
    out << good_ledger;
  }
  take_snapshots(1);
  EXPECT_LE(snapshot_count(), kKeep + 1) << "rotation resumes once the ledger reads again";
}

// A branch forked on a database with no commit has a fork base of nothing, which is a known floor of 0: it keeps every
// WAL file but no snapshot, so rotation goes on. Only an unreadable ledger, whose fork points are unknown, keeps all.
TEST_F(VersioningBoundedRecoveryTest, ABranchForkedBeforeAnyCommitDoesNotPauseSnapshotDeletion) {
  constexpr uint64_t kKeep = 2;
  constexpr int kSnapshots = 5;
  auto config = MakeConfig(false);
  config.durability.snapshot_retention_count = kKeep;
  memgraph::dbms::Database db{std::move(config)};
  const memgraph::memory::DbArenaScope arena_scope{&db.Arena()};
  ASSERT_TRUE(db.version_store()->CreateBranch("b").has_value());
  for (int i = 0; i < kSnapshots; ++i) {
    auto acc = db.Access(ms::WRITE);
    acc->CreateVertex();
    Commit(*acc);
    ASSERT_TRUE(static_cast<ms::InMemoryStorage *>(db.storage())->CreateSnapshot().has_value());
  }
  auto const snapshots = md::GetSnapshotFiles(kRoot / md::kSnapshotDirectory);
  ASSERT_TRUE(snapshots.has_value());
  EXPECT_LE(snapshots->size(), kKeep) << "a floor of 0 is a known base, not an unreadable ledger";
}

// The ledger path holding a directory is the same damage, and must not take the process down with a stream error.
TEST_F(VersioningBoundedRecoveryTest, ADirectoryInPlaceOfTheLedgerStartsMainAndSaysSo) {
  CreateBranchedDatabase();
  const auto ledger_file = kRoot / "branches" / "ledger";
  std::filesystem::remove(ledger_file);
  std::filesystem::create_directories(ledger_file);

  memgraph::dbms::Database db{MakeConfig(true)};
  const memgraph::memory::DbArenaScope arena_scope{&db.Arena()};
  EXPECT_EQ(db.Access(ms::READ)->ApproximateVertexCount(), 3U);
  ASSERT_NE(db.version_store(), nullptr);
  const auto message = db.version_store()->LedgerUnreadable();
  ASSERT_TRUE(message.has_value());
  EXPECT_THAT(*message, ::testing::HasSubstr("not a regular file"));
  EXPECT_THAT(*message, ::testing::Not(::testing::HasSubstr("basic_filebuf")));
}

TEST_F(VersioningBoundedRecoveryTest, TheBranchRegistryExistsOnlyWhileVersioningIsOnOrBranchesExist) {
  {
    memgraph::dbms::Database db{MakeConfig(false, /*versioning=*/false)};
    EXPECT_EQ(db.version_store(), nullptr);
  }
  std::filesystem::remove_all(kRoot);
  {
    memgraph::dbms::Database db{MakeConfig(false)};
    EXPECT_NE(db.version_store(), nullptr);
    ASSERT_TRUE(db.version_store()->CreateBranch("b").has_value());
    EXPECT_EQ(db.version_store()->DropBranch("b"), memgraph::versioning::DropResult::kDropped);
  }
  {
    memgraph::dbms::Database db{MakeConfig(true, /*versioning=*/false)};
    EXPECT_EQ(db.version_store(), nullptr) << "a ledger without branches protects nothing";
  }
  std::filesystem::remove_all(kRoot);
  CreateBranchedDatabase();
  {
    memgraph::dbms::Database db{MakeConfig(true, /*versioning=*/false)};
    ASSERT_NE(db.version_store(), nullptr);
    EXPECT_FALSE(db.version_store()->Empty());
  }
}
