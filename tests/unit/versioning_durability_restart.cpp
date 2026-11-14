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

// Branches survive a restart of a dbms::Database on the same directory: their fork base is rebuilt by recovering main
// up to the oldest fork point and replaying the rest of its WAL under per-branch fork pins.
#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include <fmt/format.h>
#include <fmt/ranges.h>

#include <algorithm>
#include <chrono>
#include <expected>
#include <filesystem>
#include <fstream>
#include <map>
#include <memory>
#include <optional>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include "dbms/database.hpp"
#include "memory/db_arena.hpp"
#include "storage/v2/durability/marker.hpp"
#include "storage/v2/durability/paths.hpp"
#include "storage/v2/durability/snapshot.hpp"
#include "storage/v2/fmt.hpp"
#include "storage/v2/inmemory/storage.hpp"
#include "storage/v2/view.hpp"
#include "tests/test_commit_args_helper.hpp"
#include "tests/versioning_durable_files.hpp"
#include "utils/file.hpp"
#include "utils/scheduler.hpp"
#include "versioning/branch_engine.hpp"
#include "versioning/branch_ledger.hpp"
#include "versioning/version_store.hpp"

namespace ms = memgraph::storage;
namespace mv = memgraph::versioning;

namespace {
constexpr auto kTestSuite = "versioning_durability_restart";
const std::filesystem::path kStorageDirectory{std::filesystem::temp_directory_path() / kTestSuite};

ms::Config MakeConfig(bool recover_on_startup, bool wal = true, uint64_t snapshot_retention_count = 3,
                      bool versioning = true) {
  return ms::Config{
      .durability = {.storage_directory = kStorageDirectory,
                     .recover_on_startup = recover_on_startup,
                     .snapshot_wal_mode = wal ? ms::Config::Durability::SnapshotWalMode::PERIODIC_SNAPSHOT_WITH_WAL
                                              : ms::Config::Durability::SnapshotWalMode::DISABLED,
                     // Effectively disable periodic snapshotting so recovery is WAL-only.
                     .snapshot_interval = memgraph::utils::SchedulerInterval{std::chrono::hours(24)},
                     .snapshot_retention_count = snapshot_retention_count},
      .experimental_versioning = versioning,
  };
}

void Commit(ms::Storage::Accessor &acc) {
  ASSERT_TRUE(acc.PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()).has_value());
}

auto *Mem(memgraph::dbms::Database &db) { return static_cast<ms::InMemoryStorage *>(db.storage()); }

// v1(:L {p}) -e1-> v2, v3, then main moves on past each fork.
struct Gids {
  ms::Gid v1, v2, v3, e1;
  ms::Gid v4;
};

struct Names {
  ms::LabelId l;
  ms::PropertyId p;
};

Names ResolveNames(memgraph::dbms::Database &db) {
  return {db.storage()->NameToLabel("L"), db.storage()->NameToProperty("p")};
}

Gids CreateBase(memgraph::dbms::Database &db) {
  const auto [l, p] = ResolveNames(db);
  const auto edge_type = db.storage()->NameToEdgeType("T");
  Gids gids{};
  auto acc = db.Access(ms::WRITE);
  auto v1 = acc->CreateVertex();
  auto v2 = acc->CreateVertex();
  auto v3 = acc->CreateVertex();
  EXPECT_TRUE(v1.AddLabel(l).has_value());
  EXPECT_TRUE(v1.SetProperty(p, ms::PropertyValue(1)).has_value());
  auto e1 = acc->CreateEdge(&v1, &v2, edge_type);
  EXPECT_TRUE(e1.has_value());
  gids = {v1.Gid(), v2.Gid(), v3.Gid(), e1->Gid(), ms::kInvalidGid};
  Commit(*acc);
  return gids;
}

void SetV1(memgraph::dbms::Database &db, const Gids &g, int64_t value) {
  const auto p = db.storage()->NameToProperty("p");
  auto acc = db.Access(ms::WRITE);
  auto v1 = acc->FindVertex(g.v1, ms::View::OLD);
  ASSERT_TRUE(v1.has_value());
  ASSERT_TRUE(v1->SetProperty(p, ms::PropertyValue(value)).has_value());
  Commit(*acc);
}

// update v1.p, detach-delete v2, delete v3, create v4.
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

bool HasEdge(ms::Storage::Accessor &acc, ms::Gid from, ms::Gid edge) {
  auto v = acc.FindVertex(from, ms::View::OLD);
  if (!v) return false;
  auto out = v->OutEdges(ms::View::OLD);
  if (!out) return false;
  for (auto e : out->edges) {
    if (e.Gid() == edge) return true;
  }
  return false;
}

// The state main had when the branch was created, before MoveMainOn.
void ExpectForkState(ms::Storage::Accessor &acc, const Names &n, const Gids &g, int64_t v1_value) {
  auto v1 = acc.FindVertex(g.v1, ms::View::OLD);
  ASSERT_TRUE(v1.has_value());
  EXPECT_EQ(*v1->GetProperty(n.p, ms::View::OLD), ms::PropertyValue(v1_value));
  EXPECT_TRUE(*v1->HasLabel(n.l, ms::View::OLD));
  EXPECT_TRUE(acc.FindVertex(g.v2, ms::View::OLD).has_value()) << "vertex deleted after the fork must be present";
  EXPECT_TRUE(acc.FindVertex(g.v3, ms::View::OLD).has_value());
  EXPECT_TRUE(HasEdge(acc, g.v1, g.e1));
  EXPECT_FALSE(acc.FindVertex(g.v4, ms::View::OLD).has_value()) << "vertex created after the fork must be absent";
}

void ExpectLatestState(ms::Storage::Accessor &acc, const Names &n, const Gids &g) {
  auto v1 = acc.FindVertex(g.v1, ms::View::OLD);
  ASSERT_TRUE(v1.has_value());
  EXPECT_EQ(*v1->GetProperty(n.p, ms::View::OLD), ms::PropertyValue(10));
  EXPECT_FALSE(acc.FindVertex(g.v2, ms::View::OLD).has_value());
  EXPECT_FALSE(acc.FindVertex(g.v3, ms::View::OLD).has_value());
  EXPECT_FALSE(HasEdge(acc, g.v1, g.e1));
  EXPECT_TRUE(acc.FindVertex(g.v4, ms::View::OLD).has_value());
}

// Reads `branch` the way a checkout does: main's history at the branch's runtime fork pin.
std::unique_ptr<ms::Storage::Accessor> ReadBranch(memgraph::dbms::Database &db, std::string_view branch) {
  auto info = db.version_store()->Get(branch);
  EXPECT_TRUE(info.has_value()) << "branch " << branch << " is not listed";
  if (!info) return nullptr;
  auto acc = Mem(db)->HistoricalAccess(info->fork_ts);
  EXPECT_TRUE(acc.has_value()) << "fork pin of branch " << branch << " is not held";
  return acc ? std::move(*acc) : nullptr;
}

std::optional<memgraph::versioning::LedgerContents> ReadLedger() {
  auto read = memgraph::versioning::BranchLedger{kStorageDirectory / "branches" / "ledger"}.Read();
  EXPECT_TRUE(read.has_value());
  return read.has_value() ? *read : std::nullopt;
}

size_t CountFiles(std::string_view subdirectory) {
  const auto dir = kStorageDirectory / subdirectory;
  if (!std::filesystem::exists(dir)) return 0;
  size_t count = 0;
  for (auto const &entry : std::filesystem::directory_iterator(dir)) count += entry.is_regular_file() ? 1 : 0;
  return count;
}

// Flips bytes near the end of a WAL file, in the deltas rather than the header.
void CorruptTail(const std::filesystem::path &wal) {
  std::fstream f{wal, std::ios::in | std::ios::out | std::ios::binary};
  f.seekp(-48, std::ios::end);
  f.write("\xff\xff\xff\xff\xff\xff\xff\xff", 8);
}

void Snapshot(memgraph::dbms::Database &db) { ASSERT_TRUE(Mem(db)->CreateSnapshot(/*force=*/true).has_value()); }

// Overwrites the first vertex marker so the snapshot header still reads but loading the body fails.
void CorruptSnapshot(const std::filesystem::path &path) {
  const auto info = memgraph::storage::durability::ReadSnapshotInfo(path);
  memgraph::utils::OutputFile file;
  ASSERT_TRUE(file.Open(path, memgraph::utils::OutputFile::Mode::OVERWRITE_EXISTING));
  file.SetPosition(memgraph::utils::OutputFile::Position::SET, info.offset_vertices);
  const auto value = static_cast<uint8_t>(memgraph::storage::durability::Marker::TYPE_MAP);
  ASSERT_TRUE(file.Write(&value, sizeof(value)));
  ASSERT_TRUE(file.Sync());
  file.Close();
}

// Builds a database whose snapshot is past its only fork and whose WAL is gone.
void MakeMainUnrecoverableUpToTheFork() {
  memgraph::dbms::Database db{MakeConfig(/*recover_on_startup=*/false)};
  const memgraph::memory::DbArenaScope arena_scope{&db.Arena()};
  auto gids = CreateBase(db);
  ASSERT_TRUE(db.version_store()->CreateBranch("b").has_value());
  MoveMainOn(db, gids);
  Snapshot(db);
}

void RemoveWalFiles() {
  for (const auto &entry : std::filesystem::directory_iterator(kStorageDirectory / ms::durability::kWalDirectory)) {
    std::filesystem::remove(entry.path());
  }
}
}  // namespace

class VersioningDurabilityRestartTest : public ::testing::Test {
 protected:
  void SetUp() override { std::filesystem::remove_all(kStorageDirectory); }

  void TearDown() override { std::filesystem::remove_all(kStorageDirectory); }
};

TEST_F(VersioningDurabilityRestartTest, BranchReadsMainAtItsForkAfterRestart) {
  Gids gids{};
  {
    memgraph::dbms::Database db{MakeConfig(/*recover_on_startup=*/false)};
    const memgraph::memory::DbArenaScope arena_scope{&db.Arena()};
    gids = CreateBase(db);
    ASSERT_TRUE(db.version_store()->CreateBranch("b").has_value());
    gids = MoveMainOn(db, gids);
  }

  memgraph::dbms::Database db{MakeConfig(/*recover_on_startup=*/true)};
  const memgraph::memory::DbArenaScope arena_scope{&db.Arena()};
  const auto names = ResolveNames(db);

  ASSERT_NE(db.version_store(), nullptr);
  const auto listed = db.version_store()->List();
  ASSERT_EQ(listed.size(), 1u);
  EXPECT_EQ(listed[0].first, "b");
  EXPECT_EQ(listed[0].second.number, 2u);
  EXPECT_EQ(listed[0].second.parent, "main");
  EXPECT_TRUE(Mem(db)->OldestForkPin().has_value());

  {
    auto branch = ReadBranch(db, "b");
    ASSERT_NE(branch, nullptr);
    ExpectForkState(*branch, names, gids, 1);
  }
  {
    auto main = db.Access(ms::READ);
    ExpectLatestState(*main, names, gids);
  }

  // A commit after the restart must not collide with the replayed timestamps, and stays out of the fork's view.
  ms::Gid v5;
  {
    auto acc = db.Access(ms::WRITE);
    v5 = acc->CreateVertex().Gid();
    Commit(*acc);
  }
  EXPECT_TRUE(db.Access(ms::READ)->FindVertex(v5, ms::View::OLD).has_value());
  {
    auto branch = ReadBranch(db, "b");
    ASSERT_NE(branch, nullptr);
    EXPECT_FALSE(branch->FindVertex(v5, ms::View::OLD).has_value());
    ExpectForkState(*branch, names, gids, 1);
  }
}

TEST_F(VersioningDurabilityRestartTest, MainWritesAfterARestartStayInvisibleToAnOlderBranch) {
  Gids gids{};
  {
    memgraph::dbms::Database db{MakeConfig(/*recover_on_startup=*/false)};
    const memgraph::memory::DbArenaScope arena_scope{&db.Arena()};
    gids = CreateBase(db);
    for (int i = 0; i < 10; ++i) db.Access(ms::READ)->Abort();
    ASSERT_TRUE(db.version_store()->CreateBranch("b").has_value());
  }

  const auto p_of = [&gids](memgraph::dbms::Database &db, ms::Storage::Accessor &acc) {
    return *acc.FindVertex(gids.v1, ms::View::OLD)->GetProperty(db.storage()->NameToProperty("p"), ms::View::OLD);
  };

  {
    memgraph::dbms::Database db{MakeConfig(/*recover_on_startup=*/true)};
    const memgraph::memory::DbArenaScope arena_scope{&db.Arena()};
    SetV1(db, gids, 2);
  }

  memgraph::dbms::Database db{MakeConfig(/*recover_on_startup=*/true)};
  const memgraph::memory::DbArenaScope arena_scope{&db.Arena()};
  ASSERT_TRUE(db.version_store()->Exists("b"));
  {
    auto branch = ReadBranch(db, "b");
    ASSERT_NE(branch, nullptr);
    EXPECT_EQ(p_of(db, *branch), ms::PropertyValue(1));
  }
  EXPECT_EQ(p_of(db, *db.Access(ms::READ)), ms::PropertyValue(2));

  SetV1(db, gids, 3);
  auto branch = ReadBranch(db, "b");
  ASSERT_NE(branch, nullptr);
  EXPECT_EQ(p_of(db, *branch), ms::PropertyValue(1));
  EXPECT_EQ(p_of(db, *db.Access(ms::READ)), ms::PropertyValue(3));
}

TEST_F(VersioningDurabilityRestartTest, DropAfterRestartKeepsTheOriginalForkOfTheOtherBranch) {
  Gids gids{};
  uint64_t b2_recorded_fork_ts = 0;
  {
    memgraph::dbms::Database db{MakeConfig(/*recover_on_startup=*/false)};
    const memgraph::memory::DbArenaScope arena_scope{&db.Arena()};
    gids = CreateBase(db);
    ASSERT_TRUE(db.version_store()->CreateBranch("b1").has_value());
    SetV1(db, gids, 2);
    auto b2 = db.version_store()->CreateBranch("b2");
    ASSERT_TRUE(b2.has_value());
    b2_recorded_fork_ts = b2->fork_ts;
    SetV1(db, gids, 3);
  }

  const auto value_at = [&gids](memgraph::dbms::Database &db, std::string_view branch) {
    auto acc = ReadBranch(db, branch);
    EXPECT_NE(acc, nullptr);
    return acc ? *acc->FindVertex(gids.v1, ms::View::OLD)->GetProperty(db.storage()->NameToProperty("p"), ms::View::OLD)
               : ms::PropertyValue();
  };

  {
    memgraph::dbms::Database db{MakeConfig(/*recover_on_startup=*/true)};
    const memgraph::memory::DbArenaScope arena_scope{&db.Arena()};
    ASSERT_EQ(db.version_store()->List().size(), 2u);
    EXPECT_EQ(value_at(db, "b1"), ms::PropertyValue(1));
    EXPECT_EQ(value_at(db, "b2"), ms::PropertyValue(2));
    EXPECT_EQ(*db.Access(ms::READ)
                   ->FindVertex(gids.v1, ms::View::OLD)
                   ->GetProperty(db.storage()->NameToProperty("p"), ms::View::OLD),
              ms::PropertyValue(3));

    ASSERT_EQ(db.version_store()->DropBranch("b1"), mv::DropResult::kDropped);
    auto ledger = ReadLedger();
    ASSERT_TRUE(ledger.has_value());
    ASSERT_EQ(ledger->branches.size(), 1u);
    EXPECT_EQ(ledger->branches[0].name, "b2");
    EXPECT_EQ(ledger->branches[0].info.fork_ts, b2_recorded_fork_ts) << "the ledger must keep the original fork_ts";

    // A new commit and a new branch after the restart.
    SetV1(db, gids, 4);
    ASSERT_TRUE(db.version_store()->CreateBranch("b3").has_value());
    SetV1(db, gids, 5);
  }

  memgraph::dbms::Database db{MakeConfig(/*recover_on_startup=*/true)};
  const memgraph::memory::DbArenaScope arena_scope{&db.Arena()};
  const auto names = ResolveNames(db);
  EXPECT_FALSE(db.version_store()->Exists("b1"));
  EXPECT_EQ(db.version_store()->List().size(), 2u);
  EXPECT_EQ(value_at(db, "b2"), ms::PropertyValue(2));
  EXPECT_EQ(value_at(db, "b3"), ms::PropertyValue(4));
  EXPECT_EQ(db.version_store()->Get("b3")->number, 4u) << "branch numbers are never reused";
  EXPECT_EQ(*db.Access(ms::READ)->FindVertex(gids.v1, ms::View::OLD)->GetProperty(names.p, ms::View::OLD),
            ms::PropertyValue(5));
}

// The branch is listed as unavailable with a reason holding every fragment, its ledger entry is kept, and DROP BRANCH
// removes it (the pin it holds is released).
void ExpectBranchSetAside(memgraph::dbms::Database &db, std::string_view branch,
                          std::initializer_list<std::string_view> fragments) {
  const memgraph::memory::DbArenaScope arena_scope{&db.Arena()};
  ASSERT_NE(db.version_store(), nullptr);
  const auto info = db.version_store()->Get(branch);
  ASSERT_TRUE(info.has_value());
  ASSERT_TRUE(info->unavailable_reason.has_value()) << "branch " << branch << " must be set aside";
  for (const auto fragment : fragments) {
    EXPECT_NE(info->unavailable_reason->find(fragment), std::string::npos)
        << "'" << fragment << "' missing from: " << *info->unavailable_reason;
  }
  EXPECT_FALSE(db.version_store()->TryAcquireCheckout(branch).has_value());
  const auto ledger = ReadLedger();
  ASSERT_TRUE(ledger.has_value());
  EXPECT_EQ(ledger->branches.size(), 1U);
}

// Versioning on but no WAL: main starts, the branch is set aside, the files stay, and the settings restored bring it
// back.
TEST_F(VersioningDurabilityRestartTest, AWalLessStartupSetsTheBranchesAsideAndTheyComeBackWithTheWal) {
  Gids gids{};
  {
    memgraph::dbms::Database db{MakeConfig(/*recover_on_startup=*/false)};
    const memgraph::memory::DbArenaScope arena_scope{&db.Arena()};
    gids = CreateBase(db);
    ASSERT_TRUE(db.version_store()->CreateBranch("b1").has_value());
  }

  {
    memgraph::dbms::Database db{MakeConfig(/*recover_on_startup=*/true, /*wal=*/false)};
    const memgraph::memory::DbArenaScope arena_scope{&db.Arena()};
    EXPECT_TRUE(db.Access(ms::READ)->FindVertex(gids.v1, ms::View::OLD).has_value());
    ExpectBranchSetAside(db, "b1", {"write-ahead log", "recovery on startup"});
  }

  memgraph::dbms::Database db{MakeConfig(/*recover_on_startup=*/true)};
  const memgraph::memory::DbArenaScope arena_scope{&db.Arena()};
  ASSERT_NE(db.version_store(), nullptr);
  EXPECT_TRUE(db.version_store()->Exists("b1"));
  EXPECT_FALSE(db.version_store()->Get("b1")->unavailable_reason.has_value());
  EXPECT_TRUE(db.Access(ms::READ)->FindVertex(gids.v1, ms::View::OLD).has_value());
}

TEST_F(VersioningDurabilityRestartTest, AStartupWithoutRecoveryOnStartupSetsTheBranchesAside) {
  {
    memgraph::dbms::Database db{MakeConfig(/*recover_on_startup=*/false)};
    const memgraph::memory::DbArenaScope arena_scope{&db.Arena()};
    CreateBase(db);
    ASSERT_TRUE(db.version_store()->CreateBranch("b1").has_value());
  }

  memgraph::dbms::Database db{MakeConfig(/*recover_on_startup=*/false)};
  ExpectBranchSetAside(db, "b1", {"recovery on startup"});
}

// Main cannot be recovered up to the fork: the snapshot is past it and the WAL that reaches back is gone. Main starts
// from what it has, as without versioning; the branch is set aside.
TEST_F(VersioningDurabilityRestartTest, MainStartsWhenItsHistoryDoesNotReachTheOldestFork) {
  MakeMainUnrecoverableUpToTheFork();
  RemoveWalFiles();

  memgraph::dbms::Database db{MakeConfig(/*recover_on_startup=*/true)};
  const memgraph::memory::DbArenaScope arena_scope{&db.Arena()};
  EXPECT_FALSE(db.storage()->IsBroken());
  EXPECT_EQ(db.Access(ms::READ)->ApproximateVertexCount(), 2U) << "main recovers its snapshot";
  ExpectBranchSetAside(db, "b", {"recovered up to the branch's fork point"});
  EXPECT_EQ(db.version_store()->DropBranch("b"), mv::DropResult::kDropped);
}

// With allow_recovery_failure the bounded recovery comes up broken instead of throwing; main still recovers on its own.
TEST_F(VersioningDurabilityRestartTest, MainStartsWhenTheBoundedRecoveryOnlyComesUpBroken) {
  MakeMainUnrecoverableUpToTheFork();
  RemoveWalFiles();

  auto restart = MakeConfig(/*recover_on_startup=*/true);
  restart.durability.allow_recovery_failure = true;
  memgraph::dbms::Database db{std::move(restart)};
  const memgraph::memory::DbArenaScope arena_scope{&db.Arena()};
  EXPECT_FALSE(db.storage()->IsBroken());
  EXPECT_EQ(db.Access(ms::READ)->ApproximateVertexCount(), 2U);
  ExpectBranchSetAside(db, "b", {"recovered up to the branch's fork point"});
}

// A WAL that does not replay fails main's own recovery as on master (the exception of the storage), and the half-built
// storage of the bounded attempt must not write its exit snapshot over the files.
TEST_F(VersioningDurabilityRestartTest, ACorruptWalAfterTheForkFailsAsOnMaster) {
  Gids gids{};
  {
    memgraph::dbms::Database db{MakeConfig(/*recover_on_startup=*/false)};
    const memgraph::memory::DbArenaScope arena_scope{&db.Arena()};
    gids = CreateBase(db);
    ASSERT_TRUE(db.version_store()->CreateBranch("b").has_value());
  }
  {
    // A restart starts a new WAL file, which holds only commits past the fork.
    memgraph::dbms::Database db{MakeConfig(/*recover_on_startup=*/true)};
    const memgraph::memory::DbArenaScope arena_scope{&db.Arena()};
    MoveMainOn(db, gids);
  }
  std::vector<std::filesystem::path> wals;
  for (const auto &entry : std::filesystem::directory_iterator(kStorageDirectory / ms::durability::kWalDirectory)) {
    wals.push_back(entry.path());
  }
  ASSERT_FALSE(wals.empty());
  std::ranges::sort(wals);
  CorruptTail(wals.back());

  auto restart = MakeConfig(/*recover_on_startup=*/true);
  restart.durability.snapshot_on_exit = true;
  const auto files_before = memgraph::tests::ImageDurableFiles(kStorageDirectory);
  try {
    memgraph::dbms::Database db{std::move(restart)};
    ADD_FAILURE() << "recovery must fail";
  } catch (const std::exception &e) {
    EXPECT_EQ(std::string_view{e.what()}.find("Graph Versioning"), std::string_view::npos) << e.what();
  }
  EXPECT_EQ(memgraph::tests::ImageDurableFiles(kStorageDirectory), files_before);
}

// Main's own failed recovery is master's: its exception, not a Graph Versioning one, whether or not branches exist.
TEST_F(VersioningDurabilityRestartTest, FailedRecoveryKeepsTheStorageException) {
  {
    memgraph::dbms::Database db{MakeConfig(/*recover_on_startup=*/false)};
    const memgraph::memory::DbArenaScope arena_scope{&db.Arena()};
    CreateBase(db);
    Snapshot(db);
  }
  RemoveWalFiles();
  const auto snapshots = ms::durability::GetSnapshotFiles(kStorageDirectory / ms::durability::kSnapshotDirectory);
  ASSERT_TRUE(snapshots.has_value());
  ASSERT_EQ(snapshots->size(), 1U);
  CorruptSnapshot(snapshots->front().path);

  try {
    memgraph::dbms::Database db{MakeConfig(/*recover_on_startup=*/true)};
    ADD_FAILURE() << "recovery must fail";
  } catch (const std::exception &e) {
    EXPECT_EQ(std::string_view{e.what()}.find("Graph Versioning"), std::string_view::npos) << e.what();
  }
}

// The version store's callbacks hold the InMemoryStorage they were built over; the switch to on-disk replaces that
// storage (the interpreter allows it only while no branch exists), so the store must go with it.
TEST_F(VersioningDurabilityRestartTest, SwitchingToOnDiskDropsTheVersionStoreOfTheReplacedStorage) {
  auto config = MakeConfig(/*recover_on_startup=*/false);
  const auto disk = kStorageDirectory / "disk";
  config.disk = {.main_storage_directory = disk / "db",
                 .label_index_directory = disk / "label_index",
                 .label_property_index_directory = disk / "label_property_index",
                 .unique_constraints_directory = disk / "unique_constraints",
                 .name_id_mapper_directory = disk / "name_id_mapper",
                 .id_name_mapper_directory = disk / "id_name_mapper",
                 .durability_directory = disk / "durability",
                 .wal_directory = disk / "wal"};
  memgraph::dbms::Database db{std::move(config)};
  ASSERT_NE(db.version_store(), nullptr);
  ASSERT_TRUE(db.version_store()->Empty());

  db.SwitchToOnDisk();

  EXPECT_EQ(db.version_store(), nullptr);
}

TEST_F(VersioningDurabilityRestartTest, DroppingABranchDeletesItsDirectory) {
  Gids gids{};
  std::filesystem::path branch_directory;
  {
    memgraph::dbms::Database db{MakeConfig(/*recover_on_startup=*/false)};
    const memgraph::memory::DbArenaScope arena_scope{&db.Arena()};
    gids = CreateBase(db);
    auto *store = db.version_store();
    auto info = store->CreateBranch("b");
    ASSERT_TRUE(info.has_value());
    branch_directory = kStorageDirectory / "branches" / std::to_string(info->number);
    {
      auto core =
          store->GetOrBuildCore(info->number, [&]() -> std::expected<std::shared_ptr<mv::BranchCore>, std::string> {
            auto fresh = mv::BranchCore::Build(*Mem(db), info->fork_ts, info->number);
            if (!fresh) return std::unexpected(fresh.error().message);
            (*fresh)->SetGidReserver(store->GidReserverOf(info->number));
            return std::move(*fresh);
          });
      ASSERT_TRUE(core.has_value());
      mv::BranchContext ctx{*core};
      auto diff_acc = ctx.diff_engine().Access(ms::WRITE);
      ASSERT_TRUE(ctx.BeginTransaction(diff_acc.get()).has_value());
      ctx.CreateNativeVertex();
      ASSERT_TRUE(ctx.CommitTransaction(memgraph::tests::MakeMainCommitArgs()).has_value());
      ctx.EndTransaction();
    }
    ASSERT_TRUE(std::filesystem::exists(branch_directory / "wal"));

    ASSERT_EQ(store->DropBranch("b"), mv::DropResult::kDropped);
    EXPECT_FALSE(std::filesystem::exists(branch_directory));
  }

  memgraph::dbms::Database db{MakeConfig(/*recover_on_startup=*/true)};
  const memgraph::memory::DbArenaScope arena_scope{&db.Arena()};
  EXPECT_FALSE(db.version_store()->Exists("b"));
  EXPECT_FALSE(std::filesystem::exists(branch_directory));
  EXPECT_TRUE(db.Access(ms::READ)->FindVertex(gids.v1, ms::View::OLD).has_value());
}

TEST_F(VersioningDurabilityRestartTest, BranchesSurviveRestartAndNewBranchesContinueTheNumbering) {
  {
    memgraph::dbms::Database db{MakeConfig(/*recover_on_startup=*/false)};
    const memgraph::memory::DbArenaScope arena_scope{&db.Arena()};
    CreateBase(db);
    ASSERT_TRUE(db.version_store()->CreateBranch("b1").has_value());
    ASSERT_TRUE(db.version_store()->CreateBranch("b2").has_value());
  }

  memgraph::dbms::Database db{MakeConfig(/*recover_on_startup=*/true)};
  const memgraph::memory::DbArenaScope arena_scope{&db.Arena()};
  ASSERT_EQ(db.version_store()->List().size(), 2u);
  EXPECT_EQ(Mem(db)->RetentionFloor(), mv::RecoverableBelow(*db.version_store()->Get("b1")));

  auto b3 = db.version_store()->CreateBranch("b3");
  ASSERT_TRUE(b3.has_value());
  EXPECT_EQ(b3->number, 4u);
  for (const auto *name : {"b1", "b2", "b3"}) ASSERT_EQ(db.version_store()->DropBranch(name), mv::DropResult::kDropped);
  EXPECT_FALSE(Mem(db)->OldestForkPin().has_value());
  EXPECT_FALSE(Mem(db)->RetentionFloor().has_value());
}

// A branch directory without a ledger entry (e.g. a drop whose directory removal was interrupted) keeps its number.
TEST_F(VersioningDurabilityRestartTest, NewBranchesSkipTheNumbersOfLeftoverBranchDirectories) {
  std::filesystem::create_directories(kStorageDirectory / "branches" / "7");
  for (const auto *ignored : {"12x", "x9"})
    std::filesystem::create_directories(kStorageDirectory / "branches" / ignored);
  memgraph::dbms::Database db{MakeConfig(/*recover_on_startup=*/true)};
  const memgraph::memory::DbArenaScope arena_scope{&db.Arena()};
  auto created = db.version_store()->CreateBranch("b");
  ASSERT_TRUE(created.has_value());
  EXPECT_EQ(created->number, 8u);
}

TEST_F(VersioningDurabilityRestartTest, MainSchemaOpAfterForkIsReplayedUnderTheFork) {
  ms::Gid post_fork_vertex;
  ms::LabelId indexed_label{};

  {
    memgraph::dbms::Database db{MakeConfig(/*recover_on_startup=*/false)};
    const memgraph::memory::DbArenaScope arena_scope{&db.Arena()};

    ASSERT_NE(db.version_store(), nullptr);
    ASSERT_TRUE(db.version_store()->CreateBranch("b1").has_value());

    {
      auto acc = db.UniqueAccess();
      indexed_label = acc->NameToLabel("PostForkLabel");
      ASSERT_TRUE(acc->CreateIndex(indexed_label).has_value());
      Commit(*acc);
    }
    {
      auto acc = db.Access(ms::WRITE);
      post_fork_vertex = acc->CreateVertex().Gid();
      Commit(*acc);
    }
  }

  memgraph::dbms::Database db{MakeConfig(/*recover_on_startup=*/true)};
  const memgraph::memory::DbArenaScope arena_scope{&db.Arena()};

  ASSERT_TRUE(db.version_store()->Exists("b1"));
  auto acc = db.Access(ms::READ);
  EXPECT_THAT(acc->ListAllIndices().label, ::testing::Contains(indexed_label));
  EXPECT_TRUE(acc->FindVertex(post_fork_vertex, ms::View::OLD).has_value());
  auto branch = ReadBranch(db, "b1");
  ASSERT_NE(branch, nullptr);
  EXPECT_FALSE(branch->FindVertex(post_fork_vertex, ms::View::OLD).has_value());
}

namespace {
// Every vertex with its p and out edges, ordered by gid.
std::string Dump(ms::Storage::Accessor &acc, ms::PropertyId p) {
  std::map<uint64_t, std::string> vertices;
  for (auto v : acc.Vertices(ms::View::OLD)) {
    std::vector<uint64_t> edges;
    for (const auto &e : v.OutEdges(ms::View::OLD)->edges) edges.push_back(e.Gid().AsUint());
    std::ranges::sort(edges);
    vertices[v.Gid().AsUint()] = fmt::format("{}{}", *v.GetProperty(p, ms::View::OLD), edges);
  }
  return fmt::format("{}", vertices);
}
}  // namespace

// Files written with commit-lock narrowing on recover to the same branches, each at its own fork, with it on or off.
TEST_F(VersioningDurabilityRestartTest, NarrowingOnAndOffRecoverTheSameBranches) {
  const auto write_config = [] {
    auto config = MakeConfig(/*recover_on_startup=*/false);
    config.experimental_commit_lock_narrowing = true;
    return config;
  };
  {
    memgraph::dbms::Database db{write_config()};
    const memgraph::memory::DbArenaScope arena_scope{&db.Arena()};
    auto gids = CreateBase(db);
    ASSERT_TRUE(db.version_store()->CreateBranch("a1").has_value());
    ASSERT_TRUE(db.version_store()->CreateBranch("a2").has_value());
    SetV1(db, gids, 2);
    ASSERT_TRUE(db.version_store()->CreateBranch("b").has_value());
    MoveMainOn(db, gids);
  }
  const auto pristine = kStorageDirectory.string() + "_pristine";
  std::filesystem::remove_all(pristine);
  std::filesystem::copy(kStorageDirectory, pristine, std::filesystem::copy_options::recursive);

  using State = std::map<std::string, std::pair<uint64_t, std::string>>;  // name -> {recorded fork, contents}
  const auto restart = [&](bool narrowing) {
    std::filesystem::remove_all(kStorageDirectory);
    std::filesystem::copy(pristine, kStorageDirectory, std::filesystem::copy_options::recursive);
    auto config = MakeConfig(/*recover_on_startup=*/true);
    config.experimental_commit_lock_narrowing = narrowing;
    memgraph::dbms::Database db{std::move(config)};
    const memgraph::memory::DbArenaScope arena_scope{&db.Arena()};
    const auto p = db.storage()->NameToProperty("p");
    State state;
    for (const auto &[name, info] : db.version_store()->List()) {
      auto branch = ReadBranch(db, name);
      if (branch) state[name] = {info.recorded_fork_ts, Dump(*branch, p)};
    }
    state["main"] = {0, Dump(*db.Access(ms::READ), p)};
    return state;
  };
  const auto on = restart(true);
  const auto off = restart(false);
  std::filesystem::remove_all(pristine);

  ASSERT_EQ(on.size(), 4U);
  EXPECT_EQ(on, off);
  EXPECT_EQ(on.at("a1"), on.at("a2"));
  EXPECT_LT(on.at("a1").first, on.at("b").first);
  EXPECT_NE(on.at("a1").second, on.at("b").second);
  EXPECT_NE(on.at("b").second, on.at("main").second);
}

TEST_F(VersioningDurabilityRestartTest, MainRecoveryNeverAbsorbsADiffEnginesWrites) {
  constexpr auto kForkLabel = "ForkLabel";
  constexpr auto kBranchLabel = "BranchOnlyLabel";
  constexpr auto kBranchProp = "BranchOnlyProp";
  ms::Gid fork_vertex;

  {
    memgraph::dbms::Database db{MakeConfig(/*recover_on_startup=*/false)};
    const memgraph::memory::DbArenaScope arena_scope{&db.Arena()};
    auto *storage = Mem(db);
    const auto fork_label = db.storage()->NameToLabel(kForkLabel);
    const auto branch_label = db.storage()->NameToLabel(kBranchLabel);
    const auto x = db.storage()->NameToProperty("x");
    const auto branch_prop = db.storage()->NameToProperty(kBranchProp);
    const auto edge_type = db.storage()->NameToEdgeType("E");

    {
      auto acc = db.Access(ms::WRITE);
      auto a = acc->CreateVertex();
      fork_vertex = a.Gid();
      ASSERT_TRUE(a.AddLabel(fork_label).has_value());
      ASSERT_TRUE(a.SetProperty(x, ms::PropertyValue(1)).has_value());
      auto b = acc->CreateVertex();
      ASSERT_TRUE(acc->CreateEdge(&a, &b, edge_type).has_value());
      Commit(*acc);
    }

    auto info = db.version_store()->CreateBranch("b1");
    ASSERT_TRUE(info.has_value());
    auto ctx = memgraph::versioning::BranchContext::BuildFromFork(*storage, info->fork_ts, info->number);
    ASSERT_TRUE(ctx.has_value());
    ASSERT_TRUE(
        db.version_store()
            ->RecordGidFloors(info->number, (*ctx)->core()->vertex_gid_floor(), (*ctx)->core()->edge_gid_floor())
            .has_value());
    {
      auto diff_acc = (*ctx)->diff_engine().Access(ms::WRITE);
      ASSERT_TRUE((*ctx)->BeginTransaction(diff_acc.get()).has_value());
      auto fork = (*ctx)->FindForkVertex(fork_vertex);
      ASSERT_TRUE(fork.has_value());
      ASSERT_TRUE((*ctx)->AddLabel(*fork, branch_label).has_value());
      ASSERT_TRUE((*ctx)->SetProperty(*fork, x, ms::PropertyValue(2)).has_value());
      auto c = (*ctx)->CreateNativeVertex();
      auto d = (*ctx)->CreateNativeVertex();
      ASSERT_TRUE(c.AddLabel(branch_label).has_value());
      ASSERT_TRUE(c.SetProperty(branch_prop, ms::PropertyValue(7)).has_value());
      ASSERT_TRUE((*ctx)->CreateNativeEdge(c, d, edge_type).has_value());
      ASSERT_TRUE((*ctx)->CommitTransaction(memgraph::tests::MakeMainCommitArgs()).has_value());
      (*ctx)->EndTransaction();
    }
  }

  memgraph::dbms::Database db{MakeConfig(/*recover_on_startup=*/true)};
  const memgraph::memory::DbArenaScope arena_scope{&db.Arena()};
  EXPECT_TRUE(db.version_store()->Exists("b1"));

  auto acc = db.Access(ms::READ);
  size_t vertices = 0;
  size_t edges = 0;
  for (auto v : acc->Vertices(ms::View::OLD)) {
    ++vertices;
    for (auto const label : *v.Labels(ms::View::OLD)) {
      EXPECT_NE(acc->LabelToName(label), kBranchLabel) << "branch label leaked into main";
    }
    for (auto const &[prop, value] : *v.Properties(ms::View::OLD)) {
      EXPECT_NE(db.storage()->PropertyToName(prop), kBranchProp) << "branch property leaked into main";
    }
    edges += v.OutEdges(ms::View::OLD)->edges.size();
  }
  EXPECT_EQ(vertices, 2u) << "main must hold exactly its own two vertices (no branch-native vertices)";
  EXPECT_EQ(edges, 1u) << "main must hold exactly its own edge (no branch-native edge)";

  auto fork = acc->FindVertex(fork_vertex, ms::View::OLD);
  ASSERT_TRUE(fork.has_value());
  auto labels = fork->Labels(ms::View::OLD);
  ASSERT_TRUE(labels.has_value());
  ASSERT_EQ(labels->size(), 1u);
  EXPECT_EQ(acc->LabelToName((*labels)[0]), kForkLabel);
  auto x_val = fork->GetProperty(db.storage()->NameToProperty("x"), ms::View::OLD);
  ASSERT_TRUE(x_val.has_value());
  EXPECT_EQ(*x_val, ms::PropertyValue(1)) << "the branch's property change must not reach main";

  EXPECT_FALSE(std::filesystem::exists(kStorageDirectory / "versioning_diff_engine"))
      << "a durable branch lives under branches/, never in a throwaway diff-engine directory";
}

// With versioning off, retention must still keep the hidden branch's fork base: the snapshot below the fork, or every
// WAL back to the first one when none exists.
class BranchSnapshotRetentionTest : public VersioningDurabilityRestartTest,
                                    public ::testing::WithParamInterface<bool> {};

TEST_P(BranchSnapshotRetentionTest, HiddenBranchSurvivesSnapshotRetention) {
  const bool snapshot_below_fork = GetParam();
  Gids gids{};
  uint64_t oldest_fork_ts = 0;
  {
    memgraph::dbms::Database db{MakeConfig(/*recover_on_startup=*/false, /*wal=*/true, /*retention=*/1)};
    const memgraph::memory::DbArenaScope arena_scope{&db.Arena()};
    gids = CreateBase(db);
    if (snapshot_below_fork) Snapshot(db);
    ASSERT_TRUE(db.version_store()->CreateBranch("b").has_value());
    gids = MoveMainOn(db, gids);
  }
  const auto ledger = ReadLedger();
  ASSERT_TRUE(ledger.has_value());
  ASSERT_EQ(ledger->branches.size(), 1u);
  oldest_fork_ts = ledger->branches[0].info.recorded_fork_ts;

  {
    memgraph::dbms::Database db{
        MakeConfig(/*recover_on_startup=*/true, /*wal=*/true, /*retention=*/1, /*versioning=*/false)};
    const memgraph::memory::DbArenaScope arena_scope{&db.Arena()};
    EXPECT_EQ(Mem(db)->RetentionFloor(), oldest_fork_ts - 1);
    Snapshot(db);
    SetV1(db, gids, 11);
    Snapshot(db);
    SetV1(db, gids, 12);
    Snapshot(db);
    EXPECT_EQ(CountFiles("snapshots"), snapshot_below_fork ? 2u : 1u)
        << (snapshot_below_fork ? "the newest snapshot and the one below the fork" : "only the newest snapshot");
  }

  memgraph::dbms::Database db{MakeConfig(/*recover_on_startup=*/true, /*wal=*/true, /*retention=*/1)};
  const memgraph::memory::DbArenaScope arena_scope{&db.Arena()};
  ASSERT_EQ(db.version_store()->List().size(), 1u) << "the branch must not have been dropped";
  auto branch = ReadBranch(db, "b");
  ASSERT_NE(branch, nullptr);
  ExpectForkState(*branch, ResolveNames(db), gids, 1);
}

INSTANTIATE_TEST_SUITE_P(Retention, BranchSnapshotRetentionTest, ::testing::Bool(), [](const auto &info) {
  return info.param ? "WithASnapshotBelowTheFork" : "WithoutASnapshotBelowTheFork";
});
