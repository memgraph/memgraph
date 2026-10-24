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

// Graph Versioning v2: branches are ephemeral, so a restart drops every branch. These tests build a real
// dbms::Database, create branches, destroy it, reconstruct a new Database on the same directory and assert that no
// branch (and no fork pin) survives while main's own data and schema are intact.
#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include <chrono>
#include <filesystem>
#include <fstream>
#include <optional>

#include "dbms/database.hpp"
#include "memory/db_arena.hpp"
#include "storage/v2/inmemory/storage.hpp"
#include "storage/v2/view.hpp"
#include "tests/test_commit_args_helper.hpp"
#include "utils/scheduler.hpp"
#include "versioning/version_store.hpp"

namespace {
constexpr auto kTestSuite = "versioning_durability_restart";
const std::filesystem::path kStorageDirectory{std::filesystem::temp_directory_path() / kTestSuite};

memgraph::storage::Config MakeConfig(bool recover_on_startup) {
  return memgraph::storage::Config{
      .durability = {.storage_directory = kStorageDirectory,
                     .recover_on_startup = recover_on_startup,
                     .snapshot_wal_mode =
                         memgraph::storage::Config::Durability::SnapshotWalMode::PERIODIC_SNAPSHOT_WITH_WAL,
                     // Effectively disable periodic snapshotting so recovery is WAL-only.
                     .snapshot_interval = memgraph::utils::SchedulerInterval{std::chrono::hours(24)}},
  };
}

}  // namespace

class VersioningDurabilityRestartTest : public ::testing::Test {
 protected:
  void SetUp() override { std::filesystem::remove_all(kStorageDirectory); }

  void TearDown() override { std::filesystem::remove_all(kStorageDirectory); }
};

namespace {
struct Gids {
  memgraph::storage::Gid pre_fork;
  memgraph::storage::Gid post_fork;
};

// Creates a vertex, two branches at the same fork point, then a second vertex after the fork. With `with_stray_branch_dir`, a
// leftover versioning/ directory is created to mimic one written by an older build.
Gids PopulateWithBranches(bool with_stray_branch_dir) {
  Gids gids;
  auto config = MakeConfig(/*recover_on_startup=*/false);
  memgraph::dbms::Database db{config};
  const memgraph::memory::DbArenaScope arena_scope{&db.Arena()};

  {
    auto acc = db.Access(memgraph::storage::WRITE);
    gids.pre_fork = acc->CreateVertex().Gid();
    EXPECT_TRUE(acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()).has_value());
  }

  EXPECT_NE(db.version_store(), nullptr);
  EXPECT_TRUE(db.version_store()->CreateBranch("b1", "main", std::nullopt).has_value());
  EXPECT_TRUE(db.version_store()->CreateBranch("b2", "b1", std::nullopt).has_value());
  EXPECT_TRUE(static_cast<memgraph::storage::InMemoryStorage *>(db.storage())->OldestForkPin().has_value());

  {
    auto acc = db.Access(memgraph::storage::WRITE);
    gids.post_fork = acc->CreateVertex().Gid();
    EXPECT_TRUE(acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()).has_value());
  }

  if (with_stray_branch_dir) {
    auto const stray = kStorageDirectory / "versioning" / "branches" / "b1";
    std::filesystem::create_directories(stray);
    std::ofstream{stray / "leftover"} << "x";
  }
  return gids;
}

void ExpectNoBranchState(memgraph::dbms::Database &db) {
  ASSERT_NE(db.version_store(), nullptr);
  EXPECT_TRUE(db.version_store()->Empty());
  EXPECT_TRUE(db.version_store()->List().empty());
  EXPECT_FALSE(db.version_store()->Exists("b1"));
  EXPECT_FALSE(db.version_store()->Exists("b2"));
  EXPECT_FALSE(db.version_store()->TryAcquireCheckout("b1").has_value());
  auto *storage = static_cast<memgraph::storage::InMemoryStorage *>(db.storage());
  EXPECT_FALSE(storage->OldestForkPin().has_value());
  EXPECT_FALSE(std::filesystem::exists(kStorageDirectory / "versioning"));
}
}  // namespace

TEST_F(VersioningDurabilityRestartTest, BranchesAreDroppedOnRestartAndMainIsIntact) {
  auto const gids = PopulateWithBranches(/*with_stray_branch_dir=*/true);

  auto config = MakeConfig(/*recover_on_startup=*/true);
  memgraph::dbms::Database db{config};
  const memgraph::memory::DbArenaScope arena_scope{&db.Arena()};

  ExpectNoBranchState(db);

  {
    auto acc = db.Access(memgraph::storage::READ);
    EXPECT_TRUE(acc->FindVertex(gids.pre_fork, memgraph::storage::View::OLD).has_value());
    EXPECT_TRUE(acc->FindVertex(gids.post_fork, memgraph::storage::View::OLD).has_value());
  }

  // The registry is usable again: a previously used name can be re-created.
  auto branch = db.version_store()->CreateBranch("b1", "main", std::nullopt);
  ASSERT_TRUE(branch.has_value()) << (branch.has_value() ? std::string{} : branch.error());
  auto *storage = static_cast<memgraph::storage::InMemoryStorage *>(db.storage());
  ASSERT_TRUE(storage->OldestForkPin().has_value());
  EXPECT_EQ(*storage->OldestForkPin(), branch->fork_ts);
  ASSERT_TRUE(db.version_store()->DropBranch("b1"));
  EXPECT_FALSE(storage->OldestForkPin().has_value());
}

TEST_F(VersioningDurabilityRestartTest, BranchesAreDroppedOnRestartWithRecoveryDisabled) {
  PopulateWithBranches(/*with_stray_branch_dir=*/false);

  auto config = MakeConfig(/*recover_on_startup=*/false);
  memgraph::dbms::Database db{config};
  const memgraph::memory::DbArenaScope arena_scope{&db.Arena()};

  ExpectNoBranchState(db);
}

// A schema op committed on main after a branch's fork point survives the restart that drops the branch.
TEST_F(VersioningDurabilityRestartTest, MainSchemaOpAfterForkSurvivesRestart) {
  memgraph::storage::Gid post_fork_vertex;
  memgraph::storage::LabelId indexed_label{};

  {
    auto config = MakeConfig(/*recover_on_startup=*/false);
    memgraph::dbms::Database db{config};
    const memgraph::memory::DbArenaScope arena_scope{&db.Arena()};

    ASSERT_NE(db.version_store(), nullptr);
    ASSERT_TRUE(db.version_store()->CreateBranch("b1", "main", std::nullopt).has_value());

    {
      auto acc = db.UniqueAccess();
      indexed_label = acc->NameToLabel("PostForkLabel");
      ASSERT_TRUE(acc->CreateIndex(indexed_label).has_value());
      ASSERT_TRUE(acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()).has_value());
    }
    {
      auto acc = db.Access(memgraph::storage::WRITE);
      post_fork_vertex = acc->CreateVertex().Gid();
      ASSERT_TRUE(acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()).has_value());
    }
  }

  auto config = MakeConfig(/*recover_on_startup=*/true);
  memgraph::dbms::Database db{config};
  const memgraph::memory::DbArenaScope arena_scope{&db.Arena()};

  ExpectNoBranchState(db);
  auto acc = db.Access(memgraph::storage::READ);
  EXPECT_THAT(acc->ListAllIndices().label, ::testing::Contains(indexed_label));
  EXPECT_TRUE(acc->FindVertex(post_fork_vertex, memgraph::storage::View::OLD).has_value());
}

TEST_F(VersioningDurabilityRestartTest, NoBranchRestartIsByteIdenticalFastPath) {
  memgraph::storage::Gid vertex_gid;
  {
    auto config = MakeConfig(/*recover_on_startup=*/false);
    memgraph::dbms::Database db{config};
    const memgraph::memory::DbArenaScope arena_scope{&db.Arena()};
    auto acc = db.Access(memgraph::storage::WRITE);
    auto v = acc->CreateVertex();
    vertex_gid = v.Gid();
    ASSERT_TRUE(acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()).has_value());
  }

  auto config = MakeConfig(/*recover_on_startup=*/true);
  memgraph::dbms::Database db{config};
  const memgraph::memory::DbArenaScope arena_scope{&db.Arena()};


  auto acc = db.Access(memgraph::storage::READ);
  EXPECT_TRUE(acc->FindVertex(vertex_gid, memgraph::storage::View::OLD).has_value());
}
