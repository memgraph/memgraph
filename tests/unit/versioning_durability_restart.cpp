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

// Branches are in-memory only: after restarting a dbms::Database on the same directory no branch or fork pin
// survives, while main's data and schema are intact.
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
#include "versioning/branch_engine.hpp"
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

// `with_stray_branch_dir` leaves a legacy versioning/ directory behind, as an older build would.
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

  auto branch = db.version_store()->CreateBranch("b1", "main", std::nullopt);
  ASSERT_TRUE(branch.has_value()) << (branch.has_value() ? std::string{} : branch.error());
  auto *storage = static_cast<memgraph::storage::InMemoryStorage *>(db.storage());
  ASSERT_TRUE(storage->OldestForkPin().has_value());
  EXPECT_EQ(*storage->OldestForkPin(), branch->fork_ts);
  ASSERT_TRUE(db.version_store()->DropBranch("b1"));
  EXPECT_FALSE(storage->OldestForkPin().has_value());
}

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

// Branch writes live only in the in-memory diff engine; after restart main holds only its own data.
TEST_F(VersioningDurabilityRestartTest, BranchWritesAreNotPersistedAcrossRestart) {
  namespace ms = memgraph::storage;
  constexpr auto kForkLabel = "ForkLabel";
  constexpr auto kBranchLabel = "BranchOnlyLabel";
  constexpr auto kBranchProp = "BranchOnlyProp";
  ms::Gid fork_vertex;

  {
    auto config = MakeConfig(/*recover_on_startup=*/false);
    memgraph::dbms::Database db{config};
    const memgraph::memory::DbArenaScope arena_scope{&db.Arena()};
    auto *storage = static_cast<ms::InMemoryStorage *>(db.storage());
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
      ASSERT_TRUE(acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()).has_value());
    }

    auto info = db.version_store()->CreateBranch("b1", "main", std::nullopt);
    ASSERT_TRUE(info.has_value());
    auto ctx = memgraph::versioning::BranchContext::BuildFromFork(*storage, info->fork_ts);
    ASSERT_TRUE(ctx.has_value());
    {
      auto diff_acc = (*ctx)->diff_engine().Access(ms::WRITE);
      ASSERT_TRUE((*ctx)->BeginTransaction(diff_acc.get()).has_value());
      auto stub = (*ctx)->CowVertex(fork_vertex);
      ASSERT_TRUE(stub.has_value());
      ASSERT_TRUE(stub->AddLabel(branch_label).has_value());
      ASSERT_TRUE(stub->SetProperty(x, ms::PropertyValue(2)).has_value());
      auto c = (*ctx)->CreateNativeVertex();
      auto d = (*ctx)->CreateNativeVertex();
      ASSERT_TRUE(c.AddLabel(branch_label).has_value());
      ASSERT_TRUE(c.SetProperty(branch_prop, ms::PropertyValue(7)).has_value());
      ASSERT_TRUE((*ctx)->CreateNativeEdge(&c, &d, edge_type).has_value());
      ASSERT_TRUE(diff_acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()).has_value());
      (*ctx)->EndTransaction();
    }
  }

  auto config = MakeConfig(/*recover_on_startup=*/true);
  memgraph::dbms::Database db{config};
  const memgraph::memory::DbArenaScope arena_scope{&db.Arena()};
  ExpectNoBranchState(db);

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

  // The diff engine runs with durability off, so it must leave no regular files under its directory.
  auto const diff_dir = kStorageDirectory / "versioning_diff_engine";
  if (std::filesystem::exists(diff_dir)) {
    for (auto const &entry : std::filesystem::recursive_directory_iterator(diff_dir)) {
      EXPECT_FALSE(entry.is_regular_file()) << "diff engine persisted " << entry.path();
    }
  }
}
