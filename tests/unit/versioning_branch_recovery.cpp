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

// A branch's native vertices and edges come back after a restart of the dbms::Database by replaying its diff-engine
// WAL, including the anchors that hang native edges on fork vertices. The branch's view is captured as plain data
// before the restart and compared after it.
#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include <algorithm>
#include <cstdint>
#include <expected>
#include <filesystem>
#include <memory>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include "dbms/database.hpp"
#include "memory/db_arena.hpp"
#include "storage/v2/durability/wal.hpp"
#include "storage/v2/inmemory/storage.hpp"
#include "storage/v2/property_value.hpp"
#include "storage/v2/view.hpp"
#include "versioning/branch_engine.hpp"
#include "versioning/branch_recovery.hpp"
#include "versioning/version_store.hpp"
#include "versioning_recovery_support.hpp"

using namespace memgraph::tests::versioning_recovery;  // NOLINT(google-build-using-namespace)

namespace {
const std::filesystem::path kStorageDirectory{std::filesystem::temp_directory_path() / "versioning_branch_recovery"};

ms::Config MakeConfig(bool recover_on_startup) {
  return memgraph::tests::versioning_recovery::MakeConfig(kStorageDirectory, recover_on_startup);
}

std::filesystem::path BranchWalDir(uint64_t number) {
  return memgraph::tests::versioning_recovery::BranchWalDir(kStorageDirectory, number);
}

std::vector<std::filesystem::path> WalFiles(uint64_t number) {
  std::vector<std::filesystem::path> files;
  if (!std::filesystem::exists(BranchWalDir(number))) return files;
  for (const auto &entry : std::filesystem::directory_iterator(BranchWalDir(number))) {
    if (entry.is_regular_file()) files.push_back(entry.path());
  }
  std::ranges::sort(files);
  return files;
}

// Builds and registers `name`'s core the way restart recovery does, from its recorded floors with its WAL replayed, so
// a later Checkout attaches to it.
std::expected<void, std::string> Recover(memgraph::dbms::Database &db, std::string_view name) {
  auto *store = db.version_store();
  const auto info = store->Get(name);
  if (!info || !info->vertex_gid_floor || !info->edge_gid_floor) return std::unexpected("no recorded floors");
  auto core = store->GetOrBuildCore(info->number, [&]() -> std::expected<std::shared_ptr<mv::BranchCore>, std::string> {
    auto built = mv::BranchCore::Build(*Mem(db),
                                       info->fork_ts,
                                       info->number,
                                       mv::BranchCore::RecordedFloors{.vertex_gid_floor = *info->vertex_gid_floor,
                                                                      .edge_gid_floor = *info->edge_gid_floor});
    if (!built) return std::unexpected(built.error().message);
    if (auto replayed = mv::ReplayBranchWal(**built, BranchWalDir(info->number)); !replayed) {
      return std::unexpected(replayed.error());
    }
    return std::move(*built);
  });
  if (!core) return std::unexpected(core.error());
  return {};
}

struct NativeWorkload {
  ms::Gid n1;
  ms::Gid n2;
};

// Every native delta kind in two transactions: vertices with labels and properties, edges between natives and on
// anchors of fork vertices (one with a property); then a property on an edge of the first transaction, a label removal
// and the deletes of an anchor-to-anchor edge and of a vertex.
NativeWorkload RunNativeWorkload(memgraph::dbms::Database &db, mv::BranchContext &ctx, const Base &b) {
  const auto n = ResolveNames(db);
  NativeWorkload out{};
  ms::Gid n3;
  ms::Gid n1_to_n2;
  ms::Gid between_anchors;

  BranchTxn(ctx, [&](mv::BranchContext &c, ms::Storage::Accessor &) {
    auto n1 = c.CreateNativeVertex();
    auto n2 = c.CreateNativeVertex();
    n3 = c.CreateNativeVertex().Gid();
    out = {n1.Gid(), n2.Gid()};
    ASSERT_TRUE(n1.AddLabel(n.n).has_value());
    ASSERT_TRUE(n1.AddLabel(n.x).has_value());
    ASSERT_TRUE(n1.SetProperty(n.a, ms::PropertyValue(1)).has_value());

    auto a4 = c.CowVertex(b.v4);
    auto a3 = c.CowVertex(b.v3);
    ASSERT_TRUE(a4.has_value() && a3.has_value());
    auto native = c.CreateNativeEdge(&n1, &n2, n.t);
    ASSERT_TRUE(native.has_value());
    n1_to_n2 = native->Gid();
    auto from_anchor = c.CreateNativeEdge(&*a4, &n1, n.t);
    ASSERT_TRUE(from_anchor.has_value());
    ASSERT_TRUE(from_anchor->SetProperty(n.w, ms::PropertyValue(33)).has_value());
    ASSERT_TRUE(c.CreateNativeEdge(&n2, &*a3, n.t).has_value());
    auto anchors = c.CreateNativeEdge(&*a4, &*a3, n.t);
    ASSERT_TRUE(anchors.has_value());
    between_anchors = anchors->Gid();
  });

  BranchTxn(ctx, [&](mv::BranchContext &c, ms::Storage::Accessor &diff) {
    auto edge = c.FindDiffEdge(n1_to_n2, ms::View::NEW);
    ASSERT_TRUE(edge.has_value());
    ASSERT_TRUE(edge->SetProperty(n.w, ms::PropertyValue(7)).has_value());
    auto n1 = diff.FindVertex(out.n1, ms::View::NEW);
    ASSERT_TRUE(n1.has_value());
    ASSERT_TRUE(n1->RemoveLabel(n.x).has_value());
    auto anchors = c.FindDiffEdge(between_anchors, ms::View::NEW);
    ASSERT_TRUE(anchors.has_value());
    ASSERT_TRUE(diff.DeleteEdge(&*anchors).has_value());
    auto vertex = diff.FindVertex(n3, ms::View::NEW);
    ASSERT_TRUE(vertex.has_value());
    ASSERT_TRUE(diff.DeleteVertex(&*vertex).has_value());
  });
  return out;
}

// Changes a replayed native vertex without allocating a gid.
void SetOnNative(memgraph::dbms::Database &db, mv::BranchContext &ctx, ms::Gid gid, int64_t value) {
  const auto a = ResolveNames(db).a;
  BranchTxn(ctx, [&](mv::BranchContext &, ms::Storage::Accessor &diff) {
    auto vertex = diff.FindVertex(gid, ms::View::NEW);
    ASSERT_TRUE(vertex.has_value());
    ASSERT_TRUE(vertex->SetProperty(a, ms::PropertyValue(value)).has_value());
  });
}
}  // namespace

class VersioningBranchRecoveryTest : public ::testing::Test {
 protected:
  void SetUp() override { std::filesystem::remove_all(kStorageDirectory); }

  void TearDown() override { std::filesystem::remove_all(kStorageDirectory); }
};

TEST_F(VersioningBranchRecoveryTest, NativeWritesReplayAcrossRestarts) {
  Base base{};
  NativeWorkload workload{};
  uint64_t number = 0;
  std::vector<VertexDump> branch_before;
  std::vector<VertexDump> main_before;
  {
    memgraph::dbms::Database db{MakeConfig(/*recover_on_startup=*/false)};
    const memgraph::memory::DbArenaScope arena_scope{&db.Arena()};
    base = CreateBase(db);
    auto info = db.version_store()->CreateBranch("b", "main");
    ASSERT_TRUE(info.has_value());
    number = info->number;
    auto ctx = Checkout(db, "b");
    ASSERT_NE(ctx, nullptr);
    workload = RunNativeWorkload(db, *ctx, base);
    branch_before = DumpBranch(db, *ctx);
    main_before = DumpMain(db);
  }
  ASSERT_EQ(branch_before.size(), 7u) << "5 fork vertices, 2 native vertices";
  const auto first_run_files = WalFiles(number).size();
  ASSERT_GE(first_run_files, 1u);

  std::vector<VertexDump> branch_after_second_run;
  {
    memgraph::dbms::Database db{MakeConfig(/*recover_on_startup=*/true)};
    const memgraph::memory::DbArenaScope arena_scope{&db.Arena()};
    ASSERT_TRUE(Recover(db, "b").has_value());
    bool built = false;
    auto ctx = Checkout(db, "b", &built);
    ASSERT_NE(ctx, nullptr);
    EXPECT_FALSE(built);
    EXPECT_EQ(DumpBranch(db, *ctx), branch_before);
    EXPECT_EQ(DumpMain(db), main_before) << "branch writes must not reach main";

    // The replay marks the anchored fork vertices the way CowVertex does.
    EXPECT_TRUE(ctx->MayHaveEdgeChange(base.v3));
    EXPECT_TRUE(ctx->MayHaveEdgeChange(base.v4));
    auto fork = Mem(db)->HistoricalAccess(db.version_store()->Get("b")->fork_ts);
    ASSERT_TRUE(fork.has_value());
    for (const auto v : {base.v3, base.v4}) {
      auto vertex = (*fork)->FindVertex(v, ms::View::OLD);
      ASSERT_TRUE(vertex.has_value());
      EXPECT_TRUE(vertex->vertex_->branched()) << "fork vertex " << v.AsUint();
    }

    SetOnNative(db, *ctx, workload.n2, 5);
    branch_after_second_run = DumpBranch(db, *ctx);
    ASSERT_NE(branch_after_second_run, branch_before);
  }
  EXPECT_GT(WalFiles(number).size(), first_run_files) << "the second run continues the WAL in a new file";

  memgraph::dbms::Database db{MakeConfig(/*recover_on_startup=*/true)};
  const memgraph::memory::DbArenaScope arena_scope{&db.Arena()};
  ASSERT_TRUE(Recover(db, "b").has_value());
  auto ctx = Checkout(db, "b");
  ASSERT_NE(ctx, nullptr);
  EXPECT_EQ(DumpBranch(db, *ctx), branch_after_second_run);
}

TEST_F(VersioningBranchRecoveryTest, AMissingWalFileFailsTheReplayAndKeepsTheOtherFiles) {
  uint64_t number = 0;
  NativeWorkload workload{};
  {
    memgraph::dbms::Database db{MakeConfig(/*recover_on_startup=*/false)};
    const memgraph::memory::DbArenaScope arena_scope{&db.Arena()};
    const auto base = CreateBase(db);
    auto info = db.version_store()->CreateBranch("b", "main");
    ASSERT_TRUE(info.has_value());
    number = info->number;
    auto ctx = Checkout(db, "b");
    ASSERT_NE(ctx, nullptr);
    workload = RunNativeWorkload(db, *ctx, base);
  }
  {
    memgraph::dbms::Database db{MakeConfig(/*recover_on_startup=*/true)};
    const memgraph::memory::DbArenaScope arena_scope{&db.Arena()};
    ASSERT_TRUE(Recover(db, "b").has_value());
    auto ctx = Checkout(db, "b");
    ASSERT_NE(ctx, nullptr);
    SetOnNative(db, *ctx, workload.n1, 2);
  }

  auto files = WalFiles(number);
  ASSERT_EQ(files.size(), 2u);
  const auto first = std::ranges::find_if(
      files, [](const auto &path) { return memgraph::storage::durability::ReadWalInfo(path).seq_num == 0; });
  ASSERT_NE(first, files.end());
  std::filesystem::remove(*first);
  files.erase(first);

  memgraph::dbms::Database db{MakeConfig(/*recover_on_startup=*/true)};
  const memgraph::memory::DbArenaScope arena_scope{&db.Arena()};
  const auto recovered = Recover(db, "b");
  ASSERT_FALSE(recovered.has_value());
  EXPECT_THAT(recovered.error(), ::testing::HasSubstr("missing prefix WAL files"));
  EXPECT_EQ(WalFiles(number), files) << "a failed replay leaves the branch's files as they are";
}

TEST_F(VersioningBranchRecoveryTest, TwoBranchesRecoverIndependently) {
  Base base{};
  std::vector<VertexDump> b_before;
  std::vector<VertexDump> c_before;
  {
    memgraph::dbms::Database db{MakeConfig(/*recover_on_startup=*/false)};
    const memgraph::memory::DbArenaScope arena_scope{&db.Arena()};
    const auto n = ResolveNames(db);
    base = CreateBase(db);
    ASSERT_TRUE(db.version_store()->CreateBranch("b", "main").has_value());
    ASSERT_TRUE(db.version_store()->CreateBranch("c", "main").has_value());
    auto b = Checkout(db, "b");
    auto c = Checkout(db, "c");
    ASSERT_NE(b, nullptr);
    ASSERT_NE(c, nullptr);
    RunNativeWorkload(db, *b, base);
    BranchTxn(*c, [&](mv::BranchContext &ctx, ms::Storage::Accessor &) {
      auto native = ctx.CreateNativeVertex();
      ASSERT_TRUE(native.SetProperty(n.p, ms::PropertyValue(100)).has_value());
      auto anchor = ctx.CowVertex(base.v2);
      ASSERT_TRUE(anchor.has_value());
      ASSERT_TRUE(ctx.CreateNativeEdge(&*anchor, &native, n.t).has_value());
    });
    b_before = DumpBranch(db, *b);
    c_before = DumpBranch(db, *c);
  }
  ASSERT_NE(b_before, c_before);

  memgraph::dbms::Database db{MakeConfig(/*recover_on_startup=*/true)};
  const memgraph::memory::DbArenaScope arena_scope{&db.Arena()};
  ASSERT_EQ(db.version_store()->List().size(), 2u);
  ASSERT_TRUE(Recover(db, "b").has_value());
  ASSERT_TRUE(Recover(db, "c").has_value());
  auto b = Checkout(db, "b");
  auto c = Checkout(db, "c");
  ASSERT_NE(b, nullptr);
  ASSERT_NE(c, nullptr);
  EXPECT_EQ(DumpBranch(db, *b), b_before);
  EXPECT_EQ(DumpBranch(db, *c), c_before);
}

TEST_F(VersioningBranchRecoveryTest, ABranchWithoutWritesIsStillBuiltOnFirstCheckout) {
  Base base{};
  {
    memgraph::dbms::Database db{MakeConfig(/*recover_on_startup=*/false)};
    const memgraph::memory::DbArenaScope arena_scope{&db.Arena()};
    base = CreateBase(db);
    ASSERT_TRUE(db.version_store()->CreateBranch("b", "main").has_value());
    auto acc = db.Access(ms::WRITE);
    auto v1 = acc->FindVertex(base.v1, ms::View::OLD);
    ASSERT_TRUE(v1.has_value());
    ASSERT_TRUE(v1->SetProperty(ResolveNames(db).p, ms::PropertyValue(50)).has_value());
    Commit(*acc);
  }

  memgraph::dbms::Database db{MakeConfig(/*recover_on_startup=*/true)};
  const memgraph::memory::DbArenaScope arena_scope{&db.Arena()};
  bool built = false;
  auto ctx = Checkout(db, "b", &built);
  ASSERT_NE(ctx, nullptr);
  EXPECT_TRUE(built);
  const auto dump = DumpBranch(db, *ctx);
  const auto *v1 = FindVertex(dump, base.v1);
  ASSERT_NE(v1, nullptr);
  EXPECT_EQ(v1->props.at("p"), ms::PropertyValue(1)) << "the branch still reads main at its fork";
}
