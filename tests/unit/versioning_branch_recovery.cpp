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

// A restarted dbms::Database replays each branch's diff-engine WAL, so a branch reads exactly as before the restart:
// overlay changes of fork vertices and edges, branch-native vertices and edges, and the anchors that hang native edges
// on fork vertices. The branch's view is captured as plain data before the restart and compared after it.
#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include <algorithm>
#include <atomic>
#include <chrono>
#include <cstdint>
#include <expected>
#include <filesystem>
#include <memory>
#include <optional>
#include <string>
#include <thread>
#include <utility>
#include <vector>

#include "dbms/database.hpp"
#include "memory/db_arena.hpp"
#include "storage/v2/durability/wal.hpp"
#include "storage/v2/inmemory/storage.hpp"
#include "storage/v2/property_value.hpp"
#include "storage/v2/view.hpp"
#include "tests/versioning_startup_refusal.hpp"
#include "versioning/branch_engine.hpp"
#include "versioning/version_store.hpp"
#include "versioning_recovery_support.hpp"

using namespace memgraph::tests::versioning_recovery;  // NOLINT(google-build-using-namespace)

namespace {
const std::filesystem::path kStorageDirectory{std::filesystem::temp_directory_path() / "versioning_branch_recovery"};

ms::Config MakeConfig(bool recover_on_startup, bool versioning = true) {
  return memgraph::tests::versioning_recovery::MakeConfig(kStorageDirectory, recover_on_startup, versioning);
}

std::filesystem::path BranchWalDir(uint64_t number) {
  return memgraph::tests::versioning_recovery::BranchWalDir(kStorageDirectory, number);
}

std::vector<std::filesystem::path> WalFiles(uint64_t number) {
  return memgraph::tests::versioning_recovery::WalFiles(BranchWalDir(number));
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

struct Workload {
  uint64_t max_native_vertex_gid{0};
  uint64_t max_native_edge_gid{0};
};

// Three transactions: a mixed one (overlay writes of fork vertices and a fork edge, native vertices, native edges on
// anchors), a mixed one that deletes (fork vertex, fork edge, native edge and vertex), and an overlay-only one that
// writes keys again.
void RunWorkload(memgraph::dbms::Database &db, mv::BranchContext &ctx, const Base &b, Workload &out) {
  const auto n = ResolveNames(db);
  ms::Gid ne4;
  ms::Gid n3;

  BranchTxn(ctx, [&](mv::BranchContext &c, ms::Storage::Accessor &) {
    ASSERT_TRUE(c.PrepareForkWrite(b.v1).has_value());
    ASSERT_TRUE(c.OverlaySetProperty(b.v1, n.p, ms::PropertyValue(2)).has_value());
    ASSERT_TRUE(c.OverlaySetProperty(b.v1, n.q, ms::PropertyValue()).has_value());
    ASSERT_TRUE(c.OverlayAddLabel(b.v1, n.x).has_value());
    ASSERT_TRUE(c.OverlayRemoveLabel(b.v1, n.l).has_value());

    auto e1 = c.FindForkEdge(b.e1, b.v1);
    ASSERT_TRUE(e1.has_value());
    ASSERT_TRUE(c.PrepareForkEdgeWrite(*e1).has_value());
    ASSERT_TRUE(c.EdgeOverlaySetProperty(*e1, n.w, ms::PropertyValue(9)).has_value());

    auto n1 = c.CreateNativeVertex();
    auto n2 = c.CreateNativeVertex();
    auto n3_vertex = c.CreateNativeVertex();
    n3 = n3_vertex.Gid();
    ASSERT_TRUE(n1.AddLabel(n.n).has_value());
    ASSERT_TRUE(n1.SetProperty(n.a, ms::PropertyValue(1)).has_value());

    auto a4 = c.CowVertex(b.v4);
    auto a3 = c.CowVertex(b.v3);
    ASSERT_TRUE(a4.has_value() && a3.has_value());
    ASSERT_TRUE(c.CreateNativeEdge(&n1, &n2, n.t).has_value());
    auto from_anchor = c.CreateNativeEdge(&*a4, &n1, n.t);
    ASSERT_TRUE(from_anchor.has_value());
    ASSERT_TRUE(from_anchor->SetProperty(n.w, ms::PropertyValue(33)).has_value());
    ASSERT_TRUE(c.CreateNativeEdge(&n2, &*a3, n.t).has_value());
    auto between_anchors = c.CreateNativeEdge(&*a4, &*a3, n.t);
    ASSERT_TRUE(between_anchors.has_value());
    ne4 = between_anchors->Gid();
  });

  BranchTxn(ctx, [&](mv::BranchContext &c, ms::Storage::Accessor &diff) {
    auto deleted = c.OverlayMarkDeleted(b.v5);
    ASSERT_TRUE(deleted.has_value() && *deleted);

    auto e2 = c.FindForkEdge(b.e2, b.v3);
    ASSERT_TRUE(e2.has_value());
    auto edge_deleted = c.DeleteForkEdge(*e2);
    ASSERT_TRUE(edge_deleted.has_value() && *edge_deleted);

    auto native_edge = c.FindDiffEdge(ne4, ms::View::NEW);
    ASSERT_TRUE(native_edge.has_value());
    ASSERT_TRUE(diff.DeleteEdge(&*native_edge).has_value());
    auto native_vertex = diff.FindVertex(n3, ms::View::NEW);
    ASSERT_TRUE(native_vertex.has_value());
    ASSERT_TRUE(diff.DeleteVertex(&*native_vertex).has_value());
  });

  BranchTxn(ctx, [&](mv::BranchContext &c, ms::Storage::Accessor &) {
    ASSERT_TRUE(c.PrepareForkWrite(b.v2).has_value());
    ASSERT_TRUE(c.OverlayAddLabel(b.v2, n.x).has_value());
    ASSERT_TRUE(c.OverlaySetProperty(b.v2, n.p, ms::PropertyValue(7)).has_value());
    auto e1 = c.FindForkEdge(b.e1, b.v1);
    ASSERT_TRUE(e1.has_value());
    ASSERT_TRUE(c.PrepareForkEdgeWrite(*e1).has_value());
    ASSERT_TRUE(c.EdgeOverlaySetProperty(*e1, n.w, ms::PropertyValue(10)).has_value());
  });

  out.max_native_vertex_gid = n3.AsUint();
  out.max_native_edge_gid = ne4.AsUint();
}

std::optional<ms::Gid> NewMainVertex(memgraph::dbms::Database &db) {
  auto acc = db.Access(ms::WRITE);
  const auto gid = acc->CreateVertex().Gid();
  Commit(*acc);
  return gid;
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
  auto ctx = Checkout(db, "b");
  ASSERT_NE(ctx, nullptr);
  EXPECT_EQ(DumpBranch(db, *ctx), branch_after_second_run);
}

TEST_F(VersioningBranchRecoveryTest, AMissingWalFileRefusesStartup) {
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
    auto ctx = Checkout(db, "b");
    ASSERT_NE(ctx, nullptr);
    SetOnNative(db, *ctx, workload.n1, 2);
  }

  const auto files = WalFiles(number);
  ASSERT_EQ(files.size(), 2u);
  const auto first = std::ranges::find_if(
      files, [](const auto &path) { return memgraph::storage::durability::ReadWalInfo(path).seq_num == 0; });
  ASSERT_NE(first, files.end());
  std::filesystem::remove(*first);

  memgraph::tests::ExpectStartupRefused(
      kStorageDirectory, MakeConfig(/*recover_on_startup=*/true), {"[b]", "missing prefix WAL files"});
}

TEST_F(VersioningBranchRecoveryTest, RestartReproducesTheBranchAndRebuildsItsDerivedState) {
  Base base{};
  Workload workload{};
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
    RunWorkload(db, *ctx, base, workload);
    branch_before = DumpBranch(db, *ctx);
    main_before = DumpMain(db);
  }
  ASSERT_EQ(branch_before.size(), 6u) << "4 fork vertices left, 2 native vertices";
  ASSERT_EQ(main_before.size(), 5u);

  memgraph::dbms::Database db{MakeConfig(/*recover_on_startup=*/true)};
  const memgraph::memory::DbArenaScope arena_scope{&db.Arena()};
  const auto n = ResolveNames(db);
  ASSERT_EQ(db.version_store()->List().size(), 1u);
  EXPECT_EQ(db.version_store()->Get("b")->number, number);

  bool built = false;
  auto ctx = Checkout(db, "b", &built);
  ASSERT_NE(ctx, nullptr);
  EXPECT_FALSE(built) << "the core was already rebuilt at startup";
  EXPECT_EQ(DumpBranch(db, *ctx), branch_before);
  EXPECT_EQ(DumpMain(db), main_before) << "branch writes must not reach main";

  // Filters and touched sets that a read consults to find the branch's changes.
  const auto gid = [](ms::Gid g) { return g.AsUint(); };
  auto &core = *ctx->core();
  EXPECT_TRUE(core.MayHaveLabelTouched(n.x));
  EXPECT_TRUE(core.MayHaveLabelTouched(n.l));
  EXPECT_TRUE(core.LabelTouchedSnapshot(n.x)->contains(gid(base.v1)));
  EXPECT_TRUE(core.LabelTouchedSnapshot(n.x)->contains(gid(base.v2)));
  EXPECT_TRUE(core.LabelTouchedSnapshot(n.l)->contains(gid(base.v1)));
  EXPECT_TRUE(core.MayHavePropertyTouched(n.p));
  EXPECT_TRUE(core.MayHavePropertyTouched(n.q));
  EXPECT_TRUE(core.PropertyTouchedSnapshot(n.p)->contains(gid(base.v1)));
  EXPECT_TRUE(core.PropertyTouchedSnapshot(n.q)->contains(gid(base.v1)));
  EXPECT_TRUE(core.MayHaveLabelChange(base.v1));
  EXPECT_TRUE(core.MayHavePropertyChange(base.v1));
  EXPECT_TRUE(core.MayHavePropertyFieldChange(base.v1, n.p));
  EXPECT_TRUE(core.MayHavePropertyFieldChange(base.v1, n.q));
  EXPECT_TRUE(core.MayHaveAnyVertexDeleted());
  EXPECT_TRUE(core.MayHaveVertexDeleted(base.v5));
  EXPECT_TRUE(core.MayHaveForkEdgeDeleted(base.e2));
  EXPECT_TRUE(core.MayHaveForkEdgePropertyFieldChange(base.e1, n.w));
  EXPECT_TRUE(core.MayHaveEdgeChange(base.v1));
  EXPECT_TRUE(core.MayHaveEdgeChange(base.v3));
  EXPECT_TRUE(core.MayHaveEdgeChange(base.v4)) << "anchor of a native edge";
  const auto touched_edges = core.EdgePropertyTouchedSnapshot(n.w);
  ASSERT_TRUE(touched_edges->contains(gid(base.e1)));
  EXPECT_EQ(touched_edges->at(gid(base.e1)), gid(base.v1)) << "the touched edge carries its FROM vertex";

  // Main's `branched()` hint on every fork object the branch changed or anchored.
  auto fork = Mem(db)->HistoricalAccess(db.version_store()->Get("b")->fork_ts);
  ASSERT_TRUE(fork.has_value());
  for (const auto v : {base.v1, base.v2, base.v3, base.v4, base.v5}) {
    auto vertex = (*fork)->FindVertex(v, ms::View::OLD);
    ASSERT_TRUE(vertex.has_value());
    EXPECT_TRUE(vertex->vertex_->branched()) << "fork vertex " << v.AsUint();
  }
  auto from = (*fork)->FindVertex(base.v1, ms::View::OLD);
  auto out = from->OutEdges(ms::View::OLD);
  ASSERT_TRUE(out.has_value() && out->edges.size() == 1u);
  EXPECT_TRUE(out->edges[0].edge_.ptr->branched());

  // A new main object never takes a gid a branch uses, and a branch's new object never takes main's.
  const auto main_gid = NewMainVertex(db);
  ASSERT_TRUE(main_gid.has_value());
  EXPECT_GT(main_gid->AsUint(), workload.max_native_vertex_gid);
  ms::Gid native_after;
  BranchTxn(*ctx, [&](mv::BranchContext &c, ms::Storage::Accessor &) { native_after = c.CreateNativeVertex().Gid(); });
  EXPECT_GT(native_after.AsUint(), main_gid->AsUint());
  EXPECT_GE(Mem(db)->edge_id_.load(), workload.max_native_edge_gid + 1);
}

TEST_F(VersioningBranchRecoveryTest, WritesAfterARestartSurviveTheNextOne) {
  Base base{};
  Workload workload{};
  uint64_t number = 0;
  {
    memgraph::dbms::Database db{MakeConfig(/*recover_on_startup=*/false)};
    const memgraph::memory::DbArenaScope arena_scope{&db.Arena()};
    base = CreateBase(db);
    auto info = db.version_store()->CreateBranch("b", "main");
    ASSERT_TRUE(info.has_value());
    number = info->number;
    auto ctx = Checkout(db, "b");
    ASSERT_NE(ctx, nullptr);
    RunWorkload(db, *ctx, base, workload);
  }
  const auto first_run_files = WalFiles(number).size();
  ASSERT_GE(first_run_files, 1u);

  std::vector<VertexDump> branch_before_second_restart;
  {
    memgraph::dbms::Database db{MakeConfig(/*recover_on_startup=*/true)};
    const memgraph::memory::DbArenaScope arena_scope{&db.Arena()};
    const auto n = ResolveNames(db);
    auto ctx = Checkout(db, "b");
    ASSERT_NE(ctx, nullptr);
    BranchTxn(*ctx, [&](mv::BranchContext &c, ms::Storage::Accessor &) {
      // Overwrites a head recovered from the first run, deletes a fork edge whose overlay holds properties, and
      // creates natives hanging on a recovered anchor.
      ASSERT_TRUE(c.PrepareForkWrite(base.v3).has_value());
      ASSERT_TRUE(c.OverlaySetProperty(base.v3, n.p, ms::PropertyValue(8)).has_value());
      ASSERT_TRUE(c.OverlaySetProperty(base.v1, n.p, ms::PropertyValue(3)).has_value());
      auto e1 = c.FindForkEdge(base.e1, base.v1);
      ASSERT_TRUE(e1.has_value());
      auto deleted = c.DeleteForkEdge(*e1);
      ASSERT_TRUE(deleted.has_value() && *deleted);
      auto n4 = c.CreateNativeVertex();
      auto a2 = c.CowVertex(base.v2);
      ASSERT_TRUE(a2.has_value());
      ASSERT_TRUE(c.CreateNativeEdge(&n4, &*a2, n.t).has_value());
    });
    branch_before_second_restart = DumpBranch(db, *ctx);
    const auto *v1 = FindVertex(branch_before_second_restart, base.v1);
    ASSERT_NE(v1, nullptr);
    EXPECT_EQ(v1->props.at("p"), ms::PropertyValue(3));
    EXPECT_TRUE(v1->out.empty()) << "e1 was deleted on the branch";
  }
  EXPECT_GT(WalFiles(number).size(), first_run_files) << "the second run continues the WAL in a new file";

  memgraph::dbms::Database db{MakeConfig(/*recover_on_startup=*/true)};
  const memgraph::memory::DbArenaScope arena_scope{&db.Arena()};
  auto ctx = Checkout(db, "b");
  ASSERT_NE(ctx, nullptr);
  EXPECT_EQ(DumpBranch(db, *ctx), branch_before_second_restart);
}

TEST_F(VersioningBranchRecoveryTest, TwoBranchesRecoverIndependently) {
  Base base{};
  Workload workload{};
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
    RunWorkload(db, *b, base, workload);
    BranchTxn(*c, [&](mv::BranchContext &ctx, ms::Storage::Accessor &) {
      ASSERT_TRUE(ctx.PrepareForkWrite(base.v4).has_value());
      ASSERT_TRUE(ctx.OverlaySetProperty(base.v4, n.p, ms::PropertyValue(100)).has_value());
      auto native = ctx.CreateNativeVertex();
      auto anchor = ctx.CowVertex(base.v2);
      ASSERT_TRUE(anchor.has_value());
      ASSERT_TRUE(ctx.CreateNativeEdge(&*anchor, &native, n.t).has_value());
      auto e1 = ctx.FindForkEdge(base.e1, base.v1);
      ASSERT_TRUE(e1.has_value());
      auto deleted = ctx.DeleteForkEdge(*e1);
      ASSERT_TRUE(deleted.has_value() && *deleted);
    });
    b_before = DumpBranch(db, *b);
    c_before = DumpBranch(db, *c);
  }
  ASSERT_NE(b_before, c_before);

  memgraph::dbms::Database db{MakeConfig(/*recover_on_startup=*/true)};
  const memgraph::memory::DbArenaScope arena_scope{&db.Arena()};
  ASSERT_EQ(db.version_store()->List().size(), 2u);
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

TEST_F(VersioningBranchRecoveryTest, ACorruptBranchWalRefusesStartup) {
  Base base{};
  Workload workload{};
  uint64_t b_number = 0;
  {
    memgraph::dbms::Database db{MakeConfig(/*recover_on_startup=*/false)};
    const memgraph::memory::DbArenaScope arena_scope{&db.Arena()};
    base = CreateBase(db);
    auto b_info = db.version_store()->CreateBranch("b", "main");
    ASSERT_TRUE(b_info.has_value());
    b_number = b_info->number;
    ASSERT_TRUE(db.version_store()->CreateBranch("c", "main").has_value());
    auto b = Checkout(db, "b");
    auto c = Checkout(db, "c");
    ASSERT_NE(b, nullptr);
    ASSERT_NE(c, nullptr);
    RunWorkload(db, *b, base, workload);
    BranchTxn(*c, [&](mv::BranchContext &ctx, ms::Storage::Accessor &) {
      ASSERT_TRUE(ctx.PrepareForkWrite(base.v4).has_value());
      ASSERT_TRUE(ctx.OverlaySetProperty(base.v4, ResolveNames(db).p, ms::PropertyValue(5)).has_value());
    });
  }

  ASSERT_GE(WalFiles(b_number).size(), 1u);
  for (const auto &entry : std::filesystem::directory_iterator(BranchWalDir(b_number))) {
    if (entry.is_regular_file()) std::filesystem::resize_file(entry.path(), entry.file_size() / 2);
  }

  memgraph::tests::ExpectStartupRefused(
      kStorageDirectory, MakeConfig(/*recover_on_startup=*/true), {"Database '", "[b]", "branch 'b'", "DROP BRANCH"});
}

TEST_F(VersioningBranchRecoveryTest, ConcurrentFirstCheckoutsBuildTheBranchOnce) {
  Base base{};
  Workload workload{};
  std::vector<VertexDump> branch_before;
  {
    memgraph::dbms::Database db{MakeConfig(/*recover_on_startup=*/false)};
    const memgraph::memory::DbArenaScope arena_scope{&db.Arena()};
    base = CreateBase(db);
    auto info = db.version_store()->CreateBranch("b", "main");
    ASSERT_TRUE(info.has_value());
    auto *store = db.version_store();

    std::atomic<int> builds{0};
    const auto build = [&]() -> std::expected<std::shared_ptr<mv::BranchCore>, std::string> {
      builds.fetch_add(1);
      std::this_thread::sleep_for(std::chrono::milliseconds(100));
      auto fresh = mv::BranchCore::Build(*Mem(db), info->fork_ts, info->number);
      if (!fresh) return std::unexpected(fresh.error().message);
      return std::move(*fresh);
    };
    std::expected<std::shared_ptr<mv::BranchCore>, std::string> core_a;
    std::expected<std::shared_ptr<mv::BranchCore>, std::string> core_b;
    {
      std::jthread ta{[&] {
        const memgraph::memory::DbArenaScope scope{&db.Arena()};
        core_a = store->GetOrBuildCore(info->number, build);
      }};
      std::jthread tb{[&] {
        const memgraph::memory::DbArenaScope scope{&db.Arena()};
        core_b = store->GetOrBuildCore(info->number, build);
      }};
    }
    ASSERT_TRUE(core_a.has_value());
    ASSERT_TRUE(core_b.has_value());
    EXPECT_EQ(builds.load(), 1);
    EXPECT_EQ(core_a->get(), core_b->get());

    ASSERT_TRUE(
        store->RecordGidFloors(info->number, (*core_a)->vertex_gid_floor(), (*core_a)->edge_gid_floor()).has_value());
    mv::BranchContext ctx{*core_a};
    RunWorkload(db, ctx, base, workload);
    branch_before = DumpBranch(db, ctx);
  }

  memgraph::dbms::Database db{MakeConfig(/*recover_on_startup=*/true)};
  const memgraph::memory::DbArenaScope arena_scope{&db.Arena()};
  auto ctx = Checkout(db, "b");
  ASSERT_NE(ctx, nullptr);
  EXPECT_EQ(DumpBranch(db, *ctx), branch_before);
}

TEST_F(VersioningBranchRecoveryTest, BranchesComeBackAfterARunWithVersioningOff) {
  Base base{};
  Workload workload{};
  std::vector<VertexDump> branch_before;
  std::vector<VertexDump> main_before;
  {
    memgraph::dbms::Database db{MakeConfig(/*recover_on_startup=*/false)};
    const memgraph::memory::DbArenaScope arena_scope{&db.Arena()};
    base = CreateBase(db);
    ASSERT_TRUE(db.version_store()->CreateBranch("b", "main").has_value());
    auto ctx = Checkout(db, "b");
    ASSERT_NE(ctx, nullptr);
    RunWorkload(db, *ctx, base, workload);
    branch_before = DumpBranch(db, *ctx);
    main_before = DumpMain(db);
  }

  std::vector<VertexDump> main_after_off_run;
  {
    memgraph::dbms::Database db{MakeConfig(/*recover_on_startup=*/true, /*versioning=*/false)};
    const memgraph::memory::DbArenaScope arena_scope{&db.Arena()};
    const auto n = ResolveNames(db);
    ASSERT_NE(db.version_store(), nullptr);
    EXPECT_TRUE(db.version_store()->List().empty());
    EXPECT_FALSE(db.version_store()->Empty());
    EXPECT_EQ(DumpMain(db), main_before);

    {
      auto acc = db.Access(ms::WRITE);
      auto v1 = acc->FindVertex(base.v1, ms::View::OLD);
      auto v3 = acc->FindVertex(base.v3, ms::View::OLD);
      ASSERT_TRUE(v1.has_value() && v3.has_value());
      ASSERT_TRUE(v1->SetProperty(n.p, ms::PropertyValue(50)).has_value());
      ASSERT_TRUE(acc->CreateEdge(&*v1, &*v3, n.t).has_value());
      acc->CreateVertex();
      Commit(*acc);
    }
    main_after_off_run = DumpMain(db);
    ASSERT_NE(main_after_off_run, main_before);
  }

  memgraph::dbms::Database db{MakeConfig(/*recover_on_startup=*/true)};
  const memgraph::memory::DbArenaScope arena_scope{&db.Arena()};
  ASSERT_EQ(db.version_store()->List().size(), 1u);
  auto ctx = Checkout(db, "b");
  ASSERT_NE(ctx, nullptr);
  EXPECT_EQ(DumpBranch(db, *ctx), branch_before) << "the branch must show its fork state plus its own writes only";
  EXPECT_EQ(DumpMain(db), main_after_off_run);
}

// A fork taken with no main commit after it sits above the durable timestamp; commits made while the branch is hidden
// must still land above the fork point or they would show up in the branch.
TEST_F(VersioningBranchRecoveryTest, MainCommitsWhileHiddenStayOutOfTheBranch) {
  Base base{};
  uint64_t fork_ts = 0;
  {
    memgraph::dbms::Database db{MakeConfig(/*recover_on_startup=*/false)};
    const memgraph::memory::DbArenaScope arena_scope{&db.Arena()};
    base = CreateBase(db);
    for (int i = 0; i < 10; ++i) db.Access(ms::READ)->Abort();
    const auto info = db.version_store()->CreateBranch("b", "main");
    ASSERT_TRUE(info.has_value());
    fork_ts = info->fork_ts;
  }

  ms::Gid hidden_run_vertex;
  {
    memgraph::dbms::Database db{MakeConfig(/*recover_on_startup=*/true, /*versioning=*/false)};
    const memgraph::memory::DbArenaScope arena_scope{&db.Arena()};
    hidden_run_vertex = *NewMainVertex(db);
    EXPECT_GE(Mem(db)->repl_storage_state_.commit_ts_info_.load().ldt_, fork_ts)
        << "a commit made while hidden must be stamped at or above the fork point";
  }

  memgraph::dbms::Database db{MakeConfig(/*recover_on_startup=*/true)};
  const memgraph::memory::DbArenaScope arena_scope{&db.Arena()};
  auto info = db.version_store()->Get("b");
  ASSERT_TRUE(info.has_value());
  auto branch = Mem(db)->HistoricalAccess(info->fork_ts);
  ASSERT_TRUE(branch.has_value());
  EXPECT_FALSE((*branch)->FindVertex(hidden_run_vertex, ms::View::OLD).has_value());
  EXPECT_TRUE((*branch)->FindVertex(base.v1, ms::View::OLD).has_value());
  EXPECT_TRUE(db.Access(ms::READ)->FindVertex(hidden_run_vertex, ms::View::OLD).has_value());
}

TEST_F(VersioningBranchRecoveryTest, MainIdsWhileHiddenClearTheBranchNativeIds) {
  Base base{};
  Workload workload{};
  {
    memgraph::dbms::Database db{MakeConfig(/*recover_on_startup=*/false)};
    const memgraph::memory::DbArenaScope arena_scope{&db.Arena()};
    base = CreateBase(db);
    ASSERT_TRUE(db.version_store()->CreateBranch("b", "main").has_value());
    auto ctx = Checkout(db, "b");
    ASSERT_NE(ctx, nullptr);
    RunWorkload(db, *ctx, base, workload);
  }

  memgraph::dbms::Database db{MakeConfig(/*recover_on_startup=*/true, /*versioning=*/false)};
  const memgraph::memory::DbArenaScope arena_scope{&db.Arena()};
  const auto n = ResolveNames(db);
  const auto vertex = NewMainVertex(db);
  ASSERT_TRUE(vertex.has_value());
  EXPECT_GT(vertex->AsUint(), workload.max_native_vertex_gid);
  auto acc = db.Access(ms::WRITE);
  auto from = acc->FindVertex(base.v1, ms::View::OLD);
  auto to = acc->FindVertex(base.v3, ms::View::OLD);
  ASSERT_TRUE(from.has_value() && to.has_value());
  const auto edge = acc->CreateEdge(&*from, &*to, n.t);
  ASSERT_TRUE(edge.has_value());
  EXPECT_GT(edge->Gid().AsUint(), workload.max_native_edge_gid);
  acc->Abort();
}
