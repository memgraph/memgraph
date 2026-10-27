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
#include <map>
#include <memory>
#include <optional>
#include <set>
#include <string>
#include <string_view>
#include <thread>
#include <utility>
#include <vector>

#include "dbms/database.hpp"
#include "memory/db_arena.hpp"
#include "storage/v2/inmemory/storage.hpp"
#include "storage/v2/property_value.hpp"
#include "storage/v2/view.hpp"
#include "tests/test_commit_args_helper.hpp"
#include "utils/scheduler.hpp"
#include "versioning/branch_engine.hpp"
#include "versioning/branch_ledger.hpp"
#include "versioning/version_store.hpp"
#include "versioning/vertex_overlay_store.hpp"

namespace ms = memgraph::storage;
namespace mv = memgraph::versioning;

namespace {
const std::filesystem::path kStorageDirectory{std::filesystem::temp_directory_path() / "versioning_branch_recovery"};

ms::Config MakeConfig(bool recover_on_startup) {
  return ms::Config{
      .durability = {.storage_directory = kStorageDirectory,
                     .recover_on_startup = recover_on_startup,
                     .snapshot_wal_mode = ms::Config::Durability::SnapshotWalMode::PERIODIC_SNAPSHOT_WITH_WAL,
                     // Effectively disable periodic snapshotting so recovery is WAL-only.
                     .snapshot_interval = memgraph::utils::SchedulerInterval{std::chrono::hours(24)}},
  };
}

auto *Mem(memgraph::dbms::Database &db) { return static_cast<ms::InMemoryStorage *>(db.storage()); }

void Commit(ms::Storage::Accessor &acc) {
  ASSERT_TRUE(acc.PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()).has_value());
}

struct Names {
  ms::LabelId l, m, x, n;
  ms::PropertyId p, q, w, a;
  ms::EdgeTypeId t;
};

Names ResolveNames(memgraph::dbms::Database &db) {
  auto *st = db.storage();
  return {st->NameToLabel("L"),
          st->NameToLabel("M"),
          st->NameToLabel("X"),
          st->NameToLabel("N"),
          st->NameToProperty("p"),
          st->NameToProperty("q"),
          st->NameToProperty("w"),
          st->NameToProperty("a"),
          st->NameToEdgeType("T")};
}

// v1(:L {p:1, q:5}) -e1[:T {w:1}]-> v2(:M);  v3(:L) -e2[:T {w:2}]-> v1;  v5(:L) -e3[:T {w:3}]-> v4;
struct Base {
  ms::Gid v1, v2, v3, v4, v5, e1, e2, e3;
};

Base CreateBase(memgraph::dbms::Database &db) {
  const auto n = ResolveNames(db);
  auto acc = db.Access(ms::WRITE);
  auto v1 = acc->CreateVertex();
  auto v2 = acc->CreateVertex();
  auto v3 = acc->CreateVertex();
  auto v4 = acc->CreateVertex();
  auto v5 = acc->CreateVertex();
  EXPECT_TRUE(v1.AddLabel(n.l).has_value());
  EXPECT_TRUE(v1.SetProperty(n.p, ms::PropertyValue(1)).has_value());
  EXPECT_TRUE(v1.SetProperty(n.q, ms::PropertyValue(5)).has_value());
  EXPECT_TRUE(v2.AddLabel(n.m).has_value());
  EXPECT_TRUE(v3.AddLabel(n.l).has_value());
  EXPECT_TRUE(v5.AddLabel(n.l).has_value());
  auto e1 = acc->CreateEdge(&v1, &v2, n.t);
  auto e2 = acc->CreateEdge(&v3, &v1, n.t);
  auto e3 = acc->CreateEdge(&v5, &v4, n.t);
  EXPECT_TRUE(e1.has_value() && e2.has_value() && e3.has_value());
  EXPECT_TRUE(e1->SetProperty(n.w, ms::PropertyValue(1)).has_value());
  EXPECT_TRUE(e2->SetProperty(n.w, ms::PropertyValue(2)).has_value());
  EXPECT_TRUE(e3->SetProperty(n.w, ms::PropertyValue(3)).has_value());
  Base base{v1.Gid(), v2.Gid(), v3.Gid(), v4.Gid(), v5.Gid(), e1->Gid(), e2->Gid(), e3->Gid()};
  Commit(*acc);
  return base;
}

// The branch as a session reads it, by names: vertices in gid order with labels, properties and out edges.
using NamedProps = std::map<std::string, ms::PropertyValue>;

struct EdgeDump {
  uint64_t gid;
  std::string type;
  uint64_t to;
  NamedProps props;
  friend bool operator==(const EdgeDump &, const EdgeDump &) = default;
};

struct VertexDump {
  uint64_t gid;
  std::set<std::string> labels;
  NamedProps props;
  std::vector<EdgeDump> out;
  friend bool operator==(const VertexDump &, const VertexDump &) = default;
};

NamedProps Named(ms::Storage &st, const std::map<ms::PropertyId, ms::PropertyValue> &props) {
  NamedProps named;
  for (const auto &[pid, value] : props) named.emplace(st.PropertyToName(pid), value);
  return named;
}

void ApplyOverlayProps(ms::Storage &st, const mv::OverlayVersion &version, NamedProps &props) {
  for (const auto pid : version.removed_pids()) props.erase(st.PropertyToName(pid));
  for (const auto &[pid, value] : version.properties().Properties())
    props.insert_or_assign(st.PropertyToName(pid), value);
}

void Finish(std::vector<VertexDump> &dump) {
  for (auto &v : dump) {
    std::ranges::sort(v.out, {}, [](const EdgeDump &e) { return e.gid; });
  }
  std::ranges::sort(dump, {}, [](const VertexDump &v) { return v.gid; });
}

std::vector<VertexDump> DumpBranch(memgraph::dbms::Database &db, mv::BranchContext &ctx) {
  auto &st = *db.storage();
  std::vector<VertexDump> dump;
  auto diff_acc = ctx.diff_engine().Access(ms::READ);
  EXPECT_TRUE(ctx.BeginTransaction(diff_acc.get()).has_value());
  for (auto v : ctx.Vertices(ms::View::NEW)) {
    VertexDump d;
    d.gid = v.Gid().AsUint();
    const bool fork = ctx.IsForkVertexGid(v.Gid());
    const auto view = fork ? ms::View::OLD : ms::View::NEW;
    for (const auto label : *v.Labels(view)) d.labels.insert(st.LabelToName(label));
    d.props = Named(st, *v.Properties(view));
    if (fork) {
      if (const auto *version = ctx.ReadOverlayVersion(v.Gid(), ms::View::NEW)) {
        for (const auto label : version->removed_labels()) d.labels.erase(st.LabelToName(label));
        for (const auto label : version->added_labels()) d.labels.insert(st.LabelToName(label));
        ApplyOverlayProps(st, *version, d.props);
      }
    }
    for (const auto &e : ctx.ResolveEdges(v.Gid(), ms::EdgeDirection::OUT, ms::View::NEW)) {
      EdgeDump ed;
      ed.gid = e.Gid().AsUint();
      ed.type = st.EdgeTypeToName(e.EdgeType());
      ed.to = e.ToVertex().Gid().AsUint();
      const bool fork_edge = ctx.IsForkEdgeGid(e.Gid());
      ed.props = Named(st, *e.Properties(fork_edge ? ms::View::OLD : ms::View::NEW));
      if (fork_edge) {
        if (const auto *version = ctx.ReadEdgeOverlayVersion(e.Gid(), ms::View::NEW)) {
          ApplyOverlayProps(st, *version, ed.props);
        }
      }
      d.out.push_back(std::move(ed));
    }
    dump.push_back(std::move(d));
  }
  ctx.EndTransaction();
  Finish(dump);
  return dump;
}

std::vector<VertexDump> DumpMain(memgraph::dbms::Database &db) {
  auto &st = *db.storage();
  std::vector<VertexDump> dump;
  auto acc = db.Access(ms::READ);
  for (auto v : acc->Vertices(ms::View::OLD)) {
    VertexDump d;
    d.gid = v.Gid().AsUint();
    for (const auto label : *v.Labels(ms::View::OLD)) d.labels.insert(st.LabelToName(label));
    d.props = Named(st, *v.Properties(ms::View::OLD));
    for (const auto &e : v.OutEdges(ms::View::OLD)->edges) {
      d.out.push_back({.gid = e.Gid().AsUint(),
                       .type = st.EdgeTypeToName(e.EdgeType()),
                       .to = e.ToVertex().Gid().AsUint(),
                       .props = Named(st, *e.Properties(ms::View::OLD))});
    }
    dump.push_back(std::move(d));
  }
  Finish(dump);
  return dump;
}

const VertexDump *FindVertex(const std::vector<VertexDump> &dump, ms::Gid gid) {
  const auto it = std::ranges::find(dump, gid.AsUint(), &VertexDump::gid);
  return it == dump.end() ? nullptr : &*it;
}

// Attaches to `name` the way CHECKOUT does: the registry's core, built on first use, with its floors recorded.
std::unique_ptr<mv::BranchContext> Checkout(memgraph::dbms::Database &db, std::string_view name,
                                            bool *built = nullptr) {
  auto *store = db.version_store();
  auto info = store->Get(name);
  EXPECT_TRUE(info.has_value());
  if (!info) return nullptr;
  auto core = store->GetOrBuildCore(info->number, [&]() -> std::expected<std::shared_ptr<mv::BranchCore>, std::string> {
    if (built != nullptr) *built = true;
    auto fresh = mv::BranchCore::Build(*Mem(db), info->fork_ts, info->number);
    if (!fresh) return std::unexpected(fresh.error().message);
    return std::move(*fresh);
  });
  EXPECT_TRUE(core.has_value());
  if (!core) return nullptr;
  EXPECT_TRUE(store->RecordGidFloors(info->number, (*core)->vertex_gid_floor(), (*core)->edge_gid_floor()).has_value());
  return std::make_unique<mv::BranchContext>(*core, std::string{name});
}

// Runs `body` as one branch transaction that commits. `diff` is the transaction's diff-engine accessor.
template <typename Body>
void BranchTxn(mv::BranchContext &ctx, Body &&body) {
  auto diff_acc = ctx.diff_engine().Access(ms::WRITE);
  ASSERT_TRUE(ctx.BeginTransaction(diff_acc.get()).has_value());
  body(ctx, *diff_acc);
  ASSERT_TRUE(ctx.CommitTransaction(memgraph::tests::MakeMainCommitArgs()).has_value());
  ctx.EndTransaction();
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

std::filesystem::path BranchWalDir(uint64_t number) {
  return kStorageDirectory / "branches" / std::to_string(number) / "wal";
}

size_t CountWalFiles(uint64_t number) {
  size_t count = 0;
  if (!std::filesystem::exists(BranchWalDir(number))) return 0;
  for (const auto &entry : std::filesystem::directory_iterator(BranchWalDir(number))) {
    count += entry.is_regular_file() ? 1 : 0;
  }
  return count;
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
    auto info = db.version_store()->CreateBranch("b", "main", std::nullopt);
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
  EXPECT_TRUE(ctx->MayHaveLabelTouched(n.x));
  EXPECT_TRUE(ctx->MayHaveLabelTouched(n.l));
  EXPECT_TRUE(ctx->LabelTouchedSnapshot(n.x)->contains(gid(base.v1)));
  EXPECT_TRUE(ctx->LabelTouchedSnapshot(n.x)->contains(gid(base.v2)));
  EXPECT_TRUE(ctx->LabelTouchedSnapshot(n.l)->contains(gid(base.v1)));
  EXPECT_TRUE(ctx->MayHavePropertyTouched(n.p));
  EXPECT_TRUE(ctx->MayHavePropertyTouched(n.q));
  EXPECT_TRUE(ctx->PropertyTouchedSnapshot(n.p)->contains(gid(base.v1)));
  EXPECT_TRUE(ctx->PropertyTouchedSnapshot(n.q)->contains(gid(base.v1)));
  EXPECT_TRUE(ctx->MayHaveLabelChange(base.v1));
  EXPECT_TRUE(ctx->MayHavePropertyChange(base.v1));
  EXPECT_TRUE(ctx->MayHavePropertyFieldChange(base.v1, n.p));
  EXPECT_TRUE(ctx->MayHavePropertyFieldChange(base.v1, n.q));
  EXPECT_TRUE(ctx->MayHaveAnyVertexDeleted());
  EXPECT_TRUE(ctx->MayHaveVertexDeleted(base.v5));
  EXPECT_TRUE(ctx->MayHaveForkEdgeDeleted(base.e2));
  EXPECT_TRUE(ctx->MayHaveForkEdgePropertyFieldChange(base.e1, n.w));
  EXPECT_TRUE(ctx->MayHaveEdgeChange(base.v1));
  EXPECT_TRUE(ctx->MayHaveEdgeChange(base.v3));
  EXPECT_TRUE(ctx->MayHaveEdgeChange(base.v4)) << "anchor of a native edge";
  const auto touched_edges = ctx->EdgePropertyTouchedSnapshot(n.w);
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
    auto info = db.version_store()->CreateBranch("b", "main", std::nullopt);
    ASSERT_TRUE(info.has_value());
    number = info->number;
    auto ctx = Checkout(db, "b");
    ASSERT_NE(ctx, nullptr);
    RunWorkload(db, *ctx, base, workload);
  }
  const auto first_run_files = CountWalFiles(number);
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
  EXPECT_GT(CountWalFiles(number), first_run_files) << "the second run continues the WAL in a new file";

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
    ASSERT_TRUE(db.version_store()->CreateBranch("b", "main", std::nullopt).has_value());
    ASSERT_TRUE(db.version_store()->CreateBranch("c", "main", std::nullopt).has_value());
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
    ASSERT_TRUE(db.version_store()->CreateBranch("b", "main", std::nullopt).has_value());
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

TEST_F(VersioningBranchRecoveryTest, ACorruptBranchWalDropsThatBranchOnly) {
  Base base{};
  Workload workload{};
  uint64_t b_number = 0;
  std::vector<VertexDump> c_before;
  std::vector<VertexDump> main_before;
  {
    memgraph::dbms::Database db{MakeConfig(/*recover_on_startup=*/false)};
    const memgraph::memory::DbArenaScope arena_scope{&db.Arena()};
    base = CreateBase(db);
    auto b_info = db.version_store()->CreateBranch("b", "main", std::nullopt);
    ASSERT_TRUE(b_info.has_value());
    b_number = b_info->number;
    ASSERT_TRUE(db.version_store()->CreateBranch("c", "main", std::nullopt).has_value());
    auto b = Checkout(db, "b");
    auto c = Checkout(db, "c");
    ASSERT_NE(b, nullptr);
    ASSERT_NE(c, nullptr);
    RunWorkload(db, *b, base, workload);
    BranchTxn(*c, [&](mv::BranchContext &ctx, ms::Storage::Accessor &) {
      ASSERT_TRUE(ctx.PrepareForkWrite(base.v4).has_value());
      ASSERT_TRUE(ctx.OverlaySetProperty(base.v4, ResolveNames(db).p, ms::PropertyValue(5)).has_value());
    });
    c_before = DumpBranch(db, *c);
    main_before = DumpMain(db);
  }

  ASSERT_GE(CountWalFiles(b_number), 1u);
  for (const auto &entry : std::filesystem::directory_iterator(BranchWalDir(b_number))) {
    if (entry.is_regular_file()) std::filesystem::resize_file(entry.path(), entry.file_size() / 2);
  }

  memgraph::dbms::Database db{MakeConfig(/*recover_on_startup=*/true)};
  const memgraph::memory::DbArenaScope arena_scope{&db.Arena()};
  EXPECT_FALSE(db.version_store()->Exists("b"));
  ASSERT_TRUE(db.version_store()->Exists("c"));
  EXPECT_EQ(DumpMain(db), main_before);

  auto c = Checkout(db, "c");
  ASSERT_NE(c, nullptr);
  EXPECT_EQ(DumpBranch(db, *c), c_before);

  auto ledger = mv::BranchLedger{kStorageDirectory / "branches" / "ledger"}.Read();
  ASSERT_TRUE(ledger.has_value() && ledger->has_value());
  ASSERT_EQ((*ledger)->branches.size(), 1u);
  EXPECT_EQ((*ledger)->branches[0].name, "c");
  EXPECT_EQ(Mem(db)->OldestForkPin(), db.version_store()->Get("c")->fork_ts) << "the dropped branch's pin is released";
}

TEST_F(VersioningBranchRecoveryTest, ConcurrentFirstCheckoutsBuildTheBranchOnce) {
  Base base{};
  Workload workload{};
  std::vector<VertexDump> branch_before;
  {
    memgraph::dbms::Database db{MakeConfig(/*recover_on_startup=*/false)};
    const memgraph::memory::DbArenaScope arena_scope{&db.Arena()};
    base = CreateBase(db);
    auto info = db.version_store()->CreateBranch("b", "main", std::nullopt);
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
    mv::BranchContext ctx{*core_a, "b"};
    RunWorkload(db, ctx, base, workload);
    branch_before = DumpBranch(db, ctx);
  }

  memgraph::dbms::Database db{MakeConfig(/*recover_on_startup=*/true)};
  const memgraph::memory::DbArenaScope arena_scope{&db.Arena()};
  auto ctx = Checkout(db, "b");
  ASSERT_NE(ctx, nullptr);
  EXPECT_EQ(DumpBranch(db, *ctx), branch_before);
}
