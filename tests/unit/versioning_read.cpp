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

// Branch-aware reads through query::VertexAccessor / query::EdgeAccessor. The accessors are minted here from the
// session's BranchContext; branch writes go through the BranchContext API with the change records a query-layer write
// makes.

#include <algorithm>
#include <array>
#include <cstdint>
#include <functional>
#include <map>
#include <string>
#include <vector>

#include <gtest/gtest.h>

#include "auth/models.hpp"
#include "glue/auth_checker.hpp"
#include "glue/communication.hpp"
#include "query/db_accessor.hpp"
#include "query/edge_accessor.hpp"
#include "query/frontend/ast/query/auth_query.hpp"
#include "query/graph.hpp"
#include "query/vertex_accessor.hpp"
#include "storage/v2/hops_limit.hpp"
#include "storage/v2/property_value.hpp"
#include "tests/test_commit_args_helper.hpp"
#include "utils/memory.hpp"
#include "versioning/branch_change_kind.hpp"
#include "versioning/branch_engine.hpp"
#include "versioning_fixture.hpp"

using memgraph::tests::versioning_fixture::ExplainText;
using memgraph::tests::versioning_fixture::IntRows;
using memgraph::tests::versioning_fixture::PlanHasOperator;

namespace {

namespace mq = memgraph::query;
namespace ms = memgraph::storage;
namespace mv = memgraph::versioning;

using PropertyMap = std::map<ms::PropertyId, ms::PropertyValue>;

class VersioningReadTest : public VersioningGatedTest {
 protected:
  mv::BranchContext &Ctx() { return *faker.interpreter.current_db_.branch_context(); }

  ms::Storage *Main() { return db->storage(); }

  // The single integer `query` returns on the session's current target.
  int64_t Int(const std::string &query) {
    auto rows = IntRows(faker, query);
    MG_ASSERT(rows.size() == 1 && rows[0].size() == 1, "expected one value from {}", query);
    return rows[0][0];
  }

  // One branch transaction on the session's context, committed when `commit`, else rolled back.
  void InBranchTxn(const std::function<void(mv::BranchContext &)> &body, bool commit = true) {
    auto &ctx = Ctx();
    auto diff = ctx.diff_engine().Access(ms::WRITE);
    ASSERT_TRUE(ctx.BeginTransaction(diff.get()).has_value());
    body(ctx);
    if (commit) ASSERT_TRUE(ctx.CommitTransaction(memgraph::tests::MakeMainCommitArgs()).has_value());
    ctx.EndTransaction();
  }

  // The DbAccessor a branch query gets, on the open branch transaction.
  static mq::DbAccessor BranchDba(mv::BranchContext &ctx) { return mq::DbAccessor{ctx.current_diff_txn(), &ctx}; }

  // A branch-native edge from fork vertex `from` (through its stub) to `to`, recorded as a query-layer write does.
  static ms::EdgeAccessor CreateEdgeFromFork(mv::BranchContext &ctx, int64_t from, ms::VertexAccessor to,
                                             ms::EdgeTypeId type) {
    auto const from_gid = ms::Gid::FromInt(from);
    auto stub = ctx.CowVertex(from_gid);
    MG_ASSERT(stub.has_value());
    auto edge = ctx.CreateNativeEdge(&*stub, &to, type);
    MG_ASSERT(edge.has_value());
    ctx.RecordEdgeChange(from_gid);
    ctx.RecordEdgeChange(to.Gid());
    return *edge;
  }

  // The fork vertex `gid` as the branch's query layer sees it.
  mq::VertexAccessor BranchVertex(int64_t gid) {
    auto fork = Ctx().FindForkVertex(ms::Gid::FromInt(gid));
    MG_ASSERT(fork.has_value(), "no fork vertex {}", gid);
    return mq::VertexAccessor{*fork, &Ctx()};
  }

  // A fork vertex property write with the change records a query-layer write makes.
  static void WriteProperty(mv::BranchContext &ctx, int64_t gid, ms::PropertyId pid, const ms::PropertyValue &value) {
    auto const g = ms::Gid::FromInt(gid);
    ASSERT_TRUE(ctx.PrepareForkWrite(g).has_value());
    ctx.RecordVertexChange(g, mv::BranchChangeKind::kProperty);
    ctx.core()->RecordPropertyFieldChange(g, pid);
    ASSERT_TRUE(ctx.OverlaySetProperty(g, pid, value).has_value());
  }

  static void ToggleLabel(mv::BranchContext &ctx, int64_t gid, ms::LabelId label, bool add) {
    auto const g = ms::Gid::FromInt(gid);
    ASSERT_TRUE(ctx.PrepareForkWrite(g).has_value());
    ctx.core()->RecordLabelTouched(label, g);
    ctx.RecordVertexChange(g, mv::BranchChangeKind::kLabel);
    ASSERT_TRUE((add ? ctx.OverlayAddLabel(g, label) : ctx.OverlayRemoveLabel(g, label)).has_value());
  }

  // Main's committed value of `pid` on `gid`, read with a fresh main accessor.
  ms::PropertyValue MainProperty(int64_t gid, ms::PropertyId pid) {
    auto acc = Main()->Access(ms::READ);
    auto vertex = acc->FindVertex(ms::Gid::FromInt(gid), ms::View::OLD);
    MG_ASSERT(vertex.has_value());
    return *vertex->GetProperty(pid, ms::View::OLD);
  }
};

std::vector<int64_t> EdgeGids(const mq::ExpandedEdges &edges) {
  std::vector<int64_t> gids;
  for (auto const &edge : edges) gids.push_back(edge.Gid().AsInt());
  return gids;
}

}  // namespace

// An untouched branch reads exactly main@fork: main's later commits (property, label, edge) never show up.
TEST_F(VersioningReadTest, UntouchedBranchReadsTheForkStateAndNotMainsLaterCommits) {
  faker.Interpret("CREATE (:P {id: 1, x: 1})-[:E {w: 1}]->(:Q {id: 2})");
  auto const v = Int("MATCH (n:P) RETURN id(n)");
  auto const u = Int("MATCH (n:Q) RETURN id(n)");
  auto const e = Int("MATCH ()-[e:E]->() RETURN id(e)");
  Fork();

  auto const x = Main()->NameToProperty("x");
  auto const w = Main()->NameToProperty("w");
  auto const p = Main()->NameToLabel("P");
  auto const r = Main()->NameToLabel("R");
  {
    auto acc = Main()->Access(ms::WRITE);
    auto main_v = acc->FindVertex(ms::Gid::FromInt(v), ms::View::OLD);
    ASSERT_TRUE(main_v.has_value());
    ASSERT_TRUE(main_v->SetProperty(x, ms::PropertyValue{2}).has_value());
    ASSERT_TRUE(main_v->AddLabel(r).has_value());
    auto later = acc->CreateVertex();
    ASSERT_TRUE(acc->CreateEdge(&*main_v, &later, Main()->NameToEdgeType("E")).has_value());
    ASSERT_TRUE(acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()).has_value());
  }

  InBranchTxn([&](mv::BranchContext &ctx) {
    auto vertex = BranchVertex(v);
    EXPECT_EQ(*vertex.Labels(ms::View::NEW), (ms::VertexKey{p}));
    EXPECT_FALSE(*vertex.HasLabel(ms::View::NEW, r));
    EXPECT_EQ(*vertex.GetProperty(ms::View::NEW, x), ms::PropertyValue{1});
    EXPECT_EQ(vertex.Properties(ms::View::NEW)->size(), 2U);
    EXPECT_EQ(*vertex.OutDegree(ms::View::NEW), 1U);

    auto out = vertex.OutEdges(ms::View::NEW);
    ASSERT_TRUE(out.has_value());
    ASSERT_EQ(EdgeGids(out->edges), (std::vector<int64_t>{e}));
    auto const edge = out->edges[0];
    EXPECT_EQ(edge.branch_ctx_, &ctx) << "an expanded edge stays on the branch";
    EXPECT_EQ(*edge.GetProperty(ms::View::NEW, w), ms::PropertyValue{1});
    EXPECT_EQ(edge.To().Gid().AsInt(), u);
    EXPECT_EQ(edge.To().branch_ctx_, &ctx) << "so does its endpoint";
  });
  EXPECT_EQ(MainProperty(v, x), ms::PropertyValue{2});
}

// A fork vertex the branch wrote reads as its overlay merged over the fork state, own writes included; a rollback
// drops them, and neither main nor an accessor without the branch context sees them.
TEST_F(VersioningReadTest, TouchedForkVertexReadsMergeTheOverlay) {
  faker.Interpret("CREATE (:P {x: 1, y: 2})");
  auto const v = Int("MATCH (n:P) RETURN id(n)");
  Fork();

  auto const x = Main()->NameToProperty("x");
  auto const y = Main()->NameToProperty("y");
  auto const p = Main()->NameToLabel("P");
  auto const q = Main()->NameToLabel("Q");
  auto const long_text = ms::PropertyValue{std::string(200, 'a')};
  InBranchTxn([&](mv::BranchContext &ctx) {
    WriteProperty(ctx, v, x, long_text);
    ToggleLabel(ctx, v, q, true);
    ToggleLabel(ctx, v, p, false);
  });

  InBranchTxn(
      [&](mv::BranchContext &ctx) {
        auto vertex = BranchVertex(v);
        EXPECT_EQ(*vertex.GetProperty(ms::View::NEW, x), long_text);
        EXPECT_EQ(*vertex.GetProperty(ms::View::NEW, y), ms::PropertyValue{2});
        EXPECT_EQ(*vertex.Properties(ms::View::NEW), (PropertyMap{{x, long_text}, {y, ms::PropertyValue{2}}}));
        EXPECT_GE(*vertex.GetPropertySize(x, ms::View::NEW), 200U);
        EXPECT_EQ(*vertex.Labels(ms::View::NEW), (ms::VertexKey{q}));
        EXPECT_TRUE(*vertex.HasLabel(ms::View::NEW, q));
        EXPECT_FALSE(*vertex.HasLabel(ms::View::NEW, p));
        EXPECT_TRUE(*vertex.HasAnyLabel(ms::View::NEW));

        mq::VertexAccessor const plain{vertex.impl_};
        EXPECT_EQ(*plain.GetProperty(ms::View::NEW, x), ms::PropertyValue{1}) << "impl_ stays the fork vertex";
        EXPECT_TRUE(*plain.HasLabel(ms::View::NEW, p));

        ToggleLabel(ctx, v, q, false);
        EXPECT_FALSE(*vertex.HasAnyLabel(ms::View::NEW)) << "own uncommitted write";
        EXPECT_TRUE(vertex.Labels(ms::View::NEW)->empty());
      },
      /*commit=*/false);

  InBranchTxn([&](mv::BranchContext &) {
    EXPECT_EQ(*BranchVertex(v).Labels(ms::View::NEW), (ms::VertexKey{q})) << "the rolled-back removal is gone";
  });
  EXPECT_EQ(MainProperty(v, x), ms::PropertyValue{1});
}

// A fork edge the branch wrote reads merged wherever the branch reaches it (out of one endpoint, into the other).
TEST_F(VersioningReadTest, ForkEdgeReadsMergeTheOverlay) {
  faker.Interpret("CREATE (:P {id: 1})-[:E {w: 1, z: 3}]->(:P {id: 2})");
  auto const v = Int("MATCH (n:P {id: 1}) RETURN id(n)");
  auto const u = Int("MATCH (n:P {id: 2}) RETURN id(n)");
  auto const e = Int("MATCH ()-[e:E]->() RETURN id(e)");
  Fork();

  auto const w = Main()->NameToProperty("w");
  auto const z = Main()->NameToProperty("z");
  auto const long_text = ms::PropertyValue{std::string(200, 'w')};
  InBranchTxn([&](mv::BranchContext &ctx) {
    auto fork_edge = ctx.FindForkEdge(ms::Gid::FromInt(e), ms::Gid::FromInt(v));
    ASSERT_TRUE(fork_edge.has_value());
    ASSERT_TRUE(ctx.PrepareForkEdgeWrite(*fork_edge).has_value());
    ctx.core()->RecordForkEdgeProperty(fork_edge->Gid(), w);
    ASSERT_TRUE(ctx.EdgeOverlaySetProperty(*fork_edge, w, long_text).has_value());
  });

  InBranchTxn([&](mv::BranchContext &) {
    auto out = BranchVertex(v).OutEdges(ms::View::NEW);
    auto in = BranchVertex(u).InEdges(ms::View::NEW);
    ASSERT_TRUE(out.has_value() && in.has_value());
    ASSERT_EQ(EdgeGids(out->edges), (std::vector<int64_t>{e}));
    ASSERT_EQ(EdgeGids(in->edges), (std::vector<int64_t>{e}));
    for (auto const &edge : {out->edges[0], in->edges[0]}) {
      EXPECT_EQ(*edge.GetProperty(ms::View::NEW, w), long_text);
      EXPECT_EQ(*edge.GetProperty(ms::View::NEW, z), ms::PropertyValue{3});
      EXPECT_EQ(*edge.Properties(ms::View::NEW), (PropertyMap{{w, long_text}, {z, ms::PropertyValue{3}}}));
      EXPECT_GE(*edge.GetPropertySize(w, ms::View::NEW), 200U);
      EXPECT_EQ(*mq::EdgeAccessor{edge.impl_}.GetProperty(ms::View::NEW, w), ms::PropertyValue{1})
          << "impl_ stays the fork edge";
    }
    EXPECT_EQ(out->edges[0].To(), BranchVertex(u));
    EXPECT_EQ(in->edges[0].From(), BranchVertex(v));
  });
}

// A branch-native edge on a fork vertex hangs off its diff-engine stub: it counts in the degree, passes the type and
// destination filters, and its endpoint is the fork vertex again (one identity with the stub).
TEST_F(VersioningReadTest, BranchNativeEdgesJoinTheForkAdjacencyUnderOneVertexIdentity) {
  faker.Interpret("CREATE (:P {id: 1})-[:E]->(:P {id: 2})");
  auto const v = Int("MATCH (n:P {id: 1}) RETURN id(n)");
  Fork();

  auto const native_type = Main()->NameToEdgeType("N");
  InBranchTxn([&](mv::BranchContext &ctx) {
    auto const v_gid = ms::Gid::FromInt(v);
    auto stub = ctx.CowVertex(v_gid);
    ASSERT_TRUE(stub.has_value());
    auto native = ctx.CreateNativeVertex();
    auto native_edge = ctx.CreateNativeEdge(&*stub, &native, native_type);
    ASSERT_TRUE(native_edge.has_value());
    ctx.RecordEdgeChange(v_gid);

    auto vertex = BranchVertex(v);
    EXPECT_EQ(*vertex.OutDegree(ms::View::NEW), 2U);
    EXPECT_EQ(*mq::VertexAccessor{vertex.impl_}.OutDegree(ms::View::NEW), 1U) << "the fork vertex alone";

    auto typed = vertex.OutEdges(ms::View::NEW, {native_type});
    ASSERT_TRUE(typed.has_value());
    ASSERT_EQ(EdgeGids(typed->edges), (std::vector<int64_t>{native_edge->Gid().AsInt()}));
    mq::VertexAccessor const native_vertex{native, &ctx};
    auto to_native = vertex.OutEdges(ms::View::NEW, {}, native_vertex);
    ASSERT_TRUE(to_native.has_value());
    EXPECT_EQ(EdgeGids(to_native->edges), EdgeGids(typed->edges));
    EXPECT_EQ(*native_vertex.InDegree(ms::View::NEW), 1U);

    auto const from = typed->edges[0].From();
    EXPECT_NE(from.impl_.storage_, &ctx.diff_engine()) << "the stub endpoint surfaces as the fork vertex";
    EXPECT_EQ(from, vertex);
    mq::VertexAccessor const on_stub{*stub, &ctx};
    EXPECT_EQ(on_stub, vertex);
    EXPECT_EQ(std::hash<mq::VertexAccessor>{}(on_stub), std::hash<mq::VertexAccessor>{}(vertex));
    EXPECT_NE(mq::VertexAccessor{*stub}, mq::VertexAccessor{vertex.impl_}) << "main compares by object";
  });
}

// The hops budget and the expansion counter hold on both branch expansion paths: the fork vertex's own adjacency and
// the resolved union once the branch changed its edges.
TEST_F(VersioningReadTest, HopsLimitAndCounterHoldOnBothBranchExpansionPaths) {
  faker.Interpret("CREATE (h:Hub) WITH h UNWIND range(1, 10) AS i CREATE (h)-[:NEXT]->()");
  auto const hub = Int("MATCH (h:Hub) RETURN id(h)");
  Fork();

  auto const expect_budget = [&](const char *path) {
    auto const vertex = BranchVertex(hub);
    ms::HopsLimit limit{4};
    auto limited = vertex.OutEdges(ms::View::NEW, {}, &limit);
    ASSERT_TRUE(limited.has_value());
    EXPECT_EQ(limited->edges.size(), 4U) << path;
    EXPECT_EQ(limited->expanded_count, 4) << path;
    EXPECT_TRUE(limit.IsLimitReached()) << path;
  };

  InBranchTxn([&](mv::BranchContext &) {
    auto all = BranchVertex(hub).OutEdges(ms::View::NEW);
    ASSERT_TRUE(all.has_value());
    EXPECT_EQ(all->expanded_count, 10);
    expect_budget("fork adjacency");
  });

  InBranchTxn([&](mv::BranchContext &ctx) {
    auto const hub_gid = ms::Gid::FromInt(hub);
    auto stub = ctx.CowVertex(hub_gid);
    ASSERT_TRUE(stub.has_value());
    auto native = ctx.CreateNativeVertex();
    ASSERT_TRUE(ctx.CreateNativeEdge(&*stub, &native, Main()->NameToEdgeType("NEXT")).has_value());
    ctx.RecordEdgeChange(hub_gid);

    auto all = BranchVertex(hub).OutEdges(ms::View::NEW);
    ASSERT_TRUE(all.has_value());
    EXPECT_EQ(all->expanded_count, 11);
    expect_budget("resolved union");
  });
}

// A fork vertex deleted in the open transaction fails every read, as do its fork edges from either end; its neighbour
// no longer expands to it. The rollback brings everything back.
TEST_F(VersioningReadTest, ReadsOfAForkVertexDeletedInTheTransactionFail) {
  faker.Interpret("CREATE (:A {p: 1})-[:R {p: 1}]->(:B {p: 1})");
  auto const a = Int("MATCH (n:A) RETURN id(n)");
  auto const b = Int("MATCH (n:B) RETURN id(n)");
  auto const e = Int("MATCH ()-[e:R]->() RETURN id(e)");
  Fork();

  auto const p = Main()->NameToProperty("p");
  auto const deleted_object = std::unexpected{ms::Error::DELETED_OBJECT};
  for (auto const [deleted, other] : std::array{std::pair{a, b}, std::pair{b, a}}) {
    InBranchTxn(
        [&](mv::BranchContext &ctx) {
          ASSERT_TRUE(*ctx.OverlayMarkDeleted(ms::Gid::FromInt(deleted)));

          auto const gone = BranchVertex(deleted);
          auto const label = *gone.impl_.Labels(ms::View::OLD);
          EXPECT_EQ(gone.GetProperty(ms::View::NEW, p), deleted_object);
          EXPECT_EQ(gone.GetPropertySize(p, ms::View::NEW), deleted_object);
          EXPECT_EQ(gone.Properties(ms::View::NEW), deleted_object);
          EXPECT_EQ(gone.Labels(ms::View::NEW), deleted_object);
          EXPECT_EQ(gone.HasLabel(ms::View::NEW, label[0]), deleted_object);
          EXPECT_EQ(gone.HasAnyLabel(ms::View::NEW), deleted_object);
          EXPECT_EQ(gone.InDegree(ms::View::NEW), deleted_object);
          EXPECT_EQ(gone.OutEdges(ms::View::NEW).error(), ms::Error::DELETED_OBJECT);

          auto const survivor = BranchVertex(other);
          auto const out = survivor.OutEdges(ms::View::NEW);
          auto const in = survivor.InEdges(ms::View::NEW);
          ASSERT_TRUE(out.has_value() && in.has_value());
          EXPECT_EQ(out->edges.size() + in->edges.size(), 0U) << "the edge to the deleted vertex is gone";

          auto fork_edge = ctx.FindForkEdge(ms::Gid::FromInt(e), ms::Gid::FromInt(a));
          ASSERT_TRUE(fork_edge.has_value());
          mq::EdgeAccessor const edge{*fork_edge, &ctx};
          EXPECT_EQ(edge.GetProperty(ms::View::NEW, p), deleted_object);
          EXPECT_EQ(edge.GetPropertySize(p, ms::View::NEW), deleted_object);
          EXPECT_EQ(edge.Properties(ms::View::NEW), deleted_object);
        },
        /*commit=*/false);

    InBranchTxn([&](mv::BranchContext &ctx) {
      EXPECT_EQ(*BranchVertex(deleted).GetProperty(ms::View::NEW, p), ms::PropertyValue{1});
      EXPECT_EQ(*BranchVertex(a).OutDegree(ms::View::NEW), 1U);
      auto fork_edge = ctx.FindForkEdge(ms::Gid::FromInt(e), ms::Gid::FromInt(a));
      ASSERT_TRUE(fork_edge.has_value());
      EXPECT_EQ(*mq::EdgeAccessor(*fork_edge, &ctx).GetProperty(ms::View::NEW, p), ms::PropertyValue{1});
    });
  }
}

// Fine-grained auth on a branch checks against the main database: the branch accessor reports main's name.
TEST_F(VersioningReadTest, BranchAccessorReportsMainDatabaseNameForFineGrainedAuth) {
  Fork();
  auto &ctx = Ctx();

  auto main_name = db->storage()->name();
  auto diff_acc = ctx.diff_engine().Access(ms::WRITE);
  mq::DbAccessor branch_dba{diff_acc.get(), &ctx};
  ASSERT_EQ(branch_dba.DatabaseName(), main_name);
  ASSERT_EQ(branch_dba.id(), main_name);

  const auto a = branch_dba.NameToLabel("A");
  const auto b = branch_dba.NameToLabel("B");
  const auto read = mq::AuthQuery::FineGrainedPrivilege::READ;

  // Grant on :A, access scoped to the default database only.
  memgraph::auth::User granted{"u"};
  granted.db_access().Grant(main_name);
  granted.fine_grained_access_handler().label_permissions().Grant({"A"}, memgraph::auth::kAllLabelPermissions);
  memgraph::glue::FineGrainedAuthChecker granted_checker{granted, &branch_dba};
  EXPECT_TRUE(granted_checker.Has(std::array{a}, read));
  EXPECT_FALSE(granted_checker.Has(std::array{b}, read));

  // A DENY on :B still applies next to a global grant.
  memgraph::auth::User denied{"d"};
  denied.db_access().Grant(main_name);
  denied.fine_grained_access_handler().label_permissions().GrantGlobal(memgraph::auth::kAllLabelPermissions);
  denied.fine_grained_access_handler().label_permissions().Deny({"B"}, memgraph::auth::kAllLabelPermissions);
  memgraph::glue::FineGrainedAuthChecker denied_checker{denied, &branch_dba};
  EXPECT_TRUE(denied_checker.Has(std::array{a}, read));
  EXPECT_FALSE(denied_checker.Has(std::array{b}, read));
}

// A full scan on a branch is main@fork (never main's later commits) joined with the branch's native vertices and
// without the vertices it deleted; the untouched branch takes the fork stream as is.
TEST_F(VersioningReadTest, BranchFullScanUntouchedBranchIsForkStreamOnly) {
  faker.Interpret("CREATE (:K {i: 1}), (:K {i: 2})");
  auto const one = Int("MATCH (n:K {i: 1}) RETURN id(n)");
  Fork();
  EXPECT_EQ(Int("MATCH (n) RETURN count(n)"), 2);
  InBranchTxn([](mv::BranchContext &ctx) { ctx.CreateNativeVertex(); });
  EXPECT_EQ(Int("MATCH (n) RETURN count(n)"), 3) << "a branch-native vertex joins the scan";

  faker.interpreter.current_db_.LeaveBranch();
  faker.Interpret("CREATE (:K {i: 3})");
  Fork("c2");
  faker.interpreter.current_db_.LeaveBranch();
  faker.Interpret("CREATE (:K {i: 4})");
  Attach(faker, "c2");
  EXPECT_EQ(Int("MATCH (n) RETURN count(n)"), 3) << "a post-fork main vertex must not leak";

  InBranchTxn([&](mv::BranchContext &ctx) { ASSERT_TRUE(*ctx.OverlayMarkDeleted(ms::Gid::FromInt(one))); });
  EXPECT_EQ(Int("MATCH (n) RETURN count(n)"), 2) << "a vertex deleted on the branch leaves the scan";
  EXPECT_EQ(Int("MATCH (n) WHERE n.i = 1 RETURN count(n)"), 0);

  faker.interpreter.current_db_.LeaveBranch();
  EXPECT_EQ(Int("MATCH (n) RETURN count(n)"), 4) << "main is untouched";
}

// Lookups by id (ScanAllById / ScanAllByEdgeId, and the FROM-vertex edge lookup) resolve fork and branch-native
// objects, and miss main's post-fork objects and what the branch deleted.
TEST_F(VersioningReadTest, IdLookupsResolveForkAndNativeObjectsOnly) {
  faker.Interpret("CREATE (:S)-[:E]->(:A)");
  auto const s_gid = Int("MATCH (n:S) RETURN id(n)");
  auto const a_gid = Int("MATCH (n:A) RETURN id(n)");
  auto const fork_edge = Int("MATCH ()-[e:E]->() RETURN id(e)");
  Fork();

  int64_t later_vertex = 0;
  int64_t later_edge = 0;
  {
    auto acc = Main()->Access(ms::WRITE);
    auto v = acc->CreateVertex();
    auto u = acc->CreateVertex();
    auto e = acc->CreateEdge(&v, &u, Main()->NameToEdgeType("E"));
    ASSERT_TRUE(e.has_value());
    later_vertex = v.Gid().AsInt();
    later_edge = e->Gid().AsInt();
    ASSERT_TRUE(acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()).has_value());
  }
  int64_t native_vertex = 0;
  int64_t native_edge = 0;
  InBranchTxn([&](mv::BranchContext &ctx) {
    auto native = ctx.CreateNativeVertex();
    native_vertex = native.Gid().AsInt();
    native_edge = CreateEdgeFromFork(ctx, a_gid, native, Main()->NameToEdgeType("F")).Gid().AsInt();
  });

  auto const vertex_by_id = [&](int64_t gid) {
    return "MATCH (n) WHERE id(n) = " + std::to_string(gid) + " RETURN count(n)";
  };
  auto const edge_by_id = [&](int64_t gid) {
    return "MATCH ()-[e]->() WHERE id(e) = " + std::to_string(gid) + " RETURN count(e)";
  };
  ASSERT_TRUE(PlanHasOperator(ExplainText(faker, vertex_by_id(a_gid)), "ScanAllById"));
  ASSERT_TRUE(PlanHasOperator(ExplainText(faker, edge_by_id(fork_edge)), "ScanAllByEdgeId"));
  EXPECT_EQ(Int(vertex_by_id(a_gid)), 1);
  EXPECT_EQ(Int(vertex_by_id(native_vertex)), 1);
  EXPECT_EQ(Int(vertex_by_id(later_vertex)), 0);
  EXPECT_EQ(Int(edge_by_id(fork_edge)), 1);
  EXPECT_EQ(Int(edge_by_id(native_edge)), 1);
  EXPECT_EQ(Int(edge_by_id(later_edge)), 0);
  EXPECT_EQ(Int(edge_by_id(999999)), 0);

  InBranchTxn(
      [&](mv::BranchContext &ctx) {
        auto dba = BranchDba(ctx);
        auto const by_from = dba.FindEdge(ms::Gid::FromInt(fork_edge), ms::Gid::FromInt(s_gid), ms::View::NEW);
        ASSERT_TRUE(by_from.has_value());
        EXPECT_EQ(by_from->branch_ctx_, &ctx);
        EXPECT_TRUE(dba.FindEdge(ms::Gid::FromInt(native_edge), ms::Gid::FromInt(a_gid), ms::View::NEW).has_value())
            << "a native edge hangs off the fork vertex's stub";
        EXPECT_FALSE(dba.FindEdge(ms::Gid::FromInt(fork_edge), ms::Gid::FromInt(a_gid), ms::View::NEW).has_value())
            << "only the FROM vertex's OUT adjacency is searched";

        ASSERT_TRUE(*ctx.OverlayMarkDeleted(ms::Gid::FromInt(s_gid)));
        EXPECT_FALSE(dba.FindVertex(ms::Gid::FromInt(s_gid), ms::View::NEW).has_value());
        EXPECT_TRUE(dba.FindVertex(ms::Gid::FromInt(s_gid), ms::View::OLD).has_value());
        EXPECT_FALSE(dba.FindEdge(ms::Gid::FromInt(fork_edge), ms::View::NEW).has_value())
            << "a deleted endpoint hides the fork edge";
        auto const found = dba.FindVertex(ms::Gid::FromInt(a_gid), ms::View::NEW);
        ASSERT_TRUE(found.has_value());
        EXPECT_EQ(found->branch_ctx_, &ctx);
      },
      /*commit=*/false);
}

TEST_F(VersioningReadTest, BranchHopsLimitEnforcedOnExpand) {
  faker.Interpret("UNWIND range(1, 100) AS x CREATE ()-[:NEXT]->()");
  Fork();
  EXPECT_EQ(Int("USING HOPS LIMIT 5 MATCH p=()-[:NEXT]->() RETURN count(p) AS c"), 5);
}

TEST_F(VersioningReadTest, BranchGetHopsCounterTracksTraversal) {
  faker.Interpret("UNWIND range(1, 100) AS x CREATE ()-[:NEXT]->()");
  Fork();
  // The CALL subquery traverses 50 of the 100 edges, within the 100-hop budget.
  EXPECT_EQ(Int("USING HOPS LIMIT 100 CALL { MATCH (a)-[r]->(b) WITH a, r, b LIMIT 50 RETURN count(*) AS cnt } "
                "RETURN getHopsCounter() AS hc"),
            50);
}

// Bolt encodes what the branch reads: the overlay's labels and properties, and a native edge's stub endpoint as the
// fork vertex's id.
TEST_F(VersioningReadTest, BoltEncodingOfBranchAccessorsReadsTheBranchView) {
  faker.Interpret("CREATE (:P {id: 1, x: 1})-[:E {w: 1}]->(:P {id: 2})");
  auto const v = Int("MATCH (n:P {id: 1}) RETURN id(n)");
  auto const e = Int("MATCH ()-[e:E]->() RETURN id(e)");
  Fork();

  auto const x = Main()->NameToProperty("x");
  auto const w = Main()->NameToProperty("w");
  int64_t native_edge = 0;
  InBranchTxn([&](mv::BranchContext &ctx) {
    WriteProperty(ctx, v, x, ms::PropertyValue{2});
    ToggleLabel(ctx, v, Main()->NameToLabel("Q"), true);
    auto fork_edge = ctx.FindForkEdge(ms::Gid::FromInt(e), ms::Gid::FromInt(v));
    ASSERT_TRUE(fork_edge.has_value());
    ASSERT_TRUE(ctx.PrepareForkEdgeWrite(*fork_edge).has_value());
    ctx.core()->RecordForkEdgeProperty(fork_edge->Gid(), w);
    ASSERT_TRUE(ctx.EdgeOverlaySetProperty(*fork_edge, w, ms::PropertyValue{5}).has_value());
    native_edge = CreateEdgeFromFork(ctx, v, ctx.CreateNativeVertex(), Main()->NameToEdgeType("N")).Gid().AsInt();
  });

  InBranchTxn([&](mv::BranchContext &) {
    auto const vertex = memgraph::glue::ToBoltVertex(BranchVertex(v), *Main(), ms::View::NEW, nullptr);
    ASSERT_TRUE(vertex.has_value());
    auto labels = vertex->labels;
    std::ranges::sort(labels);
    EXPECT_EQ(labels, (std::vector<std::string>{"P", "Q"}));
    EXPECT_EQ(vertex->properties.at("x").ValueInt(), 2);

    auto out = BranchVertex(v).OutEdges(ms::View::NEW);
    ASSERT_TRUE(out.has_value());
    ASSERT_EQ(out->edges.size(), 2U);
    for (auto const &edge : out->edges) {
      auto const bolt = memgraph::glue::ToBoltEdge(edge, *Main(), ms::View::NEW, nullptr);
      ASSERT_TRUE(bolt.has_value());
      EXPECT_EQ(bolt->from.AsInt(), v);
      if (edge.Gid().AsInt() == e) {
        EXPECT_EQ(bolt->properties.at("w").ValueInt(), 5);
      } else {
        EXPECT_EQ(edge.Gid().AsInt(), native_edge);
        EXPECT_EQ(bolt->type, "N");
      }
    }
  });
}

// A subgraph's adjacency on a branch is the branch's adjacency narrowed to the subgraph's edges: an edge deleted on
// the branch is gone, a native one shows from both ends, and endpoints read the branch state.
TEST_F(VersioningReadTest, SubgraphAdjacencyOnBranchIsBranchView) {
  faker.Interpret("CREATE (a:A {name: 'a'})-[:R1]->(b:B {name: 'b'}), (a)-[:R2]->(c:C {name: 'c'})");
  faker.Interpret("CREATE (:D {name: 'd'})-[:R4]->(:E {name: 'e'})");
  auto const a_gid = Int("MATCH (n:A) RETURN id(n)");
  auto const c_gid = Int("MATCH (n:C) RETURN id(n)");
  auto const b_gid = Int("MATCH (n:B) RETURN id(n)");
  auto const r2 = Int("MATCH ()-[r:R2]->() RETURN id(r)");
  Fork();

  auto const name = Main()->NameToProperty("name");
  InBranchTxn([&](mv::BranchContext &ctx) {
    auto c_stub = ctx.CowVertex(ms::Gid::FromInt(c_gid));
    ASSERT_TRUE(c_stub.has_value());
    CreateEdgeFromFork(ctx, b_gid, *c_stub, Main()->NameToEdgeType("R3"));
    auto r2_edge = ctx.FindForkEdge(ms::Gid::FromInt(r2), ms::Gid::FromInt(a_gid));
    ASSERT_TRUE(r2_edge.has_value());
    ASSERT_TRUE(*ctx.DeleteForkEdge(*r2_edge));
    ToggleLabel(ctx, a_gid, Main()->NameToLabel("A"), false);
    ToggleLabel(ctx, a_gid, Main()->NameToLabel("X"), true);
    WriteProperty(ctx, a_gid, name, ms::PropertyValue{"a2"});
  });

  InBranchTxn([&](mv::BranchContext &ctx) {
    auto dba = BranchDba(ctx);
    auto const keys = [&](const mq::ExpandedEdges &edges) {
      std::vector<std::string> types;
      for (auto const &edge : edges) types.push_back(dba.EdgeTypeToName(edge.EdgeType()));
      std::ranges::sort(types);
      return types;
    };
    using Types = std::vector<std::string>;

    std::map<std::string, mq::VertexAccessor> by_name;
    for (auto vertex : dba.Vertices(ms::View::NEW)) {
      by_name.emplace(vertex.GetProperty(ms::View::NEW, name)->ValueString(), vertex);
    }
    ASSERT_EQ(by_name.size(), 5U);
    auto const &a = by_name.at("a2");
    auto const &b = by_name.at("b");
    auto const &c = by_name.at("c");

    mq::Graph full{memgraph::utils::NewDeleteResource()};
    for (auto const &vertex : {a, b, c}) {
      full.InsertVertex(vertex);
      for (auto const &edge : vertex.OutEdges(ms::View::NEW)->edges) full.InsertEdge(edge);
    }
    ASSERT_EQ(full.edges().size(), 2U);
    mq::SubgraphVertexAccessor const sa{a, &full};
    mq::SubgraphVertexAccessor const sb{b, &full};
    mq::SubgraphVertexAccessor const sc{c, &full};
    EXPECT_EQ(keys(sa.OutEdges(ms::View::NEW)->edges), (Types{"R1"})) << "R2 was deleted on the branch";
    EXPECT_EQ(keys(sa.InEdges(ms::View::NEW)->edges), Types{});
    EXPECT_EQ(keys(sb.InEdges(ms::View::NEW)->edges), (Types{"R1"}));
    EXPECT_EQ(keys(sb.OutEdges(ms::View::NEW)->edges), (Types{"R3"}));
    EXPECT_EQ(keys(sc.InEdges(ms::View::NEW)->edges), (Types{"R3"}));
    EXPECT_EQ(keys(sc.OutEdges(ms::View::NEW)->edges), Types{});

    for (auto const &edge : sa.OutEdges(ms::View::NEW)->edges) {
      EXPECT_EQ(edge.branch_ctx_, &ctx);
      EXPECT_EQ(edge.From().GetProperty(ms::View::NEW, name)->ValueString(), "a2") << "endpoints read the branch";
      EXPECT_TRUE(*edge.From().HasLabel(ms::View::NEW, Main()->NameToLabel("X")));
    }

    // A subgraph holding only R3 hides R1 from both of its ends.
    mq::Graph partial{memgraph::utils::NewDeleteResource()};
    for (auto const &vertex : {a, b, c}) partial.InsertVertex(vertex);
    for (auto const &edge : sb.OutEdges(ms::View::NEW)->edges) partial.InsertEdge(edge);
    mq::SubgraphVertexAccessor const pa{a, &partial};
    mq::SubgraphVertexAccessor const pb{b, &partial};
    EXPECT_EQ(keys(pa.OutEdges(ms::View::NEW)->edges), Types{});
    EXPECT_EQ(keys(pb.InEdges(ms::View::NEW)->edges), Types{});
    EXPECT_EQ(keys(pb.OutEdges(ms::View::NEW)->edges), (Types{"R3"}));

    // d -R4-> e is untouched on the branch; membership still filters it.
    auto const &d = by_name.at("d");
    auto const &e = by_name.at("e");
    mq::Graph with_r4{memgraph::utils::NewDeleteResource()};
    mq::Graph without_r4{memgraph::utils::NewDeleteResource()};
    for (auto const &vertex : {d, e}) {
      with_r4.InsertVertex(vertex);
      without_r4.InsertVertex(vertex);
    }
    for (auto const &edge : d.OutEdges(ms::View::NEW)->edges) with_r4.InsertEdge(edge);
    EXPECT_EQ(keys(mq::SubgraphVertexAccessor{d, &with_r4}.OutEdges(ms::View::NEW)->edges), (Types{"R4"}));
    EXPECT_EQ(keys(mq::SubgraphVertexAccessor{e, &with_r4}.InEdges(ms::View::NEW)->edges), (Types{"R4"}));
    EXPECT_EQ(keys(mq::SubgraphVertexAccessor{d, &without_r4}.OutEdges(ms::View::NEW)->edges), Types{});
    EXPECT_EQ(keys(mq::SubgraphVertexAccessor{e, &without_r4}.InEdges(ms::View::NEW)->edges), Types{});
  });
}
