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
// session's BranchContext; branch writes go through the BranchContext write resolver.

#include <algorithm>
#include <array>
#include <atomic>
#include <cstdint>
#include <functional>
#include <map>
#include <memory>
#include <optional>
#include <string>
#include <thread>
#include <vector>

#include <gtest/gtest.h>

#include "auth/models.hpp"
#include "glue/auth_checker.hpp"
#include "glue/communication.hpp"
#include "query/db_accessor.hpp"
#include "query/edge_accessor.hpp"
#include "query/frontend/ast/query/auth_query.hpp"
#include "query/graph.hpp"
#include "query/path.hpp"
#include "query/typed_value.hpp"
#include "query/vertex_accessor.hpp"
#include "storage/v2/hops_limit.hpp"
#include "storage/v2/property_value.hpp"
#include "tests/test_commit_args_helper.hpp"
#include "utils/memory.hpp"
#include "versioning/branch_engine.hpp"
#include "versioning_fixture.hpp"

using memgraph::tests::versioning_fixture::ExplainText;
using memgraph::tests::versioning_fixture::IntRows;
using memgraph::tests::versioning_fixture::PlanHasOperator;
using memgraph::tests::versioning_fixture::Rows;
using memgraph::tests::versioning_fixture::ThrownMessage;

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

  // A branch-native edge from fork vertex `from` (through its stub) to `to`, as a query-layer write creates it.
  static ms::EdgeAccessor CreateEdgeFromFork(mv::BranchContext &ctx, int64_t from, ms::VertexAccessor to,
                                             ms::EdgeTypeId type) {
    auto stub = ctx.EnsureDiffVertex(ms::Gid::FromInt(from));
    MG_ASSERT(stub.has_value());
    auto edge = ctx.CreateNativeEdge(*stub, to, type);
    MG_ASSERT(edge.has_value());
    return *edge;
  }

  // The fork vertex `gid` as the branch's query layer sees it on `ctx`'s open transaction.
  static mq::VertexAccessor ForkVertexOn(mv::BranchContext &ctx, int64_t gid) {
    auto fork = ctx.FindForkVertex(ms::Gid::FromInt(gid));
    MG_ASSERT(fork.has_value(), "no fork vertex {}", gid);
    return mq::VertexAccessor{*fork, &ctx};
  }

  mq::VertexAccessor BranchVertex(int64_t gid) { return ForkVertexOn(Ctx(), gid); }

  // Sets `pid` on main's vertex `gid` and commits, as a main session would.
  void WriteMainProperty(int64_t gid, ms::PropertyId pid, const ms::PropertyValue &value) {
    auto acc = Main()->Access(ms::WRITE);
    auto vertex = acc->FindVertex(ms::Gid::FromInt(gid), ms::View::OLD);
    ASSERT_TRUE(vertex.has_value());
    ASSERT_TRUE(vertex->SetProperty(pid, value).has_value());
    ASSERT_TRUE(acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()).has_value());
  }

  static void WriteProperty(mv::BranchContext &ctx, int64_t gid, ms::PropertyId pid, const ms::PropertyValue &value) {
    auto fork = ctx.FindForkVertex(ms::Gid::FromInt(gid));
    ASSERT_TRUE(fork.has_value());
    ASSERT_TRUE(ctx.SetProperty(*fork, pid, value).has_value());
  }

  static void ToggleLabel(mv::BranchContext &ctx, int64_t gid, ms::LabelId label, bool add) {
    auto fork = ctx.FindForkVertex(ms::Gid::FromInt(gid));
    ASSERT_TRUE(fork.has_value());
    ASSERT_TRUE((add ? ctx.AddLabel(*fork, label) : ctx.RemoveLabel(*fork, label)).has_value());
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
    EXPECT_EQ(edge.branch_ctx(), &ctx) << "an expanded edge stays on the branch";
    EXPECT_EQ(*edge.GetProperty(ms::View::NEW, w), ms::PropertyValue{1});
    EXPECT_EQ(edge.To().Gid().AsInt(), u);
    EXPECT_EQ(edge.To().branch_ctx(), &ctx) << "so does its endpoint";
  });
  EXPECT_EQ(MainProperty(v, x), ms::PropertyValue{2});
}

// A fork vertex the branch wrote reads as its stub merged over the fork state, own writes included; a rollback
// drops them, and neither main nor an accessor without the branch context sees them.
TEST_F(VersioningReadTest, TouchedForkVertexReadsMergeTheStub) {
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

        EXPECT_EQ(*vertex.impl_.GetProperty(x, ms::View::NEW), ms::PropertyValue{1}) << "impl_ stays the fork vertex";
        EXPECT_TRUE(*vertex.impl_.HasLabel(p, ms::View::NEW));

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
TEST_F(VersioningReadTest, ForkEdgeReadsMergeTheStub) {
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
    ASSERT_TRUE(ctx.SetProperty(*fork_edge, w, long_text).has_value());
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
      EXPECT_EQ(*edge.impl_.GetProperty(w, ms::View::NEW), ms::PropertyValue{1}) << "impl_ stays the fork edge";
    }
    EXPECT_EQ(out->edges[0].To(), BranchVertex(u));
    EXPECT_EQ(in->edges[0].From(), BranchVertex(v));
  });
}

// A branch-native edge on a fork vertex hangs off its diff-engine stub: it counts in the degree, passes the type and
// destination filters, and its endpoint is the fork vertex again, never the stub.
TEST_F(VersioningReadTest, BranchNativeEdgesJoinTheForkAdjacencyAndSurfaceTheirEndpointAsTheForkVertex) {
  faker.Interpret("CREATE (:P {id: 1})-[:E]->(:P {id: 2})");
  auto const v = Int("MATCH (n:P {id: 1}) RETURN id(n)");
  Fork();

  auto const native_type = Main()->NameToEdgeType("N");
  InBranchTxn([&](mv::BranchContext &ctx) {
    auto const v_gid = ms::Gid::FromInt(v);
    auto stub = ctx.EnsureDiffVertex(v_gid);
    ASSERT_TRUE(stub.has_value());
    auto native = ctx.CreateNativeVertex();
    auto native_edge = ctx.CreateNativeEdge(*stub, native, native_type);
    ASSERT_TRUE(native_edge.has_value());

    auto vertex = BranchVertex(v);
    EXPECT_EQ(*vertex.OutDegree(ms::View::NEW), 2U);
    EXPECT_EQ(*vertex.impl_.OutDegree(ms::View::NEW), 1U) << "the fork vertex alone";

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
    EXPECT_EQ(from.branch_ctx(), &ctx);
    EXPECT_TRUE(ctx.ForkIdentity(*stub) == vertex.impl_);
    EXPECT_TRUE(ctx.ForkIdentity(vertex.impl_) == vertex.impl_) << "a fork vertex is its own identity";
    EXPECT_TRUE(ctx.ForkIdentity(native) == native) << "so is a native vertex";
    EXPECT_EQ(typed->edges[0].To(), native_vertex);
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
    auto stub = ctx.EnsureDiffVertex(hub_gid);
    ASSERT_TRUE(stub.has_value());
    auto native = ctx.CreateNativeVertex();
    ASSERT_TRUE(ctx.CreateNativeEdge(*stub, native, Main()->NameToEdgeType("NEXT")).has_value());

    auto all = BranchVertex(hub).OutEdges(ms::View::NEW);
    ASSERT_TRUE(all.has_value());
    EXPECT_EQ(all->expanded_count, 11);
    expect_budget("resolved union");
  });
}

// An untouched fork vertex expands through the storage call itself: typed, hops-limited and destination-filtered
// exactly as main does it, so each limit and filter gives main's edges, count and budget state.
TEST_F(VersioningReadTest, UntouchedForkVertexExpandsWithMainsTypeDestinationAndHopsRules) {
  faker.Interpret("CREATE (h:Hub) WITH h UNWIND range(1, 6) AS i CREATE (h)-[:A]->(:T)");
  faker.Interpret("MATCH (h:Hub) UNWIND range(1, 4) AS j CREATE (h)-[:B]->(:T)");
  auto const hub = Int("MATCH (h:Hub) RETURN id(h)");
  Fork();

  auto const a = Main()->NameToEdgeType("A");
  auto const b = Main()->NameToEdgeType("B");
  auto main_acc = Main()->Access(ms::READ);
  auto main_hub = main_acc->FindVertex(ms::Gid::FromInt(hub), ms::View::OLD);
  ASSERT_TRUE(main_hub.has_value());
  auto const main_all = *main_hub->OutEdges(ms::View::OLD);
  ASSERT_EQ(main_all.edges.size(), 10U);
  auto const dest_gid = main_all.edges[7].ToVertex().Gid();
  auto main_dest = main_acc->FindVertex(dest_gid, ms::View::OLD);
  ASSERT_TRUE(main_dest.has_value());

  InBranchTxn([&](mv::BranchContext &ctx) {
    auto const vertex = BranchVertex(hub);
    ASSERT_TRUE(ctx.DirectExpansionView(vertex.impl_, ms::EdgeDirection::OUT, ms::View::NEW).has_value());
    auto const branch_dest = BranchVertex(dest_gid.AsInt());
    auto const check = [&](const char *state) {
      ASSERT_TRUE(ctx.DirectExpansionView(vertex.impl_, ms::EdgeDirection::OUT, ms::View::NEW).has_value()) << state;
      auto const merged = ctx.OutEdges(vertex.impl_, ms::View::NEW);
      ASSERT_TRUE(merged.has_value());
      std::vector<int64_t> merged_gids;
      for (auto const &edge : *merged) merged_gids.push_back(edge.Gid().AsInt());
      EXPECT_EQ(EdgeGids(vertex.OutEdges(ms::View::NEW)->edges), merged_gids) << state;
      for (auto const &types : std::vector<std::vector<ms::EdgeTypeId>>{{}, {a}, {b}, {a, b}}) {
        for (uint64_t const budget : std::array<uint64_t, 7>{0, 1, 3, 6, 7, 10, 11}) {
          auto const label = [&] {
            return "budget " + std::to_string(budget) + ", " + std::to_string(types.size()) + " types";
          };
          std::optional<ms::HopsLimit> main_limit;
          std::optional<ms::HopsLimit> branch_limit;
          if (budget != 0) {
            main_limit.emplace(budget);
            branch_limit.emplace(budget);
          }
          auto expected = main_hub->OutEdges(ms::View::OLD, types, nullptr, main_limit ? &*main_limit : nullptr);
          auto actual = vertex.OutEdges(ms::View::NEW, types, branch_limit ? &*branch_limit : nullptr);
          ASSERT_TRUE(expected.has_value() && actual.has_value());
          std::vector<int64_t> expected_gids;
          for (auto const &edge : expected->edges) expected_gids.push_back(edge.Gid().AsInt());
          EXPECT_EQ(EdgeGids(actual->edges), expected_gids) << label();
          EXPECT_EQ(actual->expanded_count, expected->expanded_count) << label();
          if (budget != 0) EXPECT_EQ(branch_limit->IsLimitReached(), main_limit->IsLimitReached()) << label();
        }
        auto expected = main_hub->OutEdges(ms::View::OLD, types, &*main_dest);
        auto actual = vertex.OutEdges(ms::View::NEW, types, branch_dest);
        ASSERT_TRUE(expected.has_value() && actual.has_value());
        std::vector<int64_t> expected_gids;
        for (auto const &edge : expected->edges) expected_gids.push_back(edge.Gid().AsInt());
        EXPECT_EQ(EdgeGids(actual->edges), expected_gids) << types.size() << " types to one destination";
      }
    };
    check("untouched");
    // A stub that only carries a label and a property has no adjacency: the vertex still expands directly.
    auto fork = ctx.FindForkVertex(ms::Gid::FromInt(hub));
    ASSERT_TRUE(fork.has_value());
    ASSERT_TRUE(ctx.SetProperty(*fork, Main()->NameToProperty("p"), ms::PropertyValue{1}).has_value());
    ASSERT_TRUE(ctx.AddLabel(*fork, Main()->NameToLabel("Extra")).has_value());
    ASSERT_TRUE(ctx.HasVertexStub(ms::Gid::FromInt(hub)));
    check("label and property stub");
  });
}

// Once the branch attached a native edge to a fork vertex and hid one of its fork edges, the merged list is charged
// per edge before the type filter, native edges first: the first `budget` merged edges, then the filter.
TEST_F(VersioningReadTest, MergedExpansionChargesHopsBeforeTheTypeFilterOverTheVisibleEdges) {
  faker.Interpret("CREATE (h:Hub) WITH h UNWIND range(1, 5) AS i CREATE (h)-[:A]->(:T)");
  faker.Interpret("MATCH (h:Hub) UNWIND range(1, 5) AS j CREATE (h)-[:B]->(:T)");
  auto const hub = Int("MATCH (h:Hub) RETURN id(h)");
  auto const hidden = Int("MATCH (:Hub)-[e:A]->() RETURN id(e) ORDER BY id(e) LIMIT 1");
  Fork();

  auto const a = Main()->NameToEdgeType("A");
  auto const b = Main()->NameToEdgeType("B");
  auto const n = Main()->NameToEdgeType("N");
  InBranchTxn([&](mv::BranchContext &ctx) {
    auto const hub_gid = ms::Gid::FromInt(hub);
    auto stub = ctx.EnsureDiffVertex(hub_gid);
    ASSERT_TRUE(stub.has_value());
    auto native = ctx.CreateNativeVertex();
    ASSERT_TRUE(ctx.CreateNativeEdge(*stub, native, a).has_value());
    ASSERT_TRUE(ctx.CreateNativeEdge(*stub, native, n).has_value());
    auto fork_edge = ctx.FindForkEdge(ms::Gid::FromInt(hidden), hub_gid);
    ASSERT_TRUE(fork_edge.has_value());
    ASSERT_TRUE(*ctx.DeleteForkEdge(*fork_edge));

    auto const vertex = BranchVertex(hub);
    EXPECT_FALSE(ctx.DirectExpansionView(vertex.impl_, ms::EdgeDirection::OUT, ms::View::NEW).has_value());
    auto const merged = vertex.OutEdges(ms::View::NEW);
    ASSERT_TRUE(merged.has_value());
    ASSERT_EQ(merged->edges.size(), 11U) << "10 fork edges, one hidden, two native";
    std::vector<int64_t> gids;
    std::vector<ms::EdgeTypeId> types;
    for (auto const &edge : merged->edges) {
      gids.push_back(edge.Gid().AsInt());
      types.push_back(edge.EdgeType());
    }
    EXPECT_EQ(std::ranges::count(gids, hidden), 0);

    for (auto const &filter : std::vector<std::vector<ms::EdgeTypeId>>{{a}, {b}, {a, b}, {n}}) {
      for (uint64_t const budget : std::array<uint64_t, 5>{1, 3, 5, 11, 12}) {
        ms::HopsLimit limit{budget};
        auto const got = vertex.OutEdges(ms::View::NEW, filter, &limit);
        ASSERT_TRUE(got.has_value());
        auto const charged = std::min<size_t>(budget, gids.size());
        std::vector<int64_t> expected;
        for (size_t i = 0; i < charged; ++i) {
          if (std::ranges::contains(filter, types[i])) expected.push_back(gids[i]);
        }
        EXPECT_EQ(EdgeGids(got->edges), expected) << "budget " << budget << ", " << filter.size() << " types";
        EXPECT_EQ(got->expanded_count, static_cast<int64_t>(charged)) << "budget " << budget;
      }
    }
  });
}

// A vertex that expanded directly sees a fork edge the same transaction hides afterwards, from both its ends and
// under a type filter; a vertex with a native edge attached never expands directly.
TEST_F(VersioningReadTest, ForkEdgeHiddenAfterADirectExpansionDisappearsFromBothEnds) {
  faker.Interpret("CREATE (a:A)-[:R]->(b:B), (a)-[:R]->(:C)");
  auto const a = Int("MATCH (n:A) RETURN id(n)");
  auto const b = Int("MATCH (n:B) RETURN id(n)");
  auto const e = Int("MATCH (:A)-[e:R]->(:B) RETURN id(e)");
  Fork();

  auto const r = Main()->NameToEdgeType("R");
  InBranchTxn([&](mv::BranchContext &ctx) {
    auto const va = BranchVertex(a);
    auto const vb = BranchVertex(b);
    ASSERT_TRUE(ctx.DirectExpansionView(va.impl_, ms::EdgeDirection::OUT, ms::View::NEW).has_value());
    ASSERT_TRUE(ctx.DirectExpansionView(vb.impl_, ms::EdgeDirection::OUT, ms::View::NEW).has_value());
    EXPECT_EQ(va.OutEdges(ms::View::NEW, {r})->edges.size(), 2U);
    EXPECT_EQ(vb.InEdges(ms::View::NEW, {r})->edges.size(), 1U);

    auto fork_edge = ctx.FindForkEdge(ms::Gid::FromInt(e), ms::Gid::FromInt(a));
    ASSERT_TRUE(fork_edge.has_value());
    ASSERT_TRUE(*ctx.DeleteForkEdge(*fork_edge));

    auto const out = va.OutEdges(ms::View::NEW, {r});
    ASSERT_TRUE(out.has_value());
    EXPECT_EQ(out->edges.size(), 1U);
    for (auto const &edge : out->edges) EXPECT_NE(edge.Gid().AsInt(), e);
    EXPECT_EQ(va.OutEdges(ms::View::NEW)->edges.size(), 1U);
    EXPECT_EQ(vb.InEdges(ms::View::NEW, {r})->edges.size(), 0U);
    EXPECT_EQ(*vb.InDegree(ms::View::NEW), 0U);
    EXPECT_FALSE(ctx.DirectExpansionView(va.impl_, ms::EdgeDirection::OUT, ms::View::NEW).has_value());
  });

  InBranchTxn([&](mv::BranchContext &ctx) {
    auto const va = BranchVertex(a);
    EXPECT_EQ(va.OutEdges(ms::View::NEW, {r})->edges.size(), 1U) << "the delete committed";
    EXPECT_EQ(BranchVertex(b).InEdges(ms::View::NEW, {r})->edges.size(), 0U);
  });
}

// A vertex with a branch-native edge attached is never expanded directly, with or without any fork edge deleted.
TEST_F(VersioningReadTest, ForkVertexWithANativeEdgeIsNotExpandedDirectly) {
  faker.Interpret("CREATE (a:A)-[:R]->(:B)");
  auto const a = Int("MATCH (n:A) RETURN id(n)");
  Fork();

  InBranchTxn([&](mv::BranchContext &ctx) {
    auto stub = ctx.EnsureDiffVertex(ms::Gid::FromInt(a));
    ASSERT_TRUE(stub.has_value());
    auto native = ctx.CreateNativeVertex();
    ASSERT_TRUE(ctx.CreateNativeEdge(*stub, native, Main()->NameToEdgeType("N")).has_value());

    auto const va = BranchVertex(a);
    EXPECT_FALSE(ctx.MayHaveAnyForkEdgeDeleted());
    EXPECT_FALSE(ctx.DirectExpansionView(va.impl_, ms::EdgeDirection::OUT, ms::View::NEW).has_value());
    EXPECT_EQ(va.OutEdges(ms::View::NEW)->edges.size(), 2U);
    mq::VertexAccessor const native_vertex{native, &ctx};
    EXPECT_EQ(ctx.DirectExpansionView(native, ms::EdgeDirection::OUT, ms::View::NEW), ms::View::NEW)
        << "a native vertex's own view";
    EXPECT_EQ(native_vertex.InEdges(ms::View::NEW)->edges.size(), 1U);
  });
}

// A native edge that was rolled back leaves the edge filter set but the stub's adjacency empty: the vertex expands
// directly again and still gives exactly its fork edges.
TEST_F(VersioningReadTest, ForkVertexWhoseNativeEdgeWasRolledBackExpandsDirectly) {
  faker.Interpret("CREATE (a:A)-[:R]->(:B), (a)-[:R]->(:C)");
  auto const a = Int("MATCH (n:A) RETURN id(n)");
  Fork();

  InBranchTxn(
      [&](mv::BranchContext &ctx) {
        auto stub = ctx.EnsureDiffVertex(ms::Gid::FromInt(a));
        ASSERT_TRUE(stub.has_value());
        auto native = ctx.CreateNativeVertex();
        ASSERT_TRUE(ctx.CreateNativeEdge(*stub, native, Main()->NameToEdgeType("N")).has_value());
        EXPECT_FALSE(ctx.DirectExpansionView(BranchVertex(a).impl_, ms::EdgeDirection::OUT, ms::View::NEW).has_value());
      },
      /*commit=*/false);

  InBranchTxn([&](mv::BranchContext &ctx) {
    ASSERT_TRUE(ctx.MayHaveEdgeChange(ms::Gid::FromInt(a)));
    auto const va = BranchVertex(a);
    EXPECT_TRUE(ctx.DirectExpansionView(va.impl_, ms::EdgeDirection::OUT, ms::View::NEW).has_value());
    EXPECT_EQ(va.OutEdges(ms::View::NEW)->edges.size(), 2U);
  });
}

// The stub is looked at in the direction being expanded: a vertex with only a native in-edge expands OUT directly and
// the other way round, each giving what the merged path gives.
TEST_F(VersioningReadTest, NativeEdgeInOneDirectionLeavesTheOtherDirectionDirect) {
  faker.Interpret("CREATE (a:A)-[:R]->(b:B)");
  auto const a = Int("MATCH (n:A) RETURN id(n)");
  auto const b = Int("MATCH (n:B) RETURN id(n)");
  Fork();

  InBranchTxn([&](mv::BranchContext &ctx) {
    auto const native_type = Main()->NameToEdgeType("N");
    auto stub_a = ctx.EnsureDiffVertex(ms::Gid::FromInt(a));
    auto stub_b = ctx.EnsureDiffVertex(ms::Gid::FromInt(b));
    ASSERT_TRUE(stub_a.has_value() && stub_b.has_value());
    auto native = ctx.CreateNativeVertex();
    ASSERT_TRUE(ctx.CreateNativeEdge(*stub_a, native, native_type).has_value());  // a -> native: an out-edge of a
    ASSERT_TRUE(ctx.CreateNativeEdge(native, *stub_b, native_type).has_value());  // native -> b: an in-edge of b

    auto const va = BranchVertex(a);
    auto const vb = BranchVertex(b);
    constexpr auto kOut = ms::EdgeDirection::OUT;
    constexpr auto kIn = ms::EdgeDirection::IN;
    EXPECT_FALSE(ctx.DirectExpansionView(va.impl_, kOut, ms::View::NEW).has_value());
    EXPECT_TRUE(ctx.DirectExpansionView(va.impl_, kIn, ms::View::NEW).has_value());
    EXPECT_TRUE(ctx.DirectExpansionView(vb.impl_, kOut, ms::View::NEW).has_value());
    EXPECT_FALSE(ctx.DirectExpansionView(vb.impl_, kIn, ms::View::NEW).has_value());

    auto const gids_of = [](const auto &edges) {
      std::vector<int64_t> gids;
      for (auto const &edge : edges) gids.push_back(edge.Gid().AsInt());
      return gids;
    };
    EXPECT_EQ(EdgeGids(va.OutEdges(ms::View::NEW)->edges), gids_of(*ctx.OutEdges(va.impl_, ms::View::NEW)));
    EXPECT_EQ(EdgeGids(va.InEdges(ms::View::NEW)->edges), gids_of(*ctx.InEdges(va.impl_, ms::View::NEW)));
    EXPECT_EQ(EdgeGids(vb.OutEdges(ms::View::NEW)->edges), gids_of(*ctx.OutEdges(vb.impl_, ms::View::NEW)));
    EXPECT_EQ(EdgeGids(vb.InEdges(ms::View::NEW)->edges), gids_of(*ctx.InEdges(vb.impl_, ms::View::NEW)));
    EXPECT_EQ(va.OutEdges(ms::View::NEW)->edges.size(), 2U);
    EXPECT_EQ(vb.InEdges(ms::View::NEW)->edges.size(), 2U);
    EXPECT_EQ(vb.OutEdges(ms::View::NEW)->edges.size(), 0U);
  });
}

// A direct expansion's edges are wrapped as they are looked at: each carries the context and the list equals the first
// `budget` merged edges filtered by type, whichever way it is walked.
TEST_F(VersioningReadTest, LazilyWrappedDirectExpansionEqualsTheMergedEdgesForEveryTypeAndBudget) {
  faker.Interpret("CREATE (h:Hub) WITH h UNWIND range(1, 6) AS i CREATE (h)-[:A]->(:T)");
  faker.Interpret("MATCH (h:Hub) UNWIND range(1, 4) AS j CREATE (h)-[:B]->(:T)");
  auto const hub = Int("MATCH (h:Hub) RETURN id(h)");
  Fork();

  auto const a = Main()->NameToEdgeType("A");
  auto const b = Main()->NameToEdgeType("B");
  InBranchTxn([&](mv::BranchContext &ctx) {
    auto const vertex = BranchVertex(hub);
    ASSERT_TRUE(ctx.DirectExpansionView(vertex.impl_, ms::EdgeDirection::OUT, ms::View::NEW).has_value());
    auto const merged = ctx.OutEdges(vertex.impl_, ms::View::NEW);
    ASSERT_TRUE(merged.has_value());
    ASSERT_EQ(merged->size(), 10U);
    for (auto const &types : std::vector<std::vector<ms::EdgeTypeId>>{{}, {a}, {b}, {a, b}}) {
      for (uint64_t const budget : std::array<uint64_t, 5>{1, 4, 10, 11, 100}) {
        ms::HopsLimit limit{budget};
        auto const got = vertex.OutEdges(ms::View::NEW, types, &limit);
        ASSERT_TRUE(got.has_value());
        std::vector<int64_t> expected;
        for (size_t i = 0; i < std::min<size_t>(budget, merged->size()); ++i) {
          if (types.empty() || std::ranges::contains(types, (*merged)[i].EdgeType())) {
            expected.push_back((*merged)[i].Gid().AsInt());
          }
        }
        EXPECT_EQ(EdgeGids(got->edges), expected) << budget << " hops, " << types.size() << " types";
        ASSERT_EQ(got->edges.size(), expected.size());
        for (auto const &edge : got->edges) EXPECT_EQ(edge.branch_ctx(), &ctx);
        std::vector<mq::EdgeAccessor> materialised;
        got->edges.MaterialiseInto(materialised);
        ASSERT_EQ(materialised.size(), expected.size());
        for (size_t i = 0; i < expected.size(); ++i) EXPECT_EQ(materialised[i], got->edges[i]);
      }
    }
  });
}

// Native edges attached to a fork vertex while readers expand it: a reader never loses an edge it saw, never sees one
// twice, and its transaction's snapshot holds still; once the writer is done every edge is there.
TEST_F(VersioningReadTest, ConcurrentNativeEdgeAttachAndExpansionNeverLoseOrDuplicateAnEdge) {
  constexpr int kRounds = 200;
  constexpr size_t kForkEdges = 3;
  faker.Interpret("CREATE (h:Hub) WITH h UNWIND range(1, 3) AS i CREATE (h)-[:R]->(:T)");
  auto const hub = Int("MATCH (h:Hub) RETURN id(h)");
  Fork();
  auto const core = Ctx().core();
  auto &diff_engine = Ctx().diff_engine();
  auto const native_type = Main()->NameToEdgeType("N");
  std::atomic<bool> writer_done{false};

  {
    std::vector<std::jthread> threads;
    threads.emplace_back([&] {
      mv::BranchContext ctx{core};
      for (int round = 1; round <= kRounds; ++round) {
        auto diff = diff_engine.Access(ms::WRITE);
        ASSERT_TRUE(ctx.BeginTransaction(diff.get()).has_value());
        auto stub = ctx.EnsureDiffVertex(ms::Gid::FromInt(hub));
        ASSERT_TRUE(stub.has_value());
        auto native = ctx.CreateNativeVertex();
        ASSERT_TRUE(ctx.CreateNativeEdge(*stub, native, native_type).has_value());
        ASSERT_TRUE(ctx.CommitTransaction(memgraph::tests::MakeMainCommitArgs()).has_value());
        ctx.EndTransaction();
      }
      writer_done.store(true);
    });
    for (int reader = 0; reader < 3; ++reader) {
      threads.emplace_back([&] {
        mv::BranchContext ctx{core};
        size_t last_seen = 0;
        do {
          auto diff = diff_engine.Access(ms::READ);
          ASSERT_TRUE(ctx.BeginTransaction(diff.get()).has_value());
          std::vector<int64_t> first;
          for (int i = 0; i < 3; ++i) {
            auto const out = ForkVertexOn(ctx, hub).OutEdges(ms::View::OLD);
            ASSERT_TRUE(out.has_value());
            auto gids = EdgeGids(out->edges);
            std::ranges::sort(gids);
            ASSERT_EQ(std::ranges::adjacent_find(gids), gids.end()) << "an edge was listed twice";
            ASSERT_GE(gids.size(), kForkEdges + last_seen) << "an edge seen earlier is gone";
            if (i == 0) first = gids;
            ASSERT_EQ(gids, first) << "the snapshot moved inside one transaction";
          }
          last_seen = first.size() - kForkEdges;
          ctx.EndTransaction();
        } while (!writer_done.load());
      });
    }
  }

  InBranchTxn([&](mv::BranchContext &) {
    EXPECT_EQ(BranchVertex(hub).OutEdges(ms::View::OLD)->edges.size(), kForkEdges + kRounds);
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
          ASSERT_TRUE(*ctx.DeleteForkVertex(ms::Gid::FromInt(deleted)));

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

// Reading a set of properties off a branch accessor answers what the branch holds for each key (BL-0251): the
// branch's change, its removal, the fork's value where untouched, and Null where nothing is held.
TEST_F(VersioningReadTest, ReadPropertyValuesOnABranchReadsTheBranchStateOfEveryKey) {
  faker.Interpret("CREATE (:P {a: 1, b: 2, c: 3})-[:E {a: 1, b: 2, c: 3}]->(:P {a: 9})");
  auto const v = Int("MATCH (n:P {a: 1}) RETURN id(n)");
  auto const e = Int("MATCH ()-[e:E]->() RETURN id(e)");
  Fork();

  auto const a = Main()->NameToProperty("a");
  auto const b = Main()->NameToProperty("b");
  auto const c = Main()->NameToProperty("c");
  auto const absent = Main()->NameToProperty("absent");
  InBranchTxn([&](mv::BranchContext &ctx) {
    auto fork = ctx.FindForkVertex(ms::Gid::FromInt(v));
    ASSERT_TRUE(fork.has_value());
    ASSERT_TRUE(ctx.SetProperty(*fork, a, ms::PropertyValue{100}).has_value());
    ASSERT_TRUE(ctx.SetProperty(*fork, b, ms::PropertyValue{}).has_value());
    auto fork_edge = ctx.FindForkEdge(ms::Gid::FromInt(e), ms::Gid::FromInt(v));
    ASSERT_TRUE(fork_edge.has_value());
    ASSERT_TRUE(ctx.SetProperty(*fork_edge, a, ms::PropertyValue{100}).has_value());
    ASSERT_TRUE(ctx.SetProperty(*fork_edge, b, ms::PropertyValue{}).has_value());
  });

  auto const ordered = ms::OrderedPropertyPaths::FromPropertyIds(std::array{a, b, c, absent});
  auto const expect_branch_state = [&](auto const &accessor) {
    std::array<ms::PropertyValue, 4> out;
    ASSERT_TRUE(accessor.ReadPropertyValues(ordered, out, ms::View::NEW).has_value());
    for (std::size_t slot = 0; slot != out.size(); ++slot) {
      auto const key = ordered.paths()[slot][0];
      auto const expected = key == a ? ms::PropertyValue{100} : key == c ? ms::PropertyValue{3} : ms::PropertyValue{};
      EXPECT_EQ(out[slot], expected) << "slot " << slot;
      EXPECT_EQ(out[slot], *accessor.GetProperty(ms::View::NEW, key)) << "set read equals the single-key read";
    }
  };

  InBranchTxn([&](mv::BranchContext &ctx) {
    expect_branch_state(BranchVertex(v));
    auto fork_edge = ctx.FindForkEdge(ms::Gid::FromInt(e), ms::Gid::FromInt(v));
    ASSERT_TRUE(fork_edge.has_value());
    expect_branch_state(mq::EdgeAccessor{*fork_edge, &ctx});

    std::array<ms::PropertyValue, 4> out;
    ASSERT_TRUE(BranchVertex(v).impl_.ReadPropertyValues(ordered, out, ms::View::NEW).has_value());
    EXPECT_EQ(out[std::ranges::find(ordered.paths(), ms::PropertyPath{a}) - ordered.paths().begin()],
              ms::PropertyValue{1})
        << "an accessor off a branch reads the object as is";
  });

  InBranchTxn(
      [&](mv::BranchContext &ctx) {
        ASSERT_TRUE(*ctx.DeleteForkVertex(ms::Gid::FromInt(v)));
        std::array<ms::PropertyValue, 4> out;
        auto const vertex_read = BranchVertex(v).ReadPropertyValues(ordered, out, ms::View::NEW);
        ASSERT_FALSE(vertex_read.has_value());
        EXPECT_EQ(vertex_read.error(), ms::Error::DELETED_OBJECT);
        auto fork_edge = ctx.FindForkEdge(ms::Gid::FromInt(e), ms::Gid::FromInt(v));
        ASSERT_TRUE(fork_edge.has_value());
        auto const edge_read = mq::EdgeAccessor{*fork_edge, &ctx}.ReadPropertyValues(ordered, out, ms::View::NEW);
        ASSERT_FALSE(edge_read.has_value());
        EXPECT_EQ(edge_read.error(), ms::Error::DELETED_OBJECT);
      },
      /*commit=*/false);
}

// A branch-native vertex deleted in the open transaction is gone for its own adjacency reads too (BL-0228), though no
// lookup by gid can see it any more.
TEST_F(VersioningReadTest, AdjacencyOfANativeVertexDeletedInTheTransactionFails) {
  Fork();
  InBranchTxn(
      [&](mv::BranchContext &ctx) {
        auto native = ctx.CreateNativeVertex();
        mq::VertexAccessor const vertex{native, &ctx};
        EXPECT_EQ(*vertex.OutDegree(ms::View::NEW), 0U);
        ASSERT_TRUE(ctx.current_diff_txn()->DeleteVertex(&native).has_value());

        EXPECT_EQ(vertex.OutDegree(ms::View::NEW), std::unexpected{ms::Error::DELETED_OBJECT});
        EXPECT_EQ(vertex.InDegree(ms::View::NEW), std::unexpected{ms::Error::DELETED_OBJECT});
        EXPECT_EQ(vertex.OutEdges(ms::View::NEW).error(), ms::Error::DELETED_OBJECT);
        EXPECT_EQ(vertex.InEdges(ms::View::NEW).error(), ms::Error::DELETED_OBJECT);
      },
      /*commit=*/false);
}

// A value holding a fork object is deleted when the branch deleted or hid it (BL-0240), not when main deleted it after
// the fork: the branch still has that object.
TEST_F(VersioningReadTest, ContainsDeletedFollowsTheBranchAndNotMain) {
  faker.Interpret("CREATE (:A)-[:R]->(:B), (:C)-[:S]->(:D), (:Gone)");
  auto const a = Int("MATCH (n:A) RETURN id(n)");
  auto const b = Int("MATCH (n:B) RETURN id(n)");
  auto const c = Int("MATCH (n:C) RETURN id(n)");
  auto const gone = Int("MATCH (n:Gone) RETURN id(n)");
  auto const e = Int("MATCH ()-[e:R]->() RETURN id(e)");
  auto const f = Int("MATCH ()-[e:S]->() RETURN id(e)");
  Fork();

  {
    auto acc = Main()->Access(ms::WRITE);
    auto gone_vertex = acc->FindVertex(ms::Gid::FromInt(gone), ms::View::OLD);
    ASSERT_TRUE(gone_vertex.has_value());
    ASSERT_TRUE(acc->DeleteVertex(&*gone_vertex).has_value());
    auto c_vertex = acc->FindVertex(ms::Gid::FromInt(c), ms::View::OLD);
    ASSERT_TRUE(c_vertex.has_value());
    auto out = c_vertex->OutEdges(ms::View::OLD);
    ASSERT_TRUE(out.has_value() && out->edges.size() == 1);
    auto edge = out->edges[0];
    ASSERT_TRUE(acc->DeleteEdge(&edge).has_value());
    ASSERT_TRUE(acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()).has_value());
  }

  auto const contains_deleted = [](auto const &value) { return mq::TypedValue{value}.ContainsDeleted(); };
  auto const fork_edge = [](mv::BranchContext &ctx, int64_t edge, int64_t from) {
    auto found = ctx.FindForkEdge(ms::Gid::FromInt(edge), ms::Gid::FromInt(from));
    MG_ASSERT(found.has_value(), "no fork edge {}", edge);
    return mq::EdgeAccessor{*found, &ctx};
  };

  InBranchTxn(
      [&](mv::BranchContext &ctx) {
        EXPECT_FALSE(contains_deleted(BranchVertex(gone))) << "main's later delete is not the branch's";
        EXPECT_FALSE(contains_deleted(fork_edge(ctx, f, c))) << "nor is main's later edge delete";
        EXPECT_FALSE(contains_deleted(BranchVertex(a)));
        EXPECT_FALSE(contains_deleted(fork_edge(ctx, e, a)));

        ASSERT_TRUE(*ctx.DeleteForkVertex(ms::Gid::FromInt(a)));
        EXPECT_TRUE(contains_deleted(BranchVertex(a)));
        EXPECT_TRUE(contains_deleted(fork_edge(ctx, e, a))) << "an edge of a deleted vertex is hidden";
        EXPECT_FALSE(contains_deleted(BranchVertex(b)));
        EXPECT_TRUE(contains_deleted(
            mq::TypedValue{mq::TypedValue::TVector{mq::TypedValue{BranchVertex(b)}, mq::TypedValue{BranchVertex(a)}}}));
        EXPECT_TRUE(contains_deleted(mq::Path{BranchVertex(b), fork_edge(ctx, e, a), BranchVertex(a)}));
        EXPECT_FALSE(contains_deleted(mq::Path{BranchVertex(b)}));

        auto native = ctx.CreateNativeVertex();
        EXPECT_FALSE(contains_deleted(mq::VertexAccessor{native, &ctx}));
        ASSERT_TRUE(ctx.current_diff_txn()->DeleteVertex(&native).has_value());
        EXPECT_TRUE(contains_deleted(mq::VertexAccessor{native, &ctx})) << "a native vertex carries its own flag";
      },
      /*commit=*/false);

  InBranchTxn(
      [&](mv::BranchContext &ctx) {
        auto edge = ctx.FindForkEdge(ms::Gid::FromInt(e), ms::Gid::FromInt(a));
        ASSERT_TRUE(edge.has_value());
        ASSERT_TRUE(*ctx.DeleteForkEdge(*edge));
        EXPECT_TRUE(contains_deleted(mq::EdgeAccessor{*edge, &ctx})) << "an edge deleted on its own";
        EXPECT_FALSE(contains_deleted(BranchVertex(a)));
      },
      /*commit=*/false);
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

// One reader transaction keeps a single snapshot over all its reads while another session commits on the branch and
// main commits after the fork; the next transaction sees the branch's commit and still never main's.
TEST_F(VersioningReadTest, RepeatedReadsInOneBranchTransactionStaySnapshotStableAcrossCommits) {
  faker.Interpret("CREATE (:P {id: 1, x: 1})-[:E]->(:P {id: 2})");
  auto const v = Int("MATCH (n:P {id: 1}) RETURN id(n)");
  Fork();
  auto const x = Main()->NameToProperty("x");
  auto const q = Main()->NameToLabel("Q");

  auto const expect_reads = [&](int64_t want_x, bool want_q) {
    for (auto const view : {ms::View::OLD, ms::View::NEW}) {
      auto vertex = BranchVertex(v);
      EXPECT_EQ(*vertex.GetProperty(view, x), ms::PropertyValue{want_x});
      EXPECT_EQ(*vertex.HasLabel(view, q), want_q);
      EXPECT_EQ(*vertex.OutDegree(view), 1U);
    }
  };

  mv::BranchContext other_session{Ctx().core()};
  auto reader_diff = Ctx().diff_engine().Access(ms::READ);
  ASSERT_TRUE(Ctx().BeginTransaction(reader_diff.get()).has_value());
  expect_reads(1, false);
  expect_reads(1, false);

  {
    auto writer_diff = Ctx().diff_engine().Access(ms::WRITE);
    ASSERT_TRUE(other_session.BeginTransaction(writer_diff.get()).has_value());
    WriteProperty(other_session, v, x, ms::PropertyValue{2});
    ToggleLabel(other_session, v, q, true);
    ASSERT_TRUE(other_session.CommitTransaction(memgraph::tests::MakeMainCommitArgs()).has_value());
    other_session.EndTransaction();
  }
  {
    auto acc = Main()->Access(ms::WRITE);
    auto main_v = acc->FindVertex(ms::Gid::FromInt(v), ms::View::OLD);
    ASSERT_TRUE(main_v.has_value());
    ASSERT_TRUE(main_v->SetProperty(x, ms::PropertyValue{3}).has_value());
    auto later = acc->CreateVertex();
    ASSERT_TRUE(acc->CreateEdge(&*main_v, &later, Main()->NameToEdgeType("E")).has_value());
    ASSERT_TRUE(acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()).has_value());
  }

  expect_reads(1, false);
  Ctx().EndTransaction();
  reader_diff.reset();

  InBranchTxn([&](mv::BranchContext &) { expect_reads(2, true); }, /*commit=*/false);
}

// Reads, then a write, in one branch transaction: the reads before and after it agree on the snapshot, the write is
// visible to NEW only, a rollback leaves nothing behind, and once another session has committed on the object the
// transaction's own write conflicts like on main.
TEST_F(VersioningReadTest, ReadsThenAWriteInOneBranchTransaction) {
  faker.Interpret("CREATE (:P {id: 1, x: 1})");
  auto const v = Int("MATCH (n:P) RETURN id(n)");
  Fork();
  auto const x = Main()->NameToProperty("x");

  InBranchTxn(
      [&](mv::BranchContext &ctx) {
        for (int i = 0; i < 3; ++i) EXPECT_EQ(*BranchVertex(v).GetProperty(ms::View::OLD, x), ms::PropertyValue{1});
        WriteProperty(ctx, v, x, ms::PropertyValue{5});
        EXPECT_EQ(*BranchVertex(v).GetProperty(ms::View::NEW, x), ms::PropertyValue{5});
        EXPECT_EQ(*BranchVertex(v).GetProperty(ms::View::OLD, x), ms::PropertyValue{1});
      },
      /*commit=*/false);
  InBranchTxn([&](mv::BranchContext &) {
    EXPECT_EQ(*BranchVertex(v).GetProperty(ms::View::NEW, x), ms::PropertyValue{1}) << "the rollback left nothing";
  });

  mv::BranchContext other_session{Ctx().core()};
  auto diff = Ctx().diff_engine().Access(ms::WRITE);
  ASSERT_TRUE(Ctx().BeginTransaction(diff.get()).has_value());
  EXPECT_EQ(*BranchVertex(v).GetProperty(ms::View::NEW, x), ms::PropertyValue{1});
  {
    auto other_diff = Ctx().diff_engine().Access(ms::WRITE);
    ASSERT_TRUE(other_session.BeginTransaction(other_diff.get()).has_value());
    WriteProperty(other_session, v, x, ms::PropertyValue{7});
    ASSERT_TRUE(other_session.CommitTransaction(memgraph::tests::MakeMainCommitArgs()).has_value());
    other_session.EndTransaction();
  }
  EXPECT_EQ(*BranchVertex(v).GetProperty(ms::View::NEW, x), ms::PropertyValue{1}) << "the snapshot did not move";
  auto fork = Ctx().FindForkVertex(ms::Gid::FromInt(v));
  ASSERT_TRUE(fork.has_value());
  EXPECT_FALSE(Ctx().SetProperty(*fork, x, ms::PropertyValue{9}).has_value())
      << "a write over a newer commit conflicts";
  EXPECT_EQ(*BranchVertex(v).GetProperty(ms::View::NEW, x), ms::PropertyValue{1});
  Ctx().EndTransaction();
}

// Bounded stress: one branch writer keeps two fork vertices summing to zero, main rewrites the same vertices with
// junk, and concurrent readers must see, within a transaction, one unchanging pair that sums to zero and never
// moves backwards between transactions.
TEST_F(VersioningReadTest, ConcurrentBranchReadersAndWritersSeeAConstantTotal) {
  constexpr int kRounds = 300;
  faker.Interpret("CREATE (:P {id: 1, x: 0}), (:P {id: 2, x: 0})");
  auto const a = Int("MATCH (n:P {id: 1}) RETURN id(n)");
  auto const b = Int("MATCH (n:P {id: 2}) RETURN id(n)");
  Fork();
  auto const x = Main()->NameToProperty("x");
  auto const core = Ctx().core();
  auto &diff_engine = Ctx().diff_engine();
  std::atomic<int> writers_left{2};

  {
    std::vector<std::jthread> threads;
    threads.emplace_back([&] {
      mv::BranchContext ctx{core};
      for (int round = 1; round <= kRounds; ++round) {
        auto diff = diff_engine.Access(ms::WRITE);
        ASSERT_TRUE(ctx.BeginTransaction(diff.get()).has_value());
        WriteProperty(ctx, a, x, ms::PropertyValue{round});
        WriteProperty(ctx, b, x, ms::PropertyValue{-round});
        ASSERT_TRUE(ctx.CommitTransaction(memgraph::tests::MakeMainCommitArgs()).has_value());
        ctx.EndTransaction();
      }
      --writers_left;
    });
    threads.emplace_back([&] {
      for (int round = 0; round < kRounds; ++round) {
        WriteMainProperty(a, x, ms::PropertyValue{1000 + round});
        WriteMainProperty(b, x, ms::PropertyValue{1000 + round});
      }
      --writers_left;
    });
    for (int reader = 0; reader < 3; ++reader) {
      threads.emplace_back([&] {
        mv::BranchContext ctx{core};
        int64_t last_round = 0;
        do {
          auto diff = diff_engine.Access(ms::READ);
          ASSERT_TRUE(ctx.BeginTransaction(diff.get()).has_value());
          int64_t first_a = 0;
          for (int i = 0; i < 3; ++i) {
            auto const read_a = ForkVertexOn(ctx, a).GetProperty(ms::View::OLD, x)->ValueInt();
            auto const read_b = ForkVertexOn(ctx, b).GetProperty(ms::View::OLD, x)->ValueInt();
            ASSERT_EQ(read_a + read_b, 0) << "main's junk or a half-published commit leaked in";
            if (i == 0) first_a = read_a;
            ASSERT_EQ(read_a, first_a) << "the snapshot moved inside one transaction";
          }
          ASSERT_GE(first_a, last_round);
          last_round = first_a;
          ctx.EndTransaction();
        } while (writers_left.load() > 0);
      });
    }
  }

  InBranchTxn([&](mv::BranchContext &) {
    EXPECT_EQ(*BranchVertex(a).GetProperty(ms::View::OLD, x), ms::PropertyValue{kRounds});
    EXPECT_EQ(*BranchVertex(b).GetProperty(ms::View::OLD, x), ms::PropertyValue{-kRounds});
  });
  EXPECT_EQ(MainProperty(a, x), ms::PropertyValue{1000 + kRounds - 1});
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

  InBranchTxn([&](mv::BranchContext &ctx) { ASSERT_TRUE(*ctx.DeleteForkVertex(ms::Gid::FromInt(one))); });
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
        EXPECT_EQ(by_from->branch_ctx(), &ctx);
        EXPECT_TRUE(dba.FindEdge(ms::Gid::FromInt(native_edge), ms::Gid::FromInt(a_gid), ms::View::NEW).has_value())
            << "a native edge hangs off the fork vertex's stub";
        EXPECT_FALSE(dba.FindEdge(ms::Gid::FromInt(fork_edge), ms::Gid::FromInt(a_gid), ms::View::NEW).has_value())
            << "only the FROM vertex's OUT adjacency is searched";

        ASSERT_TRUE(*ctx.DeleteForkVertex(ms::Gid::FromInt(s_gid)));
        EXPECT_FALSE(dba.FindVertex(ms::Gid::FromInt(s_gid), ms::View::NEW).has_value());
        EXPECT_TRUE(dba.FindVertex(ms::Gid::FromInt(s_gid), ms::View::OLD).has_value());
        EXPECT_FALSE(dba.FindEdge(ms::Gid::FromInt(fork_edge), ms::View::NEW).has_value())
            << "a deleted endpoint hides the fork edge";
        auto const found = dba.FindVertex(ms::Gid::FromInt(a_gid), ms::View::NEW);
        ASSERT_TRUE(found.has_value());
        EXPECT_EQ(found->branch_ctx(), &ctx);
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

// Bolt encodes what the branch reads: the stub-merged labels and properties, and a native edge's stub endpoint as the
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
    ASSERT_TRUE(ctx.SetProperty(*fork_edge, w, ms::PropertyValue{5}).has_value());
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
    auto c_stub = ctx.EnsureDiffVertex(ms::Gid::FromInt(c_gid));
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
      EXPECT_EQ(edge.branch_ctx(), &ctx);
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

// A query-level reader keeps one snapshot over every statement of an explicit transaction while another session
// commits on the branch and main commits (a changed property and a new vertex) after the fork; the next statement
// outside the transaction sees the branch's commit and still never main's.
TEST_F(VersioningReadTest, StatementsOfOneBranchTransactionReadOneSnapshotWhileOthersCommit) {
  faker.Interpret("CREATE (:P {id: 1, x: 1}), (:P {id: 2, x: 1})");
  auto const a = Int("MATCH (n:P {id: 1}) RETURN id(n)");
  auto const b = Int("MATCH (n:P {id: 2}) RETURN id(n)");
  Fork();
  auto const x = Main()->NameToProperty("x");

  auto const expect_snapshot = [&](int64_t sum, int64_t count) {
    EXPECT_EQ(IntRows(faker, "MATCH (n:P) RETURN sum(n.x)"), (Rows{{sum}}));
    EXPECT_EQ(IntRows(faker, "MATCH (n:P) RETURN count(n)"), (Rows{{count}}));
    EXPECT_EQ(IntRows(faker, "MATCH (n) RETURN count(n)"), (Rows{{count}}));
  };

  faker.Interpret("BEGIN");
  expect_snapshot(2, 2);
  expect_snapshot(2, 2);

  {
    mv::BranchContext other_session{Ctx().core()};
    auto diff = Ctx().diff_engine().Access(ms::WRITE);
    ASSERT_TRUE(other_session.BeginTransaction(diff.get()).has_value());
    WriteProperty(other_session, a, x, ms::PropertyValue{10});
    WriteProperty(other_session, b, x, ms::PropertyValue{10});
    ASSERT_TRUE(other_session.CommitTransaction(memgraph::tests::MakeMainCommitArgs()).has_value());
    other_session.EndTransaction();
  }
  WriteMainProperty(a, x, ms::PropertyValue{100});
  {
    auto acc = Main()->Access(ms::WRITE);
    auto later = acc->CreateVertex();
    ASSERT_TRUE(later.AddLabel(Main()->NameToLabel("P")).has_value());
    ASSERT_TRUE(later.SetProperty(x, ms::PropertyValue{1000}).has_value());
    ASSERT_TRUE(acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()).has_value());
  }

  expect_snapshot(2, 2);
  faker.Interpret("COMMIT");
  expect_snapshot(20, 2);
}

// Bounded stress through real sessions: one branch writer keeps two fork vertices summing to zero (its commit is
// all-or-nothing), main rewrites the same vertices with junk, and reader sessions must see, in each explicit
// transaction, one unchanging pair that sums to zero.
TEST_F(VersioningReadTest, ConcurrentBranchSessionsSeeAConstantTotal) {
  constexpr int kRounds = 150;
  faker.Interpret("CREATE (:P {id: 1, x: 0}), (:P {id: 2, x: 0})");
  auto const a = Int("MATCH (n:P {id: 1}) RETURN id(n)");
  auto const b = Int("MATCH (n:P {id: 2}) RETURN id(n)");
  Fork();
  auto const x = Main()->NameToProperty("x");
  auto const core = Ctx().core();
  auto &diff_engine = Ctx().diff_engine();
  std::atomic<int> writers_left{2};

  // Built here, not in the threads: each faker repoints the context's auth checker at itself.
  std::vector<std::unique_ptr<InterpreterFaker>> readers;
  for (int reader = 0; reader < 2; ++reader) {
    readers.push_back(std::make_unique<InterpreterFaker>(&interpreter_context, db));
    Attach(*readers.back(), "b");
  }
  interpreter_context.auth_checker = &faker.auth_checker;

  {
    std::vector<std::jthread> threads;
    threads.emplace_back([&] {
      mv::BranchContext ctx{core};
      for (int round = 1; round <= kRounds; ++round) {
        auto diff = diff_engine.Access(ms::WRITE);
        ASSERT_TRUE(ctx.BeginTransaction(diff.get()).has_value());
        WriteProperty(ctx, a, x, ms::PropertyValue{round});
        WriteProperty(ctx, b, x, ms::PropertyValue{-round});
        ASSERT_TRUE(ctx.CommitTransaction(memgraph::tests::MakeMainCommitArgs()).has_value());
        ctx.EndTransaction();
      }
      --writers_left;
    });
    threads.emplace_back([&] {
      for (int round = 0; round < kRounds; ++round) {
        WriteMainProperty(a, x, ms::PropertyValue{1000 + round});
        WriteMainProperty(b, x, ms::PropertyValue{1000 + round});
      }
      --writers_left;
    });
    for (auto &reader : readers) {
      threads.emplace_back([&session = *reader, &writers_left] {
        do {
          session.Interpret("BEGIN");
          auto const first = IntRows(session, "MATCH (n:P {id: 1}) RETURN n.x, id(n)");
          auto const total = IntRows(session, "MATCH (n:P) RETURN sum(n.x)");
          auto const again = IntRows(session, "MATCH (n:P {id: 1}) RETURN n.x, id(n)");
          session.Interpret("COMMIT");
          ASSERT_EQ(total, (Rows{{0}})) << "main's junk or a half-published commit leaked in";
          ASSERT_EQ(first, again) << "the snapshot moved inside one transaction";
        } while (writers_left.load() > 0);
      });
    }
  }

  auto const final_rows = IntRows(faker, "MATCH (n:P) RETURN n.x ORDER BY n.x");
  EXPECT_EQ(final_rows, (Rows{{-kRounds}, {kRounds}}));
  EXPECT_EQ(MainProperty(a, x), ms::PropertyValue{1000 + kRounds - 1});
}

// An edge by id alone resolves to the fork edge, or to a branch-native one, and never to the diff engine's stub of a
// fork edge; ids that name nothing on the branch resolve to nothing.
TEST_F(VersioningReadTest, EdgeByIdNeverYieldsAStub) {
  faker.Interpret("CREATE (:S {i: 1})-[:E {w: 1}]->(:A), (:S {i: 2})-[:E {w: 2}]->(:A)");
  auto const s1 = Int("MATCH (n:S {i: 1}) RETURN id(n)");
  auto const s2 = Int("MATCH (n:S {i: 2}) RETURN id(n)");
  auto const stubbed_edge = Int("MATCH (:S {i: 1})-[e:E]->() RETURN id(e)");
  auto const plain_edge = Int("MATCH (:S {i: 2})-[e:E]->() RETURN id(e)");
  auto const a_gid = Int("MATCH (:S {i: 2})-->(n:A) RETURN id(n)");
  Fork();

  auto const w = Main()->NameToProperty("w");
  int64_t native_edge = 0;
  InBranchTxn([&](mv::BranchContext &ctx) {
    auto fork_edge = ctx.FindForkEdge(ms::Gid::FromInt(stubbed_edge), ms::Gid::FromInt(s1));
    ASSERT_TRUE(fork_edge.has_value());
    ASSERT_TRUE(ctx.SetProperty(*fork_edge, w, ms::PropertyValue{10}).has_value());
    native_edge = CreateEdgeFromFork(ctx, s2, ctx.CreateNativeVertex(), Main()->NameToEdgeType("F")).Gid().AsInt();
  });

  InBranchTxn(
      [&](mv::BranchContext &ctx) {
        auto dba = BranchDba(ctx);
        auto const upper = std::max({stubbed_edge, plain_edge, native_edge}) + 10;
        for (int64_t gid = 0; gid <= upper; ++gid) {
          auto const found = dba.FindEdge(ms::Gid::FromInt(gid), ms::View::NEW);
          bool const expected = gid == stubbed_edge || gid == plain_edge || gid == native_edge;
          ASSERT_EQ(found.has_value(), expected) << "edge " << gid;
          if (!found) continue;
          EXPECT_EQ(found->Gid().AsInt(), gid);
          EXPECT_EQ(found->branch_ctx(), &ctx);
          EXPECT_EQ(found->impl_.storage_ == &ctx.diff_engine(), gid == native_edge)
              << "only a native edge is the diff engine's own object (edge " << gid << ")";
        }
        EXPECT_EQ(*dba.FindEdge(ms::Gid::FromInt(stubbed_edge), ms::View::NEW)->GetProperty(ms::View::NEW, w),
                  ms::PropertyValue{10})
            << "found by id, read through the stub";
        EXPECT_EQ(dba.FindVertex(ms::Gid::FromInt(a_gid), ms::View::NEW)->impl_.storage_, Main());
      },
      /*commit=*/false);
}

// A main that keeps no edge objects (no properties on edges) answers an edge-by-id lookup with nothing; the branch
// answers the same, instead of walking every vertex's adjacency for it.
class VersioningReadReferenceEdgeTest : public VersioningFixture<false> {
 protected:
  void SetUp() override {
    VersioningFixture<false>::SetUp();
    SatisfyGate();
  }
};

TEST_F(VersioningReadReferenceEdgeTest, EdgeByIdOnABranchAnswersAsMainDoes) {
  faker.Interpret("CREATE (:S)-[:E]->(:A)");
  auto const edge_gid = IntRows(faker, "MATCH ()-[e:E]->() RETURN id(e)").at(0).at(0);
  auto const edge_by_id = "MATCH ()-[e]->() WHERE id(e) = " + std::to_string(edge_gid) + " RETURN count(e)";
  auto const on_main = IntRows(faker, edge_by_id).at(0).at(0);
  Fork();
  ASSERT_TRUE(PlanHasOperator(ExplainText(faker, edge_by_id), "ScanAllByEdgeId"));
  EXPECT_EQ(IntRows(faker, edge_by_id).at(0).at(0), on_main);
  EXPECT_EQ(IntRows(faker, "MATCH ()-[e:E]->() RETURN count(e)").at(0).at(0), 1) << "the edge is on the branch";
}

// BL-0241: the planner and mgp read approximate counts off the DbAccessor. A branch's count is main's at the fork plus
// the branch-native objects less the fork objects the branch deleted: not the diff engine's alone, and not main's live
// count, which moves with main's later writes.
TEST_F(VersioningReadTest, ApproximateCountsOnABranchCoverTheForkState) {
  faker.Interpret("UNWIND range(1, 20) AS i CREATE (:C {i: i})-[:R]->(:D)");
  faker.Interpret("CREATE (:Lone)");
  auto const lone = Int("MATCH (n:Lone) RETURN id(n)");
  auto const main_vertices = static_cast<int64_t>(Main()->Access(ms::READ)->ApproximateVertexCount());
  auto const main_edges = static_cast<int64_t>(Main()->Access(ms::READ)->ApproximateEdgeCount());
  ASSERT_EQ(main_vertices, 41);
  ASSERT_EQ(main_edges, 20);
  Fork();

  InBranchTxn(
      [&](mv::BranchContext &ctx) {
        auto dba = BranchDba(ctx);
        EXPECT_EQ(dba.VerticesCount(), main_vertices);
        EXPECT_EQ(dba.EdgesCount(), main_edges);
      },
      /*commit=*/false);

  InterpreterFaker main_session{&interpreter_context, db};
  main_session.Interpret("CREATE (:Later)-[:R]->(:Later)");
  ASSERT_EQ(Main()->Access(ms::READ)->ApproximateVertexCount(), main_vertices + 2) << "main moved on";

  InBranchTxn([&](mv::BranchContext &ctx) {
    auto first = ctx.CreateNativeVertex();
    auto second = ctx.CreateNativeVertex();
    ASSERT_TRUE(ctx.CreateNativeEdge(first, second, Main()->NameToEdgeType("N")).has_value());
  });
  InBranchTxn(
      [&](mv::BranchContext &ctx) {
        auto dba = BranchDba(ctx);
        EXPECT_EQ(dba.VerticesCount(), main_vertices + 2);
        EXPECT_EQ(dba.EdgesCount(), main_edges + 1);
      },
      /*commit=*/false);

  InBranchTxn([&](mv::BranchContext &ctx) { ASSERT_TRUE(*ctx.DeleteForkVertex(ms::Gid::FromInt(lone))); });
  InBranchTxn(
      [&](mv::BranchContext &ctx) {
        auto dba = BranchDba(ctx);
        EXPECT_EQ(dba.VerticesCount(), main_vertices + 1) << "a committed fork delete leaves the count";
        EXPECT_EQ(dba.EdgesCount(), main_edges + 1);
      },
      /*commit=*/false);
}

// The cost model reads those counts, so a branch plans a query the way main plans it over the same data.
TEST_F(VersioningReadTest, BranchPlansAQueryAsMainDoes) {
  faker.Interpret("UNWIND range(1, 200) AS i CREATE (:A {k: i})-[:R]->(:B {k: i % 7})");
  auto const query = "MATCH (a)-[:R]->(b) WHERE b.k = 3 RETURN count(a)";
  auto const main_plan = ExplainText(faker, query);
  Fork();
  EXPECT_EQ(ExplainText(faker, query), main_plan);
}

// A statement that deletes a branch-native vertex and then reads it fails the way it does on main (BL-0228).
TEST_F(VersioningReadTest, ReadingANativeVertexTheStatementDeletedFailsAsOnMain) {
  Fork();
  faker.Interpret("CREATE (:N)");
  auto const queries = {"MATCH (n:N) DETACH DELETE n RETURN degree(n)",
                        "MATCH (n:N) DETACH DELETE n RETURN in_degree(n)",
                        "MATCH (n:N) DETACH DELETE n RETURN labels(n)",
                        "MATCH (n:N) DETACH DELETE n RETURN properties(n)"};
  std::vector<std::string> on_branch;
  for (auto const *query : queries) on_branch.push_back(ThrownMessage([&] { faker.Interpret(query); }));

  faker.interpreter.current_db_.LeaveBranch();
  faker.Interpret("CREATE (:N)");
  size_t i = 0;
  for (auto const *query : queries) {
    auto const on_main = ThrownMessage([&] { faker.Interpret(query); });
    EXPECT_NE(on_main, "<no exception>") << query;
    EXPECT_EQ(on_branch[i++], on_main) << query;
  }
}

// An edge main deleted after the fork is still on the branch. With its FROM vertex it resolves through the fork-state
// adjacency; by id alone the lookup is main's, which no longer holds it.
TEST_F(VersioningReadTest, EdgeMainDeletedSinceTheForkIsFoundByIdOnlyWithItsFromVertex) {
  faker.Interpret("CREATE (:S)-[:E]->(:A)");
  auto const s_gid = Int("MATCH (n:S) RETURN id(n)");
  auto const edge = Int("MATCH ()-[e:E]->() RETURN id(e)");
  Fork();
  faker.interpreter.current_db_.LeaveBranch();
  faker.Interpret("MATCH ()-[e:E]->() DELETE e");
  Attach(faker, "b");

  EXPECT_EQ(Int("MATCH ()-[e:E]->() RETURN count(e)"), 1) << "the branch still has the edge";
  InBranchTxn(
      [&](mv::BranchContext &ctx) {
        auto dba = BranchDba(ctx);
        EXPECT_TRUE(dba.FindEdge(ms::Gid::FromInt(edge), ms::Gid::FromInt(s_gid), ms::View::NEW).has_value());
        EXPECT_FALSE(dba.FindEdge(ms::Gid::FromInt(edge), ms::View::NEW).has_value());
      },
      /*commit=*/false);
}
