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
#include <optional>
#include <string>
#include <thread>
#include <vector>

#include <gtest/gtest.h>

#include "auth/models.hpp"
#include "glue/auth_checker.hpp"
#include "query/db_accessor.hpp"
#include "query/edge_accessor.hpp"
#include "query/frontend/ast/query/auth_query.hpp"
#include "query/path.hpp"
#include "query/typed_value.hpp"
#include "query/vertex_accessor.hpp"
#include "storage/v2/hops_limit.hpp"
#include "storage/v2/property_value.hpp"
#include "tests/test_commit_args_helper.hpp"
#include "versioning/branch_engine.hpp"
#include "versioning_fixture.hpp"

using memgraph::tests::versioning_fixture::IntRows;

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
