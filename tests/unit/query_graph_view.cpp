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

#include <gtest/gtest.h>

#include <optional>
#include <set>
#include <string_view>
#include <utility>
#include <variant>
#include <vector>

#include "query/context.hpp"
#include "query/db_accessor.hpp"
#include "query/graph.hpp"
#include "query/graph_view.hpp"
#include "query/interpret/frame.hpp"
#include "query/path.hpp"
#include "query/plan/operator.hpp"
#include "query/subgraph_graph_view.hpp"
#include "query/typed_value.hpp"
#include "query/virtual_graph.hpp"
#include "query/virtual_graph_view.hpp"
#include "query/virtual_node.hpp"
#include "storage/v2/inmemory/storage.hpp"
#include "storage/v2/storage.hpp"
#include "tests/test_commit_args_helper.hpp"
#include "utils/memory.hpp"

#ifdef MG_ENTERPRISE
#include "auth/models.hpp"
#include "glue/auth_checker.hpp"
#include "license/license.hpp"
#include "query/frontend/ast/ast.hpp"
#endif

namespace memgraph::query::test {

using storage::Gid;
using storage::View;

class GraphViewTest : public ::testing::Test {
 protected:
  std::unique_ptr<storage::Storage> storage_ = std::make_unique<storage::InMemoryStorage>();

  // Inserts `count` vertices and returns their gids.
  std::set<Gid> InsertVertices(int count) {
    std::set<Gid> gids;
    auto acc = storage_->Access(storage::StorageAccessType::WRITE);
    auto dba = DbAccessor(acc.get());
    for (int i = 0; i < count; ++i) gids.insert(dba.InsertVertex().Gid());
    EXPECT_TRUE(dba.Commit(memgraph::tests::MakeMainCommitArgs()).has_value());
    return gids;
  }
};

// The identity view scans exactly the vertices in the real graph.
TEST_F(GraphViewTest, IdentityViewYieldsAllVertices) {
  const auto expected = InsertVertices(5);

  auto acc = storage_->Access(storage::StorageAccessType::READ);
  auto dba = DbAccessor(acc.get());
  DbAccessorGraphView view{&dba};

  std::set<Gid> scanned;
  for (auto vertex : view.Vertices(View::NEW)) scanned.insert(std::get<VertexAccessor>(vertex).Gid());

  EXPECT_EQ(scanned, expected);
}

// Names resolve through the view to the same ids the underlying accessor uses,
// and round-trip back to the same names.
TEST_F(GraphViewTest, NameMappingRoundTripsThroughView) {
  auto acc = storage_->Access(storage::StorageAccessType::WRITE);
  auto dba = DbAccessor(acc.get());
  DbAccessorGraphView view{&dba};

  const auto label = view.NameToLabel("Person");
  const auto prop = view.NameToProperty("name");
  const auto edge_type = view.NameToEdgeType("KNOWS");

  EXPECT_EQ(view.LabelToName(label), "Person");
  EXPECT_EQ(view.PropertyToName(prop), "name");
  EXPECT_EQ(view.EdgeTypeToName(edge_type), "KNOWS");

  // The view shares the accessor's namespace; it does not mint its own ids.
  EXPECT_EQ(label, dba.NameToLabel("Person"));
  EXPECT_EQ(prop, dba.NameToProperty("name"));
  EXPECT_EQ(edge_type, dba.NameToEdgeType("KNOWS"));
}

// ScanAll scans the graph bound into the execution context, not the context's
// own accessor. Binding a view over a populated graph while the accessor sees an
// empty one, ScanAll must yield the populated graph's vertices.
TEST_F(GraphViewTest, ScanAllReadsThroughBoundView) {
  const auto bound_gids = InsertVertices(3);

  // A second, empty storage that the context's own accessor points at.
  auto empty_storage = std::make_unique<storage::InMemoryStorage>();
  auto empty_acc = empty_storage->Access(storage::StorageAccessType::READ);
  auto empty_dba = DbAccessor(empty_acc.get());

  auto bound_acc = storage_->Access(storage::StorageAccessType::READ);
  auto bound_dba = DbAccessor(bound_acc.get());
  DbAccessorGraphView bound_view{&bound_dba};

  SymbolTable symbol_table;
  auto symbol = symbol_table.CreateSymbol("n", true);
  auto scan_all = std::make_shared<plan::ScanAll>(nullptr, symbol, View::NEW);

  memgraph::metrics::DatabaseMetricHandles metric_handles;
  ExecutionContext context{.db_accessor = &empty_dba};
  context.evaluation_context.graph_view = &bound_view;
  context.symbol_table = symbol_table;
  context.evaluation_context.memory = memgraph::utils::NewDeleteResource();
  context.metric_handles = &metric_handles;

  Frame frame(symbol_table.max_position());
  auto cursor = scan_all->MakeCursor(memgraph::utils::NewDeleteResource(), metric_handles);
  std::set<Gid> scanned;
  while (cursor->Pull(frame, context)) scanned.insert(frame[symbol].ValueVertex().Gid());

  EXPECT_EQ(scanned, bound_gids);
}

// A synthetic node carrying an import handle, so AssembleVirtualGraph can wire
// edges to it by handle.
VirtualNode HandledNode(int64_t handle, VirtualNode::label_list labels = {}) {
  VirtualNode node{std::move(labels), {}};
  node.SetHandle(handle);
  return node;
}

class VirtualGraphViewTest : public ::testing::Test {
 protected:
  std::unique_ptr<storage::Storage> storage_ = std::make_unique<storage::InMemoryStorage>();

  // A DbAccessor over an empty real graph, used only for the shared name space.
  std::unique_ptr<storage::Storage::Accessor> acc_ = storage_->Access(storage::StorageAccessType::READ);
  DbAccessor dba_{acc_.get()};
};

// The projection view scans exactly the nodes in the VirtualGraph.
TEST_F(VirtualGraphViewTest, ViewYieldsAllNodes) {
  std::vector<VirtualNode> nodes;
  nodes.push_back(HandledNode(1, {"A"}));
  nodes.push_back(HandledNode(2, {"B"}));
  nodes.push_back(HandledNode(3, {"C"}));
  std::set<Gid> expected{nodes[0].Gid(), nodes[1].Gid(), nodes[2].Gid()};

  auto graph = AssembleVirtualGraph(nodes, {}, DanglingEdgePolicy::kError, memgraph::utils::NewDeleteResource());
  VirtualGraphView view{&graph, &dba_};

  std::set<Gid> scanned;
  for (const auto &node : view.Nodes()) scanned.insert(node.Gid());

  EXPECT_EQ(scanned, expected);
}

// Out-edges and in-edges of a node are direction-respecting: a directed edge
// appears among its source's out-edges and its target's in-edges only.
TEST_F(VirtualGraphViewTest, ViewYieldsDirectedEdgesOfANode) {
  std::vector<VirtualNode> nodes;
  nodes.push_back(HandledNode(1, {"A"}));
  nodes.push_back(HandledNode(2, {"B"}));
  const auto src = nodes[0].Gid();
  const auto dst = nodes[1].Gid();

  std::vector<VirtualEdge> edges;
  edges.emplace_back(int64_t{1}, int64_t{2}, "KNOWS");

  auto graph = AssembleVirtualGraph(nodes, edges, DanglingEdgePolicy::kError, memgraph::utils::NewDeleteResource());
  VirtualGraphView view{&graph, &dba_};

  ASSERT_EQ(view.OutEdges(src).size(), 1);
  EXPECT_EQ(view.OutEdges(src)[0]->FromGid(), src);
  EXPECT_EQ(view.OutEdges(src)[0]->ToGid(), dst);

  ASSERT_EQ(view.InEdges(dst).size(), 1);
  EXPECT_EQ(view.InEdges(dst)[0]->ToGid(), dst);

  // The edge does not appear against the wrong endpoint or wrong direction.
  EXPECT_TRUE(view.InEdges(src).empty());
  EXPECT_TRUE(view.OutEdges(dst).empty());
}

// A self-loop is both an out-edge and an in-edge of its node.
TEST_F(VirtualGraphViewTest, ViewYieldsSelfLoopInBothDirections) {
  std::vector<VirtualNode> nodes;
  nodes.push_back(HandledNode(1, {"A"}));
  const auto self = nodes[0].Gid();

  std::vector<VirtualEdge> edges;
  edges.emplace_back(int64_t{1}, int64_t{1}, "SELF");

  auto graph = AssembleVirtualGraph(nodes, edges, DanglingEdgePolicy::kError, memgraph::utils::NewDeleteResource());
  VirtualGraphView view{&graph, &dba_};

  ASSERT_EQ(view.OutEdges(self).size(), 1);
  ASSERT_EQ(view.InEdges(self).size(), 1);
  EXPECT_EQ(view.OutEdges(self)[0]->Gid(), view.InEdges(self)[0]->Gid());
  EXPECT_EQ(view.OutEdges(self)[0]->FromGid(), self);
  EXPECT_EQ(view.OutEdges(self)[0]->ToGid(), self);
}

// ScanAll routed through a bound projection view yields the projection's nodes
// as VirtualNode values on the frame, the same operator that scans the real graph.
TEST_F(VirtualGraphViewTest, ScanAllOverProjectionYieldsVirtualNodes) {
  std::vector<VirtualNode> nodes;
  nodes.push_back(HandledNode(1, {"A"}));
  nodes.push_back(HandledNode(2, {"B"}));
  std::set<Gid> expected{nodes[0].Gid(), nodes[1].Gid()};
  auto graph = AssembleVirtualGraph(nodes, {}, DanglingEdgePolicy::kError, memgraph::utils::NewDeleteResource());
  VirtualGraphView view{&graph, &dba_};

  SymbolTable symbol_table;
  auto symbol = symbol_table.CreateSymbol("n", true);
  auto scan_all = std::make_shared<plan::ScanAll>(nullptr, symbol, View::NEW);

  memgraph::metrics::DatabaseMetricHandles metric_handles;
  ExecutionContext context{.db_accessor = &dba_};
  context.evaluation_context.graph_view = &view;
  context.symbol_table = symbol_table;
  context.evaluation_context.memory = memgraph::utils::NewDeleteResource();
  context.metric_handles = &metric_handles;

  Frame frame(symbol_table.max_position());
  auto cursor = scan_all->MakeCursor(memgraph::utils::NewDeleteResource(), metric_handles);
  std::set<Gid> scanned;
  while (cursor->Pull(frame, context)) scanned.insert(frame[symbol].ValueVirtualNode().Gid());

  EXPECT_EQ(scanned, expected);
}

#ifdef MG_ENTERPRISE
// A scanned overlay node is subject to the same fine-grained visibility decision
// as its origin vertex: an overlay over a vertex whose label the user is denied
// is filtered from the scan, so its read-through to the origin can never expose
// data the user could not read on the origin directly. A synthetic node carries
// no origin and stays visible.
TEST_F(VirtualGraphViewTest, ScanFiltersOverlayWhoseOriginIsDenied) {
  memgraph::license::global_license_checker.EnableTesting();

  auto wacc = storage_->Access(storage::StorageAccessType::WRITE);
  DbAccessor dba{wacc.get()};
  auto public_origin = dba.InsertVertex();
  ASSERT_TRUE(public_origin.AddLabel(dba.NameToLabel("Public")).has_value());
  auto secret_origin = dba.InsertVertex();
  ASSERT_TRUE(secret_origin.AddLabel(dba.NameToLabel("Secret")).has_value());
  dba.AdvanceCommand();

  std::vector<VirtualNode> nodes;
  nodes.emplace_back(
      VirtualNode::label_list{}, VirtualNode::property_map{}, VirtualNode::allocator_type{}, public_origin);
  nodes.back().SetHandle(1);
  const auto public_overlay_gid = nodes.back().Gid();
  nodes.emplace_back(
      VirtualNode::label_list{}, VirtualNode::property_map{}, VirtualNode::allocator_type{}, secret_origin);
  nodes.back().SetHandle(2);
  const auto secret_overlay_gid = nodes.back().Gid();
  nodes.push_back(HandledNode(3));
  const auto synthetic_gid = nodes.back().Gid();

  auto graph = AssembleVirtualGraph(nodes, {}, DanglingEdgePolicy::kError, memgraph::utils::NewDeleteResource());
  VirtualGraphView view{&graph, &dba};

  memgraph::auth::User user{"test"};
  user.db_access().GrantAll();
  user.fine_grained_access_handler().label_permissions().Grant({"Public"}, memgraph::auth::kAllLabelPermissions);
  user.fine_grained_access_handler().label_permissions().Deny({"Secret"}, memgraph::auth::kAllLabelPermissions);
  std::shared_ptr<FineGrainedAuthChecker> checker =
      std::make_shared<memgraph::glue::FineGrainedAuthChecker>(user, &dba);

  ASSERT_TRUE(checker->Has(public_origin, View::NEW, AuthQuery::FineGrainedPrivilege::READ));
  ASSERT_FALSE(checker->Has(secret_origin, View::NEW, AuthQuery::FineGrainedPrivilege::READ));

  SymbolTable symbol_table;
  auto symbol = symbol_table.CreateSymbol("n", true);
  auto scan_all = std::make_shared<plan::ScanAll>(nullptr, symbol, View::NEW);

  memgraph::metrics::DatabaseMetricHandles metric_handles;
  ExecutionContext context{.db_accessor = &dba};
  context.evaluation_context.graph_view = &view;
  context.symbol_table = symbol_table;
  context.evaluation_context.memory = memgraph::utils::NewDeleteResource();
  context.metric_handles = &metric_handles;
  context.auth_checker = checker.get();

  Frame frame(symbol_table.max_position());
  auto cursor = scan_all->MakeCursor(memgraph::utils::NewDeleteResource(), metric_handles);
  std::set<Gid> scanned;
  while (cursor->Pull(frame, context)) scanned.insert(frame[symbol].ValueVirtualNode().Gid());

  // The overlay over the denied origin is filtered; the granted overlay and the
  // synthetic node remain.
  EXPECT_EQ(scanned, (std::set<Gid>{public_overlay_gid, synthetic_gid}));
  EXPECT_EQ(scanned.count(secret_overlay_gid), 0U);
}
#endif

#ifdef MG_ENTERPRISE
// An overlay node reached by edge expansion (not by a scan) is also subject to
// its origin's fine-grained visibility: expanding to an overlay over a denied
// origin drops the edge, so a denied origin cannot be read through an expansion
// any more than through a scan.
TEST_F(VirtualGraphViewTest, ExpandToOverlayWhoseOriginIsDeniedIsFiltered) {
  memgraph::license::global_license_checker.EnableTesting();

  auto wacc = storage_->Access(storage::StorageAccessType::WRITE);
  DbAccessor dba{wacc.get()};
  auto secret_origin = dba.InsertVertex();
  ASSERT_TRUE(secret_origin.AddLabel(dba.NameToLabel("Secret")).has_value());
  dba.AdvanceCommand();

  // A synthetic source node with an out-edge to an overlay node over the denied
  // origin.
  std::vector<VirtualNode> nodes;
  nodes.push_back(HandledNode(1, {"Src"}));
  nodes.emplace_back(
      VirtualNode::label_list{}, VirtualNode::property_map{}, VirtualNode::allocator_type{}, secret_origin);
  nodes.back().SetHandle(2);
  const auto denied_gid = nodes.back().Gid();
  std::vector<VirtualEdge> edges;
  edges.emplace_back(int64_t{1}, int64_t{2}, "R");
  auto graph = AssembleVirtualGraph(nodes, edges, DanglingEdgePolicy::kError, memgraph::utils::NewDeleteResource());
  VirtualGraphView view{&graph, &dba};

  memgraph::auth::User user{"test"};
  user.db_access().GrantAll();
  user.fine_grained_access_handler().label_permissions().Deny({"Secret"}, memgraph::auth::kAllLabelPermissions);
  std::shared_ptr<FineGrainedAuthChecker> checker =
      std::make_shared<memgraph::glue::FineGrainedAuthChecker>(user, &dba);

  SymbolTable symbol_table;
  auto src = symbol_table.CreateSymbol("a", true);
  auto dst = symbol_table.CreateSymbol("m", true);
  auto edge = symbol_table.CreateSymbol("e", true);
  auto scan = std::make_shared<plan::ScanAll>(nullptr, src, View::NEW);
  auto expand = std::make_shared<plan::Expand>(
      scan, src, dst, edge, EdgeAtom::Direction::OUT, std::vector<storage::EdgeTypeId>{}, false, View::NEW);

  memgraph::metrics::DatabaseMetricHandles metric_handles;
  ExecutionContext context{.db_accessor = &dba};
  context.evaluation_context.graph_view = &view;
  context.symbol_table = symbol_table;
  context.evaluation_context.memory = memgraph::utils::NewDeleteResource();
  context.metric_handles = &metric_handles;
  context.auth_checker = checker.get();

  Frame frame(symbol_table.max_position());
  auto cursor = expand->MakeCursor(memgraph::utils::NewDeleteResource(), metric_handles);
  std::set<Gid> reached;
  while (cursor->Pull(frame, context)) reached.insert(frame[dst].ValueVirtualNode().Gid());

  // The overlay over the denied origin is not reachable; the edge to it is
  // dropped exactly as the scan would drop the node.
  EXPECT_EQ(reached.count(denied_gid), 0U);
}
#endif

// Names resolve through the view to the same ids the shared accessor uses.
TEST_F(VirtualGraphViewTest, NameMappingSharesTheRealNamespace) {
  auto graph = AssembleVirtualGraph({}, {}, DanglingEdgePolicy::kError, memgraph::utils::NewDeleteResource());
  VirtualGraphView view{&graph, &dba_};

  const auto label = view.NameToLabel("Person");
  const auto prop = view.NameToProperty("name");
  const auto edge_type = view.NameToEdgeType("KNOWS");

  EXPECT_EQ(view.LabelToName(label), "Person");
  EXPECT_EQ(view.PropertyToName(prop), "name");
  EXPECT_EQ(view.EdgeTypeToName(edge_type), "KNOWS");

  EXPECT_EQ(label, dba_.NameToLabel("Person"));
  EXPECT_EQ(prop, dba_.NameToProperty("name"));
  EXPECT_EQ(edge_type, dba_.NameToEdgeType("KNOWS"));
}

// --- Edge expansion through the GraphView seam (Stage A) -------------------
//
// The base GraphView OutEdges/InEdges are exercised on each concrete view: the
// identity view forwards to the real accessor (keeping storage-level type and
// destination pushdown), the projection view reads its own in/out index, and the
// subgraph view forwards to the real accessor then drops non-member edges.

// Collects a projection expansion into (from, to) gid pairs.
std::vector<std::pair<Gid, Gid>> VirtualPairs(EdgeRange range) {
  std::vector<std::pair<Gid, Gid>> pairs;
  for (auto edge : range) {
    const auto &ve = std::get<VirtualEdge>(edge);
    pairs.emplace_back(ve.FromGid(), ve.ToGid());
  }
  return pairs;
}

// Collects a real expansion into the set of reached (To/From) vertex gids.
std::set<Gid> RealEndpoints(EdgeRange range, bool out) {
  std::set<Gid> gids;
  for (auto edge : range) {
    const auto &ea = std::get<EdgeAccessor>(edge);
    gids.insert(out ? ea.To().Gid() : ea.From().Gid());
  }
  return gids;
}

// A directed projection edge appears among its source's out-edges and its
// target's in-edges only, expanded through the seam rather than the concrete view.
TEST_F(VirtualGraphViewTest, SeamExpandsProjectionEdgeDirectionally) {
  std::vector<VirtualNode> nodes;
  nodes.push_back(HandledNode(1, {"A"}));
  nodes.push_back(HandledNode(2, {"B"}));
  const auto src = nodes[0].Gid();
  const auto dst = nodes[1].Gid();
  std::vector<VirtualEdge> edges;
  edges.emplace_back(int64_t{1}, int64_t{2}, "KNOWS");
  auto graph = AssembleVirtualGraph(nodes, edges, DanglingEdgePolicy::kError, memgraph::utils::NewDeleteResource());
  VirtualGraphView view{&graph, &dba_};

  const auto outs = VirtualPairs(view.OutEdges(ScanVertex{nodes[0]}, View::NEW, {}, std::nullopt, nullptr));
  ASSERT_EQ(outs.size(), 1U);
  EXPECT_EQ(outs[0], std::make_pair(src, dst));

  const auto ins = VirtualPairs(view.InEdges(ScanVertex{nodes[1]}, View::NEW, {}, std::nullopt, nullptr));
  ASSERT_EQ(ins.size(), 1U);
  EXPECT_EQ(ins[0], std::make_pair(src, dst));

  // Wrong direction against each endpoint is empty.
  EXPECT_TRUE(VirtualPairs(view.InEdges(ScanVertex{nodes[0]}, View::NEW, {}, std::nullopt, nullptr)).empty());
  EXPECT_TRUE(VirtualPairs(view.OutEdges(ScanVertex{nodes[1]}, View::NEW, {}, std::nullopt, nullptr)).empty());
}

// An edge-type filter keeps only edges of a requested type.
TEST_F(VirtualGraphViewTest, SeamFiltersProjectionEdgesByType) {
  std::vector<VirtualNode> nodes;
  nodes.push_back(HandledNode(1));
  nodes.push_back(HandledNode(2));
  nodes.push_back(HandledNode(3));
  std::vector<VirtualEdge> edges;
  edges.emplace_back(int64_t{1}, int64_t{2}, "KNOWS");
  edges.emplace_back(int64_t{1}, int64_t{3}, "LIKES");
  auto graph = AssembleVirtualGraph(nodes, edges, DanglingEdgePolicy::kError, memgraph::utils::NewDeleteResource());
  VirtualGraphView view{&graph, &dba_};

  const std::vector<storage::EdgeTypeId> only_knows{dba_.NameToEdgeType("KNOWS")};
  const auto outs = VirtualPairs(view.OutEdges(ScanVertex{nodes[0]}, View::NEW, only_knows, std::nullopt, nullptr));
  ASSERT_EQ(outs.size(), 1U);
  EXPECT_EQ(outs[0].second, nodes[1].Gid());
}

// An already-bound destination keeps only the edge that reaches it.
TEST_F(VirtualGraphViewTest, SeamFiltersProjectionEdgesByExistingDest) {
  std::vector<VirtualNode> nodes;
  nodes.push_back(HandledNode(1));
  nodes.push_back(HandledNode(2));
  nodes.push_back(HandledNode(3));
  std::vector<VirtualEdge> edges;
  edges.emplace_back(int64_t{1}, int64_t{2}, "R");
  edges.emplace_back(int64_t{1}, int64_t{3}, "R");
  auto graph = AssembleVirtualGraph(nodes, edges, DanglingEdgePolicy::kError, memgraph::utils::NewDeleteResource());
  VirtualGraphView view{&graph, &dba_};

  const std::optional<ScanVertex> dest{ScanVertex{nodes[1]}};
  const auto outs = VirtualPairs(view.OutEdges(ScanVertex{nodes[0]}, View::NEW, {}, dest, nullptr));
  ASSERT_EQ(outs.size(), 1U);
  EXPECT_EQ(outs[0].second, nodes[1].Gid());
}

// The identity view expands a real vertex's edges through the seam, pushing the
// type and destination filters down to the real accessor.
TEST_F(GraphViewTest, SeamExpandsRealEdgesWithPushdown) {
  auto acc = storage_->Access(storage::StorageAccessType::WRITE);
  DbAccessor dba{acc.get()};
  auto a = dba.InsertVertex();
  auto b = dba.InsertVertex();
  auto c = dba.InsertVertex();
  const auto knows = dba.NameToEdgeType("KNOWS");
  const auto likes = dba.NameToEdgeType("LIKES");
  ASSERT_TRUE(dba.InsertEdge(&a, &b, knows).has_value());
  ASSERT_TRUE(dba.InsertEdge(&a, &c, likes).has_value());
  dba.AdvanceCommand();

  DbAccessorGraphView view{&dba};
  const ScanVertex from{a};

  EXPECT_EQ(RealEndpoints(view.OutEdges(from, View::NEW, {}, std::nullopt, nullptr), /*out=*/true),
            (std::set<Gid>{b.Gid(), c.Gid()}));

  const std::vector<storage::EdgeTypeId> only_knows{knows};
  EXPECT_EQ(RealEndpoints(view.OutEdges(from, View::NEW, only_knows, std::nullopt, nullptr), true),
            (std::set<Gid>{b.Gid()}));

  const std::optional<ScanVertex> dest{ScanVertex{c}};
  EXPECT_EQ(RealEndpoints(view.OutEdges(from, View::NEW, {}, dest, nullptr), true), (std::set<Gid>{c.Gid()}));

  // In-edges of b are a's edge, seen from the other end.
  EXPECT_EQ(RealEndpoints(view.InEdges(ScanVertex{b}, View::NEW, {}, std::nullopt, nullptr), /*out=*/false),
            (std::set<Gid>{a.Gid()}));
}

// The subgraph view forwards to the real accessor and then drops edges that are
// not members of the subgraph, so expansion stays within it.
TEST_F(GraphViewTest, SeamDropsNonMemberSubgraphEdges) {
  auto acc = storage_->Access(storage::StorageAccessType::WRITE);
  DbAccessor dba{acc.get()};
  auto a = dba.InsertVertex();
  auto b = dba.InsertVertex();
  auto c = dba.InsertVertex();
  const auto t = dba.NameToEdgeType("R");
  auto member = dba.InsertEdge(&a, &b, t);
  auto non_member = dba.InsertEdge(&a, &c, t);
  ASSERT_TRUE(member.has_value());
  ASSERT_TRUE(non_member.has_value());
  dba.AdvanceCommand();

  Graph graph{memgraph::utils::NewDeleteResource()};
  graph.InsertVertex(a);
  graph.InsertVertex(b);
  graph.InsertEdge(*member);  // a->b is a member; a->c is not
  SubgraphGraphView view{&graph, &dba};

  // Only the member edge survives; the non-member edge to c is dropped.
  EXPECT_EQ(RealEndpoints(view.OutEdges(ScanVertex{a}, View::NEW, {}, std::nullopt, nullptr), /*out=*/true),
            (std::set<Gid>{b.Gid()}));
}

// AddPathToProjection collapses a real path into an overlay projection directly -
// no Aggregate operator or plan - so the derive()/project() assembly is unit-testable
// in isolation now that it lives in the virtual_graph module.
TEST_F(GraphViewTest, AddPathToProjectionBuildsOverlayGraph) {
  auto acc = storage_->Access(storage::StorageAccessType::WRITE);
  DbAccessor dba{acc.get()};
  auto a = dba.InsertVertex();
  auto b = dba.InsertVertex();
  auto edge = dba.InsertEdge(&a, &b, dba.NameToEdgeType("R"));
  ASSERT_TRUE(edge.has_value());
  dba.AdvanceCommand();

  auto *mem = memgraph::utils::NewDeleteResource();
  TypedValue::TMap options(mem);
  options.emplace(TypedValue::TString{"virtualEdgeType", mem}, TypedValue{"E", mem});
  const TypedValue options_value{std::move(options), mem};
  const TypedValue path_value{Path{a, *edge, b}, mem};

  VirtualGraph graph{mem};
  DerivedNodeDedup dedup{mem};
  AddPathToProjection(path_value, options_value, graph, dedup, VirtualNode::kNoProjectionRef, &dba);

  // The path collapses to two overlay nodes (over the real endpoints) and one 'E'
  // synthetic edge; each overlay reads through to its real origin.
  EXPECT_EQ(graph.nodes().size(), 2U);
  ASSERT_EQ(graph.edges().size(), 1U);
  EXPECT_EQ(std::string_view{graph.edges().begin()->EdgeTypeName()}, "E");
  for (const auto &[gid, node] : graph.nodes()) EXPECT_TRUE(node->HasOrigin());
}

}  // namespace memgraph::query::test
