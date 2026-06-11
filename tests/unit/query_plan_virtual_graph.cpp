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

// Phase 1 of "Cypher over a VirtualGraph": exercises ScanAll and Expand in their virtual-graph mode by constructing
// the operators directly (the planner/USE-clause wiring lands in later phases). A VirtualGraph value is pre-seeded
// on the frame at a "graph symbol"; ScanAll iterates its nodes and Expand follows its adjacency.

#include <algorithm>
#include <string>
#include <vector>

#include "gmock/gmock.h"
#include "gtest/gtest.h"

#include "query/context.hpp"
#include "query/db_accessor.hpp"
#include "query/frontend/ast/ast.hpp"
#include "query/interpret/frame.hpp"
#include "query/plan/operator.hpp"
#include "query/typed_value.hpp"
#include "query/virtual_edge.hpp"
#include "query/virtual_graph.hpp"
#include "query/virtual_node.hpp"
#include "storage/v2/id_types.hpp"
#include "storage/v2/inmemory/storage.hpp"
#include "utils/memory.hpp"

#include "query_plan_common.hpp"

using namespace memgraph::query;
using namespace memgraph::query::plan;
using memgraph::storage::Gid;
using testing::UnorderedElementsAre;

namespace {

// Builds a small fixed VirtualGraph backed by `mem`:
//   (1:Person)-[:KNOWS]->(2:Person), (1)-[:LIKES]->(3:Person), (2)-[:KNOWS]->(3)
VirtualGraph MakeSampleGraph(memgraph::utils::MemoryResource *mem) {
  VirtualGraph vg(mem);

  const auto add_node = [&](uint64_t gid, const char *label) {
    VirtualNode::label_list labels(mem);
    labels.emplace_back(label);
    vg.InsertNode(VirtualNode{Gid::FromUint(gid), std::move(labels), VirtualNode::property_map{mem}, mem});
  };
  add_node(1, "Person");
  add_node(2, "Person");
  add_node(3, "Person");

  const auto add_edge = [&](uint64_t from, uint64_t to, const char *type) {
    vg.InsertEdgeIfNew(VirtualEdge{vg.FindNode(Gid::FromUint(from)),
                                   vg.FindNode(Gid::FromUint(to)),
                                   memgraph::utils::pmr::string{type, mem},
                                   mem});
  };
  add_edge(1, 2, "KNOWS");
  add_edge(1, 3, "LIKES");
  add_edge(2, 3, "KNOWS");

  return vg;
}

class VirtualGraphScanTest : public ::testing::Test {
 protected:
  std::unique_ptr<memgraph::storage::Storage> db_{std::make_unique<memgraph::storage::InMemoryStorage>()};
  std::unique_ptr<memgraph::storage::Storage::Accessor> storage_dba_{db_->Access(memgraph::storage::WRITE)};
  DbAccessor dba_{storage_dba_.get()};
  AstStorage ast_storage_;
  SymbolTable symbol_table_;
  memgraph::utils::MemoryResource *mem_{memgraph::utils::NewDeleteResource()};

  // Builds a frame sized for the symbol table with the sample VirtualGraph bound at `graph_sym`.
  Frame MakeSeededFrame(const Symbol &graph_sym) {
    Frame frame(symbol_table_.max_position());
    auto writer = frame.GetFrameWriter(nullptr, mem_);
    writer.Write(graph_sym, TypedValue(MakeSampleGraph(mem_), mem_));
    return frame;
  }
};

TEST_F(VirtualGraphScanTest, ScanAllIteratesVirtualNodes) {
  auto graph_sym = symbol_table_.CreateSymbol("g", true);
  auto node_sym = symbol_table_.CreateSymbol("n", true);

  auto scan = std::make_shared<ScanAll>(nullptr, node_sym, memgraph::storage::View::NEW);
  scan->graph_symbol_ = graph_sym;

  auto context = MakeContext(ast_storage_, symbol_table_, &dba_);
  auto frame = MakeSeededFrame(graph_sym);
  auto cursor = scan->MakeCursor(mem_, TestMetricHandles());

  std::vector<uint64_t> gids;
  while (cursor->Pull(frame, context)) {
    const auto &value = frame[node_sym];
    ASSERT_EQ(value.type(), TypedValue::Type::VirtualNode);
    gids.push_back(value.ValueVirtualNode().Gid().AsUint());
  }

  EXPECT_THAT(gids, UnorderedElementsAre(1U, 2U, 3U));
}

TEST_F(VirtualGraphScanTest, ExpandFollowsVirtualAdjacency) {
  auto graph_sym = symbol_table_.CreateSymbol("g", true);
  auto node_sym = symbol_table_.CreateSymbol("n", true);
  auto edge_sym = symbol_table_.CreateSymbol("e", true);
  auto dest_sym = symbol_table_.CreateSymbol("m", true);

  auto scan = std::make_shared<ScanAll>(nullptr, node_sym, memgraph::storage::View::NEW);
  scan->graph_symbol_ = graph_sym;
  auto expand = std::make_shared<Expand>(scan,
                                         node_sym,
                                         dest_sym,
                                         edge_sym,
                                         EdgeAtom::Direction::OUT,
                                         std::vector<memgraph::storage::EdgeTypeId>{},
                                         false,
                                         memgraph::storage::View::NEW);
  expand->graph_symbol_ = graph_sym;

  auto context = MakeContext(ast_storage_, symbol_table_, &dba_);
  auto frame = MakeSeededFrame(graph_sym);
  auto cursor = expand->MakeCursor(mem_, TestMetricHandles());

  std::vector<std::string> triples;
  while (cursor->Pull(frame, context)) {
    ASSERT_EQ(frame[node_sym].type(), TypedValue::Type::VirtualNode);
    ASSERT_EQ(frame[edge_sym].type(), TypedValue::Type::VirtualEdge);
    ASSERT_EQ(frame[dest_sym].type(), TypedValue::Type::VirtualNode);
    const auto &edge = frame[edge_sym].ValueVirtualEdge();
    triples.push_back(std::to_string(frame[node_sym].ValueVirtualNode().Gid().AsUint()) + "-" +
                      std::string{edge.EdgeTypeName()} + "->" +
                      std::to_string(frame[dest_sym].ValueVirtualNode().Gid().AsUint()));
  }

  EXPECT_THAT(triples, UnorderedElementsAre("1-KNOWS->2", "1-LIKES->3", "2-KNOWS->3"));
}

}  // namespace
