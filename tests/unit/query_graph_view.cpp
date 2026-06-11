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

#include <set>

#include "query/context.hpp"
#include "query/db_accessor.hpp"
#include "query/graph_view.hpp"
#include "query/interpret/frame.hpp"
#include "query/plan/operator.hpp"
#include "storage/v2/inmemory/storage.hpp"
#include "storage/v2/storage.hpp"
#include "tests/test_commit_args_helper.hpp"
#include "utils/memory.hpp"

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
  for (auto vertex : view.Vertices(View::NEW)) scanned.insert(vertex.Gid());

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
  ExecutionContext context{.db_accessor = &empty_dba, .graph_view = &bound_view};
  context.symbol_table = symbol_table;
  context.evaluation_context.memory = memgraph::utils::NewDeleteResource();
  context.metric_handles = &metric_handles;

  Frame frame(symbol_table.max_position());
  auto cursor = scan_all->MakeCursor(memgraph::utils::NewDeleteResource(), metric_handles);
  std::set<Gid> scanned;
  while (cursor->Pull(frame, context)) scanned.insert(frame[symbol].ValueVertex().Gid());

  EXPECT_EQ(scanned, bound_gids);
}

}  // namespace memgraph::query::test
