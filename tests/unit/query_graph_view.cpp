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

#include "query/db_accessor.hpp"
#include "query/graph_view.hpp"
#include "storage/v2/inmemory/storage.hpp"
#include "storage/v2/storage.hpp"
#include "tests/test_commit_args_helper.hpp"

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

}  // namespace memgraph::query::test
