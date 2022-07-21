// Copyright 2022 Memgraph Ltd.
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

#include "query_plan_common.hpp"

#include "query/frontend/semantic/symbol_table.hpp"
#include "query/plan/operator.hpp"
#include "storage/v2/property_value.hpp"
#include "storage/v2/storage.hpp"

class QueryPlanCRUDTest : public testing::Test {
 protected:
  QueryPlanCRUDTest() {
    EXPECT_TRUE(
        db.CreateSchema(label, {memgraph::storage::SchemaProperty{property, memgraph::common::SchemaType::INT}}));
  }

  memgraph::storage::Storage db;
  memgraph::storage::LabelId label = db.NameToLabel("label");
  memgraph::storage::PropertyId property = db.NameToProperty("property");
};

TEST_F(QueryPlanCRUDTest, CreateNodeWithAttributes) {
  auto dba = db.Access();

  memgraph::query::AstStorage ast;
  memgraph::query::SymbolTable symbol_table;

  memgraph::query::plan::NodeCreationInfo node;
  node.symbol = symbol_table.CreateSymbol("n", true);
  node.labels.emplace_back(label);
  std::get<std::vector<std::pair<memgraph::storage::PropertyId, Expression *>>>(node.properties)
      .emplace_back(property, ast.Create<PrimitiveLiteral>(42));

  memgraph::query::plan::CreateNode create_node(nullptr, node);
  DbAccessor execution_dba(&dba);
  auto context = MakeContext(ast, symbol_table, &execution_dba);
  Frame frame(context.symbol_table.max_position());
  auto cursor = create_node.MakeCursor(memgraph::utils::NewDeleteResource());
  int count = 0;
  while (cursor->Pull(frame, context)) {
    ++count;
    const auto &node_value = frame[node.symbol];
    EXPECT_EQ(node_value.type(), TypedValue::Type::Vertex);
    const auto &v = node_value.ValueVertex();
    EXPECT_TRUE(*v.HasLabel(memgraph::storage::View::NEW, label));
    EXPECT_EQ(v.GetProperty(memgraph::storage::View::NEW, property)->ValueInt(), 42);
    EXPECT_EQ(CountIterable(*v.InEdges(memgraph::storage::View::NEW)), 0);
    EXPECT_EQ(CountIterable(*v.OutEdges(memgraph::storage::View::NEW)), 0);
    // Invokes LOG(FATAL) instead of erroring out.
    // EXPECT_TRUE(v.HasLabel(label, memgraph::storage::View::OLD).IsError());
  }
  EXPECT_EQ(count, 1);
}

TEST_F(QueryPlanCRUDTest, ScanAllEmpty) {
  memgraph::query::AstStorage ast;
  memgraph::query::SymbolTable symbol_table;
  auto dba = db.Access();
  DbAccessor execution_dba(&dba);
  auto node_symbol = symbol_table.CreateSymbol("n", true);
  {
    memgraph::query::plan::ScanAll scan_all(nullptr, node_symbol, memgraph::storage::View::OLD);
    auto context = MakeContext(ast, symbol_table, &execution_dba);
    Frame frame(context.symbol_table.max_position());
    auto cursor = scan_all.MakeCursor(memgraph::utils::NewDeleteResource());
    int count = 0;
    while (cursor->Pull(frame, context)) ++count;
    EXPECT_EQ(count, 0);
  }
  {
    memgraph::query::plan::ScanAll scan_all(nullptr, node_symbol, memgraph::storage::View::NEW);
    auto context = MakeContext(ast, symbol_table, &execution_dba);
    Frame frame(context.symbol_table.max_position());
    auto cursor = scan_all.MakeCursor(memgraph::utils::NewDeleteResource());
    int count = 0;
    while (cursor->Pull(frame, context)) ++count;
    EXPECT_EQ(count, 0);
  }
}

TEST_F(QueryPlanCRUDTest, ScanAll) {
  {
    auto dba = db.Access();
    for (int i = 0; i < 42; ++i) {
      auto v = dba.CreateVertex(label);
      ASSERT_TRUE(v.SetProperty(property, memgraph::storage::PropertyValue(i)).HasValue());
    }
    EXPECT_FALSE(dba.Commit().HasError());
  }
  memgraph::query::AstStorage ast;
  memgraph::query::SymbolTable symbol_table;
  auto dba = db.Access();
  DbAccessor execution_dba(&dba);
  auto node_symbol = symbol_table.CreateSymbol("n", true);
  memgraph::query::plan::ScanAll scan_all(nullptr, node_symbol);
  auto context = MakeContext(ast, symbol_table, &execution_dba);
  Frame frame(context.symbol_table.max_position());
  auto cursor = scan_all.MakeCursor(memgraph::utils::NewDeleteResource());
  int count = 0;
  while (cursor->Pull(frame, context)) ++count;
  EXPECT_EQ(count, 42);
}

TEST_F(QueryPlanCRUDTest, ScanAllByLabel) {
  auto label2 = db.NameToLabel("label2");
  ASSERT_TRUE(db.CreateIndex(label2));
  {
    auto dba = db.Access();
    // Add some unlabeled vertices
    for (int i = 0; i < 12; ++i) {
      auto v = dba.CreateVertex(label);
      ASSERT_TRUE(v.SetProperty(property, memgraph::storage::PropertyValue(i)).HasValue());
    }
    // Add labeled vertices
    for (int i = 0; i < 42; ++i) {
      auto v = dba.CreateVertex(label);
      ASSERT_TRUE(v.SetProperty(property, memgraph::storage::PropertyValue(i)).HasValue());
      ASSERT_TRUE(v.AddLabel(label2).HasValue());
    }
    EXPECT_FALSE(dba.Commit().HasError());
  }
  auto dba = db.Access();
  memgraph::query::AstStorage ast;
  memgraph::query::SymbolTable symbol_table;
  auto node_symbol = symbol_table.CreateSymbol("n", true);
  DbAccessor execution_dba(&dba);
  memgraph::query::plan::ScanAllByLabel scan_all(nullptr, node_symbol, label2);
  auto context = MakeContext(ast, symbol_table, &execution_dba);
  Frame frame(context.symbol_table.max_position());
  auto cursor = scan_all.MakeCursor(memgraph::utils::NewDeleteResource());
  int count = 0;
  while (cursor->Pull(frame, context)) ++count;
  EXPECT_EQ(count, 42);
}
