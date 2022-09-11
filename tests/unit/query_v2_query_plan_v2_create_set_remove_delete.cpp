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

#include "query/v2/bindings/symbol_table.hpp"
#include "query/v2/plan/operator.hpp"
#include "query_v2_query_plan_common.hpp"
#include "storage/v3/property_value.hpp"
#include "storage/v3/storage.hpp"

namespace memgraph::query::v2::tests {

class QueryPlanCRUDTest : public testing::Test {
 protected:
  void SetUp() override {
    ASSERT_TRUE(db.CreateSchema(label, {storage::v3::SchemaProperty{property, common::SchemaType::INT}}));
  }

  storage::v3::Storage db;
  const storage::v3::LabelId label{db.NameToLabel("label")};
  const storage::v3::PropertyId property{db.NameToProperty("property")};
};

TEST_F(QueryPlanCRUDTest, CreateNodeWithAttributes) {
  auto dba = db.Access();

  AstStorage ast;
  SymbolTable symbol_table;

  plan::NodeCreationInfo node;
  node.symbol = symbol_table.CreateSymbol("n", true);
  node.labels.emplace_back(label);
  std::get<std::vector<std::pair<storage::v3::PropertyId, Expression *>>>(node.properties)
      .emplace_back(property, ast.Create<PrimitiveLiteral>(42));

  plan::CreateNode create_node(nullptr, node);
  DbAccessor execution_dba(&dba);
  auto context = MakeContext(ast, symbol_table, &execution_dba);
  Frame frame(context.symbol_table.max_position());
  auto cursor = create_node.MakeCursor(utils::NewDeleteResource());
  int count = 0;
  while (cursor->Pull(frame, context)) {
    ++count;
    const auto &node_value = frame[node.symbol];
    EXPECT_EQ(node_value.type(), TypedValue::Type::Vertex);
    const auto &v = node_value.ValueVertex();
    EXPECT_TRUE(*v.HasLabel(storage::v3::View::NEW, label));
    EXPECT_EQ(v.GetProperty(storage::v3::View::NEW, property)->ValueInt(), 42);
    EXPECT_EQ(CountIterable(*v.InEdges(storage::v3::View::NEW)), 0);
    EXPECT_EQ(CountIterable(*v.OutEdges(storage::v3::View::NEW)), 0);
    // Invokes LOG(FATAL) instead of erroring out.
    // EXPECT_TRUE(v.HasLabel(label, storage::v3::View::OLD).IsError());
  }
  EXPECT_EQ(count, 1);
}

TEST_F(QueryPlanCRUDTest, ScanAllEmpty) {
  AstStorage ast;
  SymbolTable symbol_table;
  auto dba = db.Access();
  DbAccessor execution_dba(&dba);
  auto node_symbol = symbol_table.CreateSymbol("n", true);
  {
    plan::ScanAll scan_all(nullptr, node_symbol, storage::v3::View::OLD);
    auto context = MakeContext(ast, symbol_table, &execution_dba);
    Frame frame(context.symbol_table.max_position());
    auto cursor = scan_all.MakeCursor(utils::NewDeleteResource());
    int count = 0;
    while (cursor->Pull(frame, context)) ++count;
    EXPECT_EQ(count, 0);
  }
  {
    plan::ScanAll scan_all(nullptr, node_symbol, storage::v3::View::NEW);
    auto context = MakeContext(ast, symbol_table, &execution_dba);
    Frame frame(context.symbol_table.max_position());
    auto cursor = scan_all.MakeCursor(utils::NewDeleteResource());
    int count = 0;
    while (cursor->Pull(frame, context)) ++count;
    EXPECT_EQ(count, 0);
  }
}

TEST_F(QueryPlanCRUDTest, ScanAll) {
  {
    auto dba = db.Access();
    for (int i = 0; i < 42; ++i) {
      auto v = *dba.CreateVertexAndValidate(label, {}, {{property, storage::v3::PropertyValue(i)}});
      ASSERT_TRUE(v.SetProperty(property, storage::v3::PropertyValue(i)).HasValue());
    }
    EXPECT_FALSE(dba.Commit().HasError());
  }
  AstStorage ast;
  SymbolTable symbol_table;
  auto dba = db.Access();
  DbAccessor execution_dba(&dba);
  auto node_symbol = symbol_table.CreateSymbol("n", true);
  plan::ScanAll scan_all(nullptr, node_symbol);
  auto context = MakeContext(ast, symbol_table, &execution_dba);
  Frame frame(context.symbol_table.max_position());
  auto cursor = scan_all.MakeCursor(utils::NewDeleteResource());
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
      auto v = *dba.CreateVertexAndValidate(label, {}, {{property, storage::v3::PropertyValue(i)}});
      ASSERT_TRUE(v.SetProperty(property, storage::v3::PropertyValue(i)).HasValue());
    }
    // Add labeled vertices
    for (int i = 0; i < 42; ++i) {
      auto v = *dba.CreateVertexAndValidate(label, {}, {{property, storage::v3::PropertyValue(i)}});
      ASSERT_TRUE(v.SetProperty(property, storage::v3::PropertyValue(i)).HasValue());
      ASSERT_TRUE(v.AddLabel(label2).HasValue());
    }
    EXPECT_FALSE(dba.Commit().HasError());
  }
  auto dba = db.Access();
  AstStorage ast;
  SymbolTable symbol_table;
  auto node_symbol = symbol_table.CreateSymbol("n", true);
  DbAccessor execution_dba(&dba);
  plan::ScanAllByLabel scan_all(nullptr, node_symbol, label2);
  auto context = MakeContext(ast, symbol_table, &execution_dba);
  Frame frame(context.symbol_table.max_position());
  auto cursor = scan_all.MakeCursor(utils::NewDeleteResource());
  int count = 0;
  while (cursor->Pull(frame, context)) ++count;
  EXPECT_EQ(count, 42);
}
}  // namespace memgraph::query::v2::tests
