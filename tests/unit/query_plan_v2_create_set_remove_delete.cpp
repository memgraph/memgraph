// Copyright 2021 Memgraph Ltd.
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
#include "storage/v2/storage.hpp"

TEST(QueryPlan, CreateNodeWithAttributes) {
  storage::Storage db;
  auto dba = db.Access();

  auto label = storage::LabelId::FromInt(42);
  auto property = storage::PropertyId::FromInt(1);

  query::AstStorage ast;
  query::SymbolTable symbol_table;

  query::plan::NodeCreationInfo node;
  node.symbol = symbol_table.CreateSymbol("n", true);
  node.labels.emplace_back(label);
  std::get<std::vector<std::pair<storage::PropertyId, Expression *>>>(node.properties)
      .emplace_back(property, ast.Create<PrimitiveLiteral>(42));

  query::plan::CreateNode create_node(nullptr, node);
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
    EXPECT_TRUE(*v.HasLabel(storage::View::NEW, label));
    EXPECT_EQ(v.GetProperty(storage::View::NEW, property)->ValueInt(), 42);
    EXPECT_EQ(CountIterable(*v.InEdges(storage::View::NEW)), 0);
    EXPECT_EQ(CountIterable(*v.OutEdges(storage::View::NEW)), 0);
    // Invokes LOG(FATAL) instead of erroring out.
    // EXPECT_TRUE(v.HasLabel(label, storage::View::OLD).IsError());
  }
  EXPECT_EQ(count, 1);
}

TEST(QueryPlan, ScanAllEmpty) {
  query::AstStorage ast;
  query::SymbolTable symbol_table;
  storage::Storage db;
  auto dba = db.Access();
  DbAccessor execution_dba(&dba);
  auto node_symbol = symbol_table.CreateSymbol("n", true);
  {
    query::plan::ScanAll scan_all(nullptr, node_symbol, storage::View::OLD);
    auto context = MakeContext(ast, symbol_table, &execution_dba);
    Frame frame(context.symbol_table.max_position());
    auto cursor = scan_all.MakeCursor(utils::NewDeleteResource());
    int count = 0;
    while (cursor->Pull(frame, context)) ++count;
    EXPECT_EQ(count, 0);
  }
  {
    query::plan::ScanAll scan_all(nullptr, node_symbol, storage::View::NEW);
    auto context = MakeContext(ast, symbol_table, &execution_dba);
    Frame frame(context.symbol_table.max_position());
    auto cursor = scan_all.MakeCursor(utils::NewDeleteResource());
    int count = 0;
    while (cursor->Pull(frame, context)) ++count;
    EXPECT_EQ(count, 0);
  }
}

TEST(QueryPlan, ScanAll) {
  storage::Storage db;
  {
    auto dba = db.Access();
    for (int i = 0; i < 42; ++i) dba.CreateVertex();
    EXPECT_FALSE(dba.Commit().HasError());
  }
  query::AstStorage ast;
  query::SymbolTable symbol_table;
  auto dba = db.Access();
  DbAccessor execution_dba(&dba);
  auto node_symbol = symbol_table.CreateSymbol("n", true);
  query::plan::ScanAll scan_all(nullptr, node_symbol);
  auto context = MakeContext(ast, symbol_table, &execution_dba);
  Frame frame(context.symbol_table.max_position());
  auto cursor = scan_all.MakeCursor(utils::NewDeleteResource());
  int count = 0;
  while (cursor->Pull(frame, context)) ++count;
  EXPECT_EQ(count, 42);
}

TEST(QueryPlan, ScanAllByLabel) {
  storage::Storage db;
  auto label = db.NameToLabel("label");
  ASSERT_TRUE(db.CreateIndex(label));
  {
    auto dba = db.Access();
    // Add some unlabeled vertices
    for (int i = 0; i < 12; ++i) dba.CreateVertex();
    // Add labeled vertices
    for (int i = 0; i < 42; ++i) {
      auto v = dba.CreateVertex();
      ASSERT_TRUE(v.AddLabel(label).HasValue());
    }
    EXPECT_FALSE(dba.Commit().HasError());
  }
  auto dba = db.Access();
  query::AstStorage ast;
  query::SymbolTable symbol_table;
  auto node_symbol = symbol_table.CreateSymbol("n", true);
  DbAccessor execution_dba(&dba);
  query::plan::ScanAllByLabel scan_all(nullptr, node_symbol, label);
  auto context = MakeContext(ast, symbol_table, &execution_dba);
  Frame frame(context.symbol_table.max_position());
  auto cursor = scan_all.MakeCursor(utils::NewDeleteResource());
  int count = 0;
  while (cursor->Pull(frame, context)) ++count;
  EXPECT_EQ(count, 42);
}
