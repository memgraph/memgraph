// Copyright 2023 Memgraph Ltd.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt; by using this file, you agree to be bound by the terms of the Business Source
// License, and you may not use this file except in compliance with the Business Source License.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

#include "disk_test_utils.hpp"
#include "query_plan_common.hpp"

#include <gtest/gtest.h>

#include "query/frontend/semantic/symbol_table.hpp"
#include "query/plan/operator.hpp"
#include "storage/v2/disk/storage.hpp"
#include "storage/v2/inmemory/storage.hpp"

template <typename StorageType>
class QueryPlan : public testing::Test {
 public:
  const std::string testSuite = "query_plan_v2_create_set_remove_delete";
  memgraph::storage::Config config = disk_test_utils::GenerateOnDiskConfig(testSuite);
  std::unique_ptr<memgraph::storage::Storage> db = std::make_unique<StorageType>(config);

  void TearDown() override {
    if (std::is_same<StorageType, memgraph::storage::DiskStorage>::value) {
      disk_test_utils::RemoveRocksDbDirs(testSuite);
    }
  }
};

using StorageTypes = ::testing::Types<memgraph::storage::InMemoryStorage, memgraph::storage::DiskStorage>;
TYPED_TEST_CASE(QueryPlan, StorageTypes);

TYPED_TEST(QueryPlan, CreateNodeWithAttributes) {
  auto dba = this->db->Access();

  auto label = memgraph::storage::LabelId::FromInt(42);
  auto property = memgraph::storage::PropertyId::FromInt(1);

  memgraph::query::AstStorage ast;
  memgraph::query::SymbolTable symbol_table;

  memgraph::query::plan::NodeCreationInfo node;
  node.symbol = symbol_table.CreateSymbol("n", true);
  node.labels.emplace_back(label);
  std::get<std::vector<std::pair<memgraph::storage::PropertyId, Expression *>>>(node.properties)
      .emplace_back(property, ast.Create<PrimitiveLiteral>(42));

  memgraph::query::plan::CreateNode create_node(nullptr, node);
  DbAccessor execution_dba(dba.get());
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
    dba->PrefetchInEdges(v.impl_);
    dba->PrefetchOutEdges(v.impl_);
    EXPECT_EQ(CountIterable(v.InEdges(memgraph::storage::View::NEW)->edges), 0);
    EXPECT_EQ(CountIterable(v.OutEdges(memgraph::storage::View::NEW)->edges), 0);
    // Invokes LOG(FATAL) instead of erroring out.
    // EXPECT_TRUE(v.HasLabel(label, memgraph::storage::View::OLD).IsError());
  }
  EXPECT_EQ(count, 1);
}

TYPED_TEST(QueryPlan, ScanAllEmpty) {
  memgraph::query::AstStorage ast;
  memgraph::query::SymbolTable symbol_table;
  auto dba = this->db->Access();
  DbAccessor execution_dba(dba.get());
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

TYPED_TEST(QueryPlan, ScanAll) {
  {
    auto dba = this->db->Access();
    for (int i = 0; i < 42; ++i) dba->CreateVertex();
    EXPECT_FALSE(dba->Commit().HasError());
  }
  memgraph::query::AstStorage ast;
  memgraph::query::SymbolTable symbol_table;
  auto dba = this->db->Access();
  DbAccessor execution_dba(dba.get());
  auto node_symbol = symbol_table.CreateSymbol("n", true);
  memgraph::query::plan::ScanAll scan_all(nullptr, node_symbol);
  auto context = MakeContext(ast, symbol_table, &execution_dba);
  Frame frame(context.symbol_table.max_position());
  auto cursor = scan_all.MakeCursor(memgraph::utils::NewDeleteResource());
  int count = 0;
  while (cursor->Pull(frame, context)) ++count;
  EXPECT_EQ(count, 42);
}

TYPED_TEST(QueryPlan, ScanAllByLabel) {
  auto label = this->db->NameToLabel("label");
  {
    auto unique_acc = this->db->UniqueAccess();
    ASSERT_FALSE(unique_acc->CreateIndex(label).HasError());
    ASSERT_FALSE(unique_acc->Commit().HasError());
  }
  {
    auto dba = this->db->Access();
    // Add some unlabeled vertices
    for (int i = 0; i < 12; ++i) dba->CreateVertex();
    // Add labeled vertices
    for (int i = 0; i < 42; ++i) {
      auto v = dba->CreateVertex();
      ASSERT_TRUE(v.AddLabel(label).HasValue());
    }
    EXPECT_FALSE(dba->Commit().HasError());
  }
  auto dba = this->db->Access();
  memgraph::query::AstStorage ast;
  memgraph::query::SymbolTable symbol_table;
  auto node_symbol = symbol_table.CreateSymbol("n", true);
  DbAccessor execution_dba(dba.get());
  memgraph::query::plan::ScanAllByLabel scan_all(nullptr, node_symbol, label);
  auto context = MakeContext(ast, symbol_table, &execution_dba);
  Frame frame(context.symbol_table.max_position());
  auto cursor = scan_all.MakeCursor(memgraph::utils::NewDeleteResource());
  int count = 0;
  while (cursor->Pull(frame, context)) ++count;
  EXPECT_EQ(count, 42);
}
