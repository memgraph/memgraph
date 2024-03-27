// Copyright 2024 Memgraph Ltd.
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

#include <memory>

#include "disk_test_utils.hpp"
#include "storage/v2/disk/storage.hpp"
#include "storage/v2/inmemory/storage.hpp"

#include "query/frontend/semantic/symbol_table.hpp"
#include "query/plan/operator.hpp"
#include "query/plan/read_write_type_checker.hpp"

#include "query_common.hpp"

using namespace memgraph::query;
using namespace memgraph::query::plan;
using RWType = ReadWriteTypeChecker::RWType;

template <typename StorageType>
class ReadWriteTypeCheckTest : public ::testing::Test {
 protected:
  const std::string testSuite = "query_plan_read_write_typecheck";
  AstStorage storage;
  SymbolTable symbol_table;

  memgraph::storage::Config config = disk_test_utils::GenerateOnDiskConfig(testSuite);
  std::unique_ptr<memgraph::storage::Storage> db{new StorageType(config)};
  std::unique_ptr<memgraph::storage::Storage::Accessor> dba_storage{
      db->Access(memgraph::replication_coordination_glue::ReplicationRole::MAIN)};
  memgraph::query::DbAccessor dba{dba_storage.get()};

  void TearDown() override {
    if (std::is_same<StorageType, memgraph::storage::DiskStorage>::value) {
      disk_test_utils::RemoveRocksDbDirs(testSuite);
    }
  }

  const Symbol &GetSymbol(std::string name) { return symbol_table.CreateSymbol(name, true); }

  void CheckPlanType(LogicalOperator *root, const RWType expected) {
    auto rw_type_checker = ReadWriteTypeChecker();
    rw_type_checker.InferRWType(*root);
    EXPECT_EQ(rw_type_checker.type, expected);
  }
};

using StorageTypes = ::testing::Types<memgraph::storage::InMemoryStorage, memgraph::storage::DiskStorage>;
TYPED_TEST_CASE(ReadWriteTypeCheckTest, StorageTypes);

TYPED_TEST(ReadWriteTypeCheckTest, NONEOps) {
  std::shared_ptr<LogicalOperator> once = std::make_shared<Once>();
  std::shared_ptr<LogicalOperator> produce =
      std::make_shared<Produce>(once, std::vector<NamedExpression *>{NEXPR("n", IDENT("n"))});
  this->CheckPlanType(produce.get(), RWType::NONE);
}

TYPED_TEST(ReadWriteTypeCheckTest, CreateNode) {
  std::shared_ptr<LogicalOperator> once = std::make_shared<Once>();
  std::shared_ptr<LogicalOperator> create_node = std::make_shared<CreateNode>(once, NodeCreationInfo());

  this->CheckPlanType(create_node.get(), RWType::W);
}

TYPED_TEST(ReadWriteTypeCheckTest, Filter) {
  std::shared_ptr<LogicalOperator> scan_all = std::make_shared<ScanAll>(nullptr, this->GetSymbol("node1"));
  std::shared_ptr<LogicalOperator> filter =
      std::make_shared<Filter>(scan_all, std::vector<std::shared_ptr<LogicalOperator>>{},
                               EQ(PROPERTY_LOOKUP(this->dba, "node1", this->dba.NameToProperty("prop")), LITERAL(0)));

  this->CheckPlanType(filter.get(), RWType::R);
}

TYPED_TEST(ReadWriteTypeCheckTest, ScanAllBy) {
  std::shared_ptr<LogicalOperator> last_op = std::make_shared<ScanAllByLabelPropertyRange>(
      nullptr, this->GetSymbol("node"), this->dba.NameToLabel("Label"), this->dba.NameToProperty("prop"), "prop",
      memgraph::utils::MakeBoundInclusive<Expression *>(LITERAL(1)),
      memgraph::utils::MakeBoundExclusive<Expression *>(LITERAL(20)));
  last_op = std::make_shared<ScanAllByLabelPropertyValue>(
      last_op, this->GetSymbol("node"), this->dba.NameToLabel("Label"), this->dba.NameToProperty("prop"), "prop",
      ADD(LITERAL(21), LITERAL(21)));

  this->CheckPlanType(last_op.get(), RWType::R);
}

TYPED_TEST(ReadWriteTypeCheckTest, OrderByAndLimit) {
  // We build an operator tree that would result from e.g.
  // MATCH (node:label)
  // WHERE n.property = 5
  // RETURN n
  // ORDER BY n.property
  // LIMIT 10
  Symbol node_sym = this->GetSymbol("node");
  memgraph::storage::LabelId label = this->dba.NameToLabel("label");
  memgraph::storage::PropertyId prop = this->dba.NameToProperty("property");

  std::shared_ptr<LogicalOperator> last_op = std::make_shared<Once>();
  last_op = std::make_shared<ScanAllByLabel>(last_op, node_sym, label);
  last_op = std::make_shared<Filter>(last_op, std::vector<std::shared_ptr<LogicalOperator>>{},
                                     EQ(PROPERTY_LOOKUP(this->dba, "node", prop), LITERAL(5)));
  last_op = std::make_shared<Produce>(last_op, std::vector<NamedExpression *>{NEXPR("n", IDENT("n"))});
  last_op = std::make_shared<OrderBy>(last_op,
                                      std::vector<SortItem>{{Ordering::DESC, PROPERTY_LOOKUP(this->dba, "node", prop)}},
                                      std::vector<Symbol>{node_sym});
  last_op = std::make_shared<Limit>(last_op, LITERAL(10));

  this->CheckPlanType(last_op.get(), RWType::R);
}

TYPED_TEST(ReadWriteTypeCheckTest, Delete) {
  auto node_sym = this->GetSymbol("node1");
  std::shared_ptr<LogicalOperator> last_op = std::make_shared<ScanAll>(nullptr, node_sym);

  last_op = std::make_shared<Expand>(last_op, node_sym, this->GetSymbol("node2"), this->GetSymbol("edge"),
                                     EdgeAtom::Direction::BOTH, std::vector<memgraph::storage::EdgeTypeId>{}, false,
                                     memgraph::storage::View::OLD);
  last_op = std::make_shared<plan::Delete>(last_op, std::vector<Expression *>{IDENT("node2")}, true);

  this->CheckPlanType(last_op.get(), RWType::RW);
}

TYPED_TEST(ReadWriteTypeCheckTest, ExpandVariable) {
  auto node1_sym = this->GetSymbol("node1");

  std::shared_ptr<LogicalOperator> last_op = std::make_shared<ScanAll>(nullptr, node1_sym);

  last_op = std::make_shared<ExpandVariable>(
      last_op, node1_sym, this->GetSymbol("node2"), this->GetSymbol("edge"), EdgeAtom::Type::BREADTH_FIRST,
      EdgeAtom::Direction::OUT,
      std::vector<memgraph::storage::EdgeTypeId>{this->dba.NameToEdgeType("EdgeType1"),
                                                 this->dba.NameToEdgeType("EdgeType2")},
      false, LITERAL(2), LITERAL(5), false,
      ExpansionLambda{this->GetSymbol("inner_node"), this->GetSymbol("inner_edge"),
                      PROPERTY_LOOKUP(this->dba, "inner_node", this->dba.NameToProperty("unblocked"))},
      std::nullopt, std::nullopt);

  this->CheckPlanType(last_op.get(), RWType::R);
}

TYPED_TEST(ReadWriteTypeCheckTest, EdgeUniquenessFilter) {
  auto node1_sym = this->GetSymbol("node1");
  auto node2_sym = this->GetSymbol("node2");
  auto node3_sym = this->GetSymbol("node3");
  auto node4_sym = this->GetSymbol("node4");

  auto edge1_sym = this->GetSymbol("edge1");
  auto edge2_sym = this->GetSymbol("edge2");

  std::shared_ptr<LogicalOperator> last_op = std::make_shared<ScanAll>(nullptr, node1_sym);
  last_op = std::make_shared<Expand>(last_op, node1_sym, node2_sym, edge1_sym, EdgeAtom::Direction::IN,
                                     std::vector<memgraph::storage::EdgeTypeId>{}, false, memgraph::storage::View::OLD);
  last_op = std::make_shared<ScanAll>(last_op, node3_sym);
  last_op = std::make_shared<Expand>(last_op, node3_sym, node4_sym, edge2_sym, EdgeAtom::Direction::OUT,
                                     std::vector<memgraph::storage::EdgeTypeId>{}, false, memgraph::storage::View::OLD);
  last_op = std::make_shared<EdgeUniquenessFilter>(last_op, edge2_sym, std::vector<Symbol>{edge1_sym});

  this->CheckPlanType(last_op.get(), RWType::R);
}

TYPED_TEST(ReadWriteTypeCheckTest, SetRemovePropertiesLabels) {
  auto node_sym = this->GetSymbol("node");
  memgraph::storage::PropertyId prop = this->dba.NameToProperty("prop");

  std::shared_ptr<LogicalOperator> last_op = std::make_shared<ScanAll>(nullptr, this->GetSymbol("node"));
  last_op = std::make_shared<plan::SetProperty>(last_op, prop, PROPERTY_LOOKUP(this->dba, "node", prop),
                                                ADD(PROPERTY_LOOKUP(this->dba, "node", prop), LITERAL(1)));
  last_op = std::make_shared<plan::RemoveProperty>(
      last_op, this->dba.NameToProperty("prop"), PROPERTY_LOOKUP(this->dba, "node", this->dba.NameToProperty("prop")));
  last_op = std::make_shared<plan::SetProperties>(
      last_op, node_sym,
      MAP({{this->storage.GetPropertyIx("prop1"), LITERAL(1)},
           {this->storage.GetPropertyIx("prop2"), LITERAL("this is a property")}}),
      plan::SetProperties::Op::REPLACE);
  last_op = std::make_shared<plan::SetLabels>(
      last_op, node_sym,
      std::vector<StorageLabelType>{this->dba.NameToLabel("label1"), this->dba.NameToLabel("label2")});
  last_op = std::make_shared<plan::RemoveLabels>(
      last_op, node_sym,
      std::vector<StorageLabelType>{this->dba.NameToLabel("label1"), this->dba.NameToLabel("label2")});

  this->CheckPlanType(last_op.get(), RWType::RW);
}

TYPED_TEST(ReadWriteTypeCheckTest, Cartesian) {
  Symbol x = this->GetSymbol("x");
  std::shared_ptr<LogicalOperator> lhs =
      std::make_shared<plan::Unwind>(nullptr, LIST(LITERAL(1), LITERAL(2), LITERAL(3)), x);
  Symbol node = this->GetSymbol("node");
  std::shared_ptr<LogicalOperator> rhs = std::make_shared<ScanAll>(nullptr, node);
  std::shared_ptr<LogicalOperator> cartesian =
      std::make_shared<Cartesian>(lhs, std::vector<Symbol>{x}, rhs, std::vector<Symbol>{node});

  this->CheckPlanType(cartesian.get(), RWType::R);
}

TYPED_TEST(ReadWriteTypeCheckTest, Union) {
  Symbol x = this->GetSymbol("x");
  std::shared_ptr<LogicalOperator> lhs =
      std::make_shared<plan::Unwind>(nullptr, LIST(LITERAL(2), LITERAL(3), LITERAL(2)), x);
  Symbol node = this->GetSymbol("x");
  std::shared_ptr<LogicalOperator> rhs = std::make_shared<ScanAll>(nullptr, node);
  std::shared_ptr<LogicalOperator> union_op = std::make_shared<Union>(
      lhs, rhs, std::vector<Symbol>{this->GetSymbol("x")}, std::vector<Symbol>{x}, std::vector<Symbol>{node});

  this->CheckPlanType(union_op.get(), RWType::R);
}

TYPED_TEST(ReadWriteTypeCheckTest, CallReadProcedure) {
  plan::CallProcedure call_op;
  call_op.input_ = std::make_shared<Once>();
  call_op.procedure_name_ = "mg.reload";
  call_op.arguments_ = {LITERAL("example")};
  call_op.result_fields_ = {"name", "signature"};
  call_op.is_write_ = false;
  call_op.result_symbols_ = {this->GetSymbol("name_alias"), this->GetSymbol("signature_alias")};

  this->CheckPlanType(&call_op, RWType::R);
}

TYPED_TEST(ReadWriteTypeCheckTest, CallWriteProcedure) {
  plan::CallProcedure call_op;
  call_op.input_ = std::make_shared<Once>();
  call_op.procedure_name_ = "mg.reload";
  call_op.arguments_ = {LITERAL("example")};
  call_op.result_fields_ = {"name", "signature"};
  call_op.is_write_ = true;
  call_op.result_symbols_ = {this->GetSymbol("name_alias"), this->GetSymbol("signature_alias")};

  this->CheckPlanType(&call_op, RWType::RW);
}

TYPED_TEST(ReadWriteTypeCheckTest, CallReadProcedureBeforeUpdate) {
  std::shared_ptr<LogicalOperator> last_op = std::make_shared<Once>();
  last_op = std::make_shared<CreateNode>(last_op, NodeCreationInfo());

  std::string procedure_name{"mg.reload"};
  std::vector<Expression *> arguments{LITERAL("example")};
  std::vector<std::string> result_fields{"name", "signature"};
  std::vector<Symbol> result_symbols{this->GetSymbol("name_alias"), this->GetSymbol("signature_alias")};

  last_op = std::make_shared<plan::CallProcedure>(last_op, procedure_name, arguments, result_fields, result_symbols,
                                                  nullptr, 0, false, 1);

  this->CheckPlanType(last_op.get(), RWType::RW);
}

TYPED_TEST(ReadWriteTypeCheckTest, ConstructNamedPath) {
  auto node1_sym = this->GetSymbol("node1");
  auto edge1_sym = this->GetSymbol("edge1");
  auto node2_sym = this->GetSymbol("node2");
  auto edge2_sym = this->GetSymbol("edge2");
  auto node3_sym = this->GetSymbol("node3");

  std::shared_ptr<LogicalOperator> last_op = std::make_shared<ScanAll>(nullptr, node1_sym);
  last_op = std::make_shared<Expand>(last_op, node1_sym, node2_sym, edge1_sym, EdgeAtom::Direction::OUT,
                                     std::vector<memgraph::storage::EdgeTypeId>{}, false, memgraph::storage::View::OLD);
  last_op = std::make_shared<Expand>(last_op, node2_sym, node3_sym, edge2_sym, EdgeAtom::Direction::OUT,
                                     std::vector<memgraph::storage::EdgeTypeId>{}, false, memgraph::storage::View::OLD);
  last_op = std::make_shared<ConstructNamedPath>(
      last_op, this->GetSymbol("path"), std::vector<Symbol>{node1_sym, edge1_sym, node2_sym, edge2_sym, node3_sym});

  this->CheckPlanType(last_op.get(), RWType::R);
}

TYPED_TEST(ReadWriteTypeCheckTest, Foreach) {
  Symbol x = this->GetSymbol("x");
  std::shared_ptr<LogicalOperator> foreach = std::make_shared<plan::Foreach>(nullptr, nullptr, nullptr, x);
  this->CheckPlanType(foreach.get(), RWType::RW);
}

TYPED_TEST(ReadWriteTypeCheckTest, CheckUpdateType) {
  std::array<std::array<RWType, 3>, 16> scenarios = {{
      {RWType::NONE, RWType::NONE, RWType::NONE},
      {RWType::NONE, RWType::R, RWType::R},
      {RWType::NONE, RWType::W, RWType::W},
      {RWType::NONE, RWType::RW, RWType::RW},
      {RWType::R, RWType::NONE, RWType::R},
      {RWType::R, RWType::R, RWType::R},
      {RWType::R, RWType::W, RWType::RW},
      {RWType::R, RWType::RW, RWType::RW},
      {RWType::W, RWType::NONE, RWType::W},
      {RWType::W, RWType::R, RWType::RW},
      {RWType::W, RWType::W, RWType::W},
      {RWType::W, RWType::RW, RWType::RW},
      {RWType::RW, RWType::NONE, RWType::RW},
      {RWType::RW, RWType::R, RWType::RW},
      {RWType::RW, RWType::W, RWType::RW},
      {RWType::RW, RWType::RW, RWType::RW},
  }};

  auto rw_type_checker = ReadWriteTypeChecker();
  for (auto scenario : scenarios) {
    rw_type_checker.type = scenario[0];
    rw_type_checker.UpdateType(scenario[1]);
    EXPECT_EQ(scenario[2], rw_type_checker.type);
  }
}
