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

#include <gtest/gtest.h>

#include "disk_test_utils.hpp"
#include "query/frontend/semantic/symbol_table.hpp"
#include "query/plan/operator.hpp"
#include "query/plan/pretty_print.hpp"

#include "query_common.hpp"
#include "storage/v2/disk/storage.hpp"
#include "storage/v2/inmemory/storage.hpp"

using namespace memgraph::query;
using namespace memgraph::query::plan;

// The JSON formatted plan is consumed (or will be) by Memgraph Lab, and
// therefore should not be changed before synchronizing with whoever is
// maintaining Memgraph Lab. Hopefully, one day integration tests will exist and
// there will be no need to be super careful.

template <typename StorageType>
class OperatorToStringTest : public ::testing::Test {
 protected:
  const std::string testSuite = "plan_operator_to_string";

  OperatorToStringTest()
      : config(disk_test_utils::GenerateOnDiskConfig(testSuite)),
        db(new StorageType(config)),
        dba_storage(db->Access()),
        dba(dba_storage.get()) {}

  ~OperatorToStringTest() {
    if (std::is_same<StorageType, memgraph::storage::DiskStorage>::value) {
      disk_test_utils::RemoveRocksDbDirs(testSuite);
    }
  }

  AstStorage storage;
  SymbolTable symbol_table;

  memgraph::storage::Config config;
  std::unique_ptr<memgraph::storage::Storage> db;
  std::unique_ptr<memgraph::storage::Storage::Accessor> dba_storage;
  memgraph::query::DbAccessor dba;

  Symbol GetSymbol(std::string name) { return symbol_table.CreateSymbol(name, true); }
};

using StorageTypes = ::testing::Types<memgraph::storage::InMemoryStorage, memgraph::storage::DiskStorage>;
TYPED_TEST_CASE(OperatorToStringTest, StorageTypes);

TYPED_TEST(OperatorToStringTest, Once) {
  std::shared_ptr<LogicalOperator> last_op;
  last_op = std::make_shared<Once>();

  std::string expected_string{"Once"};
  EXPECT_EQ(last_op->ToString(), expected_string);
}

TYPED_TEST(OperatorToStringTest, CreateNode) {
  std::shared_ptr<LogicalOperator> last_op;
  last_op = std::make_shared<CreateNode>(
      nullptr, NodeCreationInfo{this->GetSymbol("node"),
                                {this->dba.NameToLabel("Label1"), this->dba.NameToLabel("Label2")},
                                {{this->dba.NameToProperty("prop1"), LITERAL(5)},
                                 {this->dba.NameToProperty("prop2"), LITERAL("some cool stuff")}}});

  std::string expected_string{"CreateNode"};
  EXPECT_EQ(last_op->ToString(), expected_string);
}

TYPED_TEST(OperatorToStringTest, CreateExpand) {
  Symbol node1_sym = this->GetSymbol("node1");
  std::shared_ptr<LogicalOperator> last_op = std::make_shared<ScanAll>(nullptr, this->GetSymbol("node1"));
  last_op = std::make_shared<CreateExpand>(
      NodeCreationInfo{this->GetSymbol("node2"),
                       {this->dba.NameToLabel("Label1"), this->dba.NameToLabel("Label2")},
                       {{this->dba.NameToProperty("prop1"), LITERAL(5)},
                        {this->dba.NameToProperty("prop2"), LITERAL("some cool stuff")}}},
      EdgeCreationInfo{this->GetSymbol("edge"),
                       {{this->dba.NameToProperty("weight"), LITERAL(5.32)}},
                       this->dba.NameToEdgeType("edge_type"),
                       EdgeAtom::Direction::OUT},
      last_op, node1_sym, false);
  last_op->dba_ = &this->dba;

  std::string expected_string{"CreateExpand (node1)-[edge:edge_type]->(node2)"};
  EXPECT_EQ(last_op->ToString(), expected_string);
}

TYPED_TEST(OperatorToStringTest, ScanAll) {
  std::shared_ptr<LogicalOperator> last_op;
  last_op = std::make_shared<ScanAll>(nullptr, this->GetSymbol("node"));

  std::string expected_string{"ScanAll (node)"};
  EXPECT_EQ(last_op->ToString(), expected_string);
}

TYPED_TEST(OperatorToStringTest, ScanAllByLabel) {
  std::shared_ptr<LogicalOperator> last_op;
  last_op = std::make_shared<ScanAllByLabel>(nullptr, this->GetSymbol("node"), this->dba.NameToLabel("Label"));
  last_op->dba_ = &this->dba;

  std::string expected_string{"ScanAllByLabel (node :Label)"};
  EXPECT_EQ(last_op->ToString(), expected_string);
}

TYPED_TEST(OperatorToStringTest, ScanAllByLabelPropertyRange) {
  std::shared_ptr<LogicalOperator> last_op;
  last_op = std::make_shared<ScanAllByLabelPropertyRange>(
      nullptr, this->GetSymbol("node"), this->dba.NameToLabel("Label"), this->dba.NameToProperty("prop"), "prop",
      memgraph::utils::MakeBoundInclusive<Expression *>(LITERAL(1)),
      memgraph::utils::MakeBoundExclusive<Expression *>(LITERAL(20)));
  last_op->dba_ = &this->dba;

  std::string expected_string{"ScanAllByLabelPropertyRange (node :Label {prop})"};
  EXPECT_EQ(last_op->ToString(), expected_string);
}

TYPED_TEST(OperatorToStringTest, ScanAllByLabelPropertyValue) {
  std::shared_ptr<LogicalOperator> last_op;
  last_op = std::make_shared<ScanAllByLabelPropertyValue>(
      nullptr, this->GetSymbol("node"), this->dba.NameToLabel("Label"), this->dba.NameToProperty("prop"), "prop",
      ADD(LITERAL(21), LITERAL(21)));
  last_op->dba_ = &this->dba;

  std::string expected_string{"ScanAllByLabelPropertyValue (node :Label {prop})"};
  EXPECT_EQ(last_op->ToString(), expected_string);
}

TYPED_TEST(OperatorToStringTest, ScanAllByLabelProperty) {
  std::shared_ptr<LogicalOperator> last_op;
  last_op = std::make_shared<ScanAllByLabelProperty>(nullptr, this->GetSymbol("node"), this->dba.NameToLabel("Label"),
                                                     this->dba.NameToProperty("prop"), "prop");
  last_op->dba_ = &this->dba;

  std::string expected_string{"ScanAllByLabelProperty (node :Label {prop})"};
  EXPECT_EQ(last_op->ToString(), expected_string);
}

TYPED_TEST(OperatorToStringTest, ScanAllById) {
  std::shared_ptr<LogicalOperator> last_op;
  last_op = std::make_shared<ScanAllById>(nullptr, this->GetSymbol("node"), ADD(LITERAL(21), LITERAL(21)));
  last_op->dba_ = &this->dba;

  std::string expected_string{"ScanAllById (node)"};
  EXPECT_EQ(last_op->ToString(), expected_string);
}

TYPED_TEST(OperatorToStringTest, Expand) {
  auto node1_sym = this->GetSymbol("node1");
  std::shared_ptr<LogicalOperator> last_op = std::make_shared<ScanAll>(nullptr, node1_sym);
  last_op = std::make_shared<Expand>(last_op, node1_sym, this->GetSymbol("node2"), this->GetSymbol("edge"),
                                     EdgeAtom::Direction::BOTH,
                                     std::vector<memgraph::storage::EdgeTypeId>{this->dba.NameToEdgeType("EdgeType1"),
                                                                                this->dba.NameToEdgeType("EdgeType2")},
                                     false, memgraph::storage::View::OLD);
  last_op->dba_ = &this->dba;

  std::string expected_string{"Expand (node1)-[edge:EdgeType1|:EdgeType2]-(node2)"};
  EXPECT_EQ(last_op->ToString(), expected_string);
}

TYPED_TEST(OperatorToStringTest, ExpandVariable) {
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
  last_op->dba_ = &this->dba;

  std::string expected_string{"BFSExpand (node1)-[edge:EdgeType1|:EdgeType2]->(node2)"};
  EXPECT_EQ(last_op->ToString(), expected_string);
}

TYPED_TEST(OperatorToStringTest, ConstructNamedPath) {
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

  std::string expected_string{"ConstructNamedPath"};
  EXPECT_EQ(last_op->ToString(), expected_string);
}

TYPED_TEST(OperatorToStringTest, Filter) {
  std::shared_ptr<LogicalOperator> last_op = std::make_shared<ScanAll>(nullptr, this->GetSymbol("node1"));
  last_op =
      std::make_shared<Filter>(last_op, std::vector<std::shared_ptr<LogicalOperator>>{},
                               EQ(PROPERTY_LOOKUP(this->dba, "node1", this->dba.NameToProperty("prop")), LITERAL(5)));

  std::string expected_string{"Filter"};
  EXPECT_EQ(last_op->ToString(), expected_string);
}

TYPED_TEST(OperatorToStringTest, Produce) {
  std::shared_ptr<LogicalOperator> last_op = std::make_shared<Produce>(
      nullptr, std::vector<NamedExpression *>{NEXPR("pet", LITERAL(5)), NEXPR("string", LITERAL("string"))});

  std::string expected_string{"Produce {pet, string}"};
  EXPECT_EQ(last_op->ToString(), expected_string);
}

TYPED_TEST(OperatorToStringTest, Delete) {
  auto node_sym = this->GetSymbol("node1");
  std::shared_ptr<LogicalOperator> last_op = std::make_shared<ScanAll>(nullptr, node_sym);
  last_op = std::make_shared<Expand>(last_op, node_sym, this->GetSymbol("node2"), this->GetSymbol("edge"),
                                     EdgeAtom::Direction::BOTH, std::vector<memgraph::storage::EdgeTypeId>{}, false,
                                     memgraph::storage::View::OLD);
  last_op = std::make_shared<plan::Delete>(last_op, std::vector<Expression *>{IDENT("node2")}, true);

  std::string expected_string{"Delete"};
  EXPECT_EQ(last_op->ToString(), expected_string);
}

TYPED_TEST(OperatorToStringTest, SetProperty) {
  memgraph::storage::PropertyId prop = this->dba.NameToProperty("prop");

  std::shared_ptr<LogicalOperator> last_op = std::make_shared<ScanAll>(nullptr, this->GetSymbol("node"));
  last_op = std::make_shared<plan::SetProperty>(last_op, prop, PROPERTY_LOOKUP(this->dba, "node", prop),
                                                ADD(PROPERTY_LOOKUP(this->dba, "node", prop), LITERAL(1)));

  std::string expected_string{"SetProperty"};
  EXPECT_EQ(last_op->ToString(), expected_string);
}

TYPED_TEST(OperatorToStringTest, SetProperties) {
  auto node_sym = this->GetSymbol("node");
  std::shared_ptr<LogicalOperator> last_op = std::make_shared<ScanAll>(nullptr, node_sym);
  last_op = std::make_shared<plan::SetProperties>(last_op, node_sym,
                                                  MAP({{this->storage.GetPropertyIx("prop1"), LITERAL(1)},
                                                       {this->storage.GetPropertyIx("prop2"), LITERAL("propko")}}),
                                                  plan::SetProperties::Op::REPLACE);

  std::string expected_string{"SetProperties"};
  EXPECT_EQ(last_op->ToString(), expected_string);
}

TYPED_TEST(OperatorToStringTest, SetLabels) {
  auto node_sym = this->GetSymbol("node");
  std::shared_ptr<LogicalOperator> last_op = std::make_shared<ScanAll>(nullptr, node_sym);
  last_op = std::make_shared<plan::SetLabels>(
      last_op, node_sym,
      std::vector<memgraph::storage::LabelId>{this->dba.NameToLabel("label1"), this->dba.NameToLabel("label2")});

  std::string expected_string{"SetLabels"};
  EXPECT_EQ(last_op->ToString(), expected_string);
}

TYPED_TEST(OperatorToStringTest, RemoveProperty) {
  auto node_sym = this->GetSymbol("node");
  std::shared_ptr<LogicalOperator> last_op = std::make_shared<ScanAll>(nullptr, node_sym);
  last_op = std::make_shared<plan::RemoveProperty>(
      last_op, this->dba.NameToProperty("prop"), PROPERTY_LOOKUP(this->dba, "node", this->dba.NameToProperty("prop")));

  std::string expected_string{"RemoveProperty"};
  EXPECT_EQ(last_op->ToString(), expected_string);
}

TYPED_TEST(OperatorToStringTest, RemoveLabels) {
  auto node_sym = this->GetSymbol("node");
  std::shared_ptr<LogicalOperator> last_op = std::make_shared<ScanAll>(nullptr, node_sym);
  last_op = std::make_shared<plan::RemoveLabels>(
      last_op, node_sym,
      std::vector<memgraph::storage::LabelId>{this->dba.NameToLabel("label1"), this->dba.NameToLabel("label2")});

  std::string expected_string{"RemoveLabels"};
  EXPECT_EQ(last_op->ToString(), expected_string);
}

TYPED_TEST(OperatorToStringTest, EdgeUniquenessFilter) {
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

  std::string expected_string{"EdgeUniquenessFilter {edge1 : edge2}"};
  EXPECT_EQ(last_op->ToString(), expected_string);
}

TYPED_TEST(OperatorToStringTest, Accumulate) {
  memgraph::storage::PropertyId prop = this->dba.NameToProperty("prop");
  auto node_sym = this->GetSymbol("node");
  std::shared_ptr<LogicalOperator> last_op = std::make_shared<ScanAll>(nullptr, node_sym);
  last_op = std::make_shared<plan::SetProperty>(last_op, prop, PROPERTY_LOOKUP(this->dba, "node", prop),
                                                ADD(PROPERTY_LOOKUP(this->dba, "node", prop), LITERAL(1)));
  last_op = std::make_shared<plan::Accumulate>(last_op, std::vector<Symbol>{node_sym}, true);

  std::string expected_string{"Accumulate"};
  EXPECT_EQ(last_op->ToString(), expected_string);
}

TYPED_TEST(OperatorToStringTest, Aggregate) {
  memgraph::storage::PropertyId value = this->dba.NameToProperty("value");
  memgraph::storage::PropertyId color = this->dba.NameToProperty("color");
  memgraph::storage::PropertyId type = this->dba.NameToProperty("type");
  auto node_sym = this->GetSymbol("node");
  std::shared_ptr<LogicalOperator> last_op;
  last_op = std::make_shared<plan::Aggregate>(
      nullptr,
      std::vector<Aggregate::Element>{
          {PROPERTY_LOOKUP(this->dba, "node", value), nullptr, Aggregation::Op::SUM, this->GetSymbol("sum")},
          {PROPERTY_LOOKUP(this->dba, "node", value), PROPERTY_LOOKUP(this->dba, "node", color),
           Aggregation::Op::COLLECT_MAP, this->GetSymbol("map")},
          {nullptr, nullptr, Aggregation::Op::COUNT, this->GetSymbol("count")}},
      std::vector<Expression *>{PROPERTY_LOOKUP(this->dba, "node", type)}, std::vector<Symbol>{node_sym});

  std::string expected_string{"Aggregate {sum, map, count} {node}"};
  EXPECT_EQ(last_op->ToString(), expected_string);
}

TYPED_TEST(OperatorToStringTest, Skip) {
  std::shared_ptr<LogicalOperator> last_op = std::make_shared<ScanAll>(nullptr, this->GetSymbol("node"));
  last_op = std::make_shared<Skip>(last_op, LITERAL(42));

  std::string expected_string{"Skip"};
  EXPECT_EQ(last_op->ToString(), expected_string);
}

TYPED_TEST(OperatorToStringTest, Limit) {
  std::shared_ptr<LogicalOperator> last_op = std::make_shared<ScanAll>(nullptr, this->GetSymbol("node"));
  last_op = std::make_shared<Limit>(last_op, LITERAL(42));

  std::string expected_string{"Limit"};
  EXPECT_EQ(last_op->ToString(), expected_string);
}

TYPED_TEST(OperatorToStringTest, OrderBy) {
  Symbol person_sym = this->GetSymbol("person");
  Symbol pet_sym = this->GetSymbol("pet");
  memgraph::storage::PropertyId name = this->dba.NameToProperty("name");
  memgraph::storage::PropertyId age = this->dba.NameToProperty("age");
  std::shared_ptr<LogicalOperator> last_op;
  last_op = std::make_shared<OrderBy>(nullptr,
                                      std::vector<SortItem>{{Ordering::ASC, PROPERTY_LOOKUP(this->dba, "person", name)},
                                                            {Ordering::DESC, PROPERTY_LOOKUP(this->dba, "pet", age)}},
                                      std::vector<Symbol>{person_sym, pet_sym});

  std::string expected_string{"OrderBy {person, pet}"};
  EXPECT_EQ(last_op->ToString(), expected_string);
}

TYPED_TEST(OperatorToStringTest, Merge) {
  Symbol node_sym = this->GetSymbol("node");
  memgraph::storage::LabelId label = this->dba.NameToLabel("label");

  std::shared_ptr<LogicalOperator> match = std::make_shared<ScanAllByLabel>(nullptr, node_sym, label);

  std::shared_ptr<LogicalOperator> create =
      std::make_shared<CreateNode>(nullptr, NodeCreationInfo{node_sym, {label}, {}});

  std::shared_ptr<LogicalOperator> last_op = std::make_shared<plan::Merge>(nullptr, match, create);

  std::string expected_string{"Merge"};
  EXPECT_EQ(last_op->ToString(), expected_string);
}

TYPED_TEST(OperatorToStringTest, Optional) {
  Symbol node1_sym = this->GetSymbol("node1");
  Symbol node2_sym = this->GetSymbol("node2");
  Symbol edge_sym = this->GetSymbol("edge");

  std::shared_ptr<LogicalOperator> input = std::make_shared<ScanAll>(nullptr, node1_sym);

  std::shared_ptr<LogicalOperator> expand =
      std::make_shared<Expand>(nullptr, node1_sym, node2_sym, edge_sym, EdgeAtom::Direction::OUT,
                               std::vector<memgraph::storage::EdgeTypeId>{}, false, memgraph::storage::View::OLD);

  std::shared_ptr<LogicalOperator> last_op =
      std::make_shared<Optional>(input, expand, std::vector<Symbol>{node2_sym, edge_sym});

  std::string expected_string{"Optional"};
  EXPECT_EQ(last_op->ToString(), expected_string);
}

TYPED_TEST(OperatorToStringTest, Unwind) {
  std::shared_ptr<LogicalOperator> last_op =
      std::make_shared<plan::Unwind>(nullptr, LIST(LITERAL(1), LITERAL(2), LITERAL(3)), this->GetSymbol("x"));

  std::string expected_string{"Unwind"};
  EXPECT_EQ(last_op->ToString(), expected_string);
}

TYPED_TEST(OperatorToStringTest, Distinct) {
  Symbol x = this->GetSymbol("x");
  std::shared_ptr<LogicalOperator> last_op =
      std::make_shared<plan::Unwind>(nullptr, LIST(LITERAL(2), LITERAL(3), LITERAL(2)), x);
  last_op = std::make_shared<Distinct>(last_op, std::vector<Symbol>{x});

  std::string expected_string{"Distinct"};
  EXPECT_EQ(last_op->ToString(), expected_string);
}

TYPED_TEST(OperatorToStringTest, Union) {
  Symbol x = this->GetSymbol("x");
  std::shared_ptr<LogicalOperator> lhs =
      std::make_shared<plan::Unwind>(nullptr, LIST(LITERAL(2), LITERAL(3), LITERAL(2)), x);

  Symbol node = this->GetSymbol("x");
  std::shared_ptr<LogicalOperator> rhs = std::make_shared<ScanAll>(nullptr, node);

  std::shared_ptr<LogicalOperator> last_op = std::make_shared<Union>(
      lhs, rhs, std::vector<Symbol>{this->GetSymbol("x")}, std::vector<Symbol>{x}, std::vector<Symbol>{node});

  std::string expected_string{"Union {x : x}"};
  EXPECT_EQ(last_op->ToString(), expected_string);
}

TYPED_TEST(OperatorToStringTest, CallProcedure) {
  memgraph::query::plan::CallProcedure call_op;
  call_op.input_ = std::make_shared<Once>();
  call_op.procedure_name_ = "mg.procedures";
  call_op.arguments_ = {};
  call_op.result_fields_ = {"is_editable", "is_write", "name", "path", "signature"};
  call_op.result_symbols_ = {this->GetSymbol("is_editable"), this->GetSymbol("is_write"), this->GetSymbol("name"),
                             this->GetSymbol("path"), this->GetSymbol("signature")};

  std::string expected_string{"CallProcedure<mg.procedures> {is_editable, is_write, name, path, signature}"};
  EXPECT_EQ(call_op.ToString(), expected_string);
}

TYPED_TEST(OperatorToStringTest, LoadCsv) {
  memgraph::query::plan::LoadCsv last_op;
  last_op.input_ = std::make_shared<Once>();
  last_op.row_var_ = this->GetSymbol("transaction");

  std::string expected_string{"LoadCsv {transaction}"};
  EXPECT_EQ(last_op.ToString(), expected_string);
}

TYPED_TEST(OperatorToStringTest, Foreach) {
  Symbol x = this->GetSymbol("x");
  std::shared_ptr<LogicalOperator> create = std::make_shared<CreateNode>(
      nullptr, NodeCreationInfo{this->GetSymbol("node"), {this->dba.NameToLabel("Label1")}, {}});
  std::shared_ptr<LogicalOperator> foreach =
      std::make_shared<plan::Foreach>(nullptr, std::move(create), LIST(LITERAL(1)), x);

  std::string expected_string{"Foreach"};
  EXPECT_EQ(foreach->ToString(), expected_string);
}

TYPED_TEST(OperatorToStringTest, EmptyResult) {
  std::shared_ptr<LogicalOperator> last_op = std::make_shared<EmptyResult>(nullptr);

  std::string expected_string{"EmptyResult"};
  EXPECT_EQ(last_op->ToString(), expected_string);
}

TYPED_TEST(OperatorToStringTest, EvaluatePatternFilter) {
  std::shared_ptr<LogicalOperator> last_op = std::make_shared<EvaluatePatternFilter>(nullptr, this->GetSymbol("node"));

  std::string expected_string{"EvaluatePatternFilter"};
  EXPECT_EQ(last_op->ToString(), expected_string);
}

TYPED_TEST(OperatorToStringTest, Apply) {
  memgraph::query::plan::Apply last_op(nullptr, nullptr, false);

  std::string expected_string{"Apply"};
  EXPECT_EQ(last_op.ToString(), expected_string);
}
