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

#include "query/frontend/semantic/symbol_table.hpp"
#include "query/plan/operator.hpp"
#include "query/plan/read_write_type_checker.hpp"

#include "query_common.hpp"

using namespace query;
using namespace query::plan;
using RWType = ReadWriteTypeChecker::RWType;

class ReadWriteTypeCheckTest : public ::testing::Test {
 protected:
  ReadWriteTypeCheckTest() : db(), dba(db.Access()) {}

  AstStorage storage;
  SymbolTable symbol_table;

  storage::Storage db;
  storage::Storage::Accessor dba;

  const Symbol &GetSymbol(std::string name) { return symbol_table.CreateSymbol(name, true); }

  void CheckPlanType(LogicalOperator *root, const RWType expected) {
    auto rw_type_checker = ReadWriteTypeChecker();
    rw_type_checker.InferRWType(*root);
    EXPECT_EQ(rw_type_checker.type, expected);
  }
};

TEST_F(ReadWriteTypeCheckTest, NONEOps) {
  std::shared_ptr<LogicalOperator> once = std::make_shared<Once>();
  std::shared_ptr<LogicalOperator> produce =
      std::make_shared<Produce>(once, std::vector<NamedExpression *>{NEXPR("n", IDENT("n"))});
  CheckPlanType(produce.get(), RWType::NONE);
}

TEST_F(ReadWriteTypeCheckTest, CreateNode) {
  std::shared_ptr<LogicalOperator> once = std::make_shared<Once>();
  std::shared_ptr<LogicalOperator> create_node = std::make_shared<CreateNode>(once, NodeCreationInfo());

  CheckPlanType(create_node.get(), RWType::W);
}

TEST_F(ReadWriteTypeCheckTest, Filter) {
  std::shared_ptr<LogicalOperator> scan_all = std::make_shared<ScanAll>(nullptr, GetSymbol("node1"));
  std::shared_ptr<LogicalOperator> filter =
      std::make_shared<Filter>(scan_all, EQ(PROPERTY_LOOKUP("node1", dba.NameToProperty("prop")), LITERAL(0)));

  CheckPlanType(filter.get(), RWType::R);
}

TEST_F(ReadWriteTypeCheckTest, ScanAllBy) {
  std::shared_ptr<LogicalOperator> last_op = std::make_shared<ScanAllByLabelPropertyRange>(
      nullptr, GetSymbol("node"), dba.NameToLabel("Label"), dba.NameToProperty("prop"), "prop",
      utils::MakeBoundInclusive<Expression *>(LITERAL(1)), utils::MakeBoundExclusive<Expression *>(LITERAL(20)));
  last_op =
      std::make_shared<ScanAllByLabelPropertyValue>(last_op, GetSymbol("node"), dba.NameToLabel("Label"),
                                                    dba.NameToProperty("prop"), "prop", ADD(LITERAL(21), LITERAL(21)));

  CheckPlanType(last_op.get(), RWType::R);
}

TEST_F(ReadWriteTypeCheckTest, OrderByAndLimit) {
  // We build an operator tree that would result from e.g.
  // MATCH (node:label)
  // WHERE n.property = 5
  // RETURN n
  // ORDER BY n.property
  // LIMIT 10
  Symbol node_sym = GetSymbol("node");
  storage::LabelId label = dba.NameToLabel("label");
  storage::PropertyId prop = dba.NameToProperty("property");

  std::shared_ptr<LogicalOperator> last_op = std::make_shared<Once>();
  last_op = std::make_shared<ScanAllByLabel>(last_op, node_sym, label);
  last_op = std::make_shared<Filter>(last_op, EQ(PROPERTY_LOOKUP("node", prop), LITERAL(5)));
  last_op = std::make_shared<Produce>(last_op, std::vector<NamedExpression *>{NEXPR("n", IDENT("n"))});
  last_op = std::make_shared<OrderBy>(last_op, std::vector<SortItem>{{Ordering::DESC, PROPERTY_LOOKUP("node", prop)}},
                                      std::vector<Symbol>{node_sym});
  last_op = std::make_shared<Limit>(last_op, LITERAL(10));

  CheckPlanType(last_op.get(), RWType::R);
}

TEST_F(ReadWriteTypeCheckTest, Delete) {
  auto node_sym = GetSymbol("node1");
  std::shared_ptr<LogicalOperator> last_op = std::make_shared<ScanAll>(nullptr, node_sym);

  last_op =
      std::make_shared<Expand>(last_op, node_sym, GetSymbol("node2"), GetSymbol("edge"), EdgeAtom::Direction::BOTH,
                               std::vector<storage::EdgeTypeId>{}, false, storage::View::OLD);
  last_op = std::make_shared<plan::Delete>(last_op, std::vector<Expression *>{IDENT("node2")}, true);

  CheckPlanType(last_op.get(), RWType::RW);
}

TEST_F(ReadWriteTypeCheckTest, ExpandVariable) {
  auto node1_sym = GetSymbol("node1");

  std::shared_ptr<LogicalOperator> last_op = std::make_shared<ScanAll>(nullptr, node1_sym);

  last_op = std::make_shared<ExpandVariable>(
      last_op, node1_sym, GetSymbol("node2"), GetSymbol("edge"), EdgeAtom::Type::BREADTH_FIRST,
      EdgeAtom::Direction::OUT,
      std::vector<storage::EdgeTypeId>{dba.NameToEdgeType("EdgeType1"), dba.NameToEdgeType("EdgeType2")}, false,
      LITERAL(2), LITERAL(5), false,
      ExpansionLambda{GetSymbol("inner_node"), GetSymbol("inner_edge"),
                      PROPERTY_LOOKUP("inner_node", dba.NameToProperty("unblocked"))},
      std::nullopt, std::nullopt);

  CheckPlanType(last_op.get(), RWType::R);
}

TEST_F(ReadWriteTypeCheckTest, EdgeUniquenessFilter) {
  auto node1_sym = GetSymbol("node1");
  auto node2_sym = GetSymbol("node2");
  auto node3_sym = GetSymbol("node3");
  auto node4_sym = GetSymbol("node4");

  auto edge1_sym = GetSymbol("edge1");
  auto edge2_sym = GetSymbol("edge2");

  std::shared_ptr<LogicalOperator> last_op = std::make_shared<ScanAll>(nullptr, node1_sym);
  last_op = std::make_shared<Expand>(last_op, node1_sym, node2_sym, edge1_sym, EdgeAtom::Direction::IN,
                                     std::vector<storage::EdgeTypeId>{}, false, storage::View::OLD);
  last_op = std::make_shared<ScanAll>(last_op, node3_sym);
  last_op = std::make_shared<Expand>(last_op, node3_sym, node4_sym, edge2_sym, EdgeAtom::Direction::OUT,
                                     std::vector<storage::EdgeTypeId>{}, false, storage::View::OLD);
  last_op = std::make_shared<EdgeUniquenessFilter>(last_op, edge2_sym, std::vector<Symbol>{edge1_sym});

  CheckPlanType(last_op.get(), RWType::R);
}

TEST_F(ReadWriteTypeCheckTest, SetRemovePropertiesLabels) {
  auto node_sym = GetSymbol("node");
  storage::PropertyId prop = dba.NameToProperty("prop");

  std::shared_ptr<LogicalOperator> last_op = std::make_shared<ScanAll>(nullptr, GetSymbol("node"));
  last_op = std::make_shared<plan::SetProperty>(last_op, prop, PROPERTY_LOOKUP("node", prop),
                                                ADD(PROPERTY_LOOKUP("node", prop), LITERAL(1)));
  last_op = std::make_shared<plan::RemoveProperty>(last_op, dba.NameToProperty("prop"),
                                                   PROPERTY_LOOKUP("node", dba.NameToProperty("prop")));
  last_op =
      std::make_shared<plan::SetProperties>(last_op, node_sym,
                                            MAP({{storage.GetPropertyIx("prop1"), LITERAL(1)},
                                                 {storage.GetPropertyIx("prop2"), LITERAL("this is a property")}}),
                                            plan::SetProperties::Op::REPLACE);
  last_op = std::make_shared<plan::SetLabels>(
      last_op, node_sym, std::vector<storage::LabelId>{dba.NameToLabel("label1"), dba.NameToLabel("label2")});
  last_op = std::make_shared<plan::RemoveLabels>(
      last_op, node_sym, std::vector<storage::LabelId>{dba.NameToLabel("label1"), dba.NameToLabel("label2")});

  CheckPlanType(last_op.get(), RWType::RW);
}

TEST_F(ReadWriteTypeCheckTest, Cartesian) {
  Symbol x = GetSymbol("x");
  std::shared_ptr<LogicalOperator> lhs =
      std::make_shared<plan::Unwind>(nullptr, LIST(LITERAL(1), LITERAL(2), LITERAL(3)), x);
  Symbol node = GetSymbol("node");
  std::shared_ptr<LogicalOperator> rhs = std::make_shared<ScanAll>(nullptr, node);
  std::shared_ptr<LogicalOperator> cartesian =
      std::make_shared<Cartesian>(lhs, std::vector<Symbol>{x}, rhs, std::vector<Symbol>{node});

  CheckPlanType(cartesian.get(), RWType::R);
}

TEST_F(ReadWriteTypeCheckTest, Union) {
  Symbol x = GetSymbol("x");
  std::shared_ptr<LogicalOperator> lhs =
      std::make_shared<plan::Unwind>(nullptr, LIST(LITERAL(2), LITERAL(3), LITERAL(2)), x);
  Symbol node = GetSymbol("x");
  std::shared_ptr<LogicalOperator> rhs = std::make_shared<ScanAll>(nullptr, node);
  std::shared_ptr<LogicalOperator> union_op = std::make_shared<Union>(
      lhs, rhs, std::vector<Symbol>{GetSymbol("x")}, std::vector<Symbol>{x}, std::vector<Symbol>{node});

  CheckPlanType(union_op.get(), RWType::R);
}

TEST_F(ReadWriteTypeCheckTest, CallReadProcedure) {
  plan::CallProcedure call_op;
  call_op.input_ = std::make_shared<Once>();
  call_op.procedure_name_ = "mg.reload";
  call_op.arguments_ = {LITERAL("example")};
  call_op.result_fields_ = {"name", "signature"};
  call_op.is_write_ = false;
  call_op.result_symbols_ = {GetSymbol("name_alias"), GetSymbol("signature_alias")};

  CheckPlanType(&call_op, RWType::R);
}

TEST_F(ReadWriteTypeCheckTest, CallWriteProcedure) {
  plan::CallProcedure call_op;
  call_op.input_ = std::make_shared<Once>();
  call_op.procedure_name_ = "mg.reload";
  call_op.arguments_ = {LITERAL("example")};
  call_op.result_fields_ = {"name", "signature"};
  call_op.is_write_ = true;
  call_op.result_symbols_ = {GetSymbol("name_alias"), GetSymbol("signature_alias")};

  CheckPlanType(&call_op, RWType::RW);
}

TEST_F(ReadWriteTypeCheckTest, CallReadProcedureBeforeUpdate) {
  std::shared_ptr<LogicalOperator> last_op = std::make_shared<Once>();
  last_op = std::make_shared<CreateNode>(last_op, NodeCreationInfo());

  std::string procedure_name{"mg.reload"};
  std::vector<Expression *> arguments{LITERAL("example")};
  std::vector<std::string> result_fields{"name", "signature"};
  std::vector<Symbol> result_symbols{GetSymbol("name_alias"), GetSymbol("signature_alias")};

  last_op = std::make_shared<plan::CallProcedure>(last_op, procedure_name, arguments, result_fields, result_symbols,
                                                  nullptr, 0, false);

  CheckPlanType(last_op.get(), RWType::RW);
}

TEST_F(ReadWriteTypeCheckTest, ConstructNamedPath) {
  auto node1_sym = GetSymbol("node1");
  auto edge1_sym = GetSymbol("edge1");
  auto node2_sym = GetSymbol("node2");
  auto edge2_sym = GetSymbol("edge2");
  auto node3_sym = GetSymbol("node3");

  std::shared_ptr<LogicalOperator> last_op = std::make_shared<ScanAll>(nullptr, node1_sym);
  last_op = std::make_shared<Expand>(last_op, node1_sym, node2_sym, edge1_sym, EdgeAtom::Direction::OUT,
                                     std::vector<storage::EdgeTypeId>{}, false, storage::View::OLD);
  last_op = std::make_shared<Expand>(last_op, node2_sym, node3_sym, edge2_sym, EdgeAtom::Direction::OUT,
                                     std::vector<storage::EdgeTypeId>{}, false, storage::View::OLD);
  last_op = std::make_shared<ConstructNamedPath>(
      last_op, GetSymbol("path"), std::vector<Symbol>{node1_sym, edge1_sym, node2_sym, edge2_sym, node3_sym});

  CheckPlanType(last_op.get(), RWType::R);
}
