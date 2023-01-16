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

#include "query_plan_checker_v2.hpp"

#include <iostream>
#include <list>
#include <sstream>
#include <tuple>
#include <typeinfo>
#include <unordered_set>
#include <variant>

#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include "expr/semantic/symbol_generator.hpp"
#include "query/frontend/semantic/symbol_table.hpp"
#include "query/v2/frontend/ast/ast.hpp"
#include "query/v2/plan/operator.hpp"
#include "query/v2/plan/planner.hpp"

#include "query_v2_common.hpp"

namespace memgraph::query {
::std::ostream &operator<<(::std::ostream &os, const Symbol &sym) {
  return os << "Symbol{\"" << sym.name() << "\" [" << sym.position() << "] " << Symbol::TypeToString(sym.type()) << "}";
}
}  // namespace memgraph::query

// using namespace memgraph::query::v2::plan;
using namespace memgraph::expr::plan;
using memgraph::query::Symbol;
using memgraph::query::SymbolGenerator;
using memgraph::query::v2::AstStorage;
using memgraph::query::v2::SingleQuery;
using memgraph::query::v2::SymbolTable;
using Type = memgraph::query::v2::EdgeAtom::Type;
using Direction = memgraph::query::v2::EdgeAtom::Direction;
using Bound = ScanAllByLabelPropertyRange::Bound;

namespace {

class Planner {
 public:
  template <class TDbAccessor>
  Planner(std::vector<SingleQueryPart> single_query_parts, PlanningContext<TDbAccessor> context) {
    memgraph::expr::Parameters parameters;
    PostProcessor post_processor(parameters);
    plan_ = MakeLogicalPlanForSingleQuery<RuleBasedPlanner>(single_query_parts, &context);
    plan_ = post_processor.Rewrite(std::move(plan_), &context);
  }

  auto &plan() { return *plan_; }

 private:
  std::unique_ptr<LogicalOperator> plan_;
};

template <class... TChecker>
auto CheckPlan(LogicalOperator &plan, const SymbolTable &symbol_table, TChecker... checker) {
  std::list<BaseOpChecker *> checkers{&checker...};
  PlanChecker plan_checker(checkers, symbol_table);
  plan.Accept(plan_checker);
  EXPECT_TRUE(plan_checker.checkers_.empty());
}

template <class TPlanner, class... TChecker>
auto CheckPlan(memgraph::query::v2::CypherQuery *query, AstStorage &storage, TChecker... checker) {
  auto symbol_table = memgraph::expr::MakeSymbolTable(query);
  FakeDistributedDbAccessor dba;
  auto planner = MakePlanner<TPlanner>(&dba, storage, symbol_table, query);
  CheckPlan(planner.plan(), symbol_table, checker...);
}

template <class T>
class TestPlanner : public ::testing::Test {};

using PlannerTypes = ::testing::Types<Planner>;

TYPED_TEST_CASE(TestPlanner, PlannerTypes);

TYPED_TEST(TestPlanner, MatchFilterPropIsNotNull) {
  const char *prim_label_name = "prim_label_one";
  // Exact primary key match, one elem as PK.
  {
    FakeDistributedDbAccessor dba;
    auto label = dba.Label(prim_label_name);
    auto prim_prop_one = PRIMARY_PROPERTY_PAIR("prim_prop_one");

    dba.SetIndexCount(label, 1);
    dba.SetIndexCount(label, prim_prop_one.second, 1);

    dba.CreateSchema(label, {prim_prop_one.second});

    memgraph::query::v2::AstStorage storage;

    memgraph::query::v2::Expression *expected_primary_key;
    expected_primary_key = PROPERTY_LOOKUP("n", prim_prop_one);
    auto *query = QUERY(SINGLE_QUERY(MATCH(PATTERN(NODE("n", prim_label_name))),
                                     WHERE(EQ(PROPERTY_LOOKUP("n", prim_prop_one), LITERAL(1))), RETURN("n")));
    auto symbol_table = (memgraph::expr::MakeSymbolTable(query));
    auto planner = MakePlanner<TypeParam>(&dba, storage, symbol_table, query);
    CheckPlan(planner.plan(), symbol_table, ExpectScanAllByPrimaryKey(label, {expected_primary_key}), ExpectProduce());
  }
  // Exact primary key match, two elem as PK.
  {
    FakeDistributedDbAccessor dba;
    auto label = dba.Label(prim_label_name);
    auto prim_prop_one = PRIMARY_PROPERTY_PAIR("prim_prop_one");

    auto prim_prop_two = PRIMARY_PROPERTY_PAIR("prim_prop_two");
    auto sec_prop_one = PRIMARY_PROPERTY_PAIR("sec_prop_one");
    auto sec_prop_two = PRIMARY_PROPERTY_PAIR("sec_prop_two");
    auto sec_prop_three = PRIMARY_PROPERTY_PAIR("sec_prop_three");

    dba.SetIndexCount(label, 1);
    dba.SetIndexCount(label, prim_prop_one.second, 1);

    dba.CreateSchema(label, {prim_prop_one.second, prim_prop_two.second});

    dba.SetIndexCount(label, prim_prop_two.second, 1);
    dba.SetIndexCount(label, sec_prop_one.second, 1);
    dba.SetIndexCount(label, sec_prop_two.second, 1);
    dba.SetIndexCount(label, sec_prop_three.second, 1);
    memgraph::query::v2::AstStorage storage;

    memgraph::query::v2::Expression *expected_primary_key;
    expected_primary_key = PROPERTY_LOOKUP("n", prim_prop_one);
    auto *query = QUERY(SINGLE_QUERY(MATCH(PATTERN(NODE("n", prim_label_name))),
                                     WHERE(AND(EQ(PROPERTY_LOOKUP("n", prim_prop_one), LITERAL(1)),
                                               EQ(PROPERTY_LOOKUP("n", prim_prop_two), LITERAL(1)))),
                                     RETURN("n")));
    auto symbol_table = (memgraph::expr::MakeSymbolTable(query));
    auto planner = MakePlanner<TypeParam>(&dba, storage, symbol_table, query);
    CheckPlan(planner.plan(), symbol_table, ExpectScanAllByPrimaryKey(label, {expected_primary_key}), ExpectProduce());
  }
  // One elem is missing from PK, default to ScanAllByLabelPropertyValue.
  {
    FakeDistributedDbAccessor dba;
    auto label = dba.Label(prim_label_name);

    auto prim_prop_one = PRIMARY_PROPERTY_PAIR("prim_prop_one");
    auto prim_prop_two = PRIMARY_PROPERTY_PAIR("prim_prop_two");

    auto sec_prop_one = PRIMARY_PROPERTY_PAIR("sec_prop_one");
    auto sec_prop_two = PRIMARY_PROPERTY_PAIR("sec_prop_two");
    auto sec_prop_three = PRIMARY_PROPERTY_PAIR("sec_prop_three");

    dba.SetIndexCount(label, 1);
    dba.SetIndexCount(label, prim_prop_one.second, 1);

    dba.CreateSchema(label, {prim_prop_one.second, prim_prop_two.second});

    dba.SetIndexCount(label, prim_prop_two.second, 1);
    dba.SetIndexCount(label, sec_prop_one.second, 1);
    dba.SetIndexCount(label, sec_prop_two.second, 1);
    dba.SetIndexCount(label, sec_prop_three.second, 1);
    memgraph::query::v2::AstStorage storage;

    auto *query = QUERY(SINGLE_QUERY(MATCH(PATTERN(NODE("n", prim_label_name))),
                                     WHERE(EQ(PROPERTY_LOOKUP("n", prim_prop_one), LITERAL(1))), RETURN("n")));
    auto symbol_table = (memgraph::expr::MakeSymbolTable(query));
    auto planner = MakePlanner<TypeParam>(&dba, storage, symbol_table, query);
    CheckPlan(planner.plan(), symbol_table, ExpectScanAllByLabelPropertyValue(label, prim_prop_one, IDENT("n")),
              ExpectProduce());
  }
}

}  // namespace
