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

#include "query_plan_checker.hpp"

#include <functional>
#include <list>
#include <memory>
#include <vector>

#include <gtest/gtest.h>

#include "query/frontend/ast/ast.hpp"
#include "query/frontend/semantic/symbol_generator.hpp"
#include "query/frontend/semantic/symbol_table.hpp"
#include "query/plan/operator.hpp"
#include "query/plan/planner.hpp"

#include "query_common.hpp"

using namespace memgraph::query::plan;
using memgraph::query::AstStorage;
using memgraph::query::Ordering;
using memgraph::query::SingleQuery;
using memgraph::query::Symbol;
using memgraph::query::SymbolTable;
using Direction = memgraph::query::EdgeAtom::Direction;
namespace ms = memgraph::storage;

namespace {

class Planner {
 public:
  template <class TDbAccessor>
  Planner(QueryParts query_parts, PlanningContext<TDbAccessor> context,
          const std::vector<memgraph::query::IndexHint> &index_hints)
      : plan_(MakeLogicalPlanForSingleQuery<RuleBasedPlanner>(query_parts, &context)) {
    memgraph::query::Parameters parameters;
    PostProcessor post_processor(parameters, index_hints, context.db);
    plan_ = post_processor.Rewrite(std::move(plan_), &context);
  }

  auto &plan() { return *plan_; }

 private:
  std::unique_ptr<LogicalOperator> plan_;
};

using PlannerTypes = ::testing::Types<Planner>;

/// Walk the plan tree and return true if an operator of the given type exists.
bool PlanContainsOp(LogicalOperator &root, const memgraph::utils::TypeInfo &type_info) {
  if (root.GetTypeInfo() == type_info) return true;
  if (root.HasSingleInput()) {
    return PlanContainsOp(*root.input(), type_info);
  }
  if (root.GetTypeInfo() == Cartesian::kType) {
    auto &cart = dynamic_cast<Cartesian &>(root);
    return PlanContainsOp(*cart.left_op_, type_info) || PlanContainsOp(*cart.right_op_, type_info);
  }
  return false;
}

template <class T>
class OrderByIndexTest : public ::testing::Test {
 public:
  AstStorage storage;
};

TYPED_TEST_SUITE(OrderByIndexTest, PlannerTypes);

// Test 1: Basic elimination - ORDER BY n.prop with :L(prop) index and range filter
TYPED_TEST(OrderByIndexTest, BasicElimination) {
  // MATCH (n:L) WHERE n.prop > 5 ORDER BY n.prop RETURN n
  FakeDbAccessor dba;
  const auto *const label_name = "L";
  const auto label = dba.Label(label_name);
  const auto property = PROPERTY_PAIR(dba, "prop");
  dba.SetIndexCount(label, 1);
  dba.SetIndexCount(label, property.second, 1);

  auto *query = QUERY(SINGLE_QUERY(MATCH(PATTERN(NODE("n", label_name))),
                                   WHERE(GREATER(PROPERTY_LOOKUP(dba, "n", property.second), LITERAL(5))),
                                   RETURN("n", ORDER_BY(PROPERTY_LOOKUP(dba, "n", property.second)))));

  auto symbol_table = memgraph::query::MakeSymbolTable(query);
  auto planner = MakePlanner<TypeParam>(&dba, this->storage, symbol_table, query);

  EXPECT_TRUE(PlanContainsOp(planner.plan(), ScanAllByLabelProperties::kType))
      << "Plan should use ScanAllByLabelProperties";
  EXPECT_FALSE(PlanContainsOp(planner.plan(), OrderBy::kType)) << "OrderBy should be eliminated";
}

// Test 2: Composite prefix - ORDER BY n.a with index (a, b)
TYPED_TEST(OrderByIndexTest, CompositePrefix) {
  // MATCH (n:L) WHERE n.a > 5 ORDER BY n.a RETURN n
  FakeDbAccessor dba;
  const auto *const label_name = "L";
  const auto label = dba.Label(label_name);
  const auto prop_a = PROPERTY_PAIR(dba, "a");
  const auto prop_b = PROPERTY_PAIR(dba, "b");
  dba.SetIndexCount(label, 1);
  std::vector<ms::PropertyPath> composite_props{ms::PropertyPath{prop_a.second}, ms::PropertyPath{prop_b.second}};
  dba.SetIndexCount(label, std::span{composite_props}, 1);

  auto *query = QUERY(SINGLE_QUERY(MATCH(PATTERN(NODE("n", label_name))),
                                   WHERE(GREATER(PROPERTY_LOOKUP(dba, "n", prop_a.second), LITERAL(5))),
                                   RETURN("n", ORDER_BY(PROPERTY_LOOKUP(dba, "n", prop_a.second)))));

  auto symbol_table = memgraph::query::MakeSymbolTable(query);
  auto planner = MakePlanner<TypeParam>(&dba, this->storage, symbol_table, query);

  EXPECT_TRUE(PlanContainsOp(planner.plan(), ScanAllByLabelProperties::kType));
  EXPECT_FALSE(PlanContainsOp(planner.plan(), OrderBy::kType)) << "OrderBy should be eliminated (composite prefix)";
}

// Test 3: Equality on first column, ORDER BY second — equality-pinned skip allows elimination
TYPED_TEST(OrderByIndexTest, EqualitySkipEliminated) {
  // MATCH (n:L) WHERE n.a = 5 ORDER BY n.b RETURN n
  FakeDbAccessor dba;
  const auto *const label_name = "L";
  const auto label = dba.Label(label_name);
  const auto prop_a = PROPERTY_PAIR(dba, "a");
  const auto prop_b = PROPERTY_PAIR(dba, "b");
  dba.SetIndexCount(label, 1);
  std::vector<ms::PropertyPath> composite_props{ms::PropertyPath{prop_a.second}, ms::PropertyPath{prop_b.second}};
  dba.SetIndexCount(label, std::span{composite_props}, 1);

  auto *query = QUERY(SINGLE_QUERY(MATCH(PATTERN(NODE("n", label_name))),
                                   WHERE(EQ(PROPERTY_LOOKUP(dba, "n", prop_a.second), LITERAL(5))),
                                   RETURN("n", ORDER_BY(PROPERTY_LOOKUP(dba, "n", prop_b.second)))));

  auto symbol_table = memgraph::query::MakeSymbolTable(query);
  auto planner = MakePlanner<TypeParam>(&dba, this->storage, symbol_table, query);

  EXPECT_TRUE(PlanContainsOp(planner.plan(), ScanAllByLabelProperties::kType));
  EXPECT_FALSE(PlanContainsOp(planner.plan(), OrderBy::kType))
      << "OrderBy should be eliminated (a is equality-pinned, ORDER BY b follows in index)";
}

// Test 4: Full composite - WHERE n.a = 5 ORDER BY n.a, n.b with index (a, b)
TYPED_TEST(OrderByIndexTest, FullCompositeWithEquality) {
  // MATCH (n:L) WHERE n.a = 5 ORDER BY n.a, n.b RETURN n
  FakeDbAccessor dba;
  const auto *const label_name = "L";
  const auto label = dba.Label(label_name);
  const auto prop_a = PROPERTY_PAIR(dba, "a");
  const auto prop_b = PROPERTY_PAIR(dba, "b");
  dba.SetIndexCount(label, 1);
  std::vector<ms::PropertyPath> composite_props{ms::PropertyPath{prop_a.second}, ms::PropertyPath{prop_b.second}};
  dba.SetIndexCount(label, std::span{composite_props}, 1);

  auto *query = QUERY(SINGLE_QUERY(
      MATCH(PATTERN(NODE("n", label_name))),
      WHERE(EQ(PROPERTY_LOOKUP(dba, "n", prop_a.second), LITERAL(5))),
      RETURN("n", ORDER_BY(PROPERTY_LOOKUP(dba, "n", prop_a.second), PROPERTY_LOOKUP(dba, "n", prop_b.second)))));

  auto symbol_table = memgraph::query::MakeSymbolTable(query);
  auto planner = MakePlanner<TypeParam>(&dba, this->storage, symbol_table, query);

  EXPECT_TRUE(PlanContainsOp(planner.plan(), ScanAllByLabelProperties::kType));
  EXPECT_FALSE(PlanContainsOp(planner.plan(), OrderBy::kType))
      << "OrderBy should be eliminated (full composite with equality)";
}

// Test 5: With LIMIT - ORDER BY n.prop LIMIT 10
TYPED_TEST(OrderByIndexTest, WithLimit) {
  // MATCH (n:L) WHERE n.prop > 5 ORDER BY n.prop LIMIT 10 RETURN n
  FakeDbAccessor dba;
  const auto *const label_name = "L";
  const auto label = dba.Label(label_name);
  const auto property = PROPERTY_PAIR(dba, "prop");
  dba.SetIndexCount(label, 1);
  dba.SetIndexCount(label, property.second, 1);

  auto *query =
      QUERY(SINGLE_QUERY(MATCH(PATTERN(NODE("n", label_name))),
                         WHERE(GREATER(PROPERTY_LOOKUP(dba, "n", property.second), LITERAL(5))),
                         RETURN("n", ORDER_BY(PROPERTY_LOOKUP(dba, "n", property.second)), LIMIT(LITERAL(10)))));

  auto symbol_table = memgraph::query::MakeSymbolTable(query);
  auto planner = MakePlanner<TypeParam>(&dba, this->storage, symbol_table, query);

  EXPECT_FALSE(PlanContainsOp(planner.plan(), OrderBy::kType)) << "OrderBy should be eliminated";
  EXPECT_TRUE(PlanContainsOp(planner.plan(), Limit::kType)) << "Limit should remain";
}

// Test 6: DESC ordering is not eliminated — index only provides ASC order
TYPED_TEST(OrderByIndexTest, DescRejected) {
  // MATCH (n:L) WHERE n.prop > 5 ORDER BY n.prop DESC RETURN n
  FakeDbAccessor dba;
  const auto *const label_name = "L";
  const auto label = dba.Label(label_name);
  const auto property = PROPERTY_PAIR(dba, "prop");
  dba.SetIndexCount(label, 1);
  dba.SetIndexCount(label, property.second, 1);

  auto *query = QUERY(SINGLE_QUERY(MATCH(PATTERN(NODE("n", label_name))),
                                   WHERE(GREATER(PROPERTY_LOOKUP(dba, "n", property.second), LITERAL(5))),
                                   RETURN("n", ORDER_BY(PROPERTY_LOOKUP(dba, "n", property.second), Ordering::DESC))));

  auto symbol_table = memgraph::query::MakeSymbolTable(query);
  auto planner = MakePlanner<TypeParam>(&dba, this->storage, symbol_table, query);

  EXPECT_TRUE(PlanContainsOp(planner.plan(), OrderBy::kType)) << "OrderBy should NOT be eliminated (DESC)";
}

// Test 7: Non-property expression in ORDER BY — computed expressions can't match index columns
TYPED_TEST(OrderByIndexTest, NonPropertyExprRejected) {
  // MATCH (n:L) WHERE n.prop > 5 ORDER BY n.prop + 1 RETURN n
  FakeDbAccessor dba;
  const auto *const label_name = "L";
  const auto label = dba.Label(label_name);
  const auto property = PROPERTY_PAIR(dba, "prop");
  dba.SetIndexCount(label, 1);
  dba.SetIndexCount(label, property.second, 1);

  auto *query = QUERY(SINGLE_QUERY(MATCH(PATTERN(NODE("n", label_name))),
                                   WHERE(GREATER(PROPERTY_LOOKUP(dba, "n", property.second), LITERAL(5))),
                                   RETURN("n", ORDER_BY(ADD(PROPERTY_LOOKUP(dba, "n", property.second), LITERAL(1))))));

  auto symbol_table = memgraph::query::MakeSymbolTable(query);
  auto planner = MakePlanner<TypeParam>(&dba, this->storage, symbol_table, query);

  EXPECT_TRUE(PlanContainsOp(planner.plan(), OrderBy::kType))
      << "OrderBy should NOT be eliminated (non-property expression)";
}

// Test 8: ORDER BY property differs from index property — no elimination
TYPED_TEST(OrderByIndexTest, NoMatchingIndex) {
  // MATCH (n:L) WHERE n.a > 5 ORDER BY n.b RETURN n
  FakeDbAccessor dba;
  const auto *const label_name = "L";
  const auto label = dba.Label(label_name);
  const auto prop_a = PROPERTY_PAIR(dba, "a");
  const auto prop_b = PROPERTY_PAIR(dba, "b");
  dba.SetIndexCount(label, 1);
  dba.SetIndexCount(label, prop_a.second, 1);

  auto *query = QUERY(SINGLE_QUERY(MATCH(PATTERN(NODE("n", label_name))),
                                   WHERE(GREATER(PROPERTY_LOOKUP(dba, "n", prop_a.second), LITERAL(5))),
                                   RETURN("n", ORDER_BY(PROPERTY_LOOKUP(dba, "n", prop_b.second)))));

  auto symbol_table = memgraph::query::MakeSymbolTable(query);
  auto planner = MakePlanner<TypeParam>(&dba, this->storage, symbol_table, query);

  EXPECT_TRUE(PlanContainsOp(planner.plan(), OrderBy::kType))
      << "OrderBy should NOT be eliminated (ORDER BY property not in index)";
}

// Test 9: ORDER BY has more columns than the single-property index — can't fully satisfy
TYPED_TEST(OrderByIndexTest, OrderBySuperset) {
  // MATCH (n:L) WHERE n.a > 5 ORDER BY n.a, n.b RETURN n
  FakeDbAccessor dba;
  const auto *label_name = "L";
  const auto label = dba.Label(label_name);
  const auto prop_a = PROPERTY_PAIR(dba, "a");
  const auto prop_b = PROPERTY_PAIR(dba, "b");
  dba.SetIndexCount(label, 1);
  dba.SetIndexCount(label, prop_a.second, 1);

  auto *query = QUERY(SINGLE_QUERY(
      MATCH(PATTERN(NODE("n", label_name))),
      WHERE(GREATER(PROPERTY_LOOKUP(dba, "n", prop_a.second), LITERAL(5))),
      RETURN("n", ORDER_BY(PROPERTY_LOOKUP(dba, "n", prop_a.second), PROPERTY_LOOKUP(dba, "n", prop_b.second)))));

  auto symbol_table = memgraph::query::MakeSymbolTable(query);
  auto planner = MakePlanner<TypeParam>(&dba, this->storage, symbol_table, query);

  EXPECT_TRUE(PlanContainsOp(planner.plan(), OrderBy::kType))
      << "OrderBy should NOT be eliminated (ORDER BY has more keys than index)";
}

// Test 10: Expand preserves order - ORDER BY n.prop with Expand between OrderBy and ScanAll
TYPED_TEST(OrderByIndexTest, ExpandPreservesOrder) {
  // MATCH (n:L)-[r]->(m) WHERE n.prop > 5 ORDER BY n.prop RETURN n, m
  FakeDbAccessor dba;
  const auto *label_name = "L";
  const auto label = dba.Label(label_name);
  const auto property = PROPERTY_PAIR(dba, "prop");
  dba.SetIndexCount(label, 1);
  dba.SetIndexCount(label, property.second, 1);

  auto *query = QUERY(SINGLE_QUERY(MATCH(PATTERN(NODE("n", label_name), EDGE("r", Direction::OUT), NODE("m"))),
                                   WHERE(GREATER(PROPERTY_LOOKUP(dba, "n", property.second), LITERAL(5))),
                                   RETURN("n", "m", ORDER_BY(PROPERTY_LOOKUP(dba, "n", property.second)))));

  auto symbol_table = memgraph::query::MakeSymbolTable(query);
  auto planner = MakePlanner<TypeParam>(&dba, this->storage, symbol_table, query);

  EXPECT_FALSE(PlanContainsOp(planner.plan(), OrderBy::kType))
      << "OrderBy should be eliminated (Expand is order-preserving)";
}

// Test 10b: ORDER BY on expanded symbol m, not scan symbol n — index order is on n.prop, not m.prop.
// Only L has a property index, so the planner must scan n. ORDER BY m.prop can't be eliminated.
TYPED_TEST(OrderByIndexTest, ExpandOrderByExpandedSymbol) {
  // MATCH (n:L)-[r]->(m) WHERE n.prop > 5 ORDER BY m.prop RETURN n, m
  FakeDbAccessor dba;
  const auto *const label_name = "L";
  const auto label = dba.Label(label_name);
  const auto property = PROPERTY_PAIR(dba, "prop");
  dba.SetIndexCount(label, 1);
  dba.SetIndexCount(label, property.second, 1);

  auto *query = QUERY(SINGLE_QUERY(MATCH(PATTERN(NODE("n", label_name), EDGE("r", Direction::OUT), NODE("m"))),
                                   WHERE(GREATER(PROPERTY_LOOKUP(dba, "n", property.second), LITERAL(5))),
                                   RETURN("n", "m", ORDER_BY(PROPERTY_LOOKUP(dba, "m", property.second)))));

  auto symbol_table = memgraph::query::MakeSymbolTable(query);
  auto planner = MakePlanner<TypeParam>(&dba, this->storage, symbol_table, query);

  EXPECT_TRUE(PlanContainsOp(planner.plan(), OrderBy::kType))
      << "OrderBy should NOT be eliminated (ORDER BY m.prop but index scan is on n)";
}

// Test 11: Cartesian product between OrderBy and ScanAll breaks ordering guarantee
TYPED_TEST(OrderByIndexTest, CartesianBlocks) {
  // MATCH (n:L), (m:K) WHERE n.prop > 5 ORDER BY n.prop RETURN n, m
  FakeDbAccessor dba;
  const auto *label_l = "L";
  const auto *label_k = "K";
  const auto label_l_id = dba.Label(label_l);
  const auto label_k_id = dba.Label(label_k);
  const auto property = PROPERTY_PAIR(dba, "prop");
  dba.SetIndexCount(label_l_id, 1);
  dba.SetIndexCount(label_l_id, property.second, 1);
  dba.SetIndexCount(label_k_id, 1);

  auto *query = QUERY(SINGLE_QUERY(MATCH(PATTERN(NODE("n", label_l)), PATTERN(NODE("m", label_k))),
                                   WHERE(GREATER(PROPERTY_LOOKUP(dba, "n", property.second), LITERAL(5))),
                                   RETURN("n", "m", ORDER_BY(PROPERTY_LOOKUP(dba, "n", property.second)))));

  auto symbol_table = memgraph::query::MakeSymbolTable(query);
  auto planner = MakePlanner<TypeParam>(&dba, this->storage, symbol_table, query);

  EXPECT_TRUE(PlanContainsOp(planner.plan(), OrderBy::kType))
      << "OrderBy should NOT be eliminated (Cartesian between OrderBy and ScanAll)";
}

// Test 12: Aggregate (hash grouping) between OrderBy and ScanAll destroys ordering
TYPED_TEST(OrderByIndexTest, AggregateBlocks) {
  // MATCH (n:L) WHERE n.prop > 5 RETURN n.prop AS p, count(*) AS c ORDER BY p
  FakeDbAccessor dba;
  const auto *label_name = "L";
  const auto label = dba.Label(label_name);
  const auto property = PROPERTY_PAIR(dba, "prop");
  dba.SetIndexCount(label, 1);
  dba.SetIndexCount(label, property.second, 1);

  auto *count_expr = COUNT(LITERAL(1), false);
  auto *query = QUERY(SINGLE_QUERY(
      MATCH(PATTERN(NODE("n", label_name))),
      WHERE(GREATER(PROPERTY_LOOKUP(dba, "n", property.second), LITERAL(5))),
      RETURN(PROPERTY_LOOKUP(dba, "n", property.second), AS("p"), count_expr, AS("c"), ORDER_BY(IDENT("p")))));

  auto symbol_table = memgraph::query::MakeSymbolTable(query);
  auto planner = MakePlanner<TypeParam>(&dba, this->storage, symbol_table, query);

  EXPECT_TRUE(PlanContainsOp(planner.plan(), OrderBy::kType))
      << "OrderBy should NOT be eliminated (Aggregate between OrderBy and ScanAll)";
}

// Test 13: Equality on first column + range on second, ORDER BY second — pinned skip allows elimination
TYPED_TEST(OrderByIndexTest, EqualityPlusRangeOnSecondColumnEliminated) {
  // MATCH (n:L) WHERE n.a = 5 AND n.b > 3 ORDER BY n.b RETURN n
  FakeDbAccessor dba;
  const auto *label_name = "L";
  const auto label = dba.Label(label_name);
  const auto prop_a = PROPERTY_PAIR(dba, "a");
  const auto prop_b = PROPERTY_PAIR(dba, "b");
  dba.SetIndexCount(label, 1);
  std::vector<ms::PropertyPath> composite_props{ms::PropertyPath{prop_a.second}, ms::PropertyPath{prop_b.second}};
  dba.SetIndexCount(label, std::span{composite_props}, 1);

  auto *query = QUERY(SINGLE_QUERY(MATCH(PATTERN(NODE("n", label_name))),
                                   WHERE(AND(EQ(PROPERTY_LOOKUP(dba, "n", prop_a.second), LITERAL(5)),
                                             GREATER(PROPERTY_LOOKUP(dba, "n", prop_b.second), LITERAL(3)))),
                                   RETURN("n", ORDER_BY(PROPERTY_LOOKUP(dba, "n", prop_b.second)))));

  auto symbol_table = memgraph::query::MakeSymbolTable(query);
  auto planner = MakePlanner<TypeParam>(&dba, this->storage, symbol_table, query);

  EXPECT_TRUE(PlanContainsOp(planner.plan(), ScanAllByLabelProperties::kType));
  EXPECT_FALSE(PlanContainsOp(planner.plan(), OrderBy::kType))
      << "OrderBy should be eliminated (a is equality-pinned, ORDER BY b follows in index)";
}

// Test 14: ORDER BY matches full composite index (a, b) with range on a — elimination applies
TYPED_TEST(OrderByIndexTest, FullCompositeRangeOnFirst) {
  // MATCH (n:L) WHERE n.a > 5 ORDER BY n.a, n.b RETURN n
  FakeDbAccessor dba;
  const auto *label_name = "L";
  const auto label = dba.Label(label_name);
  const auto prop_a = PROPERTY_PAIR(dba, "a");
  const auto prop_b = PROPERTY_PAIR(dba, "b");
  dba.SetIndexCount(label, 1);
  std::vector<ms::PropertyPath> composite_props{ms::PropertyPath{prop_a.second}, ms::PropertyPath{prop_b.second}};
  dba.SetIndexCount(label, std::span{composite_props}, 1);

  auto *query = QUERY(SINGLE_QUERY(
      MATCH(PATTERN(NODE("n", label_name))),
      WHERE(GREATER(PROPERTY_LOOKUP(dba, "n", prop_a.second), LITERAL(5))),
      RETURN("n", ORDER_BY(PROPERTY_LOOKUP(dba, "n", prop_a.second), PROPERTY_LOOKUP(dba, "n", prop_b.second)))));

  auto symbol_table = memgraph::query::MakeSymbolTable(query);
  auto planner = MakePlanner<TypeParam>(&dba, this->storage, symbol_table, query);

  EXPECT_TRUE(PlanContainsOp(planner.plan(), ScanAllByLabelProperties::kType));
  EXPECT_FALSE(PlanContainsOp(planner.plan(), OrderBy::kType))
      << "OrderBy should be eliminated (ORDER BY n.a, n.b matches index (a, b) with range on a)";
}

// Test 15: Equality on a + range on b, ORDER BY a, b — equality-pinned column also in ORDER BY
TYPED_TEST(OrderByIndexTest, EqualityPlusRangeOrderByBoth) {
  // MATCH (n:L) WHERE n.a = 5 AND n.b > 3 ORDER BY n.a, n.b RETURN n
  FakeDbAccessor dba;
  const auto *label_name = "L";
  const auto label = dba.Label(label_name);
  const auto prop_a = PROPERTY_PAIR(dba, "a");
  const auto prop_b = PROPERTY_PAIR(dba, "b");
  dba.SetIndexCount(label, 1);
  std::vector<ms::PropertyPath> composite_props{ms::PropertyPath{prop_a.second}, ms::PropertyPath{prop_b.second}};
  dba.SetIndexCount(label, std::span{composite_props}, 1);

  auto *query = QUERY(SINGLE_QUERY(
      MATCH(PATTERN(NODE("n", label_name))),
      WHERE(AND(EQ(PROPERTY_LOOKUP(dba, "n", prop_a.second), LITERAL(5)),
                GREATER(PROPERTY_LOOKUP(dba, "n", prop_b.second), LITERAL(3)))),
      RETURN("n", ORDER_BY(PROPERTY_LOOKUP(dba, "n", prop_a.second), PROPERTY_LOOKUP(dba, "n", prop_b.second)))));

  auto symbol_table = memgraph::query::MakeSymbolTable(query);
  auto planner = MakePlanner<TypeParam>(&dba, this->storage, symbol_table, query);

  EXPECT_TRUE(PlanContainsOp(planner.plan(), ScanAllByLabelProperties::kType));
  EXPECT_FALSE(PlanContainsOp(planner.plan(), OrderBy::kType))
      << "OrderBy should be eliminated (equality on a + range on b, ORDER BY a, b)";
}

// Test 16: Equality-only filter — all rows have the same value, ORDER BY is trivially satisfied
TYPED_TEST(OrderByIndexTest, EqualityOnlyElimination) {
  // MATCH (n:L) WHERE n.prop = 5 ORDER BY n.prop RETURN n
  FakeDbAccessor dba;
  const auto *label_name = "L";
  const auto label = dba.Label(label_name);
  const auto property = PROPERTY_PAIR(dba, "prop");
  dba.SetIndexCount(label, 1);
  dba.SetIndexCount(label, property.second, 1);

  auto *query = QUERY(SINGLE_QUERY(MATCH(PATTERN(NODE("n", label_name))),
                                   WHERE(EQ(PROPERTY_LOOKUP(dba, "n", property.second), LITERAL(5))),
                                   RETURN("n", ORDER_BY(PROPERTY_LOOKUP(dba, "n", property.second)))));

  auto symbol_table = memgraph::query::MakeSymbolTable(query);
  auto planner = MakePlanner<TypeParam>(&dba, this->storage, symbol_table, query);

  EXPECT_TRUE(PlanContainsOp(planner.plan(), ScanAllByLabelProperties::kType));
  EXPECT_FALSE(PlanContainsOp(planner.plan(), OrderBy::kType))
      << "OrderBy should be eliminated (equality-only, trivially sorted)";
}

// Test 17: Two separate MATCHes produce a Cartesian — ORDER BY n.prop can't be eliminated
TYPED_TEST(OrderByIndexTest, MultiMatchDifferentSymbols) {
  // MATCH (n:L) WHERE n.prop > 5 MATCH (m:L) WHERE m.prop > 3 ORDER BY n.prop RETURN n, m
  FakeDbAccessor dba;
  const auto *label_name = "L";
  const auto label = dba.Label(label_name);
  const auto property = PROPERTY_PAIR(dba, "prop");
  dba.SetIndexCount(label, 1);
  dba.SetIndexCount(label, property.second, 1);

  auto *query = QUERY(SINGLE_QUERY(MATCH(PATTERN(NODE("n", label_name))),
                                   WHERE(GREATER(PROPERTY_LOOKUP(dba, "n", property.second), LITERAL(5))),
                                   MATCH(PATTERN(NODE("m", label_name))),
                                   WHERE(GREATER(PROPERTY_LOOKUP(dba, "m", property.second), LITERAL(3))),
                                   RETURN("n", "m", ORDER_BY(PROPERTY_LOOKUP(dba, "n", property.second)))));

  auto symbol_table = memgraph::query::MakeSymbolTable(query);
  auto planner = MakePlanner<TypeParam>(&dba, this->storage, symbol_table, query);

  EXPECT_TRUE(PlanContainsOp(planner.plan(), OrderBy::kType))
      << "OrderBy should NOT be eliminated (Cartesian between OrderBy and n's scan)";
}

// Test 18: WITH renames n to m — rename is tracked through Produce, elimination still applies
TYPED_TEST(OrderByIndexTest, WithRenamingAllowsElimination) {
  // MATCH (n:L) WHERE n.prop > 5 WITH n AS m ORDER BY m.prop RETURN m
  FakeDbAccessor dba;
  const auto label_name = "L";
  const auto label = dba.Label(label_name);
  const auto property = PROPERTY_PAIR(dba, "prop");
  dba.SetIndexCount(label, 1);
  dba.SetIndexCount(label, property.second, 1);

  auto *ident_n = IDENT("n");
  auto *match_clause = MATCH(PATTERN(NODE("n", label_name)));
  match_clause->where_ = WHERE(GREATER(PROPERTY_LOOKUP(dba, "n", property.second), LITERAL(5)));
  auto *with_clause = WITH(ident_n, AS("m"));
  auto *return_clause = RETURN("m", ORDER_BY(PROPERTY_LOOKUP(dba, "m", property.second)));
  auto *single = this->storage.template Create<memgraph::query::SingleQuery>();
  single->clauses_.push_back(match_clause);
  single->clauses_.push_back(with_clause);
  single->clauses_.push_back(return_clause);
  auto *query = QUERY(single);

  auto symbol_table = memgraph::query::MakeSymbolTable(query);
  auto planner = MakePlanner<TypeParam>(&dba, this->storage, symbol_table, query);

  EXPECT_FALSE(PlanContainsOp(planner.plan(), OrderBy::kType))
      << "OrderBy should be eliminated (WITH renames n to m, tracked through Produce)";
}

// Test 19: Label-only index (no property index) should not trigger elimination
TYPED_TEST(OrderByIndexTest, LabelOnlyIndexNoElimination) {
  // MATCH (n:L) ORDER BY n.prop RETURN n
  // Only a label index exists, no property index — scan will be ScanAllByLabel, not ScanAllByLabelProperties.
  FakeDbAccessor dba;
  const auto label_name = "L";
  const auto label = dba.Label(label_name);
  const auto property = PROPERTY_PAIR(dba, "prop");
  dba.SetIndexCount(label, 1);
  // No property index set — only label index

  auto *query = QUERY(SINGLE_QUERY(MATCH(PATTERN(NODE("n", label_name))),
                                   RETURN("n", ORDER_BY(PROPERTY_LOOKUP(dba, "n", property.second)))));

  auto symbol_table = memgraph::query::MakeSymbolTable(query);
  auto planner = MakePlanner<TypeParam>(&dba, this->storage, symbol_table, query);

  EXPECT_TRUE(PlanContainsOp(planner.plan(), OrderBy::kType))
      << "OrderBy should NOT be eliminated (label-only index, no property index)";
}

// Test 20: WITH passes n through without renaming — elimination still applies (complement of Test 18)
TYPED_TEST(OrderByIndexTest, WithoutRenamingAllowsElimination) {
  // MATCH (n:L) WHERE n.prop > 5 WITH n ORDER BY n.prop RETURN n
  FakeDbAccessor dba;
  const auto *const label_name = "L";
  const auto label = dba.Label(label_name);
  const auto property = PROPERTY_PAIR(dba, "prop");
  dba.SetIndexCount(label, 1);
  dba.SetIndexCount(label, property.second, 1);

  auto *ident_n = IDENT("n");
  auto *match_clause = MATCH(PATTERN(NODE("n", label_name)));
  match_clause->where_ = WHERE(GREATER(PROPERTY_LOOKUP(dba, "n", property.second), LITERAL(5)));
  auto *with_clause = WITH(ident_n, AS("n"));
  auto *return_clause = RETURN("n", ORDER_BY(PROPERTY_LOOKUP(dba, "n", property.second)));
  auto *single = this->storage.template Create<memgraph::query::SingleQuery>();
  single->clauses_.push_back(match_clause);
  single->clauses_.push_back(with_clause);
  single->clauses_.push_back(return_clause);
  auto *query = QUERY(single);

  auto symbol_table = memgraph::query::MakeSymbolTable(query);
  auto planner = MakePlanner<TypeParam>(&dba, this->storage, symbol_table, query);

  EXPECT_FALSE(PlanContainsOp(planner.plan(), OrderBy::kType))
      << "OrderBy should be eliminated (WITH passes n through without renaming)";
}

// Test 21: ORDER BY n.b, n.a with index (a, b) — reversed column order doesn't match
TYPED_TEST(OrderByIndexTest, ReverseColumnOrderNotEliminated) {
  // MATCH (n:L) WHERE n.a > 5 ORDER BY n.b, n.a RETURN n
  FakeDbAccessor dba;
  const auto *label_name = "L";
  const auto label = dba.Label(label_name);
  const auto prop_a = PROPERTY_PAIR(dba, "a");
  const auto prop_b = PROPERTY_PAIR(dba, "b");
  dba.SetIndexCount(label, 1);
  std::vector<ms::PropertyPath> composite_props{ms::PropertyPath{prop_a.second}, ms::PropertyPath{prop_b.second}};
  dba.SetIndexCount(label, std::span{composite_props}, 1);

  auto *query = QUERY(SINGLE_QUERY(
      MATCH(PATTERN(NODE("n", label_name))),
      WHERE(GREATER(PROPERTY_LOOKUP(dba, "n", prop_a.second), LITERAL(5))),
      RETURN("n", ORDER_BY(PROPERTY_LOOKUP(dba, "n", prop_b.second), PROPERTY_LOOKUP(dba, "n", prop_a.second)))));

  auto symbol_table = memgraph::query::MakeSymbolTable(query);
  auto planner = MakePlanner<TypeParam>(&dba, this->storage, symbol_table, query);

  EXPECT_TRUE(PlanContainsOp(planner.plan(), OrderBy::kType))
      << "OrderBy should NOT be eliminated (ORDER BY b, a does not match index order a, b)";
}

// Test 22: Composite index (a, b), range on a, ORDER BY b — should NOT eliminate.
// The range on a means b values are interleaved across different a values,
// so the index does not provide global ordering on b alone.
TYPED_TEST(OrderByIndexTest, CompositeRangeOnFirstOrderBySecond) {
  // MATCH (n:L) WHERE n.a > 5 ORDER BY n.b RETURN n
  FakeDbAccessor dba;
  const auto *const label_name = "L";
  const auto label = dba.Label(label_name);
  const auto prop_a = PROPERTY_PAIR(dba, "a");
  const auto prop_b = PROPERTY_PAIR(dba, "b");
  dba.SetIndexCount(label, 1);
  std::vector<ms::PropertyPath> composite_props{ms::PropertyPath{prop_a.second}, ms::PropertyPath{prop_b.second}};
  dba.SetIndexCount(label, std::span{composite_props}, 1);

  auto *query = QUERY(SINGLE_QUERY(MATCH(PATTERN(NODE("n", label_name))),
                                   WHERE(GREATER(PROPERTY_LOOKUP(dba, "n", prop_a.second), LITERAL(5))),
                                   RETURN("n", ORDER_BY(PROPERTY_LOOKUP(dba, "n", prop_b.second)))));

  auto symbol_table = memgraph::query::MakeSymbolTable(query);
  auto planner = MakePlanner<TypeParam>(&dba, this->storage, symbol_table, query);

  EXPECT_TRUE(PlanContainsOp(planner.plan(), OrderBy::kType))
      << "OrderBy should NOT be eliminated (range on a, ORDER BY b — b not globally sorted)";
}

// Test 23: ORDER BY has more columns than composite index — ORDER BY n.a, n.b, n.c with index (a, b).
// The index only covers the first two columns; it cannot guarantee order on c.
// TODO: Could partially eliminate — keep OrderBy only on n.c since (a, b) prefix is index-sorted.
TYPED_TEST(OrderByIndexTest, CompositeIndexPartialCoverage) {
  // MATCH (n:L) WHERE n.a > 5 ORDER BY n.a, n.b, n.c RETURN n
  FakeDbAccessor dba;
  const auto *const label_name = "L";
  const auto label = dba.Label(label_name);
  const auto prop_a = PROPERTY_PAIR(dba, "a");
  const auto prop_b = PROPERTY_PAIR(dba, "b");
  const auto prop_c = PROPERTY_PAIR(dba, "c");
  dba.SetIndexCount(label, 1);
  std::vector<ms::PropertyPath> composite_props{ms::PropertyPath{prop_a.second}, ms::PropertyPath{prop_b.second}};
  dba.SetIndexCount(label, std::span{composite_props}, 1);

  auto *query = QUERY(SINGLE_QUERY(MATCH(PATTERN(NODE("n", label_name))),
                                   WHERE(GREATER(PROPERTY_LOOKUP(dba, "n", prop_a.second), LITERAL(5))),
                                   RETURN("n",
                                          ORDER_BY(PROPERTY_LOOKUP(dba, "n", prop_a.second),
                                                   PROPERTY_LOOKUP(dba, "n", prop_b.second),
                                                   PROPERTY_LOOKUP(dba, "n", prop_c.second)))));

  auto symbol_table = memgraph::query::MakeSymbolTable(query);
  auto planner = MakePlanner<TypeParam>(&dba, this->storage, symbol_table, query);

  EXPECT_TRUE(PlanContainsOp(planner.plan(), OrderBy::kType))
      << "OrderBy should NOT be eliminated (ORDER BY n.a, n.b, n.c but index only covers (a, b))";
}

// Test 24: RETURN n AS m ORDER BY n.prop — ORDER BY uses input scope name (dual-scope semantics)
TYPED_TEST(OrderByIndexTest, ReturnRenameOrderByInputScope) {
  // MATCH (n:L) WHERE n.prop > 5 RETURN n AS m ORDER BY n.prop
  FakeDbAccessor dba;
  const auto *const label_name = "L";
  const auto label = dba.Label(label_name);
  const auto property = PROPERTY_PAIR(dba, "prop");
  dba.SetIndexCount(label, 1);
  dba.SetIndexCount(label, property.second, 1);

  auto *query = QUERY(SINGLE_QUERY(MATCH(PATTERN(NODE("n", label_name))),
                                   WHERE(GREATER(PROPERTY_LOOKUP(dba, "n", property.second), LITERAL(5))),
                                   RETURN(IDENT("n"), AS("m"), ORDER_BY(PROPERTY_LOOKUP(dba, "n", property.second)))));

  auto symbol_table = memgraph::query::MakeSymbolTable(query);
  auto planner = MakePlanner<TypeParam>(&dba, this->storage, symbol_table, query);

  EXPECT_FALSE(PlanContainsOp(planner.plan(), OrderBy::kType))
      << "OrderBy should be eliminated (RETURN n AS m, ORDER BY n.prop — input scope name matches scan)";
}

// Test 25: RETURN n AS m ORDER BY m.prop — ORDER BY uses output scope name (dual-scope semantics)
TYPED_TEST(OrderByIndexTest, ReturnRenameOrderByOutputScope) {
  // MATCH (n:L) WHERE n.prop > 5 RETURN n AS m ORDER BY m.prop
  FakeDbAccessor dba;
  const auto *const label_name = "L";
  const auto label = dba.Label(label_name);
  const auto property = PROPERTY_PAIR(dba, "prop");
  dba.SetIndexCount(label, 1);
  dba.SetIndexCount(label, property.second, 1);

  auto *query = QUERY(SINGLE_QUERY(MATCH(PATTERN(NODE("n", label_name))),
                                   WHERE(GREATER(PROPERTY_LOOKUP(dba, "n", property.second), LITERAL(5))),
                                   RETURN(IDENT("n"), AS("m"), ORDER_BY(PROPERTY_LOOKUP(dba, "m", property.second)))));

  auto symbol_table = memgraph::query::MakeSymbolTable(query);
  auto planner = MakePlanner<TypeParam>(&dba, this->storage, symbol_table, query);

  EXPECT_FALSE(PlanContainsOp(planner.plan(), OrderBy::kType))
      << "OrderBy should be eliminated (RETURN n AS m, ORDER BY m.prop — output scope name tracked through Produce)";
}

// Test 26: Equality on a, equality on b, ORDER BY c — double equality skip with index (a, b, c)
TYPED_TEST(OrderByIndexTest, DoubleEqualitySkipEliminated) {
  // MATCH (n:L) WHERE n.a = 1 AND n.b = 2 ORDER BY n.c RETURN n
  FakeDbAccessor dba;
  const auto *const label_name = "L";
  const auto label = dba.Label(label_name);
  const auto prop_a = PROPERTY_PAIR(dba, "a");
  const auto prop_b = PROPERTY_PAIR(dba, "b");
  const auto prop_c = PROPERTY_PAIR(dba, "c");
  dba.SetIndexCount(label, 1);
  std::vector<ms::PropertyPath> composite_props{
      ms::PropertyPath{prop_a.second}, ms::PropertyPath{prop_b.second}, ms::PropertyPath{prop_c.second}};
  dba.SetIndexCount(label, std::span{composite_props}, 1);

  auto *query = QUERY(SINGLE_QUERY(MATCH(PATTERN(NODE("n", label_name))),
                                   WHERE(AND(EQ(PROPERTY_LOOKUP(dba, "n", prop_a.second), LITERAL(1)),
                                             EQ(PROPERTY_LOOKUP(dba, "n", prop_b.second), LITERAL(2)))),
                                   RETURN("n", ORDER_BY(PROPERTY_LOOKUP(dba, "n", prop_c.second)))));

  auto symbol_table = memgraph::query::MakeSymbolTable(query);
  auto planner = MakePlanner<TypeParam>(&dba, this->storage, symbol_table, query);

  EXPECT_TRUE(PlanContainsOp(planner.plan(), ScanAllByLabelProperties::kType));
  EXPECT_FALSE(PlanContainsOp(planner.plan(), OrderBy::kType))
      << "OrderBy should be eliminated (a and b equality-pinned, ORDER BY c follows in index)";
}

// Test 27: Equality on a, range on b, ORDER BY c — range on b creates a gap, should NOT eliminate
TYPED_TEST(OrderByIndexTest, EqualityThenRangeGapNotEliminated) {
  // MATCH (n:L) WHERE n.a = 1 AND n.b > 2 ORDER BY n.c RETURN n
  FakeDbAccessor dba;
  const auto *const label_name = "L";
  const auto label = dba.Label(label_name);
  const auto prop_a = PROPERTY_PAIR(dba, "a");
  const auto prop_b = PROPERTY_PAIR(dba, "b");
  const auto prop_c = PROPERTY_PAIR(dba, "c");
  dba.SetIndexCount(label, 1);
  std::vector<ms::PropertyPath> composite_props{
      ms::PropertyPath{prop_a.second}, ms::PropertyPath{prop_b.second}, ms::PropertyPath{prop_c.second}};
  dba.SetIndexCount(label, std::span{composite_props}, 1);

  auto *query = QUERY(SINGLE_QUERY(MATCH(PATTERN(NODE("n", label_name))),
                                   WHERE(AND(EQ(PROPERTY_LOOKUP(dba, "n", prop_a.second), LITERAL(1)),
                                             GREATER(PROPERTY_LOOKUP(dba, "n", prop_b.second), LITERAL(2)))),
                                   RETURN("n", ORDER_BY(PROPERTY_LOOKUP(dba, "n", prop_c.second)))));

  auto symbol_table = memgraph::query::MakeSymbolTable(query);
  auto planner = MakePlanner<TypeParam>(&dba, this->storage, symbol_table, query);

  EXPECT_TRUE(PlanContainsOp(planner.plan(), ScanAllByLabelProperties::kType));
  EXPECT_TRUE(PlanContainsOp(planner.plan(), OrderBy::kType))
      << "OrderBy should NOT be eliminated (b is range-filtered, not pinned — gap before c)";
}

// Test 28: IN on first column, ORDER BY second — IN is multi-valued, b not globally sorted
TYPED_TEST(OrderByIndexTest, InFilterNotEqualityPinned) {
  // MATCH (n:L) WHERE n.a IN [1, 3] ORDER BY n.b RETURN n
  FakeDbAccessor dba;
  const auto *const label_name = "L";
  const auto label = dba.Label(label_name);
  const auto prop_a = PROPERTY_PAIR(dba, "a");
  const auto prop_b = PROPERTY_PAIR(dba, "b");
  dba.SetIndexCount(label, 1);
  std::vector<ms::PropertyPath> composite_props{ms::PropertyPath{prop_a.second}, ms::PropertyPath{prop_b.second}};
  dba.SetIndexCount(label, std::span{composite_props}, 1);

  auto *query =
      QUERY(SINGLE_QUERY(MATCH(PATTERN(NODE("n", label_name))),
                         WHERE(IN_LIST(PROPERTY_LOOKUP(dba, "n", prop_a.second), LIST(LITERAL(1), LITERAL(3)))),
                         RETURN("n", ORDER_BY(PROPERTY_LOOKUP(dba, "n", prop_b.second)))));

  auto symbol_table = memgraph::query::MakeSymbolTable(query);
  auto planner = MakePlanner<TypeParam>(&dba, this->storage, symbol_table, query);

  EXPECT_TRUE(PlanContainsOp(planner.plan(), OrderBy::kType))
      << "OrderBy should NOT be eliminated (IN is multi-valued, b not globally sorted)";
}

// Test 29: IN on first column, ORDER BY first — Unwind iterates in list order, not index order
TYPED_TEST(OrderByIndexTest, InFilterOrderBySameColumn) {
  // MATCH (n:L) WHERE n.a IN [3, 1] ORDER BY n.a RETURN n
  // Unwind yields 3, then 1 — scan returns a=3 rows first, then a=1. Not sorted by a.
  FakeDbAccessor dba;
  const auto *const label_name = "L";
  const auto label = dba.Label(label_name);
  const auto prop_a = PROPERTY_PAIR(dba, "a");
  const auto prop_b = PROPERTY_PAIR(dba, "b");
  dba.SetIndexCount(label, 1);
  std::vector<ms::PropertyPath> composite_props{ms::PropertyPath{prop_a.second}, ms::PropertyPath{prop_b.second}};
  dba.SetIndexCount(label, std::span{composite_props}, 1);

  auto *query =
      QUERY(SINGLE_QUERY(MATCH(PATTERN(NODE("n", label_name))),
                         WHERE(IN_LIST(PROPERTY_LOOKUP(dba, "n", prop_a.second), LIST(LITERAL(3), LITERAL(1)))),
                         RETURN("n", ORDER_BY(PROPERTY_LOOKUP(dba, "n", prop_a.second)))));

  auto symbol_table = memgraph::query::MakeSymbolTable(query);
  auto planner = MakePlanner<TypeParam>(&dba, this->storage, symbol_table, query);

  // The Unwind iterates the IN list in the given order. The scan returns rows for each value
  // in that order — not in index-sorted order. So ORDER BY cannot be safely eliminated.
  EXPECT_TRUE(PlanContainsOp(planner.plan(), OrderBy::kType))
      << "OrderBy should NOT be eliminated (IN list order is not guaranteed sorted)";
}

// Test 30: IN on first column, ORDER BY first and second — a is sorted but b is only sorted within each a-group
TYPED_TEST(OrderByIndexTest, InFilterOrderByBothColumns) {
  // MATCH (n:L) WHERE n.a IN [1, 3] ORDER BY n.a, n.b RETURN n
  FakeDbAccessor dba;
  const auto *const label_name = "L";
  const auto label = dba.Label(label_name);
  const auto prop_a = PROPERTY_PAIR(dba, "a");
  const auto prop_b = PROPERTY_PAIR(dba, "b");
  dba.SetIndexCount(label, 1);
  std::vector<ms::PropertyPath> composite_props{ms::PropertyPath{prop_a.second}, ms::PropertyPath{prop_b.second}};
  dba.SetIndexCount(label, std::span{composite_props}, 1);

  auto *query = QUERY(SINGLE_QUERY(
      MATCH(PATTERN(NODE("n", label_name))),
      WHERE(IN_LIST(PROPERTY_LOOKUP(dba, "n", prop_a.second), LIST(LITERAL(1), LITERAL(3)))),
      RETURN("n", ORDER_BY(PROPERTY_LOOKUP(dba, "n", prop_a.second), PROPERTY_LOOKUP(dba, "n", prop_b.second)))));

  auto symbol_table = memgraph::query::MakeSymbolTable(query);
  auto planner = MakePlanner<TypeParam>(&dba, this->storage, symbol_table, query);

  // With IN rewrite: Unwind [1,3] → scan with a=<val>. Each scan returns (a, b) sorted.
  // Since Unwind iterates in list order [1, 3], and within each a-value b is sorted,
  // the combined output is (1, b1), (1, b2), ..., (3, b1), (3, b2), ... which IS sorted by (a, b).
  // However, this only works if the Unwind iterates in sorted order, which is NOT guaranteed
  // (the list [3, 1] would break it). Conservative: keep OrderBy.
  EXPECT_TRUE(PlanContainsOp(planner.plan(), OrderBy::kType))
      << "OrderBy should NOT be eliminated (IN list order is not guaranteed sorted)";
}

// Test 31: User-written UNWIND between OrderBy and ScanAll — Unwind is order-preserving
// for the scan symbol, so elimination should still fire.
TYPED_TEST(OrderByIndexTest, UserUnwindPreservesOrder) {
  // MATCH (n:L) WHERE n.prop > 5 UNWIND [1, 2, 3] AS x RETURN n, x ORDER BY n.prop
  FakeDbAccessor dba;
  const auto *const label_name = "L";
  const auto label = dba.Label(label_name);
  const auto property = PROPERTY_PAIR(dba, "prop");
  dba.SetIndexCount(label, 1);
  dba.SetIndexCount(label, property.second, 1);

  auto *match_clause = MATCH(PATTERN(NODE("n", label_name)));
  match_clause->where_ = WHERE(GREATER(PROPERTY_LOOKUP(dba, "n", property.second), LITERAL(5)));
  auto *unwind_clause = UNWIND(LIST(LITERAL(1), LITERAL(2), LITERAL(3)), AS("x"));
  auto *return_clause = RETURN("n", "x", ORDER_BY(PROPERTY_LOOKUP(dba, "n", property.second)));
  auto *single = this->storage.template Create<memgraph::query::SingleQuery>();
  single->clauses_.push_back(match_clause);
  single->clauses_.push_back(unwind_clause);
  single->clauses_.push_back(return_clause);
  auto *query = QUERY(single);

  auto symbol_table = memgraph::query::MakeSymbolTable(query);
  auto planner = MakePlanner<TypeParam>(&dba, this->storage, symbol_table, query);

  // Unwind multiplies rows (1:N) but preserves the relative ordering of n.prop.
  // The ORDER BY is on the scan symbol n.prop, not on x, so elimination is safe.
  EXPECT_FALSE(PlanContainsOp(planner.plan(), OrderBy::kType))
      << "OrderBy should be eliminated (user UNWIND is order-preserving for scan symbol)";
}

// Test 32: WITH n.prop AS a ORDER BY a — bare Identifier resolved through Produce.
TYPED_TEST(OrderByIndexTest, WithPropertyAliasEliminated) {
  // MATCH (n:L) WHERE n.prop > 5 WITH n.prop AS a RETURN a ORDER BY a
  FakeDbAccessor dba;
  const auto *const label_name = "L";
  const auto label = dba.Label(label_name);
  const auto property = PROPERTY_PAIR(dba, "prop");
  dba.SetIndexCount(label, 1);
  dba.SetIndexCount(label, property.second, 1);

  auto *match_clause = MATCH(PATTERN(NODE("n", label_name)));
  match_clause->where_ = WHERE(GREATER(PROPERTY_LOOKUP(dba, "n", property.second), LITERAL(5)));
  auto *with_clause = WITH(PROPERTY_LOOKUP(dba, "n", property.second), AS("a"));
  auto *return_clause = RETURN("a", ORDER_BY(IDENT("a")));
  auto *single = this->storage.template Create<memgraph::query::SingleQuery>();
  single->clauses_.push_back(match_clause);
  single->clauses_.push_back(with_clause);
  single->clauses_.push_back(return_clause);
  auto *query = QUERY(single);

  auto symbol_table = memgraph::query::MakeSymbolTable(query);
  auto planner = MakePlanner<TypeParam>(&dba, this->storage, symbol_table, query);

  EXPECT_FALSE(PlanContainsOp(planner.plan(), OrderBy::kType))
      << "OrderBy should be eliminated (WITH n.prop AS a, ORDER BY a — alias resolved through Produce)";
}

// Test 33: RETURN n.prop AS a ORDER BY a — alias resolved through RETURN Produce above OrderBy.
TYPED_TEST(OrderByIndexTest, ReturnPropertyAliasEliminated) {
  // MATCH (n:L) WHERE n.prop > 5 RETURN n.prop AS a ORDER BY a
  FakeDbAccessor dba;
  const auto *const label_name = "L";
  const auto label = dba.Label(label_name);
  const auto property = PROPERTY_PAIR(dba, "prop");
  dba.SetIndexCount(label, 1);
  dba.SetIndexCount(label, property.second, 1);

  auto *query = QUERY(SINGLE_QUERY(MATCH(PATTERN(NODE("n", label_name))),
                                   WHERE(GREATER(PROPERTY_LOOKUP(dba, "n", property.second), LITERAL(5))),
                                   RETURN(PROPERTY_LOOKUP(dba, "n", property.second), AS("a"), ORDER_BY(IDENT("a")))));

  auto symbol_table = memgraph::query::MakeSymbolTable(query);
  auto planner = MakePlanner<TypeParam>(&dba, this->storage, symbol_table, query);

  EXPECT_FALSE(PlanContainsOp(planner.plan(), OrderBy::kType))
      << "OrderBy should be eliminated (RETURN n.prop AS a ORDER BY a — resolved through RETURN Produce)";
}

// Test 34: Composite index WITH n.a AS a, n.b AS b ORDER BY a, b — both aliases resolved, order matches.
TYPED_TEST(OrderByIndexTest, CompositeWithPropertyAliasEliminated) {
  // MATCH (n:L) WHERE n.a > 0 WITH n.a AS a, n.b AS b RETURN a, b ORDER BY a, b
  FakeDbAccessor dba;
  const auto *const label_name = "L";
  const auto label = dba.Label(label_name);
  const auto prop_a = PROPERTY_PAIR(dba, "a");
  const auto prop_b = PROPERTY_PAIR(dba, "b");
  dba.SetIndexCount(label, 1);
  std::vector<ms::PropertyPath> composite_props{ms::PropertyPath{prop_a.second}, ms::PropertyPath{prop_b.second}};
  dba.SetIndexCount(label, std::span{composite_props}, 1);

  auto *match_clause = MATCH(PATTERN(NODE("n", label_name)));
  match_clause->where_ = WHERE(GREATER(PROPERTY_LOOKUP(dba, "n", prop_a.second), LITERAL(0)));
  auto *with_clause =
      WITH(PROPERTY_LOOKUP(dba, "n", prop_a.second), AS("a"), PROPERTY_LOOKUP(dba, "n", prop_b.second), AS("b"));
  auto *return_clause = RETURN("a", "b", ORDER_BY(IDENT("a"), IDENT("b")));
  auto *single = this->storage.template Create<memgraph::query::SingleQuery>();
  single->clauses_.push_back(match_clause);
  single->clauses_.push_back(with_clause);
  single->clauses_.push_back(return_clause);
  auto *query = QUERY(single);

  auto symbol_table = memgraph::query::MakeSymbolTable(query);
  auto planner = MakePlanner<TypeParam>(&dba, this->storage, symbol_table, query);

  EXPECT_FALSE(PlanContainsOp(planner.plan(), OrderBy::kType))
      << "OrderBy should be eliminated (composite index (a,b), WITH n.a AS a, n.b AS b, ORDER BY a, b)";
}

// Test 35: Composite index WITH n.b AS b, n.a AS a ORDER BY b, a — wrong order, not eliminated.
TYPED_TEST(OrderByIndexTest, CompositeWithPropertyAliasWrongOrderNotEliminated) {
  // MATCH (n:L) WHERE n.a > 0 WITH n.b AS b, n.a AS a RETURN b, a ORDER BY b, a
  FakeDbAccessor dba;
  const auto *const label_name = "L";
  const auto label = dba.Label(label_name);
  const auto prop_a = PROPERTY_PAIR(dba, "a");
  const auto prop_b = PROPERTY_PAIR(dba, "b");
  dba.SetIndexCount(label, 1);
  std::vector<ms::PropertyPath> composite_props{ms::PropertyPath{prop_a.second}, ms::PropertyPath{prop_b.second}};
  dba.SetIndexCount(label, std::span{composite_props}, 1);

  auto *match_clause = MATCH(PATTERN(NODE("n", label_name)));
  match_clause->where_ = WHERE(GREATER(PROPERTY_LOOKUP(dba, "n", prop_a.second), LITERAL(0)));
  auto *with_clause =
      WITH(PROPERTY_LOOKUP(dba, "n", prop_b.second), AS("b"), PROPERTY_LOOKUP(dba, "n", prop_a.second), AS("a"));
  auto *return_clause = RETURN("b", "a", ORDER_BY(IDENT("b"), IDENT("a")));
  auto *single = this->storage.template Create<memgraph::query::SingleQuery>();
  single->clauses_.push_back(match_clause);
  single->clauses_.push_back(with_clause);
  single->clauses_.push_back(return_clause);
  auto *query = QUERY(single);

  auto symbol_table = memgraph::query::MakeSymbolTable(query);
  auto planner = MakePlanner<TypeParam>(&dba, this->storage, symbol_table, query);

  EXPECT_TRUE(PlanContainsOp(planner.plan(), OrderBy::kType))
      << "OrderBy should NOT be eliminated (ORDER BY b, a but index is (a, b))";
}

// Test 36: WITH n.a AS x ORDER BY x — different alias name, still resolved.
TYPED_TEST(OrderByIndexTest, WithPropertyDifferentAliasEliminated) {
  // MATCH (n:L) WHERE n.a > 0 WITH n.a AS x RETURN x ORDER BY x
  FakeDbAccessor dba;
  const auto *const label_name = "L";
  const auto label = dba.Label(label_name);
  const auto prop_a = PROPERTY_PAIR(dba, "a");
  dba.SetIndexCount(label, 1);
  dba.SetIndexCount(label, prop_a.second, 1);

  auto *match_clause = MATCH(PATTERN(NODE("n", label_name)));
  match_clause->where_ = WHERE(GREATER(PROPERTY_LOOKUP(dba, "n", prop_a.second), LITERAL(0)));
  auto *with_clause = WITH(PROPERTY_LOOKUP(dba, "n", prop_a.second), AS("x"));
  auto *return_clause = RETURN("x", ORDER_BY(IDENT("x")));
  auto *single = this->storage.template Create<memgraph::query::SingleQuery>();
  single->clauses_.push_back(match_clause);
  single->clauses_.push_back(with_clause);
  single->clauses_.push_back(return_clause);
  auto *query = QUERY(single);

  auto symbol_table = memgraph::query::MakeSymbolTable(query);
  auto planner = MakePlanner<TypeParam>(&dba, this->storage, symbol_table, query);

  EXPECT_FALSE(PlanContainsOp(planner.plan(), OrderBy::kType))
      << "OrderBy should be eliminated (WITH n.a AS x ORDER BY x — alias name doesn't matter)";
}

// Test 37: Equality-pinned skip with alias — WHERE a = 5 ORDER BY b, both through WITH.
TYPED_TEST(OrderByIndexTest, EqualityPinnedWithPropertyAlias) {
  // MATCH (n:L) WHERE n.a = 5 WITH n.a AS a, n.b AS b RETURN a, b ORDER BY b
  FakeDbAccessor dba;
  const auto *const label_name = "L";
  const auto label = dba.Label(label_name);
  const auto prop_a = PROPERTY_PAIR(dba, "a");
  const auto prop_b = PROPERTY_PAIR(dba, "b");
  dba.SetIndexCount(label, 1);
  std::vector<ms::PropertyPath> composite_props{ms::PropertyPath{prop_a.second}, ms::PropertyPath{prop_b.second}};
  dba.SetIndexCount(label, std::span{composite_props}, 1);

  auto *match_clause = MATCH(PATTERN(NODE("n", label_name)));
  match_clause->where_ = WHERE(EQ(PROPERTY_LOOKUP(dba, "n", prop_a.second), LITERAL(5)));
  auto *with_clause =
      WITH(PROPERTY_LOOKUP(dba, "n", prop_a.second), AS("a"), PROPERTY_LOOKUP(dba, "n", prop_b.second), AS("b"));
  auto *return_clause = RETURN("a", "b", ORDER_BY(IDENT("b")));
  auto *single = this->storage.template Create<memgraph::query::SingleQuery>();
  single->clauses_.push_back(match_clause);
  single->clauses_.push_back(with_clause);
  single->clauses_.push_back(return_clause);
  auto *query = QUERY(single);

  auto symbol_table = memgraph::query::MakeSymbolTable(query);
  auto planner = MakePlanner<TypeParam>(&dba, this->storage, symbol_table, query);

  EXPECT_FALSE(PlanContainsOp(planner.plan(), OrderBy::kType))
      << "OrderBy should be eliminated (equality-pinned a, ORDER BY b through alias)";
}

// Test 38: WITH n.a + 1 AS a ORDER BY a — computed expression, not a direct property. Not eliminated.
TYPED_TEST(OrderByIndexTest, ComputedExpressionAliasNotEliminated) {
  // MATCH (n:L) WHERE n.a > 0 WITH n.a + 1 AS a RETURN a ORDER BY a
  FakeDbAccessor dba;
  const auto *const label_name = "L";
  const auto label = dba.Label(label_name);
  const auto prop_a = PROPERTY_PAIR(dba, "a");
  dba.SetIndexCount(label, 1);
  dba.SetIndexCount(label, prop_a.second, 1);

  auto *match_clause = MATCH(PATTERN(NODE("n", label_name)));
  match_clause->where_ = WHERE(GREATER(PROPERTY_LOOKUP(dba, "n", prop_a.second), LITERAL(0)));
  auto *with_clause = WITH(ADD(PROPERTY_LOOKUP(dba, "n", prop_a.second), LITERAL(1)), AS("a"));
  auto *return_clause = RETURN("a", ORDER_BY(IDENT("a")));
  auto *single = this->storage.template Create<memgraph::query::SingleQuery>();
  single->clauses_.push_back(match_clause);
  single->clauses_.push_back(with_clause);
  single->clauses_.push_back(return_clause);
  auto *query = QUERY(single);

  auto symbol_table = memgraph::query::MakeSymbolTable(query);
  auto planner = MakePlanner<TypeParam>(&dba, this->storage, symbol_table, query);

  EXPECT_TRUE(PlanContainsOp(planner.plan(), OrderBy::kType))
      << "OrderBy should NOT be eliminated (computed expression n.a + 1, not a direct property)";
}

// Test 39: WITH n AS m WITH m.a AS a ORDER BY a — rename chain before projection.
TYPED_TEST(OrderByIndexTest, RenameChainBeforeProjectionEliminated) {
  // MATCH (n:L) WHERE n.a > 0 WITH n AS m WITH m.a AS a RETURN a ORDER BY a
  FakeDbAccessor dba;
  const auto *const label_name = "L";
  const auto label = dba.Label(label_name);
  const auto prop_a = PROPERTY_PAIR(dba, "a");
  dba.SetIndexCount(label, 1);
  dba.SetIndexCount(label, prop_a.second, 1);

  auto *ident_n = IDENT("n");
  auto *match_clause = MATCH(PATTERN(NODE("n", label_name)));
  match_clause->where_ = WHERE(GREATER(PROPERTY_LOOKUP(dba, "n", prop_a.second), LITERAL(0)));
  auto *with1_clause = WITH(ident_n, AS("m"));
  auto *with2_clause = WITH(PROPERTY_LOOKUP(dba, "m", prop_a.second), AS("a"));
  auto *return_clause = RETURN("a", ORDER_BY(IDENT("a")));
  auto *single = this->storage.template Create<memgraph::query::SingleQuery>();
  single->clauses_.push_back(match_clause);
  single->clauses_.push_back(with1_clause);
  single->clauses_.push_back(with2_clause);
  single->clauses_.push_back(return_clause);
  auto *query = QUERY(single);

  auto symbol_table = memgraph::query::MakeSymbolTable(query);
  auto planner = MakePlanner<TypeParam>(&dba, this->storage, symbol_table, query);

  EXPECT_FALSE(PlanContainsOp(planner.plan(), OrderBy::kType))
      << "OrderBy should be eliminated (rename n->m then m.a AS a — tracked through chain)";
}

// Test 40: Composite rename chain — WITH n AS m WITH m.a AS a, m.b AS b ORDER BY a, b.
TYPED_TEST(OrderByIndexTest, CompositeRenameChainEliminated) {
  // MATCH (n:L) WHERE n.a > 0 WITH n AS m WITH m.a AS a, m.b AS b RETURN a, b ORDER BY a, b
  FakeDbAccessor dba;
  const auto *const label_name = "L";
  const auto label = dba.Label(label_name);
  const auto prop_a = PROPERTY_PAIR(dba, "a");
  const auto prop_b = PROPERTY_PAIR(dba, "b");
  dba.SetIndexCount(label, 1);
  std::vector<ms::PropertyPath> composite_props{ms::PropertyPath{prop_a.second}, ms::PropertyPath{prop_b.second}};
  dba.SetIndexCount(label, std::span{composite_props}, 1);

  auto *ident_n = IDENT("n");
  auto *match_clause = MATCH(PATTERN(NODE("n", label_name)));
  match_clause->where_ = WHERE(GREATER(PROPERTY_LOOKUP(dba, "n", prop_a.second), LITERAL(0)));
  auto *with1_clause = WITH(ident_n, AS("m"));
  auto *with2_clause =
      WITH(PROPERTY_LOOKUP(dba, "m", prop_a.second), AS("a"), PROPERTY_LOOKUP(dba, "m", prop_b.second), AS("b"));
  auto *return_clause = RETURN("a", "b", ORDER_BY(IDENT("a"), IDENT("b")));
  auto *single = this->storage.template Create<memgraph::query::SingleQuery>();
  single->clauses_.push_back(match_clause);
  single->clauses_.push_back(with1_clause);
  single->clauses_.push_back(with2_clause);
  single->clauses_.push_back(return_clause);
  auto *query = QUERY(single);

  auto symbol_table = memgraph::query::MakeSymbolTable(query);
  auto planner = MakePlanner<TypeParam>(&dba, this->storage, symbol_table, query);

  EXPECT_FALSE(PlanContainsOp(planner.plan(), OrderBy::kType))
      << "OrderBy should be eliminated (rename n->m then composite m.a, m.b — tracked through chain)";
}

// Test 41: RETURN DISTINCT n.prop AS a ORDER BY a — Distinct between Produce and OrderBy.
TYPED_TEST(OrderByIndexTest, DistinctWithAliasEliminated) {
  // MATCH (n:L) WHERE n.prop > 5 RETURN DISTINCT n.prop AS a ORDER BY a
  FakeDbAccessor dba;
  const auto *const label_name = "L";
  const auto label = dba.Label(label_name);
  const auto property = PROPERTY_PAIR(dba, "prop");
  dba.SetIndexCount(label, 1);
  dba.SetIndexCount(label, property.second, 1);

  auto *query =
      QUERY(SINGLE_QUERY(MATCH(PATTERN(NODE("n", label_name))),
                         WHERE(GREATER(PROPERTY_LOOKUP(dba, "n", property.second), LITERAL(5))),
                         RETURN_DISTINCT(PROPERTY_LOOKUP(dba, "n", property.second), AS("a"), ORDER_BY(IDENT("a")))));

  auto symbol_table = memgraph::query::MakeSymbolTable(query);
  auto planner = MakePlanner<TypeParam>(&dba, this->storage, symbol_table, query);

  EXPECT_FALSE(PlanContainsOp(planner.plan(), OrderBy::kType))
      << "OrderBy should be eliminated (RETURN DISTINCT n.prop AS a ORDER BY a — Distinct skipped)";
}

}  // namespace
