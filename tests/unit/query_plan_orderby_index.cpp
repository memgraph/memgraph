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

// Test 3: Equality skip - WHERE n.a = 5 ORDER BY n.b with index (a, b)
TYPED_TEST(OrderByIndexTest, EqualitySkip) {
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
      << "OrderBy should be eliminated (equality skip on first index column)";
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

// Test 6: DESC rejected
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

// Test 7: Non-property expression rejected
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

// Test 8: No matching index - ORDER BY n.b with index on (a) only
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

// Test 9: ORDER BY superset - ORDER BY n.a, n.b with index on (a) only
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

// Test 10b: Expand with ORDER BY on the expanded symbol, not the scan symbol.
// Both n and m have property indexes, but the scan is on n and ORDER BY is on m.prop.
// The index scan provides order on n.prop, not m.prop — elimination must not fire.
TYPED_TEST(OrderByIndexTest, ExpandOrderByExpandedSymbol) {
  // MATCH (n:L)-[r]->(m:K) WHERE n.prop > 5 AND m.prop > 3 ORDER BY m.prop RETURN n, m
  FakeDbAccessor dba;
  const auto *const label_l = "L";
  const auto *const label_k = "K";
  const auto label_l_id = dba.Label(label_l);
  const auto label_k_id = dba.Label(label_k);
  const auto property = PROPERTY_PAIR(dba, "prop");
  dba.SetIndexCount(label_l_id, 1);
  dba.SetIndexCount(label_l_id, property.second, 1);
  dba.SetIndexCount(label_k_id, 1);
  dba.SetIndexCount(label_k_id, property.second, 1);

  auto *query = QUERY(SINGLE_QUERY(MATCH(PATTERN(NODE("n", label_l), EDGE("r", Direction::OUT), NODE("m", label_k))),
                                   WHERE(AND(GREATER(PROPERTY_LOOKUP(dba, "n", property.second), LITERAL(5)),
                                             GREATER(PROPERTY_LOOKUP(dba, "m", property.second), LITERAL(3)))),
                                   RETURN("n", "m", ORDER_BY(PROPERTY_LOOKUP(dba, "m", property.second)))));

  auto symbol_table = memgraph::query::MakeSymbolTable(query);
  auto planner = MakePlanner<TypeParam>(&dba, this->storage, symbol_table, query);

  EXPECT_TRUE(PlanContainsOp(planner.plan(), OrderBy::kType))
      << "OrderBy should NOT be eliminated (ORDER BY m.prop but index scan is on n)";
}

// Test 11: Cartesian blocks elimination
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

// Test 12: Aggregate blocks elimination
TYPED_TEST(OrderByIndexTest, AggregateBlocks) {
  // MATCH (n:L) WHERE n.prop > 5 RETURN n.prop AS p, count(*) AS c ORDER BY p
  // The Aggregate operator (hash grouping) is between OrderBy and ScanAll, blocking elimination.
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

// Test 13: Equality + range on second column - WHERE n.a = 5 AND n.b > 3 ORDER BY n.b with index (a, b)
TYPED_TEST(OrderByIndexTest, EqualityPlusRangeOnSecondColumn) {
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
      << "OrderBy should be eliminated (equality on a, range on b, ORDER BY b)";
}

// Test 14: Full composite ORDER BY - ORDER BY n.a, n.b with index (a, b) and range on a.
// Both ORDER BY columns match the full index, so elimination should apply.
TYPED_TEST(OrderByIndexTest, FullCompositeRangeOnFirst) {
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

// Test 15: Equality + range + ORDER BY both - WHERE n.a = 5 AND n.b > 3 ORDER BY n.a, n.b with index (a, b)
// Equality-pinned column is also in ORDER BY, both cursors should advance.
TYPED_TEST(OrderByIndexTest, EqualityPlusRangeOrderByBoth) {
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

// Test 16: Equality-only filter - WHERE n.prop = 5 ORDER BY n.prop with :L(prop) index
// With equality, all rows have the same property value, so ORDER BY is trivially satisfied.
TYPED_TEST(OrderByIndexTest, EqualityOnlyElimination) {
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

// Test 17: Multiple MATCH with different symbols - ORDER BY n.prop should not match m's scan
// MATCH (n:L) WHERE n.prop > 5 MATCH (m:L) WHERE m.prop > 3 ORDER BY n.prop RETURN n, m
// The second ScanAll (for m) should not falsely eliminate ORDER BY.
TYPED_TEST(OrderByIndexTest, MultiMatchDifferentSymbols) {
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

  // Two separate MATCH clauses produce a Cartesian product between n's and m's scans.
  // Cartesian is not order-preserving, so it blocks elimination even though n's scan
  // provides ordering on n.prop.
  EXPECT_TRUE(PlanContainsOp(planner.plan(), OrderBy::kType))
      << "OrderBy should NOT be eliminated (Cartesian between OrderBy and n's scan)";
}

// Test 18: WITH variable renaming blocks elimination
// MATCH (n:L) WHERE n.prop > 5 WITH n AS m ORDER BY m.prop RETURN m
// The WITH clause renames n to m, which means the OrderBy symbol (m) does not correspond
// to the scan symbol (n) through a simple identity mapping. ProduceRenamesVariable should block this.
TYPED_TEST(OrderByIndexTest, WithRenamingBlocks) {
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

  EXPECT_TRUE(PlanContainsOp(planner.plan(), OrderBy::kType))
      << "OrderBy should NOT be eliminated (WITH renames n to m)";
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

// Test 20: WITH without renaming still allows elimination.
// MATCH (n:L) WHERE n.prop > 5 WITH n ORDER BY n.prop RETURN n
// The WITH clause passes n through without renaming, so elimination should still fire.
// This complements Test 18 (WithRenamingBlocks) — same pattern but identity mapping.
TYPED_TEST(OrderByIndexTest, WithoutRenamingAllowsElimination) {
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

// Test 21: ORDER BY in reverse column order — ORDER BY n.b, n.a with index (a, b)
// The index sorts lexicographically by (a, b), but ORDER BY wants (b, a) — different order.
TYPED_TEST(OrderByIndexTest, ReverseColumnOrderNotEliminated) {
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

}  // namespace
