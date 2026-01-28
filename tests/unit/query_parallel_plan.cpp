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

#include "storage/v2/config.hpp"
#ifdef MG_ENTERPRISE
#include "query_plan_checker.hpp"

#include <iostream>
#include <list>
#include <memory>
#include <optional>
#include <set>
#include <sstream>
#include <tuple>
#include <typeinfo>
#include <unordered_map>
#include <unordered_set>
#include <variant>

#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include "query/exceptions.hpp"
#include "query/frontend/ast/ast.hpp"
#include "query/frontend/semantic/symbol_generator.hpp"
#include "query/frontend/semantic/symbol_table.hpp"
#include "query/plan/operator.hpp"
#include "query/plan/planner.hpp"

#include "query_common.hpp"
#include "utils/bound.hpp"

namespace memgraph::query {
::std::ostream &operator<<(::std::ostream &os, const Symbol &sym) {
  return os << "Symbol{\"" << sym.name() << "\" [" << sym.position() << "] " << Symbol::TypeToString(sym.type()) << "}";
}
}  // namespace memgraph::query

using namespace memgraph::query::plan;
using memgraph::query::AstStorage;
using memgraph::query::CypherUnion;
using memgraph::query::EdgeAtom;
using memgraph::query::SingleQuery;
using memgraph::query::Symbol;
using memgraph::query::SymbolTable;
using Type = memgraph::query::EdgeAtom::Type;
using Direction = memgraph::query::EdgeAtom::Direction;
using Bound = ScanAllByEdgeTypePropertyRange::Bound;
namespace ms = memgraph::storage;

namespace {

class Planner {
 public:
  template <class TDbAccessor>
  Planner(QueryParts query_parts, PlanningContext<TDbAccessor> context,
          const std::vector<memgraph::query::IndexHint> &index_hints) {
    memgraph::query::Parameters parameters;
    PostProcessor post_processor(parameters, index_hints, context.db);
    plan_ = MakeLogicalPlanForSingleQuery<RuleBasedPlanner>(query_parts, &context);
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
auto CheckPlan(memgraph::query::CypherQuery *query, AstStorage &storage, TChecker... checker) {
  auto symbol_table = memgraph::query::MakeSymbolTable(query);
  FakeDbAccessor dba;
  auto planner = MakePlanner<TPlanner>(&dba, storage, symbol_table, query);
  CheckPlan(planner.plan(), symbol_table, checker...);
}

template <class T>
class TestPlanner : public ::testing::Test {
 public:
  AstStorage storage;
};

using PlannerTypes = ::testing::Types<Planner>;

TYPED_TEST_SUITE(TestPlanner, PlannerTypes);

namespace {
struct LicenseWrapper {
  LicenseWrapper() {
    memgraph::license::global_license_checker.EnableTesting(memgraph::license::LicenseType::ENTERPRISE);
  }
  ~LicenseWrapper() { memgraph::license::global_license_checker.DisableTesting(); }
};
void DeleteListContent(std::list<BaseOpChecker *> *list) {
  for (BaseOpChecker *ptr : *list) {
    delete ptr;
  }
}
}  // namespace

TYPED_TEST(TestPlanner, ParallelExecutionAggregation) {
  LicenseWrapper license_wrapper;
  // Test USING PARALLEL EXECUTION MATCH (n) RETURN count(*)
  FakeDbAccessor dba;
  auto count_agg = COUNT(nullptr, false);
  auto *query = PARALLEL_QUERY(SINGLE_QUERY(MATCH(PATTERN(NODE("n"))), RETURN(NEXPR("count", count_agg))));
  auto symbol_table = memgraph::query::MakeSymbolTable(query);
  auto planner = MakePlanner<TypeParam>(&dba, this->storage, symbol_table, query);

  CheckPlan(planner.plan(), symbol_table, ExpectScanParallel(), ExpectParallelMerge(), ExpectScanChunk(),
            ExpectAggregate({count_agg}, {}), ExpectAggregateParallel(), ExpectProduce());
}

TYPED_TEST(TestPlanner, CountWithFilter) {
  LicenseWrapper license_wrapper;
  // Test MATCH (n) WHERE n.p < 100 RETURN count(n)
  FakeDbAccessor dba;
  auto prop_p = dba.Property("p");
  auto count_agg = COUNT(IDENT("n"), false);
  auto *query = PARALLEL_QUERY(SINGLE_QUERY(MATCH(PATTERN(NODE("n"))),
                                            WHERE(LESS(PROPERTY_LOOKUP(dba, "n", prop_p), LITERAL(100))),
                                            RETURN(NEXPR("count", count_agg))));
  auto symbol_table = memgraph::query::MakeSymbolTable(query);
  auto planner = MakePlanner<TypeParam>(&dba, this->storage, symbol_table, query);

  CheckPlan(planner.plan(), symbol_table, ExpectScanParallel(), ExpectParallelMerge(), ExpectScanChunk(),
            ExpectFilter(), ExpectAggregate({count_agg}, {}), ExpectAggregateParallel(), ExpectProduce());
}

TYPED_TEST(TestPlanner, AvgWithAlias) {
  LicenseWrapper license_wrapper;
  // Test MATCH (n) WITH n AS m RETURN avg(m.p)
  FakeDbAccessor dba;
  auto prop_p = dba.Property("p");
  auto avg_agg = AVG(PROPERTY_LOOKUP(dba, "m", prop_p), false);
  auto *query =
      PARALLEL_QUERY(SINGLE_QUERY(MATCH(PATTERN(NODE("n"))), WITH(IDENT("n"), AS("m")), RETURN(NEXPR("avg", avg_agg))));
  auto symbol_table = memgraph::query::MakeSymbolTable(query);
  auto planner = MakePlanner<TypeParam>(&dba, this->storage, symbol_table, query);

  CheckPlan(planner.plan(), symbol_table, ExpectScanParallel(), ExpectParallelMerge(), ExpectScanChunk(),
            ExpectProduce(), ExpectAggregate({avg_agg}, {}), ExpectAggregateParallel(), ExpectProduce());
}

TYPED_TEST(TestPlanner, AvgCountFilter) {
  LicenseWrapper license_wrapper;
  // Test MATCH (n) WHERE n.p < 100 RETURN avg(n.p), count(n)
  FakeDbAccessor dba;
  auto prop_p = dba.Property("p");
  auto avg_agg = AVG(PROPERTY_LOOKUP(dba, "n", prop_p), false);
  auto count_agg = COUNT(IDENT("n"), false);
  auto *query = PARALLEL_QUERY(SINGLE_QUERY(MATCH(PATTERN(NODE("n"))),
                                            WHERE(LESS(PROPERTY_LOOKUP(dba, "n", prop_p), LITERAL(100))),
                                            RETURN(NEXPR("avg", avg_agg), NEXPR("count", count_agg))));
  auto symbol_table = memgraph::query::MakeSymbolTable(query);
  auto planner = MakePlanner<TypeParam>(&dba, this->storage, symbol_table, query);

  CheckPlan(planner.plan(), symbol_table, ExpectScanParallel(), ExpectParallelMerge(), ExpectScanChunk(),
            ExpectFilter(), ExpectAggregate({avg_agg, count_agg}, {}), ExpectAggregateParallel(), ExpectProduce());
}

TYPED_TEST(TestPlanner, MinMaxWithMatch) {
  LicenseWrapper license_wrapper;
  // Test MATCH (n) WITH min(n.p) AS minp MATCH (m) RETURN max(m.p)
  FakeDbAccessor dba;
  auto prop_p = dba.Property("p");
  auto min_agg = AGG_MIN(PROPERTY_LOOKUP(dba, "n", prop_p), false);
  auto max_agg = AGG_MAX(PROPERTY_LOOKUP(dba, "m", prop_p), false);
  auto *query = PARALLEL_QUERY(SINGLE_QUERY(MATCH(PATTERN(NODE("n"))), WITH(NEXPR("minp", min_agg)),
                                            MATCH(PATTERN(NODE("m"))), RETURN(NEXPR("maxp", max_agg))));
  auto symbol_table = memgraph::query::MakeSymbolTable(query);
  auto planner = MakePlanner<TypeParam>(&dba, this->storage, symbol_table, query);

  CheckPlan(planner.plan(), symbol_table, ExpectScanParallel(), ExpectParallelMerge(), ExpectScanChunk(),
            ExpectAggregate({min_agg}, {}), ExpectAggregateParallel(), ExpectProduce(), ExpectScanParallel(),
            ExpectParallelMerge(), ExpectScanChunk(), ExpectAggregate({max_agg}, {}), ExpectAggregateParallel(),
            ExpectProduce());
}

TYPED_TEST(TestPlanner, MultiMatchCount) {
  LicenseWrapper license_wrapper;
  // Test MATCH (n) MATCH (m) RETURN count(m)
  FakeDbAccessor dba;
  auto count_agg = COUNT(IDENT("m"), false);
  auto *query = PARALLEL_QUERY(
      SINGLE_QUERY(MATCH(PATTERN(NODE("n"))), MATCH(PATTERN(NODE("m"))), RETURN(NEXPR("count", count_agg))));
  auto symbol_table = memgraph::query::MakeSymbolTable(query);
  auto planner = MakePlanner<TypeParam>(&dba, this->storage, symbol_table, query);

  std::list<BaseOpChecker *> left_cartesian_ops{new ExpectScanParallel(), new ExpectParallelMerge(),
                                                new ExpectScanChunk()};
  std::list<BaseOpChecker *> right_cartesian_ops{new ExpectScanAll()};

  CheckPlan(planner.plan(), symbol_table, ExpectCartesian(left_cartesian_ops, right_cartesian_ops),
            ExpectAggregate({count_agg}, {}), ExpectAggregateParallel(), ExpectProduce());
  DeleteListContent(&left_cartesian_ops);
  DeleteListContent(&right_cartesian_ops);
}

TYPED_TEST(TestPlanner, MultiMatchCreateCount) {
  LicenseWrapper license_wrapper;
  // Test MATCH (b), (t) CREATE () RETURN count(*)
  // Write operators should inhibit parallelization even with Cartesian joins
  FakeDbAccessor dba;
  auto count_agg = COUNT(nullptr, false);
  auto *query = PARALLEL_QUERY(SINGLE_QUERY(MATCH(PATTERN(NODE("b")), PATTERN(NODE("t"))), CREATE(PATTERN(NODE("x"))),
                                            RETURN(NEXPR("count", count_agg))));
  auto symbol_table = memgraph::query::MakeSymbolTable(query);
  auto planner = MakePlanner<TypeParam>(&dba, this->storage, symbol_table, query);

  // Expect serial plan because CREATE inhibits parallelization
  // The path from Aggregate through CreateNode to Cartesian to ScanAll is NOT read-only
  std::list<BaseOpChecker *> left_cartesian_ops{new ExpectScanAll()};
  std::list<BaseOpChecker *> right_cartesian_ops{new ExpectScanAll()};

  CheckPlan(planner.plan(), symbol_table, ExpectCartesian(left_cartesian_ops, right_cartesian_ops), ExpectCreateNode(),
            ExpectAccumulate({}), ExpectAggregate({count_agg}, {}), ExpectProduce());
  DeleteListContent(&left_cartesian_ops);
  DeleteListContent(&right_cartesian_ops);
}

TYPED_TEST(TestPlanner, MultiAgg) {
  LicenseWrapper license_wrapper;
  // Test MATCH (n) RETURN min(n.p), max(n.p), count(n.p)
  FakeDbAccessor dba;
  auto prop_p = dba.Property("p");
  auto min_agg = AGG_MIN(PROPERTY_LOOKUP(dba, "n", prop_p), false);
  auto max_agg = AGG_MAX(PROPERTY_LOOKUP(dba, "n", prop_p), false);
  auto count_agg = COUNT(PROPERTY_LOOKUP(dba, "n", prop_p), false);
  auto *query = PARALLEL_QUERY(SINGLE_QUERY(
      MATCH(PATTERN(NODE("n"))), RETURN(NEXPR("min", min_agg), NEXPR("max", max_agg), NEXPR("count", count_agg))));
  auto symbol_table = memgraph::query::MakeSymbolTable(query);
  auto planner = MakePlanner<TypeParam>(&dba, this->storage, symbol_table, query);

  CheckPlan(planner.plan(), symbol_table, ExpectScanParallel(), ExpectParallelMerge(), ExpectScanChunk(),
            ExpectAggregate({min_agg, max_agg, count_agg}, {}), ExpectAggregateParallel(), ExpectProduce());
}

TYPED_TEST(TestPlanner, BranchedSubqueries) {
  LicenseWrapper license_wrapper;
  // Test MATCH (n) CALL { MATCH (m) RETURN min(m.p) AS minp } WITH n, minp CALL { MATCH (m) RETURN max(m.p) AS maxp }
  // RETURN *
  FakeDbAccessor dba;
  auto prop_p = dba.Property("p");
  auto min_agg = AGG_MIN(PROPERTY_LOOKUP(dba, "m", prop_p), false);
  auto max_agg = AGG_MAX(PROPERTY_LOOKUP(dba, "m", prop_p), false);

  auto *query = PARALLEL_QUERY(SINGLE_QUERY(
      MATCH(PATTERN(NODE("n"))), CALL_SUBQUERY(SINGLE_QUERY(MATCH(PATTERN(NODE("m"))), RETURN(min_agg, AS("minp")))),
      WITH("n", "minp"), CALL_SUBQUERY(SINGLE_QUERY(MATCH(PATTERN(NODE("m"))), RETURN(max_agg, AS("maxp")))),
      RETURN("*")));

  auto symbol_table = memgraph::query::MakeSymbolTable(query);
  auto planner = MakePlanner<TypeParam>(&dba, this->storage, symbol_table, query);

  std::list<BaseOpChecker *> subquery1_plan{new ExpectScanParallel(),      new ExpectParallelMerge(),
                                            new ExpectScanChunk(),         new ExpectAggregate({min_agg}, {}),
                                            new ExpectAggregateParallel(), new ExpectProduce()};

  std::list<BaseOpChecker *> subquery2_plan{new ExpectScanParallel(),      new ExpectParallelMerge(),
                                            new ExpectScanChunk(),         new ExpectAggregate({max_agg}, {}),
                                            new ExpectAggregateParallel(), new ExpectProduce()};

  CheckPlan(planner.plan(), symbol_table, ExpectScanAll(), ExpectApply(subquery1_plan), ExpectProduce(),
            ExpectApply(subquery2_plan), ExpectProduce());
  DeleteListContent(&subquery1_plan);
  DeleteListContent(&subquery2_plan);
}

TYPED_TEST(TestPlanner, NesetedSubqueries) {
  LicenseWrapper license_wrapper;
  // Test MATCH (n) CALL { MATCH (m) RETURN count(*) AS c } RETURN count(*)
  FakeDbAccessor dba;
  auto count_m = COUNT(IDENT("m"), false);
  auto count_n = COUNT(IDENT("n"), false);

  auto *query = PARALLEL_QUERY(SINGLE_QUERY(
      MATCH(PATTERN(NODE("n"))), CALL_SUBQUERY(SINGLE_QUERY(MATCH(PATTERN(NODE("m"))), RETURN(count_m, AS("c")))),
      RETURN(count_n, AS("cn"))));

  auto symbol_table = memgraph::query::MakeSymbolTable(query);
  auto planner = MakePlanner<TypeParam>(&dba, this->storage, symbol_table, query);

  // Parallelize only the subquery (nested aggregation cannot be parallelized)
  std::list<BaseOpChecker *> subquery_plan{new ExpectScanParallel(),      new ExpectParallelMerge(),
                                           new ExpectScanChunk(),         new ExpectAggregate({count_m}, {}),
                                           new ExpectAggregateParallel(), new ExpectProduce()};

  CheckPlan(planner.plan(), symbol_table, ExpectScanAll(), ExpectApply(subquery_plan), ExpectAggregate({count_n}, {}),
            ExpectProduce());
  DeleteListContent(&subquery_plan);
}

TYPED_TEST(TestPlanner, MatchSetCount) {
  LicenseWrapper license_wrapper;
  // Test MATCH(n) SET n:A RETURN COUNT(n);
  FakeDbAccessor dba;
  auto *n = IDENT("n");
  auto count_agg = COUNT(n, false);
  auto *query =
      PARALLEL_QUERY(SINGLE_QUERY(MATCH(PATTERN(NODE("n"))), SET("n", {"A"}), RETURN(NEXPR("count", count_agg))));
  auto symbol_table = memgraph::query::MakeSymbolTable(query);
  auto acc = ExpectAccumulate({symbol_table.at(*n)});
  auto planner = MakePlanner<TypeParam>(&dba, this->storage, symbol_table, query);

  // Expect serial plan because SET inhibits parallelization (path not read-only)
  CheckPlan(planner.plan(), symbol_table, ExpectScanAll(), ExpectSetLabels(), acc, ExpectAggregate({count_agg}, {}),
            ExpectProduce());
}

TYPED_TEST(TestPlanner, MatchSetWithMatchCount) {
  LicenseWrapper license_wrapper;
  // Test MATCH(n) SET n:A WITH n MATCH(m) RETURN COUNT(*);
  FakeDbAccessor dba;
  auto *n = IDENT("n");
  auto count_agg = COUNT(nullptr, false);
  auto *query = PARALLEL_QUERY(SINGLE_QUERY(MATCH(PATTERN(NODE("n"))), SET("n", {"A"}), WITH(n, AS("n")),
                                            MATCH(PATTERN(NODE("m"))), RETURN(NEXPR("count", count_agg))));
  auto symbol_table = memgraph::query::MakeSymbolTable(query);
  auto acc = ExpectAccumulate({symbol_table.at(*n)});
  auto planner = MakePlanner<TypeParam>(&dba, this->storage, symbol_table, query);

  CheckPlan(planner.plan(), symbol_table, ExpectScanAll(), ExpectSetLabels(), acc, ExpectProduce(),
            ExpectScanParallel(), ExpectParallelMerge(), ExpectScanChunk(), ExpectAggregate({count_agg}, {}),
            ExpectAggregateParallel(), ExpectProduce());
}

TYPED_TEST(TestPlanner, ParallelExecutionIndexScan) {
  LicenseWrapper license_wrapper;
  // Test MATCH(n:Label)-[e:E]->() WHERE n.prop = 42 AND e.prop = true RETURN count(*);
  FakeDbAccessor dba;
  const auto *const label_name = "Label";
  const auto label = dba.Label(label_name);
  const auto *const edge_type_name = "E";
  const auto edge_type = dba.EdgeType(edge_type_name);
  const auto node_property = PROPERTY_PAIR(dba, "prop");
  const auto edge_property = PROPERTY_PAIR(dba, "eprop");
  auto count_agg = COUNT(nullptr, false);

  auto *query = PARALLEL_QUERY(
      SINGLE_QUERY(MATCH(PATTERN(NODE("n", label_name), EDGE("e", Direction::OUT, {edge_type_name}), NODE("anon1"))),
                   WHERE(AND(EQ(PROPERTY_LOOKUP(dba, "n", node_property.second), LITERAL(42)),
                             EQ(PROPERTY_LOOKUP(dba, "e", edge_property.second), LITERAL(true)))),
                   RETURN(count_agg, AS("count"))));

  auto symbol_table = memgraph::query::MakeSymbolTable(query);

  // No index
  {
    auto planner = MakePlanner<TypeParam>(&dba, this->storage, symbol_table, query);
    CheckPlan(planner.plan(), symbol_table, ExpectScanParallel(), ExpectParallelMerge(), ExpectScanChunk(),
              ExpectFilter(), ExpectExpand(), ExpectFilter(), ExpectAggregate({count_agg}, {}),
              ExpectAggregateParallel(), ExpectProduce());
  }

  // Label index
  {
    dba.SetIndexCount(label, 1000);
    auto planner = MakePlanner<TypeParam>(&dba, this->storage, symbol_table, query);
    CheckPlan(planner.plan(), symbol_table, ExpectScanParallelByLabel(), ExpectParallelMerge(), ExpectScanChunk(),
              ExpectFilter(), ExpectExpand(), ExpectFilter(), ExpectAggregate({count_agg}, {}),
              ExpectAggregateParallel(), ExpectProduce());
  }

  // LabelProperty index (loses the node filter)
  {
    dba.SetIndexCount(label, node_property.second, 1000);
    auto planner = MakePlanner<TypeParam>(&dba, this->storage, symbol_table, query);
    CheckPlan(planner.plan(), symbol_table, ExpectScanParallelByLabelProperties(), ExpectParallelMerge(),
              ExpectScanChunk(), ExpectExpand(), ExpectFilter(), ExpectAggregate({count_agg}, {}),
              ExpectAggregateParallel(), ExpectProduce());
  }
}

TYPED_TEST(TestPlanner, ParallelExecutionEdgeIndexScan) {
  LicenseWrapper license_wrapper;
  // Test MATCH(n:Label)-[e:E]->() WHERE n.prop = 42 AND e.prop = true RETURN count(*);
  FakeDbAccessor dba;
  const auto *const edge_type_name = "E";
  const auto edge_type = dba.EdgeType(edge_type_name);
  const auto edge_property = PROPERTY_PAIR(dba, "eprop");
  auto count_agg = COUNT(nullptr, false);

  auto *query = PARALLEL_QUERY(SINGLE_QUERY(
      MATCH(PATTERN(NODE("n"), EDGE("e", Direction::OUT, {edge_type_name}), NODE("anon1"))),
      WHERE(EQ(PROPERTY_LOOKUP(dba, "e", edge_property.second), LITERAL(true))), RETURN(count_agg, AS("count"))));

  auto symbol_table = memgraph::query::MakeSymbolTable(query);

  // No index
  {
    auto planner = MakePlanner<TypeParam>(&dba, this->storage, symbol_table, query);
    CheckPlan(planner.plan(), symbol_table, ExpectScanParallel(), ExpectParallelMerge(), ExpectScanChunk(),
              ExpectExpand(), ExpectFilter(), ExpectAggregate({count_agg}, {}), ExpectAggregateParallel(),
              ExpectProduce());
  }

  // EdgeType index (loses expand)
  {
    dba.SetIndexCount(edge_type, 1000);
    auto planner = MakePlanner<TypeParam>(&dba, this->storage, symbol_table, query);
    CheckPlan(planner.plan(), symbol_table, ExpectScanParallelByEdgeType(), ExpectParallelMerge(),
              ExpectScanChunkByEdge(), ExpectFilter(), ExpectAggregate({count_agg}, {}), ExpectAggregateParallel(),
              ExpectProduce());
  }

  // EdgeTypeProperty index with value filter (loses expand + the edge filter)
  {
    dba.SetIndexCount(edge_type, edge_property.second, 1000);
    auto planner = MakePlanner<TypeParam>(&dba, this->storage, symbol_table, query);
    CheckPlan(planner.plan(), symbol_table, ExpectScanParallelByEdgeTypePropertyValue(), ExpectParallelMerge(),
              ExpectScanChunkByEdge(), ExpectAggregate({count_agg}, {}), ExpectAggregateParallel(), ExpectProduce());
  }
}

// ============================================================================
// ORDER BY PARALLEL TESTS
// ============================================================================

TYPED_TEST(TestPlanner, ParallelExecutionOrderBy) {
  LicenseWrapper license_wrapper;
  // Test USING PARALLEL EXECUTION MATCH (n) RETURN n ORDER BY n.prop
  FakeDbAccessor dba;
  auto prop_p = dba.Property("prop");
  auto *as_n = NEXPR("n", IDENT("n"));
  auto ret = RETURN(as_n, ORDER_BY(PROPERTY_LOOKUP(dba, "n", prop_p)));
  auto *query = PARALLEL_QUERY(SINGLE_QUERY(MATCH(PATTERN(NODE("n"))), ret));
  auto symbol_table = memgraph::query::MakeSymbolTable(query);
  auto planner = MakePlanner<TypeParam>(&dba, this->storage, symbol_table, query);

  CheckPlan(planner.plan(), symbol_table, ExpectScanParallel(), ExpectParallelMerge(), ExpectScanChunk(),
            ExpectProduce(), ExpectOrderBy(), ExpectOrderByParallel());
}

TYPED_TEST(TestPlanner, ParallelExecutionOrderByWithFilter) {
  LicenseWrapper license_wrapper;
  // Test USING PARALLEL EXECUTION MATCH (n) WHERE n.p < 100 RETURN n ORDER BY n.p
  FakeDbAccessor dba;
  auto prop_p = dba.Property("p");
  auto *as_n = NEXPR("n", IDENT("n"));
  auto ret = RETURN(as_n, ORDER_BY(PROPERTY_LOOKUP(dba, "n", prop_p)));
  auto *query = PARALLEL_QUERY(
      SINGLE_QUERY(MATCH(PATTERN(NODE("n"))), WHERE(LESS(PROPERTY_LOOKUP(dba, "n", prop_p), LITERAL(100))), ret));
  auto symbol_table = memgraph::query::MakeSymbolTable(query);
  auto planner = MakePlanner<TypeParam>(&dba, this->storage, symbol_table, query);

  CheckPlan(planner.plan(), symbol_table, ExpectScanParallel(), ExpectParallelMerge(), ExpectScanChunk(),
            ExpectFilter(), ExpectProduce(), ExpectOrderBy(), ExpectOrderByParallel());
}

TYPED_TEST(TestPlanner, ParallelExecutionOrderByMultipleProperties) {
  LicenseWrapper license_wrapper;
  // Test USING PARALLEL EXECUTION MATCH (n) RETURN n ORDER BY n.a, n.b DESC
  FakeDbAccessor dba;
  auto prop_a = dba.Property("a");
  auto prop_b = dba.Property("b");
  auto *as_n = NEXPR("n", IDENT("n"));
  auto ret = RETURN(as_n, ORDER_BY(PROPERTY_LOOKUP(dba, "n", prop_a), PROPERTY_LOOKUP(dba, "n", prop_b)));
  auto *query = PARALLEL_QUERY(SINGLE_QUERY(MATCH(PATTERN(NODE("n"))), ret));
  auto symbol_table = memgraph::query::MakeSymbolTable(query);
  auto planner = MakePlanner<TypeParam>(&dba, this->storage, symbol_table, query);

  CheckPlan(planner.plan(), symbol_table, ExpectScanParallel(), ExpectParallelMerge(), ExpectScanChunk(),
            ExpectProduce(), ExpectOrderBy(), ExpectOrderByParallel());
}

// ============================================================================
// COMBINED AGGREGATION + ORDER BY PARALLEL TESTS
// ============================================================================

TYPED_TEST(TestPlanner, ParallelExecutionAggregationWithOrderBy) {
  LicenseWrapper license_wrapper;
  // Test USING PARALLEL EXECUTION MATCH (n) RETURN count(n) AS c ORDER BY c
  FakeDbAccessor dba;
  auto count_agg = COUNT(IDENT("n"), false);
  auto ret = RETURN(NEXPR("c", count_agg), ORDER_BY(IDENT("c")));
  auto *query = PARALLEL_QUERY(SINGLE_QUERY(MATCH(PATTERN(NODE("n"))), ret));
  auto symbol_table = memgraph::query::MakeSymbolTable(query);
  auto planner = MakePlanner<TypeParam>(&dba, this->storage, symbol_table, query);

  // Aggregation gets parallelized, then ORDER BY follows (serial since only one row after aggregation)
  CheckPlan(planner.plan(), symbol_table, ExpectScanParallel(), ExpectParallelMerge(), ExpectScanChunk(),
            ExpectAggregate({count_agg}, {}), ExpectAggregateParallel(), ExpectProduce(), ExpectOrderBy());
}

TYPED_TEST(TestPlanner, ParallelExecutionAggregationGroupByWithOrderBy) {
  LicenseWrapper license_wrapper;
  // Test USING PARALLEL EXECUTION MATCH (n) RETURN n.type AS type, count(n) AS c ORDER BY c
  FakeDbAccessor dba;
  auto prop_type = dba.Property("type");
  auto n_type = PROPERTY_LOOKUP(dba, "n", prop_type);
  auto count_agg = COUNT(IDENT("n"), false);
  auto ret = RETURN(NEXPR("type", n_type), NEXPR("c", count_agg), ORDER_BY(IDENT("c")));
  auto *query = PARALLEL_QUERY(SINGLE_QUERY(MATCH(PATTERN(NODE("n"))), ret));
  auto symbol_table = memgraph::query::MakeSymbolTable(query);
  auto planner = MakePlanner<TypeParam>(&dba, this->storage, symbol_table, query);
  auto aggr = ExpectAggregate({count_agg}, {n_type});

  // GROUP BY with aggregation, then ORDER BY on aggregated result
  // ORDER BY is NOT parallelized because the sort symbol (aggregate result) is not from ScanParallel
  CheckPlan(planner.plan(), symbol_table, ExpectScanParallel(), ExpectParallelMerge(), ExpectScanChunk(), aggr,
            ExpectAggregateParallel(), ExpectProduce(), ExpectOrderBy());
}

TYPED_TEST(TestPlanner, ParallelExecutionMinMaxWithOrderBy) {
  LicenseWrapper license_wrapper;
  // Test USING PARALLEL EXECUTION MATCH (n) RETURN min(n.p) AS minp, max(n.p) AS maxp ORDER BY minp
  FakeDbAccessor dba;
  auto prop_p = dba.Property("p");
  auto min_agg = AGG_MIN(PROPERTY_LOOKUP(dba, "n", prop_p), false);
  auto max_agg = AGG_MAX(PROPERTY_LOOKUP(dba, "n", prop_p), false);
  auto ret = RETURN(NEXPR("minp", min_agg), NEXPR("maxp", max_agg), ORDER_BY(IDENT("minp")));
  auto *query = PARALLEL_QUERY(SINGLE_QUERY(MATCH(PATTERN(NODE("n"))), ret));
  auto symbol_table = memgraph::query::MakeSymbolTable(query);
  auto planner = MakePlanner<TypeParam>(&dba, this->storage, symbol_table, query);

  CheckPlan(planner.plan(), symbol_table, ExpectScanParallel(), ExpectParallelMerge(), ExpectScanChunk(),
            ExpectAggregate({min_agg, max_agg}, {}), ExpectAggregateParallel(), ExpectProduce(), ExpectOrderBy());
}

TYPED_TEST(TestPlanner, ParallelExecutionSumGroupByWithOrderBy) {
  LicenseWrapper license_wrapper;
  // Test USING PARALLEL EXECUTION MATCH (n) RETURN n.category AS cat, sum(n.value) AS total ORDER BY total DESC
  FakeDbAccessor dba;
  auto prop_cat = dba.Property("category");
  auto prop_val = dba.Property("value");
  auto n_cat = PROPERTY_LOOKUP(dba, "n", prop_cat);
  auto sum_agg = SUM(PROPERTY_LOOKUP(dba, "n", prop_val), false);
  auto ret = RETURN(NEXPR("cat", n_cat), NEXPR("total", sum_agg), ORDER_BY(IDENT("total")));
  auto *query = PARALLEL_QUERY(SINGLE_QUERY(MATCH(PATTERN(NODE("n"))), ret));
  auto symbol_table = memgraph::query::MakeSymbolTable(query);
  auto planner = MakePlanner<TypeParam>(&dba, this->storage, symbol_table, query);
  auto aggr = ExpectAggregate({sum_agg}, {n_cat});

  // SUM with GROUP BY - aggregation is parallelized
  // ORDER BY on aggregate result is NOT parallelized (sort symbol not from scan)
  CheckPlan(planner.plan(), symbol_table, ExpectScanParallel(), ExpectParallelMerge(), ExpectScanChunk(), aggr,
            ExpectAggregateParallel(), ExpectProduce(), ExpectOrderBy());
}

TYPED_TEST(TestPlanner, ParallelExecutionAvgGroupByWithOrderByAndLimit) {
  LicenseWrapper license_wrapper;
  // Test USING PARALLEL EXECUTION MATCH (n) RETURN n.type AS t, avg(n.score) AS avg_score ORDER BY avg_score LIMIT 10
  FakeDbAccessor dba;
  auto prop_type = dba.Property("type");
  auto prop_score = dba.Property("score");
  auto n_type = PROPERTY_LOOKUP(dba, "n", prop_type);
  auto avg_agg = AVG(PROPERTY_LOOKUP(dba, "n", prop_score), false);
  auto ret = RETURN(NEXPR("t", n_type), NEXPR("avg_score", avg_agg), ORDER_BY(IDENT("avg_score")), LIMIT(LITERAL(10)));
  auto *query = PARALLEL_QUERY(SINGLE_QUERY(MATCH(PATTERN(NODE("n"))), ret));
  auto symbol_table = memgraph::query::MakeSymbolTable(query);
  auto planner = MakePlanner<TypeParam>(&dba, this->storage, symbol_table, query);
  auto aggr = ExpectAggregate({avg_agg}, {n_type});

  // AVG with GROUP BY - aggregation is parallelized
  // ORDER BY on aggregate result is NOT parallelized (sort symbol not from scan)
  CheckPlan(planner.plan(), symbol_table, ExpectScanParallel(), ExpectParallelMerge(), ExpectScanChunk(), aggr,
            ExpectAggregateParallel(), ExpectProduce(), ExpectOrderBy(), ExpectLimit());
}

TYPED_TEST(TestPlanner, ParallelExecutionGroupByWithOrderByOnGroupKey) {
  LicenseWrapper license_wrapper;
  // Test USING PARALLEL EXECUTION MATCH (n) RETURN n.type AS t, count(n) AS c ORDER BY t
  // Here ORDER BY is on the group key (n.type), which comes from the scan
  FakeDbAccessor dba;
  auto prop_type = dba.Property("type");
  auto n_type = PROPERTY_LOOKUP(dba, "n", prop_type);
  auto count_agg = COUNT(IDENT("n"), false);
  auto ret = RETURN(NEXPR("t", n_type), NEXPR("c", count_agg), ORDER_BY(IDENT("t")));
  auto *query = PARALLEL_QUERY(SINGLE_QUERY(MATCH(PATTERN(NODE("n"))), ret));
  auto symbol_table = memgraph::query::MakeSymbolTable(query);
  auto planner = MakePlanner<TypeParam>(&dba, this->storage, symbol_table, query);
  auto aggr = ExpectAggregate({count_agg}, {n_type});

  // COUNT with GROUP BY - aggregation is parallelized
  // ORDER BY on group key (n.type) - but the symbol after Produce is a new named expression symbol,
  // not the original scan symbol, so OrderBy is NOT parallelized
  CheckPlan(planner.plan(), symbol_table, ExpectScanParallel(), ExpectParallelMerge(), ExpectScanChunk(), aggr,
            ExpectAggregateParallel(), ExpectProduce(), ExpectOrderBy());
}

TYPED_TEST(TestPlanner, ParallelExecutionOrderByWithSkipLimit) {
  LicenseWrapper license_wrapper;
  // Test USING PARALLEL EXECUTION MATCH (n) RETURN n ORDER BY n.p SKIP 5 LIMIT 10
  FakeDbAccessor dba;
  auto prop_p = dba.Property("p");
  auto *as_n = NEXPR("n", IDENT("n"));
  auto ret = RETURN(as_n, ORDER_BY(PROPERTY_LOOKUP(dba, "n", prop_p)), SKIP(LITERAL(5)), LIMIT(LITERAL(10)));
  auto *query = PARALLEL_QUERY(SINGLE_QUERY(MATCH(PATTERN(NODE("n"))), ret));
  auto symbol_table = memgraph::query::MakeSymbolTable(query);
  auto planner = MakePlanner<TypeParam>(&dba, this->storage, symbol_table, query);

  CheckPlan(planner.plan(), symbol_table, ExpectScanParallel(), ExpectParallelMerge(), ExpectScanChunk(),
            ExpectProduce(), ExpectOrderBy(), ExpectOrderByParallel(), ExpectSkip(), ExpectLimit());
}

TYPED_TEST(TestPlanner, ParallelExecutionOrderByInSubquery) {
  LicenseWrapper license_wrapper;
  // Test MATCH (n) CALL { MATCH (m) RETURN m ORDER BY m.p } RETURN n, m
  // Only subquery should be parallelized
  FakeDbAccessor dba;
  auto prop_p = dba.Property("p");
  auto *as_m = NEXPR("m", IDENT("m"));
  auto subquery_ret = RETURN(as_m, ORDER_BY(PROPERTY_LOOKUP(dba, "m", prop_p)));
  auto *subquery = SINGLE_QUERY(MATCH(PATTERN(NODE("m"))), subquery_ret);
  auto *query = PARALLEL_QUERY(SINGLE_QUERY(MATCH(PATTERN(NODE("n"))), CALL_SUBQUERY(subquery), RETURN("n", "m")));
  auto symbol_table = memgraph::query::MakeSymbolTable(query);
  auto planner = MakePlanner<TypeParam>(&dba, this->storage, symbol_table, query);

  std::list<BaseOpChecker *> subquery_plan{new ExpectScanParallel(), new ExpectParallelMerge(),
                                           new ExpectScanChunk(),    new ExpectProduce(),
                                           new ExpectOrderBy(),      new ExpectOrderByParallel()};

  CheckPlan(planner.plan(), symbol_table, ExpectScanAll(), ExpectApply(subquery_plan), ExpectProduce());
  DeleteListContent(&subquery_plan);
}

TYPED_TEST(TestPlanner, ParallelExecutionAggregationAndOrderByInSubquery) {
  LicenseWrapper license_wrapper;
  // Test MATCH (n) CALL { MATCH (m) RETURN m.type AS t, count(*) AS c ORDER BY c } RETURN *
  FakeDbAccessor dba;
  auto prop_type = dba.Property("type");
  auto m_type = PROPERTY_LOOKUP(dba, "m", prop_type);
  auto count_agg = COUNT(nullptr, false);
  auto subquery_ret = RETURN(m_type, AS("t"), count_agg, AS("c"), ORDER_BY(IDENT("c")));
  auto *subquery = SINGLE_QUERY(MATCH(PATTERN(NODE("m"))), subquery_ret);
  auto *query = PARALLEL_QUERY(SINGLE_QUERY(MATCH(PATTERN(NODE("n"))), CALL_SUBQUERY(subquery), RETURN("n", "t", "c")));
  auto symbol_table = memgraph::query::MakeSymbolTable(query);
  auto planner = MakePlanner<TypeParam>(&dba, this->storage, symbol_table, query);

  // Subquery has aggregation (parallelized) and ORDER BY on aggregate result (not parallelized)
  std::list<BaseOpChecker *> subquery_plan{new ExpectScanParallel(),
                                           new ExpectParallelMerge(),
                                           new ExpectScanChunk(),
                                           new ExpectAggregate({count_agg}, {m_type}),
                                           new ExpectAggregateParallel(),
                                           new ExpectProduce(),
                                           new ExpectOrderBy()};

  CheckPlan(planner.plan(), symbol_table, ExpectScanAll(), ExpectApply(subquery_plan), ExpectProduce());
  DeleteListContent(&subquery_plan);
}

TYPED_TEST(TestPlanner, ParallelExecutionOrderByAfterWrite) {
  LicenseWrapper license_wrapper;
  // Test USING PARALLEL EXECUTION MATCH (n) SET n:A WITH n RETURN n ORDER BY n.p
  FakeDbAccessor dba;
  auto prop_p = dba.Property("p");
  auto *ident_n = IDENT("n");
  auto *as_n = NEXPR("n", IDENT("n"));
  auto ret = RETURN(as_n, ORDER_BY(PROPERTY_LOOKUP(dba, "n", prop_p)));
  auto *query = PARALLEL_QUERY(SINGLE_QUERY(MATCH(PATTERN(NODE("n"))), SET("n", {"A"}), WITH(ident_n, AS("n")), ret));
  auto symbol_table = memgraph::query::MakeSymbolTable(query);
  auto acc = ExpectAccumulate({symbol_table.at(*ident_n)});
  auto planner = MakePlanner<TypeParam>(&dba, this->storage, symbol_table, query);

  // Write operation inhibits parallelization for the scan
  // ORDER BY is also NOT parallelized because the data flow is serial after Accumulate
  CheckPlan(planner.plan(), symbol_table, ExpectScanAll(), ExpectSetLabels(), acc, ExpectProduce(), ExpectProduce(),
            ExpectOrderBy());
}

}  // namespace

#endif
