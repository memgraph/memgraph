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

/// Which queries the planner can answer without deduplicating.
///
/// Deduplication is only work where the rows below it could repeat. Where the plan already produces
/// each row once, the operator can go, and these say for which queries that holds. That the rows
/// really are the same either way is held by the DISTINCT semantics tests.

#include <string>

#include <gtest/gtest.h>

#include "query/frontend/ast/cypher_main_visitor.hpp"
#include "query/frontend/opencypher/parser.hpp"
#include "query/frontend/semantic/symbol_generator.hpp"
#include "query/plan/planner.hpp"
#include "storage/v2/inmemory/storage.hpp"

#include "query_plan_common.hpp"

namespace {

class FindsDistinct final : public HierarchicalLogicalOperatorVisitor {
 public:
  using HierarchicalLogicalOperatorVisitor::PostVisit;
  using HierarchicalLogicalOperatorVisitor::PreVisit;
  using HierarchicalLogicalOperatorVisitor::Visit;

  bool Visit(Once &) override { return true; }

  bool PreVisit(Distinct &) override {
    found = true;
    return true;
  }

  bool found{false};
};

class DistinctRemovalTest : public ::testing::Test {
 protected:
  void SetUp() override {
    db_ = std::make_unique<memgraph::storage::InMemoryStorage>(
        memgraph::storage::Config{.salient = {.items = {.properties_on_edges = true}}});
    storage_accessor_ = db_->Access(memgraph::storage::WRITE);
    dba_ = std::make_unique<DbAccessor>(storage_accessor_.get());
  }

  bool DeduplicatesFor(std::string const &query_string) {
    ast_ = AstStorage{};
    memgraph::query::Parameters parameters;
    memgraph::query::frontend::ParsingContext parsing_context{.is_query_cached = false};
    memgraph::query::frontend::opencypher::Parser parser(query_string);
    memgraph::query::frontend::CypherMainVisitor visitor(parsing_context, &ast_, &parameters);
    visitor.visit(parser.tree());
    auto *query = memgraph::utils::Downcast<CypherQuery>(visitor.query());
    EXPECT_NE(query, nullptr) << query_string;

    symbol_table_ = MakeSymbolTable(query);
    auto planning_context = MakePlanningContext(&ast_, &symbol_table_, query, dba_.get());
    auto plan = MakeLogicalPlan(&planning_context, parameters, /*use_cost_estimator=*/false).plan;

    FindsDistinct finder;
    plan->Accept(finder);
    return finder.found;
  }

  std::unique_ptr<memgraph::storage::Storage> db_;
  std::unique_ptr<memgraph::storage::Storage::Accessor> storage_accessor_;
  std::unique_ptr<DbAccessor> dba_;
  AstStorage ast_;
  SymbolTable symbol_table_;
};

// A scan reaches each vertex once, so a key of the scanned vertex alone repeats no row.
TEST_F(DistinctRemovalTest, DropsItWhereTheKeyIsAScannedVertex) {
  EXPECT_FALSE(DeduplicatesFor("MATCH (n:Item) RETURN DISTINCT n"));
}

TEST_F(DistinctRemovalTest, DropsItWhereAFilterStandsBetween) {
  EXPECT_FALSE(DeduplicatesFor("MATCH (n:Item) WHERE n.id > 1 RETURN DISTINCT n"));
}

// The narrowed key is the scanned vertex, which the projection still returns.
TEST_F(DistinctRemovalTest, DropsItWhereTheKeyNarrowsToAScannedVertex) {
  EXPECT_FALSE(DeduplicatesFor("MATCH (n:Item) RETURN DISTINCT n, n.id"));
}

// Two vertices can carry one id, so the rows can repeat where the key is the property alone.
TEST_F(DistinctRemovalTest, KeepsItWhereTheKeyIsAProperty) {
  EXPECT_TRUE(DeduplicatesFor("MATCH (n:Item) RETURN DISTINCT n.id"));
}

TEST_F(DistinctRemovalTest, KeepsItWhereTheKeyIsAConstant) {
  EXPECT_TRUE(DeduplicatesFor("MATCH (n:Item) RETURN DISTINCT 1"));
}

// Each edge is walked once from its start, so the pair of start and edge repeats no row.
TEST_F(DistinctRemovalTest, DropsItWhereTheKeyIsAnEdgeAndItsStart) {
  EXPECT_FALSE(DeduplicatesFor("MATCH (a:Item)-[r]->(b:Item) RETURN DISTINCT r, a"));
}

// Two edges can join one pair of vertices, so the pair of endpoints can repeat.
TEST_F(DistinctRemovalTest, KeepsItWhereTheKeyIsBothEndpoints) {
  EXPECT_TRUE(DeduplicatesFor("MATCH (a:Item)-[r]->(b:Item) RETURN DISTINCT a, b"));
}

TEST_F(DistinctRemovalTest, DropsItWhereTheKeyIsEveryScannedVertex) {
  EXPECT_FALSE(DeduplicatesFor("MATCH (n:Item), (m:Item) RETURN DISTINCT n, m"));
}

// One vertex pairs with every other, so it repeats once the other is left out of the key.
TEST_F(DistinctRemovalTest, KeepsItWhereTheKeyLosesOneOfTwoScannedVertices) {
  EXPECT_TRUE(DeduplicatesFor("MATCH (n:Item), (m:Item) RETURN DISTINCT n"));
}

// A list says nothing about repeating, so nothing above one can be trusted to produce a row once.
TEST_F(DistinctRemovalTest, KeepsItWhereAListWasUnwound) {
  EXPECT_TRUE(DeduplicatesFor("UNWIND [1, 1, 2] AS x RETURN DISTINCT x"));
}

TEST_F(DistinctRemovalTest, KeepsItWhereAListWasUnwoundBesideAScannedVertex) {
  EXPECT_TRUE(DeduplicatesFor("MATCH (n:Item) UNWIND [1, 1] AS x RETURN DISTINCT n, x"));
}

// Several paths can reach one vertex, and a walk of many hops is not yet accounted for.
TEST_F(DistinctRemovalTest, KeepsItAboveAWalkOfManyHops) {
  EXPECT_TRUE(DeduplicatesFor("MATCH (a:Item)-[*1..2]->(b:Item) RETURN DISTINCT b"));
}

// The key holds only what the projection returns outright, and an expression is not that.
TEST_F(DistinctRemovalTest, KeepsItWhereTheVertexReachesTheKeyOnlyThroughAnExpression) {
  EXPECT_TRUE(DeduplicatesFor("MATCH (n:Item) RETURN DISTINCT n.id + 1"));
}

// Ordering, skipping and limiting sit above the deduplication and leave its input alone.
TEST_F(DistinctRemovalTest, DropsItBeneathAnOrdering) {
  EXPECT_FALSE(DeduplicatesFor("MATCH (n:Item) RETURN DISTINCT n ORDER BY n.id"));
}

TEST_F(DistinctRemovalTest, DropsItBeneathALimit) {
  EXPECT_FALSE(DeduplicatesFor("MATCH (n:Item) RETURN DISTINCT n LIMIT 2"));
}

// A written property can differ between one row and the next, so what follows from what is not
// settled while a query writes.
TEST_F(DistinctRemovalTest, KeepsItWhereTheQueryWrites) {
  EXPECT_TRUE(DeduplicatesFor("MATCH (n:Item) SET n.id = 1 RETURN DISTINCT n, n.id"));
}

}  // namespace
