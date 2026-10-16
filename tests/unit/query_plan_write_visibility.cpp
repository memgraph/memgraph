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

// Plans for reads and writes in one query: where the planner puts command barriers, which view each read uses,
// and which rewrites must not move a read across a write.

#include <memory>
#include <string>
#include <string_view>
#include <vector>

#include <gtest/gtest.h>

#include "query/frontend/ast/ast.hpp"
#include "query/frontend/ast/cypher_main_visitor.hpp"
#include "query/frontend/opencypher/parser.hpp"
#include "query/frontend/semantic/symbol_generator.hpp"
#include "query/parameters.hpp"
#include "query/plan/operator.hpp"
#include "query/plan/planner.hpp"
#include "query_plan_checker.hpp"
#include "utils/typeinfo.hpp"

using namespace memgraph::query;
using namespace memgraph::query::plan;
using memgraph::query::frontend::CypherMainVisitor;
using memgraph::query::frontend::ParsingContext;

namespace {

/// Collects every operator of type TOp in a plan, branches included, in visit order (top first).
template <class TOp>
class OpCollector final : public HierarchicalLogicalOperatorVisitor {
 public:
  using HierarchicalLogicalOperatorVisitor::PostVisit;
  using HierarchicalLogicalOperatorVisitor::PreVisit;
  using HierarchicalLogicalOperatorVisitor::Visit;

  bool PreVisit(TOp &op) override {
    found.push_back(&op);
    return true;
  }

  bool Visit(Once & /*op*/) override { return true; }

  std::vector<TOp *> found;
};

template <class TOp>
std::vector<TOp *> Collect(LogicalOperator &plan) {
  OpCollector<TOp> collector;
  plan.Accept(collector);
  return collector.found;
}

class WriteVisibilityPlan : public ::testing::Test {
 protected:
  LogicalOperator &Plan(std::string_view query_string) {
    ::frontend::opencypher::Parser parser{std::string{query_string}};
    ParsingContext context{.is_query_cached = false};
    CypherMainVisitor visitor{context, &storage_, &parameters_};
    visitor.visit(parser.tree());
    auto *query = memgraph::utils::Downcast<CypherQuery>(visitor.query());
    EXPECT_NE(query, nullptr) << query_string;
    symbol_table_ = MakeSymbolTable(query);
    auto planning_context = MakePlanningContext(&storage_, &symbol_table_, query, &dba_);
    PostProcessor post_processor(parameters_, {}, &dba_);
    auto query_parts = CollectQueryParts(symbol_table_, storage_, query, false);
    plan_ = MakeLogicalPlanForSingleQuery<RuleBasedPlanner>(query_parts, &planning_context);
    plan_ = post_processor.Rewrite(std::move(plan_), &planning_context);
    return *plan_;
  }

  void LabelPropertyIndex(std::string const &label, std::string const &property) {
    dba_.SetIndexCount(dba_.Label(label), 1);
    dba_.SetIndexCount(dba_.Label(label), memgraph::storage::PropertyPath{dba_.Property(property)}, 1);
  }

  AstStorage storage_;
  Parameters parameters_;
  SymbolTable symbol_table_;
  FakeDbAccessor dba_;
  std::unique_ptr<LogicalOperator> plan_;
};

// ---------------------------------------------------------------- rewriter fence

TEST_F(WriteVisibilityPlan, IndexLookupKeepsFilterAboveSet) {
  LabelPropertyIndex("Q", "x");
  auto &plan = Plan("MATCH (a:Q) SET a.x = 5 WITH * WHERE a.x = 5 RETURN count(*) AS c");
  EXPECT_TRUE(Collect<ScanAllByLabelProperties>(plan).empty());
  ASSERT_EQ(Collect<Filter>(plan).size(), 1);
}

TEST_F(WriteVisibilityPlan, IndexLookupStillSeeksFilterBelowSet) {
  LabelPropertyIndex("Q", "x");
  auto &plan = Plan("MATCH (a:Q) WHERE a.x = 5 SET a.x = 6 RETURN count(*) AS c");
  EXPECT_EQ(Collect<ScanAllByLabelProperties>(plan).size(), 1);
  EXPECT_TRUE(Collect<Filter>(plan).empty());
}

TEST_F(WriteVisibilityPlan, JoinKeepsFilterAboveSet) {
  auto &plan = Plan("MATCH (a:Q), (b:P) SET a.x = 5 WITH * WHERE a.x = b.y RETURN count(*) AS c");
  EXPECT_TRUE(Collect<HashJoin>(plan).empty());
}

TEST_F(WriteVisibilityPlan, JoinStillHashesFilterBelowSet) {
  auto &plan = Plan("MATCH (a:Q), (b:P) WHERE a.x = b.y SET a.x = 5 RETURN count(*) AS c");
  EXPECT_EQ(Collect<HashJoin>(plan).size(), 1);
}

TEST_F(WriteVisibilityPlan, EdgeIndexKeepsFilterAboveSet) {
  auto edge_type = dba_.EdgeType("E");
  dba_.SetIndexCount(edge_type, 1);
  dba_.SetIndexCount(edge_type, dba_.Property("w"), 1);
  auto &plan = Plan("MATCH ()-[r:E]->() SET r.w = 5 WITH * WHERE r.w = 5 RETURN count(*) AS c");
  EXPECT_TRUE(Collect<ScanAllByEdgeTypeProperty>(plan).empty());
  EXPECT_EQ(Collect<Filter>(plan).size(), 1);
}

TEST_F(WriteVisibilityPlan, EdgeIndexStillSeeksFilterBelowSet) {
  auto edge_type = dba_.EdgeType("E");
  dba_.SetIndexCount(edge_type, 1);
  dba_.SetIndexCount(edge_type, dba_.Property("w"), 1);
  auto &plan = Plan("MATCH ()-[r:E]->() WHERE r.w = 5 SET r.w = 6 RETURN count(*) AS c");
  EXPECT_EQ(Collect<ScanAllByEdgeTypeProperty>(plan).size(), 1);
}

}  // namespace
