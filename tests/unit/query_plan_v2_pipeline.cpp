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

#include <gtest/gtest.h>

#include <list>
#include <sstream>

#include "query/db_accessor.hpp"
#include "query/frontend/ast/ast.hpp"
#include "query/frontend/ast/cypher_main_visitor.hpp"
#include "query/frontend/opencypher/parser.hpp"
#include "query/frontend/semantic/symbol_generator.hpp"
#include "query/plan/operator.hpp"
#include "query/plan/pretty_print.hpp"
#include "query/plan_v2/ast_converter.hpp"
#include "query/plan_v2/egraph.hpp"
#include "query/plan_v2/egraph_converter.hpp"
#include "query/plan_v2/rewrites.hpp"
#include "storage/v2/inmemory/storage.hpp"

namespace memgraph::query::plan::v2 {
namespace {

// Helper to get expression description
std::string DescribeExpression(Expression *expr) {
  if (!expr) return "null";

  if (auto *lit = utils::Downcast<PrimitiveLiteral>(expr)) {
    // Handle property value output
    const auto &val = lit->value_;
    if (val.IsInt()) return std::to_string(val.ValueInt());
    if (val.IsDouble()) return std::to_string(val.ValueDouble());
    if (val.IsString()) return "\"" + std::string(val.ValueString()) + "\"";
    if (val.IsBool()) return val.ValueBool() ? "true" : "false";
    if (val.IsNull()) return "null";
    return "literal";
  }
  if (auto *ident = utils::Downcast<Identifier>(expr)) {
    return ident->name_;
  }
  // For other expressions, just return the type name
  return expr->GetTypeInfo().name;
}

// Simple plan checker for verifying operator types in order
class SimplePlanChecker : public plan::HierarchicalLogicalOperatorVisitor {
 public:
  using HierarchicalLogicalOperatorVisitor::PostVisit;
  using HierarchicalLogicalOperatorVisitor::PreVisit;
  using HierarchicalLogicalOperatorVisitor::Visit;

  std::vector<std::string> operator_types;    // Just the type name
  std::vector<std::string> operator_details;  // Type + details

  // Once uses Visit (not PreVisit) because it's a leaf node
  bool Visit(plan::Once &) override {
    operator_types.push_back("Once");
    operator_details.push_back("Once");
    return true;
  }

  bool PreVisit(plan::Produce &op) override {
    operator_types.push_back("Produce");

    std::ostringstream oss;
    oss << "Produce {";
    bool first = true;
    for (auto *named_expr : op.named_expressions_) {
      if (!first) oss << ", ";
      first = false;
      oss << named_expr->name_ << ":" << DescribeExpression(named_expr->expression_);
    }
    oss << "}";
    operator_details.push_back(oss.str());
    return true;
  }

  bool PreVisit(plan::ScanAll &) override {
    operator_types.push_back("ScanAll");
    operator_details.push_back("ScanAll");
    return true;
  }

  bool PreVisit(plan::Filter &) override {
    operator_types.push_back("Filter");
    operator_details.push_back("Filter");
    return true;
  }

  bool PreVisit(plan::Expand &) override {
    operator_types.push_back("Expand");
    operator_details.push_back("Expand");
    return true;
  }

  bool PreVisit(plan::CreateNode &) override {
    operator_types.push_back("CreateNode");
    operator_details.push_back("CreateNode");
    return true;
  }

  bool PreVisit(plan::CreateExpand &) override {
    operator_types.push_back("CreateExpand");
    operator_details.push_back("CreateExpand");
    return true;
  }
};

class PlannerV2PipelineTest : public ::testing::Test {
 protected:
  void SetUp() override {
    storage_ = std::make_unique<storage::InMemoryStorage>(
        storage::Config{.salient = {.items = {.properties_on_edges = true}}});
    storage_acc_ = storage_->Access(storage::WRITE);
    dba_ = std::make_unique<DbAccessor>(storage_acc_.get());
  }

  /// Parse a Cypher query and return the CypherQuery AST node
  auto ParseQuery(const std::string &query_string) -> CypherQuery * {
    frontend::opencypher::Parser parser(query_string);
    Parameters parameters;
    frontend::ParsingContext context{.is_query_cached = false};
    frontend::CypherMainVisitor visitor(context, &ast_storage_, &parameters);
    visitor.visit(parser.tree());
    return utils::Downcast<CypherQuery>(visitor.query());
  }

  /// Run the full planner v2 pipeline: parse -> egraph -> rewrite -> extract
  auto PlanQuery(const std::string &query_string) -> std::unique_ptr<plan::LogicalOperator> {
    auto *cypher_query = ParseQuery(query_string);
    EXPECT_NE(cypher_query, nullptr);

    symbol_table_ = MakeSymbolTable(cypher_query);

    // Convert to e-graph
    auto [eg, root] = ConvertToEgraph(*cypher_query, symbol_table_);

    // Apply rewrites
    auto result = ApplyAllRewrites(eg);
    rewrite_result_ = result;

    // Extract logical plan
    // IMPORTANT: The plan references AST nodes owned by plan_ast_storage_,
    // so we must keep the storage alive as long as the plan is used.
    auto [plan, cost, new_ast_storage] = ConvertToLogicalOperator(eg, root);
    plan_ast_storage_ = std::move(new_ast_storage);

    return std::move(plan);
  }

  /// Get a string representation of the plan
  auto PlanToString(plan::LogicalOperator *plan) -> std::string {
    std::ostringstream oss;
    plan::PrettyPrint(*dba_, plan, &oss);
    return oss.str();
  }

  /// Get list of operator types in traversal order
  auto GetOperatorTypes(plan::LogicalOperator *plan) -> std::vector<std::string> {
    SimplePlanChecker checker;
    plan->Accept(checker);
    return checker.operator_types;
  }

  /// Get list of operator details (type + info) in traversal order
  auto GetOperatorDetails(plan::LogicalOperator *plan) -> std::vector<std::string> {
    SimplePlanChecker checker;
    plan->Accept(checker);
    return checker.operator_details;
  }

  AstStorage ast_storage_;       // For parsing
  AstStorage plan_ast_storage_;  // Owns AST nodes referenced by the plan
  SymbolTable symbol_table_;
  std::unique_ptr<storage::Storage> storage_;
  std::unique_ptr<storage::Storage::Accessor> storage_acc_;
  std::unique_ptr<DbAccessor> dba_;
  RewriteResult rewrite_result_;
};

// Test: WITH 1 AS a RETURN a;
// The inline rewrite should merge Identifier(a) with Literal(1)
TEST_F(PlannerV2PipelineTest, WithLiteralReturnAlias) {
  auto plan = PlanQuery("WITH 1 AS a RETURN a;");
  ASSERT_NE(plan, nullptr);

  // Print detailed plan
  auto details = GetOperatorDetails(plan.get());
  std::cout << "Plan for 'WITH 1 AS a RETURN a;':" << std::endl;
  for (const auto &detail : details) {
    std::cout << "  " << detail << std::endl;
  }
  std::cout << std::flush;

  // Verify plan structure: Produce -> Produce -> Once
  auto ops = GetOperatorTypes(plan.get());
  ASSERT_EQ(ops.size(), 3) << "Expected 3 operators";
  EXPECT_EQ(ops[0], "Produce");  // RETURN clause
  EXPECT_EQ(ops[1], "Produce");  // WITH clause
  EXPECT_EQ(ops[2], "Once");     // Base operator

  // Verify the RETURN produces 'a' with the inlined literal value (1)
  // After inlining, the identifier 'a' should be replaced with the literal 1
  ASSERT_FALSE(details.empty());
  EXPECT_EQ(details[0], "Produce {a:1}");

  // The rewrite should have applied (inlining the identifier)
  EXPECT_GE(rewrite_result_.rewrites_applied, 1);
  EXPECT_TRUE(rewrite_result_.saturated());
}

// Test: WITH 1 AS a, 2 AS b RETURN a, b;
// Both identifiers should be inlined
TEST_F(PlannerV2PipelineTest, WithMultipleLiterals) {
  auto plan = PlanQuery("WITH 1 AS a, 2 AS b RETURN a, b;");
  ASSERT_NE(plan, nullptr);

  // Print detailed plan
  auto details = GetOperatorDetails(plan.get());
  std::cout << "Plan for 'WITH 1 AS a, 2 AS b RETURN a, b;':" << std::endl;
  for (const auto &detail : details) {
    std::cout << "  " << detail << std::endl;
  }

  // Verify plan structure: Produce -> Produce -> Once
  auto ops = GetOperatorTypes(plan.get());
  ASSERT_EQ(ops.size(), 3);
  EXPECT_EQ(ops[0], "Produce");  // RETURN clause
  EXPECT_EQ(ops[1], "Produce");  // WITH clause
  EXPECT_EQ(ops[2], "Once");     // Base operator

  // After inlining, RETURN should produce a:1, b:2
  EXPECT_EQ(details[0], "Produce {a:1, b:2}");

  // Should inline both identifiers
  EXPECT_GE(rewrite_result_.rewrites_applied, 2);
  EXPECT_TRUE(rewrite_result_.saturated());
}

// Test: WITH 1 AS a WITH a AS b RETURN b;
// Chained aliases - requires multiple rewrite iterations
TEST_F(PlannerV2PipelineTest, ChainedAliases) {
  auto plan = PlanQuery("WITH 1 AS a WITH a AS b RETURN b;");
  ASSERT_NE(plan, nullptr);

  // Print detailed plan
  auto details = GetOperatorDetails(plan.get());
  std::cout << "Plan for 'WITH 1 AS a WITH a AS b RETURN b;':" << std::endl;
  for (const auto &detail : details) {
    std::cout << "  " << detail << std::endl;
  }

  // Verify plan structure
  auto ops = GetOperatorTypes(plan.get());
  // The actual structure depends on how the planner handles chained WITH
  ASSERT_GE(ops.size(), 2);
  EXPECT_EQ(ops[0], "Produce");   // RETURN clause
  EXPECT_EQ(ops.back(), "Once");  // Base operator

  // After chained inlining, b should resolve to literal 1
  EXPECT_EQ(details[0], "Produce {b:1}");

  // Should propagate through the chain
  EXPECT_GE(rewrite_result_.rewrites_applied, 1);
  EXPECT_TRUE(rewrite_result_.saturated());
}

// Test: RETURN 1;
// Simple return without WITH - no identifiers to inline
TEST_F(PlannerV2PipelineTest, SimpleReturn) {
  auto plan = PlanQuery("RETURN 1;");
  ASSERT_NE(plan, nullptr);

  // Print detailed plan
  auto details = GetOperatorDetails(plan.get());
  std::cout << "Plan for 'RETURN 1;':" << std::endl;
  for (const auto &detail : details) {
    std::cout << "  " << detail << std::endl;
  }

  // Verify plan structure: Produce -> Once
  auto ops = GetOperatorTypes(plan.get());
  ASSERT_EQ(ops.size(), 2);
  EXPECT_EQ(ops[0], "Produce");  // RETURN clause
  EXPECT_EQ(ops[1], "Once");     // Base operator

  // Produces literal 1 directly
  EXPECT_EQ(details[0], "Produce {1:1}");

  // No aliases to inline
  EXPECT_EQ(rewrite_result_.rewrites_applied, 0);
  EXPECT_TRUE(rewrite_result_.saturated());
}

// Test: RETURN 1 AS result;
// Single return with alias
TEST_F(PlannerV2PipelineTest, ReturnWithAlias) {
  auto plan = PlanQuery("RETURN 1 AS result;");
  ASSERT_NE(plan, nullptr);

  // Print detailed plan
  auto details = GetOperatorDetails(plan.get());
  std::cout << "Plan for 'RETURN 1 AS result;':" << std::endl;
  for (const auto &detail : details) {
    std::cout << "  " << detail << std::endl;
  }

  // Verify plan structure: Produce -> Once
  auto ops = GetOperatorTypes(plan.get());
  ASSERT_EQ(ops.size(), 2);
  EXPECT_EQ(ops[0], "Produce");  // RETURN clause
  EXPECT_EQ(ops[1], "Once");     // Base operator

  // Produces result:1
  EXPECT_EQ(details[0], "Produce {result:1}");

  EXPECT_TRUE(rewrite_result_.saturated());
}

}  // namespace
}  // namespace memgraph::query::plan::v2
