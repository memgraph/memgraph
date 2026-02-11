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

#include <sstream>

#include "query/db_accessor.hpp"
#include "query/frontend/ast/ast.hpp"
#include "query/frontend/ast/cypher_main_visitor.hpp"
#include "query/frontend/opencypher/parser.hpp"
#include "query/frontend/semantic/symbol_generator.hpp"
#include "query/plan/operator.hpp"
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
  return expr->GetTypeInfo().name;
}

// Plan visitor that collects operator details in traversal order
class SimplePlanChecker : public plan::HierarchicalLogicalOperatorVisitor {
 public:
  using HierarchicalLogicalOperatorVisitor::PostVisit;
  using HierarchicalLogicalOperatorVisitor::PreVisit;
  using HierarchicalLogicalOperatorVisitor::Visit;

  std::vector<std::string> operator_details;

  bool Visit(plan::Once &) override {
    operator_details.push_back("Once");
    return true;
  }

  bool PreVisit(plan::Produce &op) override {
    std::ostringstream oss;
    oss << "Produce {";
    bool first = true;
    for (auto *named_expr : op.named_expressions_) {
      if (!first) oss << ", ";
      first = false;
      // Use backtick notation to disambiguate symbols: name`pos
      oss << named_expr->name_ << "`" << named_expr->symbol_pos_ << ":" << DescribeExpression(named_expr->expression_);
    }
    oss << "}";
    operator_details.push_back(oss.str());
    return true;
  }

  bool PreVisit(plan::ScanAll &) override {
    operator_details.push_back("ScanAll");
    return true;
  }

  bool PreVisit(plan::Filter &) override {
    operator_details.push_back("Filter");
    return true;
  }

  bool PreVisit(plan::Expand &) override {
    operator_details.push_back("Expand");
    return true;
  }

  bool PreVisit(plan::CreateNode &) override {
    operator_details.push_back("CreateNode");
    return true;
  }

  bool PreVisit(plan::CreateExpand &) override {
    operator_details.push_back("CreateExpand");
    return true;
  }
};

// Test case data for parameterized testing
struct PipelineTestCase {
  std::string name;
  std::string query;
  std::vector<std::string> expected_details;
  std::size_t min_rewrites;
  bool should_saturate;
};

// Pretty-print test case name for Google Test
std::string TestCaseName(const testing::TestParamInfo<PipelineTestCase> &info) { return info.param.name; }

class PlannerV2PipelineTest : public ::testing::TestWithParam<PipelineTestCase> {
 protected:
  void SetUp() override {
    storage_ = std::make_unique<storage::InMemoryStorage>(
        storage::Config{.salient = {.items = {.properties_on_edges = true}}});
    storage_acc_ = storage_->Access(storage::WRITE);
    dba_ = std::make_unique<DbAccessor>(storage_acc_.get());
  }

  auto ParseQuery(const std::string &query_string) -> CypherQuery * {
    frontend::opencypher::Parser parser(query_string);
    Parameters parameters;
    frontend::ParsingContext context{.is_query_cached = false};
    frontend::CypherMainVisitor visitor(context, &ast_storage_, &parameters);
    visitor.visit(parser.tree());
    return utils::Downcast<CypherQuery>(visitor.query());
  }

  auto PlanQuery(const std::string &query_string) -> std::unique_ptr<plan::LogicalOperator> {
    auto *cypher_query = ParseQuery(query_string);
    EXPECT_NE(cypher_query, nullptr);

    symbol_table_ = MakeSymbolTable(cypher_query);
    auto [eg, root] = ConvertToEgraph(*cypher_query, symbol_table_);

    auto result = ApplyAllRewrites(eg);
    rewrite_result_ = result;

    // The plan references AST nodes owned by plan_ast_storage_
    auto [plan, cost, new_ast_storage] = ConvertToLogicalOperator(eg, root);
    plan_ast_storage_ = std::move(new_ast_storage);

    return std::move(plan);
  }

  auto GetOperatorDetails(plan::LogicalOperator *plan) -> std::vector<std::string> {
    SimplePlanChecker checker;
    plan->Accept(checker);
    return checker.operator_details;
  }

  AstStorage ast_storage_;
  AstStorage plan_ast_storage_;
  SymbolTable symbol_table_;
  std::unique_ptr<storage::Storage> storage_;
  std::unique_ptr<storage::Storage::Accessor> storage_acc_;
  std::unique_ptr<DbAccessor> dba_;
  RewriteResult rewrite_result_;
};

TEST_P(PlannerV2PipelineTest, Pipeline) {
  const auto &tc = GetParam();

  auto plan = PlanQuery(tc.query);
  ASSERT_NE(plan, nullptr);

  auto details = GetOperatorDetails(plan.get());

  // Print plan for debugging
  std::cout << "Plan for '" << tc.query << "':" << std::endl;
  for (const auto &detail : details) {
    std::cout << "  " << detail << std::endl;
  }

  // Verify plan structure
  ASSERT_EQ(details, tc.expected_details);

  // Verify rewrite statistics
  EXPECT_GE(rewrite_result_.rewrites_applied, tc.min_rewrites);
  if (tc.should_saturate) {
    EXPECT_TRUE(rewrite_result_.saturated());
  }
}

// clang-format off
INSTANTIATE_TEST_SUITE_P(
    InlineRewrites,
    PlannerV2PipelineTest,
    ::testing::Values(
        PipelineTestCase{
            .name = "SimpleReturn",
            .query = "RETURN 1;",
            .expected_details = {"Produce {1`0:1}", "Once"},
            .min_rewrites = 0,
            .should_saturate = true,
        },
        PipelineTestCase{
            .name = "ReturnWithAlias",
            .query = "RETURN 1 AS result;",
            .expected_details = {"Produce {result`0:1}", "Once"},
            .min_rewrites = 0,
            .should_saturate = true,
        },
        PipelineTestCase{
            .name = "WithLiteralReturnAlias",
            .query = "WITH 1 AS a RETURN a;",
            .expected_details = {"Produce {a`0:1}", "Produce {a`1:1}", "Once"},
            .min_rewrites = 1,
            .should_saturate = true,
        },
        PipelineTestCase{
            .name = "WithMultipleLiterals",
            .query = "WITH 1 AS a, 2 AS b RETURN a, b;",
            .expected_details = {"Produce {a`2:1, b`1:2}", "Produce {a`0:1, b`3:2}", "Once"},
            .min_rewrites = 2,
            .should_saturate = true,
        },
        PipelineTestCase{
            .name = "ChainedAliases",
            .query = "WITH 1 AS a WITH a AS b RETURN b;",
            .expected_details = {"Produce {b`1:1}", "Produce {a`0:1, b`2:1}", "Once"},
            .min_rewrites = 1,
            .should_saturate = true,
        },
        PipelineTestCase{
            .name = "SameNameDifferentSymbols",
            .query = "WITH 1 AS a RETURN a AS a;",
            .expected_details = {"Produce {a`0:1}", "Produce {a`1:1}", "Once"},
            .min_rewrites = 1,
            .should_saturate = true,
        }
    ),
    TestCaseName
);
// clang-format on

}  // namespace
}  // namespace memgraph::query::plan::v2
