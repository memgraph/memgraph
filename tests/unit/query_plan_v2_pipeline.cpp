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
    auto const &val = lit->value_;
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

    // The plan references AST nodes owned by plan_ast_storage_, and the compact
    // SymbolTable owns only the symbols surviving extraction. Replace the
    // parse-time symbol_table_ so downstream lookups see the authoritative table.
    auto [plan, cost, new_ast_storage, new_symbol_table] = ConvertToLogicalOperator(eg, root);
    plan_ast_storage_ = std::move(new_ast_storage);
    symbol_table_ = std::move(new_symbol_table);

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
  auto const &tc = GetParam();

  auto plan = PlanQuery(tc.query);
  ASSERT_NE(plan, nullptr);

  auto details = GetOperatorDetails(plan.get());

  // Print plan for debugging
  std::cout << "Plan for '" << tc.query << "':" << std::endl;
  for (auto const &detail : details) {
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

// Walk a plan tree and collect (NamedExpression name, symbol_pos_) pairs for every
// NamedExpression encountered under Produce operators. Used to verify that the
// extracted plan's symbol positions resolve to a symbol with the matching name.
class NamedExpressionCollector : public plan::HierarchicalLogicalOperatorVisitor {
 public:
  using HierarchicalLogicalOperatorVisitor::PostVisit;
  using HierarchicalLogicalOperatorVisitor::PreVisit;
  using HierarchicalLogicalOperatorVisitor::Visit;

  struct Entry {
    std::string name;
    int32_t symbol_pos;
  };

  std::vector<Entry> entries;

  bool Visit(plan::Once & /*unused*/) override { return true; }

  bool PreVisit(plan::Produce &op) override {
    for (auto *named_expr : op.named_expressions_) {
      entries.push_back({named_expr->name_, named_expr->symbol_pos_});
    }
    return true;
  }
};

TEST_F(PlannerV2PipelineTest, ExtractedSymbolPositionsResolveInCompactTable) {
  // Guards the compact-SymbolTable contract: ConvertToLogicalOperator returns a
  // compact SymbolTable alongside the plan, PlanQuery installs it as the
  // authoritative table, and every NamedExpression::symbol_pos_ in the extracted
  // plan must resolve in that table to a Symbol whose name matches.
  //
  // Regression target: a previous version assigned compact frame positions to
  // reconstructed Symbol objects but returned no SymbolTable, leaving
  // symbol_pos_ values referring to a table that didn't exist. Downstream
  // symbol_table.at(...) calls then fell back to the parse-time table and
  // could retrieve the wrong Symbol.
  auto plan = PlanQuery("WITH 1 AS a, 2 AS b RETURN a, b;");
  ASSERT_NE(plan, nullptr);

  NamedExpressionCollector collector;
  plan->Accept(collector);
  ASSERT_FALSE(collector.entries.empty());

  std::vector<std::string> failures;
  for (auto const &[name, pos] : collector.entries) {
    if (pos < 0 || pos >= symbol_table_.max_position()) {
      failures.push_back("'" + name + "' @ symbol_pos=" + std::to_string(pos) +
                         " OOB for parse-time SymbolTable (size=" + std::to_string(symbol_table_.max_position()) + ")");
      continue;
    }
    auto const &resolved = symbol_table_.table().at(static_cast<std::size_t>(pos));
    if (resolved.name() != name) {
      failures.push_back("'" + name + "' @ symbol_pos=" + std::to_string(pos) + " resolves to Symbol named '" +
                         resolved.name() + "'");
    }
  }

  EXPECT_TRUE(failures.empty()) << "Extracted plan's symbol_pos_ values do not resolve against the compact "
                                   "SymbolTable returned from ConvertToLogicalOperator. The compact table and "
                                   "the positions it is indexed by must be produced together. Failures:\n"
                                << [&] {
                                     std::string out;
                                     for (auto const &f : failures) out += "  " + f + "\n";
                                     return out;
                                   }();
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
