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

  // Binary operators
  auto describe_binary = [](const char *op, BinaryOperator *bin) -> std::string {
    return "(" + DescribeExpression(bin->expression1_) + " " + op + " " + DescribeExpression(bin->expression2_) + ")";
  };
  if (auto *op = utils::Downcast<AdditionOperator>(expr)) return describe_binary("+", op);
  if (auto *op = utils::Downcast<SubtractionOperator>(expr)) return describe_binary("-", op);
  if (auto *op = utils::Downcast<MultiplicationOperator>(expr)) return describe_binary("*", op);
  if (auto *op = utils::Downcast<DivisionOperator>(expr)) return describe_binary("/", op);
  if (auto *op = utils::Downcast<ModOperator>(expr)) return describe_binary("%", op);
  if (auto *op = utils::Downcast<ExponentiationOperator>(expr)) return describe_binary("^", op);
  if (auto *op = utils::Downcast<EqualOperator>(expr)) return describe_binary("=", op);
  if (auto *op = utils::Downcast<NotEqualOperator>(expr)) return describe_binary("<>", op);
  if (auto *op = utils::Downcast<LessOperator>(expr)) return describe_binary("<", op);
  if (auto *op = utils::Downcast<LessEqualOperator>(expr)) return describe_binary("<=", op);
  if (auto *op = utils::Downcast<GreaterOperator>(expr)) return describe_binary(">", op);
  if (auto *op = utils::Downcast<GreaterEqualOperator>(expr)) return describe_binary(">=", op);
  if (auto *op = utils::Downcast<AndOperator>(expr)) return describe_binary("AND", op);
  if (auto *op = utils::Downcast<OrOperator>(expr)) return describe_binary("OR", op);
  if (auto *op = utils::Downcast<XorOperator>(expr)) return describe_binary("XOR", op);

  // Unary operators
  if (auto *op = utils::Downcast<NotOperator>(expr)) return "(NOT " + DescribeExpression(op->expression_) + ")";
  if (auto *op = utils::Downcast<UnaryMinusOperator>(expr)) return "(-" + DescribeExpression(op->expression_) + ")";
  if (auto *op = utils::Downcast<UnaryPlusOperator>(expr)) return "(+" + DescribeExpression(op->expression_) + ")";

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
            .expected_details = {"Produce {a`0:1}", "Once"},  // dead store: Bind for a eliminated
            .min_rewrites = 1,
            .should_saturate = true,
        },
        PipelineTestCase{
            .name = "WithMultipleLiterals",
            .query = "WITH 1 AS a, 2 AS b RETURN a, b;",
            .expected_details = {"Produce {a`2:1, b`1:2}", "Once"},  // dead store: both Binds eliminated
            .min_rewrites = 2,
            .should_saturate = true,
        },
        PipelineTestCase{
            .name = "ChainedAliases",
            .query = "WITH 1 AS a WITH a AS b RETURN b;",
            .expected_details = {"Produce {b`1:1}", "Once"},  // dead store: both Binds eliminated
            .min_rewrites = 1,
            .should_saturate = true,
        },
        PipelineTestCase{
            .name = "SameNameDifferentSymbols",
            .query = "WITH 1 AS a RETURN a AS a;",
            .expected_details = {"Produce {a`0:1}", "Once"},  // dead store: Bind for a eliminated
            .min_rewrites = 1,
            .should_saturate = true,
        }
    ),
    TestCaseName
);

// --- Expression operator pipeline round-trip tests ---

INSTANTIATE_TEST_SUITE_P(
    ArithmeticOperators,
    PlannerV2PipelineTest,
    ::testing::Values(
        PipelineTestCase{
            .name = "Addition",
            .query = "RETURN 1 + 2 AS r;",
            .expected_details = {"Produce {r`0:(1 + 2)}", "Once"},
            .min_rewrites = 0,
            .should_saturate = true,
        },
        PipelineTestCase{
            .name = "Subtraction",
            .query = "RETURN 5 - 3 AS r;",
            .expected_details = {"Produce {r`0:(5 - 3)}", "Once"},
            .min_rewrites = 0,
            .should_saturate = true,
        },
        PipelineTestCase{
            .name = "Multiplication",
            .query = "RETURN 2 * 3 AS r;",
            .expected_details = {"Produce {r`0:(2 * 3)}", "Once"},
            .min_rewrites = 0,
            .should_saturate = true,
        },
        PipelineTestCase{
            .name = "Division",
            .query = "RETURN 6 / 2 AS r;",
            .expected_details = {"Produce {r`0:(6 / 2)}", "Once"},
            .min_rewrites = 0,
            .should_saturate = true,
        },
        PipelineTestCase{
            .name = "Modulo",
            .query = "RETURN 7 % 3 AS r;",
            .expected_details = {"Produce {r`0:(7 % 3)}", "Once"},
            .min_rewrites = 0,
            .should_saturate = true,
        },
        PipelineTestCase{
            .name = "Exponentiation",
            .query = "RETURN 2 ^ 3 AS r;",
            .expected_details = {"Produce {r`0:(2 ^ 3)}", "Once"},
            .min_rewrites = 0,
            .should_saturate = true,
        },
        PipelineTestCase{
            .name = "NestedArithmetic",
            .query = "RETURN 1 + 2 * 3 AS r;",
            .expected_details = {"Produce {r`0:(1 + (2 * 3))}", "Once"},
            .min_rewrites = 0,
            .should_saturate = true,
        }
    ),
    TestCaseName
);

INSTANTIATE_TEST_SUITE_P(
    ComparisonOperators,
    PlannerV2PipelineTest,
    ::testing::Values(
        PipelineTestCase{
            .name = "Equal",
            .query = "RETURN 1 = 2 AS r;",
            .expected_details = {"Produce {r`0:(1 = 2)}", "Once"},
            .min_rewrites = 0,
            .should_saturate = true,
        },
        PipelineTestCase{
            .name = "NotEqual",
            .query = "RETURN 1 <> 2 AS r;",
            .expected_details = {"Produce {r`0:(1 <> 2)}", "Once"},
            .min_rewrites = 0,
            .should_saturate = true,
        },
        PipelineTestCase{
            .name = "LessThan",
            .query = "RETURN 1 < 2 AS r;",
            .expected_details = {"Produce {r`0:(1 < 2)}", "Once"},
            .min_rewrites = 0,
            .should_saturate = true,
        },
        PipelineTestCase{
            .name = "GreaterEqual",
            .query = "RETURN 1 >= 2 AS r;",
            .expected_details = {"Produce {r`0:(1 >= 2)}", "Once"},
            .min_rewrites = 0,
            .should_saturate = true,
        }
    ),
    TestCaseName
);

INSTANTIATE_TEST_SUITE_P(
    BooleanOperators,
    PlannerV2PipelineTest,
    ::testing::Values(
        PipelineTestCase{
            .name = "And",
            .query = "RETURN true AND false AS r;",
            .expected_details = {"Produce {r`0:(true AND false)}", "Once"},
            .min_rewrites = 0,
            .should_saturate = true,
        },
        PipelineTestCase{
            .name = "Or",
            .query = "RETURN true OR false AS r;",
            .expected_details = {"Produce {r`0:(true OR false)}", "Once"},
            .min_rewrites = 0,
            .should_saturate = true,
        },
        PipelineTestCase{
            .name = "Xor",
            .query = "RETURN true XOR false AS r;",
            .expected_details = {"Produce {r`0:(true XOR false)}", "Once"},
            .min_rewrites = 0,
            .should_saturate = true,
        },
        PipelineTestCase{
            .name = "Not",
            .query = "RETURN NOT true AS r;",
            .expected_details = {"Produce {r`0:(NOT true)}", "Once"},
            .min_rewrites = 0,
            .should_saturate = true,
        },
        PipelineTestCase{
            .name = "ComparisonWithBoolean",
            .query = "RETURN 1 < 2 AND 3 > 1 AS r;",
            .expected_details = {"Produce {r`0:((1 < 2) AND (3 > 1))}", "Once"},
            .min_rewrites = 0,
            .should_saturate = true,
        }
    ),
    TestCaseName
);

INSTANTIATE_TEST_SUITE_P(
    UnaryOperators,
    PlannerV2PipelineTest,
    ::testing::Values(
        PipelineTestCase{
            .name = "UnaryMinus",
            .query = "RETURN -5 AS r;",
            .expected_details = {"Produce {r`0:(-5)}", "Once"},
            .min_rewrites = 0,
            .should_saturate = true,
        },
        PipelineTestCase{
            .name = "UnaryMinusOnExpression",
            .query = "RETURN -(1 + 2) AS r;",
            .expected_details = {"Produce {r`0:(-(1 + 2))}", "Once"},
            .min_rewrites = 0,
            .should_saturate = true,
        },
        PipelineTestCase{
            .name = "UnaryPlus",
            .query = "RETURN +5 AS r;",
            .expected_details = {"Produce {r`0:(+5)}", "Once"},
            .min_rewrites = 0,
            .should_saturate = true,
        },
        PipelineTestCase{
            .name = "UnaryPlusOnExpression",
            .query = "RETURN +(1 + 2) AS r;",
            .expected_details = {"Produce {r`0:(+(1 + 2))}", "Once"},
            .min_rewrites = 0,
            .should_saturate = true,
        }
    ),
    TestCaseName
);

INSTANTIATE_TEST_SUITE_P(
    InlineWithOperators,
    PlannerV2PipelineTest,
    ::testing::Values(
        PipelineTestCase{
            .name = "InlineThroughAdd",
            .query = "WITH 1 AS a RETURN a + 2 AS r;",
            .expected_details = {"Produce {r`0:(1 + 2)}", "Once"},  // dead store: Bind for a eliminated
            .min_rewrites = 1,
            .should_saturate = true,
        },
        PipelineTestCase{
            .name = "InlineThroughNested",
            .query = "WITH 1 AS a, 2 AS b RETURN a + b AS r;",
            .expected_details = {"Produce {r`1:(1 + 2)}", "Once"},  // dead store: both Binds eliminated
            .min_rewrites = 2,
            .should_saturate = true,
        },
        PipelineTestCase{
            .name = "InlineThroughComparison",
            .query = "WITH 1 AS a RETURN a < 2 AS r;",
            .expected_details = {"Produce {r`0:(1 < 2)}", "Once"},  // dead store: Bind for a eliminated
            .min_rewrites = 1,
            .should_saturate = true,
        },
        PipelineTestCase{
            .name = "InlineSameVarBothSides",
            .query = "WITH 1 AS a RETURN a + a AS r;",
            .expected_details = {"Produce {r`0:(1 + 1)}", "Once"},  // dead store: Bind for a eliminated
            .min_rewrites = 1,
            .should_saturate = true,
        },
        PipelineTestCase{
            .name = "InlineThroughUnaryMinus",
            .query = "WITH 5 AS a RETURN -a AS r;",
            .expected_details = {"Produce {r`0:(-5)}", "Once"},  // dead store: Bind for a eliminated
            .min_rewrites = 1,
            .should_saturate = true,
        },
        // Chained: b = a + 2, r = b * 3. The DemandCostModel determines that full inlining
        // (cost=8) is cheaper than keeping Bind_b alive (cost=11), because the Bind/Produce
        // overhead exceeds the per-node savings of Identifier(b) over Add(1,2).
        PipelineTestCase{
            .name = "InlineChainedWithOperators",
            .query = "WITH 1 AS a WITH a + 2 AS b RETURN b * 3 AS r;",
            .expected_details = {"Produce {r`1:((1 + 2) * 3)}", "Once"},  // dead store: both Binds eliminated
            .min_rewrites = 0,
            .should_saturate = true,
        },
        // Same symbol used in multiple outputs — both inline to Literal(1)
        PipelineTestCase{
            .name = "MultipleUsesOfSameSymbol",
            .query = "WITH 1 AS x RETURN x AS a, x AS b;",
            .expected_details = {"Produce {a`1:1, b`0:1}", "Once"},
            .min_rewrites = 1,
            .should_saturate = true,
        },
        // Triple chained aliases — all three binds eliminated
        PipelineTestCase{
            .name = "TripleChainedAliases",
            .query = "WITH 1 AS a WITH a AS b WITH b AS c RETURN c;",
            .expected_details = {"Produce {c`2:1}", "Once"},
            .min_rewrites = 1,
            .should_saturate = true,
        }
    ),
    TestCaseName
);

INSTANTIATE_TEST_SUITE_P(
    InlineWithMiscOperators,
    PlannerV2PipelineTest,
    ::testing::Values(
        PipelineTestCase{
            .name = "InlineThroughXor",
            .query = "WITH true AS a RETURN a XOR false AS r;",
            .expected_details = {"Produce {r`0:(true XOR false)}", "Once"},
            .min_rewrites = 1,
            .should_saturate = true,
        },
        PipelineTestCase{
            .name = "InlineThroughMod",
            .query = "WITH 7 AS a RETURN a % 3 AS r;",
            .expected_details = {"Produce {r`0:(7 % 3)}", "Once"},
            .min_rewrites = 1,
            .should_saturate = true,
        },
        PipelineTestCase{
            .name = "InlineThroughExp",
            .query = "WITH 2 AS a RETURN a ^ 3 AS r;",
            .expected_details = {"Produce {r`0:(2 ^ 3)}", "Once"},
            .min_rewrites = 1,
            .should_saturate = true,
        },
        PipelineTestCase{
            .name = "InlineThroughUnaryPlus",
            .query = "WITH 5 AS a RETURN +a AS r;",
            .expected_details = {"Produce {r`0:(+5)}", "Once"},
            .min_rewrites = 1,
            .should_saturate = true,
        }
    ),
    TestCaseName
);

INSTANTIATE_TEST_SUITE_P(
    SharedSubexpressions,
    PlannerV2PipelineTest,
    ::testing::Values(
        PipelineTestCase{
            .name = "SameExpressionTwoOutputs",
            .query = "RETURN 1 + 2 AS a, 1 + 2 AS b;",
            .expected_details = {"Produce {a`1:(1 + 2), b`0:(1 + 2)}", "Once"},
            .min_rewrites = 0,
            .should_saturate = true,
        },
        // Shared subexpression with Bind — after inline, Identifier(a) merges with
        // Add(1,2). The return expression a + (1+2) becomes Add(merged, merged) where
        // both children are the same eclass. This is a diamond DAG.
        PipelineTestCase{
            .name = "SharedSubexprWithBind",
            .query = "WITH 1 + 2 AS a RETURN a + (1 + 2) AS r;",
            .expected_details = {"Produce {r`0:((1 + 2) + (1 + 2))}", "Once"},
            .min_rewrites = 1,
            .should_saturate = true,
        }
    ),
    TestCaseName
);
// --- Dead store elimination tests ---

INSTANTIATE_TEST_SUITE_P(
    DeadStoreElimination,
    PlannerV2PipelineTest,
    ::testing::Values(
        // Dead bind: unused variable — Bind eliminated entirely
        PipelineTestCase{
            .name = "UnusedVariable",
            .query = "WITH 1 AS unused RETURN 2 AS r;",
            .expected_details = {"Produce {r`0:2}", "Once"},
            .min_rewrites = 0,
            .should_saturate = true,
        },
        // After inline, Identifier(a) is replaced by Literal(1) — Bind dead
        PipelineTestCase{
            .name = "InlinedVariableBindDead",
            .query = "WITH 1 AS a RETURN a AS r;",
            .expected_details = {"Produce {r`0:1}", "Once"},
            .min_rewrites = 1,
            .should_saturate = true,
        },
        // Transitive: both Binds dead after inline propagation
        PipelineTestCase{
            .name = "TransitiveDeadBinds",
            .query = "WITH 1 AS a WITH a AS b RETURN 2 AS r;",
            .expected_details = {"Produce {r`1:2}", "Once"},
            .min_rewrites = 1,
            .should_saturate = true,
        },
        // Partial: both binds dead after inline (Literal cheaper than Identifier)
        PipelineTestCase{
            .name = "PartialDeadBind",
            .query = "WITH 1 AS a, 2 AS b RETURN a AS r;",
            .expected_details = {"Produce {r`1:1}", "Once"},
            .min_rewrites = 1,
            .should_saturate = true,
        },
        // Inlined expression: Bind dead since Literal replaces Identifier in Add
        PipelineTestCase{
            .name = "InlinedExpressionBindDead",
            .query = "WITH 1 AS x RETURN x + 1 AS r;",
            .expected_details = {"Produce {r`0:(1 + 1)}", "Once"},
            .min_rewrites = 1,
            .should_saturate = true,
        }
    ),
    TestCaseName
);
// clang-format on

}  // namespace
}  // namespace memgraph::query::plan::v2
