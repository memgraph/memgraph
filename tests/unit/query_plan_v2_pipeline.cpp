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

// Full-pipeline tests for plan_v2: parse -> egraph -> rewrite -> extract -> v1
// plan.
//
// Inclusion criterion: a test here MUST either capture a working optimisation
// (inline, dead-store elimination, demand-driven variant selection,
// cardinality-aware choice) or document a missing optimisation opportunity.
// Per-operator round-trip coverage lives at the converter layer, not here; one
// representative round-trip guards the parser->pipeline path.

#include <gtest/gtest.h>

#include <sstream>
#include <typeindex>
#include <unordered_map>

#include "query/db_accessor.hpp"
#include "query/exceptions.hpp"
#include "query/frontend/ast/ast.hpp"
#include "query/frontend/ast/cypher_main_visitor.hpp"
#include "query/frontend/opencypher/parser.hpp"
#include "query/frontend/semantic/symbol_generator.hpp"
#include "query/plan/operator.hpp"
#include "query/plan_v2/cost/cardinality_estimator.hpp"
#include "query/plan_v2/egraph/egraph.hpp"
#include "query/plan_v2/frontend/ast_converter.hpp"
#include "query/plan_v2/frontend/egraph_converter.hpp"
#include "query/plan_v2/rewrite/rewrites.hpp"
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

  // Binary operators: map concrete type -> infix symbol.
  static auto const kBinaryOpSymbols = std::unordered_map<std::type_index, std::string_view>{
      {typeid(AdditionOperator), "+"},
      {typeid(SubtractionOperator), "-"},
      {typeid(MultiplicationOperator), "*"},
      {typeid(DivisionOperator), "/"},
      {typeid(ModOperator), "%"},
      {typeid(ExponentiationOperator), "^"},
      {typeid(EqualOperator), "="},
      {typeid(NotEqualOperator), "<>"},
      {typeid(LessOperator), "<"},
      {typeid(LessEqualOperator), "<="},
      {typeid(GreaterOperator), ">"},
      {typeid(GreaterEqualOperator), ">="},
      {typeid(AndOperator), "AND"},
      {typeid(OrOperator), "OR"},
      {typeid(XorOperator), "XOR"},
  };
  if (auto *bin = dynamic_cast<BinaryOperator *>(expr)) {
    if (auto const it = kBinaryOpSymbols.find(typeid(*expr)); it != kBinaryOpSymbols.end()) {
      return "(" + DescribeExpression(bin->expression1_) + " " + std::string(it->second) + " " +
             DescribeExpression(bin->expression2_) + ")";
    }
  }

  // Unary operators: map concrete type -> prefix (space included for word operators).
  static auto const kUnaryOpPrefixes = std::unordered_map<std::type_index, std::string_view>{
      {typeid(NotOperator), "NOT "},
      {typeid(UnaryMinusOperator), "-"},
      {typeid(UnaryPlusOperator), "+"},
  };
  if (auto *un = dynamic_cast<UnaryOperator *>(expr)) {
    if (auto const it = kUnaryOpPrefixes.find(typeid(*expr)); it != kUnaryOpPrefixes.end()) {
      return "(" + std::string(it->second) + DescribeExpression(un->expression_) + ")";
    }
  }

  // Parameter lookup
  if (utils::Downcast<ParameterLookup>(expr)) return "ParameterLookup";

  // Function call: render as name(arg, arg, ...).
  if (auto *fn = utils::Downcast<Function>(expr)) {
    std::string out = fn->function_name_ + "(";
    bool first = true;
    for (auto *arg : fn->arguments_) {
      if (!first) out += ", ";
      first = false;
      out += DescribeExpression(arg);
    }
    out += ")";
    return out;
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

  bool PreVisit(plan::Unwind &op) override {
    operator_details.push_back("Unwind {" + op.output_symbol_.name() + ":" + DescribeExpression(op.input_expression_) +
                               "}");
    return true;
  }

  bool PreVisit(plan::Apply &) override {
    operator_details.push_back("Apply");
    return true;
  }
};

// Test case data for parameterized testing
struct PipelineTestCase {
  std::string name;
  std::string query;
  std::vector<std::string> expected_details;
  std::size_t expected_rewrites;
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
    auto [plan, cost, cardinality, new_ast_storage, new_symbol_table] =
        ConvertToLogicalOperator(eg, root, planner_context_);
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
  plan::v2::QueryPlannerContext planner_context_;
};

TEST_P(PlannerV2PipelineTest, Pipeline) {
  auto const &tc = GetParam();

  auto plan = PlanQuery(tc.query);
  ASSERT_NE(plan, nullptr);

  auto details = GetOperatorDetails(plan.get());

  // Verify plan structure
  if (details != tc.expected_details) {
    std::cerr << "Plan for '" << tc.query << "':" << std::endl;
    for (auto const &detail : details) {
      std::cerr << "  " << detail << std::endl;
    }
  }

  ASSERT_EQ(details, tc.expected_details);

  // The rewrite engine runs to fixpoint for every query today. When time/memory
  // budgets that can stop the engine early land, this becomes a per-case field.
  EXPECT_TRUE(rewrite_result_.saturated());

  // Exact rewrite count pins both "exactly N fired" and "none fired" - a >=
  // bound let a spurious rewrite slip through on the zero cases.
  EXPECT_EQ(rewrite_result_.rewrites_applied, tc.expected_rewrites);
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

TEST(PlannerV2BuildCacheRehash, NoCacheCorruptionAtRehashThreshold) {
  // Invariant: build_cache must hand out stable references across the
  // Builder loop even when an insertion triggers a rehash.  build_cache is
  // an open-addressing flat_map, so rehash invalidates *all* outstanding
  // references; if the loop ever holds one across an assignment that may
  // insert, it reads garbage.
  //
  // To exercise the rehash, build an egraph with > 64 selected eclasses so
  // the threshold is crossed regardless of small load-factor tweaks, then
  // run ConvertToLogicalOperator end-to-end. Both the up-front
  // build_cache.reserve() and any per-call defensive copy must hold.
  egraph eg;
  eclass current = eg.MakeOnce();
  for (int64_t i = 0; i < 32; ++i) {
    auto sym = eg.MakeSymbol(static_cast<int32_t>(i), "s_" + std::to_string(i));
    // Build a 4-op arithmetic tree: Add(Add(Add(Lit(i), Lit(i+1)), Lit(i+2)), Lit(i+3)).
    // Each Bind contributes ~5 selected eclasses (Bind + Add×3 + 4 unique Lits across the chain),
    // pushing total selection well past the rehash threshold by depth 32.
    auto lit = [&](int64_t v) { return eg.MakeLiteral(storage::ExternalPropertyValue{v}); };
    auto expr = eg.MakeAdd(eg.MakeAdd(eg.MakeAdd(lit(i), lit(i + 1)), lit(i + 2)), lit(i + 3));
    current = eg.MakeBind(current, sym, expr);
  }
  std::vector<eclass> named_outputs;
  for (int64_t i = 0; i < 32; ++i) {
    auto out_sym = eg.MakeSymbol(static_cast<int32_t>(1'000'000 + i), "o_" + std::to_string(i));
    auto out_expr = eg.MakeLiteral(storage::ExternalPropertyValue{i});
    named_outputs.push_back(eg.MakeNamedOutput("o_" + std::to_string(i), out_sym, out_expr));
  }
  auto root = eg.MakeOutput(current, std::move(named_outputs));

  plan::v2::QueryPlannerContext planner_context;
  auto result = ConvertToLogicalOperator(eg, root, planner_context);
  ASSERT_NE(result.plan, nullptr);
}

// Construct an egraph where extraction reaches root with no self-contained
// alternative: an Output projecting an Identifier whose symbol is never bound.
// The root-satisfiability precondition must surface as PlannerBug (mapped to
// Bolt DatabaseError by the glue layer) rather than a generic QueryException
// (mapped to ClientError) - retrying will fail the same way and there is
// nothing the client can do.  Also verifies the dynamic_cast dispatch that
// RewrapQueryException relies on still resolves PlannerBug when caught as
// QueryException.
TEST(PlannerV2Errors, RootUnsatisfiableSurfacesAsPlannerBug) {
  egraph eg;
  auto sym_a = eg.MakeSymbol(0, "a");
  auto sym_r = eg.MakeSymbol(1, "r");
  auto named_out = eg.MakeNamedOutput("r", sym_r, eg.MakeIdentifier(sym_a));
  auto root = eg.MakeOutput(eg.MakeOnce(), {named_out});

  QueryPlannerContext planner_context;
  EXPECT_THROW(ConvertToLogicalOperator(eg, root, planner_context), PlannerBug);

  try {
    ConvertToLogicalOperator(eg, root, planner_context);
    FAIL() << "expected PlannerBug to be thrown";
  } catch (QueryException const &e) {
    EXPECT_NE(nullptr, dynamic_cast<PlannerBug const *>(&e));
  }
}

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

// plan_v2 has no USE-scope support: a `CALL { USE g ... }` must surface loudly
// rather than silently dropping the binding and scanning the real graph. The
// binding is to a literal so the only construct plan_v2 cannot lower is USE -
// the throw isolates the missing USE guard, not an unlowerable graph value.
TEST_F(PlannerV2PipelineTest, UseScopeInCallSubquerySurfacesNotYetImplemented) {
  auto *cypher_query = ParseQuery("WITH 1 AS g CALL { USE g RETURN 1 AS y } RETURN y;");
  ASSERT_NE(cypher_query, nullptr);
  symbol_table_ = MakeSymbolTable(cypher_query);
  EXPECT_THROW((void)ConvertToEgraph(*cypher_query, symbol_table_), NotYetImplemented);
}

// Positive control: the same subquery shape without USE lowers cleanly, so the
// throw above is attributable to USE and not to the CALL block itself.
TEST_F(PlannerV2PipelineTest, NonUseCallSubqueryLowersWithoutThrow) {
  auto *cypher_query = ParseQuery("WITH 1 AS g CALL { RETURN 1 AS y } RETURN y;");
  ASSERT_NE(cypher_query, nullptr);
  symbol_table_ = MakeSymbolTable(cypher_query);
  EXPECT_NO_THROW((void)ConvertToEgraph(*cypher_query, symbol_table_));
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
            .expected_rewrites = 0,
        },
        PipelineTestCase{
            .name = "ReturnWithAlias",
            .query = "RETURN 1 AS result;",
            .expected_details = {"Produce {result`0:1}", "Once"},
            .expected_rewrites = 0,
        },
        PipelineTestCase{
            .name = "WithLiteralReturnAlias",
            .query = "WITH 1 AS a RETURN a;",
            .expected_details = {"Produce {a`0:1}", "Once"},  // dead store: Bind for a eliminated
            .expected_rewrites = 1,
        },
        PipelineTestCase{
            .name = "WithMultipleLiterals",
            .query = "WITH 1 AS a, 2 AS b RETURN a, b;",
            .expected_details = {"Produce {a`0:1, b`1:2}", "Once"},  // dead store: both Binds eliminated
            .expected_rewrites = 2,
        },
        PipelineTestCase{
            .name = "ChainedAliases",
            .query = "WITH 1 AS a WITH a AS b RETURN b;",
            .expected_details = {"Produce {b`0:1}", "Once"},  // dead store: both Binds eliminated
            .expected_rewrites = 2,
        },
        PipelineTestCase{
            .name = "SameNameDifferentSymbols",
            .query = "WITH 1 AS a RETURN a AS a;",
            .expected_details = {"Produce {a`0:1}", "Once"},  // dead store: Bind for a eliminated
            .expected_rewrites = 1,
        }
    ),
    TestCaseName
);

// --- Representative expression round-trip (per-operator coverage lives at the
//     converter layer) ---

INSTANTIATE_TEST_SUITE_P(
    ExpressionRoundTrip,
    PlannerV2PipelineTest,
    ::testing::Values(
        // Single representative full-pipeline round-trip.  Per-operator symbol
        // reconstruction is covered at the converter layer; this case earns its
        // place by checking that the parser builds a correctly-nested egraph
        // (operator precedence: `*` binds tighter than `+`) that survives the
        // pipeline.
        PipelineTestCase{
            .name = "NestedArithmetic",
            .query = "RETURN 1 + 2 * 3 AS r;",
            .expected_details = {"Produce {r`0:(1 + (2 * 3))}", "Once"},
            .expected_rewrites = 0,
        }
    ),
    TestCaseName
);




INSTANTIATE_TEST_SUITE_P(
    InlineWithOperators,
    PlannerV2PipelineTest,
    ::testing::Values(
        // Representative: inline fires when the Identifier sits inside a parent
        // expression, and the now-dead Bind is eliminated end-to-end.  The
        // inline rewrite is operator-agnostic (it matches Identifier(sym)
        // regardless of the holding operator), so a single case suffices;
        // multi-hop cascade and shared-symbol behaviours are covered by the
        // dead-store-elimination and multi-alt resolution suites below.
        PipelineTestCase{
            .name = "InlineThroughAdd",
            .query = "WITH 1 AS a RETURN a + 2 AS r;",
            .expected_details = {"Produce {r`0:(1 + 2)}", "Once"},  // dead store: Bind for a eliminated
            .expected_rewrites = 1,
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
            .expected_details = {"Produce {a`0:(1 + 2), b`1:(1 + 2)}", "Once"},
            .expected_rewrites = 0,
        },
        // Shared subexpression with Bind - after inline, Identifier(a) merges with
        // Add(1,2). The return expression a + (1+2) becomes Add(merged, merged) where
        // both children are the same eclass. This is a diamond DAG.
        PipelineTestCase{
            .name = "SharedSubexprWithBind",
            .query = "WITH 1 + 2 AS a RETURN a + (1 + 2) AS r;",
            .expected_details = {"Produce {r`0:((1 + 2) + (1 + 2))}", "Once"},
            .expected_rewrites = 1,
        }
    ),
    TestCaseName
);
// --- Parameter query pipeline tests ---

INSTANTIATE_TEST_SUITE_P(
    ParameterQueries,
    PlannerV2PipelineTest,
    ::testing::Values(
        // Parameter lookup inlined through WITH binding
        PipelineTestCase{
            .name = "ParamLookupInlined",
            .query = "WITH $param AS a RETURN a;",
            .expected_details = {"Produce {a`0:ParameterLookup}", "Once"},
            .expected_rewrites = 1,
        },
        // Parameter used in arithmetic expression
        PipelineTestCase{
            .name = "ParamLookupInArithmetic",
            .query = "WITH $x AS a RETURN a + 1 AS r;",
            .expected_details = {"Produce {r`0:(ParameterLookup + 1)}", "Once"},
            .expected_rewrites = 1,
        },
        // Dead bind with parameter - param never used, bind eliminated
        PipelineTestCase{
            .name = "UnusedParamBinding",
            .query = "WITH $x AS unused RETURN 1 AS r;",
            .expected_details = {"Produce {r`0:1}", "Once"},
            .expected_rewrites = 0,
        }
    ),
    TestCaseName
);

// --- Dead store elimination tests ---

INSTANTIATE_TEST_SUITE_P(
    DeadStoreElimination,
    PlannerV2PipelineTest,
    ::testing::Values(
        // Dead bind: unused variable - Bind eliminated entirely
        PipelineTestCase{
            .name = "UnusedVariable",
            .query = "WITH 1 AS unused RETURN 2 AS r;",
            .expected_details = {"Produce {r`0:2}", "Once"},
            .expected_rewrites = 0,
        },
        // After inline, Identifier(a) is replaced by Literal(1) - Bind dead
        PipelineTestCase{
            .name = "InlinedVariableBindDead",
            .query = "WITH 1 AS a RETURN a AS r;",
            .expected_details = {"Produce {r`0:1}", "Once"},
            .expected_rewrites = 1,
        },
        // Transitive: both Binds dead after inline propagation
        PipelineTestCase{
            .name = "TransitiveDeadBinds",
            .query = "WITH 1 AS a WITH a AS b RETURN 2 AS r;",
            .expected_details = {"Produce {r`0:2}", "Once"},
            .expected_rewrites = 1,
        },
        // Partial: both binds dead after inline (Literal cheaper than Identifier)
        PipelineTestCase{
            .name = "PartialDeadBind",
            .query = "WITH 1 AS a, 2 AS b RETURN a AS r;",
            .expected_details = {"Produce {r`0:1}", "Once"},
            .expected_rewrites = 1,
        },
        // Inlined expression: Bind dead since Literal replaces Identifier in Add
        PipelineTestCase{
            .name = "InlinedExpressionBindDead",
            .query = "WITH 1 AS x RETURN x + 1 AS r;",
            .expected_details = {"Produce {r`0:(1 + 1)}", "Once"},
            .expected_rewrites = 1,
        }
    ),
    TestCaseName
);
// --- DAG resolution and cascade tests ---

// clang-format off
INSTANTIATE_TEST_SUITE_P(
    DAGResolution,
    PlannerV2PipelineTest,
    ::testing::Values(
        // Shared Bind consumed by two outputs - exercises DAG memoization in resolver
        PipelineTestCase{
            .name = "SharedBindTwoConsumers",
            .query = "WITH 1 AS a RETURN a AS x, a AS y;",
            .expected_details = {"Produce {x`0:1, y`1:1}", "Once"},
            .expected_rewrites = 1,
        },
        // Chained Binds with cascade - both eliminated after inline
        PipelineTestCase{
            .name = "ChainedBindCascade",
            .query = "WITH 1 AS a WITH a AS b RETURN b AS x, 1 AS y;",
            .expected_details = {"Produce {x`0:1, y`1:1}", "Once"},
            .expected_rewrites = 2,
        }
    ),
    TestCaseName
);
// clang-format on

TEST_F(PlannerV2PipelineTest, PickCompatibleDemandedIntroducesFiltersDeadBind) {
  // demanded_introduces as the sole discriminator in pick_compatible:
  // build a minimal egraph directly (no rewrites) so both the alive and dead
  // Bind alternatives are present.  The Output can only pick the alive path
  // because the dead Output alt has required={sym_a} which is incompatible
  // with root provided={}.  Once the Output selects the alive alt,
  // demanded_introduces={sym_a} flows down and filters the dead Bind alt
  // (introduces={}) in the subsequent pick_compatible call, leaving only the
  // costlier alive alt (introduces={sym_a}) as compatible.
  //
  // Observable: the plan has an inner Produce that binds a=1 (alive Bind)
  // and an outer Produce that reads a via Identifier (not the inlined literal).
  egraph eg;
  auto sym_a = eg.MakeSymbol(0, "a");
  auto sym_r = eg.MakeSymbol(1, "r");
  auto lit1 = eg.MakeLiteral(storage::ExternalPropertyValue{int64_t{1}});
  auto bind = eg.MakeBind(eg.MakeOnce(), sym_a, lit1);
  auto named_out = eg.MakeNamedOutput("r", sym_r, eg.MakeIdentifier(sym_a));
  auto root = eg.MakeOutput(bind, {named_out});

  auto [plan, cost, cardinality, ast_storage, symbol_table] = ConvertToLogicalOperator(eg, root, planner_context_);
  ASSERT_NE(plan, nullptr);

  auto details = GetOperatorDetails(plan.get());
  // Alive Bind chosen: inner Produce binds a=1, outer Produce reads a via Identifier.
  ASSERT_EQ(details, (std::vector<std::string>{"Produce {r`1:a}", "Produce {a`0:1}", "Once"}));
}

// clang-format off
INSTANTIATE_TEST_SUITE_P(
    Subqueries,
    PlannerV2PipelineTest,
    ::testing::Values(
        // Minimal CALL { ... } RETURN ...: the inner block exposes one
        // projection (`y`) into the outer scope.  The outer RETURN reads it
        // back via Identifier(y).  Validates the (B) "available downstream"
        // semantic for `introduces`: the inner's row-pipe introductions are
        // stripped at the Subquery boundary, only the explicit exposed_syms
        // cross.  Without the barrier, the outer Identifier(y) would have
        // been satisfied by inner row-pipe alts directly (incorrect for
        // queries that have inner-only bindings).
        PipelineTestCase{
            .name = "MinimalCallReturn",
            .query = "CALL { RETURN 1 AS y } RETURN y;",
            .expected_details = {"Produce {y`1:y}", "Apply", "Once", "Produce {y`0:1}", "Once"},
            .expected_rewrites = 0,
        }
    ),
    TestCaseName
);
// clang-format on

// clang-format off
INSTANTIATE_TEST_SUITE_P(
    UnwindClauses,
    PlannerV2PipelineTest,
    ::testing::Values(
        PipelineTestCase{
            .name = "UnwindRangeReturnLiteral",
            .query = "UNWIND range(0, 5) AS x RETURN 1 AS r;",
            .expected_details = {"Produce {r`1:1}", "Unwind {x:RANGE(0, 5)}", "Once"},
            .expected_rewrites = 0,
        },
        // The introduces-axis lets Output's NamedOutput see symbols the
        // input row pipe binds.  RETURN x resolves to a per-row Identifier
        // reference, which is what we want when x is the row-pipe variable.
        PipelineTestCase{
            .name = "UnwindRangeReturnSymbol",
            .query = "UNWIND range(0, 5) AS x RETURN x;",
            .expected_details = {"Produce {x`1:x}", "Unwind {x:RANGE(0, 5)}", "Once"},
            .expected_rewrites = 0,
        },
        // Regression target for the WITH/inline cardinality decision.
        // The Bind for `a` is ALIVE because its sym is referenced by the
        // NamedOutput Identifier(a) downstream, even though no node in
        // Bind's input subtree demands it.  With per-row Output cost
        // scaling, evaluating Identifier(a) per output row beats inlining
        // the addition tree 101 times.
        PipelineTestCase{
            .name = "WithUnwindPrefersNonInlined",
            .query = "WITH 1+1+1+1+1+1 AS a UNWIND range(0, 100) AS X RETURN a;",
            .expected_details = {"Produce {a`2:a}",
                                 "Unwind {X:RANGE(0, 100)}",
                                 "Produce {a`0:(((((1 + 1) + 1) + 1) + 1) + 1)}",
                                 "Once"},
            .expected_rewrites = 1,
        }
    ),
    TestCaseName
);
// clang-format on

// clang-format off
INSTANTIATE_TEST_SUITE_P(
    FunctionCalls,
    PlannerV2PipelineTest,
    ::testing::Values(
        // The cypher parser uppercases builtin function names; the planner
        // round-trips that exact name through the e-graph interner.
        PipelineTestCase{
            .name = "RangeWithIntLiterals",
            .query = "RETURN range(0, 5) AS r;",
            .expected_details = {"Produce {r`0:RANGE(0, 5)}", "Once"},
            .expected_rewrites = 0,
        },
        PipelineTestCase{
            .name = "RangeWithParameter",
            .query = "RETURN range(0, $n) AS r;",
            .expected_details = {"Produce {r`0:RANGE(0, ParameterLookup)}", "Once"},
            .expected_rewrites = 0,
        }
    ),
    TestCaseName
);
// clang-format on

// --- Alive Bind chain tests ---
//
// These exercise alive Bind propagation in chains where at least one Bind
// cannot be inlined away, so the demand-set algebra (AliveRequired) runs with
// non-empty required sets at the pipeline level.
//
// True T2 (same eclass picking different alternatives under different `provided`
// sets) and T3 (AliveRequired overlap when expr_required intersects
// input_required\{sym}) at the full pipeline level require scan operators
// (MATCH (n)) to produce non-empty row-pipe `provided` sets.  Until scan
// operators land, this suite uses UNWIND as the row-pipe source and covers the
// alive Bind demand-propagation paths that are reachable today.
//
// KNOWN COVERAGE GAP: the resolver's conservative selection when a DAG-shared
// eclass is reached under divergent `in_scope` from two parents (T2) is not
// reachable by any query today, so it is intentionally untested here. The
// single-context alt-selection predicate it builds on IS covered, against
// production, by PlannerV2PipelineTest's PickCompatibleDemandedIntroduces... /
// the dead-store suite. Cover T2 directly when MATCH-sourced provided sets land.

// clang-format off
INSTANTIATE_TEST_SUITE_P(
    AliveBindChains,
    PlannerV2PipelineTest,
    ::testing::Values(
        // Bind for y uses the Unwind row-pipe variable x.  Because x is not a
        // literal, InlineRule leaves Identifier(x) in place; the cost model
        // then elects to inline y (dead Bind) since adding a Bind node for y
        // would cost more than evaluating (x+1) directly in the output.
        // Tests that dead-bind elimination works correctly when the expression
        // references a row-pipe symbol: the Identifier(x) reference in the
        // output must remain valid after the Bind for y is dropped.
        PipelineTestCase{
            .name = "BindExprUsesUnwindVar",
            .query = "UNWIND range(0, 5) AS x WITH x + 1 AS y RETURN y;",
            .expected_details = {"Produce {y`1:(x + 1)}", "Unwind {x:RANGE(0, 5)}", "Once"},
            .expected_rewrites = 1,
        },
        // Dead Bind (z = x+1, never used in RETURN) alongside an inlined alive
        // Bind (y = x, used in RETURN).  Tests that z's Bind is stripped while
        // the Identifier(x) in the output correctly references the Unwind variable.
        // Note: `WITH x AS y, x+1 AS z` is the correct Cypher form to pass x
        // through into the new scope; `WITH 999 AS unused RETURN x` would be a
        // Cypher scope error because x falls out of scope after WITH.
        PipelineTestCase{
            .name = "DeadBindDroppedAliveBindInlined",
            .query = "UNWIND range(0, 5) AS x WITH x AS y, x + 1 AS z RETURN y;",
            .expected_details = {"Produce {y`1:x}", "Unwind {x:RANGE(0, 5)}", "Once"},
            .expected_rewrites = 1,
        },
        // Two output columns both referencing the same Unwind symbol.  Tests
        // that the row-pipe symbol is shared across two NamedOutput consumers
        // without duplication or premature elimination.
        PipelineTestCase{
            .name = "TwoOutputsFromUnwindVar",
            .query = "UNWIND range(0, 5) AS x RETURN x AS a, x + 1 AS b;",
            .expected_details = {"Produce {a`1:x, b`3:(x + 1)}", "Unwind {x:RANGE(0, 5)}", "Once"},
            .expected_rewrites = 0,
        },
        // Scalar alive Bind for `a` (evaluated once before Unwind) combined
        // with a per-row output `y = a + x` that is inlined into the RETURN.
        // - `a` stays alive as a scalar Produce because 101 rows × inline cost
        //   exceeds the one-time Bind cost (same reasoning as WithUnwindPrefersNonInlined).
        // - `y = a + x` is inlined (dead Bind): adding a Bind for y per-row
        //   costs more than evaluating (a+x) directly in the output.
        // The RETURN Produce references `a` by Identifier (the scalar pre-Unwind
        // value) and `x` by Identifier (the per-row Unwind value).
        // AliveRequired at y's Bind level: expr_required={a,x} is added to
        // (input_required\{y}), exercising the set-union branch in demand propagation.
        PipelineTestCase{
            .name = "ScalarAliveBindUnwindInlinedPerRowExpr",
            .query = "WITH 1+1+1+1+1+1 AS a UNWIND range(0, 100) AS x WITH a + x AS y RETURN y;",
            .expected_details = {"Produce {y`2:(a + x)}", "Unwind {x:RANGE(0, 100)}",
                                 "Produce {a`0:(((((1 + 1) + 1) + 1) + 1) + 1)}", "Once"},
            .expected_rewrites = 2,
        },
        // Chained Bind where the inner Bind's expr references the outer
        // Bind's sym.  Both Binds inline into a single Produce.
        PipelineTestCase{
            .name = "ChainedBindExprUsesPriorBindSym",
            .query = "WITH 1 AS a WITH a+1 AS b RETURN b;",
            .expected_details = {"Produce {b`0:(1 + 1)}", "Once"},
            .expected_rewrites = 2,
        },
        // Deeper chain with a multi-symbol expression at one level and a
        // NamedOutput in the RETURN.  Exercises Output's NamedOutput-sym
        // injection into `introduces` (the `own_syms` rule).
        PipelineTestCase{
            .name = "ChainedBindMultiSymWithNamedOutput",
            .query = "WITH 1 AS a WITH a+a AS b RETURN b+200 AS c;",
            .expected_details = {"Produce {c`0:((1 + 1) + 200)}", "Once"},
            .expected_rewrites = 2,
        }
    ),
    TestCaseName
);
// clang-format on

}  // namespace
}  // namespace memgraph::query::plan::v2
