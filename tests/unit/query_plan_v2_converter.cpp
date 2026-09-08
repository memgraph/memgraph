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

// Converter-layer tests for plan_v2: egraph -> v1 logical-operator
// reconstruction.
//
// Inclusion criterion: a test here pins that an operator symbol reconstructs to
// the correct v1 AST node, or that the resolver enforces a scope/semantic
// contract.  These build the egraph directly and call ConvertToLogicalOperator
// (no parser, no storage), isolating reconstruction from the parser and from
// the full pipeline.  Per-operator symbol coverage lives here, not at the
// pipeline layer.

#include <gtest/gtest.h>

#include <algorithm>
#include <typeindex>

#include "query/exceptions.hpp"
#include "query/frontend/ast/ast.hpp"
#include "query/plan/operator.hpp"
#include "query/plan_v2/egraph/egraph.hpp"
#include "query/plan_v2/egraph/op_ast_lists.hpp"
#include "query/plan_v2/frontend/egraph_converter.hpp"
#include "query/plan_v2/test_support/literals.hpp"
#include "storage/v2/property_value.hpp"

namespace memgraph::query::plan::v2 {
namespace {

// Wrap an expression e-class in Output(Once, {NamedOutput("r", r, expr)}),
// extract, and return the dynamic type of the single reconstructed
// NamedExpression's expression.  The NamedOutput demands the expression, so it
// survives extraction.  Returned by value: `result` owns the AstStorage the
// plan points into and is destroyed on return, so a raw node pointer must not
// escape.
auto ReconstructedExprType(egraph &eg, eclass expr) -> std::type_index {
  auto r_sym = eg.MakeSymbol(0, "r");
  auto named = eg.MakeNamedOutput("r", r_sym, expr);
  auto root = eg.MakeOutput(eg.MakeOnce(), {named});

  QueryPlannerContext ctx;
  auto result = ConvertToLogicalOperator(eg, root, ctx);
  auto const &produce = dynamic_cast<plan::Produce const &>(*result.plan);
  EXPECT_FALSE(produce.named_expressions_.empty());
  auto const *expr_node = produce.named_expressions_.front()->expression_;
  return std::type_index(typeid(*expr_node));
}

// Every EGRAPH_BINARY_OPS symbol must reconstruct to its mapped v1 AST node.
// Driven by the X-macro so the coverage stays complete as operators are added:
// a new binary op without a working reconstruction path fails here.
TEST(ConverterRoundTrip, BinaryOperatorsReconstructToExpectedAst) {
  egraph eg;
  auto a = IntLit(eg, 1);
  auto b = IntLit(eg, 2);
#define MG_CHECK_BINARY(Name, AstType, ...)                                                   \
  EXPECT_EQ(ReconstructedExprType(eg, eg.Make##Name(a, b)), std::type_index(typeid(AstType))) \
      << "Make" #Name " did not reconstruct to " #AstType;
  EGRAPH_BINARY_OPS(MG_CHECK_BINARY)
#undef MG_CHECK_BINARY
}

// Every EGRAPH_UNARY_OPS symbol must reconstruct to its mapped v1 AST node.
TEST(ConverterRoundTrip, UnaryOperatorsReconstructToExpectedAst) {
  egraph eg;
  auto a = IntLit(eg, 1);
#define MG_CHECK_UNARY(Name, AstType, ...)                                                 \
  EXPECT_EQ(ReconstructedExprType(eg, eg.Make##Name(a)), std::type_index(typeid(AstType))) \
      << "Make" #Name " did not reconstruct to " #AstType;
  EGRAPH_UNARY_OPS(MG_CHECK_UNARY)
#undef MG_CHECK_UNARY
}

// ============================================================================
// Resolver scope / semantic contracts (moved from the cardinality suite:
// these assert converter semantics, not cardinality)
// ============================================================================

TEST(SubqueryBarrier, InnerBindingsStripped) {
  egraph eg;
  auto inner_once = eg.MakeOnce();
  auto x_sym = eg.MakeSymbol(0, "x");
  auto inner_one = eg.MakeLiteral(storage::ExternalPropertyValue{int64_t{1}});
  auto inner_bind = eg.MakeBind(inner_once, x_sym, inner_one);
  auto y_sym = eg.MakeSymbol(1, "y");
  auto inner_id_x = eg.MakeIdentifier(x_sym);
  auto inner_named = eg.MakeNamedOutput("y", y_sym, inner_id_x);
  auto inner_root = eg.MakeOutput(inner_bind, {inner_named});

  auto outer_once = eg.MakeOnce();
  auto subq = eg.MakeSubquery(outer_once, inner_root, {y_sym});

  auto col_y_sym = eg.MakeSymbol(2, "y");
  auto outer_id_y = eg.MakeIdentifier(y_sym);
  auto outer_named_ok = eg.MakeNamedOutput("y", col_y_sym, outer_id_y);
  auto outer_root_ok = eg.MakeOutput(subq, {outer_named_ok});

  QueryPlannerContext ctx;
  auto [plan_ok, _cost, _card, _ast, _sym] = ConvertToLogicalOperator(eg, outer_root_ok, ctx);
  ASSERT_NE(plan_ok, nullptr);

  auto col_x_sym = eg.MakeSymbol(3, "x");
  auto outer_id_x = eg.MakeIdentifier(x_sym);
  auto outer_named_bad = eg.MakeNamedOutput("x", col_x_sym, outer_id_x);
  auto outer_root_bad = eg.MakeOutput(subq, {outer_named_bad});

  QueryPlannerContext ctx_bad;
  EXPECT_THROW((void)ConvertToLogicalOperator(eg, outer_root_bad, ctx_bad), QueryException);
}

TEST(SubqueryBarrier, ImportingCallSurfacesNotYetImplemented) {
  egraph eg;
  auto outer_sym = eg.MakeSymbol(0, "x");
  auto outer_once = eg.MakeOnce();
  auto outer_bind = eg.MakeBind(outer_once, outer_sym, eg.MakeLiteral(storage::ExternalPropertyValue{int64_t{1}}));

  auto inner_once = eg.MakeOnce();
  auto inner_id = eg.MakeIdentifier(outer_sym);
  auto y_sym = eg.MakeSymbol(1, "y");
  auto inner_named = eg.MakeNamedOutput("y", y_sym, inner_id);
  auto inner_root = eg.MakeOutput(inner_once, {inner_named});

  auto subq = eg.MakeSubquery(outer_bind, inner_root, {y_sym});

  auto col_y_sym = eg.MakeSymbol(2, "y");
  auto outer_id_y = eg.MakeIdentifier(y_sym);
  auto outer_named = eg.MakeNamedOutput("y", col_y_sym, outer_id_y);
  auto outer_root = eg.MakeOutput(subq, {outer_named});

  QueryPlannerContext ctx;
  EXPECT_THROW((void)ConvertToLogicalOperator(eg, outer_root, ctx), NotYetImplemented);
}

TEST(OutputIntroduces, IncludesNamedOutputSyms) {
  egraph eg;
  auto once = eg.MakeOnce();
  auto a_sym = eg.MakeSymbol(0, "a");
  auto one = eg.MakeLiteral(storage::ExternalPropertyValue{int64_t{1}});
  auto bind_a = eg.MakeBind(once, a_sym, one);

  auto c_sym = eg.MakeSymbol(1, "c");
  auto id_a = eg.MakeIdentifier(a_sym);
  auto named_c = eg.MakeNamedOutput("c", c_sym, id_a);
  auto root = eg.MakeOutput(bind_a, {named_c});

  QueryPlannerContext ctx;
  auto [plan, _cost, _card, _ast, _sym] = ConvertToLogicalOperator(eg, root, ctx);
  ASSERT_NE(plan, nullptr);

  // The root Produce must carry a column named `c`: if Output's `introduces`
  // dropped the NamedOutput sym, that column would not survive extraction.
  // (Asserted on the plan directly so this also fails in release builds, not
  // only via the resolver's debug DMG_ASSERT.)
  auto const &produce = dynamic_cast<plan::Produce const &>(*plan);
  auto const produces_c =
      std::ranges::any_of(produce.named_expressions_, [](auto const *named) { return named->name_ == "c"; });
  EXPECT_TRUE(produces_c) << "Output dropped NamedOutput sym 'c' from its produced columns";
}

TEST(BindAbsorption, ExprRequiredAbsorbedByInputIntroduces) {
  egraph eg;
  auto once = eg.MakeOnce();
  auto a_sym = eg.MakeSymbol(0, "a");
  auto one = eg.MakeLiteral(storage::ExternalPropertyValue{int64_t{1}});
  auto bind_a = eg.MakeBind(once, a_sym, one);

  auto b_sym = eg.MakeSymbol(1, "b");
  auto id_a = eg.MakeIdentifier(a_sym);
  auto bind_b = eg.MakeBind(bind_a, b_sym, id_a);  // b = a, expr references a

  auto c_sym = eg.MakeSymbol(2, "c");
  auto id_b = eg.MakeIdentifier(b_sym);
  auto named_c = eg.MakeNamedOutput("c", c_sym, id_b);
  auto root = eg.MakeOutput(bind_b, {named_c});

  QueryPlannerContext ctx;
  auto [plan, _cost, _card, _ast, _sym] = ConvertToLogicalOperator(eg, root, ctx);
  ASSERT_NE(plan, nullptr);
}

}  // namespace
}  // namespace memgraph::query::plan::v2
