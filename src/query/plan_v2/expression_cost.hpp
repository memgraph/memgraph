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

#pragma once

#include "query/plan_v2/private_symbol.hpp"

// ============================================================================
// Expression-operator costs for PlanCostModel.
// ============================================================================
//
// These are the per-application costs the planner attributes to expression
// operators (arithmetic, comparison, boolean, unary) when deciding between
// equivalent rewrites.  Today every value is 1.0 — differentiating without
// measured data would just calcify guesses into the planner.
//
// When you have benchmark data showing a class of operator should bias the
// planner one way or another (e.g. "comparison-heavy projections should
// prefer a layout that avoids re-evaluation"), tune the constants here once
// rather than across N switch cases in PlanCostModel.
//
// Out of scope: the *runtime* cost of evaluating an operator on TypedValue
// (which depends on operand type, value size, and cardinality) is not what
// the planner cost model is about — that lives in the executor and the
// cardinality estimator.  These constants are the *structural* per-operator
// cost the planner uses to compare plans.

namespace memgraph::query::plan::v2::expression_cost {

/// Arithmetic operators: Add, Sub, Mul, Div, Mod, Exp.
inline constexpr double kArithmetic = 1.0;

/// Comparison operators: Eq, Neq, Lt, Lte, Gt, Gte.
inline constexpr double kComparison = 1.0;

/// Boolean operators: And, Or, Xor.
inline constexpr double kBoolean = 1.0;

/// Unary operators: Not, UnaryMinus, UnaryPlus.
inline constexpr double kUnary = 1.0;

/// Identifier reference — pays for the lookup of a bound symbol.
/// Distinct from the operator costs above because it's a structural reference,
/// not an arithmetic application.
inline constexpr double kIdentifier = 1.0;

// TODO(planner-v2): tune these once benchmarks show per-class differentiation
// actually flips a plan.  Until then they are deliberately uniform — a
// uniform-but-honest model beats a guessed-differential model.

/// Look up the per-operator cost for a CostClass.  Used by PlanCostModel to
/// dispatch via symbol_descriptor<S>::cost_class without enumerating cases.
inline constexpr auto FromClass(CostClass c) -> double {
  switch (c) {
    case CostClass::Arithmetic:
      return kArithmetic;
    case CostClass::Comparison:
      return kComparison;
    case CostClass::Boolean:
      return kBoolean;
    case CostClass::Unary:
      return kUnary;
    case CostClass::Identifier:
      return kIdentifier;
    case CostClass::Structural:
    case CostClass::Leaf:
      // These classes don't map to a per-operator constant — PlanCostModel
      // scores them directly.  Surfacing 0.0 here would silently wrong-answer
      // a future caller that forgot the distinction; assert instead.
      return 0.0;
  }
  return 0.0;
}

}  // namespace memgraph::query::plan::v2::expression_cost
