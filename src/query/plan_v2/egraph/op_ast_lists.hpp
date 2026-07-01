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

// The operator registry: one `X(Symbol, Frontend-AST-type, fold-op)` row per
// expression operator, grouped by cost class. Columns:
//   1: the e-graph symbol name.
//   2: the v1 frontend AST node it lowers to / reconstructs from.
//   3: the constant fold - an expression over operand TypedValues `lhs`/`rhs`
//      (binary) or `operand` (unary); fold.cpp generates Eval* from it.
// Rule generation and fold evaluation read the same rows, so an operator cannot
// get a fold rule without its evaluation. `EGRAPH_BINARY_OPS = arithmetic +
// comparison + boolean`; a consumer drops unwanted columns via trailing `, ...)`.

// NOLINTBEGIN(cppcoreguidelines-macro-usage)

#define EGRAPH_ARITHMETIC_OPS(X)           \
  X(Add, AdditionOperator, lhs + rhs)      \
  X(Sub, SubtractionOperator, lhs - rhs)   \
  X(Mul, MultiplicationOperator, lhs *rhs) \
  X(Div, DivisionOperator, lhs / rhs)      \
  X(Mod, ModOperator, lhs % rhs)           \
  X(Exp, ExponentiationOperator, pow(lhs, rhs))

#define EGRAPH_COMPARISON_OPS(X)        \
  X(Eq, EqualOperator, lhs == rhs)      \
  X(Neq, NotEqualOperator, lhs != rhs)  \
  X(Lt, LessOperator, lhs < rhs)        \
  X(Lte, LessEqualOperator, lhs <= rhs) \
  X(Gt, GreaterOperator, lhs > rhs)     \
  X(Gte, GreaterEqualOperator, lhs >= rhs)

#define EGRAPH_BOOLEAN_OPS(X)    \
  X(And, AndOperator, lhs &&rhs) \
  X(Or, OrOperator, lhs || rhs)  \
  X(Xor, XorOperator, lhs ^ rhs)

#define EGRAPH_BINARY_OPS(X) \
  EGRAPH_ARITHMETIC_OPS(X)   \
  EGRAPH_COMPARISON_OPS(X)   \
  EGRAPH_BOOLEAN_OPS(X)

#define EGRAPH_UNARY_OPS(X)                   \
  X(Not, NotOperator, !operand)               \
  X(UnaryMinus, UnaryMinusOperator, -operand) \
  X(UnaryPlus, UnaryPlusOperator, +operand)

// NOLINTEND(cppcoreguidelines-macro-usage)
