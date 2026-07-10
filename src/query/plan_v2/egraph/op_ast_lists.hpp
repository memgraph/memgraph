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

// `(Symbol-name, Frontend-AST-type)` X-lists. Binary/unary opcodes grouped
// by cost class. `EGRAPH_BINARY_OPS = arithmetic + comparison + boolean`.

// NOLINTBEGIN(cppcoreguidelines-macro-usage)

#define EGRAPH_ARITHMETIC_OPS(X) \
  X(Add, AdditionOperator)       \
  X(Sub, SubtractionOperator)    \
  X(Mul, MultiplicationOperator) \
  X(Div, DivisionOperator)       \
  X(Mod, ModOperator)            \
  X(Exp, ExponentiationOperator)

#define EGRAPH_COMPARISON_OPS(X) \
  X(Eq, EqualOperator)           \
  X(Neq, NotEqualOperator)       \
  X(Lt, LessOperator)            \
  X(Lte, LessEqualOperator)      \
  X(Gt, GreaterOperator)         \
  X(Gte, GreaterEqualOperator)

#define EGRAPH_BOOLEAN_OPS(X) \
  X(And, AndOperator)         \
  X(Or, OrOperator)           \
  X(Xor, XorOperator)

#define EGRAPH_BINARY_OPS(X) \
  EGRAPH_ARITHMETIC_OPS(X)   \
  EGRAPH_COMPARISON_OPS(X)   \
  EGRAPH_BOOLEAN_OPS(X)

#define EGRAPH_UNARY_OPS(X)         \
  X(Not, NotOperator)               \
  X(UnaryMinus, UnaryMinusOperator) \
  X(UnaryPlus, UnaryPlusOperator)

// NOLINTEND(cppcoreguidelines-macro-usage)
