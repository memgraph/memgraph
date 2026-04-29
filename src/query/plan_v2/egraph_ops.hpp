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

// Master X-lists for arithmetic, comparison, boolean, and unary symbol categories.
//
// Each row is (Symbol-name, Frontend-AST-type).  The AST-type column is
// opaque text in this header — token-pasted at expansion sites — and is only
// resolved as a real type by the two converter .cpps that already include
// the frontend AST headers.  egraph.hpp expands these lists with a callback
// that ignores the AST-type column entirely, so this header has zero
// dependencies and is safe to include from the public egraph API.
//
// Cross-check (in egraph.cpp): every entry must satisfy is_binary_op_v / is_unary_op_v,
// and the X-list count must match the count of binary/unary symbols in AllSymbolsSeq.
// Adding a new binary/unary symbol therefore requires updating the symbol enum,
// AllSymbolsSeq, symbol_descriptor, AND this list — the cross-check fails loudly
// if any are out of sync.

// NOLINTBEGIN(cppcoreguidelines-macro-usage)

#define EGRAPH_BINARY_OPS(X)     \
  X(Add, AdditionOperator)       \
  X(Sub, SubtractionOperator)    \
  X(Mul, MultiplicationOperator) \
  X(Div, DivisionOperator)       \
  X(Mod, ModOperator)            \
  X(Exp, ExponentiationOperator) \
  X(Eq, EqualOperator)           \
  X(Neq, NotEqualOperator)       \
  X(Lt, LessOperator)            \
  X(Lte, LessEqualOperator)      \
  X(Gt, GreaterOperator)         \
  X(Gte, GreaterEqualOperator)   \
  X(And, AndOperator)            \
  X(Or, OrOperator)              \
  X(Xor, XorOperator)

#define EGRAPH_UNARY_OPS(X)         \
  X(Not, NotOperator)               \
  X(UnaryMinus, UnaryMinusOperator) \
  X(UnaryPlus, UnaryPlusOperator)

// NOLINTEND(cppcoreguidelines-macro-usage)
