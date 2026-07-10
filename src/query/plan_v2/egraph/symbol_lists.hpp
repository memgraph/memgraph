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

// Name-only X-lists partitioning every `symbol` by arity. This is the
// structural definition of "what symbols exist"; the `symbol` enum,
// `symbol_descriptor<S>::arity`, and `AllSymbols` in symbol.hpp
// are all derived from these lists.
//
// AST-node mappings for the codegen-eligible subset (binary/unary expression
// operators, partitioned by cost class) live separately in op_ast_lists.hpp.
//
// Adding a new symbol: append it to the relevant arity list here. If the
// symbol has a direct AST node and goes through CostModel's switch, also
// append to the matching EGRAPH_*_OPS list in op_ast_lists.hpp. The cross-check
// in egraph.cpp fires if cost-class membership disagrees with this taxonomy.

// NOLINTBEGIN(cppcoreguidelines-macro-usage)

#define EGRAPH_LEAF_SYMBOLS(X)                             \
  X(Once)                                                  \
  X(Symbol) /* Symbol e-classes must remain singletons. */ \
  X(Literal)                                               \
  X(ParamLookup)

#define EGRAPH_UNARY_SYMBOLS(X) \
  X(Identifier)                 \
  X(Not)                        \
  X(UnaryMinus)                 \
  X(UnaryPlus)

#define EGRAPH_BINARY_SYMBOLS(X) \
  X(Add) X(Sub) X(Mul) X(Div) X(Mod) X(Exp) X(Eq) X(Neq) X(Lt) X(Lte) X(Gt) X(Gte) X(And) X(Or) X(Xor)

#define EGRAPH_SPECIAL_SYMBOLS(X)                                                      \
  X(Bind)                                                                              \
  X(Output)                                                                            \
  X(NamedOutput)                                                                       \
  /* Function call (builtin or UDF); disambiguator is the per-egraph function id. */   \
  X(Function)                                                                          \
  /* UNWIND clause: 3 children [input, sym, list_expr]; mirrors Bind's shape. */       \
  X(Unwind)                                                                            \
  /* CALL { ... } subquery: variadic [outer_input, inner_root, exposed_sym_1, ...]. */ \
  /* Acts as a scope barrier: inner's introduces are stripped at the boundary; */      \
  /* only the explicit exposed_sym children become visible to the outer scope. */      \
  X(Subquery)

#define EGRAPH_ALL_SYMBOLS(X) \
  EGRAPH_LEAF_SYMBOLS(X)      \
  EGRAPH_UNARY_SYMBOLS(X)     \
  EGRAPH_BINARY_SYMBOLS(X)    \
  EGRAPH_SPECIAL_SYMBOLS(X)

// NOLINTEND(cppcoreguidelines-macro-usage)
