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

#include "query/plan_v2/rewrite/fold.hpp"

#include <string_view>

#include "query/plan_v2/egraph/op_ast_lists.hpp"
#include "query/typed_value.hpp"

namespace memgraph::query::plan::v2 {

namespace {

using storage::ExternalPropertyValue;

/// Map a TypedValue back to a constant. Only the scalar types arithmetic /
/// comparison / boolean folding can produce are representable; richer results
/// (lists, maps, graph values) decline so the fold leaves them alone.
auto ToConstant(TypedValue const &v) -> std::optional<ExternalPropertyValue> {
  switch (v.type()) {
    case TypedValue::Type::Null:
      return ExternalPropertyValue{};
    case TypedValue::Type::Bool:
      return ExternalPropertyValue{v.ValueBool()};
    case TypedValue::Type::Int:
      return ExternalPropertyValue{v.ValueInt()};
    case TypedValue::Type::Double:
      return ExternalPropertyValue{v.ValueDouble()};
    case TypedValue::Type::String:
      return ExternalPropertyValue{std::string_view{v.ValueString()}};
    default:
      return std::nullopt;
  }
}

// The binary/unary switch arms are generated from the operator registry's fold
// column (op_ast_lists.hpp), so an operator's evaluation and its fold rule come
// from the same row and cannot drift. The AST-node column is unused here.
// Reusing runtime TypedValue operators matches interpreter semantics exactly -
// including its signed-int64 overflow UB, now reachable at plan time.
// NOLINTBEGIN(cppcoreguidelines-macro-usage)
auto EvalBinary(symbol op, TypedValue const &lhs, TypedValue const &rhs) -> std::optional<TypedValue> {
  switch (op) {
#define MG_FOLD_BINARY(Name, AstOp, Expr) \
  case symbol::Name:                      \
    return (Expr);
    EGRAPH_BINARY_OPS(MG_FOLD_BINARY)
#undef MG_FOLD_BINARY
    default:
      return std::nullopt;
  }
}

auto EvalUnary(symbol op, TypedValue const &operand) -> std::optional<TypedValue> {
  switch (op) {
#define MG_FOLD_UNARY(Name, AstOp, Expr) \
  case symbol::Name:                     \
    return (Expr);
    EGRAPH_UNARY_OPS(MG_FOLD_UNARY)
#undef MG_FOLD_UNARY
    default:
      return std::nullopt;
  }
}

// NOLINTEND(cppcoreguidelines-macro-usage)

}  // namespace

auto FoldConstant(symbol op, std::span<ExternalPropertyValue const *const> operands)
    -> std::optional<ExternalPropertyValue> {
  try {
    std::optional<TypedValue> result;
    if (operands.size() == 2) {
      result = EvalBinary(op, TypedValue{*operands[0]}, TypedValue{*operands[1]});
    } else if (operands.size() == 1) {
      result = EvalUnary(op, TypedValue{*operands[0]});
    }
    if (!result) return std::nullopt;
    return ToConstant(*result);
    // Only the operators' own domain errors mean "decline to fold"; let anything
    // else (bad_alloc, internal bugs) propagate rather than silently mask it.
  } catch (TypedValueException const &) {
    return std::nullopt;
  }
}

}  // namespace memgraph::query::plan::v2
