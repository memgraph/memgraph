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

#include "query/typed_value.hpp"

namespace memgraph::query::plan::v2 {

using enum symbol;

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

auto EvalBinary(symbol op, TypedValue const &a, TypedValue const &b) -> std::optional<TypedValue> {
  switch (op) {
    case Add:
      return a + b;
    case Sub:
      return a - b;
    case Mul:
      return a * b;
    case Div:
      return a / b;
    case Mod:
      return a % b;
    case Exp:
      return pow(a, b);
    case Eq:
      return a == b;
    case Neq:
      return a != b;
    case Lt:
      return a < b;
    case Lte:
      return a <= b;
    case Gt:
      return a > b;
    case Gte:
      return a >= b;
    case And:
      return a && b;
    case Or:
      return a || b;
    case Xor:
      return a ^ b;
    default:
      return std::nullopt;
  }
}

auto EvalUnary(symbol op, TypedValue const &a) -> std::optional<TypedValue> {
  switch (op) {
    case Not:
      return !a;
    case UnaryMinus:
      return -a;
    case UnaryPlus:
      return +a;
    default:
      return std::nullopt;
  }
}

}  // namespace

auto FoldConstant(symbol op, std::span<ExternalPropertyValue const> operands) -> std::optional<ExternalPropertyValue> {
  try {
    std::optional<TypedValue> result;
    if (operands.size() == 2) {
      result = EvalBinary(op, TypedValue{operands[0]}, TypedValue{operands[1]});
    } else if (operands.size() == 1) {
      result = EvalUnary(op, TypedValue{operands[0]});
    }
    if (!result) return std::nullopt;
    return ToConstant(*result);
  } catch (...) {
    return std::nullopt;
  }
}

}  // namespace memgraph::query::plan::v2
