// Copyright 2024 Memgraph Ltd.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt; by using this file, you agree to be bound by the terms of the Business Source
// License, and you may not use this file except in compliance with the Business Source License.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

#include "query/interpret/eval.hpp"

namespace memgraph::query {

int64_t EvaluateInt(ExpressionVisitor<TypedValue> &eval, Expression *expr, std::string_view what) {
  TypedValue value = expr->Accept(eval);
  try {
    return value.ValueInt();
  } catch (TypedValueException &e) {
    throw QueryRuntimeException(std::string(what) + " must be an int");
  }
}

std::optional<int64_t> EvaluateUint(ExpressionVisitor<TypedValue> &eval, Expression *expr, std::string_view what) {
  if (!expr) {
    return std::nullopt;
  }

  TypedValue value = expr->Accept(eval);
  try {
    auto value_uint = value.ValueInt();
    if (value_uint < 0) {
      throw QueryRuntimeException(std::string(what) + " must be a non-negative integer");
    }
    return value_uint;
  } catch (TypedValueException &e) {
    throw QueryRuntimeException(std::string(what) + " must be a non-negative integer");
  }
}

std::optional<int64_t> EvaluateHopsLimit(ExpressionVisitor<TypedValue> &eval, Expression *expr) {
  return EvaluateUint(eval, expr, "Hops limit");
}

std::optional<int64_t> EvaluateCommitFrequency(ExpressionVisitor<TypedValue> &eval, Expression *expr) {
  return EvaluateUint(eval, expr, "Commit frequency");
}

std::optional<int64_t> EvaluateDeleteBufferSize(ExpressionVisitor<TypedValue> &eval, Expression *expr) {
  return EvaluateUint(eval, expr, "Delete buffer size");
}

std::optional<size_t> EvaluateMemoryLimit(ExpressionVisitor<TypedValue> &eval, Expression *memory_limit,
                                          size_t memory_scale) {
  if (!memory_limit) return std::nullopt;
  auto limit_value = memory_limit->Accept(eval);
  if (!limit_value.IsInt() || limit_value.ValueInt() <= 0)
    throw QueryRuntimeException("Memory limit must be a non-negative integer.");
  size_t limit = limit_value.ValueInt();
  if (std::numeric_limits<size_t>::max() / memory_scale < limit) throw QueryRuntimeException("Memory limit overflow.");
  return limit * memory_scale;
}

}  // namespace memgraph::query
