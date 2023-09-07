// Copyright 2023 Memgraph Ltd.
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

int64_t EvaluateInt(ExpressionEvaluator *evaluator, Expression *expr, const std::string &what) {
  TypedValue value = expr->Accept(*evaluator);
  try {
    return value.ValueInt();
  } catch (TypedValueException &e) {
    throw QueryRuntimeException(what + " must be an int");
  }
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
