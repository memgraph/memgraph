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

#include "query/plan/rewrite/index_lookup.hpp"

#include "utils/flag_validation.hpp"

// NOLINTNEXTLINE (cppcoreguidelines-avoid-non-const-global-variables)
DEFINE_VALIDATED_int64(query_vertex_count_to_expand_existing, 10,
                       "Maximum count of indexed vertices which provoke "
                       "indexed lookup and then expand to existing, instead of "
                       "a regular expand. Default is 10, to turn off use -1.",
                       FLAG_IN_RANGE(-1, std::numeric_limits<std::int64_t>::max()));

namespace memgraph::query::plan::impl {

ExpressionRemovalResult RemoveAndExpressions(Expression *expr,
                                             const std::unordered_set<Expression *> &exprs_to_remove) {
  auto *and_op = utils::Downcast<AndOperator>(expr);
  if (!and_op) return ExpressionRemovalResult{.expression = expr};
  if (utils::Contains(exprs_to_remove, and_op)) {
    return ExpressionRemovalResult{.expression = nullptr, .removed = true};
  }

  bool removed = false;
  if (utils::Contains(exprs_to_remove, and_op->expression1_)) {
    and_op->expression1_ = nullptr;
    removed = true;
  }
  if (utils::Contains(exprs_to_remove, and_op->expression2_)) {
    and_op->expression2_ = nullptr;
    removed = true;
  }

  auto removal1 = RemoveAndExpressions(and_op->expression1_, exprs_to_remove);
  and_op->expression1_ = removal1.expression;
  removed = removed || removal1.removed;

  auto removal2 = RemoveAndExpressions(and_op->expression2_, exprs_to_remove);
  and_op->expression2_ = removal2.expression;
  removed = removed || removal2.removed;

  if (!and_op->expression1_ && !and_op->expression2_) {
    return ExpressionRemovalResult{.expression = nullptr, .removed = removed};
  }

  if (and_op->expression1_ && !and_op->expression2_) {
    return ExpressionRemovalResult{.expression = and_op->expression1_, .removed = removed};
  }

  if (and_op->expression2_ && !and_op->expression1_) {
    return ExpressionRemovalResult{.expression = and_op->expression2_, .removed = removed};
  }

  return ExpressionRemovalResult{.expression = and_op, .removed = removed};
}

}  // namespace memgraph::query::plan::impl
