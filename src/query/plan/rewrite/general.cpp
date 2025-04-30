// Copyright 2025 Memgraph Ltd.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt; by using this file, you agree to be bound by the terms of the Business Source
// License, and you may not use this file except in compliance with the Business Source License.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

#include "query/plan/rewrite/general.hpp"

#include "query/frontend/ast/ast.hpp"

namespace memgraph::query::plan {

ExpressionRemovalResult RemoveExpressions(Expression *expr, const std::unordered_set<Expression *> &exprs_to_remove) {
  if (utils::Contains(exprs_to_remove, expr)) {
    return ExpressionRemovalResult{.trimmed_expression = nullptr, .did_remove = true};
  }

  auto *and_op = utils::Downcast<AndOperator>(expr);

  // currently we are processing expressions by dividing them into and disjoint expressions
  // no work needed if there is no multiple and expressions
  if (!and_op) return ExpressionRemovalResult{.trimmed_expression = expr};

  // and operation is fully contained inside the expressions to remove
  if (utils::Contains(exprs_to_remove, and_op)) {
    return ExpressionRemovalResult{.trimmed_expression = nullptr, .did_remove = true};
  }

  bool did_remove = false;
  if (utils::Contains(exprs_to_remove, and_op->expression1_)) {
    and_op->expression1_ = nullptr;
    did_remove = true;
  }
  if (utils::Contains(exprs_to_remove, and_op->expression2_)) {
    and_op->expression2_ = nullptr;
    did_remove = true;
  }

  auto removal1 = RemoveExpressions(and_op->expression1_, exprs_to_remove);
  and_op->expression1_ = removal1.trimmed_expression;
  did_remove = did_remove || removal1.did_remove;

  auto removal2 = RemoveExpressions(and_op->expression2_, exprs_to_remove);
  and_op->expression2_ = removal2.trimmed_expression;
  did_remove = did_remove || removal2.did_remove;

  // Removed both the right and left side of the AND => remove the whole AND operator
  if (!and_op->expression1_ && !and_op->expression2_) {
    return ExpressionRemovalResult{.trimmed_expression = nullptr, .did_remove = did_remove};
  }

  // Removed the left side => return only the right side
  if (and_op->expression1_ && !and_op->expression2_) {
    return ExpressionRemovalResult{.trimmed_expression = and_op->expression1_, .did_remove = did_remove};
  }

  // Removed the right side => return only the left side
  if (and_op->expression2_ && !and_op->expression1_) {
    return ExpressionRemovalResult{.trimmed_expression = and_op->expression2_, .did_remove = did_remove};
  }

  // Nothing removed => return whole expression as is
  return ExpressionRemovalResult{.trimmed_expression = and_op, .did_remove = did_remove};
}

}  // namespace memgraph::query::plan
