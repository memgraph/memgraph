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

#include "query/plan/rewrite/general.hpp"

#include "query/frontend/ast/ast.hpp"

namespace memgraph::query::plan {

ExpressionRemovalResult RemoveExpressions(Expression *expr, const std::unordered_set<Expression *> &exprs_to_remove,
                                          AstStorage *storage) {
  // Handle null input
  if (!expr) return ExpressionRemovalResult{.trimmed_expression = nullptr, .did_remove = false};

  // If this expression should be removed, return null
  if (exprs_to_remove.contains(expr)) {
    return ExpressionRemovalResult{.trimmed_expression = nullptr, .did_remove = true};
  }

  auto *and_op = utils::Downcast<AndOperator>(expr);

  // Currently we are processing expressions by dividing them into and disjoint expressions
  // No work needed if there is no multiple and expressions
  if (!and_op) return ExpressionRemovalResult{.trimmed_expression = expr, .did_remove = false};

  // Recursively process both children WITHOUT modifying the original
  Expression *child1 = and_op->expression1_;
  Expression *child2 = and_op->expression2_;

  // Check if children are directly in exprs_to_remove
  const bool child1_removed = exprs_to_remove.contains(child1);
  const bool child2_removed = exprs_to_remove.contains(child2);

  // Recursively process children that weren't directly removed
  const ExpressionRemovalResult removal1 =
      child1_removed ? ExpressionRemovalResult{.trimmed_expression = nullptr, .did_remove = true}
                     : RemoveExpressions(child1, exprs_to_remove, storage);
  const ExpressionRemovalResult removal2 =
      child2_removed ? ExpressionRemovalResult{.trimmed_expression = nullptr, .did_remove = true}
                     : RemoveExpressions(child2, exprs_to_remove, storage);

  const bool did_remove = removal1.did_remove || removal2.did_remove;
  Expression *new_child1 = removal1.trimmed_expression;
  Expression *new_child2 = removal2.trimmed_expression;

  // Removed both children => remove the whole AND operator
  if (!new_child1 && !new_child2) {
    return ExpressionRemovalResult{.trimmed_expression = nullptr, .did_remove = did_remove};
  }

  // Only child1 survives => return child1
  if (new_child1 && !new_child2) {
    return ExpressionRemovalResult{.trimmed_expression = new_child1, .did_remove = did_remove};
  }

  // Only child2 survives => return child2
  if (new_child2 && !new_child1) {
    return ExpressionRemovalResult{.trimmed_expression = new_child2, .did_remove = did_remove};
  }

  // Both children survive
  // If neither child changed, return the original AND operator
  if (new_child1 == child1 && new_child2 == child2) {
    return ExpressionRemovalResult{.trimmed_expression = and_op, .did_remove = did_remove};
  }

  // At least one child changed, create a new AND operator with the new children
  // to avoid mutating the original expression tree
  auto *new_and_op = storage->Create<AndOperator>();
  new_and_op->expression1_ = new_child1;
  new_and_op->expression2_ = new_child2;
  return ExpressionRemovalResult{.trimmed_expression = new_and_op, .did_remove = did_remove};
}

}  // namespace memgraph::query::plan
