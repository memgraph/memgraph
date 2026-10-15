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

/// @file
/// This file provides a plan rewriter which replaces `Filter` and `ScanAll`
/// operations with `ScanAllBy<Index>` if possible. The public entrypoint is
/// `RewriteWithIndexLookup`.

#pragma once

#include <memory>
#include <unordered_set>

#include "query/frontend/ast/query/expression.hpp"

namespace memgraph::query {
class AstStorage;
class Identifier;
class SymbolTable;
}  // namespace memgraph::query

namespace memgraph::query::plan {

class LogicalOperator;

struct ExpressionRemovalResult {
  Expression *trimmed_expression;
  bool did_remove{false};
};

/// A membership list lowered to an Unwind for a per-element index scan.
struct UnwoundMembershipList {
  /// Unwind(toSet(coalesce(list, []))) wrapping the caller's input.
  std::shared_ptr<LogicalOperator> op;
  /// Identifier bound to each unwound element, to drive the downstream scan.
  Identifier *element;
};

/// Lower an `x IN list` filter to the input of a per-element index scan. Wraps
/// `input` in Unwind(toSet(coalesce(list_expr, []))): coalesce turns a null
/// list into zero rows instead of throwing in Unwind (matching the membership
/// Filter; a non-list scalar still throws), and toSet dedups so a value
/// repeated in the list drives a single scan. The dedup collapses whole-number
/// doubles onto their int, safe only because the downstream scan unifies them
/// too: an id scan coerces to an int64 id, and the property index compares
/// numerics cross-type, so a collapsed value still matches what the raw list
/// would.
UnwoundMembershipList UnwindMembershipList(SymbolTable &symbol_table, AstStorage *ast_storage,
                                           std::shared_ptr<LogicalOperator> input, Expression *list_expr);

// Return the new root expression after removing the given expressions from the
// given expression tree. This function does NOT modify the original expression tree;
// instead it creates new AndOperator nodes when needed using the provided storage.
ExpressionRemovalResult RemoveExpressions(Expression *expr, const std::unordered_set<Expression *> &exprs_to_remove,
                                          AstStorage *storage);

}  // namespace memgraph::query::plan
