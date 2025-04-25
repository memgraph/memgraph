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

/// @file
/// This file provides a plan rewriter which replaces `Filter` and `ScanAll`
/// operations with `ScanAllBy<Index>` if possible. The public entrypoint is
/// `RewriteWithIndexLookup`.

#pragma once

#include <unordered_set>

#include "query/frontend/ast/query/expression.hpp"

namespace memgraph::query::plan {

struct ExpressionRemovalResult {
  Expression *trimmed_expression;
  bool did_remove{false};
};

// Return the new root expression after removing the given expressions from the
// given expression tree.
ExpressionRemovalResult RemoveExpressions(Expression *expr, const std::unordered_set<Expression *> &exprs_to_remove);

}  // namespace memgraph::query::plan
