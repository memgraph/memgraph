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

#include "planner/rewrite/rewriter.hpp"
#include "query/plan_v2/egraph.hpp"

namespace memgraph::query::plan::v2 {

// Import generic rewrite types from planner/core
using planner::core::RewriteConfig;
using planner::core::RewriteResult;

/**
 * @brief Apply identifier inlining rewrite to the e-graph
 *
 * This rewrite finds patterns where:
 * - Bind(input, sym, expr) binds an identifier to an expression
 * - Identifier(sym) references that identifier
 *
 * It merges the Identifier's e-class with expr's e-class, effectively
 * inlining the bound expression wherever the identifier is used.
 *
 * Example:
 *   RETURN x AS y, y + 1
 *   Before: Bind(Once, sym_x, x), Identifier(sym_x) for "y + 1"
 *   After:  Identifier(sym_x) merged with x's e-class
 *
 * @param eg The e-graph to apply the rewrite to
 * @return Number of merges performed
 */
auto ApplyInlineRewrite(egraph &eg) -> std::size_t;

/**
 * @brief Apply all optimization rewrites to the e-graph
 *
 * Runs all available rewrites until a fixed point or a limit is reached.
 *
 * @param eg The e-graph to optimize
 * @param config Configuration controlling limits and timeout
 * @return Result containing statistics and stop reason
 */
auto ApplyAllRewrites(egraph &eg, RewriteConfig const &config = RewriteConfig::Default()) -> RewriteResult;

}  // namespace memgraph::query::plan::v2
