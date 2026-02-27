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
#include "test_patterns.hpp"

namespace memgraph::planner::core::test {

// ============================================================================
// Rewriter Test Types
// ============================================================================

using TestRewriteRule = RewriteRule<Op, NoAnalysis>;
using TestRuleContext = RuleContext<Op, NoAnalysis>;

// ============================================================================
// Reusable Test Rules
// ============================================================================
//
// Common rewrite rules for testing. These are extracted here so multiple
// test files can share them without duplication.

/**
 * @brief Double negation elimination: Neg(Neg(?x)) -> ?x
 *
 * A mathematically valid identity. Merges the double-negated expression
 * with its inner value.
 */
inline auto make_double_neg_rule() -> TestRewriteRule {
  return TestRewriteRule::Builder{"double_negation"}
      .pattern(make_double_neg_pattern())
      .apply([](TestRuleContext &ctx, Match const &match) { ctx.merge(match[kVarDoubleNegRoot], match[kVarX]); });
}

/**
 * @brief Synthetic idempotent rule: F(?x, ?x) -> ?x
 *
 * Uses F (no mathematical semantics) for testing multi-rule scenarios.
 * Merges F(x,x) with x.
 */
inline auto make_idempotent_f_rule() -> TestRewriteRule {
  return TestRewriteRule::Builder{"idempotent_f"}
      .pattern(TestPattern::build(Op::F, {Var{kVarX}, Var{kVarX}}, kVarRoot))
      .apply([](TestRuleContext &ctx, Match const &match) { ctx.merge(match[kVarRoot], match[kVarX]); });
}

/**
 * @brief Synthetic chain join rule: F(?x), F2(?x, ?y), F(?y) -> merge
 *
 * Uses F/F2 (no mathematical semantics) for testing 3-pattern transitive joins.
 * P1 and P3 share no variables directly but are connected through P2.
 */
inline auto make_chain_join_rule() -> TestRewriteRule {
  return TestRewriteRule::Builder{"chain_join"}
      .pattern(TestPattern::build(Op::F, {Var{kVarX}}, kVarRootP1), "f_x")
      .pattern(TestPattern::build(Op::F2, {Var{kVarX}, Var{kVarY}}), "f2_xy")
      .pattern(TestPattern::build(Op::F, {Var{kVarY}}, kVarRootP3), "f_y")
      .apply([](TestRuleContext &ctx, Match const &match) { ctx.merge(match[kVarRootP1], match[kVarRootP3]); });
}

/**
 * @brief Merge any two Var nodes: Var(?x), Var(?y) -> merge
 *
 * Useful for testing congruence propagation after merge.
 */
inline auto make_merge_vars_rule() -> TestRewriteRule {
  return TestRewriteRule::Builder{"merge_vars"}
      .pattern(TestPattern::build(Op::Var, kVarX))
      .pattern(TestPattern::build(Op::Var, kVarY))
      .apply([](TestRuleContext &ctx, Match const &match) { ctx.merge(match[kVarX], match[kVarY]); });
}

}  // namespace memgraph::planner::core::test
