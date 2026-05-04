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

#include "bench_common.hpp"
#include "planner/rewrite/rewriter.hpp"

namespace memgraph::planner::bench {

// ============================================================================
// Rewrite-Specific Types
// ============================================================================

namespace rewrite = core::rewrite;

using rewrite::Match;
using rewrite::RewriteConfig;

using TestRewriteRule = rewrite::RewriteRule<Op, NoAnalysis>;
using TestRuleSet = rewrite::RuleSet<Op, NoAnalysis>;
using TestRewriter = rewrite::Rewriter<Op, NoAnalysis>;
using TestRuleContext = rewrite::RuleContext<Op, NoAnalysis>;
using TestRewriteContext = rewrite::RewriteContext<Op, NoAnalysis>;

// ============================================================================
// Rule Builders
// ============================================================================

using pattern::dsl::Var;

inline auto RuleDoubleNeg() {
  return TestRewriteRule::Builder{"double_neg"}
      .pattern(PatternDoubleNeg())
      .apply([](TestRuleContext &ctx, Match const &m) { ctx.merge(m[kRootDoubleNeg], m[kX]); });
}

inline auto RuleNoOp() {
  return TestRewriteRule::Builder{"noop"}.pattern(PatternAdd()).apply([](TestRuleContext &, Match const &) {});
}

inline auto RuleMergeAddMul() {
  return TestRewriteRule::Builder{"merge_add_mul"}
      .pattern(TestPattern::build(kRootAdd, Op::Add, {Var{kX}, Var{kY}}), "add")
      .pattern(TestPattern::build(kRootMul, Op::Mul, {Var{kX}, Var{kY}}), "mul")
      .apply([](TestRuleContext &ctx, Match const &m) { ctx.merge(m[kRootAdd], m[kRootMul]); });
}

inline auto RuleThreePattern() {
  return TestRewriteRule::Builder{"three"}
      .pattern(TestPattern::build(kRootAdd, Op::Add, {Var{kX}, Var{kY}}), "add")
      .pattern(TestPattern::build(kRootMul, Op::Mul, {Var{kX}, Var{kZ}}), "mul")
      .pattern(TestPattern::build(kRootNeg, Op::Neg, {Var{kX}}), "neg")
      .apply([](TestRuleContext &, Match const &) {});
}

inline auto RuleCartesian() {
  return TestRewriteRule::Builder{"cartesian"}
      .pattern(TestPattern::build(kRootAdd, Op::Add, {Var{kX}, Var{kY}}), "add")
      .pattern(TestPattern::build(Op::Neg, {Var{kZ}}), "neg")
      .apply([](TestRuleContext &, Match const &) {});
}

inline auto RuleWideJoin() {
  return TestRewriteRule::Builder{"wide"}
      .pattern(TestPattern::build(kRootAdd, Op::Add, {Var{kX}, Var{kY}}), "add")
      .pattern(TestPattern::build(kRootMul, Op::Mul, {Var{kX}, Var{kZ}}), "mul")
      .apply([](TestRuleContext &, Match const &) {});
}

// ============================================================================
// Rewriter Fixture Base
// ============================================================================

class RewriterFixtureBase : public benchmark::Fixture {
 protected:
  TestEGraph egraph_;
  std::unique_ptr<TestMatcherIndex> matcher_;
  std::unique_ptr<TestRewriter> rewriter_;
  TestRewriteContext rewrite_context_{egraph_};

  void ResetEGraph() { egraph_ = TestEGraph{}; }

  void CreateMatcher() { matcher_ = std::make_unique<TestMatcherIndex>(egraph_); }

  void CreateRewriter(TestRuleSet const &rules) { rewriter_ = std::make_unique<TestRewriter>(egraph_, rules); }

  template <typename BuilderFn>
  void SetupGraphAndMatcher(BuilderFn &&build_fn) {
    ResetEGraph();
    build_fn(egraph_);
    CreateMatcher();
  }

  template <typename BuilderFn>
  void SetupGraphAndRewriter(BuilderFn &&build_fn, TestRuleSet const &rules) {
    ResetEGraph();
    build_fn(egraph_);
    CreateRewriter(rules);
  }
};

}  // namespace memgraph::planner::bench
