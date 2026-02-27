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

#include <array>
#include <string>
#include <unordered_map>

#include "planner/rewrite/rewriter.hpp"
#include "test_egraph_fixture.hpp"
#include "test_patterns.hpp"

import memgraph.planner.core.eids;

namespace memgraph::planner::core::test {

// ============================================================================
// Rewriter Test Types
// ============================================================================

using TestRewriteRule = RewriteRule<Op, NoAnalysis>;
using TestRuleSet = RuleSet<Op, NoAnalysis>;
using TestRewriter = Rewriter<Op, NoAnalysis>;
using TestRuleContext = RuleContext<Op, NoAnalysis>;

// ============================================================================
// Rewriter Test Fixture
// ============================================================================
//
// Provides e-graph building DSL (leaf, node, merge, rebuild) from EGraphTestBase
// plus rewriter-specific helpers.

class RewriterTest : public EGraphTestBase {
 protected:
  TestRewriter rewriter_{egraph};
  RewriteResult result_;
  std::unordered_map<std::string, std::size_t> rule_indices_;

  // ---------------------------------------------------------------------------
  // Setup
  // ---------------------------------------------------------------------------

  /// Install rules into the rewriter.
  template <typename... Rules>
  void use_rules(Rules &&...rules) {
    rule_indices_.clear();
    std::size_t idx = 0;
    ((rule_indices_[std::string(rules.name())] = idx++), ...);
    rewriter_.set_rules(TestRuleSet::Build(std::forward<Rules>(rules)...));
  }

  auto rewriter() -> TestRewriter & { return rewriter_; }

  // ---------------------------------------------------------------------------
  // Matcher Index Rebuild
  // ---------------------------------------------------------------------------

  void rebuild_index() { rewriter_.rebuild_index(); }

  template <typename... Ids>
  void rebuild_index_with(Ids... ids) {
    std::array<EClassId, sizeof...(Ids)> updated{ids...};
    rewriter_.rebuild_index(updated);
  }

  // ---------------------------------------------------------------------------
  // Execution
  // ---------------------------------------------------------------------------

  void saturate(RewriteConfig config = RewriteConfig::Default()) {
    result_ = rewriter_.saturate(config);
    // Invariant: total rewrites must equal sum of per-rule rewrites
    std::size_t sum = 0;
    for (auto count : result_.rewrites_per_rule) {
      sum += count;
    }
    EXPECT_EQ(result_.rewrites_applied, sum) << "Total rewrites doesn't match per-rule sum";
  }

  // ---------------------------------------------------------------------------
  // Assertions
  // ---------------------------------------------------------------------------

  void expect_saturated() { EXPECT_TRUE(result_.saturated()); }

  void expect_not_saturated() { EXPECT_FALSE(result_.saturated()); }

  void expect_rewrites(std::size_t n) { EXPECT_EQ(result_.rewrites_applied, n); }

  void expect_iterations(std::size_t n) { EXPECT_EQ(result_.iterations, n); }

  void expect_stop_reason(RewriteResult::StopReason reason) { EXPECT_EQ(result_.stop_reason, reason); }

  void expect_rule_rewrites(TestRewriteRule const &rule, std::size_t n) {
    auto idx = rule_index(rule);
    ASSERT_LT(idx, result_.rewrites_per_rule.size());
    EXPECT_EQ(result_.rewrites_per_rule[idx], n);
  }

 private:
  auto rule_index(TestRewriteRule const &rule) const -> std::size_t {
    auto it = rule_indices_.find(std::string(rule.name()));
    EXPECT_NE(it, rule_indices_.end()) << "Rule not found: " << rule.name();
    return it->second;
  }
};

}  // namespace memgraph::planner::core::test
