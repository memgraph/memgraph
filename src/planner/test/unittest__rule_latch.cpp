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

#include <optional>
#include <vector>

#include <gtest/gtest.h>

#include "planner/rewrite/arming_index.hpp"
#include "planner/rewrite/rule_latch.hpp"
#include "test_support/types.hpp"

// The rule latch is the incremental-saturation scheduler in isolation: given an
// arming index, a max pattern depth, and an e-graph with a touched-set, it
// decides which rules to arm and (when the change is sparse) which e-classes a
// matcher may restrict to. These tests drive it directly - no Rewriter, no
// RuleSet, no saturation loop - so an arming decision is a pure input -> output
// assertion rather than something inferred from per-rule rewrite counts.

namespace memgraph::planner::core {

using namespace test;
using rewrite::ArmingIndex;
using rewrite::RuleLatch;

namespace {
using Latch = RuleLatch<Op, NoAnalysis>;
using Roots = std::vector<std::vector<std::optional<Op>>>;
}  // namespace

TEST(RuleLatch, FirstArmArmsEveryRuleAndMatchesEveryCandidate) {
  TestEGraph eg;
  eg.emplace(Op::A);
  auto const index = ArmingIndex<Op>::from_root_symbols(Roots{{Op::A}, {Op::B}, {Op::C}});

  Latch latch;
  latch.reset(index, /*max_pattern_depth=*/0, /*num_rules=*/3);
  latch.arm(eg);

  EXPECT_EQ(latch.armed(), (boost::unordered_flat_set<std::size_t>{0, 1, 2}));
  EXPECT_EQ(latch.active(), nullptr) << "first arm matches every candidate, so no active-set restriction";
}

TEST(RuleLatch, EmptyTouchedArmsOnlyAlwaysArmedRules) {
  TestEGraph eg;
  eg.emplace(Op::A);
  eg.emplace(Op::B);
  // Rule 0 roots at Op::A; rule 1 has a symbol-less root, so it is always armed.
  auto const index = ArmingIndex<Op>::from_root_symbols(Roots{{Op::A}, {std::nullopt}});

  Latch latch;
  latch.reset(index, /*max_pattern_depth=*/0, /*num_rules=*/2);
  latch.arm(eg);  // arm-all, consumes the pending flag
  eg.clear_touched();
  latch.arm(eg);  // nothing touched since

  EXPECT_EQ(latch.armed(), (boost::unordered_flat_set<std::size_t>{1})) << "only the always-armed rule survives";
  // A settled but non-empty graph is "sparse", so active() is non-null and EMPTY
  // (restrict to nothing) - distinct from nullptr (match all). The always-armed
  // rule still fires because a symbol-less root ignores the active set.
  ASSERT_NE(latch.active(), nullptr);
  EXPECT_TRUE(latch.active()->empty());
}

TEST(RuleLatch, TouchedSymbolArmsItsIndexedRulesAndKeepsSparseActiveSet) {
  TestEGraph eg;
  eg.emplace(Op::A);
  eg.emplace(Op::B);
  for (std::uint64_t i = 0; i < 4; ++i) eg.emplace(Op::C, i);  // padding: keeps the change sparse
  auto const index = ArmingIndex<Op>::from_root_symbols(Roots{{Op::A}, {Op::B}});

  Latch latch;
  latch.reset(index, /*max_pattern_depth=*/0, /*num_rules=*/2);
  latch.arm(eg);
  eg.clear_touched();
  auto const fresh_a = eg.emplace(Op::A, /*disambiguator=*/99).eclass_id;  // touch one A-class
  latch.arm(eg);

  EXPECT_EQ(latch.armed(), (boost::unordered_flat_set<std::size_t>{0})) << "only the Op::A-rooted rule arms";
  ASSERT_NE(latch.active(), nullptr) << "a one-class change in a larger graph is sparse";
  EXPECT_TRUE(latch.active()->contains(eg.find(fresh_a)));
}

TEST(RuleLatch, ParentClosureArmsAnAncestorRootedRuleToDepth) {
  // a <- F(a) <- G(F(a)). A change at the leaf `a` must arm a rule rooted at the
  // grandparent symbol G, but only when the closure depth reaches it.
  auto build = [](TestEGraph &eg) {
    auto const a = eg.emplace(Op::A).eclass_id;
    auto const fa = eg.emplace(Op::F, {a}).eclass_id;
    eg.emplace(Op::G, {fa});
    auto const b = eg.emplace(Op::B).eclass_id;
    eg.clear_touched();
    eg.merge(a, b);  // touch the class holding `a`, whose ancestors are F(a), G(F(a))
    TestProcessingContext pc;
    eg.rebuild(pc);
  };
  auto const index = ArmingIndex<Op>::from_root_symbols(Roots{{Op::G}});

  TestEGraph deep_enough;
  build(deep_enough);
  Latch to_depth_two;
  to_depth_two.reset(index, /*max_pattern_depth=*/2, /*num_rules=*/1);
  to_depth_two.arm(deep_enough);  // arm-all, consume pending
  to_depth_two.arm(deep_enough);  // arm from the merge's touched-set
  EXPECT_TRUE(to_depth_two.armed().contains(0)) << "depth-2 closure from `a` reaches G(F(a))";

  TestEGraph too_shallow;
  build(too_shallow);
  Latch to_depth_one;
  to_depth_one.reset(index, /*max_pattern_depth=*/1, /*num_rules=*/1);
  to_depth_one.arm(too_shallow);
  to_depth_one.arm(too_shallow);
  EXPECT_FALSE(to_depth_one.armed().contains(0)) << "depth-1 closure stops at F(a); G is unreached";
}

TEST(RuleLatch, NonSparseChangeDropsTheActiveSet) {
  TestEGraph eg;
  eg.emplace(Op::A);
  auto const index = ArmingIndex<Op>::from_root_symbols(Roots{{Op::A}});

  Latch latch;
  latch.reset(index, /*max_pattern_depth=*/0, /*num_rules=*/1);
  latch.arm(eg);
  eg.clear_touched();
  eg.emplace(Op::A, /*disambiguator=*/7);  // touched half the (now 2) classes
  latch.arm(eg);

  EXPECT_EQ(latch.active(), nullptr) << "change is not a sparse slice, so match via arming alone";
  EXPECT_TRUE(latch.armed().contains(0)) << "the touched Op::A still arms its rule";
}

TEST(RuleLatch, ResetReArmsEveryRule) {
  TestEGraph eg;
  eg.emplace(Op::A);
  auto const index = ArmingIndex<Op>::from_root_symbols(Roots{{Op::A}, {Op::B}});

  Latch latch;
  latch.reset(index, /*max_pattern_depth=*/0, /*num_rules=*/2);
  latch.arm(eg);
  eg.clear_touched();
  latch.arm(eg);  // arm-from-touched: settled, arms nothing symbol-rooted
  EXPECT_FALSE(latch.armed().contains(1));

  latch.reset(index, /*max_pattern_depth=*/0, /*num_rules=*/2);  // e.g. after set_rules
  latch.arm(eg);
  EXPECT_EQ(latch.armed(), (boost::unordered_flat_set<std::size_t>{0, 1})) << "reset re-arms all on the next arm";
}

}  // namespace memgraph::planner::core
