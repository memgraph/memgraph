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

// The rule latch is the incremental-saturation arming engine in isolation: given an
// arming index, a max pattern depth, and an e-graph with a touched-set, it
// decides which rules to arm and (when the change is sparse) which e-classes a
// matcher may restrict to. These tests drive it directly - no Rewriter, no
// RuleSet, no saturation loop - so an arming decision is a pure input -> output
// assertion rather than something inferred from per-rule rewrite counts.

namespace memgraph::planner::core {

using namespace test;
using rewrite::ArmingIndex;
using rewrite::PatternArm;
using rewrite::RuleLatch;

namespace {
using Latch = RuleLatch<Op, NoAnalysis>;
using Roots = std::vector<std::vector<std::optional<Op>>>;
using Arms = std::vector<std::vector<PatternArm<Op>>>;

/// Build per-rule arming specs from root symbols, all at one depth. These
/// scenarios vary the closure depth through reset(), not the per-pattern depth,
/// so one depth per index keeps the literals readable.
auto RootArms(Roots const &roots, std::size_t depth) -> Arms {
  Arms arms;
  arms.reserve(roots.size());
  for (auto const &rule_roots : roots) {
    std::vector<PatternArm<Op>> rule;
    rule.reserve(rule_roots.size());
    for (auto const &root : rule_roots) rule.push_back(PatternArm<Op>{.root = root, .depth = depth});
    arms.push_back(std::move(rule));
  }
  return arms;
}

/// The armed rule indices, recovered from the latch's dense armed predicate.
auto ArmedSet(Latch const &latch) -> boost::unordered_flat_set<std::size_t> {
  boost::unordered_flat_set<std::size_t> out;
  auto const &armed = latch.armed();
  for (std::size_t i = 0; i < armed.size(); ++i) {
    if (armed[i] != 0) out.insert(i);
  }
  return out;
}
}  // namespace

TEST(RuleLatch, FirstArmArmsEveryRuleAndMatchesEveryCandidate) {
  TestEGraph eg;
  eg.emplace(Op::A);
  auto const index = ArmingIndex<Op>::from_pattern_arms(RootArms(Roots{{Op::A}, {Op::B}, {Op::C}}, 0));

  Latch latch;
  latch.reset(index, /*max_pattern_depth=*/0, /*num_rules=*/3);
  latch.arm(eg);

  EXPECT_EQ(ArmedSet(latch), (boost::unordered_flat_set<std::size_t>{0, 1, 2}));
  EXPECT_TRUE(latch.active().matches_all()) << "first arm matches every candidate, so no active-set restriction";
}

TEST(RuleLatch, EmptyTouchedArmsOnlyAlwaysArmedRules) {
  TestEGraph eg;
  eg.emplace(Op::A);
  eg.emplace(Op::B);
  // Rule 0 roots at Op::A; rule 1 has a symbol-less root, so it is always armed.
  auto const index = ArmingIndex<Op>::from_pattern_arms(RootArms(Roots{{Op::A}, {std::nullopt}}, 0));

  Latch latch;
  latch.reset(index, /*max_pattern_depth=*/0, /*num_rules=*/2);
  latch.arm(eg);  // arm-all, consumes the pending flag
  eg.clear_touched();
  latch.arm(eg);  // nothing touched since

  EXPECT_EQ(ArmedSet(latch), (boost::unordered_flat_set<std::size_t>{1})) << "only the always-armed rule survives";
  // A settled but non-empty graph is "sparse", so active() is RestrictTo the EMPTY
  // set (restrict to nothing) - distinct from MatchAll. The always-armed rule still
  // fires because a symbol-less root ignores the restriction.
  ASSERT_FALSE(latch.active().matches_all());
  EXPECT_TRUE(latch.active().restricted_roots().empty());
}

TEST(RuleLatch, TouchedSymbolArmsItsIndexedRulesAndKeepsSparseActiveSet) {
  TestEGraph eg;
  eg.emplace(Op::A);
  eg.emplace(Op::B);
  for (std::uint64_t i = 0; i < 4; ++i) eg.emplace(Op::C, i);  // padding: keeps the change sparse
  auto const index = ArmingIndex<Op>::from_pattern_arms(RootArms(Roots{{Op::A}, {Op::B}}, 0));

  Latch latch;
  latch.reset(index, /*max_pattern_depth=*/0, /*num_rules=*/2);
  latch.arm(eg);
  eg.clear_touched();
  auto const fresh_a = eg.emplace(Op::A, /*disambiguator=*/99).eclass_id;  // touch one A-class
  latch.arm(eg);

  EXPECT_EQ(ArmedSet(latch), (boost::unordered_flat_set<std::size_t>{0})) << "only the Op::A-rooted rule arms";
  ASSERT_FALSE(latch.active().matches_all()) << "a one-class change in a larger graph is sparse";
  EXPECT_TRUE(latch.active().restricted_roots().contains(eg.find(fresh_a)));
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
  auto const index = ArmingIndex<Op>::from_pattern_arms(RootArms(Roots{{Op::G}}, 2));

  TestEGraph deep_enough;
  build(deep_enough);
  Latch to_depth_two;
  to_depth_two.reset(index, /*max_pattern_depth=*/2, /*num_rules=*/1);
  to_depth_two.arm(deep_enough);  // arm-all, consume pending
  to_depth_two.arm(deep_enough);  // arm from the merge's touched-set
  EXPECT_TRUE(to_depth_two.armed()[0]) << "depth-2 closure from `a` reaches G(F(a))";

  TestEGraph too_shallow;
  build(too_shallow);
  Latch to_depth_one;
  to_depth_one.reset(index, /*max_pattern_depth=*/1, /*num_rules=*/1);
  to_depth_one.arm(too_shallow);
  to_depth_one.arm(too_shallow);
  EXPECT_FALSE(to_depth_one.armed()[0]) << "depth-1 closure stops at F(a); G is unreached";
}

TEST(RuleLatch, ArmingGatesEachPatternByItsOwnDepth) {
  // a <- F(a) <- G(F(a)): a change at `a` reaches G at hop 2. A rule rooted at G
  // arms only if its pattern depth reaches that hop, independent of any deeper
  // rule in the set that widened the closure.
  auto build = [](TestEGraph &eg) {
    auto const a = eg.emplace(Op::A).eclass_id;
    auto const fa = eg.emplace(Op::F, {a}).eclass_id;
    eg.emplace(Op::G, {fa});
    auto const b = eg.emplace(Op::B).eclass_id;
    eg.clear_touched();
    eg.merge(a, b);  // touch the class holding `a`
    TestProcessingContext pc;
    eg.rebuild(pc);
  };
  // Rule 0 roots at G with depth 1; rule 1 roots at G with depth 2.
  auto const index = ArmingIndex<Op>::from_pattern_arms(Arms{{{Op::G, 1}}, {{Op::G, 2}}});

  TestEGraph eg;
  build(eg);
  Latch latch;
  latch.reset(index, /*max_pattern_depth=*/2, /*num_rules=*/2);
  latch.arm(eg);  // arm-all, consume pending
  latch.arm(eg);  // arm from the merge's touched-set

  EXPECT_FALSE(latch.armed()[0]) << "G sits at hop 2, beyond the depth-1 rule's reach";
  EXPECT_TRUE(latch.armed()[1]) << "the depth-2 rule reaches G at hop 2";
}

TEST(RuleLatch, RestrictionSliceStopsAtDirectParents) {
  // a <- F(a) <- G(F(a)): touching `a` leaves `a` and its direct parent F(a) as
  // candidate roots for a root-entry (depth-1) rule, never the 2-hop G(F(a)) -
  // even though the arming closure walks that deep.
  TestEGraph eg;
  auto const a = eg.emplace(Op::A).eclass_id;
  auto const fa = eg.emplace(Op::F, {a}).eclass_id;
  auto const gfa = eg.emplace(Op::G, {fa}).eclass_id;
  for (std::uint64_t i = 0; i < 6; ++i) eg.emplace(Op::C, i);  // padding: keeps the change sparse
  auto const index = ArmingIndex<Op>::from_pattern_arms(RootArms(Roots{{Op::F}}, 1));

  Latch latch;
  latch.reset(index, /*max_pattern_depth=*/2, /*num_rules=*/1);
  latch.arm(eg);  // arm-all, consume pending
  eg.clear_touched();
  auto const b = eg.emplace(Op::B).eclass_id;
  eg.merge(a, b);  // touch the class holding `a`
  TestProcessingContext pc;
  eg.rebuild(pc);
  latch.arm(eg);

  ASSERT_FALSE(latch.active().matches_all()) << "a one-class change in a larger graph is sparse";
  EXPECT_TRUE(latch.active().restricted_roots().contains(eg.find(a)));
  EXPECT_TRUE(latch.active().restricted_roots().contains(eg.find(fa)));
  EXPECT_FALSE(latch.active().restricted_roots().contains(eg.find(gfa)))
      << "the 2-hop grandparent is not a depth-1 re-fire root";
}

TEST(RuleLatch, NonSparseChangeDropsTheActiveSet) {
  TestEGraph eg;
  eg.emplace(Op::A);
  auto const index = ArmingIndex<Op>::from_pattern_arms(RootArms(Roots{{Op::A}}, 0));

  Latch latch;
  latch.reset(index, /*max_pattern_depth=*/0, /*num_rules=*/1);
  latch.arm(eg);
  eg.clear_touched();
  eg.emplace(Op::A, /*disambiguator=*/7);  // touched half the (now 2) classes
  latch.arm(eg);

  EXPECT_TRUE(latch.active().matches_all()) << "change is not a sparse slice, so match via arming alone";
  EXPECT_TRUE(latch.armed()[0]) << "the touched Op::A still arms its rule";
}

TEST(RuleLatch, ResetReArmsEveryRule) {
  TestEGraph eg;
  eg.emplace(Op::A);
  auto const index = ArmingIndex<Op>::from_pattern_arms(RootArms(Roots{{Op::A}, {Op::B}}, 0));

  Latch latch;
  latch.reset(index, /*max_pattern_depth=*/0, /*num_rules=*/2);
  latch.arm(eg);
  eg.clear_touched();
  latch.arm(eg);  // arm-from-touched: settled, arms nothing symbol-rooted
  EXPECT_FALSE(latch.armed()[1]);

  latch.reset(index, /*max_pattern_depth=*/0, /*num_rules=*/2);  // e.g. after set_rules
  latch.arm(eg);
  EXPECT_EQ(ArmedSet(latch), (boost::unordered_flat_set<std::size_t>{0, 1})) << "reset re-arms all on the next arm";
}

}  // namespace memgraph::planner::core
