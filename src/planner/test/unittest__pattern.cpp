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

#include <gtest/gtest.h>

#include <set>

#include "test_patterns.hpp"

namespace memgraph::planner::core {

using namespace test;  // Bring in Op, TestPattern, kVarX, etc.

// Helper to verify pattern has exactly the expected variables with unique slots
void expect_vars(TestPattern const &pattern, std::initializer_list<PatternVar> vars) {
  EXPECT_EQ(pattern.num_vars(), vars.size());
  std::set<uint8_t> slots;
  for (auto var : vars) {
    EXPECT_TRUE(pattern.has_var(var)) << "Pattern missing expected variable";
    auto slot = pattern.var_slot(var);
    EXPECT_LT(slot, vars.size()) << "Slot out of range";
    slots.insert(slot);
  }
  EXPECT_EQ(slots.size(), vars.size()) << "Slots not unique";
}

// ============================================================================
// Pattern Unit Tests
// ============================================================================
//
// Pattern represents a tree structure for matching e-graph expressions.
// Key concepts:
//   - PatternVar: captures a binding (?x, ?y, etc.)
//   - Wildcard: matches anything without binding (_)
//   - SymbolWithChildren: operator with children (Add, Mul, etc.)
//
// Patterns are built bottom-up; the last node added becomes the root.
// ============================================================================

// ============================================================================
// Fluent DSL Tests
// ============================================================================
//
// The fluent DSL allows concise pattern construction:
//   Pattern<Op>::build(Op::Add, {Var{x}, Sym(Op::Neg, Var{y})})
//
// This creates:
//       Add          <-- root
//      /   \
//    ?x    Neg
//           |
//          ?y
// ============================================================================

TEST(PatternDSL, NestedPatternConstruction) {
  // Build: Add(Neg(?x), Mul(?y, ?z))
  //
  //         Add         <-- root (depth 0)
  //        /   \
  //      Neg   Mul      (depth 1)
  //       |    / \
  //      ?x  ?y  ?z     (depth 2)

  auto pattern = TestPattern::build(Op::Add, {Sym(Op::Neg, Var{kVarX}), Sym(Op::Mul, Var{kVarY}, Var{kVarZ})});

  EXPECT_EQ(pattern.size(), 6);  // 3 vars + 3 ops
  EXPECT_EQ(pattern.depth(), 2);
  EXPECT_FALSE(pattern.is_variable_pattern());
  expect_vars(pattern, {kVarX, kVarY, kVarZ});
}

TEST(PatternDSL, LeafSymbolPattern) {
  // Build: Const (leaf symbol with no children)
  auto pattern = TestPattern::build(Op::Const);

  EXPECT_EQ(pattern.size(), 1);
  EXPECT_EQ(pattern.depth(), 0);
  EXPECT_EQ(pattern.num_vars(), 0);
  EXPECT_FALSE(pattern.is_variable_pattern());
}

TEST(PatternDSL, WildcardVsVariable) {
  // Build: Add(_, ?x)
  // Wildcard matches any e-class without creating a binding
  //
  //       Add        depth 0
  //      /   \
  //     _    ?x      depth 1

  auto pattern = TestPattern::build(Op::Add, {Wildcard{}, Var{kVarX}});

  EXPECT_EQ(pattern.size(), 3);  // Add + wildcard + var
  EXPECT_EQ(pattern.depth(), 1);
  expect_vars(pattern, {kVarX});  // wildcard doesn't count
}

// ============================================================================
// Variable Sharing Tests
// ============================================================================
//
// Same PatternVar ID used multiple times enforces equality during matching.
// Example: Add(?x, ?x) only matches when both children are in the same e-class.
// ============================================================================

TEST(PatternVariables, RepeatedVariableCountsOnce) {
  // Build: Add(?x, ?x)
  // Same PatternVar ID used multiple times → only one unique variable.
  // During matching, both positions must bind to the same e-class.
  //
  //       Add        depth 0
  //      /   \
  //     ?x   ?x      depth 1

  auto pattern = TestPattern::build(Op::Add, {Var{kVarX}, Var{kVarX}});

  EXPECT_EQ(pattern.size(), 3);  // Add + 2 var nodes
  EXPECT_EQ(pattern.depth(), 1);
  expect_vars(pattern, {kVarX});  // only one unique variable
}

TEST(PatternVariables, RootBinding) {
  // Build: Add(?x, ?y) with root bound to ?root
  // Bindings capture the matched e-class at a symbol node.
  //
  //       Add[?root]   <-- root bound to ?root (depth 0)
  //      /   \
  //     ?x   ?y        (depth 1)

  auto pattern = TestPattern::build(Op::Add, {Var{kVarX}, Var{kVarY}}, kVarRoot);

  EXPECT_EQ(pattern.size(), 3);  // Add + 2 vars
  EXPECT_EQ(pattern.depth(), 1);
  EXPECT_TRUE(pattern.has_bindings());
  expect_vars(pattern, {kVarX, kVarY, kVarRoot});
}

// ============================================================================
// Depth Calculation Tests
// ============================================================================
//
// Depth = max edges from root to any leaf.
// Important for matching algorithm scheduling (deeper patterns are more specific).
// ============================================================================

TEST(PatternDepth, AsymmetricTreeUsesDeepestBranch) {
  // Depth is max of all branches, not first branch.
  // Test both orderings to ensure correctness.

  // Deep branch on left: Add(Neg(Neg(?x)), ?y)
  //         Add              depth 0
  //        /   \
  //      Neg   ?y            depth 1
  //       |
  //      Neg                 depth 2
  //       |
  //      ?x                  depth 3
  auto left_deep = TestPattern::build(Op::Add, {Sym(Op::Neg, Sym(Op::Neg, Var{kVarX})), Var{kVarY}});
  EXPECT_EQ(left_deep.depth(), 3);
  expect_vars(left_deep, {kVarX, kVarY});

  // Deep branch on right: Add(?x, Neg(Neg(?y)))
  //         Add              depth 0
  //        /   \
  //      ?x   Neg            depth 1
  //            |
  //           Neg            depth 2
  //            |
  //           ?y             depth 3
  auto right_deep = TestPattern::build(Op::Add, {Var{kVarX}, Sym(Op::Neg, Sym(Op::Neg, Var{kVarY}))});
  EXPECT_EQ(right_deep.depth(), 3);
  expect_vars(right_deep, {kVarX, kVarY});
}

TEST(PatternDepth, WideButShallow) {
  // Build: F(?x, ?y, ?z, ?w) - 4-ary function
  // Wide patterns can have low depth.
  //
  //          F           depth 0
  //       / | | \
  //     ?x ?y ?z ?w      depth 1

  auto pattern = TestPattern::build(Op::F, {Var{kVarX}, Var{kVarY}, Var{kVarZ}, Var{kVarW}});

  EXPECT_EQ(pattern.size(), 5);
  EXPECT_EQ(pattern.depth(), 1);  // wide but shallow
  expect_vars(pattern, {kVarX, kVarY, kVarZ, kVarW});
}

// ============================================================================
// Builder vs Fluent DSL Equivalence
// ============================================================================
//
// Both construction methods should produce identical patterns.
// ============================================================================

TEST(PatternConstruction, BuilderAndFluentDSLEquivalent) {
  // Pattern: Mul(Add(?x, ?y), Neg(?z))
  //
  //         Mul
  //        /   \
  //      Add   Neg
  //     /  \    |
  //   ?x  ?y   ?z

  // Fluent DSL construction
  auto fluent = TestPattern::build(Op::Mul, {Sym(Op::Add, Var{kVarX}, Var{kVarY}), Sym(Op::Neg, Var{kVarZ})});

  // Builder construction
  auto builder = TestPattern::Builder{};
  auto x = builder.var(kVarX);
  auto y = builder.var(kVarY);
  auto add = builder.sym(Op::Add, {x, y});
  auto z = builder.var(kVarZ);
  auto neg = builder.sym(Op::Neg, {z});
  builder.sym(Op::Mul, {add, neg});
  auto built = std::move(builder).build();

  // Both should produce identical patterns
  EXPECT_EQ(fluent, built);
  EXPECT_EQ(fluent.size(), built.size());
  EXPECT_EQ(fluent.depth(), built.depth());
  expect_vars(fluent, {kVarX, kVarY, kVarZ});
  expect_vars(built, {kVarX, kVarY, kVarZ});
}

// ============================================================================
// Edge Cases
// ============================================================================

TEST(PatternEdgeCases, SingleVariableIsVariablePattern) {
  // Build: ?x (single variable pattern)
  // Variable patterns match any e-class.
  auto pattern = make_var_pattern(kVarX);

  EXPECT_TRUE(pattern.is_variable_pattern());
  EXPECT_EQ(pattern.size(), 1);
  EXPECT_EQ(pattern.depth(), 0);
  expect_vars(pattern, {kVarX});
}

TEST(PatternEdgeCases, SymbolPatternNotVariablePattern) {
  // Build: Const() (leaf symbol)
  auto pattern = TestPattern::build(Op::Const);

  EXPECT_EQ(pattern.size(), 1);
  EXPECT_EQ(pattern.depth(), 0);
  EXPECT_EQ(pattern.num_vars(), 0);
  EXPECT_FALSE(pattern.is_variable_pattern());
}

}  // namespace memgraph::planner::core
