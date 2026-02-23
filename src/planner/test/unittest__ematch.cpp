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

#include "test_ematcher_fixture.hpp"
#include "test_patterns.hpp"

namespace memgraph::planner::core {

using namespace test;

// ============================================================================
// Basic Pattern Matching
// ============================================================================
//
// Tests fundamental matching: variables match anything, symbols filter by type,
// and arity must match exactly.

TEST_F(EMatcherTest, VariableMatchesAnyEClass) {
  // Variable patterns (?x) match every e-class unconditionally.
  // This is the most permissive pattern type.
  //
  //   Pattern: ?x
  //
  //   E-graph:          Matches:
  //      Add            ?x = Add ✓
  //     /   \           ?x = a   ✓
  //    a     b          ?x = b   ✓
  use_pattern(make_var_pattern(kVarX));

  auto a = leaf(Op::Var, 1);
  auto b = leaf(Op::Const, 2);
  auto c = node(Op::Add, a, b);
  rebuild_index();

  expect_matches({{{kVarX, a}}, {{kVarX, b}}, {{kVarX, c}}});
}

TEST_F(EMatcherTest, SymbolFiltersAndArityMustMatch) {
  // Symbol patterns (Op::X) only match e-nodes with that symbol.
  // Arity must also match exactly.
  //
  //   E-graph:     Pattern Add(?x,?y):
  //   [Const]      - NO: wrong symbol
  //   [F(a,b)]     - YES: symbol+arity match
  //   [F(a,b,c)]   - NO: wrong symbol
  //   [F(a)]       - NO: arity mismatch (if it existed)
  use_pattern(make_binary_pattern(Op::F, kVarX, kVarY, kTestRoot));

  auto a = leaf(Op::Const, 1);
  auto b = leaf(Op::Const, 2);
  auto c = leaf(Op::Const, 3);
  auto correct_f = node(Op::F, a, b);
  node(Op::F, a, b, c);  // 3-ary, won't match 2-ary pattern
  rebuild_index();

  expect_matches({{{kTestRoot, correct_f}, {kVarX, a}, {kVarY, b}}});
}

TEST_F(EMatcherTest, SymbolMismatchReturnsNoMatches) {
  // Pattern only matches e-nodes with the exact symbol.
  //
  //   Pattern: Add(?x, ?y)
  //   E-graph: [a: Var(1)], [b: Var(2)], [Mul(a,b)], [Neg(a)]  ← no Add nodes
  //   Result: no matches
  use_pattern(make_binary_pattern(Op::Add, kVarX, kVarY, kTestRoot));

  auto a = leaf(Op::Var, 1);
  auto b = leaf(Op::Var, 2);
  node(Op::Mul, a, b);
  node(Op::Neg, a);
  rebuild_index();

  expect_no_matches();
}

TEST_F(EMatcherTest, WildcardMatchesWithoutBinding) {
  // Wildcards (_) match any e-class but create no binding.
  // Useful for "don't care" positions in patterns.
  //
  //   Pattern: Add(_, ?x)
  //
  //      Add(_, ?x)
  //       /      \
  //      _       ?x  ← only ?x is bound
  //
  //   E-graph:        Bindings:
  //   Add(a, b)  →    ?x = b (first arg ignored)
  //   Add(b, a)  →    ?x = a (first arg ignored)
  use_pattern(TestPattern::build(Op::Add, {Wildcard{}, Var{kVarX}}));

  auto a = leaf(Op::Var, 1);
  auto b = leaf(Op::Var, 2);
  node(Op::Add, a, b);
  node(Op::Add, b, a);
  rebuild_index();

  expect_matches({{{kVarX, b}}, {{kVarX, a}}});
}

TEST_F(EMatcherTest, PatternWithNoVariablesReturnsNoMatches) {
  // Edge case: patterns without variables have no bindings to return.
  // Matcher returns 0 matches since results would be meaningless.
  auto builder = TestPattern::Builder{};
  builder.wildcard();
  use_pattern(std::move(builder).build());
  EXPECT_EQ(pattern().num_vars(), 0);

  leaf(Op::Var, 1);
  leaf(Op::Var, 2);
  rebuild_index();

  expect_no_matches();
}

// ============================================================================
// Variable Equality Constraints
// ============================================================================
//
// When the same variable appears multiple times in a pattern, the matcher
// enforces that all occurrences bind to the same e-class. This is a key
// feature for detecting equivalent subexpressions.

TEST_F(EMatcherTest, RepeatedVariableEnforcesEquality) {
  // Pattern Add(?x, ?x) only matches when both children are equivalent.
  //
  //   E-graph:          Pattern:
  //   Add(a, a)  ─────>  Add(?x, ?x)  ✓ matches, ?x=a
  //   Add(a, b)  ─────>  Add(?x, ?x)  ✗ no match (a ≠ b)
  use_pattern(TestPattern::build(Op::Add, {Var{kVarX}, Var{kVarX}}, kTestRoot));

  auto a = leaf(Op::Var, 1);
  auto b = leaf(Op::Var, 2);
  auto add_aa = node(Op::Add, a, a);
  node(Op::Add, a, b);  // Should not match
  rebuild_index();

  expect_matches({{{kTestRoot, add_aa}, {kVarX, a}}});
}

// ============================================================================
// Nested and Wide Patterns
// ============================================================================
//
// Patterns can have arbitrary nesting depth and width. The matcher collects
// bindings from all levels into a single match result.

TEST_F(EMatcherTest, DeepPatternCollectsAllBindings) {
  // Deeply nested pattern with variables at multiple levels.
  //
  //   Pattern: Add(Mul(Neg(?x), ?y), ?z)
  //
  //            Add
  //           /   \
  //         Mul    ?z
  //        /   \
  //      Neg    ?y
  //       |
  //      ?x
  use_pattern(TestPattern::build(Op::Add, {Sym(Op::Mul, Sym(Op::Neg, Var{kVarX}), Var{kVarY}), Var{kVarZ}}, kTestRoot));

  auto x = leaf(Op::Var, 1);
  auto y = leaf(Op::Var, 2);
  auto z = leaf(Op::Var, 3);
  auto expr = node(Op::Add, node(Op::Mul, node(Op::Neg, x), y), z);
  rebuild_index();

  expect_matches({{{kTestRoot, expr}, {kVarX, x}, {kVarY, y}, {kVarZ, z}}});
}

TEST_F(EMatcherTest, WidePatternCollectsFromAllBranches) {
  // Pattern with multiple branches, each containing variables.
  // All variables must be collected into one match.
  //
  //   Pattern: F(Add(?w, ?x), Mul(?y, ?z))
  //
  //            F
  //          /   \
  //        Add   Mul
  //       / \    / \
  //     ?w  ?x  ?y  ?z
  use_pattern(TestPattern::build(
      Op::F, {Sym(Op::Add, Var{kVarW}, Var{kVarX}), Sym(Op::Mul, Var{kVarY}, Var{kVarZ})}, kTestRoot));

  auto w = leaf(Op::Var, 1);
  auto x = leaf(Op::Var, 2);
  auto y = leaf(Op::Var, 3);
  auto z = leaf(Op::Var, 4);
  auto f = node(Op::F, node(Op::Add, w, x), node(Op::Mul, y, z));
  rebuild_index();

  expect_matches({{{kTestRoot, f}, {kVarW, w}, {kVarX, x}, {kVarY, y}, {kVarZ, z}}});
}

TEST_F(EMatcherTest, PatternMatchesMultipleLocations) {
  // A pattern can match at multiple locations in the e-graph.
  //
  //   E-graph: Add(a, Add(b, c))
  //
  //        Add  <── matches here (outer)
  //       /   \
  //      a    Add  <── matches here (inner)
  //          /   \
  //         b     c
  use_pattern(make_binary_pattern(Op::Add, kVarX, kVarY, kTestRoot));

  auto a = leaf(Op::Var, 1);
  auto b = leaf(Op::Var, 2);
  auto c = leaf(Op::Var, 3);
  auto inner = node(Op::Add, b, c);
  auto outer = node(Op::Add, a, inner);
  rebuild_index();

  expect_matches({
      {{kTestRoot, outer}, {kVarX, a}, {kVarY, inner}},
      {{kTestRoot, inner}, {kVarX, b}, {kVarY, c}},
  });
}

// ============================================================================
// E-Graph Equivalence Classes
// ============================================================================
//
// E-graphs represent equivalences: multiple e-nodes can be in the same e-class.
// The matcher must handle both scenarios:
// 1. Matching different e-nodes that represent the same equivalence
// 2. Patterns becoming matchable after merges create equivalences

TEST_F(EMatcherTest, MergedEClassMatchesBothRepresentations) {
  // Scenario: Analysis determined Add(x,y) and Mul(x,y) evaluate to the same
  // value (e.g., constant folding showed both equal 0). After merging, both
  // Add and Mul patterns should match the merged e-class.
  //
  //   Before merge:           After merge:
  //   [Add(x,y)]              [Add(x,y), Mul(x,y)]  <- same e-class
  //   [Mul(x,y)]
  //
  //   Pattern Add(?a,?b) matches the merged class
  //   Pattern Mul(?a,?b) also matches the same class

  auto x = leaf(Op::Var, 1);
  auto y = leaf(Op::Var, 2);
  auto add = node(Op::Add, x, y);
  auto mul = node(Op::Mul, x, y);

  // Simulate analysis discovering equivalence
  auto merged = merge(add, mul);
  rebuild_egraph();
  rebuild_index();

  use_pattern(make_binary_pattern(Op::Add, kVarX, kVarY, kTestRoot));
  expect_matches({{{kTestRoot, merged}, {kVarX, x}, {kVarY, y}}});

  use_pattern(make_binary_pattern(Op::Mul, kVarX, kVarY, kTestRoot));
  expect_matches({{{kTestRoot, merged}, {kVarX, x}, {kVarY, y}}});
}

TEST_F(EMatcherTest, MergeEnablesRepeatedVariableMatch) {
  // Merge can make a repeated-variable pattern match.
  //
  //   Pattern: Add(?x, ?x)
  //
  //   Before merge:         After merge(a, b):
  //
  //      Add                   Add
  //     /   \                 /   \
  //    a     b   (a ≠ b)     [a,b] [a,b]  (a ≡ b)
  //
  //   No match               ?x = [a,b] ✓
  use_pattern(TestPattern::build(Op::Add, {Var{kVarX}, Var{kVarX}}, kTestRoot));

  auto a = leaf(Op::Var, 1);
  auto b = leaf(Op::Var, 2);
  auto add = node(Op::Add, a, b);
  rebuild_index();

  // Before merge: no match (a ≠ b)
  expect_no_matches();

  // After merge: matches (a ≡ b)
  merge(a, b);
  rebuild_egraph();
  rebuild_index();

  expect_matches({{{kTestRoot, add}, {kVarX, egraph.find(a)}}});
}

TEST_F(EMatcherTest, SelfReferentialEClassMatchesMultipleTimes) {
  // BUG: EMatcher misses matches when non-root frames should try multiple
  // e-nodes in a self-referential e-class.
  //
  // Setup (from egglog):
  //   (let n0 (B 64))
  //   (let n1 (F n0))
  //   (let n2 (F n1))
  //   (union n1 n2)
  //
  // After merge and rebuild:
  //   EC0 = { B(64) }
  //   EC1 = { F(EC0), F(EC1) }   <- self-referential
  //
  // Pattern: F(F(F(?v0)))
  //
  // Expected matches (egglog finds both):
  //   Match 1: ?v0 = EC0
  //     - outermost F: F(EC1) from EC1
  //     - middle F: F(EC1) from EC1 (self-loop)
  //     - inner F: F(EC0) from EC1
  //     - ?v0 binds to EC0
  //
  //   Match 2: ?v0 = EC1
  //     - outermost F: F(EC1) from EC1
  //     - middle F: F(EC1) from EC1
  //     - inner F: F(EC1) from EC1 (self-loop)
  //     - ?v0 binds to EC1 itself
  //
  // BUG: EMatcher only finds match 1. The innermost F(?v0) frame at EC1
  // tries F(EC0), yields, gets popped via ChildYielded, and never tries F(EC1).

  auto n0 = leaf(Op::B, 64);
  auto n1 = node(Op::F, n0);
  auto n2 = node(Op::F, n1);

  merge(n1, n2);
  rebuild_egraph();
  rebuild_index();

  // EC1 should now be self-referential: { F(EC0), F(EC1) }
  auto ec1 = egraph.find(n1);
  ASSERT_EQ(egraph.find(n2), ec1) << "n1 and n2 should be in same e-class";

  // Pattern: F(F(F(?v0)))
  use_pattern(TestPattern::build(Op::F, {Sym(Op::F, Sym(Op::F, Var{kVarX}))}, kTestRoot));

  // Should find 2 matches: ?v0 = EC0 and ?v0 = EC1
  expect_matches({
      {{kTestRoot, ec1}, {kVarX, n0}},   // ?v0 = EC0 (B(64))
      {{kTestRoot, ec1}, {kVarX, ec1}},  // ?v0 = EC1 (self-reference)
  });
}

TEST_F(EMatcherTest, UnionNodeWithChildCreatesSimpleSelfReferentialEClass) {
  // Simulates the egglog scenario:
  //
  //   (let n0 (A 10))
  //   (let n1 (G n0))
  //   (let n2 (A 0))
  //   (union n0 n1)
  //   (let n3 (G n1))
  //   (let n4 (G n3))
  //
  //   (rule ((= ?root (G ?v0))) ((MatchResult ?v0)))
  //
  //   Egglog output: (MatchResult (A 10))
  //
  // After union(n0, n1) and rebuild:
  //   EC_merged = { A(10), F(EC_merged) }   <- self-referential
  //
  // n3 = F(n1) and n4 = F(n3) collapse into EC_merged by congruence since
  // F(EC_merged) is already in EC_merged.
  //
  // Pattern F(?v0) should produce exactly one match:
  //   ?root = EC_merged,  ?v0 = EC_merged
  //
  // (egglog reports ?v0 as "(A 10)" which is the canonical form of EC_merged)

  auto n0 = leaf(Op::A, 10);
  auto n1 = node(Op::F, n0);
  leaf(Op::A, 0);  // n2 — separate e-class, does not participate in the match

  merge(n0, n1);
  rebuild_egraph();

  // n3 = F(n1=EC_merged) and n4 = F(n3=EC_merged): both are congruent to the
  // existing F(EC_merged) already inside EC_merged, so they collapse into it.
  auto n3 = node(Op::F, n1);
  node(Op::F, n3);  // n4
  rebuild_egraph();
  rebuild_index();

  // Pattern: F(?v0)
  use_pattern(make_unary_pattern(Op::F, kVarX, kTestRoot));

  auto ec = egraph.find(n0);  // canonical ID for EC_merged
  ASSERT_EQ(egraph.find(n1), ec) << "n0 and n1 should be in the same e-class after union";

  // One match: the single G/F node in EC_merged has child EC_merged itself
  expect_matches({{{kTestRoot, ec}, {kVarX, ec}}});
}

// ============================================================================
// Matcher Index Maintenance
// ============================================================================
//
// The EMatcher maintains a symbol index for efficient candidate lookup.
// These tests verify index correctness during incremental updates.

TEST_F(EMatcherTest, IncrementalIndexFindsNewNodes) {
  // After adding nodes, incremental rebuild_index() makes them findable.
  //
  // Scenario:
  // 1. Create matcher (indexes existing nodes)
  // 2. Add new node (not in index)
  // 3. rebuild_index({new_node}) updates index incrementally
  // 4. New node is now matchable

  use_pattern(make_leaf_pattern(Op::Const, kTestRoot));

  // No Const nodes initially
  rebuild_index();
  expect_no_matches();

  // Add Const, but index is stale
  auto c = leaf(Op::Const, 42);
  expect_no_matches();

  // Incremental update finds the new node
  rebuild_index_with(c);
  expect_matches({{{kTestRoot, c}}});
}

TEST_F(EMatcherTest, MatchCountCorrectAfterCongruence) {
  // Congruence merges e-nodes that become structurally identical.
  // Pattern matching must return correct count (no duplicates).
  //
  //   Before merge:              After merge(a≡c, b≡d):
  //
  //   Add(a,b)   Add(c,d)        Add(x,y)  ← only one e-node remains
  //    /   \      /   \           /   \
  //   a     b    c     d         x     y   (x = a≡c, y = b≡d)
  //
  //   2 matches                  1 match
  use_pattern(TestPattern::build(Op::Add, {Var{kVarX}, Var{kVarY}}));

  auto a = leaf(Op::Var, 0);
  auto b = leaf(Op::Var, 1);
  auto c = leaf(Op::Var, 2);
  auto d = leaf(Op::Var, 3);
  node(Op::Add, a, b);
  node(Op::Add, c, d);
  rebuild_index();

  // Initially 2 matches
  expect_matches({
      {{kVarX, a}, {kVarY, b}},
      {{kVarX, c}, {kVarY, d}},
  });

  // Merge causes congruence
  auto x = merge(a, c);
  auto y = merge(b, d);
  rebuild_egraph();
  rebuild_index();

  // After rebuild, duplicate e-node removed → exactly 1 match
  expect_matches({{{kVarX, x}, {kVarY, y}}});
}

// ============================================================================
// MatchArena Tests
// ============================================================================
//
// MatchArena is a monotonic allocator for storing match bindings efficiently.

TEST(MatchArena, InternAndRetrieve) {
  MatchArena arena;

  std::array bindings = {EClassId{10}, EClassId{20}, EClassId{30}};
  auto offset = arena.intern(bindings);

  EXPECT_EQ(arena.get(offset, 0), EClassId{10});
  EXPECT_EQ(arena.get(offset, 1), EClassId{20});
  EXPECT_EQ(arena.get(offset, 2), EClassId{30});
}

TEST(MatchArena, MultipleAllocationsAreIndependent) {
  MatchArena arena;

  auto offset1 = arena.intern(std::array{EClassId{1}, EClassId{2}});
  auto offset2 = arena.intern(std::array{EClassId{10}, EClassId{20}, EClassId{30}});

  // Separate allocations
  EXPECT_NE(offset1, offset2);

  // Each retrieves correct values
  EXPECT_EQ(arena.get(offset1, 0), EClassId{1});
  EXPECT_EQ(arena.get(offset2, 2), EClassId{30});
}

TEST(MatchArena, ClearResetsForReuse) {
  MatchArena arena;

  arena.intern(std::array{EClassId{1}, EClassId{2}, EClassId{3}});
  EXPECT_EQ(arena.size(), 3);

  arena.clear();
  EXPECT_EQ(arena.size(), 0);

  // Can reuse after clear
  arena.intern(std::array{EClassId{10}});
  EXPECT_EQ(arena.size(), 1);
}

}  // namespace memgraph::planner::core
