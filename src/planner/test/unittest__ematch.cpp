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

#include "planner/pattern/vm/compiler.hpp"
#include "planner/pattern/vm/executor.hpp"
#include "planner/rewrite/rule.hpp"
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

// ============================================================================
// EMatcher vs VM Executor Consistency
// ============================================================================
//
// These tests verify that EMatcher and VM executor produce identical results.
// This is important because the VM executor is used as an optimized alternative
// to EMatcher in the rewrite engine.

TEST_F(EMatcherTest, VMExecutorMatchesSameAsEMatcher) {
  // Basic test: both matchers should produce the same results for a simple pattern.
  //
  //   Pattern: Add(?x, ?y)
  //   E-graph: Add(a, b)
  //
  //   Both EMatcher and VM should find exactly 1 match.
  use_pattern(make_binary_pattern(Op::Add, kVarX, kVarY, kTestRoot));

  auto a = leaf(Op::Var, 1);
  auto b = leaf(Op::Var, 2);
  auto add = node(Op::Add, a, b);
  rebuild_index();

  // EMatcher results
  matches.clear();
  ematcher.match_into(pattern(), ctx, matches);
  ASSERT_EQ(matches.size(), 1);

  // VM results
  vm::PatternCompiler<Op> compiler;
  auto compiled = compiler.compile(std::span{&pattern(), 1});
  ASSERT_TRUE(compiled.has_value()) << "Pattern should compile successfully";

  vm::VMExecutorVerify<Op, NoAnalysis> vm_executor(egraph);
  EMatchContext vm_ctx;
  TestMatches vm_matches;
  vm_executor.execute(*compiled, ematcher, vm_ctx, vm_matches);

  EXPECT_EQ(matches.size(), vm_matches.size()) << "EMatcher and VM should produce same number of matches";
}

TEST_F(EMatcherTest, LeafSymbolChildWithMergedENodesConsistency) {
  // Regression test: When a leaf symbol appears as a CHILD (not root) of a
  // pattern, and that child e-class contains multiple e-nodes with the same
  // symbol (but different disambiguators), EMatcher and VM should produce
  // the same number of matches.
  //
  // This tests the bug found by the fuzzer where EMatcher was producing
  // more matches than VM for leaf symbol children.
  //
  //   Setup:
  //     a0 = A(0)
  //     a1 = A(1)
  //     x = X(0)
  //     f = F(x, a0)
  //     merge(a0, a1)  -> merged_a = { A(0), A(1) }
  //
  //   After rebuild, f = F(x, merged_a)
  //
  //   Pattern: F(?x, A)  - A is a leaf child (not root)
  //
  //   Expected: EMatcher and VM produce same number of matches

  auto x = leaf(Op::X, 0);
  auto a0 = leaf(Op::A, 0);
  auto a1 = leaf(Op::A, 1);
  node(Op::F, x, a0);  // F(x, a0)

  // Merge a0 and a1 into the same e-class
  merge(a0, a1);
  rebuild_egraph();
  rebuild_index();

  // Pattern: F(?x, A) - leaf symbol A as child
  use_pattern(TestPattern::build(Op::F, {Var{kVarX}, Sym(Op::A)}, kTestRoot));

  // EMatcher results
  matches.clear();
  ematcher.match_into(pattern(), ctx, matches);
  auto ematcher_count = matches.size();

  // VM results
  vm::PatternCompiler<Op> compiler;
  auto compiled = compiler.compile(std::span{&pattern(), 1});
  ASSERT_TRUE(compiled.has_value()) << "Pattern should compile successfully";

  vm::VMExecutorVerify<Op, NoAnalysis> vm_executor(egraph);
  EMatchContext vm_ctx;
  TestMatches vm_matches;
  vm_executor.execute(*compiled, ematcher, vm_ctx, vm_matches);
  auto vm_count = vm_matches.size();

  // EMatcher and VM should produce the same number of matches
  // Correct behavior: 1 match per e-class (existence check for leaf symbols)
  EXPECT_EQ(ematcher_count, vm_count) << "EMatcher and VM should produce same number of matches. "
                                      << "EMatcher: " << ematcher_count << ", VM: " << vm_count;
  // Verify the correct count
  EXPECT_EQ(ematcher_count, 1u) << "Expected 1 match (one per e-class, not per e-node)";
}

TEST_F(EMatcherTest, TernaryPatternWithLeafSymbolChildConsistency) {
  // Regression test from fuzzer: Pattern with a leaf symbol child in 3-ary pattern.
  //
  //   Setup (reproduces fuzzer bug):
  //     v0 = X(0)
  //     v1 = Y(0)
  //     a0 = A(0)
  //     a1 = A(1)
  //     t = F(v0, a0, v1)   <- 3-ary F with A in the middle
  //     merge(a0, a1)       -> merged_a = { A(0), A(1) }
  //
  //   After rebuild, t = F(v0, merged_a, v1)
  //
  //   Pattern: F(?x, A, ?y)  <- A is a leaf symbol, not a variable
  //
  //   Bug: EMatcher produces 2 matches while VM produces 1 match.
  //        EMatcher iterates e-nodes for leaf symbol children and produces
  //        one match per e-node, when it should produce one match per e-class.

  auto v0 = leaf(Op::X, 0);
  auto v1 = leaf(Op::Y, 0);
  auto a0 = leaf(Op::A, 0);
  auto a1 = leaf(Op::A, 1);
  node(Op::F, v0, a0, v1);  // 3-ary F

  // Merge a0 and a1
  merge(a0, a1);
  rebuild_egraph();
  rebuild_index();

  // Pattern: F(?x, A, ?y) - leaf symbol A in the middle
  use_pattern(TestPattern::build(Op::F, {Var{kVarX}, Sym(Op::A), Var{kVarY}}, kTestRoot));

  // EMatcher results
  matches.clear();
  ematcher.match_into(pattern(), ctx, matches);

  // VM results
  vm::PatternCompiler<Op> compiler;
  auto compiled = compiler.compile(std::span{&pattern(), 1});
  ASSERT_TRUE(compiled.has_value()) << "Pattern should compile successfully";

  vm::VMExecutorVerify<Op, NoAnalysis> vm_executor(egraph);
  EMatchContext vm_ctx;
  TestMatches vm_matches;
  vm_executor.execute(*compiled, ematcher, vm_ctx, vm_matches);

  // EMatcher and VM should produce the same number of matches
  // Currently fails: EMatcher produces 2, VM produces 1 (VM is correct)
  EXPECT_EQ(matches.size(), vm_matches.size()) << "EMatcher and VM should produce same number of matches. "
                                               << "EMatcher: " << matches.size() << ", VM: " << vm_matches.size();
}

// ============================================================================
// Multi-Pattern Matching Tests
// ============================================================================

TEST_F(EMatcherTest, MultiPatternVMFiltersBySymbolInVerifyMode) {
  // Regression test for VM multi-pattern matching bug found by fuzzer.
  //
  // Bug: In VMExecutorVerify, IterParentsSym was falling back to
  // exec_iter_parents() which iterates ALL parents without filtering by symbol.
  // Since the compiler doesn't emit CheckSymbol after IterParentsSym (assumes
  // the instruction filters), wrong parents could slip through and produce
  // spurious matches.
  //
  // Reproducer from fuzzer (crash-43736aae77a61652d5e1db4fabcfc47e26e548ca):
  //   E-graph: A(4), F(A(4))   <- one leaf, one unary
  //   Pattern 0: F(?v0)        <- matches F(A(4)), binds ?v0 = A class
  //   Pattern 1: F2(?v0)       <- should match F2 parents of ?v0 (none exist)
  //
  //   Bug: VM found 1 match (wrong), EMatcher found 0 (correct)
  //   VM was iterating F parent instead of F2 parents, and since F's child
  //   equals ?v0, the CheckSlot passed spuriously.

  // Setup: A(4), F(A(4))
  auto a = leaf(Op::A, 4);
  auto f_node = node(Op::F, a);
  rebuild_egraph();
  rebuild_index();

  // Multi-pattern: F(?v0) and F2(?v0)
  // F2 doesn't exist in graph, so this should produce 0 matches
  auto pattern1 = TestPattern::build(Op::F, {Var{kVarX}}, std::nullopt);
  auto pattern2 = TestPattern::build(Op::F2, {Var{kVarX}}, std::nullopt);

  // Count matches via RewriteRule
  std::size_t ematcher_count = 0;
  std::size_t vm_count = 0;

  // EMatcher-based multi-pattern matching
  {
    auto rule = RewriteRule<Op, NoAnalysis>::Builder("test_ematcher")
                    .pattern(TestPattern::build(Op::F, {Var{kVarX}}, std::nullopt))
                    .pattern(TestPattern::build(Op::F2, {Var{kVarX}}, std::nullopt))
                    .apply([&ematcher_count](RuleContext<Op, NoAnalysis> &, Match const &) { ++ematcher_count; });

    RewriteContext rewrite_ctx;
    rule.apply(egraph, ematcher, rewrite_ctx);
  }

  // VM-based multi-pattern matching
  {
    auto rule = RewriteRule<Op, NoAnalysis>::Builder("test_vm")
                    .pattern(TestPattern::build(Op::F, {Var{kVarX}}, std::nullopt))
                    .pattern(TestPattern::build(Op::F2, {Var{kVarX}}, std::nullopt))
                    .apply([&vm_count](RuleContext<Op, NoAnalysis> &, Match const &) { ++vm_count; });

    ASSERT_TRUE(rule.compiled_pattern().has_value()) << "Multi-pattern should compile for VM";

    RewriteContext rewrite_ctx;
    vm::VMExecutorVerify<Op, NoAnalysis> vm_executor(egraph);
    rule.apply_vm(egraph, ematcher, vm_executor, rewrite_ctx);
  }

  // Both should produce 0 matches (no F2 nodes in the graph)
  EXPECT_EQ(ematcher_count, 0u) << "EMatcher should find 0 multi-pattern matches";
  EXPECT_EQ(vm_count, 0u) << "VM should find 0 multi-pattern matches (was finding 1 before fix)";
  EXPECT_EQ(ematcher_count, vm_count) << "EMatcher and VM should produce same number of matches";
}

TEST_F(EMatcherTest, MultiPatternMatchWithSharedVariable) {
  // Test that multi-pattern matching correctly finds matches when both patterns
  // can be satisfied with a shared variable.
  //
  //   E-graph: A(1), F(A(1)), F2(A(1))
  //   Pattern 0: F(?v0)   <- matches F(A(1)), binds ?v0 = A class
  //   Pattern 1: F2(?v0)  <- matches F2(A(1)), checks ?v0 = A class
  //
  //   Should produce 1 match where ?v0 = A class

  // Setup: A(1), F(A(1)), F2(A(1))
  auto a = leaf(Op::A, 1);
  node(Op::F, a);
  node(Op::F2, a);
  rebuild_egraph();
  rebuild_index();

  std::size_t ematcher_count = 0;
  std::size_t vm_count = 0;

  // EMatcher-based multi-pattern matching
  {
    auto rule = RewriteRule<Op, NoAnalysis>::Builder("test_ematcher")
                    .pattern(TestPattern::build(Op::F, {Var{kVarX}}, std::nullopt))
                    .pattern(TestPattern::build(Op::F2, {Var{kVarX}}, std::nullopt))
                    .apply([&ematcher_count](RuleContext<Op, NoAnalysis> &, Match const &) { ++ematcher_count; });

    RewriteContext rewrite_ctx;
    rule.apply(egraph, ematcher, rewrite_ctx);
  }

  // VM-based multi-pattern matching
  {
    auto rule = RewriteRule<Op, NoAnalysis>::Builder("test_vm")
                    .pattern(TestPattern::build(Op::F, {Var{kVarX}}, std::nullopt))
                    .pattern(TestPattern::build(Op::F2, {Var{kVarX}}, std::nullopt))
                    .apply([&vm_count](RuleContext<Op, NoAnalysis> &, Match const &) { ++vm_count; });

    ASSERT_TRUE(rule.compiled_pattern().has_value()) << "Multi-pattern should compile for VM";

    RewriteContext rewrite_ctx;
    vm::VMExecutorVerify<Op, NoAnalysis> vm_executor(egraph);
    rule.apply_vm(egraph, ematcher, vm_executor, rewrite_ctx);
  }

  // Both should produce 1 match
  EXPECT_EQ(ematcher_count, 1u) << "EMatcher should find 1 multi-pattern match";
  EXPECT_EQ(vm_count, 1u) << "VM should find 1 multi-pattern match";
  EXPECT_EQ(ematcher_count, vm_count) << "EMatcher and VM should produce same number of matches";
}

// ============================================================================
// Match Deduplication Tests
// ============================================================================

TEST_F(EMatcherTest, DuplicateBindingsFromDifferentENodes) {
  // Test: Multiple e-nodes can produce different binding tuples even when
  // some variables have the same values. VM deduplicates by FULL tuple.
  //
  // Setup:
  //   a = A(0)   <- leaf A
  //   b = A(1)   <- different leaf A
  //   t1 = F(a, b, b)   <- 3-ary F with first child a
  //   t2 = F(b, b, b)   <- 3-ary F with first child b
  //
  // Pattern: F(A, ?x, ?y)  where root is bound to kTestRoot
  //   t1 matches: root=t1, ?x=b, ?y=b
  //   t2 matches: root=t2, ?x=b, ?y=b
  //
  // Both EMatcher and VM find 2 matches because the full tuples differ (different roots).
  // VM deduplicates by FULL tuple (all slots), not just non-root variables.

  auto a = leaf(Op::A, 0);
  auto b = leaf(Op::A, 1);
  auto t1 = node(Op::F, a, b, b);  // 3-ary F(a, b, b)
  auto t2 = node(Op::F, b, b, b);  // 3-ary F(b, b, b)
  rebuild_egraph();
  rebuild_index();

  // Pattern: F(A, ?x, ?y) - leaf symbol A as first child
  use_pattern(TestPattern::build(Op::F, {Sym(Op::A), Var{kVarX}, Var{kVarY}}, kTestRoot));

  // Get raw matches from EMatcher
  matches.clear();
  ematcher.match_into(pattern(), ctx, matches);

  // EMatcher finds 2 raw matches (one per F e-node that has A as first child)
  EXPECT_EQ(matches.size(), 2u) << "EMatcher expected 2 raw matches (one per F node with A child)";

  // Full tuples including root are unique
  std::set<std::vector<EClassId>> full_tuples;
  for (auto const &m : matches) {
    std::vector<EClassId> tuple;
    for (size_t i = 0; i < pattern().num_vars(); ++i) {
      tuple.push_back(egraph.find(ctx.arena().get(m, i)));
    }
    full_tuples.insert(tuple);
  }
  EXPECT_EQ(full_tuples.size(), 2u) << "Full tuples (including root) should be unique";

  // Non-root bindings are identical
  std::set<std::vector<EClassId>> non_root_bindings;
  for (auto const &m : matches) {
    std::vector<EClassId> tuple;
    // Only include non-root variables: kVarX and kVarY
    tuple.push_back(egraph.find(ctx.arena().get(m, pattern().var_slot(kVarX))));
    tuple.push_back(egraph.find(ctx.arena().get(m, pattern().var_slot(kVarY))));
    non_root_bindings.insert(tuple);
  }
  EXPECT_EQ(non_root_bindings.size(), 1u)
      << "Non-root bindings (?x, ?y) should deduplicate to 1 (both have ?x=b, ?y=b)";

  // VM deduplicates by FULL tuple (all slots including root)
  vm::PatternCompiler<Op> compiler;
  auto compiled = compiler.compile(std::span{&pattern(), 1});
  ASSERT_TRUE(compiled.has_value());

  vm::VMExecutorVerify<Op, NoAnalysis> vm_executor(egraph);
  EMatchContext vm_ctx;
  TestMatches vm_matches;
  vm_executor.execute(*compiled, ematcher, vm_ctx, vm_matches);

  // VM finds 2 matches because full tuples (including root) are different
  EXPECT_EQ(vm_matches.size(), 2u) << "VM finds 2 unique full tuples";
}

TEST_F(EMatcherTest, DeepNestedTernaryPatternNoMatches) {
  // Regression test from fuzzer crash-2f63faa4a8ed614df99ebc535024ccfb510b171f
  // Tests that deeply nested patterns with many variables correctly return
  // 0 matches when the e-graph doesn't contain matching structure.
  //
  // Pattern (simplified from crash):
  //   F(F(?v0, Neg(F(?v1, ?v2, ?v3)), Neg(Neg(?v4))), Neg(Neg(Neg(?v5))), ...)
  //
  // E-graph: Single leaf node B(0)
  // Expected: 0 matches (pattern structure doesn't exist in e-graph)

  auto b = leaf(Op::B, 0);
  rebuild_egraph();
  rebuild_index();

  // Pattern: F(F(?x, ?y, ?z), Neg(Neg(?w)), Neg(?a))
  // Uses Neg as unary operator for nesting depth
  use_pattern(TestPattern::build(Op::F,
                                 {Sym(Op::F, Var{kVarX}, Var{kVarY}, Var{kVarZ}),
                                  Sym(Op::Neg, Sym(Op::Neg, Var{kVarW})),
                                  Sym(Op::Neg, Var{kVarA})},
                                 kTestRoot));

  // EMatcher should find 0 matches
  matches.clear();
  ematcher.match_into(pattern(), ctx, matches);
  EXPECT_EQ(matches.size(), 0u) << "EMatcher should find 0 matches for non-existent pattern structure";

  // VM should also find 0 matches
  vm::PatternCompiler<Op> compiler;
  auto compiled = compiler.compile(std::span{&pattern(), 1});
  ASSERT_TRUE(compiled.has_value()) << "Deep pattern should compile successfully";

  vm::VMExecutorVerify<Op, NoAnalysis> vm_executor(egraph);
  EMatchContext vm_ctx;
  TestMatches vm_matches;
  vm_executor.execute(*compiled, ematcher, vm_ctx, vm_matches);

  EXPECT_EQ(vm_matches.size(), 0u) << "VM should find 0 matches for non-existent pattern structure";
}

TEST_F(EMatcherTest, DeepNestedTernaryPatternWithMatches) {
  // Test deeply nested patterns actually finding matches when structure exists.
  // This ensures the pattern compilation and execution work correctly for
  // complex nested structures.
  //
  // Pattern: F(Neg(?x), Neg(?y), Neg(?z))
  // E-graph: F(Neg(a), Neg(b), Neg(c))
  // Expected: 1 match with ?x=a, ?y=b, ?z=c

  auto a = leaf(Op::A, 0);
  auto b = leaf(Op::B, 0);
  auto c = leaf(Op::C, 0);
  auto neg_a = node(Op::Neg, a);
  auto neg_b = node(Op::Neg, b);
  auto neg_c = node(Op::Neg, c);
  auto f = node(Op::F, neg_a, neg_b, neg_c);
  rebuild_egraph();
  rebuild_index();

  // Pattern: F(Neg(?x), Neg(?y), Neg(?z))
  use_pattern(TestPattern::build(
      Op::F, {Sym(Op::Neg, Var{kVarX}), Sym(Op::Neg, Var{kVarY}), Sym(Op::Neg, Var{kVarZ})}, kTestRoot));

  // EMatcher results
  matches.clear();
  ematcher.match_into(pattern(), ctx, matches);
  auto ematcher_count = matches.size();

  // VM results
  vm::PatternCompiler<Op> compiler;
  auto compiled = compiler.compile(std::span{&pattern(), 1});
  ASSERT_TRUE(compiled.has_value()) << "Deep pattern should compile successfully";

  vm::VMExecutorVerify<Op, NoAnalysis> vm_executor(egraph);
  EMatchContext vm_ctx;
  TestMatches vm_matches;
  vm_executor.execute(*compiled, ematcher, vm_ctx, vm_matches);
  auto vm_count = vm_matches.size();

  EXPECT_EQ(ematcher_count, 1u) << "EMatcher should find exactly 1 match";
  EXPECT_EQ(vm_count, 1u) << "VM should find exactly 1 match";
  EXPECT_EQ(ematcher_count, vm_count) << "EMatcher and VM should agree on match count";
}

}  // namespace memgraph::planner::core
