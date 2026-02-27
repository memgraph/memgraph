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

// VM executor correctness tests: match results, same-variable handling, deduplication.

#include <sstream>

#include <gtest/gtest.h>

#include "planner/pattern/vm/compiler.hpp"
#include "planner/pattern/vm/executor.hpp"
#include "planner/pattern/vm/tracer.hpp"
#include "test_egraph_fixture.hpp"
#include "test_patterns.hpp"

import memgraph.planner.core.eids;

namespace memgraph::planner::core {

using namespace test;
using namespace vm;

// ============================================================================
// VM Execution with Tracer Tests
// ============================================================================

class VMExecutionTest : public EGraphTestBase {
 protected:
  EMatchContext ctx;
  std::vector<PatternMatch> results;
};

TEST_F(VMExecutionTest, SimpleMatch) {
  // Create e-graph: Neg(Const(42))
  auto c = leaf(Op::Const, 42);
  node(Op::Neg, c);
  rebuild_egraph();

  // Pattern: Neg(?x)
  auto pattern = TestPattern::build(Op::Neg, {Var{kVarX}}, kTestRoot);

  PatternCompiler<Op> compiler;
  auto compiled = compiler.compile(pattern);

  auto bytecode = disassemble<Op>(compiled->code(), compiled->symbols());

  // Execute with tracer
  RecordingTracer tracer;
  VMExecutorVerify<Op, NoAnalysis, true> executor(egraph, &tracer);

  executor.execute(*compiled, matcher, ctx, results);

  std::ostringstream trace_ss;
  tracer.print(trace_ss);

  EXPECT_EQ(results.size(), 1) << "Expected exactly 1 match\nBytecode:\n" << bytecode << "\nTrace:\n" << trace_ss.str();

  // Check that we found the right match
  if (!results.empty()) {
    auto bound_x = ctx.arena().get(results[0], 0);  // slot 0 = ?x
    EXPECT_EQ(egraph.find(bound_x), egraph.find(c)) << "?x should be bound to the Const node";
  }
}

TEST_F(VMExecutionTest, NoMatch) {
  // Create e-graph: Add(Const(1), Const(2))
  auto c1 = leaf(Op::Const, 1);
  auto c2 = leaf(Op::Const, 2);
  node(Op::Add, c1, c2);
  rebuild_egraph();

  // Pattern: Neg(?x) - should not match
  auto pattern = TestPattern::build(Op::Neg, {Var{kVarX}}, kTestRoot);

  PatternCompiler<Op> compiler;
  auto compiled = compiler.compile(pattern);

  VMExecutorVerify<Op, NoAnalysis> executor(egraph);

  executor.execute(*compiled, matcher, ctx, results);

  EXPECT_EQ(results.size(), 0) << "Expected no matches";
}

TEST_F(VMExecutionTest, MultipleMatches) {
  // Create e-graph with multiple Neg nodes
  auto c1 = leaf(Op::Const, 1);
  auto c2 = leaf(Op::Const, 2);
  node(Op::Neg, c1);
  node(Op::Neg, c2);
  node(Op::Neg, node(Op::Neg, c1));  // Neg(Neg(Const(1)))
  rebuild_egraph();

  // Pattern: Neg(?x)
  auto pattern = TestPattern::build(Op::Neg, {Var{kVarX}}, kTestRoot);

  PatternCompiler<Op> compiler;
  auto compiled = compiler.compile(pattern);

  VMExecutorVerify<Op, NoAnalysis> executor(egraph);

  executor.execute(*compiled, matcher, ctx, results);

  EXPECT_EQ(results.size(), 3) << "Expected 3 matches (n1, n2, n3 all contain Neg)";
}

TEST_F(VMExecutionTest, NestedPatternMatch) {
  // Create e-graph: Neg(Neg(Const(42)))
  auto c = leaf(Op::Const, 42);
  node(Op::Neg, node(Op::Neg, c));
  rebuild_egraph();

  // Pattern: Neg(Neg(?x))
  auto pattern = TestPattern::build(Op::Neg, {Sym(Op::Neg, Var{kVarX})}, kTestRoot);

  PatternCompiler<Op> compiler;
  auto compiled = compiler.compile(pattern);

  auto bytecode = disassemble<Op>(compiled->code(), compiled->symbols());

  VMExecutorVerify<Op, NoAnalysis> executor(egraph);

  executor.execute(*compiled, matcher, ctx, results);

  EXPECT_EQ(results.size(), 1) << "Expected exactly 1 match for Neg(Neg(?x))\nBytecode:\n" << bytecode;

  if (!results.empty()) {
    auto bound_x = ctx.arena().get(results[0], 0);  // slot 0 = ?x
    EXPECT_EQ(egraph.find(bound_x), egraph.find(c)) << "?x should be bound to the Const node";
  }
}

TEST_F(VMExecutionTest, SelfReferentialEClass) {
  // The self-referential case from the bug:
  // n0 = B(64)
  // n1 = F(n0)
  // n2 = F(n1)
  // merge(n1, n2) => EC1 = {F(n0), F(EC1)}
  auto n0 = leaf(Op::B, 64);
  auto n1 = node(Op::F, n0);
  auto n2 = node(Op::F, n1);

  merge(n1, n2);
  rebuild_egraph();

  // Pattern: F(F(?x))
  auto pattern = TestPattern::build(Op::F, {Sym(Op::F, Var{kVarX})}, kTestRoot);

  PatternCompiler<Op> compiler;
  auto compiled = compiler.compile(pattern);

  auto bytecode = disassemble<Op>(compiled->code(), compiled->symbols());

  RecordingTracer tracer;
  VMExecutorVerify<Op, NoAnalysis, true> executor(egraph, &tracer);

  executor.execute(*compiled, matcher, ctx, results);

  std::ostringstream trace_ss;
  tracer.print(trace_ss);

  std::ostringstream matches_ss;
  matches_ss << "Found " << results.size() << " matches:\n";
  for (std::size_t i = 0; i < results.size(); ++i) {
    auto bound_x = ctx.arena().get(results[i], 0);  // slot 0 = ?x
    matches_ss << "  Match " << i << ": ?x = " << bound_x.value_of() << "\n";
  }

  // Should find at least 2 matches due to self-reference
  EXPECT_GE(results.size(), 2) << "Expected at least 2 matches for self-referential F(F(?x))\nBytecode:\n"
                               << bytecode << "\nTrace:\n"
                               << trace_ss.str() << "\n"
                               << matches_ss.str();
}

// ============================================================================
// Deep Pattern Tests (regression for crash)
// ============================================================================

TEST_F(VMExecutionTest, DeepNestedPattern) {
  // Create a chain of Neg nodes: Neg(Neg(Neg(...Neg(x)...)))
  constexpr int kDepth = 10;

  auto x = leaf(Op::Const, 0);
  auto current = x;
  for (int i = 0; i < kDepth; ++i) {
    current = node(Op::Neg, current);
  }

  rebuild_egraph();

  // Pattern: Neg(Neg(Neg(?x))) - depth 3
  auto pattern = Pattern<Op>::build(Op::Neg, {Sym(Op::Neg, Sym(Op::Neg, Var{kVarX}))}, kTestRoot);

  PatternCompiler<Op> compiler;
  auto compiled = compiler.compile(pattern);

  auto bytecode = disassemble<Op>(compiled->code(), compiled->symbols());

  VMExecutorVerify<Op, NoAnalysis> executor(egraph);

  executor.execute(*compiled, matcher, ctx, results);

  // With depth 10 chain, pattern depth 3, we should find (10 - 3 + 1) = 8 matches
  EXPECT_EQ(results.size(), kDepth - 3 + 1)
      << "Expected " << (kDepth - 3 + 1) << " matches for depth-3 pattern on depth-" << kDepth << " chain\nBytecode:\n"
      << bytecode;
}

TEST_F(VMExecutionTest, VeryDeepNestedPattern) {
  // Create a chain of Neg nodes: Neg(Neg(Neg(...Neg(x)...)))
  constexpr int kDepth = 50;

  auto x = leaf(Op::Const, 0);
  auto current = x;
  for (int i = 0; i < kDepth; ++i) {
    current = node(Op::Neg, current);
  }

  rebuild_egraph();

  // Build a deep pattern dynamically
  constexpr int kPatternDepth = 10;
  auto b = Pattern<Op>::Builder{};
  auto cur = b.var(kVarX);
  for (int i = 0; i < kPatternDepth; ++i) {
    cur = b.sym(Op::Neg, {cur});
  }
  auto pattern = std::move(b).build();

  PatternCompiler<Op> compiler;
  auto compiled = compiler.compile(pattern);

  auto bytecode = disassemble<Op>(compiled->code(), compiled->symbols());

  VMExecutorVerify<Op, NoAnalysis> executor(egraph);

  executor.execute(*compiled, matcher, ctx, results);

  // With depth 50 chain, pattern depth 10, we should find (50 - 10 + 1) = 41 matches
  EXPECT_EQ(results.size(), kDepth - kPatternDepth + 1)
      << "Expected " << (kDepth - kPatternDepth + 1) << " matches for depth-" << kPatternDepth << " pattern on depth-"
      << kDepth << " chain\nBytecode:\n"
      << bytecode;
}

// Test that deep patterns compile successfully (dynamic register allocation)
TEST_F(VMExecutionTest, DeepPatternCompiles) {
  // VM now supports dynamic register allocation (up to 256 due to uint8_t)
  // Deep patterns need ~2*depth+1 registers, so depth 35 uses ~71 registers
  constexpr int kPatternDepth = 35;

  auto b = Pattern<Op>::Builder{};
  auto cur = b.var(kVarX);
  for (int i = 0; i < kPatternDepth; ++i) {
    cur = b.sym(Op::Neg, {cur});
  }
  auto pattern = std::move(b).build();

  PatternCompiler<Op> compiler;
  auto compiled = compiler.compile(pattern);

  // Should compile successfully with dynamic registers
  ASSERT_TRUE(compiled.has_value()) << "Deep patterns should compile with dynamic registers";
  // Deep patterns need ~depth eclass registers (LoadChild) and ~depth enode registers (IterENodes)
  EXPECT_GE(compiled->num_eclass_regs() + compiled->num_enode_regs(), kPatternDepth)
      << "Should allocate enough registers";
}

// ============================================================================
// Same Variable Correctness Tests
// ============================================================================

// Test: Verify same variable pattern works correctly with merged e-classes.
// Pattern Add(?x, Neg(?x)) uses CheckSlot for the second ?x occurrence.
TEST_F(VMExecutionTest, SameVariableMergedEClass) {
  // Create e-graph with merged e-class containing:
  //   n0 = Add(a, Neg(a))  -- ?x binds to a, Neg child is a, should match
  //   n1 = Add(a, Neg(b))  -- ?x binds to a, Neg child is b, should NOT match
  //
  // After merge(n0, n1), both are in same e-class.
  // Pattern Add(?x, Neg(?x)) should find 1 match (only n0).

  auto a = leaf(Op::Const, 1);
  auto b = leaf(Op::Const, 2);
  auto neg_a = node(Op::Neg, a);
  auto neg_b = node(Op::Neg, b);
  auto n0 = node(Op::Add, a, neg_a);  // Add(a, Neg(a)) - should match
  auto n1 = node(Op::Add, a, neg_b);  // Add(a, Neg(b)) - should NOT match

  // Merge the two Add nodes into same e-class
  merge(n0, n1);
  rebuild_egraph();

  // Verify both Add nodes are in same e-class
  ASSERT_EQ(egraph.find(n0), egraph.find(n1)) << "Both Add nodes should be in same e-class after merge";

  // Pattern: Add(?x, Neg(?x))
  auto pattern = TestPattern::build(Op::Add, {Var{kVarX}, Sym(Op::Neg, Var{kVarX})}, kTestRoot);

  PatternCompiler<Op> compiler;
  auto compiled = compiler.compile(pattern);
  ASSERT_TRUE(compiled.has_value());

  auto bytecode = disassemble<Op>(compiled->code(), compiled->symbols());

  RecordingTracer tracer;
  VMExecutorVerify<Op, NoAnalysis, true> executor(egraph, &tracer);

  EMatchContext ctx;
  std::vector<PatternMatch> results;
  executor.execute(*compiled, matcher, ctx, results);

  std::ostringstream trace_ss;
  tracer.print(trace_ss);

  std::ostringstream matches_ss;
  matches_ss << "Found " << results.size() << " matches:\n";
  for (std::size_t i = 0; i < results.size(); ++i) {
    auto bound_x = ctx.arena().get(results[i], 0);
    matches_ss << "  Match " << i << ": ?x = EC" << bound_x.value_of() << "\n";
  }

  // Should find exactly 1 match: Add(a, Neg(a)) where ?x = a
  // Bug would cause 2 matches if second Add(a, Neg(b)) incorrectly matches
  EXPECT_EQ(results.size(), 1) << "Should find exactly 1 match (Add(a, Neg(a))), not 2\nBytecode:\n"
                               << bytecode << "\nTrace:\n"
                               << trace_ss.str() << matches_ss.str();
}

// Critical bug test: Inner loop with multiple e-nodes, outer has one
// This specifically tests when Yield clears bound and inner continues
TEST_F(VMExecutionTest, SameVariableInnerLoopMultipleENodes) {
  // Pattern: Add(?x, Neg(?x))
  // E-graph:
  //   a = Const(1)
  //   b = Const(2)
  //   neg_merged = Neg(a) MERGED WITH Neg(b)  <- inner has 2 e-nodes!
  //   add = Add(a, neg_merged)  <- outer has 1 e-node
  //
  // Expected: 1 match (Add(a, Neg(a)))
  // Bug would cause 2 matches (incorrectly matching Add(a, Neg(b)))
  //
  // IMPORTANT: Create neg_b first so neg_a comes SECOND in the e-class.
  // After merge, iteration order is typically: neg_a, neg_b
  // This ensures the matching one (neg_a) is tried first, yields, clears bound,
  // and then neg_b should fail but the bug would cause it to rebind.

  auto a = leaf(Op::Const, 1);
  auto b = leaf(Op::Const, 2);

  // Create neg_b first, then neg_a - this affects iteration order after merge
  auto neg_b = node(Op::Neg, b);
  auto neg_a = node(Op::Neg, a);

  // Merge into neg_b's class (neg_a merges into neg_b)
  // After merge, iteration should be: neg_a, neg_b (insertion order matters)
  merge(neg_b, neg_a);
  rebuild_egraph();

  // Create Add that uses 'a' and the merged Neg e-class
  auto add = node(Op::Add, a, neg_b);  // Uses the merged Neg e-class
  rebuild_egraph();

  // Verify the structure
  auto neg_class = egraph.find(neg_a);
  ASSERT_EQ(egraph.find(neg_b), neg_class) << "Neg nodes should be in same e-class";
  auto const &neg_eclass = egraph.eclass(neg_class);
  ASSERT_EQ(neg_eclass.nodes().size(), 2) << "Neg e-class should have 2 e-nodes";

  // Pattern: Add(?x, Neg(?x))
  auto pattern = TestPattern::build(Op::Add, {Var{kVarX}, Sym(Op::Neg, Var{kVarX})}, kTestRoot);

  PatternCompiler<Op> compiler;
  auto compiled = compiler.compile(pattern);
  ASSERT_TRUE(compiled.has_value());

  auto bytecode = disassemble<Op>(compiled->code(), compiled->symbols());

  RecordingTracer tracer;
  VMExecutorVerify<Op, NoAnalysis, true> executor(egraph, &tracer);

  EMatchContext ctx;
  std::vector<PatternMatch> results;
  executor.execute(*compiled, matcher, ctx, results);

  std::ostringstream trace_ss;
  tracer.print(trace_ss);

  std::ostringstream matches_ss;
  matches_ss << "Found " << results.size() << " matches:\n";
  for (std::size_t i = 0; i < results.size(); ++i) {
    auto bound_x = ctx.arena().get(results[i], 0);
    matches_ss << "  Match " << i << ": ?x = EC" << bound_x.value_of() << "\n";
  }

  // CRITICAL: Should find exactly 1 match
  // The inner loop iterates Neg(a) and Neg(b).
  // - Neg(a): ?x = a, child = a, check passes -> Yield
  // - Neg(b): ?x should still be bound to a, child = b, check should FAIL
  // Bug: After Yield, bound is cleared, so ?x gets rebound to b, and we get
  // an incorrect second match.
  EXPECT_EQ(results.size(), 1) << "Should find exactly 1 match. Bug causes 2 matches.\nBytecode:\n"
                               << bytecode << "\nTrace:\n"
                               << trace_ss.str() << matches_ss.str();
}

// Additional test: Three e-nodes, only one matches
TEST_F(VMExecutionTest, SameVariableMultipleMergedENodes) {
  // n0 = Add(a, Neg(a))  -- matches
  // n1 = Add(a, Neg(b))  -- no match
  // n2 = Add(b, Neg(b))  -- matches

  auto a = leaf(Op::Const, 1);
  auto b = leaf(Op::Const, 2);
  auto neg_a = node(Op::Neg, a);
  auto neg_b = node(Op::Neg, b);
  auto n0 = node(Op::Add, a, neg_a);  // matches
  auto n1 = node(Op::Add, a, neg_b);  // no match
  auto n2 = node(Op::Add, b, neg_b);  // matches

  // Merge all into same e-class
  merge(n0, n1);
  merge(n1, n2);
  rebuild_egraph();

  // Pattern: Add(?x, Neg(?x))
  auto pattern = TestPattern::build(Op::Add, {Var{kVarX}, Sym(Op::Neg, Var{kVarX})}, kTestRoot);

  PatternCompiler<Op> compiler;
  auto compiled = compiler.compile(pattern);
  ASSERT_TRUE(compiled.has_value());

  VMExecutorVerify<Op, NoAnalysis> executor(egraph);

  EMatchContext ctx;
  std::vector<PatternMatch> results;
  executor.execute(*compiled, matcher, ctx, results);

  // Should find 2 matches: Add(a, Neg(a)) and Add(b, Neg(b))
  EXPECT_EQ(results.size(), 2) << "Should find 2 matches (Add(a,Neg(a)) and Add(b,Neg(b)))";
}

// ============================================================================
// Deduplication Tests
// ============================================================================

// Test that VM executor correctly deduplicates matches
// This tests the bind-time dedup mechanism with self-referential e-classes
TEST_F(VMExecutionTest, DeduplicationSelfReferentialEClass) {
  // Create a self-referential e-class where multiple paths lead to the same binding:
  //   a = Const(1)
  //   n0 = F(a)
  //   n1 = F(n0)
  //   merge(n0, n1) => EC1 = {F(a), F(EC1)}
  //
  // Pattern F(?x) matching against EC1 will iterate both e-nodes:
  //   - F(a) binds ?x = a
  //   - F(EC1) binds ?x = EC1
  // These are different bindings, so both should be returned (2 matches).
  //
  // But Pattern F(F(?x)) matching against EC1:
  //   - F(a) -> inner F(?x) needs F symbol, a has no F, no match
  //   - F(EC1) -> inner F(?x) iterates EC1's e-nodes:
  //       - F(a) binds ?x = a
  //       - F(EC1) binds ?x = EC1
  // So we get 2 matches, which are distinct (no dedup needed here).
  //
  // For actual deduplication, we need multiple candidates that lead to the same binding.

  auto a = leaf(Op::Const, 1);
  auto n0 = node(Op::F, a);
  auto n1 = node(Op::F, n0);

  merge(n0, n1);
  rebuild_egraph();

  // EC1 = {F(a), F(EC1)} - self-referential
  auto ec1 = egraph.find(n0);

  // Pattern: F(?x)
  auto pattern = TestPattern::build(Op::F, {Var{kVarX}}, kTestRoot);

  PatternCompiler<Op> compiler;
  auto compiled = compiler.compile(pattern);
  ASSERT_TRUE(compiled.has_value());

  VMExecutorVerify<Op, NoAnalysis> executor(egraph);

  // EC1 has 2 e-nodes with different children - F(a) and F(EC1)
  executor.execute(*compiled, matcher, ctx, results);

  // Should get 2 distinct matches: {?x=a} and {?x=EC1}
  EXPECT_EQ(results.size(), 2) << "Self-referential e-class should yield 2 distinct matches";
}

// Test that a simple pattern produces exactly one match
TEST_F(VMExecutionTest, SingleMatchForSimplePattern) {
  // Simple test: one F node should produce one match.

  auto a = leaf(Op::Const, 1);
  node(Op::F, a);
  rebuild_egraph();

  // Pattern: F(?x)
  auto pattern = TestPattern::build(Op::F, {Var{kVarX}}, kTestRoot);

  PatternCompiler<Op> compiler;
  auto compiled = compiler.compile(pattern);
  ASSERT_TRUE(compiled.has_value());

  VMExecutorVerify<Op, NoAnalysis> executor(egraph);

  executor.execute(*compiled, matcher, ctx, results);

  // Should get exactly 1 match
  EXPECT_EQ(results.size(), 1) << "Should find exactly 1 match for single F node";
}

// Test deduplication with merged e-classes that have different symbols
TEST_F(VMExecutionTest, DeduplicationMergedDifferentSymbols) {
  // Create e-class with multiple e-nodes of different symbols pointing to same child:
  //   a = Const(1)
  //   f_a = F(a)
  //   f2_a = F2(a)
  //   merge(f_a, f2_a) => EC1 = {F(a), F2(a)}
  //
  // Pattern F(?x) should match only F(a), not F2(a).

  auto a = leaf(Op::Const, 1);
  auto f_a = node(Op::F, a);
  auto f2_a = node(Op::F2, a);

  merge(f_a, f2_a);
  rebuild_egraph();

  auto ec1 = egraph.find(f_a);
  auto const &eclass = egraph.eclass(ec1);
  ASSERT_EQ(eclass.nodes().size(), 2) << "Merged e-class should have F(a) and F2(a)";

  // Pattern: F(?x) - should match only the F(a) e-node
  auto pattern = TestPattern::build(Op::F, {Var{kVarX}}, kTestRoot);

  PatternCompiler<Op> compiler;
  auto compiled = compiler.compile(pattern);
  ASSERT_TRUE(compiled.has_value());

  VMExecutorVerify<Op, NoAnalysis> executor(egraph);

  executor.execute(*compiled, matcher, ctx, results);

  // Should get exactly 1 match: {?x=a} from F(a), F2(a) doesn't match F pattern
  EXPECT_EQ(results.size(), 1) << "Should match only F(a), not F2(a)";

  if (!results.empty()) {
    auto bound_x = ctx.arena().get(results[0], 0);
    EXPECT_EQ(egraph.find(bound_x), egraph.find(a)) << "?x should be bound to 'a'";
  }
}

// ============================================================================
// Deduplication Edge Case Tests
// ============================================================================

// Test: max_seen_slot_ tracking with non-sequential slot binding
// Bug: bind() incorrectly sets max_seen_slot_ = end after clearing seen sets
// This could cause incorrect deduplication behavior when slots are bound out of order
TEST_F(VMExecutionTest, DeduplicationSlotOrderIndependence) {
  // Create a pattern where the slot binding order might not match slot indices
  // Pattern: F(?y, ?x) - variables may be bound as y=slot0, x=slot1 or vice versa
  //
  // E-graph with multiple F nodes that should yield unique matches:
  //   F(a, b), F(a, c), F(b, a)
  // Should yield 3 unique matches, no duplicates

  auto a = leaf(Op::Const, 1);
  auto b = leaf(Op::Const, 2);
  auto c = leaf(Op::Const, 3);

  auto f1 = node(Op::F, a, b);  // F(a, b)
  auto f2 = node(Op::F, a, c);  // F(a, c)
  auto f3 = node(Op::F, b, a);  // F(b, a)

  rebuild_egraph();

  // Pattern: F(?x, ?y)
  auto pattern = TestPattern::build(Op::F, {Var{kVarX}, Var{kVarY}}, kTestRoot);

  PatternCompiler<Op> compiler;
  auto compiled = compiler.compile(pattern);
  ASSERT_TRUE(compiled.has_value());

  VMExecutorVerify<Op, NoAnalysis> executor(egraph);

  executor.execute(*compiled, matcher, ctx, results);

  // Should find exactly 3 unique matches
  EXPECT_EQ(results.size(), 3) << "Should find 3 unique matches for F(?x, ?y)";
}

// Test: Deduplication with repeated candidate causing same bindings
// Verifies that deduplication correctly prevents duplicate matches when
// the same binding tuple is reached via different paths
TEST_F(VMExecutionTest, DeduplicationMultiplePaths) {
  // Create e-graph where same binding can be reached multiple ways:
  //   a, b are leaves
  //   f1 = F(a, b)
  //   f2 = F(a, b) - structurally identical to f1
  //
  // After e-graph rebuild, f1 and f2 should be in same e-class (structural sharing)
  // But if we provide both as candidates, deduplication should prevent duplicates

  auto a = leaf(Op::Const, 1);
  auto b = leaf(Op::Const, 2);

  auto f1 = node(Op::F, a, b);
  auto f2 = node(Op::F, a, b);  // Identical structure

  rebuild_egraph();

  // After rebuild, f1 and f2 should be same e-class
  ASSERT_EQ(egraph.find(f1), egraph.find(f2)) << "Identical F(a,b) nodes should merge";

  auto pattern = TestPattern::build(Op::F, {Var{kVarX}, Var{kVarY}}, kTestRoot);

  PatternCompiler<Op> compiler;
  auto compiled = compiler.compile(pattern);
  ASSERT_TRUE(compiled.has_value());

  VMExecutorVerify<Op, NoAnalysis> executor(egraph);

  executor.execute(*compiled, matcher, ctx, results);

  // Should find only 1 unique match (identical F(a,b) nodes merge)
  EXPECT_EQ(results.size(), 1) << "Identical nodes should produce single match";
}

// Test: Deduplication with changing prefix (slot rebinding)
// This tests the seen_per_slot clearing logic when a slot value changes
TEST_F(VMExecutionTest, DeduplicationPrefixChange) {
  // Create pattern: Add(?x, F(?x, ?y))
  // This binds ?x first (from Add's first child), then ?y (from F's second child)
  //
  // E-graph:
  //   a = Const(1), b = Const(2), c = Const(3)
  //   f_ab = F(a, b), f_ac = F(a, c)
  //   add1 = Add(a, f_ab) - matches with ?x=a, ?y=b
  //   add2 = Add(a, f_ac) - matches with ?x=a, ?y=c
  //
  // Both should match (different ?y values)

  auto a = leaf(Op::Const, 1);
  auto b = leaf(Op::Const, 2);
  auto c = leaf(Op::Const, 3);

  auto f_ab = node(Op::F, a, b);
  auto f_ac = node(Op::F, a, c);
  auto add1 = node(Op::Add, a, f_ab);
  auto add2 = node(Op::Add, a, f_ac);

  rebuild_egraph();

  // Pattern: Add(?x, F(?x, ?y)) - ?x appears twice, ?y once
  auto pattern = TestPattern::build(Op::Add, {Var{kVarX}, Sym(Op::F, Var{kVarX}, Var{kVarY})}, kTestRoot);

  PatternCompiler<Op> compiler;
  auto compiled = compiler.compile(pattern);
  ASSERT_TRUE(compiled.has_value());

  VMExecutorVerify<Op, NoAnalysis> executor(egraph);

  executor.execute(*compiled, matcher, ctx, results);

  // Should find 2 unique matches: (?x=a, ?y=b) and (?x=a, ?y=c)
  EXPECT_EQ(results.size(), 2) << "Should find 2 matches with same ?x but different ?y";
}

// Test: Wide e-class with many e-nodes and deduplication
// This stress-tests the deduplication when iterating through many e-nodes
TEST_F(VMExecutionTest, DeduplicationWideEClass) {
  // Create multiple F(a, x) nodes for different x values, then merge them
  // Pattern F(?x, ?y) should find each unique binding once

  auto a = leaf(Op::Const, 0);
  std::vector<EClassId> f_nodes;

  constexpr std::size_t kNumVariants = 50;
  for (std::size_t i = 1; i <= kNumVariants; ++i) {
    auto x = leaf(Op::Const, i);
    f_nodes.push_back(node(Op::F, a, x));
  }

  // Merge all F nodes into one e-class
  for (std::size_t i = 1; i < f_nodes.size(); ++i) {
    merge(f_nodes[0], f_nodes[i]);
  }
  rebuild_egraph();

  auto f_class = egraph.find(f_nodes[0]);
  auto const &eclass = egraph.eclass(f_class);
  ASSERT_EQ(eclass.nodes().size(), kNumVariants) << "All F nodes should be in same e-class";

  auto pattern = TestPattern::build(Op::F, {Var{kVarX}, Var{kVarY}}, kTestRoot);

  PatternCompiler<Op> compiler;
  auto compiled = compiler.compile(pattern);
  ASSERT_TRUE(compiled.has_value());

  VMExecutorVerify<Op, NoAnalysis> executor(egraph);

  executor.execute(*compiled, matcher, ctx, results);

  // Should find kNumVariants unique matches (each F(a, xi) is unique)
  EXPECT_EQ(results.size(), kNumVariants) << "Should find " << kNumVariants << " unique matches";
}

// ============================================================================
// Multi-Pattern Execution Tests
// ============================================================================

TEST_F(VMExecutionTest, MultiPattern_JoinWithParentTraversal) {
  // Build e-graph:
  //   sym_val = Const(1)
  //   expr_val = Const(2)
  //   bind_node = Bind(Const(0), sym_val, expr_val)
  //   ident_node = Ident(sym_val)  <- shares sym_val with bind_node
  auto placeholder = leaf(Op::Const, 0);
  auto sym_val = leaf(Op::Const, 1);
  auto expr_val = leaf(Op::Const, 2);
  auto bind_node = node(Op::Bind, placeholder, sym_val, expr_val);
  [[maybe_unused]] auto ident_node = node(Op::Ident, sym_val);

  rebuild_egraph();

  // Anchor pattern: Bind(_, ?sym, ?expr)
  constexpr PatternVar kVarSym{1};
  constexpr PatternVar kVarExpr{2};
  constexpr PatternVar kVarId{3};

  auto anchor = Pattern<Op>::build(Op::Bind, {Wildcard{}, Var{kVarSym}, Var{kVarExpr}}, kTestRoot);
  auto joined = Pattern<Op>::build(Op::Ident, {Var{kVarSym}}, kVarId);

  PatternCompiler<Op> compiler;
  std::array patterns = {anchor, joined};
  auto compiled = compiler.compile(patterns);
  ASSERT_TRUE(compiled.has_value());

  VMExecutorVerify<Op, NoAnalysis> executor(egraph);

  executor.execute(*compiled, matcher, ctx, results);

  // Should find 1 match: Bind paired with Ident (both reference same sym_val)
  EXPECT_EQ(results.size(), 1) << "Should find exactly 1 match where Bind and Ident share ?sym";
}

TEST_F(VMExecutionTest, MultiPattern_NoMatchingJoin) {
  // Ident uses different symbol than Bind - no match expected
  auto placeholder = leaf(Op::Const, 0);
  auto sym_val1 = leaf(Op::Const, 1);
  auto sym_val2 = leaf(Op::Const, 2);
  auto expr_val = leaf(Op::Const, 3);
  auto bind_node = node(Op::Bind, placeholder, sym_val1, expr_val);
  node(Op::Ident, sym_val2);  // Uses different sym!

  rebuild_egraph();

  constexpr PatternVar kVarSym{1};
  constexpr PatternVar kVarExpr{2};
  constexpr PatternVar kVarId{3};

  auto anchor = Pattern<Op>::build(Op::Bind, {Wildcard{}, Var{kVarSym}, Var{kVarExpr}}, kTestRoot);
  auto joined = Pattern<Op>::build(Op::Ident, {Var{kVarSym}}, kVarId);

  PatternCompiler<Op> compiler;
  std::array patterns = {anchor, joined};
  auto compiled = compiler.compile(patterns);
  ASSERT_TRUE(compiled.has_value());

  VMExecutorVerify<Op, NoAnalysis> executor(egraph);

  executor.execute(*compiled, matcher, ctx, results);

  EXPECT_EQ(results.size(), 0) << "Should find no matches when Ident uses different symbol";
}

TEST_F(VMExecutionTest, MultiPattern_MultipleJoinMatches) {
  // Multiple Ident nodes reference same symbol -> multiple matches
  auto placeholder = leaf(Op::Const, 0);
  auto sym_val = leaf(Op::Const, 1);
  auto expr_val = leaf(Op::Const, 2);
  auto bind_node = node(Op::Bind, placeholder, sym_val, expr_val);
  auto ident1 = node(Op::Ident, sym_val);
  auto ident2 = node(Op::Ident, sym_val);

  rebuild_egraph();

  constexpr PatternVar kVarSym{1};
  constexpr PatternVar kVarExpr{2};
  constexpr PatternVar kVarId{3};

  auto anchor = Pattern<Op>::build(Op::Bind, {Wildcard{}, Var{kVarSym}, Var{kVarExpr}}, kTestRoot);
  auto joined = Pattern<Op>::build(Op::Ident, {Var{kVarSym}}, kVarId);

  PatternCompiler<Op> compiler;
  std::array patterns = {anchor, joined};
  auto compiled = compiler.compile(patterns);
  ASSERT_TRUE(compiled.has_value());

  VMExecutorVerify<Op, NoAnalysis> executor(egraph);

  executor.execute(*compiled, matcher, ctx, results);

  // Idents may merge due to structural sharing
  bool idents_same_eclass = (egraph.find(ident1) == egraph.find(ident2));
  std::size_t expected_matches = idents_same_eclass ? 1 : 2;

  EXPECT_EQ(results.size(), expected_matches) << "Expected " << expected_matches << " matches";
}

TEST_F(VMExecutionTest, MultiPattern_NestedJoinedPattern) {
  // Joined pattern with nested symbols: F(?sym, Neg(?x))
  constexpr PatternVar kVarSym{0};
  constexpr PatternVar kVarX{1};
  constexpr PatternVar kBindRoot{2};

  auto sym_val = leaf(Op::Const, 1);
  auto x_val = leaf(Op::Const, 2);
  auto neg_x = node(Op::Neg, x_val);
  [[maybe_unused]] auto f_node = node(Op::F, sym_val, neg_x);
  [[maybe_unused]] auto bind_node = node(Op::Bind, leaf(Op::A), sym_val, leaf(Op::B));

  rebuild_egraph();

  auto anchor = Pattern<Op>::build(Op::Bind, {Wildcard{}, Var{kVarSym}, Wildcard{}}, kBindRoot);
  auto joined = Pattern<Op>::build(Op::F, {Var{kVarSym}, Sym(Op::Neg, Var{kVarX})});

  PatternCompiler<Op> compiler;
  std::array patterns = {anchor, joined};
  auto compiled = compiler.compile(patterns);
  ASSERT_TRUE(compiled.has_value());

  VMExecutorVerify<Op, NoAnalysis> executor(egraph);

  executor.execute(*compiled, matcher, ctx, results);

  EXPECT_EQ(results.size(), 1) << "Should find 1 match for nested pattern";
}

TEST_F(VMExecutionTest, MultiPattern_ThreePatternJoin) {
  // Three patterns with shared variables
  auto placeholder = leaf(Op::Const, 0);
  auto sym_val = leaf(Op::Const, 1);
  auto expr_val = leaf(Op::Const, 2);
  auto bind_node = node(Op::Bind, placeholder, sym_val, expr_val);
  [[maybe_unused]] auto ident_node = node(Op::Ident, sym_val);
  [[maybe_unused]] auto neg_node = node(Op::Neg, sym_val);

  rebuild_egraph();

  constexpr PatternVar kVarSym{1};
  constexpr PatternVar kVarIdent{2};
  constexpr PatternVar kVarNeg{3};

  auto anchor = Pattern<Op>::build(Op::Bind, {Wildcard{}, Var{kVarSym}, Wildcard{}}, kTestRoot);
  auto joined_ident = Pattern<Op>::build(Op::Ident, {Var{kVarSym}}, kVarIdent);
  auto joined_neg = Pattern<Op>::build(Op::Neg, {Var{kVarSym}}, kVarNeg);

  PatternCompiler<Op> compiler;
  std::array patterns = {anchor, joined_ident, joined_neg};
  auto compiled = compiler.compile(patterns);
  ASSERT_TRUE(compiled.has_value());

  VMExecutorVerify<Op, NoAnalysis> executor(egraph);

  executor.execute(*compiled, matcher, ctx, results);

  EXPECT_EQ(results.size(), 1) << "Should find 1 match for three-pattern join";
}

TEST_F(VMExecutionTest, MultiPattern_ManyParentTraversals) {
  // Multiple distinct parents of shared variable
  auto placeholder = leaf(Op::Const, 0);
  auto sym_val = leaf(Op::Const, 1);
  auto expr_val = leaf(Op::Const, 2);
  auto bind_node = node(Op::Bind, placeholder, sym_val, expr_val);

  // Create 3 structurally distinct F2 parents
  [[maybe_unused]] auto f2_1 = node(Op::F2, sym_val, leaf(Op::Const, 101));
  [[maybe_unused]] auto f2_2 = node(Op::F2, sym_val, leaf(Op::Const, 102));
  [[maybe_unused]] auto f2_3 = node(Op::F2, sym_val, leaf(Op::Const, 103));

  // Create 1 Neg parent
  [[maybe_unused]] auto neg_1 = node(Op::Neg, sym_val);

  rebuild_egraph();

  constexpr PatternVar kVarSym{1};
  constexpr PatternVar kVarF2{2};
  constexpr PatternVar kVarNeg{3};

  auto anchor = Pattern<Op>::build(Op::Bind, {Wildcard{}, Var{kVarSym}, Wildcard{}}, kTestRoot);
  auto joined_f2 = Pattern<Op>::build(Op::F2, {Var{kVarSym}, Wildcard{}}, kVarF2);
  auto joined_neg = Pattern<Op>::build(Op::Neg, {Var{kVarSym}}, kVarNeg);

  PatternCompiler<Op> compiler;
  std::array patterns = {anchor, joined_f2, joined_neg};
  auto compiled = compiler.compile(patterns);
  ASSERT_TRUE(compiled.has_value());

  VMExecutorVerify<Op, NoAnalysis> executor(egraph);

  executor.execute(*compiled, matcher, ctx, results);

  // Expected: 3 F2 e-classes * 1 Neg e-class = 3 matches
  EXPECT_EQ(results.size(), 3) << "Expected 3 F2 * 1 Neg = 3 matches";
}

TEST_F(VMExecutionTest, MultiPattern_DeduplicationWithPrefixChange) {
  // Tests deduplication when prefix slot changes
  auto a1 = leaf(Op::Const, 1);
  auto a2 = leaf(Op::Const, 2);
  auto b1 = leaf(Op::Const, 10);
  auto c1 = leaf(Op::Const, 100);
  auto c2 = leaf(Op::Const, 101);

  auto f1 = node(Op::F, a1, b1);
  auto f2 = node(Op::F, a2, b1);
  [[maybe_unused]] auto g1 = node(Op::F2, b1, c1);
  [[maybe_unused]] auto g2 = node(Op::F2, b1, c2);

  rebuild_egraph();

  constexpr PatternVar kVarA{0};
  constexpr PatternVar kVarB{1};
  constexpr PatternVar kVarC{2};
  constexpr PatternVar kVarF{3};
  constexpr PatternVar kVarF2{4};

  auto pattern_f = Pattern<Op>::build(Op::F, {Var{kVarA}, Var{kVarB}}, kVarF);
  auto pattern_f2 = Pattern<Op>::build(Op::F2, {Var{kVarB}, Var{kVarC}}, kVarF2);

  PatternCompiler<Op> compiler;
  std::array patterns = {pattern_f, pattern_f2};
  auto compiled = compiler.compile(patterns);
  ASSERT_TRUE(compiled.has_value());

  VMExecutorVerify<Op, NoAnalysis> executor(egraph);

  executor.execute(*compiled, matcher, ctx, results);

  // Expected: 2 F's * 2 F2's = 4 matches
  EXPECT_EQ(results.size(), 4) << "Expected 4 unique matches (2 F's * 2 F2's)";
}

TEST_F(VMExecutionTest, MultiPattern_DeduplicationMultiplePaths) {
  // Same tuple reachable via multiple paths should be deduplicated
  auto x = leaf(Op::Const, 1);
  auto f_xx = node(Op::F, x, x);  // F(x, x)

  [[maybe_unused]] auto x2 = leaf(Op::Const, 1);  // Same value, will merge
  rebuild_egraph();

  constexpr PatternVar kVarA{0};
  constexpr PatternVar kVarF{1};

  auto pattern = Pattern<Op>::build(Op::F, {Var{kVarA}, Var{kVarA}}, kVarF);

  PatternCompiler<Op> compiler;
  auto compiled = compiler.compile(pattern);
  ASSERT_TRUE(compiled.has_value());

  VMExecutorVerify<Op, NoAnalysis> executor(egraph);

  executor.execute(*compiled, matcher, ctx, results);

  EXPECT_EQ(results.size(), 1) << "Should find exactly 1 match for F(?a, ?a)";
}

}  // namespace memgraph::planner::core
