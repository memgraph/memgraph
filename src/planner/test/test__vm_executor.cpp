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
  VMExecutorVerify<Op, NoAnalysis, RecordingTracer> executor(egraph, &tracer);

  std::vector<EClassId> candidates;
  for (auto id : egraph.canonical_class_ids()) {
    candidates.push_back(id);
  }

  executor.execute(*compiled, candidates, ctx, results);

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

  std::vector<EClassId> candidates;
  for (auto id : egraph.canonical_class_ids()) {
    candidates.push_back(id);
  }

  executor.execute(*compiled, candidates, ctx, results);

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

  std::vector<EClassId> candidates;
  for (auto id : egraph.canonical_class_ids()) {
    candidates.push_back(id);
  }

  executor.execute(*compiled, candidates, ctx, results);

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

  std::vector<EClassId> candidates;
  for (auto id : egraph.canonical_class_ids()) {
    candidates.push_back(id);
  }

  executor.execute(*compiled, candidates, ctx, results);

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
  VMExecutorVerify<Op, NoAnalysis, RecordingTracer> executor(egraph, &tracer);

  std::vector<EClassId> candidates;
  for (auto id : egraph.canonical_class_ids()) {
    candidates.push_back(id);
  }

  executor.execute(*compiled, candidates, ctx, results);

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

  std::vector<EClassId> candidates;
  for (auto id : egraph.canonical_class_ids()) {
    candidates.push_back(id);
  }

  executor.execute(*compiled, candidates, ctx, results);

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

  std::vector<EClassId> candidates;
  for (auto id : egraph.canonical_class_ids()) {
    candidates.push_back(id);
  }

  executor.execute(*compiled, candidates, ctx, results);

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

// Bug: After Yield clears bound flags, BindOrCheck for repeated variable
// incorrectly rebinds instead of checking when iterating multiple e-nodes.
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
  VMExecutorVerify<Op, NoAnalysis, RecordingTracer> executor(egraph, &tracer);

  // Use the merged Add e-class as candidate
  std::vector<EClassId> candidates = {egraph.find(n0)};

  EMatchContext ctx;
  std::vector<PatternMatch> results;
  executor.execute(*compiled, candidates, ctx, results);

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
  VMExecutorVerify<Op, NoAnalysis, RecordingTracer> executor(egraph, &tracer);

  std::vector<EClassId> candidates = {egraph.find(add)};

  EMatchContext ctx;
  std::vector<PatternMatch> results;
  executor.execute(*compiled, candidates, ctx, results);

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
  std::vector<EClassId> candidates = {egraph.find(n0)};

  EMatchContext ctx;
  std::vector<PatternMatch> results;
  executor.execute(*compiled, candidates, ctx, results);

  // Should find 2 matches: Add(a, Neg(a)) and Add(b, Neg(b))
  EXPECT_EQ(results.size(), 2) << "Should find 2 matches (Add(a,Neg(a)) and Add(b,Neg(b)))";
}

// ============================================================================
// Deduplication Tests
// ============================================================================

// Test that VM executor correctly deduplicates matches
// This tests the try_yield_dedup mechanism with self-referential e-classes
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

  // Use EC1 as candidate - it has 2 e-nodes with different children
  std::vector<EClassId> candidates = {ec1};
  executor.execute(*compiled, candidates, ctx, results);

  // Should get 2 distinct matches: {?x=a} and {?x=EC1}
  EXPECT_EQ(results.size(), 2) << "Self-referential e-class should yield 2 distinct matches";
}

// Test deduplication when same candidate is provided multiple times
TEST_F(VMExecutionTest, DeduplicationDuplicateCandidates) {
  // If the same candidate is provided twice, deduplication should prevent
  // duplicate matches.

  auto a = leaf(Op::Const, 1);
  auto f_a = node(Op::F, a);

  // Pattern: F(?x)
  auto pattern = TestPattern::build(Op::F, {Var{kVarX}}, kTestRoot);

  PatternCompiler<Op> compiler;
  auto compiled = compiler.compile(pattern);
  ASSERT_TRUE(compiled.has_value());

  VMExecutorVerify<Op, NoAnalysis> executor(egraph);

  // Provide the same candidate twice
  auto candidate = egraph.find(f_a);
  std::vector<EClassId> candidates = {candidate, candidate};
  executor.execute(*compiled, candidates, ctx, results);

  // Should get only 1 match despite duplicate candidates
  EXPECT_EQ(results.size(), 1) << "Should deduplicate matches from duplicate candidates";
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

  std::vector<EClassId> candidates = {ec1};
  executor.execute(*compiled, candidates, ctx, results);

  // Should get exactly 1 match: {?x=a} from F(a), F2(a) doesn't match F pattern
  EXPECT_EQ(results.size(), 1) << "Should match only F(a), not F2(a)";

  if (!results.empty()) {
    auto bound_x = ctx.arena().get(results[0], 0);
    EXPECT_EQ(egraph.find(bound_x), egraph.find(a)) << "?x should be bound to 'a'";
  }
}

}  // namespace memgraph::planner::core
