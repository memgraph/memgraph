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

// Ground-truth based correctness tests for pattern matchers.
//
// Both EMatcher (recursive) and VM executor (bytecode) are tested against
// known expected results. XFAIL markers document known implementation bugs.
//
// When a known bug is fixed, the XFAIL will start passing - update the test!

#include <random>

#include <gtest/gtest.h>

#include "planner/pattern/vm/compiler.hpp"
#include "planner/pattern/vm/executor.hpp"
#include "test_egraph_fixture.hpp"
#include "test_patterns.hpp"

namespace memgraph::planner::core {

using namespace test;
using namespace vm;

// ============================================================================
// Test Result Helpers
// ============================================================================

/// Result of running a matcher implementation
struct MatcherResult {
  std::size_t count{0};
  bool succeeded{false};
  std::string error;
};

/// Expected behavior for an implementation
enum class Expect {
  Pass,  // Implementation should produce correct result
  XFail  // Implementation has a known bug (expected to fail)
};

/// Configuration for verify_both()
struct VerifyConfig {
  Expect ematcher = Expect::Pass;
  Expect vm = Expect::Pass;
};

// ============================================================================
// Ground Truth Comparison Fixture
// ============================================================================

class MatcherCorrectnessTest : public EGraphTestBase {
 protected:
  EMatchContext ctx;
  std::vector<PatternMatch> results;

  /// Run EMatcher and return result
  auto run_ematcher(TestPattern const &pattern) -> MatcherResult {
    EMatcher<Op, NoAnalysis> ematcher(egraph);
    results.clear();
    ematcher.match_into(pattern, ctx, results);
    return {.count = results.size(), .succeeded = true};
  }

  /// Run VM executor and return result
  auto run_vm(TestPattern const &pattern) -> MatcherResult {
    PatternCompiler<Op> compiler;
    auto compiled = compiler.compile(pattern);
    if (!compiled.has_value()) {
      return {.count = 0, .succeeded = false, .error = "Compilation failed"};
    }

    VMExecutor<Op, NoAnalysis> vm_executor(egraph);
    results.clear();
    vm_executor.execute(*compiled, matcher, ctx, results);
    return {.count = results.size(), .succeeded = true};
  }

  /// Verify implementation result against expected count
  void verify_impl(std::string_view name, MatcherResult const &result, std::size_t expected, Expect expectation) {
    ASSERT_TRUE(result.succeeded) << name << " failed: " << result.error;

    if (expectation == Expect::XFail) {
      // Known bug: we expect the implementation to produce wrong results
      if (result.count == expected) {
        // Bug appears to be fixed! Update the test.
        ADD_FAILURE() << name << " XFAIL now passes! Expected " << expected << ", got " << result.count
                      << ". Update test to Expect::Pass.";
      } else {
        // Document the known discrepancy
        GTEST_LOG_(INFO) << name << " XFAIL: expected " << expected << ", got " << result.count << " (known bug)";
      }
    } else {
      EXPECT_EQ(result.count, expected) << name << " mismatch";
    }
  }

  /// Test both implementations against ground truth
  void verify_both(TestPattern const &pattern, std::size_t expected, VerifyConfig config = {}) {
    auto ematcher_result = run_ematcher(pattern);
    auto vm_result = run_vm(pattern);

    verify_impl("EMatcher", ematcher_result, expected, config.ematcher);
    verify_impl("VM", vm_result, expected, config.vm);
  }
};

// ============================================================================
// Basic Pattern Tests (Both implementations should pass)
// ============================================================================

TEST_F(MatcherCorrectnessTest, SimplePattern_Neg) {
  // Ground truth: N Neg nodes -> N matches
  constexpr std::size_t kNumNodes = 100;

  for (std::size_t i = 0; i < kNumNodes; ++i) {
    auto x = leaf(Op::Const, i);
    node(Op::Neg, x);
  }
  rebuild_egraph();

  auto pattern = TestPattern::build(Op::Neg, {Var{kVarX}}, kTestRoot);
  verify_both(pattern, kNumNodes);
}

TEST_F(MatcherCorrectnessTest, NestedPattern_NegNeg) {
  // Ground truth: N chains of Neg(Neg(x)) -> N matches
  constexpr std::size_t kNumChains = 50;

  for (std::size_t i = 0; i < kNumChains; ++i) {
    auto x = leaf(Op::Const, i);
    auto neg1 = node(Op::Neg, x);
    node(Op::Neg, neg1);
  }
  rebuild_egraph();

  auto pattern = TestPattern::build(Op::Neg, {Sym(Op::Neg, Var{kVarX})}, kTestRoot);
  verify_both(pattern, kNumChains);
}

TEST_F(MatcherCorrectnessTest, DeepPattern_Chain4) {
  // Ground truth: N 4-deep chains -> N matches
  constexpr std::size_t kNumChains = 20;

  for (std::size_t i = 0; i < kNumChains; ++i) {
    auto x = leaf(Op::Const, i);
    auto n1 = node(Op::Neg, x);
    auto n2 = node(Op::Neg, n1);
    auto n3 = node(Op::Neg, n2);
    node(Op::Neg, n3);
  }
  rebuild_egraph();

  auto pattern = TestPattern::build(Op::Neg, {Sym(Op::Neg, Sym(Op::Neg, Sym(Op::Neg, Var{kVarX})))}, kTestRoot);
  verify_both(pattern, kNumChains);
}

TEST_F(MatcherCorrectnessTest, SameVariablePattern_AddXX) {
  // Ground truth: Add(x, x) matches only when both children are same e-class
  constexpr std::size_t kNumLeaves = 20;

  std::vector<EClassId> leaves;
  for (std::size_t i = 0; i < kNumLeaves; ++i) {
    leaves.push_back(leaf(Op::Const, i));
  }

  // Create Add(x, x) for each leaf (these should match)
  for (auto l : leaves) {
    node(Op::Add, l, l);
  }

  // Create Add(x, y) for different leaves (these should NOT match)
  std::mt19937 rng(42);
  for (std::size_t i = 0; i < 50; ++i) {
    auto a = leaves[rng() % leaves.size()];
    auto b = leaves[rng() % leaves.size()];
    if (a != b) {
      node(Op::Add, a, b);
    }
  }
  rebuild_egraph();

  auto pattern = TestPattern::build(Op::Add, {Var{kVarX}, Var{kVarX}}, kTestRoot);
  verify_both(pattern, kNumLeaves);
}

TEST_F(MatcherCorrectnessTest, WideEClass_ManyENodes) {
  // Ground truth: Merging N Neg nodes into one e-class -> N matches
  // Each e-node in the e-class produces a distinct binding for ?x
  constexpr std::size_t kNumMerges = 20;

  std::vector<EClassId> neg_nodes;
  for (std::size_t i = 0; i < kNumMerges; ++i) {
    auto x = leaf(Op::Const, i);
    neg_nodes.push_back(node(Op::Neg, x));
  }

  // Merge them all into one e-class
  for (std::size_t i = 1; i < neg_nodes.size(); ++i) {
    merge(neg_nodes[0], neg_nodes[i]);
  }
  rebuild_egraph();

  auto pattern = TestPattern::build(Op::Neg, {Var{kVarX}}, kTestRoot);
  verify_both(pattern, kNumMerges);
}

// ============================================================================
// Self-Referential E-Class Tests
// ============================================================================

TEST_F(MatcherCorrectnessTest, SelfReferentialEClass_DeepPattern) {
  // Setup:
  //   n0 = B(64)
  //   n1 = F(n0)
  //   n2 = F(n1)
  //   union(n1, n2)
  //
  // After merge and rebuild:
  //   EC0 = { B(64) }
  //   EC1 = { F(EC0), F(EC1) }   <- self-referential
  //
  // Pattern: F(F(F(?v0)))
  //
  // Expected: 2 matches
  //   Match 1: ?v0 = EC0 (via F(EC0) at each level)
  //   Match 2: ?v0 = EC1 (via F(EC1) self-loop)
  //
  // Historical EMatcher bug (now fixed): Only found match 1. The innermost
  // F(?v0) frame at EC1 tried F(EC0), yielded, got popped, never tried F(EC1).

  auto n0 = leaf(Op::B, 64);
  auto n1 = node(Op::F, n0);
  auto n2 = node(Op::F, n1);

  merge(n1, n2);
  rebuild_egraph();

  auto pattern = TestPattern::build(Op::F, {Sym(Op::F, Sym(Op::F, Var{kVarX}))}, kTestRoot);

  // EMatcher bug was fixed - both implementations now correct
  verify_both(pattern, /*expected=*/2);
}

// ============================================================================
// Leaf Symbol in Pattern Tests
// ============================================================================

TEST_F(MatcherCorrectnessTest, TernaryPatternWithLeafSymbol) {
  // Setup:
  //   v0 = X(0), v1 = Y(0)
  //   a0 = A(0), a1 = A(1)
  //   t = F(v0, a0, v1)
  //   merge(a0, a1)
  //
  // After rebuild: t = F(v0, merged_a, v1) where merged_a = { A(0), A(1) }
  //
  // Pattern: F(?x, A, ?y) - A is a leaf symbol, not a variable
  //
  // Ground truth: 1 match (one F node, regardless of e-nodes in child e-class)
  //
  // Historical EMatcher bug (now fixed): Produced 2 matches because it
  // iterated e-nodes for leaf symbol children instead of matching per e-class.

  auto v0 = leaf(Op::X, 0);
  auto v1 = leaf(Op::Y, 0);
  auto a0 = leaf(Op::A, 0);
  auto a1 = leaf(Op::A, 1);
  node(Op::F, v0, a0, v1);

  merge(a0, a1);
  rebuild_egraph();

  auto pattern = TestPattern::build(Op::F, {Var{kVarX}, Sym(Op::A), Var{kVarY}}, kTestRoot);

  // EMatcher bug was fixed - both implementations now correct
  verify_both(pattern, /*expected=*/1);
}

// ============================================================================
// Complex Pattern Tests
// ============================================================================

TEST_F(MatcherCorrectnessTest, MixedPattern_Complex) {
  // Pattern: F(Add(?x, ?y), Neg(?z))
  // Ground truth: Each F node with Add and Neg children produces 1 match
  constexpr std::size_t kNumNodes = 20;

  std::vector<EClassId> leaves;
  for (std::size_t i = 0; i < 10; ++i) {
    leaves.push_back(leaf(Op::Const, i));
  }

  std::mt19937 rng(42);
  for (std::size_t i = 0; i < kNumNodes; ++i) {
    auto x = leaves[rng() % leaves.size()];
    auto y = leaves[rng() % leaves.size()];
    auto z = leaves[rng() % leaves.size()];
    auto add_xy = node(Op::Add, x, y);
    auto neg_z = node(Op::Neg, z);
    node(Op::F, add_xy, neg_z);
  }
  rebuild_egraph();

  auto pattern = TestPattern::build(Op::F, {Sym(Op::Add, Var{kVarX}, Var{kVarY}), Sym(Op::Neg, Var{kVarZ})}, kTestRoot);

  // Note: Due to hash-consing, duplicate structures are merged, so actual
  // count may be less than kNumNodes. We verify both matchers agree.
  auto ematcher_result = run_ematcher(pattern);
  auto vm_result = run_vm(pattern);

  EXPECT_EQ(ematcher_result.count, vm_result.count) << "EMatcher and VM should agree";
  EXPECT_GT(ematcher_result.count, 0u) << "Should find at least one match";
}

TEST_F(MatcherCorrectnessTest, BinaryPattern_RandomStructure) {
  // Random binary patterns - both matchers should agree
  std::vector<EClassId> leaves;
  for (std::size_t i = 0; i < 20; ++i) {
    leaves.push_back(leaf(Op::Const, i));
  }

  std::mt19937 rng(42);
  for (std::size_t i = 0; i < 50; ++i) {
    auto a = leaves[rng() % leaves.size()];
    auto b = leaves[rng() % leaves.size()];
    node(Op::Add, a, b);
  }
  rebuild_egraph();

  auto pattern = TestPattern::build(Op::Add, {Var{kVarX}, Var{kVarY}}, kTestRoot);

  auto ematcher_result = run_ematcher(pattern);
  auto vm_result = run_vm(pattern);

  EXPECT_EQ(ematcher_result.count, vm_result.count) << "EMatcher and VM should agree";
}

}  // namespace memgraph::planner::core
