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

// Correctness tests for VM executor modes (Verify vs Clean) and EMatcher vs VM comparison.
// Performance benchmarks have been moved to src/planner/bench/bench_vm.cpp.

#include <random>

#include <gtest/gtest.h>

#include "planner/pattern/vm/compiler.hpp"
#include "planner/pattern/vm/executor.hpp"
#include "planner/pattern/vm/parent_index.hpp"
#include "test_egraph_fixture.hpp"
#include "test_patterns.hpp"

namespace memgraph::planner::core {

using namespace test;
using namespace vm;

// ============================================================================
// VM Mode Comparison Fixture
// ============================================================================

class VMModesComparison : public EGraphTestBase {
 protected:
  EMatchContext ctx;
  std::vector<PatternMatch> verify_results;
  std::vector<PatternMatch> clean_results;

  /// Run both executors and verify they produce the same number of matches
  void verify_modes_match(CompiledPattern<Op> const &pattern, std::span<EClassId const> candidates) {
    // Build parent index for clean mode
    ParentSymbolIndex<Op, NoAnalysis> parent_index(egraph);
    parent_index.rebuild();

    VMExecutorVerify<Op, NoAnalysis> verify_executor(egraph);
    VMExecutorClean<Op, NoAnalysis> clean_executor(egraph, parent_index);

    verify_results.clear();
    verify_executor.execute(pattern, candidates, ctx, verify_results);

    clean_results.clear();
    clean_executor.execute(pattern, candidates, ctx, clean_results);

    // Both modes should produce the same number of matches
    EXPECT_EQ(verify_results.size(), clean_results.size());
  }

  auto get_all_candidates() -> std::vector<EClassId> {
    std::vector<EClassId> candidates;
    for (auto id : egraph.canonical_class_ids()) {
      candidates.push_back(id);
    }
    return candidates;
  }
};

// ============================================================================
// Test: Verify vs Clean mode produce same results
// ============================================================================

TEST_F(VMModesComparison, ParentTraversalSelectivity) {
  // Create an e-graph with many nodes sharing common children.
  // Both modes should find the same matches.
  constexpr std::size_t kNumLeaves = 100;
  constexpr std::size_t kParentsPerLeaf = 20;

  std::vector<EClassId> leaves;
  leaves.reserve(kNumLeaves);

  for (std::size_t i = 0; i < kNumLeaves; ++i) {
    leaves.push_back(leaf(Op::Const, static_cast<uint64_t>(i)));
  }

  std::mt19937 rng(42);
  std::uniform_int_distribution<int> sym_dist(0, 3);
  std::array<Op, 4> symbols = {Op::Add, Op::Mul, Op::Neg, Op::F};

  for (auto leaf_id : leaves) {
    for (std::size_t p = 0; p < kParentsPerLeaf; ++p) {
      auto sym = symbols[sym_dist(rng)];
      if (sym == Op::Neg) {
        node(Op::Neg, leaf_id);
      } else {
        auto other_leaf = leaves[rng() % leaves.size()];
        node(sym, leaf_id, other_leaf);
      }
    }
  }

  rebuild_egraph();

  PatternCompiler<Op> compiler;
  auto pattern = compiler.compile(TestPattern::build(Op::Neg, {Var{kVarX}}, kTestRoot));
  auto candidates = get_all_candidates();

  verify_modes_match(*pattern, candidates);
}

TEST_F(VMModesComparison, DeepPatternMatching) {
  // Chains of Neg nodes - both modes should match identically.
  constexpr std::size_t kNumChains = 100;
  constexpr std::size_t kChainDepth = 10;

  for (std::size_t i = 0; i < kNumChains; ++i) {
    auto current = leaf(Op::Const, static_cast<uint64_t>(i));
    for (std::size_t d = 0; d < kChainDepth; ++d) {
      current = node(Op::Neg, current);
    }
  }

  rebuild_egraph();

  PatternCompiler<Op> compiler;
  auto pattern = compiler.compile(TestPattern::build(Op::Neg, {Sym(Op::Neg, Sym(Op::Neg, Var{kVarX}))}, kTestRoot));
  auto candidates = get_all_candidates();

  verify_modes_match(*pattern, candidates);
}

TEST_F(VMModesComparison, SelfReferentialEClass) {
  // Self-referential e-class via merge.
  // Setup: n0 = Const(64), n1 = F(n0), n2 = F(n1), merge(n1, n2)
  // Results in EC1 = {F(n0), F(EC1)}

  auto n0 = leaf(Op::B, 64);
  auto n1 = node(Op::F, n0);
  auto n2 = node(Op::F, n1);

  merge(n1, n2);
  rebuild_egraph();

  PatternCompiler<Op> compiler;
  auto pattern = compiler.compile(TestPattern::build(Op::F, {Sym(Op::F, Var{kVarX})}, kTestRoot));
  auto candidates = get_all_candidates();

  verify_modes_match(*pattern, candidates);

  // Should find at least 2 matches due to self-reference
  EXPECT_GE(verify_results.size(), 2);
}

TEST_F(VMModesComparison, HighParentCount) {
  // Hub node with many parents of different types.
  constexpr std::size_t kFParents = 1000;
  constexpr std::size_t kNegParents = 100;

  auto hub = leaf(Op::Const, 0);

  for (std::size_t i = 0; i < kFParents; ++i) {
    auto other = leaf(Op::Const, i + 1);
    node(Op::F, hub, other);
  }

  for (std::size_t i = 0; i < kNegParents; ++i) {
    node(Op::Neg, hub);
  }

  rebuild_egraph();

  PatternCompiler<Op> compiler;
  auto pattern = compiler.compile(TestPattern::build(Op::Neg, {Var{kVarX}}, kTestRoot));
  auto candidates = get_all_candidates();

  verify_modes_match(*pattern, candidates);
}

// ============================================================================
// EMatcher vs VM Comparison Fixture
// ============================================================================

class EMatcherVsVMComparison : public EGraphTestBase {
 protected:
  EMatchContext ctx;
  std::vector<PatternMatch> ematcher_results;
  std::vector<PatternMatch> vm_results;

  /// Compare EMatcher and VM executor on the same pattern
  void verify_ematcher_vs_vm(TestPattern const &pattern) {
    EMatcher<Op, NoAnalysis> ematcher(egraph);

    PatternCompiler<Op> compiler;
    auto compiled = compiler.compile(pattern);
    ASSERT_TRUE(compiled.has_value()) << "Pattern should compile successfully";

    // Get candidates for VM based on entry symbol
    std::vector<EClassId> candidates;
    if (auto entry_sym = compiled->entry_symbol()) {
      ematcher.candidates_for_symbol(*entry_sym, candidates);
    } else {
      ematcher.all_candidates(candidates);
    }

    VMExecutorVerify<Op, NoAnalysis> vm_executor(egraph);

    ematcher_results.clear();
    ematcher.match_into(pattern, ctx, ematcher_results);

    vm_results.clear();
    vm_executor.execute(*compiled, candidates, ctx, vm_results);

    // Both should find the same number of matches
    EXPECT_EQ(ematcher_results.size(), vm_results.size());
  }
};

// ============================================================================
// Tests: EMatcher vs VM produce same results
// ============================================================================

TEST_F(EMatcherVsVMComparison, SimplePattern_Neg) {
  constexpr std::size_t kNumNodes = 1000;

  for (std::size_t i = 0; i < kNumNodes; ++i) {
    auto x = leaf(Op::Const, i);
    node(Op::Neg, x);
  }
  rebuild_egraph();

  auto pattern = TestPattern::build(Op::Neg, {Var{kVarX}}, kTestRoot);
  verify_ematcher_vs_vm(pattern);

  EXPECT_EQ(ematcher_results.size(), kNumNodes);
}

TEST_F(EMatcherVsVMComparison, NestedPattern_NegNeg) {
  constexpr std::size_t kNumChains = 500;

  for (std::size_t i = 0; i < kNumChains; ++i) {
    auto x = leaf(Op::Const, i);
    auto neg1 = node(Op::Neg, x);
    node(Op::Neg, neg1);
  }
  rebuild_egraph();

  auto pattern = TestPattern::build(Op::Neg, {Sym(Op::Neg, Var{kVarX})}, kTestRoot);
  verify_ematcher_vs_vm(pattern);

  EXPECT_EQ(ematcher_results.size(), kNumChains);
}

TEST_F(EMatcherVsVMComparison, BinaryPattern_Add) {
  constexpr std::size_t kNumNodes = 500;

  std::vector<EClassId> leaves;
  for (std::size_t i = 0; i < 50; ++i) {
    leaves.push_back(leaf(Op::Const, i));
  }

  std::mt19937 rng(42);
  for (std::size_t i = 0; i < kNumNodes; ++i) {
    auto a = leaves[rng() % leaves.size()];
    auto b = leaves[rng() % leaves.size()];
    node(Op::Add, a, b);
  }
  rebuild_egraph();

  auto pattern = TestPattern::build(Op::Add, {Var{kVarX}, Var{kVarY}}, kTestRoot);
  verify_ematcher_vs_vm(pattern);
}

TEST_F(EMatcherVsVMComparison, DeepPattern_Chain4) {
  constexpr std::size_t kNumChains = 200;

  for (std::size_t i = 0; i < kNumChains; ++i) {
    auto x = leaf(Op::Const, i);
    auto n1 = node(Op::Neg, x);
    auto n2 = node(Op::Neg, n1);
    auto n3 = node(Op::Neg, n2);
    node(Op::Neg, n3);
  }
  rebuild_egraph();

  auto pattern = TestPattern::build(Op::Neg, {Sym(Op::Neg, Sym(Op::Neg, Sym(Op::Neg, Var{kVarX})))}, kTestRoot);
  verify_ematcher_vs_vm(pattern);

  EXPECT_EQ(ematcher_results.size(), kNumChains);
}

TEST_F(EMatcherVsVMComparison, SameVariablePattern_AddXX) {
  constexpr std::size_t kNumLeaves = 100;

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
  for (std::size_t i = 0; i < 200; ++i) {
    auto a = leaves[rng() % leaves.size()];
    auto b = leaves[rng() % leaves.size()];
    if (a != b) {
      node(Op::Add, a, b);
    }
  }
  rebuild_egraph();

  auto pattern = TestPattern::build(Op::Add, {Var{kVarX}, Var{kVarX}}, kTestRoot);
  verify_ematcher_vs_vm(pattern);

  EXPECT_EQ(ematcher_results.size(), kNumLeaves);
}

TEST_F(EMatcherVsVMComparison, WideEClass_ManyENodes) {
  // Multiple e-nodes per e-class after merges
  constexpr std::size_t kNumMerges = 100;

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
  verify_ematcher_vs_vm(pattern);

  EXPECT_EQ(ematcher_results.size(), kNumMerges);
}

TEST_F(EMatcherVsVMComparison, MixedPattern_Complex) {
  // Pattern: F(Add(?x, ?y), Neg(?z))
  constexpr std::size_t kNumNodes = 200;

  std::vector<EClassId> leaves;
  for (std::size_t i = 0; i < 30; ++i) {
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
  verify_ematcher_vs_vm(pattern);
}

}  // namespace memgraph::planner::core
