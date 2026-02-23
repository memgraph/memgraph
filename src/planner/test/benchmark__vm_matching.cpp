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

#include <chrono>
#include <iostream>
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
// Benchmark Fixture
// ============================================================================

class VMMatchingBenchmark : public EGraphTestBase {
 protected:
  using Clock = std::chrono::high_resolution_clock;
  using Duration = std::chrono::duration<double, std::milli>;

  EMatchContext ctx;
  std::vector<PatternMatch> results;

  struct BenchmarkResult {
    Duration verify_time;
    Duration clean_time;
    Duration index_build_time;
    std::size_t verify_matches;
    std::size_t clean_matches;
    VMStats verify_stats;
    VMStats clean_stats;
  };

  /// Run both executors and compare results
  auto run_benchmark(CompiledPattern<Op> const &pattern, std::span<EClassId const> candidates, std::size_t iterations)
      -> BenchmarkResult {
    BenchmarkResult result{};

    // Build parent index
    ParentSymbolIndex<Op, NoAnalysis> parent_index(egraph);
    auto index_start = Clock::now();
    parent_index.rebuild();
    result.index_build_time = Clock::now() - index_start;

    // Warm up
    VMExecutorVerify<Op, NoAnalysis> verify_executor(egraph);
    VMExecutorClean<Op, NoAnalysis> clean_executor(egraph, parent_index);

    results.clear();
    verify_executor.execute(pattern, candidates, ctx, results);
    results.clear();
    clean_executor.execute(pattern, candidates, ctx, results);

    // Benchmark verify mode
    auto verify_start = Clock::now();
    for (std::size_t i = 0; i < iterations; ++i) {
      results.clear();
      verify_executor.execute(pattern, candidates, ctx, results);
    }
    result.verify_time = Clock::now() - verify_start;
    result.verify_matches = results.size();
    result.verify_stats = verify_executor.stats();

    // Benchmark clean mode
    auto clean_start = Clock::now();
    for (std::size_t i = 0; i < iterations; ++i) {
      results.clear();
      clean_executor.execute(pattern, candidates, ctx, results);
    }
    result.clean_time = Clock::now() - clean_start;
    result.clean_matches = results.size();
    result.clean_stats = clean_executor.stats();

    return result;
  }

  void print_result(std::string const &name, BenchmarkResult const &r) {
    std::cout << "\n=== " << name << " ===\n";
    std::cout << "Index build: " << r.index_build_time.count() << " ms\n";
    std::cout << "Verify mode: " << r.verify_time.count() << " ms (" << r.verify_matches << " matches)\n";
    std::cout << "Clean mode:  " << r.clean_time.count() << " ms (" << r.clean_matches << " matches)\n";
    std::cout << "Speedup:     " << (r.verify_time / r.clean_time) << "x\n";
    std::cout << "\nVerify stats:\n";
    std::cout << "  Instructions: " << r.verify_stats.instructions_executed << "\n";
    std::cout << "  Parent iterations: " << r.verify_stats.iter_parent_calls << "\n";
    std::cout << "  Symbol misses: " << r.verify_stats.parent_symbol_misses << "\n";
    std::cout << "  CheckSlot misses: " << r.verify_stats.check_slot_misses << "\n";
    std::cout << "\nClean stats:\n";
    std::cout << "  Instructions: " << r.clean_stats.instructions_executed << "\n";
    std::cout << "  Parent iterations: " << r.clean_stats.iter_parent_calls << "\n";
    std::cout << "  Symbol misses: " << r.clean_stats.parent_symbol_misses << "\n";
    std::cout << "  CheckSlot misses: " << r.clean_stats.check_slot_misses << "\n";
  }
};

// ============================================================================
// Benchmark: Parent Traversal Selectivity
// ============================================================================

TEST_F(VMMatchingBenchmark, ParentTraversalSelectivity) {
  // Create an e-graph with many nodes sharing common children.
  // This tests how well the symbol index helps filter parents.
  //
  // Structure:
  //   - 100 leaf nodes (Const)
  //   - Each leaf has N parents of various symbols (Add, Mul, Neg, F)
  //   - Pattern: Neg(?x) - should only match Neg parents
  //
  // Expected: Clean mode with symbol index should be much faster
  // because it skips non-Neg parents entirely.

  constexpr std::size_t kNumLeaves = 100;
  constexpr std::size_t kParentsPerLeaf = 20;
  constexpr std::size_t kIterations = 100;

  std::vector<EClassId> leaves;
  leaves.reserve(kNumLeaves);

  // Create leaves
  for (std::size_t i = 0; i < kNumLeaves; ++i) {
    leaves.push_back(leaf(Op::Const, static_cast<uint64_t>(i)));
  }

  // Create diverse parents for each leaf
  std::mt19937 rng(42);
  std::uniform_int_distribution<int> sym_dist(0, 3);
  std::array<Op, 4> symbols = {Op::Add, Op::Mul, Op::Neg, Op::F};

  for (auto leaf_id : leaves) {
    for (std::size_t p = 0; p < kParentsPerLeaf; ++p) {
      auto sym = symbols[sym_dist(rng)];
      if (sym == Op::Neg) {
        node(Op::Neg, leaf_id);
      } else {
        // Binary ops need two children
        auto other_leaf = leaves[rng() % leaves.size()];
        node(sym, leaf_id, other_leaf);
      }
    }
  }

  rebuild_egraph();

  // Pattern: Neg(?x)
  PatternCompiler<Op> compiler;
  auto pattern = compiler.compile(TestPattern::build(Op::Neg, {Var{kVarX}}, kTestRoot));

  // Get all candidates (all canonical e-classes)
  std::vector<EClassId> candidates;
  for (auto id : egraph.canonical_class_ids()) {
    candidates.push_back(id);
  }

  auto result = run_benchmark(*pattern, candidates, kIterations);
  print_result("Parent Traversal Selectivity", result);

  // Verify both modes found the same matches
  EXPECT_EQ(result.verify_matches, result.clean_matches);

  // Note: The current pattern compiler generates code that iterates through e-nodes
  // in each candidate e-class (going down), not by traversing parents (going up).
  // Therefore, both modes will have similar symbol miss counts since they both
  // iterate through e-nodes and check symbols the same way.
  // The parent symbol index would be useful for patterns that traverse upward
  // (e.g., "find all Neg parents of a given e-class"), which would require
  // additional compiler support for parent traversal instructions.
}

// ============================================================================
// Benchmark: Deep Pattern Matching
// ============================================================================

TEST_F(VMMatchingBenchmark, DeepPatternMatching) {
  // Create chains of Neg nodes and match with a deep pattern.
  // This tests the basic VM execution efficiency.
  //
  // Structure:
  //   - Create 100 chains of Neg(Neg(Neg(...Const)))
  //   - Each chain has depth 10
  //   - Pattern: Neg(Neg(Neg(?x)))
  //
  // Expected: Both modes should perform similarly since this
  // doesn't heavily use parent traversal.

  constexpr std::size_t kNumChains = 100;
  constexpr std::size_t kChainDepth = 10;
  constexpr std::size_t kIterations = 100;

  std::vector<EClassId> chain_roots;
  chain_roots.reserve(kNumChains);

  for (std::size_t i = 0; i < kNumChains; ++i) {
    auto current = leaf(Op::Const, static_cast<uint64_t>(i));
    for (std::size_t d = 0; d < kChainDepth; ++d) {
      current = node(Op::Neg, current);
    }
    chain_roots.push_back(current);
  }

  rebuild_egraph();

  // Pattern: Neg(Neg(Neg(?x)))
  PatternCompiler<Op> compiler;
  auto pattern = compiler.compile(TestPattern::build(Op::Neg, {Sym(Op::Neg, Sym(Op::Neg, Var{kVarX}))}, kTestRoot));

  // Get all candidates
  std::vector<EClassId> candidates;
  for (auto id : egraph.canonical_class_ids()) {
    candidates.push_back(id);
  }

  auto result = run_benchmark(*pattern, candidates, kIterations);
  print_result("Deep Pattern Matching", result);

  EXPECT_EQ(result.verify_matches, result.clean_matches);
}

// ============================================================================
// Benchmark: Self-Referential E-Class
// ============================================================================

TEST_F(VMMatchingBenchmark, SelfReferentialEClass) {
  // Create a self-referential e-class (via merge) and match.
  // This is the bug scenario from the failing test.
  //
  // Setup:
  //   n0 = B(64)
  //   n1 = F(n0)
  //   n2 = F(n1)
  //   merge(n1, n2) => EC1 = {F(n0), F(EC1)}
  //
  // Pattern: F(F(?x))
  //
  // Expected: Should find multiple matches due to self-reference.

  constexpr std::size_t kIterations = 1000;

  auto n0 = leaf(Op::B, 64);
  auto n1 = node(Op::F, n0);
  auto n2 = node(Op::F, n1);

  merge(n1, n2);
  rebuild_egraph();

  // Pattern: F(F(?x))
  PatternCompiler<Op> compiler;
  auto pattern = compiler.compile(TestPattern::build(Op::F, {Sym(Op::F, Var{kVarX})}, kTestRoot));

  std::vector<EClassId> candidates;
  for (auto id : egraph.canonical_class_ids()) {
    candidates.push_back(id);
  }

  auto result = run_benchmark(*pattern, candidates, kIterations);
  print_result("Self-Referential E-Class", result);

  EXPECT_EQ(result.verify_matches, result.clean_matches);
  // Should find at least 2 matches (the self-referential case)
  EXPECT_GE(result.verify_matches, 2);
}

// ============================================================================
// Benchmark: High Parent Count
// ============================================================================

TEST_F(VMMatchingBenchmark, HighParentCount) {
  // Create a "hub" node that is referenced by many parents.
  // This tests the worst case for parent traversal.
  //
  // Structure:
  //   - 1 "hub" leaf
  //   - 1000 F(hub) nodes, 100 Neg(hub) nodes
  //   - Pattern: Neg(?x)
  //
  // Expected: Clean mode should be much faster because it
  // only iterates the 100 Neg parents, not all 1100.

  constexpr std::size_t kFParents = 1000;
  constexpr std::size_t kNegParents = 100;
  constexpr std::size_t kIterations = 100;

  auto hub = leaf(Op::Const, 0);

  // Create many F parents
  for (std::size_t i = 0; i < kFParents; ++i) {
    auto other = leaf(Op::Const, i + 1);
    node(Op::F, hub, other);
  }

  // Create Neg parents (these are what we're matching)
  for (std::size_t i = 0; i < kNegParents; ++i) {
    node(Op::Neg, hub);
  }

  rebuild_egraph();

  // Pattern: Neg(?x)
  PatternCompiler<Op> compiler;
  auto pattern = compiler.compile(TestPattern::build(Op::Neg, {Var{kVarX}}, kTestRoot));

  std::vector<EClassId> candidates;
  for (auto id : egraph.canonical_class_ids()) {
    candidates.push_back(id);
  }

  auto result = run_benchmark(*pattern, candidates, kIterations);
  print_result("High Parent Count", result);

  EXPECT_EQ(result.verify_matches, result.clean_matches);

  // Clean mode should have significantly fewer instructions
  // because it doesn't iterate through F parents
  std::cout << "\nInstruction ratio (verify/clean): "
            << (static_cast<double>(result.verify_stats.instructions_executed) /
                static_cast<double>(result.clean_stats.instructions_executed))
            << "\n";
}

}  // namespace memgraph::planner::core
