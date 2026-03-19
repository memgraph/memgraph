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

#include <algorithm>
#include <limits>
#include <set>

#include "planner/extract/extractor.hpp"

import memgraph.planner.core.egraph;

using namespace memgraph::planner::core;
using namespace memgraph::planner::core::extract;

enum struct symbol : std::uint8_t { A, B, ADD, LITERAL };

struct analysis {};

// Helper functions to make tests read like s-expressions
template <typename EGraph>
auto Lit(EGraph &egraph, uint64_t value) {
  return egraph.emplace(symbol::LITERAL, value);
}

template <typename EGraph>
auto Add(EGraph &egraph, EClassId left, EClassId right) {
  return egraph.emplace(symbol::ADD, {left, right});
}

// Simple cost models using DefaultCostResult<double>.
// All models use the unified signature: (enode, enode_id, span<CostResult>) -> CostResult.
struct UniformCostModel {
  using CostResult = DefaultCostResult<double>;

  static auto operator()(ENode<symbol> const & /*current*/, ENodeId enode_id, std::span<CostResult const> children)
      -> CostResult {
    auto child_sum = std::ranges::fold_left(
        children, 0.0, [](double acc, CostResult const &c) { return acc + CostResult::min_cost(c); });
    return CostResult{1.0 + child_sum, enode_id};
  }
};

struct SymbolCostModel {
  using CostResult = DefaultCostResult<double>;
  double a_cost;
  double b_cost;

  auto operator()(ENode<symbol> const &current, ENodeId enode_id, std::span<CostResult const> children) const
      -> CostResult {
    auto child_sum = std::ranges::fold_left(
        children, 0.0, [](double acc, CostResult const &c) { return acc + CostResult::min_cost(c); });
    return CostResult{(current.symbol() == symbol::A ? a_cost : b_cost) + child_sum, enode_id};
  }
};

TEST(Extract_Basic, BasicLeafExtraction) {
  auto egraph = EGraph<symbol, analysis>{};
  auto [root_class, root_enode, root_new] = egraph.emplace(symbol::A);
  auto extractor = Extractor{egraph, UniformCostModel{}};
  auto extracted = extractor.Extract(root_class);
  ASSERT_EQ(extracted.size(), 1);
  ASSERT_EQ(extracted[0].first, root_class);
  ASSERT_EQ(extracted[0].second, root_enode);
}

TEST(Extract_Basic, CheapestRootSelected) {
  auto egraph = EGraph<symbol, analysis>{};
  auto [aclass, anode, a_new] = egraph.emplace(symbol::A);
  auto [bclass, bnode, b_new] = egraph.emplace(symbol::B);
  auto [root, _] = egraph.merge(aclass, bclass);
  auto ctx = ProcessingContext<symbol>{};
  egraph.rebuild(ctx);
  auto extractor = Extractor{egraph, SymbolCostModel{1.0, 2.0}};
  auto extracted = extractor.Extract(root);
  ASSERT_EQ(extracted.size(), 1);
  ASSERT_EQ(extracted[0].first, root);
  ASSERT_EQ(extracted[0].second, anode);
}

// ========================================
// Extract_Cost Tests
// ========================================

// Helper: wraps a simple (ENode -> double) cost function into a cost model.
template <typename Fn>
struct SimpleCostModel {
  using CostResult = DefaultCostResult<double>;
  Fn fn;

  auto operator()(ENode<symbol> const &enode, ENodeId enode_id, std::span<CostResult const> children) const
      -> CostResult {
    auto child_sum = std::ranges::fold_left(
        children, 0.0, [](double acc, CostResult const &c) { return acc + CostResult::min_cost(c); });
    return CostResult{fn(enode) + child_sum, enode_id};
  }
};

// Helper: keep test assertions readable.
template <typename CostModel>
using FrontierMap = std::unordered_map<EClassId, EClassFrontier<typename CostModel::CostResult>>;

template <typename CostModel>
auto frontier_cost(FrontierMap<CostModel> const &m, EClassId id) {
  return CostModel::CostResult::min_cost(*m.at(id));
}

template <typename CostModel>
auto frontier_enode(FrontierMap<CostModel> const &m, EClassId id) {
  return CostModel::CostResult::resolve(*m.at(id));
}

TEST(Extract_Cost, SingleLeafNode) {
  auto cost_model = SimpleCostModel{[](ENode<symbol> const &) { return 5.0; }};
  using CostModel = decltype(cost_model);

  auto egraph = EGraph<symbol, analysis>{};
  auto [leaf_class, leaf_node, leaf_new] = egraph.emplace(symbol::A);

  FrontierMap<CostModel> frontiers;
  auto cost = ComputeFrontiers(egraph, cost_model, leaf_class, frontiers);

  ASSERT_TRUE(cost.has_value());
  ASSERT_EQ(cost->cost, 5.0);
  ASSERT_EQ(frontiers.size(), 1);
  ASSERT_EQ(frontier_enode<CostModel>(frontiers, leaf_class), leaf_node);
  ASSERT_EQ(frontier_cost<CostModel>(frontiers, leaf_class), 5.0);
}

TEST(Extract_Cost, SimpleTree) {
  auto cost_model = SimpleCostModel{[](ENode<symbol> const &) { return 1.0; }};
  using CostModel = decltype(cost_model);

  auto egraph = EGraph<symbol, analysis>{};
  auto [left_class, left_node, left_new] = egraph.emplace(symbol::A);
  auto [right_class, right_node, right_new] = egraph.emplace(symbol::B);
  auto [root_class, root_node, root_new] = egraph.emplace(symbol::A, {left_class, right_class});

  FrontierMap<CostModel> frontiers;
  auto cost = ComputeFrontiers(egraph, cost_model, root_class, frontiers);

  // Cost should be: 1 (root) + 1 (left) + 1 (right) = 3
  ASSERT_TRUE(cost.has_value());
  ASSERT_EQ(cost->cost, 3.0);
  ASSERT_EQ(frontiers.size(), 3);
}

TEST(Extract_Cost, DeepTree) {
  auto cost_model = SimpleCostModel{[](ENode<symbol> const &) { return 1.0; }};
  using CostModel = decltype(cost_model);

  auto egraph = EGraph<symbol, analysis>{};
  auto [leaf_class, leaf_node, leaf_new] = egraph.emplace(symbol::A);
  auto [mid_class, mid_node, mid_new] = egraph.emplace(symbol::B, {leaf_class});
  auto [root_class, root_node, root_new] = egraph.emplace(symbol::A, {mid_class});

  FrontierMap<CostModel> frontiers;
  auto cost = ComputeFrontiers(egraph, cost_model, root_class, frontiers);

  // Cost should be: 1 (root) + 1 (mid) + 1 (leaf) = 3
  ASSERT_TRUE(cost.has_value());
  ASSERT_EQ(cost->cost, 3.0);
  ASSERT_EQ(frontiers.size(), 3);
}

TEST(Extract_Cost, DiamondDAGSharedNode) {
  auto cost_model = SimpleCostModel{[](ENode<symbol> const &) { return 1.0; }};
  using CostModel = decltype(cost_model);

  auto egraph = EGraph<symbol, analysis>{};
  auto [shared_class, shared_node, shared_new] = egraph.emplace(symbol::A);
  auto [left_class, left_node, left_new] = egraph.emplace(symbol::B, {shared_class}, 1);     // disambiguator = 1
  auto [right_class, right_node, right_new] = egraph.emplace(symbol::B, {shared_class}, 2);  // disambiguator = 2
  auto [root_class, root_node, root_new] = egraph.emplace(symbol::A, {left_class, right_class});

  FrontierMap<CostModel> frontiers;
  auto cost = ComputeFrontiers(egraph, cost_model, root_class, frontiers);

  // Shared node should only be computed once via memoization
  // Cost: root=1 + (left=1+shared=1) + (right=1+shared=1) = 1+2+2 = 5
  ASSERT_TRUE(cost.has_value());
  ASSERT_EQ(cost->cost, 5.0);
  ASSERT_EQ(frontiers.size(), 4);
  ASSERT_EQ(frontier_cost<CostModel>(frontiers, shared_class), 1);
  ASSERT_EQ(frontier_cost<CostModel>(frontiers, left_class), 2);
  ASSERT_EQ(frontier_cost<CostModel>(frontiers, right_class), 2);
  ASSERT_EQ(frontier_cost<CostModel>(frontiers, root_class), 5);
}

TEST(Extract_Cost, VariableCostBySymbol) {
  auto cost_model = SimpleCostModel{[](ENode<symbol> const &e) { return e.symbol() == symbol::A ? 2.0 : 5.0; }};
  using CostModel = decltype(cost_model);

  auto egraph = EGraph<symbol, analysis>{};
  auto [aclass, anode, a_new] = egraph.emplace(symbol::A);
  auto [bclass, bnode, b_new] = egraph.emplace(symbol::B);

  FrontierMap<CostModel> frontiers;

  auto cost_a = ComputeFrontiers(egraph, cost_model, aclass, frontiers);
  auto cost_b = ComputeFrontiers(egraph, cost_model, bclass, frontiers);

  ASSERT_TRUE(cost_a.has_value());
  ASSERT_TRUE(cost_b.has_value());
  ASSERT_EQ(cost_a->cost, 2.0);
  ASSERT_EQ(cost_b->cost, 5.0);
}

TEST(Extract_Cost, SelectsCheapestAmongEquivalents) {
  auto cost_model = SimpleCostModel{[](ENode<symbol> const &e) { return e.symbol() == symbol::A ? 1.0 : 10.0; }};
  using CostModel = decltype(cost_model);

  auto egraph = EGraph<symbol, analysis>{};
  auto [aclass, anode, a_new] = egraph.emplace(symbol::A);
  auto [bclass, bnode, b_new] = egraph.emplace(symbol::B);
  auto [root, _] = egraph.merge(aclass, bclass);
  auto ctx = ProcessingContext<symbol>{};
  egraph.rebuild(ctx);

  FrontierMap<CostModel> frontiers;

  auto cost = ComputeFrontiers(egraph, cost_model, root, frontiers);

  ASSERT_TRUE(cost.has_value());
  ASSERT_EQ(cost->cost, 1.0);
  ASSERT_EQ(frontier_enode<CostModel>(frontiers, root), anode);
  ASSERT_EQ(frontier_cost<CostModel>(frontiers, root), 1.0);
}

TEST(Extract_Cost, CostAccumulationWithVariableCosts) {
  auto cost_model = SimpleCostModel{[](ENode<symbol> const &e) { return e.symbol() == symbol::A ? 2.0 : 3.0; }};
  using CostModel = decltype(cost_model);

  auto egraph = EGraph<symbol, analysis>{};
  auto [leaf1_class, leaf1_node, leaf1_new] = egraph.emplace(symbol::A);
  auto [leaf2_class, leaf2_node, leaf2_new] = egraph.emplace(symbol::B);
  auto [root_class, root_node, root_new] = egraph.emplace(symbol::A, {leaf1_class, leaf2_class});

  FrontierMap<CostModel> frontiers;

  auto cost = ComputeFrontiers(egraph, cost_model, root_class, frontiers);

  // Cost: 2 (root, symbol A) + 2 (leaf1, symbol A) + 3 (leaf2, symbol B) = 7
  ASSERT_TRUE(cost.has_value());
  ASSERT_EQ(cost->cost, 7.0);
}

TEST(Extract_Cost, CyclicEGraphInfiniteCost) {
  // Test case: X (LITERAL 1) merged with (ADD X (LITERAL 0))
  // This creates a cycle: the 'ADD' e-node has a child that is its own e-class
  // When evaluating 'ADD' e-node: cost = 1 + cost(X_class) + cost(LITERAL 0)
  // But X_class now contains both LITERAL 1 and the 'ADD' node
  // The 'ADD' node can't be the cheapest because it depends on itself
  // If all e-nodes in the e-class are cyclic, the cost should be infinite

  auto cost_model = SimpleCostModel{[](ENode<symbol> const &) { return 1.0; }};
  using CostModel = decltype(cost_model);

  auto egraph = EGraph<symbol, analysis>{};

  // Create X (LITERAL 1) - a leaf node
  auto [x_class, x_node, x_new] = egraph.emplace(symbol::LITERAL, 1);

  // Create (LITERAL 0) - another leaf
  auto [zero_class, zero_node, zero_new] = egraph.emplace(symbol::LITERAL, 0);

  // Create (ADD X 0) - a node with children
  auto [plus_class, plus_node, plus_new] = egraph.emplace(symbol::ADD, {x_class, zero_class});

  // Now merge X with (ADD X 0) - this creates the cycle!
  auto [cyclic_class, _] = egraph.merge(x_class, plus_class);
  auto ctx = ProcessingContext<symbol>{};
  egraph.rebuild(ctx);

  FrontierMap<CostModel> frontiers;

  // Process the cyclic e-class
  auto cost = ComputeFrontiers(egraph, cost_model, cyclic_class, frontiers);

  // The cyclic 'ADD' node should have infinite cost (or very large)
  // The non-cyclic LITERAL node should be selected with cost 1
  ASSERT_TRUE(cost.has_value());
  ASSERT_EQ(cost->cost, 1.0);
  ASSERT_EQ(frontier_enode<CostModel>(frontiers, cyclic_class), x_node);
  ASSERT_EQ(frontier_cost<CostModel>(frontiers, cyclic_class), 1.0);
}

TEST(Extract_Cost, CyclicEGraphInfiniteCostComplex) {
  // Test case: X (LITERAL 1) merged with (ADD X (LITERAL 0))
  // This creates a cycle: the 'ADD' e-node has a child that is its own e-class
  // When evaluating 'ADD' e-node: cost = 1 + cost(X_class) + cost(LITERAL 0)
  // But X_class now contains both LITERAL 1 and the 'ADD' node
  // The 'ADD' node can't be the cheapest because it depends on itself
  // If all e-nodes in the e-class are cyclic, the cost should be infinite

  auto cost_model = SimpleCostModel{[](ENode<symbol> const &) { return 1.0; }};
  using CostModel = decltype(cost_model);

  auto egraph = EGraph<symbol, analysis>{};

  // Create X (LITERAL 1) - a leaf node
  auto [x_class, x_node, x_new] = egraph.emplace(symbol::LITERAL, 1);

  auto [y_class, y_node, y_new] = egraph.emplace(symbol::A, {x_class});

  // Create (LITERAL 0) - another leaf
  auto [zero_class, zero_node, zero_new] = egraph.emplace(symbol::LITERAL, 0);

  // Create (ADD X 0) - a node with children
  auto [plus_class, plus_node, plus_new] = egraph.emplace(symbol::ADD, {y_class, zero_class});

  // Now merge X with (ADD X 0) - this creates the cycle!
  auto [merged_class, _] = egraph.merge(x_class, plus_class);
  auto ctx = ProcessingContext<symbol>{};
  egraph.rebuild(ctx);

  FrontierMap<CostModel> frontiers;

  // Process the cyclic e-class
  auto cost = ComputeFrontiers(egraph, cost_model, merged_class, frontiers);

  // The cyclic 'ADD' node should have infinite cost (or very large)
  // The non-cyclic LITERAL node should be selected with cost 1
  ASSERT_TRUE(cost.has_value());
  ASSERT_EQ(cost->cost, 1.0);
  ASSERT_EQ(frontiers.size(), 2);
  ASSERT_TRUE(frontiers.contains(merged_class));
  ASSERT_EQ(frontier_cost<CostModel>(frontiers, merged_class), 1.0);
  ASSERT_EQ(frontier_enode<CostModel>(frontiers, merged_class), x_node);
  ASSERT_TRUE(frontiers.contains(zero_class));
  ASSERT_EQ(frontier_cost<CostModel>(frontiers, zero_class), 1.0);
  ASSERT_EQ(frontier_enode<CostModel>(frontiers, zero_class), zero_node);
}

TEST(Extract_Cost, FullyCyclicEClassInfiniteCost) {
  // Test case: All e-nodes in an e-class are cyclic
  // E.g., X contains only (ADD X (LITERAL 1)) and (ADD X (LITERAL 2))
  // Both ADD nodes reference the e-class they're in, creating unavoidable
  // cycles

  auto cost_model = SimpleCostModel{[](ENode<symbol> const &) { return 1.0; }};
  using CostModel = decltype(cost_model);

  auto egraph = EGraph<symbol, analysis>{};

  // Create a placeholder e-class (will become cyclic)
  auto [x_class, x_node, x_new] = egraph.emplace(symbol::LITERAL, 1);

  // Create literals
  auto [one_class, one_node, one_new] = egraph.emplace(symbol::LITERAL, 1);
  auto [two_class, two_node, two_new] = egraph.emplace(symbol::LITERAL, 2);

  // Create (ADD X 1) and (ADD X 2)
  auto [plus1_class, plus1_node, plus1_new] = egraph.emplace(symbol::ADD, {x_class, one_class});
  auto [plus2_class, plus2_node, plus2_new] = egraph.emplace(symbol::ADD, {x_class, two_class});

  // Merge X with (ADD X 1)
  auto [cyclic1, _1] = egraph.merge(x_class, plus1_class);
  // Merge result with (ADD X 2)
  auto [fully_cyclic, _2] = egraph.merge(cyclic1, plus2_class);

  auto ctx = ProcessingContext<symbol>{};
  egraph.rebuild(ctx);

  FrontierMap<CostModel> frontiers;

  // This should either:
  // 1. Return infinity/max double if all nodes are cyclic
  // 2. Select the original X node if it still exists in the e-class
  auto cost = ComputeFrontiers(egraph, cost_model, fully_cyclic, frontiers);

  // After merges, the original x_node should still be selectable
  // It has cost 1 (no children), while the 'ADD' nodes have infinite cost
  ASSERT_TRUE(cost.has_value());
  ASSERT_EQ(cost->cost, 1.0);
  ASSERT_EQ(frontier_enode<CostModel>(frontiers, fully_cyclic), x_node);
}

// ========================================
// Extract_Dependencies Tests
// ========================================

TEST(Extract_Dependencies, SingleLeafNode) {
  auto egraph = EGraph<symbol, analysis>{};
  auto [leaf_class, leaf_node, leaf_new] = egraph.emplace(symbol::A);
  std::unordered_map<EClassId, Selection<double>> cheapest_enode;
  cheapest_enode[leaf_class] = {leaf_node, 1.0};

  auto in_degree = CollectDependencies(egraph, cheapest_enode, leaf_class);

  // Leaf has no children, so in_degree should be empty
  ASSERT_EQ(in_degree.size(), 1);
}

TEST(Extract_Dependencies, LinearChain) {
  auto egraph = EGraph<symbol, analysis>{};
  auto [leaf_class, leaf_node, leaf_new] = egraph.emplace(symbol::A);
  auto [mid_class, mid_node, mid_new] = egraph.emplace(symbol::B, {leaf_class});
  auto [root_class, root_node, root_new] = egraph.emplace(symbol::A, {mid_class});

  std::unordered_map<EClassId, Selection<double>> cheapest_enode;
  cheapest_enode[leaf_class] = {leaf_node, 1.0};
  cheapest_enode[mid_class] = {mid_node, 1.0};
  cheapest_enode[root_class] = {root_node, 1.0};

  auto in_degree = CollectDependencies(egraph, cheapest_enode, root_class);

  // mid has in_degree 1 (from root), leaf has in_degree 1 (from mid)
  ASSERT_EQ(in_degree.size(), 3);
  ASSERT_EQ(in_degree[root_class], 0);
  ASSERT_EQ(in_degree[mid_class], 1);
  ASSERT_EQ(in_degree[leaf_class], 1);
}

TEST(Extract_Dependencies, SimpleTree) {
  auto egraph = EGraph<symbol, analysis>{};
  auto [left_class, left_node, left_new] = egraph.emplace(symbol::A);
  auto [right_class, right_node, right_new] = egraph.emplace(symbol::B);
  auto [root_class, root_node, root_new] = egraph.emplace(symbol::A, {left_class, right_class});

  std::unordered_map<EClassId, Selection<double>> cheapest_enode;
  cheapest_enode[left_class] = {left_node, 1.0};
  cheapest_enode[right_class] = {right_node, 1.0};
  cheapest_enode[root_class] = {root_node, 1.0};

  auto in_degree = CollectDependencies(egraph, cheapest_enode, root_class);

  // Both left and right have in_degree 1 (from root)
  ASSERT_EQ(in_degree.size(), 3);
  ASSERT_EQ(in_degree[left_class], 1);
  ASSERT_EQ(in_degree[right_class], 1);
  ASSERT_EQ(in_degree[root_class], 0);
}

TEST(Extract_Dependencies, DiamondDAG) {
  auto egraph = EGraph<symbol, analysis>{};
  auto [shared_class, shared_node, shared_new] = egraph.emplace(symbol::A);
  auto [left_class, left_node, left_new] = egraph.emplace(symbol::B, {shared_class}, 1);     // disambiguator = 1
  auto [right_class, right_node, right_new] = egraph.emplace(symbol::B, {shared_class}, 2);  // disambiguator = 2
  auto [root_class, root_node, root_new] = egraph.emplace(symbol::A, {left_class, right_class});

  std::unordered_map<EClassId, Selection<double>> cheapest_enode;
  cheapest_enode[shared_class] = {shared_node, 1.0};
  cheapest_enode[left_class] = {left_node, 1.0};
  cheapest_enode[right_class] = {right_node, 1.0};
  cheapest_enode[root_class] = {root_node, 1.0};

  auto in_degree = CollectDependencies(egraph, cheapest_enode, root_class);

  // shared has in_degree 2 (from left and right)
  // left and right each have in_degree 1 (from root)
  ASSERT_EQ(in_degree.size(), 4);
  ASSERT_EQ(in_degree[root_class], 0);
  ASSERT_EQ(in_degree[left_class], 1);
  ASSERT_EQ(in_degree[right_class], 1);
  ASSERT_EQ(in_degree[shared_class], 2);
}

TEST(Extract_Dependencies, DeadBindChildrenSkipped) {
  // Simulates a dead Bind: enode has 3 children (input, sym, expr) but
  // only input is in the selection (sym/expr were skipped by resolution).
  // CollectDependencies should skip sym/expr. TopologicalSort should not
  // corrupt in_degree with default-inserted entries for missing children.
  auto egraph = EGraph<symbol, analysis>{};
  auto [input_class, input_node, input_new] = egraph.emplace(symbol::A);
  auto [sym_class, sym_node, sym_new] = egraph.emplace(symbol::B);
  auto [expr_class, expr_node, expr_new] = egraph.emplace(symbol::A, {}, 42);
  // Bind enode with 3 children: input, sym, expr
  auto [bind_class, bind_node, bind_new] = egraph.emplace(symbol::A, {input_class, sym_class, expr_class});

  // Selection: bind and input are resolved, sym and expr are NOT (dead Bind)
  using Sel = Selection<double>;
  std::unordered_map<EClassId, Sel> selection;
  selection[bind_class] = Sel{bind_node, 1.0};
  selection[input_class] = Sel{input_node, 1.0};
  // sym_class and expr_class intentionally absent — dead Bind

  auto in_degree = CollectDependencies(egraph, selection, bind_class);

  // Only bind and input should be in in_degree
  ASSERT_EQ(in_degree.size(), 2);
  ASSERT_EQ(in_degree[bind_class], 0);
  ASSERT_EQ(in_degree[input_class], 1);
  // sym and expr must NOT be in in_degree
  ASSERT_FALSE(in_degree.contains(sym_class));
  ASSERT_FALSE(in_degree.contains(expr_class));

  // TopologicalSort should produce exactly [bind, input] and not corrupt in_degree
  // with default-inserted entries for dead sym/expr children.
  auto in_degree_copy = in_degree;  // TopologicalSort takes by value — keep a copy
  auto topo = TopologicalSort(egraph, selection, std::move(in_degree_copy));
  ASSERT_EQ(topo.size(), 2);
  ASSERT_EQ(topo[0].first, bind_class);
  ASSERT_EQ(topo[1].first, input_class);

  // Verify: calling TopologicalSort should not have side effects on the original
  // in_degree, but more importantly, we can detect the bug by running it again
  // and checking that the result still has exactly 2 entries.
  // The real check: TopologicalSort walks enode.children() which includes dead
  // sym/expr. If it does --in_degree[child] without guarding, it default-inserts
  // entries for sym and expr with value -1. These never reach 0, so the topo
  // output is correct, but the in_degree map is corrupted.
  // We verify correctness by checking the output size matches the selection size.
  ASSERT_EQ(topo.size(), selection.size()) << "Topo sort should contain exactly the resolved eclasses";
}

// ========================================
// Extract_TopologicalSort Tests
// ========================================

TEST(Extract_TopologicalSort, SingleNode) {
  auto egraph = EGraph<symbol, analysis>{};
  auto [leaf_class, leaf_node, leaf_new] = egraph.emplace(symbol::A);
  std::unordered_map<EClassId, Selection<double>> cheapest_enode{};
  cheapest_enode[leaf_class] = {leaf_node, 1.0};
  auto in_degree = CollectDependencies(egraph, cheapest_enode, leaf_class);
  auto result = TopologicalSort(egraph, cheapest_enode, std::move(in_degree));

  ASSERT_EQ(result.size(), 1);
  ASSERT_EQ(result[0].first, leaf_class);
  ASSERT_EQ(result[0].second, leaf_node);
}

TEST(Extract_TopologicalSort, LinearChainOrdering) {
  auto egraph = EGraph<symbol, analysis>{};
  auto [leaf_class, leaf_node, leaf_new] = egraph.emplace(symbol::A);
  auto [mid_class, mid_node, mid_new] = egraph.emplace(symbol::B, {leaf_class});
  auto [root_class, root_node, root_new] = egraph.emplace(symbol::A, {mid_class});

  std::unordered_map<EClassId, Selection<double>> cheapest_enode;
  cheapest_enode[leaf_class] = {leaf_node, 1.0};
  cheapest_enode[mid_class] = {mid_node, 1.0};
  cheapest_enode[root_class] = {root_node, 1.0};
  auto in_degree = CollectDependencies(egraph, cheapest_enode, root_class);
  auto result = TopologicalSort(egraph, cheapest_enode, std::move(in_degree));

  ASSERT_EQ(result.size(), 3);
  // Order should be: root, mid, leaf
  ASSERT_EQ(result[0].first, root_class);
  ASSERT_EQ(result[1].first, mid_class);
  ASSERT_EQ(result[2].first, leaf_class);
}

TEST(Extract_TopologicalSort, SimpleTreeOrdering) {
  auto egraph = EGraph<symbol, analysis>{};
  auto [left_class, left_node, left_new] = egraph.emplace(symbol::A);
  auto [right_class, right_node, right_new] = egraph.emplace(symbol::B);
  auto [root_class, root_node, root_new] = egraph.emplace(symbol::A, {left_class, right_class});

  std::unordered_map<EClassId, Selection<double>> cheapest_enode;
  cheapest_enode[left_class] = {left_node, 1.0};
  cheapest_enode[right_class] = {right_node, 1.0};
  cheapest_enode[root_class] = {root_node, 1.0};
  auto in_degree = CollectDependencies(egraph, cheapest_enode, root_class);
  auto result = TopologicalSort(egraph, cheapest_enode, std::move(in_degree));

  ASSERT_EQ(result.size(), 3);
  // Root should come first
  ASSERT_EQ(result[0].first, root_class);
  // Left and right can come in any order, but both should be after root
}

TEST(Extract_TopologicalSort, DiamondTopology) {
  auto egraph = EGraph<symbol, analysis>{};
  auto [shared_class, shared_node, shared_new] = egraph.emplace(symbol::A);
  auto [left_class, left_node, left_new] = egraph.emplace(symbol::B, {shared_class}, 1);     // disambiguator = 1
  auto [right_class, right_node, right_new] = egraph.emplace(symbol::B, {shared_class}, 2);  // disambiguator = 2
  auto [root_class, root_node, root_new] = egraph.emplace(symbol::A, {left_class, right_class});

  std::unordered_map<EClassId, Selection<double>> cheapest_enode;
  cheapest_enode[shared_class] = {shared_node, 1.0};
  cheapest_enode[left_class] = {left_node, 1.0};
  cheapest_enode[right_class] = {right_node, 1.0};
  cheapest_enode[root_class] = {root_node, 1.0};
  auto in_degree = CollectDependencies(egraph, cheapest_enode, root_class);
  auto result = TopologicalSort(egraph, cheapest_enode, std::move(in_degree));

  ASSERT_EQ(result.size(), 4);
  // Root comes first
  ASSERT_EQ(result[0].first, root_class);
  // Shared must come after both left and right
  ASSERT_EQ(result[3].first, shared_class);
}

// ========================================
// Integration Tests (Full Extract Pipeline)
// ========================================

TEST(Extract_Basic, IntegrationComplexTree) {
  auto egraph = EGraph<symbol, analysis>{};
  auto [l1_class, l1_node, l1_new] = egraph.emplace(symbol::A);
  auto [l2_class, l2_node, l2_new] = egraph.emplace(symbol::B);
  auto [mid_class, mid_node, mid_new] = egraph.emplace(symbol::A, {l1_class, l2_class});
  auto [root_class, root_node, root_new] = egraph.emplace(symbol::B, {mid_class});

  auto extractor = Extractor{egraph, UniformCostModel{}};
  auto extracted = extractor.Extract(root_class);

  ASSERT_EQ(extracted.size(), 4);
  // Verify root is first
  ASSERT_EQ(extracted[0].first, root_class);
  ASSERT_EQ(extracted[1].first, mid_class);
}

TEST(Extract_Basic, IntegrationDiamondDAG) {
  auto egraph = EGraph<symbol, analysis>{};
  auto [shared_class, shared_node, shared_new] = egraph.emplace(symbol::A);
  auto [left_class, left_node, left_new] = egraph.emplace(symbol::B, {shared_class}, 1);     // disambiguator = 1
  auto [right_class, right_node, right_new] = egraph.emplace(symbol::B, {shared_class}, 2);  // disambiguator = 2
  auto [root_class, root_node, root_new] = egraph.emplace(symbol::A, {left_class, right_class});

  auto extractor = Extractor{egraph, UniformCostModel{}};
  auto extracted = extractor.Extract(root_class);

  ASSERT_EQ(extracted.size(), 4);
  // Verify shared node comes last
  ASSERT_EQ(extracted[0].first, root_class);
  ASSERT_EQ(extracted[3].first, shared_class);
}

TEST(Extract_Basic, IntegrationNestedEquivalence) {
  auto egraph = EGraph<symbol, analysis>{};
  // Create equivalence at leaf level
  auto [a1_class, a1_node, a1_new] = egraph.emplace(symbol::A);
  auto [b1_class, b1_node, b1_new] = egraph.emplace(symbol::B);
  auto [leaf_equiv, _1] = egraph.merge(a1_class, b1_class);

  // Create parent with equivalence
  auto [a2_class, a2_node, a2_new] = egraph.emplace(symbol::A, {leaf_equiv});
  auto [b2_class, b2_node, b2_new] = egraph.emplace(symbol::B, {leaf_equiv});
  auto [root, _2] = egraph.merge(a2_class, b2_class);
  auto ctx = ProcessingContext<symbol>{};
  egraph.rebuild(ctx);

  auto extractor = Extractor{egraph, SymbolCostModel{1.0, 5.0}};
  auto extracted = extractor.Extract(root);

  ASSERT_EQ(extracted.size(), 2);
  // Should select cheaper A nodes at both levels
  ASSERT_EQ(extracted[0].second, a2_node);
  ASSERT_EQ(extracted[1].second, a1_node);
}

// ========================================
// Extract_Safety Tests
// ========================================
// These tests verify defensive paths in ComputeFrontiers:
//   - best_alt() returns nullptr on empty frontier
//   - resolve() asserts non-null
//   - pick_compatible fallback asserts in ResolvePlanSelection
// The key scenarios are eclasses where ALL enodes are cyclic (no leaf escape),
// causing ComputeFrontiers to return nullopt and erase the sentinel from frontier_map.

TEST(Extract_Safety, ComputeFrontiers_FullyCyclicReturnsNullopt) {
  // Build an egraph where an intermediate eclass has NO non-cyclic enode.
  //
  //   merged_class = merge(LITERAL(1), ADD(y_class, zero_class))
  //   y_class      = A(merged_class)
  //
  // When computing y_class:
  //   - Its only enode A(merged_class) recurses into merged_class which is
  //     already "in progress" -> returns nullopt (cycle detected).
  //   - ALL enodes of y_class are cyclic -> sentinel erased, returns nullopt.
  //
  // Verify:
  //   1. ComputeFrontiers on merged_class succeeds (LITERAL leaf provides an escape).
  //   2. frontier_map does NOT contain y_class (fully cyclic, erased).
  //   3. frontier_map DOES contain merged_class and zero_class.

  auto egraph = EGraph<symbol, analysis>{};

  auto [x_class, x_node, x_new] = egraph.emplace(symbol::LITERAL, 1);
  auto [y_class, y_node, y_new] = egraph.emplace(symbol::A, {x_class});
  auto [zero_class, zero_node, zero_new] = egraph.emplace(symbol::LITERAL, 0);
  auto [plus_class, plus_node, plus_new] = egraph.emplace(symbol::ADD, {y_class, zero_class});

  auto [merged_class, _] = egraph.merge(x_class, plus_class);
  auto ctx = ProcessingContext<symbol>{};
  egraph.rebuild(ctx);

  FrontierMap<UniformCostModel> frontiers;
  auto cost = ComputeFrontiers(egraph, UniformCostModel{}, merged_class, frontiers);

  // merged_class has LITERAL(1) as a leaf escape - should succeed
  ASSERT_TRUE(cost.has_value());
  ASSERT_EQ(cost->cost, 1.0);  // LITERAL leaf cost

  // y_class is fully cyclic (its only enode A(merged_class) is a cycle)
  // ComputeFrontiers should have erased its sentinel - NOT present in frontier_map
  EXPECT_FALSE(frontiers.contains(y_class)) << "Fully cyclic y_class should not be in frontier_map";

  // merged_class and zero_class should be present
  EXPECT_TRUE(frontiers.contains(merged_class));
  EXPECT_TRUE(frontiers.contains(zero_class));

  // The selected enode for merged_class should be the leaf (x_node), not the cyclic ADD
  EXPECT_EQ(frontier_enode<UniformCostModel>(frontiers, merged_class), x_node);
}

TEST(Extract_Safety, ComputeFrontiers_CyclicChildCostsCached) {
  // Verify that when an enode has a cyclic child, the non-cyclic children's
  // costs are still computed and cached in frontier_map.
  //
  // Build:
  //   leaf_c  = LITERAL(42)          - non-cyclic leaf
  //   leaf_d  = LITERAL(99)          - non-cyclic leaf
  //   b_class = A(leaf_d)            - will become cyclic via merge
  //   root    = ADD(b_class, leaf_c) - has one cyclic child (b_class) and one non-cyclic (leaf_c)
  //   merge(b_class, root)           - makes b_class cyclic: {A(leaf_d), ADD(b_class, leaf_c)}
  //
  // After merge, b_class contains:
  //   A(leaf_d)              - references leaf_d (non-cyclic) -> cost = 1+1 = 2
  //   ADD(b_class, leaf_c)   - references b_class itself (cyclic!) -> skipped
  //
  // Computing from b_class:
  //   b_class "in progress"
  //   -> Try A(leaf_d): compute leaf_d -> cost=1, cached. A cost=2. Not cyclic.
  //   -> Try ADD(b_class, leaf_c): compute b_class -> in-progress (cycle!).
  //       But we continue processing leaf_c -> cost=1, cached.
  //       ADD is skipped because of cyclic child.
  //   -> b_class frontier = {A(leaf_d), cost=2}
  //
  // Key assertion: leaf_c is cached even though it was a sibling of the cyclic child.

  auto egraph = EGraph<symbol, analysis>{};

  auto [leaf_c_class, leaf_c_node, leaf_c_new] = egraph.emplace(symbol::LITERAL, 42);
  auto [leaf_d_class, leaf_d_node, leaf_d_new] = egraph.emplace(symbol::LITERAL, 99);
  auto [b_class, b_node, b_new] = egraph.emplace(symbol::A, {leaf_d_class});
  auto [root_class, root_node, root_new] = egraph.emplace(symbol::ADD, {b_class, leaf_c_class});

  // Merge b_class with root_class - b_class now has ADD(b_class, leaf_c) creating cycle
  auto [merged, _] = egraph.merge(b_class, root_class);
  auto ctx = ProcessingContext<symbol>{};
  egraph.rebuild(ctx);

  FrontierMap<UniformCostModel> frontiers;
  auto cost = ComputeFrontiers(egraph, UniformCostModel{}, merged, frontiers);

  // merged has A(leaf_d) as non-cyclic escape
  ASSERT_TRUE(cost.has_value());
  ASSERT_EQ(cost->cost, 2.0);  // 1 (A) + 1 (leaf_d)

  // Key: leaf_c should be cached even though it was encountered while processing
  // the cyclic ADD enode. The "continue processing remaining children" logic
  // ensures non-cyclic siblings are still computed.
  EXPECT_TRUE(frontiers.contains(leaf_c_class)) << "Non-cyclic sibling leaf_c should be cached in frontier_map";
  EXPECT_EQ(frontier_cost<UniformCostModel>(frontiers, leaf_c_class), 1.0);

  // leaf_d should also be cached (child of the non-cyclic A enode)
  EXPECT_TRUE(frontiers.contains(leaf_d_class));
  EXPECT_EQ(frontier_cost<UniformCostModel>(frontiers, leaf_d_class), 1.0);

  // merged should be present with the A enode selected (not the cyclic ADD)
  EXPECT_TRUE(frontiers.contains(merged));
  EXPECT_EQ(frontier_enode<UniformCostModel>(frontiers, merged), b_node);
}

// ========================================
// Multi-Alt Extraction Tests
// ========================================

namespace {

// A simple multi-alt cost model for testing.
// Each alternative has a cost, a set of required "demands" (ints), and the enode_id.
struct TestDemandAlt {
  double cost;
  std::set<int> required;
  ENodeId enode_id;

  auto dominated_by(TestDemandAlt const &other) const -> bool {
    return other.cost <= cost && std::ranges::includes(required, other.required);
  }
};

struct TestDominance {
  auto operator()(TestDemandAlt const &a, TestDemandAlt const &b) const -> bool { return a.dominated_by(b); }
};

/// TestFrontier: ParetoFrontier with resolve/min_cost for the extraction contract.
struct TestFrontier : ParetoFrontier<TestDemandAlt, TestDominance> {
  using Base = ParetoFrontier<TestDemandAlt, TestDominance>;
  using Base::Base;

  TestFrontier() = default;

  // NOLINTNEXTLINE(google-explicit-constructor)
  TestFrontier(Base base) : Base(std::move(base)) {}

  TestFrontier(std::initializer_list<TestDemandAlt> init) : Base{std::vector<TestDemandAlt>(init)} {}

  static auto resolve(TestFrontier const &f) -> ENodeId {
    auto it = std::ranges::min_element(f.alts, {}, &TestDemandAlt::cost);
    assert(it != f.alts.end());
    return it->enode_id;
  }

  static auto min_cost(TestFrontier const &f) -> double {
    auto it = std::ranges::min_element(f.alts, {}, &TestDemandAlt::cost);
    return it != f.alts.end() ? it->cost : std::numeric_limits<double>::infinity();
  }
};

// Simple multi-alt cost model: each enode produces a single alternative with cost=1,
// required={} and the enode_id. This should behave identically to single-best extraction.
// resolve/min_cost live on TestFrontier (the CostResult type).
// These models only need to provide operator().
struct SimpleMultiAltCostModel {
  using CostResult = TestFrontier;

  static auto operator()(ENode<symbol> const & /*current*/, ENodeId enode_id, std::span<CostResult const> children)
      -> CostResult {
    auto child_cost = std::ranges::fold_left(
        children, 0.0, [](double acc, CostResult const &c) { return acc + CostResult::min_cost(c); });
    return CostResult{{{.cost = 1.0 + child_cost, .required = {}, .enode_id = enode_id}}};
  }
};

struct DemandAwareMultiAltCostModel {
  using CostResult = TestFrontier;

  static auto operator()(ENode<symbol> const &current, ENodeId enode_id, std::span<CostResult const> children)
      -> CostResult {
    auto child_cost = std::ranges::fold_left(
        children, 0.0, [](double acc, CostResult const &c) { return acc + CostResult::min_cost(c); });
    if (current.symbol() == symbol::A) {
      return CostResult{{
          {.cost = 1.0 + child_cost, .required = {1}, .enode_id = enode_id},
          {.cost = 2.0 + child_cost, .required = {}, .enode_id = enode_id},
      }};
    }
    return CostResult{{{.cost = 1.0 + child_cost, .required = {}, .enode_id = enode_id}}};
  }
};

}  // namespace

// ========================================
// ParetoFrontier::prune() Direct Tests
// ========================================

TEST(ParetoFrontier_Prune, EmptyFrontier) {
  auto frontier = TestFrontier{};
  frontier.prune();
  ASSERT_TRUE(frontier.alts.empty());
}

TEST(ParetoFrontier_Prune, SingleElement) {
  auto frontier = TestFrontier{{{.cost = 5.0, .required = {1}, .enode_id = ENodeId{0}}}};
  frontier.prune();
  ASSERT_EQ(frontier.alts.size(), 1);
  ASSERT_EQ(frontier.alts[0].cost, 5.0);
}

TEST(ParetoFrontier_Prune, NoneDominated) {
  // Three Pareto-incomparable alts: lower cost always requires more demands.
  auto frontier = TestFrontier{{
      {.cost = 1.0, .required = {1, 2}, .enode_id = ENodeId{0}},  // cheapest, most demands
      {.cost = 2.0, .required = {1}, .enode_id = ENodeId{1}},     // medium
      {.cost = 3.0, .required = {}, .enode_id = ENodeId{2}},      // expensive, no demands
  }};
  frontier.prune();
  ASSERT_EQ(frontier.alts.size(), 3) << "All three Pareto-incomparable alts should survive";
}

TEST(ParetoFrontier_Prune, AllDominatedByOne) {
  // Alt B (cost=1, req={}) dominates both A (cost=2, req={1}) and C (cost=3, req={2}).
  // dominated_by checks: other.cost <= cost && required ⊇ other.required
  // A.dominated_by(B): B.cost=1 <= 2 && {1} ⊇ {} → true
  // C.dominated_by(B): B.cost=1 <= 3 && {2} ⊇ {} → true
  auto frontier = TestFrontier{{
      {.cost = 2.0, .required = {1}, .enode_id = ENodeId{0}},  // A: dominated by B
      {.cost = 1.0, .required = {}, .enode_id = ENodeId{1}},   // B: dominator
      {.cost = 3.0, .required = {2}, .enode_id = ENodeId{2}},  // C: dominated by B
  }};
  frontier.prune();
  ASSERT_EQ(frontier.alts.size(), 1);
  ASSERT_EQ(frontier.alts[0].enode_id, ENodeId{1}) << "Only the dominator (B) should survive";
}

TEST(ParetoFrontier_Prune, DuplicateAlternatives) {
  // Two alts with identical cost and required but different enode_id.
  // Neither dominates the other because dominated_by uses strict <=
  // for cost and subset-or-equal for required — both conditions hold symmetrically.
  // So a.dominated_by(b) is true AND b.dominated_by(a) is true.
  // In the prune algorithm, whichever is tested first as "i" will dominate the other
  // via the j-loop. The first alt (lower index) marks the second as dominated.
  // Result: only the first alt (by input order) survives.
  auto frontier = TestFrontier{{
      {.cost = 2.0, .required = {1}, .enode_id = ENodeId{0}},
      {.cost = 2.0, .required = {1}, .enode_id = ENodeId{1}},
  }};
  frontier.prune();
  // When both dominate each other, the algorithm processes i=0 first:
  //   j=1: DominanceFn(alts[0], alts[1]) → true (0 dominated by 1) → mark 0, break
  // Then i=1 survives. So the SECOND alt wins (the one that dominates i=0).
  // Actually let's trace carefully:
  //   i=0, j=1: DominanceFn{}(alts[0], alts[1]) checks alts[0].dominated_by(alts[1])
  //     = alts[1].cost <= alts[0].cost && alts[0].required ⊇ alts[1].required
  //     = 2.0 <= 2.0 && {1} ⊇ {1} → true → dominated[0] = true, break
  //   i=1: not dominated → survives
  // Result: the second alt (enode_id=1) survives.
  ASSERT_EQ(frontier.alts.size(), 1) << "One of the duplicates should be pruned";
  ASSERT_EQ(frontier.alts[0].enode_id, ENodeId{1}) << "Second alt survives (first is marked dominated)";
}

TEST(ParetoFrontier_Prune, TransitiveDominance) {
  // A dominates B, B dominates C. After prune, only A survives.
  // A: cost=1, req={}
  // B: cost=2, req={1}    — A dominates B: A.cost<=B.cost && B.req ⊇ A.req → 1<=2 && {1}⊇{} → true
  // C: cost=3, req={1,2}  — B dominates C: B.cost<=C.cost && C.req ⊇ B.req → 2<=3 && {1,2}⊇{1} → true
  //                        — A dominates C: 1<=3 && {1,2}⊇{} → true (transitivity)
  // The break optimization: when i=1 (B) is checked, it gets marked dominated by i=0 (A)
  // in the i=0 loop, so B is skipped. C is also marked dominated in the i=0 loop.
  auto frontier = TestFrontier{{
      {.cost = 1.0, .required = {}, .enode_id = ENodeId{0}},      // A
      {.cost = 2.0, .required = {1}, .enode_id = ENodeId{1}},     // B
      {.cost = 3.0, .required = {1, 2}, .enode_id = ENodeId{2}},  // C
  }};
  frontier.prune();
  ASSERT_EQ(frontier.alts.size(), 1);
  ASSERT_EQ(frontier.alts[0].enode_id, ENodeId{0}) << "Only A should survive (dominates B and C)";

  // Also test with reversed input order to exercise the break optimization path.
  // When C is at index 0: i=0(C), j=1(B): C.dominated_by(B) → true → dominated[0]=true, break
  // Then i=1(B), j=2(A): B.dominated_by(A) → true → dominated[1]=true, break
  // Then i=2(A): survives.
  auto frontier_reversed = TestFrontier{{
      {.cost = 3.0, .required = {1, 2}, .enode_id = ENodeId{2}},  // C
      {.cost = 2.0, .required = {1}, .enode_id = ENodeId{1}},     // B
      {.cost = 1.0, .required = {}, .enode_id = ENodeId{0}},      // A
  }};
  frontier_reversed.prune();
  ASSERT_EQ(frontier_reversed.alts.size(), 1);
  ASSERT_EQ(frontier_reversed.alts[0].enode_id, ENodeId{0}) << "Only A should survive regardless of input order";
}

TEST(ParetoFrontier_Prune, BeamLimit) {
  // 10 Pareto-incomparable alternatives: each has a unique required set (disjoint),
  // so no dominance pruning occurs. Costs increase from 1.0 to 10.0.
  auto frontier = TestFrontier{};
  for (int i = 0; i < 10; ++i) {
    frontier.alts.push_back({.cost = static_cast<double>(i + 1),
                             .required = {static_cast<uint16_t>(i + 100)},  // disjoint required sets
                             .enode_id = ENodeId{static_cast<uint32_t>(i)}});
  }

  // Verify prune alone doesn't reduce (all are Pareto-incomparable)
  frontier.prune();
  ASSERT_EQ(frontier.alts.size(), 10) << "Dominance pruning should not remove any (all incomparable)";

  // Apply beam limit of 5 — should keep the 5 cheapest
  frontier.beam(5, [](TestDemandAlt const &a) { return a.cost; });
  ASSERT_EQ(frontier.alts.size(), 5) << "Beam limit should reduce to 5 alternatives";

  // Verify the survivors are the 5 cheapest (costs 1..5)
  auto costs = std::vector<double>{};
  for (auto const &alt : frontier.alts) {
    costs.push_back(alt.cost);
  }
  std::sort(costs.begin(), costs.end());
  ASSERT_EQ(costs, (std::vector<double>{1.0, 2.0, 3.0, 4.0, 5.0}));
}

TEST(ParetoFrontier_Prune, BeamLimitNoEffectWhenSmall) {
  // 3 Pareto-incomparable alternatives, beam limit of 10 — should not reduce.
  auto frontier = TestFrontier{{
      {.cost = 1.0, .required = {1, 2}, .enode_id = ENodeId{0}},
      {.cost = 2.0, .required = {3}, .enode_id = ENodeId{1}},
      {.cost = 3.0, .required = {4}, .enode_id = ENodeId{2}},
  }};
  frontier.prune();
  ASSERT_EQ(frontier.alts.size(), 3) << "All three are Pareto-incomparable";

  frontier.beam(10, [](TestDemandAlt const &a) { return a.cost; });
  ASSERT_EQ(frontier.alts.size(), 3) << "Beam limit of 10 should not reduce 3 alternatives";
}

TEST(Extract_MultiAlt, SingleAlternative_BehavesLikeSingleBest) {
  // Multi-alt with a single alternative per enode should produce the same result
  // as single-best extraction
  auto egraph = EGraph<symbol, analysis>{};
  auto [l1_class, l1_node, l1_new] = egraph.emplace(symbol::A);
  auto [l2_class, l2_node, l2_new] = egraph.emplace(symbol::B);
  auto [root_class, root_node, root_new] = egraph.emplace(symbol::A, {l1_class, l2_class});

  auto extractor = Extractor{egraph, SimpleMultiAltCostModel{}};
  auto extracted = extractor.Extract(root_class);

  ASSERT_EQ(extracted.size(), 3);
  ASSERT_EQ(extracted[0].first, root_class);
  ASSERT_EQ(extracted[0].second, root_node);
}

TEST(Extract_MultiAlt, TwoAlternatives_MergeFrontier) {
  // Two enodes in same eclass producing different alternatives.
  // A produces {cost=1,req={1}} and {cost=2,req={}}
  // B produces {cost=1,req={}}
  // After merge: B's {cost=1,req={}} dominates A's {cost=2,req={}},
  // and A's {cost=1,req={1}} is non-dominated.
  // Frontier = [{cost=1,req={1},B}, {cost=1,req={},B}]
  // Wait — B's alt has cost=1,req={} which dominates A's cost=2,req={}.
  // A's cost=1,req={1} is not dominated by B's cost=1,req={} because req={1} ⊄ req={}.
  // Actually, dominated_by checks: other.cost <= cost && required ⊇ other.required
  // A's {cost=1,req={1}} dominated by B's {cost=1,req={}}? other.cost=1 <= 1 ✓, req={1} ⊇ {} ✓ → YES!
  // So B's {cost=1,req={}} dominates both of A's alternatives.
  // Resolved enode should be B.
  auto egraph = EGraph<symbol, analysis>{};
  auto [aclass, anode, a_new] = egraph.emplace(symbol::A);
  auto [bclass, bnode, b_new] = egraph.emplace(symbol::B);
  auto [root, _] = egraph.merge(aclass, bclass);
  auto ctx = ProcessingContext<symbol>{};
  egraph.rebuild(ctx);

  auto extractor = Extractor{egraph, DemandAwareMultiAltCostModel{}};
  auto extracted = extractor.Extract(root);

  ASSERT_EQ(extracted.size(), 1);
  // B has single alt {cost=1, req={}}, which dominates A's alternatives
  ASSERT_EQ(extracted[0].second, bnode);
}

TEST(Extract_MultiAlt, DemandPropagation) {
  // Parent (B) depends on child (A or B equivalent).
  // A produces {cost=1,req={1}} and {cost=2,req={}}
  // B produces {cost=1,req={}}
  // After merging A and B in child eclass: frontier = [{cost=1,req={},B_node}]
  // Parent (symbol B, one child): {cost=1+1=2, req={}, parent_node}
  auto egraph = EGraph<symbol, analysis>{};
  auto [a_class, a_node, a_new] = egraph.emplace(symbol::A);
  auto [b_class, b_node, b_new] = egraph.emplace(symbol::B);
  auto [child_class, _] = egraph.merge(a_class, b_class);
  auto ctx = ProcessingContext<symbol>{};
  egraph.rebuild(ctx);

  auto [parent_class, parent_node, parent_new] = egraph.emplace(symbol::B, {child_class});

  auto extractor = Extractor{egraph, DemandAwareMultiAltCostModel{}};
  auto extracted = extractor.Extract(parent_class);

  ASSERT_EQ(extracted.size(), 2);
  ASSERT_EQ(extracted[0].first, parent_class);
  ASSERT_EQ(extracted[0].second, parent_node);
  // Child should resolve to B (cheapest no-demand alternative)
  ASSERT_EQ(extracted[1].second, b_node);
}

TEST(Extract_MultiAlt, DominatedPruning) {
  // Test that dominated alternatives are correctly pruned.
  // Create two equivalent enodes where one strictly dominates the other.
  // A: {cost=1,req={1}} and {cost=2,req={}} — two alternatives
  // A (another): {cost=3,req={}} — one alternative
  // After merge: {cost=1,req={1}}, {cost=2,req={}}, {cost=3,req={}}
  // {cost=2,req={}} dominates {cost=3,req={}} → pruned
  // Final: {cost=1,req={1}}, {cost=2,req={}}
  // Resolve picks min cost: {cost=1,req={1}}
  auto egraph = EGraph<symbol, analysis>{};
  auto [a1_class, a1_node, a1_new] = egraph.emplace(symbol::A);  // produces 2 alts
  auto [a2_class, a2_node, a2_new] = egraph.emplace(symbol::A);  // produces 2 alts
  // a2 is a separate A node; merge to get both in same eclass
  auto [merged, _] = egraph.merge(a1_class, a2_class);
  auto ctx = ProcessingContext<symbol>{};
  egraph.rebuild(ctx);

  // The frontier should have: from a1: {1,{1}},{2,{}} and from a2: {1,{1}},{2,{}}
  // After Pareto pruning, effectively 2 unique alternatives
  std::unordered_map<EClassId, EClassFrontier<DemandAwareMultiAltCostModel::CostResult>> frontier_map;
  ComputeFrontiers(egraph, DemandAwareMultiAltCostModel{}, merged, frontier_map);

  auto it = frontier_map.find(merged);
  ASSERT_NE(it, frontier_map.end());
  ASSERT_TRUE(it->second.has_value());
  auto const &frontier = *it->second;
  // After pruning, exactly 2 non-dominated alternatives should survive:
  //   {cost=1, required={1}} and {cost=2, required={}}
  ASSERT_EQ(frontier.alts.size(), 2);

  // Verify both specific alternatives are present
  auto has_demand_alt = std::ranges::any_of(
      frontier.alts, [](auto const &alt) { return alt.cost == 1.0 && alt.required == std::set<int>{1}; });
  auto has_no_demand_alt =
      std::ranges::any_of(frontier.alts, [](auto const &alt) { return alt.cost == 2.0 && alt.required.empty(); });
  ASSERT_TRUE(has_demand_alt) << "Expected alternative with cost=1.0, required={1}";
  ASSERT_TRUE(has_no_demand_alt) << "Expected alternative with cost=2.0, required={}";

  // Min cost is 1.0 (the {1}-requiring alternative)
  ASSERT_DOUBLE_EQ(TestFrontier::min_cost(frontier), 1.0);
}

TEST(Extract_MultiAlt, DiamondDAG_WithDemand) {
  // Diamond DAG where a shared eclass is referenced by two parents.
  // Shared (symbol A) produces two alternatives: {cost=1,req={1}} and {cost=2,req={}}.
  // Left and Right (symbol B) each reference Shared as a child.
  // Root (symbol B) references Left and Right.
  //
  // The shared eclass should be extracted once, and the correct alternative
  // selected based on demand resolution.
  auto egraph = EGraph<symbol, analysis>{};
  auto [shared_class, shared_node, shared_new] = egraph.emplace(symbol::A);
  auto [left_class, left_node, left_new] = egraph.emplace(symbol::B, {shared_class}, 1);     // disambiguator = 1
  auto [right_class, right_node, right_new] = egraph.emplace(symbol::B, {shared_class}, 2);  // disambiguator = 2
  auto [root_class, root_node, root_new] = egraph.emplace(symbol::B, {left_class, right_class});

  auto extractor = Extractor{egraph, DemandAwareMultiAltCostModel{}};
  auto extracted = extractor.Extract(root_class);

  // Should have 4 nodes: root, left, right, shared
  ASSERT_EQ(extracted.size(), 4);
  // Root comes first in topological order
  ASSERT_EQ(extracted[0].first, root_class);
  ASSERT_EQ(extracted[0].second, root_node);
  // Shared comes last (highest in-degree = 2)
  ASSERT_EQ(extracted[3].first, shared_class);
  // The shared eclass should resolve to its A enode (only enode in the class)
  ASSERT_EQ(extracted[3].second, shared_node);

  // Verify shared eclass appears exactly once (not duplicated for each parent)
  auto shared_count = std::ranges::count_if(extracted, [&](auto const &p) { return p.first == shared_class; });
  ASSERT_EQ(shared_count, 1);
}

TEST(Extract_MultiAlt, ThreeNonDominatedAlternatives) {
  // Create a cost model that produces 3 Pareto-incomparable alternatives.
  // Use symbol C and D to produce alternatives with different cost/demand tradeoffs.
  // We define a custom cost model inline.
  struct ThreeAltCostModel {
    using CostResult = TestFrontier;

    static auto operator()(ENode<symbol> const &current, ENodeId enode_id, std::span<CostResult const> children)
        -> CostResult {
      auto child_cost = std::ranges::fold_left(
          children, 0.0, [](double acc, CostResult const &c) { return acc + CostResult::min_cost(c); });
      if (current.symbol() == symbol::A) {
        return CostResult{{
            {.cost = 1.0 + child_cost, .required = {1, 2}, .enode_id = enode_id},
            {.cost = 2.0 + child_cost, .required = {1}, .enode_id = enode_id},
            {.cost = 3.0 + child_cost, .required = {}, .enode_id = enode_id},
        }};
      }
      return CostResult{{{.cost = 1.0 + child_cost, .required = {}, .enode_id = enode_id}}};
    }
  };

  auto egraph = EGraph<symbol, analysis>{};
  auto [a_class, a_node, a_new] = egraph.emplace(symbol::A);

  // Directly compute frontiers to inspect the Pareto set
  std::unordered_map<EClassId, EClassFrontier<ThreeAltCostModel::CostResult>> frontier_map;
  ComputeFrontiers(egraph, ThreeAltCostModel{}, a_class, frontier_map);

  auto it = frontier_map.find(a_class);
  ASSERT_NE(it, frontier_map.end());
  ASSERT_TRUE(it->second.has_value());
  auto const &frontier = *it->second;

  // All 3 alternatives are Pareto-incomparable: none dominates another
  ASSERT_EQ(frontier.alts.size(), 3);

  // Verify each specific alternative is present
  auto has_alt_1_2 = std::ranges::any_of(
      frontier.alts, [](auto const &alt) { return alt.cost == 1.0 && alt.required == std::set<int>{1, 2}; });
  auto has_alt_1 = std::ranges::any_of(
      frontier.alts, [](auto const &alt) { return alt.cost == 2.0 && alt.required == std::set<int>{1}; });
  auto has_alt_none =
      std::ranges::any_of(frontier.alts, [](auto const &alt) { return alt.cost == 3.0 && alt.required.empty(); });
  ASSERT_TRUE(has_alt_1_2) << "Expected alternative with cost=1.0, required={1,2}";
  ASSERT_TRUE(has_alt_1) << "Expected alternative with cost=2.0, required={1}";
  ASSERT_TRUE(has_alt_none) << "Expected alternative with cost=3.0, required={}";

  // Min cost should be 1.0
  ASSERT_DOUBLE_EQ(TestFrontier::min_cost(frontier), 1.0);

  // Full extraction should still work and select the min-cost alternative
  auto extractor = Extractor{egraph, ThreeAltCostModel{}};
  auto extracted = extractor.Extract(a_class);
  ASSERT_EQ(extracted.size(), 1);
  ASSERT_EQ(extracted[0].second, a_node);
}

// Demonstrates the DAG resolution fix: when a shared eclass is visited from
// two parents with different provided sets, the resolver re-resolves on
// incompatible re-visit rather than using first-visitor-wins caching.
//
// Diamond DAG: Root(B) → Left(B, {Shared}), Right(B, {Shared})
// Shared eclass (symbol A) has two non-dominated alternatives:
//   {cost=1, req={1}}  — cheap but demands "1"
//   {cost=2, req={}}   — expensive but no demands
//
// Left provides {1}, Right provides {}. Without the fix, Left locks in
// {cost=1, req={1}} and Right gets an infeasible cached result. With the fix,
// Right's visit triggers re-resolution to {cost=2, req={}} — feasible for all.
TEST(Extract_MultiAlt, DAGResolution_FirstVisitorWins) {
  auto egraph = EGraph<symbol, analysis>{};
  auto [shared_class, shared_node, shared_new] = egraph.emplace(symbol::A);
  auto [left_class, left_node, left_new] = egraph.emplace(symbol::B, {shared_class}, 1);
  auto [right_class, right_node, right_new] = egraph.emplace(symbol::B, {shared_class}, 2);
  auto [root_class, root_node, root_new] = egraph.emplace(symbol::B, {left_class, right_class});

  // Compute frontiers bottom-up
  using CM = DemandAwareMultiAltCostModel;
  std::unordered_map<EClassId, EClassFrontier<CM::CostResult>> frontier_map;
  ComputeFrontiers(egraph, CM{}, root_class, frontier_map);

  // Verify shared eclass has both non-dominated alternatives
  auto const &shared_frontier = *frontier_map.at(shared_class);
  ASSERT_EQ(shared_frontier.alts.size(), 2);

  // Context-aware resolver with re-resolve on incompatible re-visit.
  // Mimics the fixed ResolvePlanSelection pattern.
  auto resolved = std::unordered_map<EClassId, std::pair<ENodeId, double>>{};
  auto resolved_required = std::unordered_map<EClassId, std::set<int>>{};

  auto pick_best_compatible = [](TestFrontier const &frontier, std::set<int> const &provided) -> TestDemandAlt const * {
    TestDemandAlt const *best = nullptr;
    for (auto const &alt : frontier.alts) {
      if (std::ranges::includes(provided, alt.required)) {
        if (!best || alt.cost < best->cost) best = &alt;
      }
    }
    return best;
  };

  // DFS resolver where the Root node provides different contexts per child:
  //   Left child gets provided={1} (mimics alive Bind providing a symbol)
  //   Right child gets provided={} (no extra symbols)
  auto resolve = [&](this auto const &self, EClassId id, std::set<int> const &provided) -> void {
    if (auto existing = resolved.find(id); existing != resolved.end()) {
      // DAG re-visit: check if cached selection is compatible with this context
      if (std::ranges::includes(provided, resolved_required[id])) return;  // still feasible
      // Incompatible: re-resolve with more restrictive provided
      auto const &frontier = *frontier_map.at(id);
      auto const *chosen = pick_best_compatible(frontier, provided);
      ASSERT_NE(chosen, nullptr);
      existing->second = {chosen->enode_id, chosen->cost};
      resolved_required[id] = chosen->required;
      return;
    }
    auto const &frontier = *frontier_map.at(id);
    auto const *chosen = pick_best_compatible(frontier, provided);
    ASSERT_NE(chosen, nullptr);
    resolved[id] = {chosen->enode_id, chosen->cost};
    resolved_required[id] = chosen->required;
    auto const &enode = egraph.get_enode(chosen->enode_id);
    auto const &children = enode.children();

    if (id == root_class && children.size() == 2) {
      // Root provides different contexts per child (like an Output with alive Bind + NamedOutput)
      self(children[0], {1});  // Left child: provided={1}
      self(children[1], {});   // Right child: provided={}
    } else {
      for (auto child : children) {
        self(child, provided);
      }
    }
  };

  resolve(root_class, {});

  // Left was visited first with provided={1}.
  // Its child (Shared) was initially resolved with provided={1} → picks {cost=1, req={1}}.
  //
  // Right then visits Shared with provided={}. The cached {cost=1, req={1}} is INCOMPATIBLE
  // (req={1} ⊄ provided={}). The resolver re-resolves with provided={} → picks {cost=2, req={}}.
  auto const &[shared_enode, shared_cost] = resolved.at(shared_class);
  ASSERT_EQ(shared_cost, 2.0);  // Re-resolved to the conservative (no-demand) alt

  // Verify: the final selection has empty required — feasible for ALL visiting contexts
  ASSERT_TRUE(resolved_required[shared_class].empty());
}

TEST(Extract_MultiAlt, DAGResolution_CascadesToChildren) {
  // 3-level diamond demonstrating the cascade bug:
  //   Root(B) → Left(B,{Shared}), Right(B,{Shared})
  //   Shared(A,{Leaf}) — demand-aware: has alts with req={1} and req={}
  //   Leaf(A)          — demand-aware: {cost=1,req={1}} and {cost=2,req={}}
  //
  // Root provides {1} to Left, {} to Right.
  //
  // 1. Left visits Shared with provided={1} → picks cheap alt with req={1}.
  //    Shared visits Leaf with provided={1} → Leaf picks {cost=1,req={1}}.
  // 2. Right visits Shared with provided={} → req={1} not subset of {}, re-resolves to req={}.
  //    BUG: returns without visiting Leaf. Leaf keeps stale {cost=1,req={1}}.
  //    FIX: cascade to children → Leaf re-resolves to {cost=2,req={}}.
  auto egraph = EGraph<symbol, analysis>{};
  auto [leaf_class, leaf_node, leaf_new] = egraph.emplace(symbol::A);
  auto [shared_class, shared_node, shared_new] = egraph.emplace(symbol::A, {leaf_class});
  auto [left_class, left_node, left_new] = egraph.emplace(symbol::B, {shared_class}, 1);
  auto [right_class, right_node, right_new] = egraph.emplace(symbol::B, {shared_class}, 2);
  auto [root_class, root_node, root_new] = egraph.emplace(symbol::B, {left_class, right_class});

  // Compute frontiers bottom-up
  using CM = DemandAwareMultiAltCostModel;
  std::unordered_map<EClassId, EClassFrontier<CM::CostResult>> frontier_map;
  ComputeFrontiers(egraph, CM{}, root_class, frontier_map);

  // Verify both Shared and Leaf have two non-dominated alternatives
  auto const &shared_frontier = *frontier_map.at(shared_class);
  ASSERT_EQ(shared_frontier.alts.size(), 2);
  auto const &leaf_frontier = *frontier_map.at(leaf_class);
  ASSERT_EQ(leaf_frontier.alts.size(), 2);

  // Context-aware resolver — WITHOUT cascade fix (reproduces the bug).
  auto resolved = std::unordered_map<EClassId, std::pair<ENodeId, double>>{};
  auto resolved_required = std::unordered_map<EClassId, std::set<int>>{};

  auto pick_best_compatible = [](TestFrontier const &frontier, std::set<int> const &provided) -> TestDemandAlt const * {
    TestDemandAlt const *best = nullptr;
    for (auto const &alt : frontier.alts) {
      if (std::ranges::includes(provided, alt.required)) {
        if (!best || alt.cost < best->cost) best = &alt;
      }
    }
    return best;
  };

  auto resolve_no_cascade = [&](this auto const &self, EClassId id, std::set<int> const &provided) -> void {
    if (auto existing = resolved.find(id); existing != resolved.end()) {
      if (std::ranges::includes(provided, resolved_required[id])) return;
      auto const &frontier = *frontier_map.at(id);
      auto const *chosen = pick_best_compatible(frontier, provided);
      ASSERT_NE(chosen, nullptr);
      existing->second = {chosen->enode_id, chosen->cost};
      resolved_required[id] = chosen->required;
      // BUG: no cascade to children
      return;
    }
    auto const &frontier = *frontier_map.at(id);
    auto const *chosen = pick_best_compatible(frontier, provided);
    ASSERT_NE(chosen, nullptr);
    resolved[id] = {chosen->enode_id, chosen->cost};
    resolved_required[id] = chosen->required;
    auto const &enode = egraph.get_enode(chosen->enode_id);
    auto const &children = enode.children();
    if (id == root_class && children.size() == 2) {
      self(children[0], {1});
      self(children[1], {});
    } else {
      for (auto child : children) {
        self(child, provided);
      }
    }
  };

  resolve_no_cascade(root_class, {});

  // Shared was re-resolved to req={} — correct
  ASSERT_TRUE(resolved_required[shared_class].empty());
  // BUG: Leaf still has stale req={1} from Left's context
  ASSERT_EQ(resolved_required[leaf_class], std::set<int>{1});

  // Now resolve WITH the cascade fix
  resolved.clear();
  resolved_required.clear();

  auto resolve_with_cascade = [&](this auto const &self, EClassId id, std::set<int> const &provided) -> void {
    if (auto existing = resolved.find(id); existing != resolved.end()) {
      if (std::ranges::includes(provided, resolved_required[id])) return;
      auto const &frontier = *frontier_map.at(id);
      auto const *chosen = pick_best_compatible(frontier, provided);
      ASSERT_NE(chosen, nullptr);
      existing->second = {chosen->enode_id, chosen->cost};
      resolved_required[id] = chosen->required;
      // FIX: cascade to children of the new selection
      auto const &enode = egraph.get_enode(chosen->enode_id);
      for (auto child : enode.children()) {
        self(child, provided);
      }
      return;
    }
    auto const &frontier = *frontier_map.at(id);
    auto const *chosen = pick_best_compatible(frontier, provided);
    ASSERT_NE(chosen, nullptr);
    resolved[id] = {chosen->enode_id, chosen->cost};
    resolved_required[id] = chosen->required;
    auto const &enode = egraph.get_enode(chosen->enode_id);
    auto const &children = enode.children();
    if (id == root_class && children.size() == 2) {
      self(children[0], {1});
      self(children[1], {});
    } else {
      for (auto child : children) {
        self(child, provided);
      }
    }
  };

  resolve_with_cascade(root_class, {});

  // Shared re-resolved to req={} — same as before
  ASSERT_TRUE(resolved_required[shared_class].empty());
  // FIX: Leaf is now re-resolved to {cost=2,req={}} via cascade
  ASSERT_EQ(resolved.at(leaf_class).second, 2.0);
  ASSERT_TRUE(resolved_required[leaf_class].empty());
}

TEST(Extract_MultiAlt, CyclicEClass) {
  // Multi-alt should handle cycles the same as single-best: skip cyclic enodes
  auto egraph = EGraph<symbol, analysis>{};
  auto [x_class, x_node, x_new] = egraph.emplace(symbol::LITERAL, 1);
  auto [zero_class, zero_node, zero_new] = egraph.emplace(symbol::LITERAL, 0);
  auto [plus_class, plus_node, plus_new] = egraph.emplace(symbol::ADD, {x_class, zero_class});
  auto [cyclic_class, _] = egraph.merge(x_class, plus_class);
  auto ctx = ProcessingContext<symbol>{};
  egraph.rebuild(ctx);

  auto extractor = Extractor{egraph, SimpleMultiAltCostModel{}};
  auto extracted = extractor.Extract(cyclic_class);

  // Should select the non-cyclic LITERAL node
  ASSERT_EQ(extracted.size(), 1);
  ASSERT_EQ(extracted[0].second, x_node);
}
