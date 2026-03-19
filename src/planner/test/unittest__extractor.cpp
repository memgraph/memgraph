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

// Simple cost models — CostModelTraits provides merge/resolve/min_cost defaults.
struct UniformCostModel {
  using CostResult = double;

  static auto operator()(ENode<symbol> const & /*current*/, std::span<CostResult const> children) -> CostResult {
    return 1.0 + std::ranges::fold_left(children, 0.0, [](double acc, CostResult const &val) { return acc + val; });
  }
};

struct SymbolCostModel {
  using CostResult = double;
  double a_cost;
  double b_cost;

  auto operator()(ENode<symbol> const &current, std::span<CostResult const> children) const -> CostResult {
    return (current.symbol() == symbol::A ? a_cost : b_cost) +
           std::ranges::fold_left(children, 0.0, [](double acc, CostResult const &val) { return acc + val; });
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

TEST(Extract_Cost, SingleLeafNode) {
  struct CostModel {
    using CostResult = double;

    static auto operator()(ENode<symbol> const & /* current */, std::span<CostResult const> children) -> CostResult {
      return 5.0 + std::ranges::fold_left(children, 0.0, [](double acc, CostResult const &val) { return acc + val; });
    }
  };

  auto egraph = EGraph<symbol, analysis>{};
  auto [leaf_class, leaf_node, leaf_new] = egraph.emplace(symbol::A);

  std::unordered_map<EClassId, Selection<CostModel::CostResult>> cheapest_enode;

  auto cost = ProcessCosts(egraph, CostModel{}, leaf_class, cheapest_enode);

  ASSERT_EQ(cost, 5.0);
  ASSERT_EQ(cheapest_enode.size(), 1);
  ASSERT_EQ(cheapest_enode[leaf_class].enode_id, leaf_node);
  ASSERT_EQ(cheapest_enode[leaf_class].cost_result, 5.0);
}

TEST(Extract_Cost, SimpleTree) {
  struct CostModel {
    using CostResult = double;

    static auto operator()(ENode<symbol> const & /*current*/, std::span<CostResult const> children) -> CostResult {
      return 1.0 + std::ranges::fold_left(children, 0.0, [](double acc, CostResult const &val) { return acc + val; });
    }
  };

  auto egraph = EGraph<symbol, analysis>{};
  auto [left_class, left_node, left_new] = egraph.emplace(symbol::A);
  auto [right_class, right_node, right_new] = egraph.emplace(symbol::B);
  auto [root_class, root_node, root_new] = egraph.emplace(symbol::A, {left_class, right_class});

  std::unordered_map<EClassId, Selection<CostModel::CostResult>> cheapest_enode;

  auto cost = ProcessCosts(egraph, CostModel{}, root_class, cheapest_enode);

  // Cost should be: 1 (root) + 1 (left) + 1 (right) = 3
  ASSERT_EQ(cost, 3.0);
  ASSERT_EQ(cheapest_enode.size(), 3);
}

TEST(Extract_Cost, DeepTree) {
  struct CostModel {
    using CostResult = double;

    static auto operator()(ENode<symbol> const & /*current*/, std::span<CostResult const> children) -> CostResult {
      return 1.0 + std::ranges::fold_left(children, 0.0, [](double acc, CostResult const &val) { return acc + val; });
    }
  };

  auto egraph = EGraph<symbol, analysis>{};
  auto [leaf_class, leaf_node, leaf_new] = egraph.emplace(symbol::A);
  auto [mid_class, mid_node, mid_new] = egraph.emplace(symbol::B, {leaf_class});
  auto [root_class, root_node, root_new] = egraph.emplace(symbol::A, {mid_class});

  std::unordered_map<EClassId, Selection<CostModel::CostResult>> cheapest_enode;
  auto cost = ProcessCosts(egraph, CostModel{}, root_class, cheapest_enode);

  // Cost should be: 1 (root) + 1 (mid) + 1 (leaf) = 3
  ASSERT_EQ(cost, 3.0);
  ASSERT_EQ(cheapest_enode.size(), 3);
}

TEST(Extract_Cost, DiamondDAGSharedNode) {
  struct CostModel {
    using CostResult = double;

    static auto operator()(ENode<symbol> const & /*current*/, std::span<CostResult const> children) -> CostResult {
      return 1.0 + std::ranges::fold_left(children, 0.0, [](double acc, CostResult const &val) { return acc + val; });
    }
  };

  auto egraph = EGraph<symbol, analysis>{};
  auto [shared_class, shared_node, shared_new] = egraph.emplace(symbol::A);
  auto [left_class, left_node, left_new] = egraph.emplace(symbol::B, {shared_class}, 1);     // disambiguator = 1
  auto [right_class, right_node, right_new] = egraph.emplace(symbol::B, {shared_class}, 2);  // disambiguator = 2
  auto [root_class, root_node, root_new] = egraph.emplace(symbol::A, {left_class, right_class});

  std::unordered_map<EClassId, Selection<CostModel::CostResult>> cheapest_enode;
  auto cost = ProcessCosts(egraph, CostModel{}, root_class, cheapest_enode);

  // Shared node should only be computed once via memoization
  // Cost: root=1 + (left=1+shared=1) + (right=1+shared=1) = 1+2+2 = 5
  ASSERT_EQ(cost, 5.0);
  ASSERT_EQ(cheapest_enode.size(), 4);
  ASSERT_EQ(cheapest_enode[shared_class].cost_result, 1);
  ASSERT_EQ(cheapest_enode[left_class].cost_result, 2);
  ASSERT_EQ(cheapest_enode[right_class].cost_result, 2);
  ASSERT_EQ(cheapest_enode[root_class].cost_result, 5);
}

TEST(Extract_Cost, VariableCostBySymbol) {
  struct CostModel {
    using CostResult = double;

    static auto operator()(ENode<symbol> const &current, std::span<CostResult const> children) -> CostResult {
      return (current.symbol() == symbol::A ? 2.0 : 5.0) +
             std::ranges::fold_left(children, 0.0, [](double acc, CostResult const &val) { return acc + val; });
    }
  };

  auto egraph = EGraph<symbol, analysis>{};
  auto [aclass, anode, a_new] = egraph.emplace(symbol::A);
  auto [bclass, bnode, b_new] = egraph.emplace(symbol::B);

  std::unordered_map<EClassId, Selection<CostModel::CostResult>> cheapest_enode;

  auto cost_a = ProcessCosts(egraph, CostModel{}, aclass, cheapest_enode);
  auto cost_b = ProcessCosts(egraph, CostModel{}, bclass, cheapest_enode);

  ASSERT_EQ(cost_a, 2.0);
  ASSERT_EQ(cost_b, 5.0);
}

TEST(Extract_Cost, SelectsCheapestAmongEquivalents) {
  struct CostModel {
    using CostResult = double;

    static auto operator()(ENode<symbol> const &current, std::span<CostResult const> children) -> CostResult {
      return (current.symbol() == symbol::A ? 1.0 : 10.0) +
             std::ranges::fold_left(children, 0.0, [](double acc, CostResult const &val) { return acc + val; });
    }
  };

  auto egraph = EGraph<symbol, analysis>{};
  auto [aclass, anode, a_new] = egraph.emplace(symbol::A);
  auto [bclass, bnode, b_new] = egraph.emplace(symbol::B);
  auto [root, _] = egraph.merge(aclass, bclass);
  auto ctx = ProcessingContext<symbol>{};
  egraph.rebuild(ctx);

  std::unordered_map<EClassId, Selection<CostModel::CostResult>> cheapest_enode;

  auto cost = ProcessCosts(egraph, CostModel{}, root, cheapest_enode);

  ASSERT_EQ(cost, 1.0);
  ASSERT_EQ(cheapest_enode[root].enode_id, anode);
  ASSERT_EQ(cheapest_enode[root].cost_result, 1.0);
}

TEST(Extract_Cost, CostAccumulationWithVariableCosts) {
  struct CostModel {
    using CostResult = double;

    static auto operator()(ENode<symbol> const &current, std::span<CostResult const> children) -> CostResult {
      return (current.symbol() == symbol::A ? 2.0 : 3.0) +
             std::ranges::fold_left(children, 0.0, [](double acc, CostResult const &val) { return acc + val; });
    }
  };

  auto egraph = EGraph<symbol, analysis>{};
  auto [leaf1_class, leaf1_node, leaf1_new] = egraph.emplace(symbol::A);
  auto [leaf2_class, leaf2_node, leaf2_new] = egraph.emplace(symbol::B);
  auto [root_class, root_node, root_new] = egraph.emplace(symbol::A, {leaf1_class, leaf2_class});

  std::unordered_map<EClassId, Selection<CostModel::CostResult>> cheapest_enode;

  auto cost = ProcessCosts(egraph, CostModel{}, root_class, cheapest_enode);

  // Cost: 2 (root, symbol A) + 2 (leaf1, symbol A) + 3 (leaf2, symbol B) = 7
  ASSERT_EQ(cost, 7.0);
}

TEST(Extract_Cost, CyclicEGraphInfiniteCost) {
  // Test case: X (LITERAL 1) merged with (ADD X (LITERAL 0))
  // This creates a cycle: the 'ADD' e-node has a child that is its own e-class
  // When evaluating 'ADD' e-node: cost = 1 + cost(X_class) + cost(LITERAL 0)
  // But X_class now contains both LITERAL 1 and the 'ADD' node
  // The 'ADD' node can't be the cheapest because it depends on itself
  // If all e-nodes in the e-class are cyclic, the cost should be infinite

  struct CostModel {
    using CostResult = double;

    static auto operator()(ENode<symbol> const & /* current */, std::span<CostResult const> children) -> CostResult {
      return 1.0 + std::ranges::fold_left(children, 0.0, [](double acc, CostResult const &val) { return acc + val; });
    }
  };

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

  std::unordered_map<EClassId, Selection<CostModel::CostResult>> cheapest_enode;

  // Process the cyclic e-class
  auto cost = ProcessCosts(egraph, CostModel{}, cyclic_class, cheapest_enode);

  // The cyclic 'ADD' node should have infinite cost (or very large)
  // The non-cyclic LITERAL node should be selected with cost 1
  ASSERT_EQ(cost, 1.0);
  ASSERT_EQ(cheapest_enode[cyclic_class].enode_id, x_node);
  ASSERT_EQ(cheapest_enode[cyclic_class].cost_result, 1.0);
}

TEST(Extract_Cost, CyclicEGraphInfiniteCostComplex) {
  // Test case: X (LITERAL 1) merged with (ADD X (LITERAL 0))
  // This creates a cycle: the 'ADD' e-node has a child that is its own e-class
  // When evaluating 'ADD' e-node: cost = 1 + cost(X_class) + cost(LITERAL 0)
  // But X_class now contains both LITERAL 1 and the 'ADD' node
  // The 'ADD' node can't be the cheapest because it depends on itself
  // If all e-nodes in the e-class are cyclic, the cost should be infinite

  struct CostModel {
    using CostResult = double;

    static auto operator()(ENode<symbol> const & /* current */, std::span<CostResult const> children) -> CostResult {
      return 1.0 + std::ranges::fold_left(children, 0.0, [](double acc, CostResult const &val) { return acc + val; });
    }
  };

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

  std::unordered_map<EClassId, Selection<CostModel::CostResult>> cheapest_enode;

  // Process the cyclic e-class
  auto cost = ProcessCosts(egraph, CostModel{}, merged_class, cheapest_enode);

  // The cyclic 'ADD' node should have infinite cost (or very large)
  // The non-cyclic LITERAL node should be selected with cost 1
  ASSERT_EQ(cost, 1.0);
  ASSERT_EQ(cheapest_enode.size(), 2);
  ASSERT_TRUE(cheapest_enode.contains(merged_class));
  ASSERT_EQ(cheapest_enode[merged_class].cost_result, 1.0);
  ASSERT_EQ(cheapest_enode[merged_class].enode_id, x_node);
  ASSERT_TRUE(cheapest_enode.contains(zero_class));
  ASSERT_EQ(cheapest_enode[zero_class].cost_result, 1.0);
  ASSERT_EQ(cheapest_enode[zero_class].enode_id, zero_node);
}

TEST(Extract_Cost, FullyCyclicEClassInfiniteCost) {
  // Test case: All e-nodes in an e-class are cyclic
  // E.g., X contains only (ADD X (LITERAL 1)) and (ADD X (LITERAL 2))
  // Both ADD nodes reference the e-class they're in, creating unavoidable
  // cycles

  struct CostModel {
    using CostResult = double;

    static auto operator()(ENode<symbol> const & /* current */, std::span<CostResult const> children) -> CostResult {
      return 1.0 + std::ranges::fold_left(children, 0.0, [](double acc, CostResult const &val) { return acc + val; });
    }
  };

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

  std::unordered_map<EClassId, Selection<CostModel::CostResult>> cheapest_enode;

  // This should either:
  // 1. Return infinity/max double if all nodes are cyclic
  // 2. Select the original X node if it still exists in the e-class
  auto cost = ProcessCosts(egraph, CostModel{}, fully_cyclic, cheapest_enode);

  // After merges, the original x_node should still be selectable
  // It has cost 1 (no children), while the 'ADD' nodes have infinite cost
  ASSERT_EQ(cost, 1.0);
  ASSERT_EQ(cheapest_enode[fully_cyclic].enode_id, x_node);
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

// ========================================
// Extract_TopologicalSort Tests
// ========================================

TEST(Extract_TopologicalSort, SingleNode) {
  auto egraph = EGraph<symbol, analysis>{};
  auto [leaf_class, leaf_node, leaf_new] = egraph.emplace(symbol::A);
  std::unordered_map<EClassId, Selection<double>> cheapest_enode{};
  cheapest_enode[leaf_class] = {leaf_node, 1.0};
  auto in_degree = std::unordered_map<EClassId, int>{{leaf_class, 0}};
  auto result = TopologicalSort(egraph, cheapest_enode, in_degree);

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
  std::unordered_map<EClassId, int> in_degree;
  in_degree[root_class] = 0;
  in_degree[mid_class] = 1;
  in_degree[leaf_class] = 1;
  auto result = TopologicalSort(egraph, cheapest_enode, in_degree);

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
  std::unordered_map<EClassId, int> in_degree;
  in_degree[left_class] = 1;
  in_degree[right_class] = 1;
  in_degree[root_class] = 0;
  auto result = TopologicalSort(egraph, cheapest_enode, in_degree);

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
  std::unordered_map<EClassId, int> in_degree;
  in_degree[left_class] = 1;
  in_degree[right_class] = 1;
  in_degree[shared_class] = 2;
  in_degree[root_class] = 0;
  auto result = TopologicalSort(egraph, cheapest_enode, in_degree);

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

struct TestDemandCostResult {
  std::vector<TestDemandAlt> alts;

  auto min_cost() const -> double {
    const auto it = std::ranges::min_element(alts, {}, &TestDemandAlt::cost);
    return it != alts.end() ? it->cost : std::numeric_limits<double>::infinity();
  }

  void prune() {
    std::erase_if(alts, [this](TestDemandAlt const &a) {
      return std::ranges::any_of(alts, [&a](TestDemandAlt const &b) { return &a != &b && a.dominated_by(b); });
    });
  }

  static auto merge(TestDemandCostResult const &a, TestDemandCostResult const &b) -> TestDemandCostResult {
    auto result = TestDemandCostResult{};
    result.alts.insert(result.alts.end(), a.alts.begin(), a.alts.end());
    result.alts.insert(result.alts.end(), b.alts.begin(), b.alts.end());
    result.prune();
    return result;
  }
};

// Simple multi-alt cost model: each enode produces a single alternative with cost=1,
// required={} and the enode_id. This should behave identically to single-best extraction.
struct SimpleMultiAltCostModel {
  using cost_model_tag = pareto_frontier_tag;
  using CostResult = TestDemandCostResult;

  static auto operator()(ENode<symbol> const & /*current*/, ENodeId enode_id, std::span<CostResult const> children)
      -> CostResult {
    auto child_cost =
        std::ranges::fold_left(children, 0.0, [](double acc, CostResult const &c) { return acc + c.min_cost(); });
    return CostResult{{{.cost = 1.0 + child_cost, .required = {}, .enode_id = enode_id}}};
  }

  static auto min_cost(CostResult const &r) -> double { return r.min_cost(); }

  static auto resolve(CostResult const &frontier) -> ENodeId {
    return std::ranges::min_element(frontier.alts, {}, &TestDemandAlt::cost)->enode_id;
  }
};

// Multi-alt cost model where symbol A produces two alternatives:
// one with empty required (cost=2) and one with required={1} (cost=1).
// This tests Pareto frontier merging and resolution.
struct DemandAwareMultiAltCostModel {
  using cost_model_tag = pareto_frontier_tag;
  using CostResult = TestDemandCostResult;

  static auto operator()(ENode<symbol> const &current, ENodeId enode_id, std::span<CostResult const> children)
      -> CostResult {
    auto child_cost =
        std::ranges::fold_left(children, 0.0, [](double acc, CostResult const &c) { return acc + c.min_cost(); });

    if (current.symbol() == symbol::A) {
      // A produces two alternatives: cheap-with-demand and expensive-without
      return CostResult{{
          {.cost = 1.0 + child_cost, .required = {1}, .enode_id = enode_id},
          {.cost = 2.0 + child_cost, .required = {}, .enode_id = enode_id},
      }};
    }
    // Other symbols: single alternative, no demand
    return CostResult{{{.cost = 1.0 + child_cost, .required = {}, .enode_id = enode_id}}};
  }

  static auto min_cost(CostResult const &r) -> double { return r.min_cost(); }

  static auto resolve(CostResult const &frontier) -> ENodeId {
    return std::ranges::min_element(frontier.alts, {}, &TestDemandAlt::cost)->enode_id;
  }
};

// Verify tag classification and concept satisfaction
static_assert(std::is_same_v<cost_model_tag_t<SimpleMultiAltCostModel>, pareto_frontier_tag>);
static_assert(std::is_same_v<cost_model_tag_t<DemandAwareMultiAltCostModel>, pareto_frontier_tag>);
static_assert(std::is_same_v<cost_model_tag_t<UniformCostModel>, single_best_tag>);
static_assert(std::is_same_v<cost_model_tag_t<SymbolCostModel>, single_best_tag>);
static_assert(ParetoCostModel<SimpleMultiAltCostModel>);
static_assert(ParetoCostModel<DemandAwareMultiAltCostModel>);

}  // namespace

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
  ASSERT_TRUE(it->second.frontier.has_value());
  auto const &frontier = *it->second.frontier;
  // After pruning, should have at most 2 non-dominated alternatives
  ASSERT_LE(frontier.alts.size(), 2);
  // Min cost should be 1.0
  ASSERT_DOUBLE_EQ(frontier.min_cost(), 1.0);
}

TEST(Extract_MultiAlt, ProcessCosts_CyclicEClass) {
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
