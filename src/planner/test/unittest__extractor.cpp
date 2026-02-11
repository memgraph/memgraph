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

#include "planner/egraph/egraph.hpp"
#include "planner/extract/extractor.hpp"

#include <limits>

import memgraph.planner.core.eids;

using namespace memgraph::planner::core;

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

TEST(Extractor, Thing) {
  struct CostModel {
    using CostResult = double;

    static auto operator()(ENode<symbol> const & /* current */, std::span<CostResult const> children) -> CostResult {
      return 1.0 + std::ranges::fold_left(children, 0.0, [](double acc, CostResult const &val) { return acc + val; });
    }
  };

  auto egraph = EGraph<symbol, analysis>{};
  auto [root_class, root_enode, root_new] = egraph.emplace(symbol::A);
  auto extractor = Extractor{egraph, CostModel{}};
  auto extracted = extractor.Extract(root_class);
  ASSERT_EQ(extracted.size(), 1);
  ASSERT_EQ(extracted[0].first, root_class);
  ASSERT_EQ(extracted[0].second, root_enode);
}

TEST(Extractor, CheapestRootSelected) {
  struct CostModel {
    using CostResult = double;

    static auto operator()(ENode<symbol> const &current, std::span<CostResult const> children) -> CostResult {
      return (current.symbol() == symbol::A ? 1.0 : 2.0) +
             std::ranges::fold_left(children, 0.0, [](double acc, CostResult const &val) { return acc + val; });
    }
  };

  auto egraph = EGraph<symbol, analysis>{};
  auto [aclass, anode, a_new] = egraph.emplace(symbol::A);
  auto [bclass, bnode, b_new] = egraph.emplace(symbol::B);
  auto [root, _] = egraph.merge(aclass, bclass);
  auto ctx = ProcessingContext<symbol>{};
  egraph.rebuild(ctx);
  auto extractor = Extractor{egraph, CostModel{}};
  auto extracted = extractor.Extract(root);
  ASSERT_EQ(extracted.size(), 1);
  ASSERT_EQ(extracted[0].first, root);
  ASSERT_EQ(extracted[0].second, anode);
}

// ========================================
// ProcessEGraph Tests
// ========================================

TEST(ProcessEGraph, SingleLeafNode) {
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

TEST(ProcessEGraph, SimpleTree) {
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

TEST(ProcessEGraph, DeepTree) {
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

TEST(ProcessEGraph, DiamondDAGSharedNode) {
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

TEST(ProcessEGraph, VariableCostBySymbol) {
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

TEST(ProcessEGraph, SelectsCheapestAmongEquivalents) {
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

TEST(ProcessEGraph, CostAccumulationWithVariableCosts) {
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

TEST(ProcessEGraph, CyclicEGraphInfiniteCost) {
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

TEST(ProcessEGraph, CyclicEGraphInfiniteCostComplex) {
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

TEST(ProcessEGraph, FullyCyclicEClassInfiniteCost) {
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
// CollectDependencies Tests
// ========================================

TEST(CollectDependencies, SingleLeafNode) {
  auto egraph = EGraph<symbol, analysis>{};
  auto [leaf_class, leaf_node, leaf_new] = egraph.emplace(symbol::A);
  std::unordered_map<EClassId, Selection<double>> cheapest_enode;
  cheapest_enode[leaf_class] = {leaf_node, 1.0};

  auto in_degree = CollectDependencies(egraph, cheapest_enode, leaf_class);

  // Leaf has no children, so in_degree should be empty
  ASSERT_EQ(in_degree.size(), 1);
}

TEST(CollectDependencies, LinearChain) {
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

TEST(CollectDependencies, SimpleTree) {
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

TEST(CollectDependencies, DiamondDAG) {
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
// TopologicalSort Tests
// ========================================

TEST(TopologicalSort, SingleNode) {
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

TEST(TopologicalSort, LinearChainOrdering) {
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

TEST(TopologicalSort, SimpleTreeOrdering) {
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

TEST(TopologicalSort, DiamondTopology) {
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

TEST(Extractor, IntegrationComplexTree) {
  struct CostModel {
    using CostResult = double;

    static auto operator()(ENode<symbol> const & /* current */, std::span<CostResult const> children) -> CostResult {
      return 1.0 + std::ranges::fold_left(children, 0.0, [](double acc, CostResult const &val) { return acc + val; });
    }
  };

  auto egraph = EGraph<symbol, analysis>{};
  auto [l1_class, l1_node, l1_new] = egraph.emplace(symbol::A);
  auto [l2_class, l2_node, l2_new] = egraph.emplace(symbol::B);
  auto [mid_class, mid_node, mid_new] = egraph.emplace(symbol::A, {l1_class, l2_class});
  auto [root_class, root_node, root_new] = egraph.emplace(symbol::B, {mid_class});

  auto extractor = Extractor{egraph, CostModel{}};
  auto extracted = extractor.Extract(root_class);

  ASSERT_EQ(extracted.size(), 4);
  // Verify root is first
  ASSERT_EQ(extracted[0].first, root_class);
  ASSERT_EQ(extracted[1].first, mid_class);
}

TEST(Extractor, IntegrationDiamondDAG) {
  struct CostModel {
    using CostResult = double;

    static auto operator()(ENode<symbol> const & /* current */, std::span<CostResult const> children) -> CostResult {
      return 1.0 + std::ranges::fold_left(children, 0.0, [](double acc, CostResult const &val) { return acc + val; });
    }
  };

  auto egraph = EGraph<symbol, analysis>{};
  auto [shared_class, shared_node, shared_new] = egraph.emplace(symbol::A);
  auto [left_class, left_node, left_new] = egraph.emplace(symbol::B, {shared_class}, 1);     // disambiguator = 1
  auto [right_class, right_node, right_new] = egraph.emplace(symbol::B, {shared_class}, 2);  // disambiguator = 2
  auto [root_class, root_node, root_new] = egraph.emplace(symbol::A, {left_class, right_class});

  auto extractor = Extractor{egraph, CostModel{}};
  auto extracted = extractor.Extract(root_class);

  ASSERT_EQ(extracted.size(), 4);
  // Verify shared node comes last
  ASSERT_EQ(extracted[0].first, root_class);
  ASSERT_EQ(extracted[3].first, shared_class);
}

TEST(Extractor, IntegrationNestedEquivalence) {
  struct CostModel {
    using CostResult = double;

    static auto operator()(ENode<symbol> const &current, std::span<CostResult const> children) -> CostResult {
      auto base_cost = (current.symbol() == symbol::A ? 1.0 : 5.0);
      return base_cost +
             std::ranges::fold_left(children, 0.0, [](double acc, CostResult const &val) { return acc + val; });
    }
  };

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

  auto extractor = Extractor{egraph, CostModel{}};
  auto extracted = extractor.Extract(root);

  ASSERT_EQ(extracted.size(), 2);
  // Should select cheaper A nodes at both levels
  ASSERT_EQ(extracted[0].second, a2_node);
  ASSERT_EQ(extracted[1].second, a1_node);
}
