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

// Tests for the extraction algorithm: cost computation (ComputeFrontiers),
// cycle handling, frontier safety, Pareto prune/merge, and demand-driven
// multi-alt DAG resolution.
//
// Inclusion criterion: each test pins one distinct extraction invariant.  We do
// not re-derive an invariant already covered by another test at a different
// altitude (e.g. wide-vs-deep cost trees, or the same cycle handled by both the
// single-best and multi-alt cost-result types).

#include <gtest/gtest.h>

#include <algorithm>
#include <set>

#include "planner/extract/extractor.hpp"
#include "test_support/extract.hpp"

import memgraph.planner.core.egraph;

using namespace memgraph::planner::core;
using namespace memgraph::planner::core::extract;
using memgraph::planner::test_support::DefaultCostResult;
using memgraph::planner::test_support::DefaultResolver;

enum struct symbol : std::uint8_t { A, B, ADD, LITERAL };

// Dummy analysis: carries no facts, its no-op merge satisfies the e-graph.
struct analysis {
  void merge(analysis const & /*other*/) {}
};

// Simple cost models using DefaultCostResult<double>.
// All models use the unified signature: (enode, enode_id, span<CostResult>) -> CostResult.
struct UniformCostModel {
  using CostResult = DefaultCostResult<double>;

  static auto operator()(ENode<symbol> const & /*current*/, ENodeId enode_id,
                         std::span<CostResult const *const> children) -> CostResult {
    auto child_sum = std::ranges::fold_left(
        children, 0.0, [](double acc, CostResult const *c) { return acc + c->resolve().second; });
    return CostResult{1.0 + child_sum, enode_id};
  }
};

struct SymbolCostModel {
  using CostResult = DefaultCostResult<double>;
  double a_cost;
  double b_cost;

  auto operator()(ENode<symbol> const &current, ENodeId enode_id, std::span<CostResult const *const> children) const
      -> CostResult {
    auto child_sum = std::ranges::fold_left(
        children, 0.0, [](double acc, CostResult const *c) { return acc + c->resolve().second; });
    return CostResult{(current.symbol() == symbol::A ? a_cost : b_cost) + child_sum, enode_id};
  }
};

// Test helper: runs the full extraction pipeline (ComputeFrontiers + DefaultResolver).
// Returns entries in children-before-parents order.
template <typename CostModel>
auto TestExtract(EGraph<symbol, analysis> const &egraph, CostModel const &cost_model, EClassId root)
    -> std::vector<std::pair<EClassId, ENodeId>> {
  extract::FrontierContext<typename CostModel::CostResult> frontier_ctx;
  std::vector<std::pair<EClassId, ENodeId>> out;
  (void)extract::ComputeFrontiers(egraph, cost_model, root, frontier_ctx);
  DefaultResolver{}(egraph, frontier_ctx.frontier_map, root, out);
  return out;
}

TEST(Extract_Basic, CheapestRootSelected) {
  auto egraph = EGraph<symbol, analysis>{};
  auto [aclass, anode, a_new] = egraph.emplace(symbol::A);
  auto [bclass, bnode, b_new] = egraph.emplace(symbol::B);
  auto [root, _] = egraph.merge(aclass, bclass);
  auto ctx = ProcessingContext<symbol>{};
  egraph.rebuild(ctx);
  auto extracted = TestExtract(egraph, SymbolCostModel{1.0, 2.0}, root);
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

  auto operator()(ENode<symbol> const &enode, ENodeId enode_id, std::span<CostResult const *const> children) const
      -> CostResult {
    auto child_sum = std::ranges::fold_left(
        children, 0.0, [](double acc, CostResult const *c) { return acc + c->resolve().second; });
    return CostResult{fn(enode) + child_sum, enode_id};
  }
};

// Convenience alias: FrontierMap keyed by CostModel rather than CostResult.
template <typename CostModel>
using TestFrontierContext = extract::FrontierContext<typename CostModel::CostResult>;

template <typename CostResult>
auto FrontierCost(FrontierMap<CostResult> const &m, EClassId id) {
  return m.at(id)->resolve().second;
}

template <typename CostResult>
auto FrontierEnode(FrontierMap<CostResult> const &m, EClassId id) {
  return m.at(id)->resolve().first;
}

TEST(Extract_Cost, SingleLeafNode) {
  auto cost_model = SimpleCostModel{[](ENode<symbol> const &) { return 5.0; }};
  using CostModel = decltype(cost_model);

  auto egraph = EGraph<symbol, analysis>{};
  auto [leaf_class, leaf_node, leaf_new] = egraph.emplace(symbol::A);

  TestFrontierContext<CostModel> frontiers;
  auto cost = extract::ComputeFrontiers(egraph, cost_model, leaf_class, frontiers);

  ASSERT_TRUE(cost != nullptr);
  ASSERT_EQ(cost->cost, 5.0);
  ASSERT_EQ(frontiers.frontier_map.size(), 1);
  ASSERT_EQ(FrontierEnode(frontiers.frontier_map, leaf_class), leaf_node);
  ASSERT_EQ(FrontierCost(frontiers.frontier_map, leaf_class), 5.0);
}

TEST(Extract_Cost, DiamondDAGSharedNode) {
  auto cost_model = SimpleCostModel{[](ENode<symbol> const &) { return 1.0; }};
  using CostModel = decltype(cost_model);

  auto egraph = EGraph<symbol, analysis>{};
  auto [shared_class, shared_node, shared_new] = egraph.emplace(symbol::A);
  auto [left_class, left_node, left_new] = egraph.emplace(symbol::B, {shared_class}, 1);     // disambiguator = 1
  auto [right_class, right_node, right_new] = egraph.emplace(symbol::B, {shared_class}, 2);  // disambiguator = 2
  auto [root_class, root_node, root_new] = egraph.emplace(symbol::A, {left_class, right_class});

  TestFrontierContext<CostModel> frontiers;
  auto cost = extract::ComputeFrontiers(egraph, cost_model, root_class, frontiers);

  // Shared node is computed once via memoization; per-eclass costs asserted below.
  ASSERT_TRUE(cost != nullptr);
  ASSERT_EQ(cost->cost, 5.0);
  ASSERT_EQ(frontiers.frontier_map.size(), 4);
  ASSERT_EQ(FrontierCost(frontiers.frontier_map, shared_class), 1);
  ASSERT_EQ(FrontierCost(frontiers.frontier_map, left_class), 2);
  ASSERT_EQ(FrontierCost(frontiers.frontier_map, right_class), 2);
  ASSERT_EQ(FrontierCost(frontiers.frontier_map, root_class), 5);
}

TEST(Extract_Cost, CostAccumulationWithVariableCosts) {
  auto cost_model = SimpleCostModel{[](ENode<symbol> const &e) { return e.symbol() == symbol::A ? 2.0 : 3.0; }};
  using CostModel = decltype(cost_model);

  auto egraph = EGraph<symbol, analysis>{};
  auto [leaf1_class, leaf1_node, leaf1_new] = egraph.emplace(symbol::A);
  auto [leaf2_class, leaf2_node, leaf2_new] = egraph.emplace(symbol::B);
  auto [root_class, root_node, root_new] = egraph.emplace(symbol::A, {leaf1_class, leaf2_class});

  TestFrontierContext<CostModel> frontiers;

  auto cost = extract::ComputeFrontiers(egraph, cost_model, root_class, frontiers);

  // Cost: 2 (root, symbol A) + 2 (leaf1, symbol A) + 3 (leaf2, symbol B) = 7
  ASSERT_TRUE(cost != nullptr);
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

  TestFrontierContext<CostModel> frontiers;

  // Process the cyclic e-class
  auto cost = extract::ComputeFrontiers(egraph, cost_model, cyclic_class, frontiers);

  // The cyclic 'ADD' node should have infinite cost (or very large)
  // The non-cyclic LITERAL node should be selected with cost 1
  ASSERT_TRUE(cost != nullptr);
  ASSERT_EQ(cost->cost, 1.0);
  ASSERT_EQ(FrontierEnode(frontiers.frontier_map, cyclic_class), x_node);
  ASSERT_EQ(FrontierCost(frontiers.frontier_map, cyclic_class), 1.0);
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

  TestFrontierContext<CostModel> frontiers;

  // This should either:
  // 1. Return infinity/max double if all nodes are cyclic
  // 2. Select the original X node if it still exists in the e-class
  auto cost = extract::ComputeFrontiers(egraph, cost_model, fully_cyclic, frontiers);

  // After merges, the original x_node should still be selectable
  // It has cost 1 (no children), while the 'ADD' nodes have infinite cost
  ASSERT_TRUE(cost != nullptr);
  ASSERT_EQ(cost->cost, 1.0);
  ASSERT_EQ(FrontierEnode(frontiers.frontier_map, fully_cyclic), x_node);
}

// ========================================
// Integration Tests (Full Extract Pipeline)
// ========================================

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

  auto extracted = TestExtract(egraph, SymbolCostModel{1.0, 5.0}, root);

  ASSERT_EQ(extracted.size(), 2);
  ASSERT_EQ(extracted[0].second, a1_node);
  ASSERT_EQ(extracted[1].second, a2_node);
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

  TestFrontierContext<UniformCostModel> frontiers;
  auto cost = extract::ComputeFrontiers(egraph, UniformCostModel{}, merged_class, frontiers);

  // merged_class has LITERAL(1) as a leaf escape - should succeed
  ASSERT_TRUE(cost != nullptr);
  ASSERT_EQ(cost->cost, 1.0);  // LITERAL leaf cost

  // y_class is fully cyclic (its only enode A(merged_class) is a cycle)
  // ComputeFrontiers should have erased its sentinel - NOT present in frontier_map
  EXPECT_FALSE(frontiers.frontier_map.contains(y_class)) << "Fully cyclic y_class should not be in frontier_map";

  // merged_class and zero_class should be present
  EXPECT_TRUE(frontiers.frontier_map.contains(merged_class));
  EXPECT_TRUE(frontiers.frontier_map.contains(zero_class));

  // The selected enode for merged_class should be the leaf (x_node), not the cyclic ADD
  EXPECT_EQ(FrontierEnode(frontiers.frontier_map, merged_class), x_node);
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

  TestFrontierContext<UniformCostModel> frontiers;
  auto cost = extract::ComputeFrontiers(egraph, UniformCostModel{}, merged, frontiers);

  // merged has A(leaf_d) as non-cyclic escape
  ASSERT_TRUE(cost != nullptr);
  ASSERT_EQ(cost->cost, 2.0);  // 1 (A) + 1 (leaf_d)

  // Key: leaf_c should be cached even though it was encountered while processing
  // the cyclic ADD enode. The "continue processing remaining children" logic
  // ensures non-cyclic siblings are still computed.
  EXPECT_TRUE(frontiers.frontier_map.contains(leaf_c_class))
      << "Non-cyclic sibling leaf_c should be cached in frontier_map";
  EXPECT_EQ(FrontierCost(frontiers.frontier_map, leaf_c_class), 1.0);

  // leaf_d should also be cached (child of the non-cyclic A enode)
  EXPECT_TRUE(frontiers.frontier_map.contains(leaf_d_class));
  EXPECT_EQ(FrontierCost(frontiers.frontier_map, leaf_d_class), 1.0);

  // merged should be present with the A enode selected (not the cyclic ADD)
  EXPECT_TRUE(frontiers.frontier_map.contains(merged));
  EXPECT_EQ(FrontierEnode(frontiers.frontier_map, merged), b_node);
}

TEST(Extract_Safety, ComputeFrontiers_CyclicExprChildOfBind) {
  // Simulates a Bind-like enode (symbol::A with 3 children [input, sym, expr])
  // where the expr child's eclass contains a cyclic ADD enode alongside a non-cyclic
  // LITERAL escape. The has_cyclic_child guard should skip the Bind enode entirely,
  // but input and sym (processed before expr) must still be cached.
  auto egraph = EGraph<symbol, analysis>{};
  auto [input_class, input_node, input_new] = egraph.emplace(symbol::B);
  auto [sym_class, sym_node, sym_new] = egraph.emplace(symbol::B, {}, 1);
  auto [leaf_class, leaf_node, leaf_new] = egraph.emplace(symbol::LITERAL, 0);
  auto [expr_class, expr_node, expr_new] = egraph.emplace(symbol::LITERAL, 99);
  auto [add_class, add_node, add_new] = egraph.emplace(symbol::ADD, {expr_class, leaf_class});
  // Merge expr with add to create a cycle in expr_class
  auto [cyclic_expr, _] = egraph.merge(expr_class, add_class);
  auto ctx = ProcessingContext<symbol>{};
  egraph.rebuild(ctx);

  // Bind-like: symbol A with 3 children [input, sym, cyclic_expr]
  auto [bind_class, bind_node, bind_new] = egraph.emplace(symbol::A, {input_class, sym_class, cyclic_expr});

  TestFrontierContext<UniformCostModel> frontiers;
  auto cost = extract::ComputeFrontiers(egraph, UniformCostModel{}, bind_class, frontiers);

  // cyclic_expr has LITERAL(99) as an escape - it resolves, so the Bind enode is NOT
  // skipped. The Bind enode's cost = 1 (self) + 1 (input) + 1 (sym) + 1 (cyclic_expr LITERAL) = 4.
  ASSERT_TRUE(cost != nullptr);
  ASSERT_EQ(cost->cost, 4.0);

  // All children must be cached
  EXPECT_TRUE(frontiers.frontier_map.contains(input_class));
  EXPECT_TRUE(frontiers.frontier_map.contains(sym_class));
  EXPECT_TRUE(frontiers.frontier_map.contains(cyclic_expr)) << "cyclic_expr has a LITERAL escape path - must be cached";
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
};

/// TestDemandAlt's Pareto dims: lower cost, smaller required-set.
using TestDim_Cost = extract::Dim<&TestDemandAlt::cost, extract::LowerIsBetter>;
using TestDim_Required = extract::Dim<&TestDemandAlt::required, extract::SmallerSubsetIsBetter>;

/// TestFrontier: ParetoFrontier with resolve/min_cost for the extraction contract.
struct TestFrontier : CostResultBase<TestDemandAlt, TestDim_Cost, TestDim_Required> {
  using CostResultBase::CostResultBase;
};

// TestDemandAlt has public mutable fields and so satisfies the LazyMap
// refinement.  An alt with a const `enode_id` data member still satisfies
// ParetoAlt (only the read expression is required) but fails MutableParetoAlt
// because the member cannot be assigned to - the named diagnostic LazyMap
// surfaces at instantiation.
struct ReadOnlyEnodeIdAlt {
  struct EnodeIdHolder {
    ENodeId value{};
    auto operator=(ENodeId) -> EnodeIdHolder & = delete;
  };

  double cost;
  EnodeIdHolder enode_id;
};

static_assert(extract::ParetoAlt<TestDemandAlt>);
static_assert(extract::MutableParetoAlt<TestDemandAlt, ENodeId>);
static_assert(extract::ParetoAlt<ReadOnlyEnodeIdAlt>);
static_assert(!extract::MutableParetoAlt<ReadOnlyEnodeIdAlt, ENodeId>);

// Simple multi-alt cost model: each enode produces a single alternative with cost=1,
// required={} and the enode_id. This should behave identically to single-best extraction.
// resolve/min_cost live on TestFrontier (the CostResult type).
// These models only need to provide operator().
struct SimpleMultiAltCostModel {
  using CostResult = TestFrontier;

  static auto operator()(ENode<symbol> const & /*current*/, ENodeId enode_id,
                         std::span<CostResult const *const> children) -> CostResult {
    auto child_cost = std::ranges::fold_left(
        children, 0.0, [](double acc, CostResult const *c) { return acc + c->resolve().second; });
    return CostResult{{{.cost = 1.0 + child_cost, .required = {}, .enode_id = enode_id}}};
  }
};

struct DemandAwareMultiAltCostModel {
  using CostResult = TestFrontier;

  static auto operator()(ENode<symbol> const &current, ENodeId enode_id, std::span<CostResult const *const> children)
      -> CostResult {
    auto child_cost = std::ranges::fold_left(
        children, 0.0, [](double acc, CostResult const *c) { return acc + c->resolve().second; });
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

TEST(ParetoFrontier_Prune, NoneDominated) {
  // Three Pareto-incomparable alts: lower cost always requires more demands.
  auto frontier = TestFrontier{{
      {.cost = 1.0, .required = {1, 2}, .enode_id = ENodeId{0}},  // cheapest, most demands
      {.cost = 2.0, .required = {1}, .enode_id = ENodeId{1}},     // medium
      {.cost = 3.0, .required = {}, .enode_id = ENodeId{2}},      // expensive, no demands
  }};
  ASSERT_EQ(frontier.alts().size(), 3) << "All three Pareto-incomparable alts should survive";
}

TEST(ParetoFrontier_Prune, DuplicateAlternatives) {
  // When two alts are Pareto-equivalent (identical cost and required), exactly
  // one survives prune.  Which one is implementation-defined.
  auto frontier = TestFrontier{{
      {.cost = 2.0, .required = {1}, .enode_id = ENodeId{0}},
      {.cost = 2.0, .required = {1}, .enode_id = ENodeId{1}},
  }};
  ASSERT_EQ(frontier.alts().size(), 1);
  EXPECT_TRUE(frontier.alts()[0].enode_id == ENodeId{0} || frontier.alts()[0].enode_id == ENodeId{1});
}

TEST(ParetoFrontier_Prune, TransitiveDominance_AllPermutations) {
  // A={cost=1,req={}}, B={cost=2,req={1}}, C={cost=3,req={1,2}}
  // A dominates both B and C. After prune, only A survives regardless of input order.
  using Alt = TestDemandAlt;
  Alt A = {.cost = 1.0, .required = {}, .enode_id = ENodeId{0}};
  Alt B = {.cost = 2.0, .required = {1}, .enode_id = ENodeId{1}};
  Alt C = {.cost = 3.0, .required = {1, 2}, .enode_id = ENodeId{2}};

  std::array<Alt, 3> elems{A, B, C};
  std::array<std::array<int, 3>, 6> perms{{{0, 1, 2}, {0, 2, 1}, {1, 0, 2}, {1, 2, 0}, {2, 0, 1}, {2, 1, 0}}};
  for (auto const &perm : perms) {
    auto frontier = TestFrontier{{elems[perm[0]], elems[perm[1]], elems[perm[2]]}};
    ASSERT_EQ(frontier.alts().size(), 1) << "permutation " << perm[0] << perm[1] << perm[2];
    ASSERT_EQ(frontier.alts()[0].enode_id, ENodeId{0}) << "Only A (cost=1, req={}) should survive";
  }
}

// ========================================
// ParetoFrontier::merge_in_place Tests
// ========================================

TEST(ParetoFrontier_MergeInPlace, CrossDominatedElementsRemoved) {
  // PF_A has one cheap self-contained alt.  PF_B has two alts, both dominated
  // by PF_A's alt.  After merge into PF_A, only PF_A's original alt survives.
  auto pf_a = TestFrontier{{{.cost = 1.0, .required = {}, .enode_id = ENodeId{0}}}};
  auto pf_b = TestFrontier{{
      {.cost = 3.0, .required = {}, .enode_id = ENodeId{1}},   // dominated: req same, cost worse
      {.cost = 2.0, .required = {1}, .enode_id = ENodeId{2}},  // dominated: req bigger, cost worse
  }};
  pf_a.merge_in_place(std::move(pf_b));
  ASSERT_EQ(pf_a.alts().size(), 1);
  EXPECT_EQ(pf_a.alts()[0].enode_id, ENodeId{0});
}

TEST(ParetoFrontier_MergeInPlace, IncomparableSurviveBothSides) {
  // PF_A has a cheap alt with req={1}.  PF_B has a costlier self-contained alt.
  // Neither dominates the other: PF_A's alt is cheaper but has a larger required
  // set; PF_B's alt has smaller required set but is costlier.  Both survive.
  auto pf_a = TestFrontier{{{.cost = 1.0, .required = {1}, .enode_id = ENodeId{0}}}};
  auto pf_b = TestFrontier{{{.cost = 2.0, .required = {}, .enode_id = ENodeId{1}}}};
  pf_a.merge_in_place(std::move(pf_b));
  ASSERT_EQ(pf_a.alts().size(), 2);
}

TEST(ParetoFrontier_MergeInPlace, OtherDominatesPrefix) {
  // PF_B has a better alt than PF_A's.  PF_A's original alt is cross-dominated
  // and must be evicted after the merge.
  auto pf_a = TestFrontier{{{.cost = 5.0, .required = {1}, .enode_id = ENodeId{0}}}};
  auto pf_b = TestFrontier{{{.cost = 1.0, .required = {}, .enode_id = ENodeId{1}}}};
  pf_a.merge_in_place(std::move(pf_b));
  ASSERT_EQ(pf_a.alts().size(), 1);
  EXPECT_EQ(pf_a.alts()[0].enode_id, ENodeId{1});
}

TEST(Extract_MultiAlt, SingleAlternative_BehavesLikeSingleBest) {
  // Multi-alt with a single alternative per enode should produce the same result
  // as single-best extraction
  auto egraph = EGraph<symbol, analysis>{};
  auto [l1_class, l1_node, l1_new] = egraph.emplace(symbol::A);
  auto [l2_class, l2_node, l2_new] = egraph.emplace(symbol::B);
  auto [root_class, root_node, root_new] = egraph.emplace(symbol::A, {l1_class, l2_class});

  auto extracted = TestExtract(egraph, SimpleMultiAltCostModel{}, root_class);

  ASSERT_EQ(extracted.size(), 3);
  ASSERT_EQ(extracted.back().first, root_class);
  ASSERT_EQ(extracted.back().second, root_node);
}

TEST(Extract_MultiAlt, TwoAlternatives_MergeFrontier) {
  // A produces {cost=1,req={1}} and {cost=2,req={}}; B produces {cost=1,req={}}.
  // B's {cost=1,req={}} dominates both of A's alts (cost no worse and a subset
  // required-set), so the merged frontier resolves to B.
  auto egraph = EGraph<symbol, analysis>{};
  auto [aclass, anode, a_new] = egraph.emplace(symbol::A);
  auto [bclass, bnode, b_new] = egraph.emplace(symbol::B);
  auto [root, _] = egraph.merge(aclass, bclass);
  auto ctx = ProcessingContext<symbol>{};
  egraph.rebuild(ctx);

  auto extracted = TestExtract(egraph, DemandAwareMultiAltCostModel{}, root);

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

  auto extracted = TestExtract(egraph, DemandAwareMultiAltCostModel{}, parent_class);

  ASSERT_EQ(extracted.size(), 2);
  ASSERT_EQ(extracted.back().first, parent_class);
  ASSERT_EQ(extracted.back().second, parent_node);
  ASSERT_EQ(extracted[0].second, b_node);
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

  auto extracted = TestExtract(egraph, DemandAwareMultiAltCostModel{}, root_class);

  ASSERT_EQ(extracted.size(), 4);
  ASSERT_EQ(extracted.back().first, root_class);
  ASSERT_EQ(extracted.back().second, root_node);
  ASSERT_EQ(extracted[0].first, shared_class);
  ASSERT_EQ(extracted[0].second, shared_node);

  // Verify shared eclass appears exactly once (not duplicated for each parent)
  auto shared_count = std::ranges::count_if(extracted, [&](auto const &p) { return p.first == shared_class; });
  ASSERT_EQ(shared_count, 1);
}

// A demand-aware eclass reached by two parents must keep BOTH non-dominated
// alternatives in its frontier so a later resolver can pick the one feasible
// for each visiting context.  This pins the production ComputeFrontiers result
// (Pareto merge across parents) that demand-driven resolution depends on.
//
//   Root(B) -> {Left(B,1), Right(B,2)} -> Shared(A) -> Mid(A) -> Leaf(A)
//
// DemandAwareMultiAltCostModel makes every A-node a two-alt frontier
// ({cost,req={1}} and {cost,req={}}), so Shared/Mid/Leaf must each retain two.
TEST(Extract_MultiAlt, DemandAwareFrontierRetainsBothAltsPerLevel) {
  auto egraph = EGraph<symbol, analysis>{};
  auto [leaf_class, leaf_node, leaf_new] = egraph.emplace(symbol::A);
  auto [mid_class, mid_node, mid_new] = egraph.emplace(symbol::A, {leaf_class});
  auto [shared_class, shared_node, shared_new] = egraph.emplace(symbol::A, {mid_class});
  auto [left_class, left_node, left_new] = egraph.emplace(symbol::B, {shared_class}, 1);
  auto [right_class, right_node, right_new] = egraph.emplace(symbol::B, {shared_class}, 2);
  auto [root_class, root_node, root_new] = egraph.emplace(symbol::B, {left_class, right_class});

  FrontierContext<DemandAwareMultiAltCostModel::CostResult> frontier_ctx;
  (void)extract::ComputeFrontiers(egraph, DemandAwareMultiAltCostModel{}, root_class, frontier_ctx);

  EXPECT_EQ(frontier_ctx.frontier_map.at(shared_class)->alts().size(), 2);
  EXPECT_EQ(frontier_ctx.frontier_map.at(mid_class)->alts().size(), 2);
  EXPECT_EQ(frontier_ctx.frontier_map.at(leaf_class)->alts().size(), 2);
}
