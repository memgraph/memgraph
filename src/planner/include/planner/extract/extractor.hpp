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

#pragma once

#include <functional>
#include <queue>
#include <unordered_set>
#include <utility>
#include <vector>

#include <cassert>

#include "planner/extract/pareto_frontier.hpp"

import memgraph.planner.core.egraph;

namespace memgraph::planner::core::extract {

// ============================================================================
// CostResult contract
// ============================================================================
//
// Every cost model defines:
//   using CostResult = ...;
//   operator()(ENode const &, ENodeId, span<CostResult const>) -> CostResult
//
// CostResult must satisfy CostResultType (defined below).  Production users
// derive from CostResultBase (planner/extract/pareto_frontier.hpp).  A simple
// scalar fallback (DefaultCostResult) is available to test code only — see
// src/planner/test/extractor_test_helpers.hpp.

/// CostResult contract — enforced at compile time.
/// Every CostResult type must provide:
///   cost_t                 — the scalar cost type (must be totally_ordered)
///   merge(a, b)            — combine frontiers
///   resolve(r)             — pick the best enode
///   min_cost(r)            — extract the comparable cost (asserts non-empty for frontiers)
///   resolve_with_cost(r)   — paired (enode_id, cost) so callers needing both
///                            avoid scanning the frontier twice
template <typename CR>
concept CostResultType = requires(CR const &a, CR const &b) {
  typename CR::cost_t;
  requires std::totally_ordered<typename CR::cost_t>;
  { CR::merge(a, b) } -> std::convertible_to<CR>;
  { CR::resolve(a) } -> std::convertible_to<ENodeId>;
  { CR::min_cost(a) } -> std::convertible_to<typename CR::cost_t>;
  { CR::resolve_with_cost(a) } -> std::convertible_to<std::pair<ENodeId, typename CR::cost_t>>;
};

/// In-degree map for topological sorting.
using InDegreeMap = std::unordered_map<EClassId, int>;

// ============================================================================
// Extraction pipeline
// ============================================================================
// TODO: frontier_map and other std::unordered_map parameters could be switched
// to boost::unordered_flat_map for better cache locality, but the type change
// propagates through template signatures to all callers.

/// Per-eclass frontier during cost propagation.
/// nullopt means "in progress" (cycle detection).
template <typename CostResult>
using EClassFrontier = std::optional<CostResult>;

/// Selection: one enode chosen per eclass, with its cost.
template <typename CostType>
struct Selection {
  ENodeId enode_id;
  CostType cost;
};

/// Bottom-up cost propagation. Calls cost_model(enode, enode_id, children) for each enode,
/// merges results via CostResult::merge across enodes in the same eclass.
template <typename Symbol, typename Analysis, typename CostModel>
  requires CostResultType<typename CostModel::CostResult>
[[nodiscard]] auto ComputeFrontiers(
    EGraph<Symbol, Analysis> const &egraph, CostModel const &cost_model, EClassId eclass_id,
    std::unordered_map<EClassId, EClassFrontier<typename CostModel::CostResult>> &frontier_map)
    -> std::optional<typename CostModel::CostResult> {
  using CostResult = typename CostModel::CostResult;

  assert(!egraph.needs_rebuild() && "egraph must be rebuilt before extraction");

  if (auto const it = frontier_map.find(eclass_id); it != frontier_map.end()) {
    return it->second;
  }

  auto const &eclass = egraph.eclass(eclass_id);

  // Mark this e-class as "in progress" with nullopt frontier to detect cycles
  frontier_map.emplace(eclass_id, std::nullopt);

  auto merged_frontier = std::optional<CostResult>{};

  auto children_frontiers = std::vector<CostResult>{};
  for (auto const &enode_id : eclass.nodes()) {
    auto const &enode = egraph.get_enode(enode_id);
    children_frontiers.clear();
    auto has_cyclic_child = false;
    for (auto child : enode.children()) {
      auto frontier = ComputeFrontiers(egraph, cost_model, child, frontier_map);
      if (!frontier) {
        has_cyclic_child = true;
        // N.B. intentionally no break — continue processing remaining children
        // so their costs are computed and cached for other extraction paths
      } else {
        children_frontiers.emplace_back(*frontier);
      }
    }
    if (has_cyclic_child) continue;

    auto enode_frontier = cost_model(enode, enode_id, children_frontiers);

    if (!merged_frontier) {
      merged_frontier = std::move(enode_frontier);
    } else {
      merged_frontier = CostResult::merge(*merged_frontier, enode_frontier);
    }
  }

  if (merged_frontier) {
    frontier_map[eclass_id] = *merged_frontier;
    return merged_frontier;
  }

  // All enodes cyclic — remove sentinel
  frontier_map.erase(eclass_id);
  return std::nullopt;
}

/// Generic top-down resolver lives in src/planner/test/extractor_test_helpers.hpp.
/// Production code uses PlanResolver (egraph_converter.cpp), which handles
/// Bind alive/dead decisions by propagating a "provided" SymbolSet top-down.

template <typename Symbol, typename Analysis, typename CostResult>
[[nodiscard]] auto CollectDependencies(EGraph<Symbol, Analysis> const &egraph,
                                       std::unordered_map<EClassId, Selection<CostResult>> const &enode_selection,
                                       EClassId root) -> InDegreeMap {
  auto in_degree = std::unordered_map<EClassId, int>{{root, 0}};
  auto bfs = std::vector{root};
  auto visited = std::unordered_set{root};
  bfs.reserve(enode_selection.size());
  visited.reserve(enode_selection.size());

  // Non-recursive BFS search
  while (!bfs.empty()) {
    auto curr = bfs.back();
    bfs.pop_back();

    auto enode_it = enode_selection.find(curr);
    assert(enode_it != enode_selection.end() && "all reachable EClasses should have selected ENode");

    auto const &enode = egraph.get_enode(enode_it->second.enode_id);
    for (auto child : enode.children()) {
      // Only walk children that were resolved — dead Bind's sym/expr are skipped
      if (!enode_selection.contains(child)) continue;
      // Count the in-degree, used for Kahn's topological sorting
      ++in_degree[child];
      // Only add to BFS if not already added
      if (visited.insert(child).second) {
        bfs.emplace_back(child);
      }
    }
  }
  return in_degree;
}

template <typename Symbol, typename Analysis, typename CostResult>
[[nodiscard]] auto TopologicalSort(EGraph<Symbol, Analysis> const &egraph,
                                   std::unordered_map<EClassId, Selection<CostResult>> const &enode_selection,
                                   InDegreeMap in_degree) -> std::vector<std::pair<EClassId, ENodeId>> {
  auto result = std::vector<std::pair<EClassId, ENodeId>>{};
  result.reserve(in_degree.size());

  auto queue = std::queue<EClassId>{};
  for (auto const &[eclass, degree] : in_degree)
    if (degree == 0) queue.emplace(eclass);

  while (!queue.empty()) {
    auto current = queue.front();
    queue.pop();

    auto it = enode_selection.find(current);
    assert(it != enode_selection.end() && "all reachable EClasses should have selected ENode");

    auto enode_id = it->second.enode_id;
    result.emplace_back(current, enode_id);

    auto const &enode = egraph.get_enode(enode_id);
    for (EClassId child : enode.children()) {
      auto deg_it = in_degree.find(child);
      if (deg_it == in_degree.end()) continue;  // dead Bind child — not in resolved set
      if (--deg_it->second == 0) {
        queue.emplace(child);
      }
    }
  }

  // Post-condition: all nodes must have been emitted. If not, the input contained a cycle,
  // which means an upstream stage (ComputeFrontiers, PlanResolver, or CollectDependencies)
  // admitted a cyclic dependency into the resolved selection — a bug in that stage.
  assert(result.size() == in_degree.size() &&
         "TopologicalSort: cycle detected — resolved selection is not a DAG; "
         "check ComputeFrontiers and PlanResolver for upstream bug");

  return result;
}

}  // namespace memgraph::planner::core::extract
