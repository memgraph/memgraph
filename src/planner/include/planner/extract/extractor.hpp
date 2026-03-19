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
//   using CostResult = ...; // must satisfy:
//     CostResult::merge(a, b) -> CostResult   — combine frontiers from different enodes
//     CostResult::resolve(r)  -> ENodeId       — pick the best enode from the frontier
//     CostResult::min_cost(r) -> <totally_ordered> — extract comparable cost
//
//   operator()(ENode const &, ENodeId, span<CostResult const>) -> CostResult
//
// DefaultCostResult<T> provides all three for simple scalar cost models.
// ParetoFrontier users provide resolve/min_cost via their CostModel's CostResult.

/// Default CostResult for simple cost models. Wraps a scalar cost with enode metadata.
template <typename T>
struct DefaultCostResult {
  T cost;
  ENodeId enode_id;

  static auto merge(DefaultCostResult const &a, DefaultCostResult const &b) -> DefaultCostResult {
    return a.cost <= b.cost ? a : b;
  }

  static auto resolve(DefaultCostResult const &r) -> ENodeId { return r.enode_id; }

  static auto min_cost(DefaultCostResult const &r) -> T { return r.cost; }
};

/// CostResult contract — enforced at compile time.
/// Every CostResult type must provide merge, resolve, and min_cost as static methods.
template <typename CR>
concept CostResultType = requires(CR const &a, CR const &b) {
  { CR::merge(a, b) } -> std::convertible_to<CR>;
  { CR::resolve(a) } -> std::convertible_to<ENodeId>;
  { CR::min_cost(a) } -> std::totally_ordered;
};

static_assert(CostResultType<DefaultCostResult<double>>);

/// In-degree map for topological sorting.
using InDegreeMap = std::unordered_map<EClassId, int>;

// ============================================================================
// Extraction pipeline
// ============================================================================

/// Per-eclass frontier during cost propagation.
/// nullopt means "in progress" (cycle detection).
template <typename CostResult>
using EClassFrontier = std::optional<CostResult>;

/// Selection: one enode chosen per eclass, with its cost.
template <typename CostType>
struct Selection {
  ENodeId enode_id;
  std::optional<CostType> cost;
};

/// Bottom-up cost propagation. Calls cost_model(enode, enode_id, children) for each enode,
/// merges results via CostResult::merge across enodes in the same eclass.
template <typename Symbol, typename Analysis, typename CostModel>
  requires CostResultType<typename CostModel::CostResult>
auto ComputeFrontiers(EGraph<Symbol, Analysis> const &egraph, CostModel const &cost_model, EClassId eclass_id,
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

/// Top-down resolution of frontiers into a Selection map.
/// Walks from root, picks the best enode at each eclass via CostResult::resolve.
template <typename Symbol, typename Analysis, typename CostResult>
  requires CostResultType<CostResult>
auto ResolveSelection(EGraph<Symbol, Analysis> const &egraph,
                      std::unordered_map<EClassId, EClassFrontier<CostResult>> const &frontier_map, EClassId root) {
  using CostType = decltype(CostResult::min_cost(std::declval<CostResult const &>()));

  auto resolved = std::unordered_map<EClassId, Selection<CostType>>{};
  auto to_visit = std::vector{root};
  auto visited = std::unordered_set{root};

  while (!to_visit.empty()) {
    auto current = to_visit.back();
    to_visit.pop_back();

    auto it = frontier_map.find(current);
    assert(it != frontier_map.end() && it->second.has_value());

    auto const &frontier = *it->second;
    auto enode_id = CostResult::resolve(frontier);
    resolved[current] = Selection<CostType>{enode_id, CostResult::min_cost(frontier)};

    auto const &enode = egraph.get_enode(enode_id);
    for (auto child : enode.children()) {
      if (!visited.contains(child)) {
        to_visit.push_back(child);
        visited.insert(child);
      }
    }
  }

  return resolved;
}

template <typename Symbol, typename Analysis, typename CostResult>
auto CollectDependencies(EGraph<Symbol, Analysis> const &egraph,
                         std::unordered_map<EClassId, Selection<CostResult>> const &enode_selection, EClassId root)
    -> InDegreeMap {
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
      if (!visited.contains(child)) {
        bfs.emplace_back(child);
        visited.insert(child);
      }
    }
  }
  return in_degree;
}

template <typename Symbol, typename Analysis, typename CostResult>
auto TopologicalSort(EGraph<Symbol, Analysis> const &egraph,
                     std::unordered_map<EClassId, Selection<CostResult>> const &enode_selection, InDegreeMap in_degree)
    -> std::vector<std::pair<EClassId, ENodeId>> {
  auto result = std::vector<std::pair<EClassId, ENodeId>>{};
  result.reserve(in_degree.size());

  auto queue = std::queue<EClassId>{};
  for (auto const &p : in_degree)
    if (p.second == 0) queue.emplace(p.first);

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
  return result;
}

/// Extracts the lowest-cost expression tree from an e-graph.
/// CostModel must provide CostResult with merge/resolve/min_cost, and
/// operator()(ENode const &, ENodeId, span<CostResult const>) -> CostResult.
/// ResolverFn customizes top-down resolution (default: ResolveSelection).
template <typename Symbol, typename Analysis, typename CostModel>
struct Extractor {
  Extractor(EGraph<Symbol, Analysis> const &egraph, CostModel cost_model)
      : egraph_(egraph), cost_model_(std::move(cost_model)) {}

  auto Extract(EClassId const root_id) -> std::vector<std::pair<EClassId, ENodeId>> {
    using CostResult = typename CostModel::CostResult;
    auto frontier_map = std::unordered_map<EClassId, EClassFrontier<CostResult>>{};
    ComputeFrontiers(egraph_, cost_model_, root_id, frontier_map);
    auto resolved = ResolveSelection<Symbol, Analysis, CostResult>(egraph_, frontier_map, root_id);
    auto in_degree = CollectDependencies(egraph_, resolved, root_id);
    return TopologicalSort(egraph_, resolved, std::move(in_degree));
  }

 private:
  // NOLINTNEXTLINE(cppcoreguidelines-avoid-const-or-ref-data-members)
  EGraph<Symbol, Analysis> const &egraph_;
  CostModel cost_model_;
};

}  // namespace memgraph::planner::core::extract
