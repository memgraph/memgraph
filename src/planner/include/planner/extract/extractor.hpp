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

#include <concepts>
#include <functional>
#include <queue>
#include <unordered_set>
#include <utility>
#include <vector>

#include <cassert>

#include "planner/extract/pareto_frontier.hpp"

import memgraph.planner.core.egraph;

namespace memgraph::planner::core::extract {

template <typename CostResult>
struct Selection {
  ENodeId enode_id;
  std::optional<CostResult> cost_result;
};

// ============================================================================
// CostModelTraits — tag-dispatched defaults for cost models
// ============================================================================

/// Cost model tags
struct single_best_tag {};

struct pareto_frontier_tag {};

/// Resolve the tag:
///   1. Explicit: model declares `using cost_model_tag = ...`
///   2. Implicit: CostResult is a ParetoFrontier → pareto_frontier_tag
///   3. Default:  single_best_tag
template <typename CM, typename = void>
struct cost_model_tag {
  using type = std::conditional_t<is_pareto_frontier_v<typename CM::CostResult>, pareto_frontier_tag, single_best_tag>;
};

template <typename CM>
struct cost_model_tag<CM, std::void_t<typename CM::cost_model_tag>> {
  using type = CM::cost_model_tag;
};

template <typename CM>
using cost_model_tag_t = cost_model_tag<CM>::type;

/// Required interface for pareto_frontier_tag cost models.
template <typename CM>
concept ParetoCostModel = requires(typename CM::CostResult const &a) {
  { CM::resolve(a) } -> std::convertible_to<ENodeId>;
  { CM::min_cost(a) } -> std::totally_ordered;
};

/// Default CostResult wrapper for single_best models.
/// Pairs the model's raw cost (e.g. double) with the enode that produced it.
template <typename T>
struct DefaultCostResult {
  T cost;
  ENodeId enode_id;

  static auto merge(DefaultCostResult const &a, DefaultCostResult const &b) -> DefaultCostResult {
    return a.cost <= b.cost ? a : b;
  }
};

/// Provides defaults for cost model operations, like std::allocator_traits.
/// Dispatches on cost_model_tag_t:
///   single_best_tag      — wraps CostResult, resolve returns enode_id
///   pareto_frontier_tag  — resolve/min_cost delegate to model (concept-checked)
template <typename CostModel, typename Tag = cost_model_tag_t<CostModel>>
struct CostModelTraits;

template <typename CostModel>
struct CostModelTraits<CostModel, single_best_tag> {
  using CostResult = DefaultCostResult<typename CostModel::CostResult>;
  using CostType = CostModel::CostResult;

  static auto min_cost(CostResult const &r) -> CostType { return r.cost; }

  static auto resolve(CostResult const &r) -> ENodeId { return r.enode_id; }

  template <typename ENodeT>
  static auto invoke(CostModel const &cost_model, ENodeT const &enode, ENodeId enode_id,
                     std::vector<CostResult> const &children_frontiers) -> CostResult {
    auto raw_costs = std::vector<CostType>{};
    raw_costs.reserve(children_frontiers.size());
    for (auto const &c : children_frontiers) raw_costs.push_back(c.cost);
    return CostResult{cost_model(enode, std::span<CostType const>{raw_costs}), enode_id};
  }
};

template <typename CostModel>
  requires ParetoCostModel<CostModel>
struct CostModelTraits<CostModel, pareto_frontier_tag> {
  using CostResult = CostModel::CostResult;
  using CostType = decltype(CostModel::min_cost(std::declval<CostResult const &>()));

  static auto min_cost(CostResult const &r) -> CostType { return CostModel::min_cost(r); }

  static auto resolve(CostResult const &r) -> ENodeId { return CostModel::resolve(r); }

  template <typename ENodeT>
  static auto invoke(CostModel const &cost_model, ENodeT const &enode, ENodeId enode_id,
                     std::vector<CostResult> const &children_frontiers) -> CostResult {
    return cost_model(enode, enode_id, children_frontiers);
  }
};

// ============================================================================
// Extraction (traits-based)
// ============================================================================

/// Per-eclass frontier during cost propagation.
/// nullopt means "in progress" (cycle detection).
template <typename CostResult>
using EClassFrontier = std::optional<CostResult>;

/// Bottom-up cost propagation using CostModelTraits.
/// Works with any cost model: single_best keeps the cheapest, pareto_frontier merges.
template <typename Symbol, typename Analysis, typename CostModel>
auto ComputeFrontiers(
    EGraph<Symbol, Analysis> const &egraph, CostModel const &cost_model, EClassId eclass_id,
    std::unordered_map<EClassId, EClassFrontier<typename CostModelTraits<CostModel>::CostResult>> &frontier_map)
    -> std::optional<typename CostModelTraits<CostModel>::CostResult> {
  using Traits = CostModelTraits<CostModel>;
  using CostResult = Traits::CostResult;

  assert(!egraph.needs_rebuild() && "to avoid internal cost of getting canonical looking up we should");

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

    auto enode_frontier = Traits::invoke(cost_model, enode, enode_id, children_frontiers);

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
/// Walks from root, at each eclass picks the best alternative via CostModelTraits::resolve.
template <typename CostModel, typename Symbol, typename Analysis>
auto ResolveSelection(
    EGraph<Symbol, Analysis> const &egraph,
    std::unordered_map<EClassId, EClassFrontier<typename CostModelTraits<CostModel>::CostResult>> const &frontier_map,
    EClassId root) {
  using Traits = CostModelTraits<CostModel>;
  using CostType = Traits::CostType;

  auto resolved = std::unordered_map<EClassId, Selection<CostType>>{};
  auto to_visit = std::vector{root};
  auto visited = std::unordered_set{root};

  while (!to_visit.empty()) {
    auto current = to_visit.back();
    to_visit.pop_back();

    auto it = frontier_map.find(current);
    assert(it != frontier_map.end() && it->second.has_value());

    auto const &frontier = *it->second;
    auto enode_id = Traits::resolve(frontier);
    resolved[current] = Selection<CostType>{enode_id, Traits::min_cost(frontier)};

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
                         std::unordered_map<EClassId, Selection<CostResult>> const &enode_selection,
                         const EClassId root) -> std::unordered_map<EClassId, int> {
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
                     std::unordered_map<EClassId, Selection<CostResult>> const &enode_selection,
                     std::unordered_map<EClassId, int> in_degree) -> std::vector<std::pair<EClassId, ENodeId>> {
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
      if (--in_degree[child] == 0) {
        queue.emplace(child);
      }
    }
  }
  return result;
}

/**
 * @brief Extracts the lowest-cost expression tree from an e-graph
 *
 * The extraction result contains ENodeIds that are valid only as long as no
 * rebuild() is performed on the e-graph. Use get_enode() to access e-node
 * data immediately after extraction.
 */
template <typename Symbol, typename Analysis, typename CostModel>
struct Extractor {
  Extractor(EGraph<Symbol, Analysis> const &egraph, CostModel cost_model)
      : egraph_(egraph), cost_model_(std::move(cost_model)) {}

  /**
   * @brief Extract lowest-cost tree rooted at the given e-class
   *
   * @return Topologically sorted (EClassId, ENodeId) pairs. The ENodeIds are
   *         valid for use with egraph.get_enode() until the next rebuild().
   */
  auto Extract(EClassId const root_id) -> std::vector<std::pair<EClassId, ENodeId>> {
    using TraitsCostResult = CostModelTraits<CostModel>::CostResult;
    auto frontier_map = std::unordered_map<EClassId, EClassFrontier<TraitsCostResult>>{};
    ComputeFrontiers(egraph_, cost_model_, root_id, frontier_map);
    auto resolved = ResolveSelection<CostModel>(egraph_, frontier_map, root_id);
    auto in_degree = CollectDependencies(egraph_, resolved, root_id);
    return TopologicalSort(egraph_, resolved, std::move(in_degree));
  }

 private:
  // NOLINTNEXTLINE(cppcoreguidelines-avoid-const-or-ref-data-members)
  EGraph<Symbol, Analysis> const &egraph_;
  CostModel cost_model_;
};

}  // namespace memgraph::planner::core::extract
