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
#include <numeric>
#include <queue>
#include <unordered_set>
#include <utility>
#include <vector>

#include <cassert>
#include "utils/tag.hpp"

import memgraph.planner.core.egraph;

namespace memgraph::planner::core::extract {

template <typename CostResult>
struct Selection {
  ENodeId enode_id;
  std::optional<CostResult> cost_result;
};

// TODO: decouple, calculated cost useful for debugging and for making the
// selection. But later phases only need the selection that was made.
template <typename Symbol, typename Analysis, typename CostModel>
auto ProcessCosts(EGraph<Symbol, Analysis> const &egraph, CostModel const &cost_model, EClassId eclass_id,
                  std::unordered_map<EClassId, Selection<typename CostModel::CostResult>> &enode_selection)
    -> std::optional<typename CostModel::CostResult> {
  using CostResult = CostModel::CostResult;

  assert(!egraph.needs_rebuild() && "to avoid internal cost of getting canonical looking up we should");

  if (auto const it = enode_selection.find(eclass_id); it != enode_selection.end()) {
    // If cost is nullopt, we're currently processing this e-class (cycle
    // detected) If cost is set, we've already computed it
    return it->second.cost_result;
  }

  auto const &eclass = egraph.eclass(eclass_id);

  // Mark this e-class as "in progress" with infinity cost to detect cycles
  auto [it2, _] = enode_selection.emplace(eclass_id, Selection<CostResult>(eclass.representative(), std::nullopt));

  auto best_node = std::optional<Selection<CostResult>>{};

  for (auto const &enode_id : eclass.nodes()) {
    auto &enode = egraph.get_enode(enode_id);
    auto children_costs = std::vector<CostResult>{};
    auto has_cyclic_child = false;
    for (auto child : enode.children()) {
      auto cost = ProcessCosts(egraph, cost_model, child, enode_selection);
      if (!cost) {
        has_cyclic_child = true;
        // N.B. intentionally no break — continue processing remaining children
        // so their costs are computed and cached for other extraction paths
      } else {
        children_costs.emplace_back(*cost);
      }
    }
    if (has_cyclic_child) continue;

    auto current_cost = cost_model(enode, children_costs);
    if (!best_node || current_cost < *best_node->cost_result) {
      best_node = Selection<CostResult>{enode_id, current_cost};
    }
  }

  // Update with the actual computed cost
  if (best_node) {
    it2->second = *best_node;
    return best_node->cost_result;
  }

  // Remove infinite cycle case
  enode_selection.erase(it2);
  return std::nullopt;
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
    auto enode_selection = std::unordered_map<EClassId, Selection<typename CostModel::CostResult>>{};
    ProcessCosts(egraph_, cost_model_, root_id, enode_selection);
    auto in_degree = CollectDependencies(egraph_, enode_selection, root_id);
    return TopologicalSort(egraph_, enode_selection, std::move(in_degree));
  }

 private:
  // NOLINTNEXTLINE(cppcoreguidelines-avoid-const-or-ref-data-members)
  EGraph<Symbol, Analysis> const &egraph_;
  CostModel cost_model_;
};

}  // namespace memgraph::planner::core::extract
