// Copyright 2025 Memgraph Ltd.
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

#include "planner/core/egraph.hpp"
#include "planner/core/enode.hpp"

#include "utils/tag.hpp"

import memgraph.planner.core.eids;

namespace memgraph::planner::core {

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

  DMG_ASSERT(!egraph.needs_rebuild(), "to avoid internal cost of getting canonical looking up we should");

  if (const auto it = enode_selection.find(eclass_id); it != enode_selection.end()) {
    // If cost is nullopt, we're currently processing this e-class (cycle
    // detected) If cost is set, we've already computed it
    return it->second.cost_result;
  }

  auto const &eclass = egraph.eclass(eclass_id);

  // Mark this e-class as "in progress" with infinity cost to detect cycles
  auto [it2, _] = enode_selection.emplace(eclass_id, Selection<CostResult>(eclass.representative(), std::nullopt));

  auto best_node = std::optional<Selection<CostResult>>{};

  for (auto const &enode_id : eclass.nodes()) {
    auto skip = false;
    auto &enode = egraph.get_enode(enode_id);
    auto children_costs = std::vector<CostResult>{};  // TODO: use a context
    for (auto child : enode.children()) {
      auto cost = ProcessCosts(egraph, cost_model, child, enode_selection);
      if (!cost) {
        skip = true;
        // break;
      } else {
        children_costs.emplace_back(*cost);
      }
    }
    if (skip) continue;

    // auto current_cost =
    //     std::accumulate(enode.children().begin(), enode.children().end(),
    //     std::vector<CostResult>,
    //                     [&](CostResult acc, EClassId child_eclass_id) ->
    //                     CostResult {
    //                       return  acc +
    //                     });
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

  // Remove infinate cycle case
  enode_selection.erase(it2);
  return std::nullopt;
}

// TODO: can we do everything in a single phase algorithm
//  template <typename Symbol, typename Analysis, typename Func>
//    requires std::is_invocable_r_v<double, Func, ENode<Symbol> const &>
//  auto ExtractMega(EGraph<Symbol, Analysis> const &egraph, Func const
//  &cost_function, EClassId root)
//      -> std::unordered_map<EClassId, Cost> {
//    // start with root in the queue
//    // we want to process each enode in eclass
//    // each enode has children, those eclasses need to be on the queue (make
//    sure only inserted once)
//    // We need a counter for each enode (number of children)
//    // an enode cost can be calculated when all its children have been
//    processed (count is 0)
//    // process cost, then see if it is the smallest cost for the eclass the
//    enode belongs to...if so update
//    // Eclass also has a counter for all the enodes it has. Once a enode has
//    been processed, the eclass counter goes down.
//    // when the elcass counter is 0, then go to all the parents enodes
//    counters are decremented.
//    // we also need to handle infinate loops/cycles in the graph. If....
//  }

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
    DMG_ASSERT(enode_it != enode_selection.end(), "all reachable EClasses should have selected ENode");

    auto const &enode = egraph.get_enode(enode_it->second.enode_id);
    for (auto child : enode.children()) {
      // Count the in-degree, used for Kahn’s topological sorting
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
    DMG_ASSERT(it != enode_selection.end(), "all reachable EClasses should have selected ENode");

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

template <typename Symbol, typename Analysis, typename CostModel>
struct Extractor {
  Extractor(EGraph<Symbol, Analysis> const &egraph, CostModel &&cost_model)
      : egraph_(egraph), cost_model_(std::forward<CostModel>(cost_model)) {}

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

// template <typename Symbol, typename Analysis, typename CostModel>
// Extractor(EGraph<Symbol, Analysis> const &, CostModel) -> Extractor<Symbol,
// Analysis, CostModel, Func>;

}  // namespace memgraph::planner::core
