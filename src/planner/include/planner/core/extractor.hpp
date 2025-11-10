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

import memgraph.planner.core.eids;

namespace memgraph::planner::core {

struct Cost {
  ENodeId enode_id;
  double cost;
};

template <typename Symbol, typename Analysis, typename Func>
requires std::is_invocable_r_v < double, Func, ENode<Symbol>
const & > auto ProcessCosts(EGraph<Symbol, Analysis> const &egraph, Func const &cost_function, EClassId eclass_id,
                            std::unordered_map<EClassId, Cost> &cheapest_enode) -> double {
  DMG_ASSERT(!egraph.needs_rebuild(), "to avoid internal cost of getting canonical looking up we should");

  if (const auto it = cheapest_enode.find(eclass_id); it != cheapest_enode.end()) {
    // If cost is infinity, we're currently processing this e-class (cycle detected)
    // If cost is finite, we've already computed it
    return it->second.cost;
  }

  auto const &eclass = egraph.eclass(eclass_id);

  // Mark this e-class as "in progress" with infinity cost to detect cycles
  auto [it2, _] =
      cheapest_enode.emplace(eclass_id, Cost(eclass.representative(), std::numeric_limits<double>::infinity()));

  auto best_node = std::optional<Cost>{};

  for (auto const &enode_id : eclass.nodes()) {
    auto &enode = egraph.get_enode(enode_id);
    auto current_cost =
        std::accumulate(enode.children().begin(), enode.children().end(), cost_function(enode),
                        [&](double acc, EClassId child_eclass_id) {
                          return acc + ProcessCosts(egraph, cost_function, child_eclass_id, cheapest_enode);
                        });

    // ignore enodes who have infinate cost
    if (std::isinf(current_cost)) {
      continue;
    }

    if (!best_node || current_cost < best_node->cost) {
      best_node = Cost{enode_id, current_cost};
    }
  }

  // Update with the actual computed cost
  if (best_node) {
    it2->second = *best_node;
    return best_node->cost;
  }

  // Remove infinate cycle case
  cheapest_enode.erase(it2);
  return std::numeric_limits<double>::infinity();
}

// TODO: can we do everything in a single phase algorithm
//  template <typename Symbol, typename Analysis, typename Func>
//    requires std::is_invocable_r_v<double, Func, ENode<Symbol> const &>
//  auto ExtractMega(EGraph<Symbol, Analysis> const &egraph, Func const &cost_function, EClassId root)
//      -> std::unordered_map<EClassId, Cost> {
//    // start with root in the queue
//    // we want to process each enode in eclass
//    // each enode has children, those eclasses need to be on the queue (make sure only inserted once)
//    // We need a counter for each enode (number of children)
//    // an enode cost can be calculated when all its children have been processed (count is 0)
//    // process cost, then see if it is the smallest cost for the eclass the enode belongs to...if so update
//    // Eclass also has a counter for all the enodes it has. Once a enode has been processed, the eclass counter goes
//    down.
//    // when the elcass counter is 0, then go to all the parents enodes counters are decremented.
//    // we also need to handle infinate loops/cycles in the graph. If....
//  }

template <typename Symbol, typename Analysis>
auto CollectDependencies(EGraph<Symbol, Analysis> const &egraph,
                         std::unordered_map<EClassId, Cost> const &enode_selection, const EClassId root)
    -> std::unordered_map<EClassId, int> {
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

template <typename Symbol, typename Analysis>
auto TopologicalSort(EGraph<Symbol, Analysis> const &egraph, std::unordered_map<EClassId, Cost> const &enode_selection,
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

template <typename Symbol, typename Analysis, typename Func>
requires(std::is_invocable_r_v<double, Func, ENode<Symbol> const &>) struct Extractor {
  template <typename FuncInner>
  requires(std::is_invocable_r_v<double, FuncInner, ENode<Symbol> const &>)
      Extractor(EGraph<Symbol, Analysis> const &egraph, FuncInner &&cost_function)
      : egraph_(egraph), cost_function_(std::forward<FuncInner>(cost_function)) {}

  auto Extract(EClassId const root_id) -> std::vector<std::pair<EClassId, ENodeId>> {
    auto cheapest_enode = std::unordered_map<EClassId, Cost>{};
    ProcessCosts(egraph_, cost_function_, root_id, cheapest_enode);
    auto in_degree = CollectDependencies(egraph_, cheapest_enode, root_id);
    return TopologicalSort(egraph_, cheapest_enode, std::move(in_degree));
  }

 private:
  // NOLINTNEXTLINE(cppcoreguidelines-avoid-const-or-ref-data-members)
  EGraph<Symbol, Analysis> const &egraph_;
  Func cost_function_;
};

template <typename Symbol, typename Analysis, typename Func>
requires std::is_invocable_r_v < double, Func, ENode<Symbol>
const & > Extractor(EGraph<Symbol, Analysis> const &, Func)->Extractor<Symbol, Analysis, Func>;

}  // namespace memgraph::planner::core
