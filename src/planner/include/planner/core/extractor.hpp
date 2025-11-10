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

// template <typename Symbol, typename Analysis>
// auto CollectDependencies(EGraph<Symbol, Analysis> const &egraph,
//                          std::unordered_map<EClassId, Cost> const &cheapest_enode, EClassId eclass_id,
//                          std::unordered_map<EClassId, int> &in_degree) -> void {
//   auto child_it = cheapest_enode.find(eclass_id);
//   if (child_it == cheapest_enode.end()) return;
//   auto enode = egraph.get_enode(child_it->second.enode_id);
//   for (auto const &child_eclass_id : enode.children()) {
//     if (!in_degree.contains(child_eclass_id)) {
//       // if not visited yet go visit
//       CollectDependencies(egraph, cheapest_enode, child_eclass_id, in_degree);
//     }
//     ++in_degree[child_eclass_id];
//   }
// }

// Standalone function: Perform topological sort to produce ordered output
template <typename Symbol, typename Analysis>
auto TopologicalSort(EGraph<Symbol, Analysis> const &egraph, std::unordered_map<EClassId, Cost> const &cheapest_enode,
                     std::unordered_map<EClassId, int> in_degree, EClassId eclass_id)
    -> std::vector<std::pair<EClassId, ENodeId>> {
  auto result = std::vector<std::pair<EClassId, ENodeId>>{};
  result.reserve(in_degree.size() + 1);
  // egraph is an acyclic graph so we can use topological sort

  // root should be the only eclass with in_degree 0
  std::queue<EClassId> queue{{eclass_id}};

  while (!queue.empty()) {
    EClassId current_id = queue.front();
    queue.pop();

    auto it = cheapest_enode.find(current_id);
    if (it == cheapest_enode.end()) continue;
    auto enode_id = it->second.enode_id;

    result.emplace_back(current_id, enode_id);

    for (EClassId child_eclass_id : egraph.get_enode(enode_id).children()) {
      --in_degree[child_eclass_id];
      if (in_degree[child_eclass_id] == 0) {
        queue.push(child_eclass_id);
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
    return TopologicalSort(egraph_, cheapest_enode, in_degree, root_id);
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
