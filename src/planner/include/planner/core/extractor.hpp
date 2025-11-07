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
#include <utility>
#include <vector>

#include "planner/core/egraph.hpp"
#include "planner/core/enode.hpp"

import memgraph.planner.core.eids;

namespace memgraph::planner::core {

template <typename Symbol>
using CostFunction = std::function<double(ENode<Symbol> const &)>;

struct Cost {
  ENodeId enode_id;
  double cost;
};

// Standalone function: Process an e-graph to find the cheapest e-node for each e-class
template <typename Symbol, typename Analysis, typename Func>
requires std::is_invocable_r_v < double, Func, ENode<Symbol>
const & > auto ProcessEGraph(EGraph<Symbol, Analysis> const &egraph, Func const &cost_function, EClassId id,
                             std::unordered_map<EClassId, Cost> &cheapest_enode) -> double {
  // if (!is_eclass) {
  //   // enode
  //   const auto &enode = egraph.get_enode(id);
  //   return std::accumulate(enode.children().begin(), enode.children().end(), 0.0,
  //                          [&](double acc, EClassId child_eclass_id) {
  //                            return acc + ProcessEGraph(egraph, cost_function, child_eclass_id, cheapest_enode);
  //                          });
  // }

  // eclass
  auto it = cheapest_enode.find(id);
  if (it != cheapest_enode.end()) {
    // If cost is infinity, we're currently processing this e-class (cycle detected)
    // If cost is finite, we've already computed it
    return it->second.cost;
  }

  // Mark this e-class as "in progress" with infinity cost to detect cycles
  cheapest_enode.emplace(id, Cost(ENodeId{0}, std::numeric_limits<double>::infinity()));

  double best_cost = std::numeric_limits<double>::max();
  ENodeId best_node{0};
  for (auto const &enode_id : egraph.eclass(id).nodes()) {
    auto cost = cost_function(egraph.get_enode(enode_id)) +
                ProcessEGraph(egraph, cost_function, egraph.find(enode_id), cheapest_enode);

    if (cost < best_cost) {
      best_cost = cost;
      best_node = enode_id;
    }
  }

  // Update with the actual computed cost
  cheapest_enode[id] = Cost(best_node, best_cost);
  return best_cost;
}

// Standalone function: Collect dependencies and compute in-degrees for topological sort
template <typename Symbol, typename Analysis>
auto CollectDependencies(EGraph<Symbol, Analysis> const &egraph,
                         std::unordered_map<EClassId, Cost> const &cheapest_enode, EClassId eclass_id,
                         std::unordered_map<EClassId, int> &in_degree) -> void {
  auto child_it = cheapest_enode.find(eclass_id);
  if (child_it == cheapest_enode.end()) return;
  auto enode = egraph.get_enode(child_it->second.enode_id);
  for (auto const &child_eclass_id : enode.children()) {
    if (!in_degree.contains(child_eclass_id)) {
      // if not visited yet go visit
      CollectDependencies(egraph, cheapest_enode, child_eclass_id, in_degree);
    }
    ++in_degree[child_eclass_id];
  }
}

// Standalone function: Perform topological sort to produce ordered output
template <typename Symbol, typename Analysis>
auto TopologicalSort(EGraph<Symbol, Analysis> const &egraph, std::unordered_map<EClassId, Cost> const &cheapest_enode,
                     std::unordered_map<EClassId, int> in_degree, EClassId eclass_id,
                     std::vector<std::pair<EClassId, ENodeId>> &result) -> void {
  // egraph is an acyclic graph so we can use topological sort

  // root should be the only eclass with in_degree 0
  std::queue<EClassId> queue{{eclass_id}};
  result.reserve(in_degree.size() + 1);

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
}

template <typename Symbol, typename Analysis, typename Func>
requires std::is_invocable_r_v < double, Func, ENode<Symbol>
const & > struct Extractor {
  template <typename FuncInner>
  requires std::is_invocable_r_v < double, FuncInner, ENode<Symbol>
  const & > Extractor(EGraph<Symbol, Analysis> const &egraph, FuncInner &&cost_function)
      : egraph_(egraph),
  cost_function_(std::forward<FuncInner>(cost_function)) {}

  auto Extract(EClassId const root_id) -> std::vector<std::pair<EClassId, ENodeId>> {
    ProcessEGraph(egraph_, cost_function_, root_id, cheapest_enode_);
    CollectDependencies(egraph_, cheapest_enode_, root_id, in_degree_);
    TopologicalSort(egraph_, cheapest_enode_, in_degree_, root_id, result_);
    return result_;
  }
  auto GetResult() -> std::vector<std::pair<EClassId, ENodeId>> const & { return result_; }

 private:
  // NOLINTNEXTLINE(cppcoreguidelines-avoid-const-or-ref-data-members)
  EGraph<Symbol, Analysis> const &egraph_;
  Func cost_function_;
  std::vector<std::pair<EClassId, ENodeId>> result_;
  std::unordered_map<EClassId, Cost> cheapest_enode_;
  std::unordered_map<EClassId, int> in_degree_;
};

template <typename Symbol, typename Analysis, typename Func>
requires std::is_invocable_r_v < double, Func, ENode<Symbol>
const & > Extractor(EGraph<Symbol, Analysis> const &, Func)->Extractor<Symbol, Analysis, Func>;

}  // namespace memgraph::planner::core
