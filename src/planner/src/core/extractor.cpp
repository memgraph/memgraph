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

#include "planner/core/extractor.hpp"

#include <numeric>
#include <queue>
#include <vector>

namespace memgraph::planner::core {

template <typename Symbol, typename Analysis>
auto Extractor<Symbol, Analysis>::Process(uint32_t id, bool is_eclass) -> double {
  if (!is_eclass) {
    // enode
    const auto &enode = egraph_.get_enode(id);
    return std::accumulate(
        enode.children().begin(), enode.children().end(), 0.0,
        [this](double acc, EClassId child_eclass_id) { return acc + Process(egraph_.eclass(child_eclass_id)); });
  }

  // eclass
  auto it = cheapest_enode_.find(id);
  if (it != cheapest_enode_.end()) {
    return it->second.second;
  }

  double best_cost = std::numeric_limits<double>::max();
  ENodeId best_node{0};
  for (auto const &enode_id : egraph_.eclass(id).nodes()) {
    auto cost = cost_function_(egraph_.get_enode(enode_id)) + Process(enode_id);

    if (cost < best_cost) {
      best_cost = cost;
      best_node = enode_id;
    }
  }

  cheapest_enode_.emplace(id, std::make_pair(best_node, best_cost));
  return best_cost;
}

template <typename Symbol, typename Analysis>
auto Extractor<Symbol, Analysis>::CollectDependencies(EClassId eclass_id) -> void {
  auto child_it = cheapest_enode_.find(eclass_id);
  if (child_it == cheapest_enode_.end()) return;
  auto enode = egraph_.get_enode(child_it->second.first);
  for (auto const &child_eclass_id : enode.children()) {
    if (!in_degree_.contains(child_eclass_id)) {
      // if not visited yet go visit
      CollectDependencies(child_eclass_id);
    }
    in_degree_[child_eclass_id]++;
  }
}

template <typename Symbol, typename Analysis>
auto Extractor<Symbol, Analysis>::TopologicalSort(EClassId eclass_id) -> void {
  // egraph is an acyclic graph so we can use topological sort

  // root should be the only eclass with in_degree 0
  std::queue<EClassId> queue{{eclass_id}};
  result_.reserve(in_degree_.size() + 1);

  while (!queue.empty()) {
    EClassId current_id = queue.front();
    queue.pop();

    auto it = cheapest_enode_.find(current_id);
    if (it == cheapest_enode_.end()) continue;
    auto enode_id = it->second.first;

    result_.emplace_back(current_id, enode_id);

    for (EClassId child_eclass_id : egraph_.get_enode(enode_id).children()) {
      in_degree_[child_eclass_id]--;
      if (in_degree_[child_eclass_id] == 0) {
        queue.push(child_eclass_id);
      }
    }
  }
}

}  // namespace memgraph::planner::core
