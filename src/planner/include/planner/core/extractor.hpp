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

#include "planner/core/enode.hpp"
#include "planner/core/fwd.hpp"

import memgraph.planner.core.eids;

namespace memgraph::planner::core {

template <typename Symbol>
using CostFunction = std::function<double(ENode<Symbol> const &)>;

template <typename Symbol, typename Analysis, typename Func>
requires std::is_invocable_r_v < double, Func, ENode<Symbol>
const & > struct Extractor {
  template <typename FuncInner>
  requires std::is_invocable_r_v < double, FuncInner, ENode<Symbol>
  const & > Extractor(EGraph<Symbol, Analysis> const &egraph, FuncInner &&cost_function)
      : egraph_(egraph),
  cost_function_(std::forward<FuncInner>(cost_function)) {}

  auto Process(EClassId id, bool is_eclass = true) -> double {
    if (!is_eclass) {
      // enode
      const auto &enode = egraph_.get_enode(id);
      return std::accumulate(enode.children().begin(), enode.children().end(), 0.0,
                             [this](double acc, EClassId child_eclass_id) { return acc + Process(child_eclass_id); });
    }

    // eclass
    auto it = cheapest_enode_.find(id);
    if (it != cheapest_enode_.end()) {
      return it->second.second;
    }

    double best_cost = std::numeric_limits<double>::max();
    ENodeId best_node{0};
    for (auto const &enode_id : egraph_.eclass(id).nodes()) {
      auto cost = cost_function_(egraph_.get_enode(enode_id)) + Process(enode_id, false);

      if (cost < best_cost) {
        best_cost = cost;
        best_node = enode_id;
      }
    }

    cheapest_enode_.emplace(id, std::make_pair(best_node, best_cost));
    return best_cost;
  }

  auto CollectDependencies(EClassId eclass_id) -> void {
    auto child_it = cheapest_enode_.find(eclass_id);
    if (child_it == cheapest_enode_.end()) return;
    auto enode = egraph_.get_enode(child_it->second.first);
    for (auto const &child_eclass_id : enode.children()) {
      if (!in_degree_.contains(child_eclass_id)) {
        // if not visited yet go visit
        CollectDependencies(child_eclass_id);
      }
      ++in_degree_[child_eclass_id];
    }
  }

  auto TopologicalSort(EClassId eclass_id) -> void {
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
        --in_degree_[child_eclass_id];
        if (in_degree_[child_eclass_id] == 0) {
          queue.push(child_eclass_id);
        }
      }
    }
  }

  auto Extract(EClassId const root_id) -> std::vector<std::pair<EClassId, ENodeId>> {
    Process(root_id);
    CollectDependencies(root_id);
    TopologicalSort(root_id);
    return result_;
  }
  auto GetResult() -> std::vector<std::pair<EClassId, ENodeId>> const & { return result_; }

 private:
  // NOLINTNEXTLINE(cppcoreguidelines-avoid-const-or-ref-data-members)
  EGraph<Symbol, Analysis> const &egraph_;
  Func cost_function_;
  std::vector<std::pair<EClassId, ENodeId>> result_;
  std::unordered_map<EClassId, std::pair<ENodeId, double>> cheapest_enode_;
  std::unordered_map<EClassId, int> in_degree_;
};

template <typename Symbol, typename Analysis, typename Func>
requires std::is_invocable_r_v < double, Func, ENode<Symbol>
const & > Extractor(EGraph<Symbol, Analysis> const &, Func)->Extractor<Symbol, Analysis, Func>;

}  // namespace memgraph::planner::core
