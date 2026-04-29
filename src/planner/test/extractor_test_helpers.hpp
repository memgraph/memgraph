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

// Test-only helpers for the extraction pipeline.
//
// DefaultCostResult and ResolveSelection were promoted out of the production
// header (planner/extract/extractor.hpp) once it was clear they exist solely
// to drive the cost-model unit tests in unittest__extractor.cpp:
//
//   * Production cost models (PlanCostModel in egraph_converter.cpp) use a
//     ParetoFrontier-derived CostResult, not DefaultCostResult.
//   * Production resolution (PlanResolver) is context-aware and handles
//     Bind alive/dead decisions; the generic ResolveSelection here is not
//     safe for cost models with conditional child semantics.
//
// Keeping these here prevents new production cost models from picking up the
// generic resolver "by accident" and getting subtly wrong results.

#include <utility>

#include "planner/extract/extractor.hpp"

import memgraph.planner.core.egraph;

namespace memgraph::planner::core::extract::testing {

/// Default CostResult for simple scalar cost-model tests.
template <typename T>
struct DefaultCostResult {
  using cost_t = T;

  T cost;
  ENodeId enode_id;

  static auto merge(DefaultCostResult const &a, DefaultCostResult const &b) -> DefaultCostResult {
    return a.cost <= b.cost ? a : b;
  }

  static auto resolve_with_cost(DefaultCostResult const &r) -> std::pair<ENodeId, cost_t> {
    return {r.enode_id, r.cost};
  }

  static auto resolve(DefaultCostResult const &r) -> ENodeId { return r.enode_id; }

  static auto min_cost(DefaultCostResult const &r) -> cost_t { return r.cost; }
};

static_assert(CostResultType<DefaultCostResult<double>>);

/// Generic top-down resolver. Picks the best enode at each eclass via
/// CostResult::resolve_with_cost. Used by Extract() in unittest__extractor.cpp
/// for cost-model tests that do not require context-sensitive child visitation.
///
/// NOTE: cost models with conditional child semantics (e.g., Bind alive/dead)
/// must use a custom resolver (see PlanResolver in egraph_converter.cpp) —
/// this generic resolver will silently produce wrong selections for them.
template <typename Symbol, typename Analysis, typename CostResult>
  requires CostResultType<CostResult>
[[nodiscard]] auto ResolveSelection(EGraph<Symbol, Analysis> const &egraph,
                                    std::unordered_map<EClassId, EClassFrontier<CostResult>> const &frontier_map,
                                    EClassId root)
    -> std::unordered_map<EClassId, Selection<typename CostResult::cost_t>> {
  using CostType = typename CostResult::cost_t;

  auto resolved = std::unordered_map<EClassId, Selection<CostType>>{};
  auto to_visit = std::vector{root};
  auto visited = std::unordered_set{root};

  while (!to_visit.empty()) {
    auto current = to_visit.back();
    to_visit.pop_back();

    auto it = frontier_map.find(current);
    assert(it != frontier_map.end() && it->second.has_value());

    auto const &frontier = *it->second;
    auto [enode_id, cost] = CostResult::resolve_with_cost(frontier);
    resolved[current] = Selection<CostType>{enode_id, cost};

    auto const &enode = egraph.get_enode(enode_id);
    for (auto child : enode.children()) {
      if (visited.insert(child).second) {
        to_visit.push_back(child);
      }
    }
  }

  return resolved;
}

}  // namespace memgraph::planner::core::extract::testing
