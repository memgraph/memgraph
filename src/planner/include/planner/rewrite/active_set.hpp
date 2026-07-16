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

#include <algorithm>
#include <cstddef>
#include <vector>

#include <boost/unordered/unordered_flat_set.hpp>

#include "planner/rewrite/rule_set.hpp"

import memgraph.planner.core.egraph;

namespace memgraph::planner::core::rewrite {

/// The parent-closure depth the active set must use: the deepest position any
/// rule binds, which is the maximum pattern depth over the rule set. A change at
/// a bind that deep reaches the pattern root in this many parent hops, so the
/// active set must close to here for incremental arming to be sound. Derived from the
/// patterns, never hardcoded - a deeper rule widens it automatically.
template <RewritableGraph Graph>
[[nodiscard]] auto MaxRuleSetPatternDepth(RuleSet<Graph> const &rules) -> std::size_t {
  std::size_t max_depth = 0;
  for (auto const &rule : rules.rules()) {
    for (auto const &pattern : rule->patterns()) {
      max_depth = std::max(max_depth, pattern.depth());
    }
  }
  return max_depth;
}

/// Close the active set under parents to `depth` hops, in place. `active` enters
/// holding the canonical touched e-classes and grows to their parent-closure.
/// A rule newly-fires only when an e-class bound anywhere in a match changes;
/// such a change reaches the rule's pattern root by walking parents, so closing
/// to the max pattern depth surfaces every e-class a pass could re-enable a rule
/// on. Parents are canonicalized here.
template <typename Symbol, typename Analysis>
void ComputeActiveSet(EGraph<Symbol, Analysis> const &egraph, boost::unordered_flat_set<EClassId> &active,
                      std::size_t depth) {
  std::vector<EClassId> frontier(active.begin(), active.end());
  std::vector<EClassId> next_frontier;
  for (std::size_t hop = 0; hop < depth && !frontier.empty(); ++hop) {
    next_frontier.clear();
    for (auto const eclass_id : frontier) {
      for (auto const parent_enode : egraph.eclass(eclass_id).parents()) {
        auto const parent_eclass = egraph.find(parent_enode);
        if (active.insert(parent_eclass).second) next_frontier.push_back(parent_eclass);
      }
    }
    frontier.swap(next_frontier);
  }
}

}  // namespace memgraph::planner::core::rewrite
