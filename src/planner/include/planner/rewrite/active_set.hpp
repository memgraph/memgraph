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
#include <utility>
#include <vector>

#include <boost/unordered/unordered_flat_set.hpp>

#include "planner/rewrite/rule_set.hpp"

import memgraph.planner.core.egraph;

namespace memgraph::planner::core::rewrite {

/// The parent-closure depth the active set must use: the deepest position any
/// rule binds, which is the maximum pattern depth over the rule set. A change at
/// a bind that deep reaches the pattern root in this many parent hops, so the
/// active set must close to here for the latch to be sound. Derived from the
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

/// The active set: the touched e-classes closed under parents to `depth` hops.
/// A rule newly-fires only when an e-class bound anywhere in a match changes;
/// such a change reaches the rule's pattern root by walking parents, so closing
/// the touched-set to the max pattern depth surfaces every e-class a pass could
/// re-enable a rule on. `touched` must already be canonical (as
/// `EGraph::touched_eclasses()` returns it); parents are canonicalized here.
template <typename Symbol, typename Analysis>
[[nodiscard]] auto ComputeActiveSet(EGraph<Symbol, Analysis> const &egraph, boost::unordered_flat_set<EClassId> touched,
                                    std::size_t depth) -> boost::unordered_flat_set<EClassId> {
  auto active = std::move(touched);
  std::vector<EClassId> frontier(active.begin(), active.end());

  for (std::size_t hop = 0; hop < depth && !frontier.empty(); ++hop) {
    std::vector<EClassId> next_frontier;
    for (auto const eclass_id : frontier) {
      for (auto const parent_enode : egraph.eclass(eclass_id).parents()) {
        auto const parent_eclass = egraph.find(parent_enode);
        if (active.insert(parent_eclass).second) next_frontier.push_back(parent_eclass);
      }
    }
    frontier = std::move(next_frontier);
  }
  return active;
}

}  // namespace memgraph::planner::core::rewrite
