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

#include <boost/unordered/unordered_flat_map.hpp>
#include <boost/unordered/unordered_flat_set.hpp>

import memgraph.planner.core.egraph;

namespace memgraph::planner::core::rewrite {

/// Close the active set under parents to `depth` hops, in place, and record the
/// shallowest hop at which each e-node symbol is reached in `min_hop`. `active`
/// enters holding the canonical touched e-classes (hop 0) and grows to their
/// parent-closure.
///
/// A rule newly-fires only when an e-class bound anywhere in a match changes;
/// such a change reaches the rule's pattern root by walking parents, so an
/// e-class carrying a root symbol at hop `h` can re-enable a pattern of depth
/// `>= h` and no shallower one. Recording each symbol's shallowest hop lets
/// arming gate a pattern by its own depth (`min_hop(S) <= d_P`) rather than by
/// the rule set's global maximum. Walk to the global maximum depth so every
/// pattern's symbol is seen; the per-pattern gate happens at arming. Parents are
/// canonicalized here; the BFS visits each e-class once, at its shallowest hop.
template <typename Symbol, typename Analysis>
void ComputeActiveSet(EGraph<Symbol, Analysis> const &egraph, boost::unordered_flat_set<EClassId> &active,
                      std::size_t depth, boost::unordered_flat_map<Symbol, std::size_t> &min_hop) {
  auto project = [&](EClassId eclass_id, std::size_t hop) {
    for (auto const enode_id : egraph.eclass(eclass_id).nodes()) {
      // BFS reaches each e-class at its shallowest hop, so the first insert of a
      // symbol is its minimum; a later, deeper sighting must not overwrite it.
      min_hop.try_emplace(egraph.get_enode(enode_id).symbol(), hop);
    }
  };

  std::vector<EClassId> frontier(active.begin(), active.end());
  for (auto const eclass_id : frontier) project(eclass_id, 0);

  std::vector<EClassId> next_frontier;
  for (std::size_t hop = 1; hop <= depth && !frontier.empty(); ++hop) {
    next_frontier.clear();
    for (auto const eclass_id : frontier) {
      for (auto const parent_enode : egraph.eclass(eclass_id).parents()) {
        auto const parent_eclass = egraph.find(parent_enode);
        if (active.insert(parent_eclass).second) {
          next_frontier.push_back(parent_eclass);
          project(parent_eclass, hop);
        }
      }
    }
    frontier.swap(next_frontier);
  }
}

/// Parent-closure only, discarding the per-symbol hops (see the primary overload).
template <typename Symbol, typename Analysis>
void ComputeActiveSet(EGraph<Symbol, Analysis> const &egraph, boost::unordered_flat_set<EClassId> &active,
                      std::size_t depth) {
  boost::unordered_flat_map<Symbol, std::size_t> discard;
  ComputeActiveSet(egraph, active, depth, discard);
}

}  // namespace memgraph::planner::core::rewrite
