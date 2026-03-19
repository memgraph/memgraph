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
#include <cassert>
#include <span>
#include <vector>

namespace memgraph::planner::core::extract {

/// Generic Pareto frontier over alternatives of type Alt.
///
/// DominanceFn: (Alt const&, Alt const&) -> bool
///   Returns true if the first argument is dominated by the second.
///   Must be transitive: if a is dominated by b and b is dominated by c,
///   then a must be dominated by c. The prune() algorithm relies on this.
///
/// CombineFn: (Alt const&, Alt const&) -> Alt
///   Produces a new alternative from two parent alternatives (Cartesian product element).
///
/// Example:
///   using Frontier = ParetoFrontier<MyAlt,
///     [](MyAlt const &a, MyAlt const &b) { return b.cost <= a.cost && b.req ⊆ a.req; }>;
// Forward declaration for trait detection.
template <typename Alt, typename DominanceFn>
struct ParetoFrontier;

/// Detect whether T is a ParetoFrontier specialization.
template <typename>
struct is_pareto_frontier : std::false_type {};

template <typename Alt, typename Dom>
struct is_pareto_frontier<ParetoFrontier<Alt, Dom>> : std::true_type {};

template <typename T>
inline constexpr bool is_pareto_frontier_v = is_pareto_frontier<T>::value;

template <typename Alt, typename DominanceFn>
struct ParetoFrontier {
  std::vector<Alt> alts;

  /// Remove alternatives dominated by any other alternative in the frontier.
  /// Two-pass: first mark dominated indices, then erase. This avoids reading
  /// moved-from elements (std::erase_if/remove_if moves elements during its pass).
  void prune() {
    auto const n = alts.size();
    auto dominated = std::vector<bool>(n, false);
    for (size_t i = 0; i < n; ++i) {
      if (dominated[i]) continue;
      for (size_t j = i + 1; j < n; ++j) {
        if (dominated[j]) continue;
        if (DominanceFn{}(alts[i], alts[j])) {
          dominated[i] = true;
          break;
        }
        if (DominanceFn{}(alts[j], alts[i])) {
          dominated[j] = true;
        }
      }
    }
    size_t write = 0;
    for (size_t read = 0; read < n; ++read) {
      if (!dominated[read]) {
        if (write != read) alts[write] = std::move(alts[read]);
        ++write;
      }
    }
    alts.resize(write);
  }

  /// Beam limit: after pruning, if the frontier still exceeds max_alts,
  /// keep only the top-N alternatives by the given cost projection (beam search).
  /// This is a principled approximation — we keep the cheapest alternatives.
  /// @param max_alts  Maximum number of alternatives to keep. 0 means no limit.
  /// @param proj      Projection from Alt to a comparable cost value.
  template <typename CostProjection>
  void beam(size_t max_alts, CostProjection &&proj) {
    if (max_alts == 0 || alts.size() <= max_alts) return;
    std::partial_sort(alts.begin(), alts.begin() + max_alts, alts.end(), [&](Alt const &a, Alt const &b) {
      return proj(a) < proj(b);
    });
    alts.resize(max_alts);
  }

  /// Flat-map: for each alternative, produce zero or more new alternatives via a callback,
  /// collect into a new frontier, then prune. This is the general pattern for conditional
  /// transformations (e.g., Bind alive/dead branching).
  /// @param fn  (Alt const&) -> void, calls `emit(Alt)` to produce output alternatives.
  template <typename Fn>
  static auto flat_map(ParetoFrontier const &input, Fn &&fn) -> ParetoFrontier {
    auto result = ParetoFrontier{};
    for (auto const &alt : input.alts) {
      fn(alt, [&](Alt &&out) { result.alts.push_back(std::move(out)); });
    }
    result.prune();
    return result;
  }

  /// Union two frontiers from different enodes in the same eclass, then prune.
  static auto merge(ParetoFrontier const &a, ParetoFrontier const &b) -> ParetoFrontier {
    auto result = ParetoFrontier{};
    result.alts.reserve(a.alts.size() + b.alts.size());
    result.alts.insert(result.alts.end(), a.alts.begin(), a.alts.end());
    result.alts.insert(result.alts.end(), b.alts.begin(), b.alts.end());
    result.prune();
    return result;
  }

  /// Cartesian product of two frontiers. For each (l, r) pair, calls combine_fn(l, r)
  /// to produce a new alternative, then prunes the result.
  template <typename CombineFn>
  static auto combine(ParetoFrontier const &lhs, ParetoFrontier const &rhs, CombineFn &&combine_fn) -> ParetoFrontier {
    auto result = ParetoFrontier{};
    result.alts.reserve(lhs.alts.size() * rhs.alts.size());
    for (auto const &l : lhs.alts) {
      for (auto const &r : rhs.alts) {
        result.alts.push_back(combine_fn(l, r));
      }
    }
    result.prune();
    return result;
  }

  /// Left-fold combine over multiple frontiers.
  template <typename CombineFn>
  static auto combine_all(std::span<ParetoFrontier const> frontiers, CombineFn &&combine_fn) -> ParetoFrontier {
    assert(!frontiers.empty());
    auto result = frontiers[0];
    for (size_t i = 1; i < frontiers.size(); ++i) {
      result = combine(result, frontiers[i], combine_fn);
    }
    return result;
  }
};

}  // namespace memgraph::planner::core::extract
