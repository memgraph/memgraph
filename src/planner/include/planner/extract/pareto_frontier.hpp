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
  void prune() {
    std::erase_if(alts, [this](Alt const &a) {
      return std::ranges::any_of(alts, [&a](Alt const &b) { return &a != &b && DominanceFn{}(a, b); });
    });
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

  /// Left-fold combine over multiple frontiers. Stamps each intermediate with transform_fn
  /// before combining with the next frontier.
  template <typename CombineFn, typename TransformFn>
  static auto combine_all(std::span<ParetoFrontier const> frontiers, CombineFn &&combine_fn, TransformFn &&transform_fn)
      -> ParetoFrontier {
    assert(!frontiers.empty());
    auto result = frontiers[0];
    for (auto &alt : result.alts) {
      transform_fn(alt);
    }
    for (size_t i = 1; i < frontiers.size(); ++i) {
      result = combine(result, frontiers[i], combine_fn);
    }
    return result;
  }
};

}  // namespace memgraph::planner::core::extract
