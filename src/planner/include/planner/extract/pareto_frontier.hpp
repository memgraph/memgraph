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
#include <concepts>
#include <limits>
#include <utility>
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
    constexpr size_t kSmallSize = 64;
    char small_buf[kSmallSize] = {};
    std::vector<char> heap_buf;
    char *dominated;
    if (n <= kSmallSize) {
      dominated = small_buf;
    } else {
      heap_buf.assign(n, 0);
      dominated = heap_buf.data();
    }
    for (size_t i = 0; i < n; ++i) {
      if (dominated[i] != 0) continue;
      for (size_t j = i + 1; j < n; ++j) {
        if (dominated[j] != 0) continue;
        if (DominanceFn{}(alts[i], alts[j])) {
          dominated[i] = 1;
          break;
        }
        if (DominanceFn{}(alts[j], alts[i])) {
          dominated[j] = 1;
        }
      }
    }
    size_t write = 0;
    for (size_t read = 0; read < n; ++read) {
      if (dominated[read] == 0) {
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
    requires std::invocable<CostProjection, Alt const &>
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
  /// @param fn  (Alt const&, auto emit) -> void — calls emit(Alt&&) to produce output alternatives.
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
};

/// Concept for alternatives usable with CostResultBase.
/// Alt must have a totally_ordered `.cost` and an `.enode_id` field.
template <typename Alt>
concept ParetoAlt = requires(Alt const &a) {
  { a.cost } -> std::totally_ordered;
  { a.enode_id };
};

/// CRTP base for ParetoFrontier types that use min-cost as their resolve/min_cost strategy.
/// Derived types get resolve(), min_cost(), and convenience constructors for free.
/// Alt must have `.cost` (double-compatible) and `.enode_id` fields.
template <typename Derived, typename Alt, typename DominanceFn>
  requires ParetoAlt<Alt>
struct CostResultBase : ParetoFrontier<Alt, DominanceFn> {
  using Base = ParetoFrontier<Alt, DominanceFn>;
  using Base::Base;
  using cost_t = decltype(Alt::cost);

  CostResultBase() = default;

  // NOLINTNEXTLINE(google-explicit-constructor)
  CostResultBase(Base base) : Base(std::move(base)) {}

  CostResultBase(std::initializer_list<Alt> init) : Base{std::vector<Alt>(init)} {}

  static auto resolve_with_cost(Derived const &f) -> std::pair<decltype(Alt::enode_id), cost_t> {
    auto it = std::ranges::min_element(f.alts, {}, &Alt::cost);
    assert(it != f.alts.end() && "resolve_with_cost called on empty frontier");
    return {it->enode_id, it->cost};
  }

  static auto resolve(Derived const &f) -> decltype(Alt::enode_id) { return resolve_with_cost(f).first; }

  static auto min_cost(Derived const &f) -> cost_t {
    if (f.alts.empty()) return std::numeric_limits<cost_t>::infinity();
    return resolve_with_cost(f).second;
  }
};

}  // namespace memgraph::planner::core::extract
