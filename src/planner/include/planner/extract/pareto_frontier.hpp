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

#include <boost/container/small_vector.hpp>

namespace memgraph::planner::core::extract {

/// A dominance relation over Alt: a default-constructible binary callable
/// (Alt const&, Alt const&) -> convertible-to-bool returning true when the
/// first argument is dominated by the second. The relation MUST be transitive
/// — if dominates(a, b) and dominates(b, c) then dominates(a, c). prune()'s
/// early break relies on this property and on nothing else; reflexivity is
/// permitted (a may dominate itself), and antisymmetry is not required.
/// Reflexive duplicates resolve to "the later index in iteration order survives."
///
/// We use std::invocable (not std::predicate): std::predicate requires
/// regular_invocable which implies semantic equality preservation, but a
/// dominance relation is reflexive and not equality-preserving in that sense.
template <typename Fn, typename Alt>
concept DominanceRelation =
    std::invocable<Fn, Alt const &, Alt const &> &&
    std::convertible_to<std::invoke_result_t<Fn, Alt const &, Alt const &>, bool> && std::default_initializable<Fn>;

/// Generic Pareto frontier over alternatives of type Alt.
///
/// DominanceFn must satisfy `DominanceRelation<DominanceFn, Alt>` — see the
/// concept above for the transitivity contract.
///
/// CombineFn (used by combine/flat_map, not part of the type): (Alt, Alt) -> Alt
///   Produces a new alternative from two parent alternatives (Cartesian product element).
///
/// Example:
///   struct MyDominance {
///     static auto operator()(MyAlt const &a, MyAlt const &b) -> bool {
///       return b.cost <= a.cost && b.req.contains_all_of(a.req);
///     }
///   };
///   using Frontier = ParetoFrontier<MyAlt, MyDominance>;
template <typename Alt, typename DominanceFn>
  requires DominanceRelation<DominanceFn, Alt>
struct ParetoFrontier {
  std::vector<Alt> alts;

  /// Remove alternatives dominated by any other alternative in the frontier.
  /// Two-pass: first mark dominated indices, then erase. This avoids reading
  /// moved-from elements (std::erase_if/remove_if moves elements during its pass).
  void prune() {
    auto const n = alts.size();
    // SBO buffer for the dominated-flag array. Frontiers rarely exceed 64
    // alternatives after pruning; larger ones fall back to heap.
    boost::container::small_vector<bool, 64> dominated(n, false);
    for (size_t i = 0; i < n; ++i) {
      if (dominated[i]) continue;
      for (size_t j = i + 1; j < n; ++j) {
        if (dominated[j]) continue;
        if (DominanceFn{}(alts[i], alts[j])) {
          dominated[i] = true;
          // Safe to break by transitivity of DominanceFn (enforced by the
          // DominanceRelation concept). If alts[i] is dominated by alts[j],
          // anything alts[i] would have dominated at higher j is either
          // dominated by alts[j] (transitivity) or will be discovered when the
          // outer loop reaches those indices as i. Continuing past j is redundant.
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
  [[nodiscard]] static auto flat_map(ParetoFrontier const &input, Fn &&fn) -> ParetoFrontier {
    auto result = ParetoFrontier{};
    result.alts.reserve(input.alts.size());  // heuristic: at least one output per input
    for (auto const &alt : input.alts) {
      fn(alt, [&](Alt &&out) { result.alts.push_back(std::move(out)); });
    }
    result.prune();
    return result;
  }

  /// Union two frontiers from different enodes in the same eclass, then prune.
  [[nodiscard]] static auto merge(ParetoFrontier const &a, ParetoFrontier const &b) -> ParetoFrontier {
    auto result = ParetoFrontier{};
    result.alts.reserve(a.alts.size() + b.alts.size());
    result.alts.append_range(a.alts);
    result.alts.append_range(b.alts);
    result.prune();
    return result;
  }

  /// Cartesian product of two frontiers. For each (l, r) pair, calls combine_fn(l, r)
  /// to produce a new alternative, then prunes the result.
  template <typename CombineFn>
  [[nodiscard]] static auto combine(ParetoFrontier const &lhs, ParetoFrontier const &rhs, CombineFn &&combine_fn)
      -> ParetoFrontier {
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

  /// Asserts non-empty (consistent with resolve / resolve_with_cost). An empty
  /// frontier at the cost-model layer indicates a structural bug in the caller
  /// (e.g., handing in a child enode with zero alternatives) — we want a loud
  /// failure, not a silent +infinity that propagates upward and corrupts costs.
  static auto min_cost(Derived const &f) -> cost_t { return resolve_with_cost(f).second; }
};

}  // namespace memgraph::planner::core::extract
