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
#include <span>
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

/// A combine function for ParetoFrontier::combine: produces a new Alt from a
/// pair of input Alts (one cartesian-product element).  Callable repeatedly
/// over the n × m product, so the operator must be const-callable on whatever
/// state the functor carries.
template <typename Fn, typename Alt>
concept Combiner = std::invocable<Fn const &, Alt const &, Alt const &> &&
                   std::convertible_to<std::invoke_result_t<Fn const &, Alt const &, Alt const &>, Alt>;

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
  requires std::copyable<Alt> && DominanceRelation<DominanceFn, Alt>
struct ParetoFrontier {
  ParetoFrontier() = default;

  /// Read-only view over the (Pareto-pruned) alternatives.  Returning span keeps
  /// the storage choice (currently std::vector) out of the public contract.
  [[nodiscard]] auto alts() const noexcept -> std::span<Alt const> { return alts_; }

  /// Construct from an unpruned list of alternatives.  Prunes on construction
  /// so the resulting frontier satisfies the Pareto invariant.  This is the
  /// only public way to seed a frontier from raw data; the static factories
  /// below (flat_map / merge / combine) handle compositional construction.
  [[nodiscard]] static auto from_unpruned(std::vector<Alt> alts) -> ParetoFrontier {
    auto result = ParetoFrontier{};
    result.alts_ = std::move(alts);
    result.prune();
    return result;
  }

  /// Beam limit: trim to the top-N alternatives by the given cost projection.
  /// Pruned-invariant preserving (a subset of a Pareto-pruned set is still
  /// Pareto-pruned).  No-op when max_alts == 0 or already within budget.
  template <typename CostProjection>
    requires std::invocable<CostProjection, Alt const &>
  void beam(size_t max_alts, CostProjection &&proj) {
    if (max_alts == 0 || alts_.size() <= max_alts) return;
    std::partial_sort(alts_.begin(), alts_.begin() + max_alts, alts_.end(), [&](Alt const &a, Alt const &b) {
      return proj(a) < proj(b);
    });
    alts_.resize(max_alts);
  }

  /// Flat-map: for each alternative, produce zero or more new alternatives via a callback,
  /// collect into a new frontier, then prune. This is the general pattern for conditional
  /// transformations (e.g., Bind alive/dead branching).
  /// @param fn  (Alt const&, auto emit) -> void — calls emit(Alt&&) to produce output alternatives.
  template <typename Fn>
  [[nodiscard]] static auto flat_map(ParetoFrontier const &input, Fn &&fn) -> ParetoFrontier {
    auto result = ParetoFrontier{};
    result.alts_.reserve(input.alts_.size());  // heuristic: at least one output per input
    for (auto const &alt : input.alts_) {
      fn(alt, [&](Alt &&out) { result.alts_.push_back(std::move(out)); });
    }
    result.prune();
    return result;
  }

  /// Union two frontiers from different enodes in the same eclass, then prune.
  [[nodiscard]] static auto merge(ParetoFrontier const &a, ParetoFrontier const &b) -> ParetoFrontier {
    auto result = ParetoFrontier{};
    result.alts_.reserve(a.alts_.size() + b.alts_.size());
    result.alts_.append_range(a.alts_);
    result.alts_.append_range(b.alts_);
    result.prune();
    return result;
  }

  /// Rvalue overload — moves alts from both inputs instead of copying.  Hot path:
  /// extractor.hpp's ComputeFrontiers folds owned `enode_frontier` into the
  /// running `merged_frontier` and discards both, so moving avoids two
  /// vector copies per multi-enode eclass.
  [[nodiscard]] static auto merge(ParetoFrontier &&a, ParetoFrontier &&b) -> ParetoFrontier {
    auto result = ParetoFrontier{std::move(a)};
    result.alts_.reserve(result.alts_.size() + b.alts_.size());
    result.alts_.insert(
        result.alts_.end(), std::make_move_iterator(b.alts_.begin()), std::make_move_iterator(b.alts_.end()));
    result.prune();
    return result;
  }

  /// Cartesian product of two frontiers. For each (l, r) pair, calls combine_fn(l, r)
  /// to produce a new alternative, then prunes the result.
  template <typename CombineFn>
    requires Combiner<CombineFn, Alt>
  [[nodiscard]] static auto combine(ParetoFrontier const &lhs, ParetoFrontier const &rhs, CombineFn &&combine_fn)
      -> ParetoFrontier {
    auto result = ParetoFrontier{};
    result.alts_.reserve(lhs.alts_.size() * rhs.alts_.size());
    for (auto const &l : lhs.alts_) {
      for (auto const &r : rhs.alts_) {
        result.alts_.push_back(combine_fn(l, r));
      }
    }
    result.prune();
    return result;
  }

 protected:
  std::vector<Alt> alts_;

 private:
  /// Remove alternatives dominated by any other alternative in the frontier.
  /// Two-pass: first mark dominated indices, then erase. This avoids reading
  /// moved-from elements (std::erase_if/remove_if moves elements during its pass).
  /// Private — there is no path to seed an unpruned frontier from outside, so
  /// external prune() calls would always be no-ops.
  void prune() {
    auto const n = alts_.size();
    if (n <= 1) return;  // 0 or 1 alts: nothing can dominate; skip the SBO setup.
    // SBO buffer for the dominated-flag array. Frontiers rarely exceed 64
    // alternatives after pruning; larger ones fall back to heap.
    boost::container::small_vector<bool, 64> dominated(n, false);
    for (size_t i = 0; i < n; ++i) {
      if (dominated[i]) continue;
      for (size_t j = i + 1; j < n; ++j) {
        if (dominated[j]) continue;
        if (DominanceFn{}(alts_[i], alts_[j])) {
          dominated[i] = true;
          // Safe to break by transitivity of DominanceFn (enforced by the
          // DominanceRelation concept). If alts_[i] is dominated by alts_[j],
          // anything alts_[i] would have dominated at higher j is either
          // dominated by alts_[j] (transitivity) or will be discovered when the
          // outer loop reaches those indices as i. Continuing past j is redundant.
          break;
        }
        if (DominanceFn{}(alts_[j], alts_[i])) {
          dominated[j] = true;
        }
      }
    }
    size_t write = 0;
    for (size_t read = 0; read < n; ++read) {
      if (!dominated[read]) {
        if (write != read) alts_[write] = std::move(alts_[read]);
        ++write;
      }
    }
    alts_.resize(write);
  }
};

/// Concept for alternatives usable with CostResultBase.  Beyond what
/// ParetoFrontier needs (copyable; a totally-ordered cost), CostResultBase
/// projects via `&Alt::cost` in resolve_with_cost — so `cost` must be a
/// non-static data member (not a property/function).  The concept doesn't
/// spell that out directly because the constraint is exercised at use site
/// in resolve_with_cost; a non-member `cost` would produce a clear error
/// pointing at the projection.
template <typename Alt>
concept ParetoAlt = std::copyable<Alt> && requires(Alt const &a) {
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

  /// Initializer-list construction prunes on construction — no path to a
  /// non-pruned frontier from outside the class hierarchy.
  CostResultBase(std::initializer_list<Alt> init) : Base(Base::from_unpruned(std::vector<Alt>(init))) {}

  /// Hides Base::merge so the static return type matches Derived (required by CostResultType concept).
  [[nodiscard]] static auto merge(Derived const &a, Derived const &b) -> Derived { return Derived{Base::merge(a, b)}; }

  /// Rvalue overload — forwards to Base's move-merge to avoid copying alts when
  /// both inputs are owned (e.g. ComputeFrontiers folding accumulated frontier).
  [[nodiscard]] static auto merge(Derived &&a, Derived &&b) -> Derived {
    return Derived{Base::merge(std::move(static_cast<Base &>(a)), std::move(static_cast<Base &>(b)))};
  }

  /// Derived-returning analogue of Base::from_unpruned.  Tests use this to
  /// seed Pareto-pruned frontiers from raw alternative lists.
  [[nodiscard]] static auto from_unpruned(std::vector<Alt> alts) -> Derived {
    return Derived{Base::from_unpruned(std::move(alts))};
  }

  static auto resolve_with_cost(Derived const &f) -> std::pair<decltype(Alt::enode_id), cost_t> {
    auto const alts = f.alts();
    auto it = std::ranges::min_element(alts, {}, &Alt::cost);
    assert(it != alts.end() && "resolve_with_cost called on empty frontier");
    return {it->enode_id, it->cost};
  }

  static auto resolve(Derived const &f) noexcept -> decltype(Alt::enode_id) { return resolve_with_cost(f).first; }

  /// Asserts non-empty (consistent with resolve / resolve_with_cost). An empty
  /// frontier at the cost-model layer indicates a structural bug in the caller
  /// (e.g., handing in a child enode with zero alternatives) — we want a loud
  /// failure, not a silent +infinity that propagates upward and corrupts costs.
  static auto min_cost(Derived const &f) noexcept -> cost_t { return resolve_with_cost(f).second; }
};

}  // namespace memgraph::planner::core::extract
