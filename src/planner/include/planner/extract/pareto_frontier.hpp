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
#include <compare>
#include <concepts>
#include <functional>
#include <ranges>
#include <span>
#include <type_traits>
#include <utility>
#include <vector>

namespace memgraph::planner::core::extract {

// ============================================================================
// Comparators
// ============================================================================
// A comparator is a stateless struct exposing static `compare(a, b)` returning
// some `<=>` ordering type.  Totality is detected at compile time from that
// return type: `strong_ordering` / `weak_ordering` ⇒ totally ordered.
// Comparators that need set-based reasoning return `partial_ordering`.

/// "Lower is better" for any three-way-comparable type.  Returns whatever the
/// underlying `<=>` yields (typically `strong_ordering`) so the totally-ordered
/// trait is detectable.  Swapping operands inverts the ordering so a < b ⇒
/// "a dominates b" ⇒ `greater`.
struct LowerIsBetter {
  static constexpr auto compare(auto const &a, auto const &b) { return b <=> a; }
};

/// Types that expose a fast subset comparison (e.g. a bitset) can hook
/// SmallerSubsetIsBetter / LargerSubsetIsBetter by implementing
/// `subset_compare(other)`: greater iff `*this ⊂ other`, less iff
/// `other ⊂ *this`, equivalent iff equal, unordered otherwise.
template <typename T>
concept HasSubsetCompare = requires(T const &a, T const &b) {
  { a.subset_compare(b) } -> std::convertible_to<std::partial_ordering>;
};

/// "Smaller-by-inclusion is better".  Types with a member `subset_compare`
/// use it directly (bitset path: O(words)).  Otherwise falls back to a
/// single forward merge over two sorted ranges with early-exit once both
/// subset flags are false; an inexpensive size-based prefilter rules out
/// the impossible subset direction.
struct SmallerSubsetIsBetter {
  template <typename T>
    requires HasSubsetCompare<T>
  static auto compare(T const &a, T const &b) -> std::partial_ordering {
    return a.subset_compare(b);
  }

  template <std::ranges::input_range R>
    requires(!HasSubsetCompare<R>)
  static auto compare(R const &a, R const &b) -> std::partial_ordering {
    auto const size_a = std::ranges::size(a);
    auto const size_b = std::ranges::size(b);
    // Cardinality rules out subset directions cheaply.  a ⊆ b requires
    // |a| ≤ |b|; b ⊆ a requires |b| ≤ |a|.
    bool a_subset_b = size_a <= size_b;
    bool b_subset_a = size_b <= size_a;

    auto it_a = std::ranges::begin(a);
    auto const end_a = std::ranges::end(a);
    auto it_b = std::ranges::begin(b);
    auto const end_b = std::ranges::end(b);
    while (it_a != end_a && it_b != end_b) {
      auto const cmp = *it_a <=> *it_b;
      if (std::is_lt(cmp)) {
        a_subset_b = false;
        ++it_a;
      } else if (std::is_gt(cmp)) {
        b_subset_a = false;
        ++it_b;
      } else {
        ++it_a;
        ++it_b;
      }
      if (!a_subset_b && !b_subset_a) break;
    }
    if (it_a != end_a) a_subset_b = false;
    if (it_b != end_b) b_subset_a = false;
    // a ⊆ b means a "needs less" → a is better → a dominates b → greater.
    if (a_subset_b && b_subset_a) return std::partial_ordering::equivalent;
    if (a_subset_b) return std::partial_ordering::greater;
    if (b_subset_a) return std::partial_ordering::less;
    return std::partial_ordering::unordered;
  }
};

/// "Larger-by-inclusion is better" - the dual of SmallerSubsetIsBetter.
/// Use for "introduces" / "provides" axes where having more is helpful.
struct LargerSubsetIsBetter {
  template <typename T>
  static auto compare(T const &a, T const &b) -> std::partial_ordering {
    return SmallerSubsetIsBetter::compare(b, a);
  }
};

// ============================================================================
// Dim
// ============================================================================
// A dimension projects an `Alt` through a member pointer and ranks the
// projected values with a comparator.  `Dim<MemPtr, Cmp>` is the only way to
// spell a frontier dimension; `ParetoFrontier` is parameterised by a pack of
// `Dim`s directly (no separate dominance-functor type).

template <auto MemPtr, typename Cmp>
struct Dim {
  template <typename Alt>
  static constexpr auto compare(Alt const &a, Alt const &b) {
    return Cmp::compare(a.*MemPtr, b.*MemPtr);
  }
};

// ============================================================================
// Concepts
// ============================================================================

template <typename D, typename Alt>
concept ParetoDimension = requires(Alt const &a, Alt const &b) {
  { D::compare(a, b) } -> std::convertible_to<std::partial_ordering>;
};

/// A combine function for ParetoFrontier::cartesian_product: produces a new Alt
/// from a pair of input Alts (one cartesian-product element).
template <typename Fn, typename Alt>
concept Combiner = std::invocable<Fn const &, Alt const &, Alt const &> &&
                   std::convertible_to<std::invoke_result_t<Fn const &, Alt const &, Alt const &>, Alt>;

/// A pair-predicate for ParetoFrontier::cartesian_product_if: decides whether
/// a given (l, r) pair should produce an Alt at all.  False ⇒ skip.
template <typename Fn, typename Alt>
concept PairPredicate = std::invocable<Fn const &, Alt const &, Alt const &> &&
                        std::convertible_to<std::invoke_result_t<Fn const &, Alt const &, Alt const &>, bool>;

// ============================================================================
// Dominance
// ============================================================================

/// Pareto fold over all dims:
///   - Any unordered dim ⇒ result is unordered.
///   - Disagreement (one less, another greater) ⇒ unordered.
///   - All equivalent ⇒ equivalent.
///   - Otherwise the agreed direction wins.
/// Short-circuits as soon as the running result hits unordered, saving
/// remaining dim comparisons (e.g. the set-merge in SmallerSubsetIsBetter
/// when an earlier scalar dim already conflicts).  Public so that callers
/// can query dominance directly without constructing a ParetoFrontier (e.g.
/// algebra-level tests).
template <typename... Dims, typename Alt>
auto dominance_compare(Alt const &a, Alt const &b) -> std::partial_ordering {
  using std::partial_ordering;
  auto acc = partial_ordering::equivalent;
  auto step = [&](partial_ordering next) -> bool {
    if (next == partial_ordering::equivalent) return true;
    if (acc == partial_ordering::equivalent) {
      acc = next;
    } else if (acc != next) {
      acc = partial_ordering::unordered;
    }
    return acc != partial_ordering::unordered;
  };
  (void)(step(static_cast<partial_ordering>(Dims::compare(a, b))) && ...);
  return acc;
}

// ============================================================================
// ParetoFrontier
// ============================================================================

/// Generic Pareto frontier over alternatives of type `Alt` ranked by a pack of
/// dimensions `Dims...`.  Each `Dims` must satisfy `ParetoDimension<D, Alt>`.
///
/// The dominance relation is the pareto-fold of all per-dim comparators (see
/// `dominance_compare`).  Every comparator must be transitive on its own axis:
/// a ≥ b and b ≥ c ⇒ a ≥ c (with `≥` here meaning "dominates or ties").  The
/// pruner's early-out on dominance relies on this property.
///
/// Storage order of `alts_` is implementation-defined; callers must not depend
/// on it.  Iterate via `alts()` and select via `PickBest` / `min_element`.
///
/// Example:
///   struct MyAlt { double cost; std::set<int> req; ENodeId id; };
///   using Frontier = ParetoFrontier<MyAlt,
///       Dim<&MyAlt::cost, LowerIsBetter>,
///       Dim<&MyAlt::req,  SmallerSubsetIsBetter>>;

/// Concept for alternatives usable with CostResultBase.  `cost` must be a
/// non-static data member (not a property/function); resolve projects via
/// `&Alt::cost`.
template <typename Alt>
concept ParetoAlt = std::copyable<Alt> && requires(Alt const &a) {
  { a.cost } -> std::totally_ordered;
  { a.enode_id };
};

/// Refinement of `ParetoAlt` for alternatives that additionally support the
/// in-place mutations LazyMap applies when materialising a view: bumping
/// `cost` by a delta and overwriting `enode_id`.
template <typename Alt, typename EnodeId>
concept MutableParetoAlt = ParetoAlt<Alt> && requires(Alt &a, double d, EnodeId e) {
  { a.cost += d };
  { a.enode_id = e };
};

template <typename Alt, typename... Dims>
  requires std::copyable<Alt> && (ParetoDimension<Dims, Alt> && ...)
struct ParetoFrontier {
  ParetoFrontier() = default;

  /// Construct from an unpruned list of alternatives.  Prunes on construction
  /// so the resulting frontier satisfies the Pareto invariant.  flat_map /
  /// cartesian_product / merge_in_place / LazyMap are the compositional
  /// alternatives.
  explicit ParetoFrontier(std::vector<Alt> alts) : alts_(std::move(alts)) { prune(); }

  /// Returns a lazy "view" frontier whose alts are `source.alts()` with each
  /// `cost` bumped by `cost_delta` and each `enode_id` replaced by
  /// `enode_id_override`.  No materialisation until the view is iterated
  /// (`alts()` / `merge_in_place` / `cartesian_product` / `flat_map`) or
  /// mutated.  The returned frontier holds a non-owning pointer to `source`;
  /// the caller is responsible for ensuring `source` outlives the view.
  ///
  /// Chains collapse on construction: `LazyMap(LazyMap(s, d1, e1), d2, e2)`
  /// is equivalent to `LazyMap(s, d1+d2, e2)`, so the source pointer always
  /// targets a materialised frontier.  This bounds materialisation cost to
  /// a single pass over the original source's alts no matter how deep the
  /// view chain has been threaded through cost-model recursion.
  ///
  /// Pareto invariant: `cost_delta` is uniform across alts, so relative
  /// dominance under `Dim<&Alt::cost, LowerIsBetter>` is preserved.
  /// `enode_id_override` is uniform too; if any dim reads `enode_id`, that
  /// is also order-preserving (all alts get the same value).  Other dims
  /// (e.g. set-based) are unchanged because source's alts are
  /// untouched.  Net: the view satisfies the same prune invariant as the
  /// source, so no re-prune is needed at materialisation.
  template <typename EnodeId>
  [[nodiscard]] static auto LazyMap(ParetoFrontier const &source, double cost_delta, EnodeId enode_id_override)
      -> ParetoFrontier
    requires MutableParetoAlt<Alt, EnodeId>
  {
    ParetoFrontier result;
    if (source.lazy_source_ != nullptr) {
      result.lazy_source_ = source.lazy_source_;
      result.lazy_cost_delta_ = source.lazy_cost_delta_ + cost_delta;
    } else {
      result.lazy_source_ = &source;
      result.lazy_cost_delta_ = cost_delta;
    }
    result.lazy_enode_id_override_ = enode_id_override;
    return result;
  }

  /// Read-only view over the (Pareto-pruned) alternatives.  Order is
  /// implementation-defined.  Returning span keeps the storage choice out of
  /// the public contract.  Triggers materialisation of any pending lazy view.
  [[nodiscard]] auto alts() const -> std::span<Alt const> {
    if (lazy_source_ != nullptr) materialise();
    return alts_;
  }

  /// In-place mutation that the caller promises preserves the Pareto invariant.
  /// Calls fn(alt) on each surviving alt; no re-prune is performed.
  ///
  /// Contract: fn must not change the relative ordering of any pair under the
  /// composed dominance relation, i.e., for every pair (A, B) in the frontier,
  /// `dominance_compare<Dims...>(A, B)` must be unchanged by fn.  Adding a
  /// uniform constant to a `cost` field, or rewriting a field that no dim
  /// reads (e.g., enode_id), both satisfy this contract.  Mutations that
  /// could flip dominance must go through flat_map / merge_in_place /
  /// cartesian_product instead, which re-prune.
  template <typename Fn>
    requires std::invocable<Fn, Alt &>
  void mutate_pruning_invariant_preserving(Fn &&fn) {
    if (lazy_source_ != nullptr) materialise();
    for (Alt &alt : alts_) fn(alt);
  }

  /// Flat-map: for each alternative, produce zero or more new alternatives via
  /// a callback, collect into a new frontier, then prune.
  /// @param fn  (Alt const&, auto emit) -> void - calls emit(Alt&&) to produce
  ///            output alternatives.
  template <typename Fn>
  [[nodiscard]] static auto flat_map(ParetoFrontier const &input, Fn &&fn) -> ParetoFrontier {
    auto const input_alts = input.alts();
    auto out = std::vector<Alt>{};
    out.reserve(input_alts.size());
    for (Alt const &alt : input_alts) {
      fn(alt, [&out](Alt &&v) { out.push_back(std::move(v)); });
    }
    return ParetoFrontier{std::move(out)};
  }

  /// Union another frontier into this one and re-prune.  Both `*this` and
  /// `other` are already Pareto-pruned, so within-`*this` pairs need not be
  /// re-checked; the prune scan starts at the boundary.  `other`'s alts are
  /// moved-from on return.  Materialises both sides if they're lazy views.
  void merge_in_place(ParetoFrontier &&other) {
    if (lazy_source_ != nullptr) materialise();
    auto const other_alts = other.alts();  // forces materialisation of `other`
    auto const pruned_prefix = alts_.size();
    alts_.reserve(pruned_prefix + other_alts.size());
    std::ranges::move(other.alts_, std::back_inserter(alts_));
    prune(pruned_prefix);
  }

  /// Cartesian product of two frontiers.  For each (l, r) pair, calls
  /// combine_fn(l, r) to produce a new alternative, then prunes the result.
  /// Output size before pruning is lhs.size() * rhs.size(); callers paying
  /// that cost should expect it.
  template <typename CombineFn>
    requires Combiner<CombineFn, Alt>
  [[nodiscard]] static auto cartesian_product(ParetoFrontier const &lhs, ParetoFrontier const &rhs,
                                              CombineFn &&combine_fn) -> ParetoFrontier {
    auto const lhs_alts = lhs.alts();
    auto const rhs_alts = rhs.alts();
    auto out = std::vector<Alt>{};
    out.reserve(lhs_alts.size() * rhs_alts.size());
    for (Alt const &l : lhs_alts) {
      for (Alt const &r : rhs_alts) {
        out.push_back(combine_fn(l, r));
      }
    }
    return ParetoFrontier{std::move(out)};
  }

  /// Filtered cartesian product.  For each (l, r) pair where pred(l, r) is
  /// true, calls combine_fn(l, r) to produce a new alternative; pairs where
  /// pred is false are skipped.  Use when some pair combinations cannot form
  /// valid alternatives by construction (e.g. an operator/expression boundary
  /// where the expression's demand isn't satisfied by the operator's
  /// introductions).
  template <typename PredFn, typename CombineFn>
    requires PairPredicate<PredFn, Alt> && Combiner<CombineFn, Alt>
  [[nodiscard]] static auto cartesian_product_if(ParetoFrontier const &lhs, ParetoFrontier const &rhs, PredFn &&pred,
                                                 CombineFn &&combine_fn) -> ParetoFrontier {
    auto const lhs_alts = lhs.alts();
    auto const rhs_alts = rhs.alts();
    auto out = std::vector<Alt>{};
    out.reserve(lhs_alts.size() * rhs_alts.size());  // upper bound; predicate may reduce
    for (Alt const &l : lhs_alts) {
      for (Alt const &r : rhs_alts) {
        if (!pred(l, r)) continue;
        out.push_back(combine_fn(l, r));
      }
    }
    return ParetoFrontier{std::move(out)};
  }

 private:
  // std::vector chosen so ParetoFrontier moves stay pointer-swap; small_vector's
  // element-wise move dominates here because Alt is large with a non-trivial move.
  mutable std::vector<Alt> alts_;
  // Lazy view state.  Non-null `lazy_source_` means this frontier represents
  // `source.alts()` with cost_delta and enode_id_override applied; `alts_` is
  // empty until materialise() runs.  See LazyMap for the chain-collapse
  // invariant.
  mutable ParetoFrontier const *lazy_source_ = nullptr;
  mutable double lazy_cost_delta_ = 0;
  mutable std::remove_cvref_t<decltype(std::declval<Alt>().enode_id)> lazy_enode_id_override_{};

  /// Realise the lazy view into `alts_`.  Source is guaranteed non-lazy by the
  /// chain-collapse done in LazyMap, so this is a single pass over the source.
  void materialise() const {
    assert(lazy_source_ != nullptr);
    auto const &source_alts = lazy_source_->alts_;  // source is non-lazy: read directly
    alts_.clear();
    alts_.reserve(source_alts.size());
    for (Alt const &a : source_alts) {
      Alt copy = a;
      copy.cost += lazy_cost_delta_;
      copy.enode_id = lazy_enode_id_override_;
      alts_.push_back(std::move(copy));
    }
    lazy_source_ = nullptr;
    lazy_cost_delta_ = 0;
  }

  /// Forward-sweep skyline maintenance.  For each candidate, scans the
  /// running survivor list: drops the candidate if any survivor dominates it;
  /// drops survivors that the candidate dominates; keeps everything else.
  /// Transitivity lets us stop scanning the survivor list once the candidate
  /// itself has been dominated (it can no longer dominate any survivor it
  /// hasn't already inspected, because that would chain via the dominator).
  ///
  /// `pruned_prefix` is the count of leading `alts_` entries the caller
  /// asserts are already mutually Pareto-pruned.  Those entries seed the
  /// survivor list directly and are not pairwise re-checked against each
  /// other; the sweep starts at `pruned_prefix`.  Used by `merge_in_place`,
  /// where `*this` is known pruned before the suffix is appended.  Other
  /// public ops pass 0 (the default).
  void prune(size_t pruned_prefix = 0) {
    auto const n = alts_.size();
    if (n < 2 || n <= pruned_prefix) return;
    // `alts_[0, survivors)` are the survivors found so far.  Each candidate
    // round rebuilds that region in place: `kept` counts the survivors retained
    // so far this round (and is the next slot to write one into, the region
    // starting at index 0).  The gap `j - kept` is the running eviction count.
    size_t survivors = pruned_prefix;
    for (size_t read = pruned_prefix; read < n; ++read) {
      auto candidate = std::move(alts_[read]);
      bool candidate_dominated = false;
      size_t kept = 0;
      for (size_t j = 0; j < survivors; ++j) {
        auto const &incumbent = alts_[j];
        auto const cmp = dominance_compare<Dims...>(incumbent, candidate);
        if (cmp == std::partial_ordering::greater || cmp == std::partial_ordering::equivalent) {
          // Incumbent dominates or ties the candidate, so the candidate is out.
          // No earlier incumbent was evicted: the candidate could only evict an
          // incumbent it dominates, but this incumbent dominates the candidate,
          // so transitivity would make those two incumbents comparable -
          // impossible among survivors.  Hence kept == j and the tail
          // [j, survivors) is untouched, so every incumbent is kept as-is.
          kept = survivors;
          candidate_dominated = true;
          break;
        }
        if (cmp == std::partial_ordering::less) {
          // Candidate dominates this incumbent: drop it (skip writing).
          continue;
        }
        // Unordered: incumbent stays.  Compact it down past any evicted holes.
        if (kept != j) alts_[kept] = std::move(alts_[j]);
        ++kept;
      }
      survivors = kept;
      if (!candidate_dominated) alts_[survivors++] = std::move(candidate);
    }
    alts_.resize(survivors);
  }
};

// ============================================================================
// PickBest
// ============================================================================

/// Returns a pointer to the minimum-cost alternative in `alts` satisfying
/// `pred`, or nullptr if none match.  Caller decides what nullptr means.
template <std::ranges::range Alts, typename Pred>
  requires std::invocable<Pred, std::ranges::range_value_t<Alts> const &>
[[nodiscard]] auto PickBest(Alts &&alts, Pred &&pred) -> std::ranges::range_value_t<Alts> const * {
  using Alt = std::ranges::range_value_t<Alts>;
  Alt const *best = nullptr;
  for (Alt const &alt : alts) {
    if (!std::invoke(pred, alt)) continue;
    if (!best || alt.cost < best->cost) best = &alt;
  }
  return best;
}

// ============================================================================
// CostResultBase
// ============================================================================

/// Base for ParetoFrontier types that use min-cost as their resolve strategy.
/// Derived types get resolve() and convenience constructors for free.  The
/// Self type for *this is deduced via C++23 explicit object parameters;
/// derived classes do not pass themselves through a CRTP template parameter.
/// Alt must have `.cost` (totally ordered) and `.enode_id` fields.
///
/// resolve() is O(N) over surviving alts (frontiers are small, so this stays
/// negligible against the recursion costs).  Storage order of `alts()` is
/// implementation-defined, so resolve() does a full min-element scan rather
/// than relying on a head-of-sorted invariant.
template <typename Alt, typename... Dims>
  requires ParetoAlt<Alt>
struct CostResultBase : ParetoFrontier<Alt, Dims...> {
  using Base = ParetoFrontier<Alt, Dims...>;
  using Base::Base;
  using cost_t = decltype(Alt::cost);

  CostResultBase() = default;

  // NOLINTNEXTLINE(google-explicit-constructor)
  CostResultBase(Base base) : Base(std::move(base)) {}

  CostResultBase(std::initializer_list<Alt> init) : Base(std::vector<Alt>(init)) {}

  // NOLINTNEXTLINE(google-explicit-constructor)
  CostResultBase(std::vector<Alt> alts) : Base(std::vector<Alt>(std::move(alts))) {}

  template <typename Self>
  auto resolve(this Self const &self) -> std::pair<decltype(Alt::enode_id), cost_t const &> {
    auto const alts = self.alts();
    auto it = std::ranges::min_element(alts, {}, &Alt::cost);
    assert(it != alts.end() && "resolve called on empty frontier");
    return {it->enode_id, it->cost};
  }
};

}  // namespace memgraph::planner::core::extract
