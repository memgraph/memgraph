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

module;

#include <concepts>
#include <cstddef>
#include <cstdint>
#include <optional>
#include <span>
#include <type_traits>
#include <utility>

#include "utils/small_vector.hpp"

export module memgraph.planner.core.typed_egraph;

import memgraph.planner.core.egraph;

export namespace memgraph::planner::core {

/// One emplace operation lowered to the core e-graph's contract: a child
/// list plus an optional disambiguator.
struct LoweredNode {
  utils::small_vector<EClassId> children;
  std::optional<std::uint64_t> disambiguator;
};

/// What `make()` returns: the lowered e-node plus the new e-class's analysis
/// seed. `seed` is default-constructed when a symbol attaches no facts (empty
/// for `NoAnalysis`). `TypedEGraph::Make<S>` passes both to `EGraph::emplace`.
template <typename Analysis>
struct MakeResult {
  LoweredNode lowered;
  Analysis seed{};
};

/// Per-symbol trait contract. A `Traits<S>` specialisation must expose:
///   - `storage_type`: per-symbol side-data (interning maps, counters,
///     etc.). Empty struct if none.
///   - `static auto make(storage_type&, Args...) -> MakeResult<Analysis>`:
///     lowers the user's per-symbol arguments and seeds the new e-class's
///     analysis, updating storage as needed.
///
/// The `seed` must be a pure function of the e-node's identity (symbol,
/// children, disambiguator): on a hash-cons hit the new seed is discarded and
/// the existing e-class keeps its own, so equal identity must imply an agreeing
/// seed (a fact from insertion order or ambient state breaks this).
/// `EGraph::emplace` enforces it on every hit by merging the discarded seed
/// against the kept analysis, throwing on contradiction.
///
/// The concept is parameterised on `Args` so that calls with the wrong
/// arity or wrong types fail at the constraint with a clear message
/// rather than deep inside `TypedEGraph::Make`.
template <typename T, typename Analysis, typename... Args>
concept SymbolMakeTraits =
    std::is_default_constructible_v<typename T::storage_type> && requires(typename T::storage_type &s, Args &&...args) {
      { T::make(s, std::forward<Args>(args)...) } -> std::same_as<MakeResult<Analysis>>;
    };

/// A symbol whose facts depend on its children's, and so must be recomputed
/// when theirs move. Optional: a symbol without `update` keeps whatever its
/// make-time seed established.
///
/// `update` widens `out` from the children's current analyses and answers
/// whether it changed anything. It writes in place so that recomputing a fact
/// costs no copy of the analysis, which for a fact holding a string or a list
/// would allocate.
///
/// Two requirements the signature cannot state. It must be a pure function of
/// the children's analyses, so that two e-classes with the same children agree.
/// And it must only ever add a fact, never retract one: that is what makes the
/// refresh terminate, and what lets a rewrite trust a fact it has already read.
///
/// A trait that declines the protocol keeps its seed, so a near miss - a
/// misspelled name, a swapped parameter, a void return - costs no error and
/// stops the symbol's facts being recomputed. The second requirement below is
/// what pins the one near miss that would otherwise satisfy the first: taking
/// `out` by value compiles, widens a copy, and discards it. Only a reference
/// parameter refuses a temporary.
template <typename T, typename Analysis, typename EGraphT>
concept SymbolUpdatesAnalysis = requires(EGraphT const &eg, std::span<EClassId const> children, Analysis &out) {
  { T::update(eg, children, out) } -> std::same_as<bool>;
} && !requires(EGraphT const &eg, std::span<EClassId const> children) { T::update(eg, children, Analysis{}); };

/// A symbol's update, or a no-op for one that offers none, so the refresh can
/// call every symbol alike.
template <typename Analysis, typename EGraphT, typename TraitT>
struct AnalysisUpdate {
  static constexpr bool updates = SymbolUpdatesAnalysis<TraitT, Analysis, EGraphT>;

  static auto apply(EGraphT const &eg, std::span<EClassId const> children, Analysis &out) -> bool {
    if constexpr (updates) {
      return TraitT::update(eg, children, out);
    } else {
      return false;
    }
  }
};

/// Compile-time list of Symbol values. Used to derive the combined storage
/// type for a `TypedEGraph` instance.
template <typename SymbolT, SymbolT... Ss>
struct SymbolSequence {};

namespace detail {

/// Call `f` with the symbol as a template argument and answer what it answers.
/// The fold expands to a direct call per symbol, so the compiler sees an
/// ordinary branch chain and can inline through it; nothing is dispatched
/// through a pointer. A symbol outside the sequence answers false, which cannot
/// arise while the sequence lists every symbol its graph can hold.
template <typename SymbolT, SymbolT... Ss, typename F>
auto DispatchSymbol(SymbolSequence<SymbolT, Ss...> /*seq*/, SymbolT s, F &&f) -> bool {
  auto answer = false;
  (void)(... || (s == Ss ? (answer = f.template operator()<Ss>(), true) : false));
  return answer;
}

/// Whether any symbol in the set recomputes facts from its children.
template <typename Analysis, typename EGraphT, template <auto> class Traits, typename SymbolT, SymbolT... Ss>
constexpr auto AnySymbolUpdates(SymbolSequence<SymbolT, Ss...> /*seq*/) -> bool {
  return (AnalysisUpdate<Analysis, EGraphT, Traits<Ss>>::updates || ...);
}

}  // namespace detail

/// Bag of per-symbol storages combined via inheritance, so a static_cast
/// recovers any individual storage by type. Mirrors the "overloads" trick.
template <typename... Ts>
struct CombinedStorage : Ts... {};

namespace detail {

template <typename Seq, template <auto> class Traits>
struct CombinedStorageFor;

template <typename SymbolT, SymbolT... Ss, template <auto> class Traits>
struct CombinedStorageFor<SymbolSequence<SymbolT, Ss...>, Traits> {
  using type = CombinedStorage<typename Traits<Ss>::storage_type...>;
};

}  // namespace detail

/// Derive a `CombinedStorage<...>` from a `SymbolSequence` + a `Traits`
/// template. The resulting type contains one `Traits<S>::storage_type`
/// sub-object per `S` in the sequence.
template <typename Seq, template <auto> class Traits>
using SymbolStorageFor = typename detail::CombinedStorageFor<Seq, Traits>::type;

/// E-graph wrapper that adds typed `Make<S>(args...)` dispatch on top of
/// `EGraph<Symbol, Analysis>`. Per-symbol semantics live entirely in
/// `Traits<S>` specialisations; this class owns the e-graph, the combined
/// trait storage, and the one place where lowered nodes are emplaced into
/// the core.
///
/// `Make<S>` returns the full `EmplaceResult`, so callers can tell a fresh
/// insert from a hash-cons hit; most just take `.eclass_id`.
template <typename Symbol, typename Analysis, typename SymbolSeq, template <auto> class Traits>
class TypedEGraph {
 public:
  /// Mirror EGraph's aliases so a TypedEGraph models the same graph concept the
  /// rewrite engine drives.
  using symbol_type = Symbol;
  using analysis_type = Analysis;
  using egraph_type = EGraph<Symbol, Analysis>;

  TypedEGraph() { core_.set_tracks_analysis(updates_analysis); }

  TypedEGraph(TypedEGraph &&) noexcept = default;
  TypedEGraph &operator=(TypedEGraph &&) noexcept = default;

  /// Construct (or find) the e-class for the enode `S(args...)`. Dispatches
  /// to `Traits<S>::make` to lower user args and seed the e-class's analysis,
  /// then emplaces into the core e-graph.
  template <Symbol S, typename... Args>
    requires SymbolMakeTraits<Traits<S>, Analysis, Args...>
  auto Make(Args &&...args) -> EmplaceResult {
    auto made = Traits<S>::make(storage<S>(), std::forward<Args>(args)...);
    auto const disambiguator = made.lowered.disambiguator.value_or(0);
    return core_.emplace(S, std::move(made.lowered.children), disambiguator, std::move(made.seed));
  }

  /// Whether this graph has any facts to recompute. False leaves
  /// `refresh_analysis` with nothing it could find, so the walk is dropped
  /// rather than run to no effect; a consumer that declares no update pays
  /// nothing for the phase existing.
  static constexpr bool updates_analysis = detail::AnySymbolUpdates<Analysis, egraph_type, Traits>(SymbolSeq{});

  /// Recompute the facts a change this pass can reach, and report how many
  /// e-classes moved. The core owns the propagation, which follows the same
  /// parent edges congruence does; this supplies only the per-symbol meaning.
  ///
  /// A make-time seed reads the children's facts as they stood when the e-node
  /// was built. A later merge can establish a fact the seed could not derive,
  /// and merging widens only the class it merged - never a parent whose own
  /// facts came from it. This is the pass that closes that gap.
  template <egraph_type::AnalysisScope Scope = egraph_type::AnalysisScope::Changed>
  auto refresh_analysis(BaseProcessingContext &ctx) -> std::size_t {
    if constexpr (!updates_analysis) return 0;
    return core_.template refresh_analysis<Scope>(ctx, node_updater());
  }

 private:
  /// What one e-node contributes to its e-class's facts, by symbol.
  auto node_updater() {
    return [this](auto const &enode, Analysis &facts) {
      return detail::DispatchSymbol(SymbolSeq{}, enode.symbol(), [&]<Symbol S>() {
        using Update = AnalysisUpdate<Analysis, egraph_type, Traits<S>>;
        return Update::apply(core_, std::span<EClassId const>{enode.children()}, facts);
      });
    };
  }

 public:
  /// Typed read/write access to one symbol's side-data. Callers use this
  /// to look up interned data by id (names, literals, function info, ...).
  template <Symbol S>
  auto storage() -> typename Traits<S>::storage_type & {
    return static_cast<typename Traits<S>::storage_type &>(storage_);
  }

  template <Symbol S>
  auto storage() const -> typename Traits<S>::storage_type const & {
    return static_cast<typename Traits<S>::storage_type const &>(storage_);
  }

  /// Access to the underlying core e-graph. Rewriters, matchers, and
  /// extraction all consume `EGraph<Symbol, Analysis>` directly. Prefer
  /// `Make<S>` to seed new e-classes: it derives the seed's kind from `S` at
  /// compile time, which a caller-built seed passed to raw `emplace` does not.
  auto core() -> egraph_type & { return core_; }

  auto core() const -> egraph_type const & { return core_; }

 private:
  egraph_type core_;
  SymbolStorageFor<SymbolSeq, Traits> storage_;
};

}  // namespace memgraph::planner::core
