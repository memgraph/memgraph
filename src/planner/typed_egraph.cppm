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
#include <cstdint>
#include <optional>
#include <utility>

#include "utils/small_vector.hpp"

export module memgraph.planner.core.typed_egraph;

import memgraph.planner.core.egraph;

export namespace memgraph::planner::core {

/// One emplace operation lowered to the core e-graph's contract: a child
/// list plus an optional disambiguator. Trait specialisations return this
/// from `make()`; `TypedEGraph::Make<S>` shuttles it into
/// `EGraph<Symbol, Analysis>::emplace`.
struct LoweredNode {
  utils::small_vector<EClassId> children;
  std::optional<std::uint64_t> disambiguator;
};

/// Per-symbol trait contract. A `Traits<S>` specialisation must expose:
///   - `storage_type`: per-symbol side-data (interning maps, counters,
///     etc.). Empty struct if none.
///   - `static auto make(storage_type&, Args...) -> LoweredNode`: maps the
///     user's per-symbol arguments to (children, optional disambiguator),
///     updating storage as needed.
///
/// The concept is parameterised on `Args` so that calls with the wrong
/// arity or wrong types fail at the constraint with a clear message
/// rather than deep inside `TypedEGraph::Make`.
template <typename T, typename... Args>
concept SymbolMakeTraits =
    std::is_default_constructible_v<typename T::storage_type> && requires(typename T::storage_type &s, Args &&...args) {
      { T::make(s, std::forward<Args>(args)...) } -> std::same_as<LoweredNode>;
    };

/// Compile-time list of Symbol values. Used to derive the combined storage
/// type for a `TypedEGraph` instance.
template <typename SymbolT, SymbolT... Ss>
struct SymbolSequence {};

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
/// `Make<S>(args...)` returns the resulting `EClassId`. Callers that want
/// strong-typed e-class wrappers should wrap the returned id at the
/// boundary (the trait protocol itself talks in raw `EClassId`).
template <typename Symbol, typename Analysis, typename SymbolSeq, template <auto> class Traits>
class TypedEGraph {
 public:
  TypedEGraph() = default;
  TypedEGraph(TypedEGraph &&) noexcept = default;
  TypedEGraph &operator=(TypedEGraph &&) noexcept = default;

  /// Construct (or find) the e-class for the enode `S(args...)`. Dispatches
  /// to `Traits<S>::make` to lower user args into a `LoweredNode`, then
  /// emplaces into the core e-graph.
  template <Symbol S, typename... Args>
    requires SymbolMakeTraits<Traits<S>, Args...>
  auto Make(Args &&...args) -> EClassId {
    auto lowered = Traits<S>::make(storage<S>(), std::forward<Args>(args)...);
    auto const res = lowered.disambiguator ? core_.emplace(S, std::move(lowered.children), *lowered.disambiguator)
                                           : core_.emplace(S, std::move(lowered.children));
    return res.eclass_id;
  }

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
  /// extraction all consume `EGraph<Symbol, Analysis>` directly.
  auto core() -> EGraph<Symbol, Analysis> & { return core_; }

  auto core() const -> EGraph<Symbol, Analysis> const & { return core_; }

 private:
  EGraph<Symbol, Analysis> core_;
  SymbolStorageFor<SymbolSeq, Traits> storage_;
};

}  // namespace memgraph::planner::core
