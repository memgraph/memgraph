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

// NOTE: This header should NOT be included by public API consumers.
// It exposes implementation details of the egraph pimpl.

#include <ranges>

import memgraph.planner.core.egraph;

#include "query/plan_v2/egraph.hpp"
#include "query/plan_v2/private_analysis.hpp"
#include "query/plan_v2/private_symbol.hpp"
#include "query/plan_v2/symbol_make_traits.hpp"

namespace memgraph::query::plan::v2 {

/**
 * @brief Internal accessor for egraph implementation details
 */
struct internal {
  static auto get_impl(egraph const &e) -> egraph::impl const &;
  static auto get_impl(egraph &e) -> egraph::impl &;

  static auto to_core_id(eclass id) -> memgraph::planner::core::EClassId {
    return memgraph::planner::core::EClassId{id.value_of()};
  }

  static auto from_core_id(memgraph::planner::core::EClassId id) -> eclass { return eclass{id.value_of()}; }
};

// ========================================================================
// egraph::impl definition
// ========================================================================

struct egraph::impl {
  impl() = default;
  impl(impl &&) = default;
  impl &operator=(impl &&) = default;
  ~impl() = default;

  // Storage accessor - extracts specific symbol's storage from combined storage
  template <symbol S>
  auto storage() -> typename symbol_make_traits<S>::storage_type & {
    return static_cast<typename symbol_make_traits<S>::storage_type &>(storage_);
  }

  template <symbol S>
  auto storage() const -> typename symbol_make_traits<S>::storage_type const & {
    return static_cast<typename symbol_make_traits<S>::storage_type const &>(storage_);
  }

  /// Unified emplace entry point. All per-symbol semantics (what side-data,
  /// what user args, how to lower them) live in `symbol_make_traits<S>`.
  /// This function is the single place where lowered (children, disambiguator)
  /// shapes are turned into a call on the core e-graph — change tracing,
  /// instrumentation, or the core emplace contract here once, not per-symbol.
  template <symbol S, typename... Args>
    requires SymbolMakeTraits<symbol_make_traits<S>, Args...>
  auto Make(Args &&...args) -> eclass {
    auto const lowered = symbol_make_traits<S>::make(storage<S>(), std::forward<Args>(args)...);
    auto core_children = lowered.children | std::views::transform([](eclass e) { return internal::to_core_id(e); }) |
                         std::ranges::to<utils::small_vector<planner::core::EClassId>>();
    auto const res = lowered.disambiguator ? egraph_.emplace(S, std::move(core_children), *lowered.disambiguator)
                                           : egraph_.emplace(S, std::move(core_children));
    return internal::from_core_id(res.eclass_id);
  }

  memgraph::planner::core::EGraph<symbol, analysis> egraph_;
  symbol_storage storage_;
};

}  // namespace memgraph::query::plan::v2
