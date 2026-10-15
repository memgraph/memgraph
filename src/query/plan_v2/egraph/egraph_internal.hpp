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

import memgraph.planner.core.egraph;
import memgraph.planner.core.typed_egraph;

#include <span>

#include "query/plan_v2/egraph/egraph.hpp"
#include "query/plan_v2/egraph/symbol.hpp"
#include "query/plan_v2/egraph/symbol_make_traits.hpp"
#include "query/plan_v2/resolve/analysis.hpp"
#include "utils/small_vector.hpp"

namespace memgraph::query::plan::v2 {

// ========================================================================
// egraph::impl - holds a TypedEGraph by composition. Future plan_v2-specific
// side-data (caches, interners) can become sibling members without touching
// the inheritance line. Per-symbol lowering lives in `symbol_make_traits<S>`
// specialisations.
// ========================================================================

/// The concrete TypedEGraph plan_v2 builds on. Rewrite rules whose apply mints
/// interned nodes (`ctx.Make<S>`) are parameterised on this so the rule context
/// can reach the per-symbol trait interners; rules that only merge need not be.
using typed_egraph = planner::core::TypedEGraph<symbol, analysis, AllSymbols, symbol_make_traits>;

struct egraph::impl {
  typed_egraph graph;
};

/// Pimpl back-door for plan_v2-internal TUs (converter, rewriter, estimator).
/// Declared `friend` on `egraph` so private member access works.
auto impl_of(egraph &e) -> egraph::impl &;
auto impl_of(egraph const &e) -> egraph::impl const &;

/// Strong-type boundary conversions between plan_v2's `eclass` and the core's
/// `EClassId`. Free functions because `eclass::value_of()` is public; no
/// friendship needed.
inline auto to_core(eclass id) -> planner::core::EClassId { return planner::core::EClassId{id.value_of()}; }

inline auto from_core(planner::core::EClassId id) -> eclass { return eclass{id.value_of()}; }

/// Build a `small_vector<EClassId>` from a span of eclass; the trait protocol
/// (`symbol_make_traits<S>::make`) consumes this directly, removing the
/// intermediate `std::vector` that the public-API wrappers used to build.
inline auto to_core(std::span<eclass const> ids) -> utils::small_vector<planner::core::EClassId> {
  auto out = utils::small_vector<planner::core::EClassId>{};
  out.reserve(ids.size());
  for (const auto id : ids) out.push_back(to_core(id));
  return out;
}

}  // namespace memgraph::query::plan::v2
