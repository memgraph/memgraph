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

#include <cstddef>
#include <cstdint>
#include <functional>
#include <utility>

#include "query/plan_v2/egraph/symbol_lists.hpp"

import memgraph.planner.core.typed_egraph;

namespace memgraph::query::plan::v2 {

/// The `symbol` enum: one entry per X-list row in symbol_lists.hpp.
/// Adding a symbol = adding an X-list entry; arity, descriptor, and
/// AllSymbols all flow from that single edit.
enum struct symbol : std::uint8_t {
// NOLINTBEGIN(cppcoreguidelines-macro-usage)
#define MG_SYM_ENUM_ENTRY(Name) Name,
  EGRAPH_ALL_SYMBOLS(MG_SYM_ENUM_ENTRY)
#undef MG_SYM_ENUM_ENTRY
  // NOLINTEND(cppcoreguidelines-macro-usage)
};

/// Per-arity classification predicates, true for the symbols appearing in
/// the matching X-list in symbol_lists.hpp. Cost-class membership
/// (which binary symbols are arithmetic / comparison / boolean) lives
/// separately in op_ast_lists.hpp.
// NOLINTBEGIN(cppcoreguidelines-macro-usage)
template <symbol S>
constexpr bool is_leaf_v = false;
#define MG_DEFN_PRED(Name) \
  template <>              \
  inline constexpr bool is_leaf_v<symbol::Name> = true;
EGRAPH_LEAF_SYMBOLS(MG_DEFN_PRED)

template <symbol S>
constexpr bool is_unary_op_v = false;
#undef MG_DEFN_PRED
#define MG_DEFN_PRED(Name) \
  template <>              \
  inline constexpr bool is_unary_op_v<symbol::Name> = true;
EGRAPH_UNARY_SYMBOLS(MG_DEFN_PRED)
#undef MG_DEFN_PRED

template <symbol S>
constexpr bool is_binary_op_v = false;
#define MG_DEFN_PRED(Name) \
  template <>              \
  inline constexpr bool is_binary_op_v<symbol::Name> = true;
EGRAPH_BINARY_SYMBOLS(MG_DEFN_PRED)
#undef MG_DEFN_PRED
// NOLINTEND(cppcoreguidelines-macro-usage)

/// Canonical enumeration of all symbols. Consumed by `TypedEGraph` (via
/// egraph_internal.hpp) and any runtime symbol -> property dispatch.
///
/// We use mg-planner's `SymbolSequence<Symbol, Ss...>` directly as the
/// pack-holder. This is the same type `TypedEGraph` consumes, so plan_v2 talks
/// to the library in its native vocabulary and the reference-client lesson is
/// "use `planner::core::SymbolSequence<MyEnum, Ss...>` for your own enum."
using AllSymbols = memgraph::planner::core::SymbolSequence<symbol
// NOLINTBEGIN(cppcoreguidelines-macro-usage)
#define MG_SYM_LIST_ENTRY(Name) , symbol::Name
                                                               EGRAPH_ALL_SYMBOLS(MG_SYM_LIST_ENTRY)
#undef MG_SYM_LIST_ENTRY
                                                           // NOLINTEND(cppcoreguidelines-macro-usage)
                                                           >;

}  // namespace memgraph::query::plan::v2

namespace std {

template <>
struct hash<memgraph::query::plan::v2::symbol> {
  size_t operator()(memgraph::query::plan::v2::symbol const &value) const noexcept { return std::to_underlying(value); }
};
}  // namespace std
