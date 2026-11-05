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

#include <cstdint>
#include <map>
#include <optional>
#include <span>
#include <string>
#include <string_view>
#include <vector>

#include <boost/unordered/unordered_flat_map.hpp>
#include <boost/unordered/unordered_node_map.hpp>

#include "query/plan_v2/egraph/analysis.hpp"
#include "query/plan_v2/egraph/builtin_functions.hpp"
#include "query/plan_v2/egraph/constant_identity.hpp"
#include "query/plan_v2/egraph/interner.hpp"
#include "query/plan_v2/egraph/symbol.hpp"
#include "storage/v2/property_value.hpp"
#include "utils/small_vector.hpp"

import memgraph.planner.core.egraph;
import memgraph.planner.core.typed_egraph;

namespace memgraph::query::plan::v2 {

/// What every `symbol_make_traits<S>::make` returns: a lowered e-node plus the
/// analysis seed for the e-class it lands in. The seed gives a new e-class the
/// variant arm for its kind (the make half of e-class analysis) rather than a
/// default-constructed one.
using seeded_node = planner::core::MakeResult<analysis>;

/// The empty analysis seed for `S`: the variant arm matching its e-class kind.
/// Facts (e.g. a Literal's value) are layered on by the symbol's own `make`.
template <symbol S>
auto default_analysis_seed() -> analysis {
  if constexpr (is_symbol_kind_v<S>) {
    return analysis{SymbolAnalysis{}};
  } else if constexpr (is_operator_kind_v<S>) {
    return analysis{OperatorAnalysis{}};
  } else {
    static_assert(is_expression_kind_v<S>);
    return analysis{ExpressionAnalysis{}};
  }
}

// ========================================================================
// symbol_make_traits - per-symbol lowering for the TypedEGraph protocol.
//
// Each specialisation provides:
//   - storage_type: per-symbol side-data (interning maps, counters, ...);
//     empty struct if none.
//   - static auto make(storage_type&, user_args...) -> seeded_node:
//     lowers user arguments to (children, optional disambiguator) and the
//     analysis seed for the new e-class.
//   - optionally static auto update(...) -> bool: the facts an e-node's children
//     imply, recomputed on every analysis refresh. A fact belongs here rather
//     than in the seed whenever it reads a child's analysis, because a child can
//     be proved constant long after this e-node was built, and a seed is fixed
//     when the e-node is built and dropped on a hash-cons hit.
//
// The protocol talks in raw planner::core::EClassId; the plan_v2 strong-typed
// `eclass` is converted at the facade boundary in egraph.cpp.
// ========================================================================

template <symbol S>
struct symbol_make_traits;

/// Once: auto-incrementing counter
template <>
struct symbol_make_traits<symbol::Once> {
  struct storage_type {
    uint64_t counter = 0;
  };

  static auto make(storage_type &s) -> seeded_node;
};

/// Symbol: position -> name mapping
template <>
struct symbol_make_traits<symbol::Symbol> {
  struct storage_type {
    std::map<int32_t, SymbolInfo> store;
  };

  static auto make(storage_type &s, int32_t pos, std::string_view name, int64_t token_position = -1) -> seeded_node;
};

/// Literal: the value is interned and its id is the disambiguator; the Builder
/// recovers the value from the reverse table. The reverse entry points at the
/// interned key rather than copying it, so a large list or map constant is
/// stored once.
template <>
struct symbol_make_traits<symbol::Literal> {
  struct storage_type {
    // Node-stable (not flat) because the reverse entries point into the keys;
    // O(1) hashing vs the ordered map's operator<=> chain. Keyed on constant
    // identity, the relation the analysis merge uses: finer than
    // `operator<=>`, whose coercion would intern `1` and `1.0` as one literal,
    // and where NaN is one constant rather than never-equal-to-itself.
    using map_type =
        boost::unordered_node_map<storage::ExternalPropertyValue, uint64_t, ConstantIdentityHash, ConstantIdentityEq>;
    Interner<map_type, storage::ExternalPropertyValue const *> values;
  };

  static auto make(storage_type &s, storage::ExternalPropertyValue const &value) -> seeded_node;
};

/// ParamLookup: no storage, position IS the disambiguator
template <>
struct symbol_make_traits<symbol::ParamLookup> {
  struct storage_type {};

  static auto make(storage_type &, int32_t pos) -> seeded_node;
};

/// Bind: no storage, just children
template <>
struct symbol_make_traits<symbol::Bind> {
  struct storage_type {};

  static auto make(storage_type &, planner::core::EClassId input, planner::core::EClassId sym,
                   planner::core::EClassId expr) -> seeded_node;
};

/// Identifier: no storage, just child
template <>
struct symbol_make_traits<symbol::Identifier> {
  struct storage_type {};

  static auto make(storage_type &, planner::core::EClassId sym) -> seeded_node;
};

/// Output: no storage, prepends input to children
template <>
struct symbol_make_traits<symbol::Output> {
  struct storage_type {};

  static auto make(storage_type &, utils::small_vector<planner::core::EClassId> children) -> seeded_node;
};

/// NamedOutput: the output name is interned and its id is the disambiguator.
/// The reverse entry views the interned key rather than copying the name, which
/// is why the map is an ordered one - its nodes keep their address.
template <>
struct symbol_make_traits<symbol::NamedOutput> {
  struct storage_type {
    Interner<std::map<std::string, uint64_t>, std::string_view> names;
  };

  static auto make(storage_type &s, std::string_view name, planner::core::EClassId sym, planner::core::EClassId expr)
      -> seeded_node;
};

/// Function: interns the name to a stable id (the disambiguator) and caches its
/// BuiltinKind at that point, so dispatch is an id lookup rather than a per-call
/// string compare. Children are the argument e-classes; the id space is shared
/// between builtins and UDFs.
template <>
struct symbol_make_traits<symbol::Function> {
  struct storage_type {
    /// Flat is enough here: the reverse entry owns its name rather than
    /// borrowing the key, since the Builder needs the user's own spelling and
    /// the record carries the classification beside it.
    Interner<boost::unordered::unordered_flat_map<std::string, uint64_t>, FunctionInfo> functions;
  };

  /// args is the complete children list (the function arguments). `is_pure` is
  /// cached on the FunctionInfo (see its doc).
  static auto make(storage_type &s, std::string_view name, utils::small_vector<planner::core::EClassId> args,
                   bool is_pure) -> seeded_node;

  /// What a builtin's semantics establish about its result from its arguments:
  /// `range` over constant integer bounds has a known length, and `size` of a
  /// known-length list is that length as a constant. Which of those applies is
  /// decided by the interned name, so this needs the function id and the store
  /// that holds it; the arguments' facts alone do not say.
  static auto update(planner::core::EGraph<symbol, analysis> const &eg, storage_type const &s,
                     std::span<planner::core::EClassId const> args, std::uint64_t function_id, analysis &out) -> bool;
};

/// Unwind: no storage, mirrors Bind's [input, sym, list_expr] shape so the
/// resolver's alive-Bind dispatch covers Unwind without a second branch.
template <>
struct symbol_make_traits<symbol::Unwind> {
  struct storage_type {};

  static auto make(storage_type &, planner::core::EClassId input, planner::core::EClassId sym,
                   planner::core::EClassId list_expr) -> seeded_node;
};

/// Filter: no storage; children [input, predicate_expr]. Binds nothing (no sym child).
template <>
struct symbol_make_traits<symbol::Filter> {
  struct storage_type {};

  static auto make(storage_type &, planner::core::EClassId input, planner::core::EClassId predicate) -> seeded_node;
};

/// Subquery: no storage; children are [outer_input, inner_root, exposed_syms...].
/// Variadic to encode the projection set of the inner block as direct e-graph
/// children, so the cost case and resolver don't have to peek into inner_root's
/// enode shape to discover what crosses the scope barrier.
template <>
struct symbol_make_traits<symbol::Subquery> {
  struct storage_type {};

  static auto make(storage_type &, utils::small_vector<planner::core::EClassId> children) -> seeded_node;
};

/// Binary operator: no storage, just two children
template <symbol S>
  requires(is_binary_op_v<S>)
struct symbol_make_traits<S> {
  struct storage_type {};

  static auto make(storage_type & /*s*/, planner::core::EClassId lhs, planner::core::EClassId rhs) -> seeded_node {
    return {.lowered = {.children = utils::small_vector{lhs, rhs}, .disambiguator = std::nullopt},
            .seed = default_analysis_seed<S>()};
  }
};

/// Add carries what list concatenation fixes: concatenating two known-length
/// lists gives their sum. Only a list's length answers `known_list_length`, so
/// both operands answering means list + list rather than numeric addition or an
/// append, which produce a different length. Spelled out rather than extending
/// the primary, whose other operators fix no length.
template <>
struct symbol_make_traits<symbol::Add> {
  struct storage_type {};

  static auto make(storage_type & /*s*/, planner::core::EClassId lhs, planner::core::EClassId rhs) -> seeded_node {
    return {.lowered = {.children = utils::small_vector{lhs, rhs}, .disambiguator = std::nullopt},
            .seed = default_analysis_seed<symbol::Add>()};
  }

  static auto update(planner::core::EGraph<symbol, analysis> const &eg, storage_type const & /*s*/,
                     std::span<planner::core::EClassId const> children, std::uint64_t /*disambiguator*/, analysis &out)
      -> bool;
};

/// Unary operator: no storage, just one child
template <symbol S>
  requires(is_unary_op_v<S>)
struct symbol_make_traits<S> {
  struct storage_type {};

  static auto make(storage_type & /*s*/, planner::core::EClassId operand) -> seeded_node {
    return {.lowered = {.children = utils::small_vector{operand}, .disambiguator = std::nullopt},
            .seed = default_analysis_seed<S>()};
  }
};

}  // namespace memgraph::query::plan::v2
