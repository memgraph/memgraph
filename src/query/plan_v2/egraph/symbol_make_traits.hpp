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
#include <string>
#include <string_view>
#include <vector>

#include <boost/unordered/unordered_flat_map.hpp>
#include <boost/unordered/unordered_node_map.hpp>

#include "query/plan_v2/egraph/builtin_functions.hpp"
#include "query/plan_v2/egraph/symbol.hpp"
#include "query/plan_v2/resolve/analysis.hpp"
#include "query/plan_v2/resolve/constant_identity.hpp"
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

/// Literal: value <-> id mapping.  `store` (value -> id) is the hash-consing
/// direction used by `make`; `info` (id -> value, indexed by disambiguator)
/// is the reverse used by the Builder and BuiltinEstimator.  `info` holds
/// pointers into `store`'s node-stable keys -- no key duplication, valid as
/// long as `store` outlives the reads.  Kept in lockstep by `make`.
template <>
struct symbol_make_traits<symbol::Literal> {
  struct storage_type {
    // Hash-consed value -> id. Node-stable (not flat) so `info`'s pointers stay
    // valid; O(1) hashing vs the ordered map's operator<=> chain. Keyed on
    // constant identity, the relation the analysis merge uses: finer than
    // `operator<=>`, whose coercion would intern `1` and `1.0` as one literal,
    // and where NaN is one constant rather than never-equal-to-itself.
    boost::unordered_node_map<storage::ExternalPropertyValue, uint64_t, ConstantIdentityHash, ConstantIdentityEq> store;
    std::vector<storage::ExternalPropertyValue const *> info;
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

/// NamedOutput: name <-> id mapping + children.  `store` (name -> id) is the
/// hash-consing direction; `info` (id -> name, indexed by disambiguator) is
/// the reverse used by the Builder.  `info` holds string_views into `store`'s
/// node-stable keys -- no name duplication, valid as long as `store` outlives
/// the reads.  Kept in lockstep by `make`.
template <>
struct symbol_make_traits<symbol::NamedOutput> {
  struct storage_type {
    std::map<std::string, uint64_t> store;
    std::vector<std::string_view> info;
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
    /// name -> stable id.
    boost::unordered::unordered_flat_map<std::string, uint64_t> store;
    /// id -> FunctionInfo, parallel to `store`. `intern` is the sole writer of
    /// both, keeping them in lockstep; do not insert directly.
    std::vector<FunctionInfo> info;

    /// Intern a name to its stable id, classifying BuiltinKind and caching
    /// `is_pure` on first sight.  The same name always returns the same id.
    auto intern(std::string_view name, bool is_pure) -> uint64_t;
  };

  /// args is the complete children list (the function arguments).
  /// `seed` carries any analysis facts the caller can establish from the
  /// builtin's semantics (e.g. `range`'s known length, or `size`'s known
  /// constant value); empty for opaque functions.  `is_pure` is cached on the
  /// FunctionInfo (see its doc); the caller has already gated `seed` on it.
  static auto make(storage_type &s, std::string_view name, utils::small_vector<planner::core::EClassId> args,
                   ExpressionAnalysis seed, bool is_pure) -> seeded_node;
};

/// Unwind: no storage, mirrors Bind's [input, sym, list_expr] shape so the
/// resolver's alive-Bind dispatch covers Unwind without a second branch.
template <>
struct symbol_make_traits<symbol::Unwind> {
  struct storage_type {};

  static auto make(storage_type &, planner::core::EClassId input, planner::core::EClassId sym,
                   planner::core::EClassId list_expr) -> seeded_node;
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

  /// `known_list_length` seeds the result when the operator's semantics fix the
  /// produced list's length over its operands (list concatenation over two
  /// known-length lists); nullopt leaves the e-class without the fact.
  static auto make(storage_type & /*s*/, planner::core::EClassId lhs, planner::core::EClassId rhs,
                   std::optional<std::size_t> known_list_length = std::nullopt) -> seeded_node {
    return {.lowered = {.children = utils::small_vector{lhs, rhs}, .disambiguator = std::nullopt},
            .seed = analysis{ExpressionAnalysis{.known_list_length = known_list_length}}};
  }
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
