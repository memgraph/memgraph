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
#include <concepts>
#include <cstdint>
#include <iterator>
#include <map>
#include <optional>
#include <string>
#include <string_view>
#include <vector>

#include "query/plan_v2/egraph.hpp"
#include "query/plan_v2/private_symbol.hpp"
#include "storage/v2/property_value.hpp"
#include "utils/small_vector.hpp"

namespace memgraph::query::plan::v2 {

// ========================================================================
// symbol_make_traits — single place where per-symbol semantics live.
//
// Every specialisation provides:
//   - storage_type: what auxiliary side-data this symbol needs (interning
//     maps, counters, etc). Empty struct if none.
//   - make(storage, user_args...) -> lowered_node: maps the user-facing
//     constructor arguments to (children, optional disambiguator), updating
//     storage as needed. The caller (egraph::impl::Make) does the actual
//     emplace into the core e-graph using the returned lowered form.
//
// The lowering contract lives in exactly one place (this header). Adding a
// new symbol is one specialisation plus one facade method on egraph.
// ========================================================================

struct lowered_node {
  utils::small_vector<eclass> children;
  std::optional<uint64_t> disambiguator;
};

template <symbol S>
struct symbol_make_traits;

/// Concept every symbol_make_traits<S> specialisation must satisfy for a given
/// user-arg pack. Verifies both that the storage_type is well-formed and that
/// make() is callable with (storage&, Args...) returning lowered_node, so a
/// malformed trait or a wrong-arity call fails at the constraint with a clear
/// message rather than deep inside impl::Make.
template <typename T, typename... Args>
concept SymbolMakeTraits =
    std::is_default_constructible_v<typename T::storage_type> && requires(typename T::storage_type &s, Args &&...args) {
      { T::make(s, std::forward<Args>(args)...) } -> std::same_as<lowered_node>;
    };

/// Once: auto-incrementing counter
template <>
struct symbol_make_traits<symbol::Once> {
  struct storage_type {
    uint64_t counter = 0;
  };

  static auto make(storage_type &s) -> lowered_node { return {.children = {}, .disambiguator = s.counter++}; }
};

/// Symbol: position -> name mapping
template <>
struct symbol_make_traits<symbol::Symbol> {
  struct storage_type {
    std::map<int32_t, std::string> store;
  };

  static auto make(storage_type &s, int32_t pos, std::string_view name) -> lowered_node {
    s.store.try_emplace(pos, std::string{name});
    return {.children = {}, .disambiguator = static_cast<uint64_t>(pos)};
  }
};

/// Literal: value -> id mapping
template <>
struct symbol_make_traits<symbol::Literal> {
  struct storage_type {
    std::map<storage::ExternalPropertyValue, uint64_t> store;
    uint64_t next_id = 0;
  };

  static auto make(storage_type &s, storage::ExternalPropertyValue const &value) -> lowered_node {
    auto [it, inserted] = s.store.try_emplace(value, s.next_id);
    if (inserted) ++s.next_id;
    return {.children = {}, .disambiguator = it->second};
  }
};

/// ParamLookup: no storage, position IS the disambiguator
template <>
struct symbol_make_traits<symbol::ParamLookup> {
  struct storage_type {};

  static auto make(storage_type &, int32_t pos) -> lowered_node {
    return {.children = {}, .disambiguator = static_cast<uint64_t>(pos)};
  }
};

/// Bind: no storage, just children
template <>
struct symbol_make_traits<symbol::Bind> {
  struct storage_type {};

  static auto make(storage_type &, eclass input, eclass sym, eclass expr) -> lowered_node {
    return {.children = utils::small_vector<eclass>{input, sym, expr}, .disambiguator = std::nullopt};
  }
};

/// Identifier: no storage, just child
template <>
struct symbol_make_traits<symbol::Identifier> {
  struct storage_type {};

  static auto make(storage_type &, eclass sym) -> lowered_node {
    return {.children = utils::small_vector<eclass>{sym}, .disambiguator = std::nullopt};
  }
};

/// Output: no storage, prepends input to children
template <>
struct symbol_make_traits<symbol::Output> {
  struct storage_type {};

  static auto make(storage_type &, eclass input, std::vector<eclass> named_outputs) -> lowered_node {
    auto children = utils::small_vector<eclass>{};
    children.reserve(named_outputs.size() + 1);
    children.push_back(input);
    std::ranges::copy(named_outputs, std::back_inserter(children));
    return {.children = std::move(children), .disambiguator = std::nullopt};
  }
};

/// NamedOutput: name -> id mapping + children
template <>
struct symbol_make_traits<symbol::NamedOutput> {
  struct storage_type {
    std::map<std::string, uint64_t> store;
    uint64_t next_id = 0;
  };

  static auto make(storage_type &s, std::string_view name, eclass sym, eclass expr) -> lowered_node {
    auto [it, inserted] = s.store.try_emplace(std::string{name}, s.next_id);
    if (inserted) ++s.next_id;
    return {.children = utils::small_vector<eclass>{sym, expr}, .disambiguator = it->second};
  }
};

/// Binary operator: no storage, just two children
template <symbol S>
  requires(is_binary_op_v<S>)
struct symbol_make_traits<S> {
  struct storage_type {};

  static auto make(storage_type & /*s*/, eclass lhs, eclass rhs) -> lowered_node {
    return {.children = utils::small_vector<eclass>{lhs, rhs}, .disambiguator = std::nullopt};
  }
};

/// Unary operator: no storage, just one child
template <symbol S>
  requires(is_unary_op_v<S>)
struct symbol_make_traits<S> {
  struct storage_type {};

  static auto make(storage_type & /*s*/, eclass operand) -> lowered_node {
    return {.children = utils::small_vector<eclass>{operand}, .disambiguator = std::nullopt};
  }
};

// ========================================================================
// Combined storage using inheritance (like the overloads trick)
// ========================================================================

template <typename... Ts>
struct combined_storage : Ts... {};

template <symbol... Ss>
using symbol_storage_for = combined_storage<typename symbol_make_traits<Ss>::storage_type...>;

using symbol_storage =
    symbol_storage_for<symbol::Once, symbol::Symbol, symbol::Literal, symbol::ParamLookup, symbol::Bind,
                       symbol::Identifier, symbol::Output, symbol::NamedOutput, symbol::Add, symbol::Sub, symbol::Mul,
                       symbol::Div, symbol::Mod, symbol::Exp, symbol::Eq, symbol::Neq, symbol::Lt, symbol::Lte,
                       symbol::Gt, symbol::Gte, symbol::And, symbol::Or, symbol::Xor, symbol::Not, symbol::UnaryMinus,
                       symbol::UnaryPlus>;

}  // namespace memgraph::query::plan::v2
