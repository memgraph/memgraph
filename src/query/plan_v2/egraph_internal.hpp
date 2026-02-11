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

#include <cstdint>

#include "planner/egraph/egraph.hpp"
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
// Symbol flags for low-level emplace dispatch
// ========================================================================

enum struct symbol_flags : std::uint8_t {
  none = 0b0000'0000,
  with_children = 0b0100'0000,
  disambiguated = 0b1000'0000,
};

constexpr symbol_flags operator|(symbol_flags a, symbol_flags b) {
  return static_cast<symbol_flags>(static_cast<std::uint8_t>(a) | static_cast<std::uint8_t>(b));
}

constexpr bool operator&(symbol_flags a, symbol_flags b) {
  return (static_cast<std::uint8_t>(a) & static_cast<std::uint8_t>(b)) != 0;
}

using enum symbol_flags;
inline constexpr symbol_flags symbol_traits_table[] = {
    disambiguated,                  // Once
    with_children,                  // Bind
    disambiguated,                  // Symbol
    disambiguated,                  // Literal
    with_children,                  // Identifier
    with_children,                  // Output
    with_children | disambiguated,  // NamedOutput
    disambiguated,                  // ParamLookup
};

template <symbol S>
struct symbol_traits {
  static constexpr auto flags = symbol_traits_table[static_cast<std::uint8_t>(S)];
  static constexpr auto has_children = flags & with_children;
  static constexpr auto is_disambiguated = flags & disambiguated;
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

  // Low-level emplace functions - dispatch based on symbol_traits
  template <symbol S, typename... Args>
    requires(!symbol_traits<S>::is_disambiguated && symbol_traits<S>::has_children)
  auto emplace_node(Args &&...args) -> eclass {
    auto res =
        std::array{std::forward<Args>(args)...} |
        std::ranges::views::transform([](eclass ec) -> planner::core::EClassId { return internal::to_core_id(ec); }) |
        std::ranges::to<utils::small_vector<planner::core::EClassId>>();
    return internal::from_core_id(egraph_.emplace(S, std::move(res)).eclass_id);
  }

  template <symbol S>
    requires(!symbol_traits<S>::is_disambiguated && symbol_traits<S>::has_children)
  auto emplace_node(std::vector<eclass> children) -> eclass {
    auto res =
        children |
        std::ranges::views::transform([](eclass ec) -> planner::core::EClassId { return internal::to_core_id(ec); }) |
        std::ranges::to<utils::small_vector<planner::core::EClassId>>();
    return internal::from_core_id(egraph_.emplace(S, std::move(res)).eclass_id);
  }

  template <symbol S>
    requires(symbol_traits<S>::is_disambiguated && !symbol_traits<S>::has_children)
  auto emplace_node(uint64_t const disambiguator) -> eclass {
    return internal::from_core_id(
        egraph_.emplace(S, utils::small_vector<planner::core::EClassId>{}, disambiguator).eclass_id);
  }

  template <symbol S, typename... Args>
    requires(symbol_traits<S>::is_disambiguated && symbol_traits<S>::has_children)
  auto emplace_node(uint64_t const disambiguator, Args &&...args) -> eclass {
    auto res =
        std::array{std::forward<Args>(args)...} |
        std::ranges::views::transform([](eclass ec) -> planner::core::EClassId { return internal::to_core_id(ec); }) |
        std::ranges::to<utils::small_vector<planner::core::EClassId>>();
    return internal::from_core_id(egraph_.emplace(S, std::move(res), disambiguator).eclass_id);
  }

  // Unified Make<S>() - extracts specific storage and passes emplacer
  template <symbol S, typename... Args>
  auto Make(Args &&...args) -> eclass {
    auto emplacer = [this](auto &&...emplace_args) {
      return this->emplace_node<S>(std::forward<decltype(emplace_args)>(emplace_args)...);
    };
    return symbol_make_traits<S>::make(storage<S>(), emplacer, std::forward<Args>(args)...);
  }

  memgraph::planner::core::EGraph<symbol, analysis> egraph_;
  symbol_storage storage_;
};

}  // namespace memgraph::query::plan::v2
