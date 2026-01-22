// Copyright 2025 Memgraph Ltd.
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

#include "plan_v2/private_analysis.hpp"
#include "plan_v2/private_symbol.hpp"
#include "planner/core/egraph.hpp"
#include "query/plan_v2/egraph.hpp"

#include "planner/core/egraph.hpp"
#include "query/plan_v2/private_analysis.hpp"
#include "query/plan_v2/private_symbol.hpp"

namespace memgraph::query::plan::v2 {

/**
 * @brief Internal accessor for egraph implementation details
 *
 * This struct provides controlled access to the internal egraph_ member
 * for functions that need it (like ConvertToLogicalOperator).
 *
 * NOTE: This header should NOT be included by public API consumers.
 * It is only for internal implementation files that need access to
 * the underlying EGraph<symbol, analysis> instance.
 */
struct internal {
  static auto get_impl(egraph const &e) -> egraph::impl const &;
  static auto get_impl(egraph &e) -> egraph::impl &;

  // Conversion helpers between eclass and planner::core::EClassId
  static auto to_core_id(eclass id) -> memgraph::planner::core::EClassId {
    return memgraph::planner::core::EClassId{id.value_of()};
  }

  static auto from_core_id(memgraph::planner::core::EClassId id) -> eclass { return eclass{id.value_of()}; }
};

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

/// EGRAPH
struct egraph::impl {
  impl() = default;
  impl(impl &&) = default;
  impl &operator=(impl &&) = default;
  ~impl() = default;

  template <symbol S, typename... Args>
    requires(!symbol_traits<S>::is_disambiguated && symbol_traits<S>::has_children)
  auto make(Args &&...args) -> eclass {
    auto res =
        std::array{std::forward<Args>(args)...} |
        std::ranges::views::transform([](eclass ec) -> planner::core::EClassId { return internal::to_core_id(ec); }) |
        std::ranges::to<utils::small_vector<planner::core::EClassId>>();
    return internal::from_core_id(egraph_.emplace(S, std::move(res)).current_eclassid);
  }

  template <symbol S>
    requires(!symbol_traits<S>::is_disambiguated && symbol_traits<S>::has_children)
  auto make(std::vector<eclass> children) -> eclass {
    auto res =
        children |
        std::ranges::views::transform([](eclass ec) -> planner::core::EClassId { return internal::to_core_id(ec); }) |
        std::ranges::to<utils::small_vector<planner::core::EClassId>>();
    return internal::from_core_id(egraph_.emplace(S, std::move(res)).current_eclassid);
  }

  template <symbol S>
    requires(symbol_traits<S>::is_disambiguated && !symbol_traits<S>::has_children)
  auto make(uint64_t const disambiguator) -> eclass {
    return internal::from_core_id(
        egraph_.emplace(S, utils::small_vector<planner::core::EClassId>{}, disambiguator).current_eclassid);
  }

  template <symbol S, typename... Args>
    requires(symbol_traits<S>::is_disambiguated && symbol_traits<S>::has_children)
  auto make(uint64_t const disambiguator, Args &&...args) -> eclass {
    auto res =
        std::array{std::forward<Args>(args)...} |
        std::ranges::views::transform([](eclass ec) -> planner::core::EClassId { return internal::to_core_id(ec); }) |
        std::ranges::to<utils::small_vector<planner::core::EClassId>>();
    return internal::from_core_id(egraph_.emplace(S, std::move(res), disambiguator).current_eclassid);
  }

  auto store_literal(storage::ExternalPropertyValue const &value) -> uint64_t {
    auto [it, inserted] = literal_store_.try_emplace(value, next_literal_disambiguator_);
    if (inserted) ++next_literal_disambiguator_;
    return it->second;
  }

  auto store_name(std::string_view name) -> uint64_t {
    auto [it, inserted] = name_store_.try_emplace(std::string{name}, next_name_disambiguator_);
    if (inserted) ++next_name_disambiguator_;
    return it->second;
  }

  memgraph::planner::core::EGraph<symbol, analysis> egraph_;
  uint64_t next_literal_disambiguator_ = 0;
  uint64_t next_once_disambiguator_ = 0;
  uint64_t next_name_disambiguator_ = 0;
  std::map<storage::ExternalPropertyValue, uint64_t> literal_store_;
  std::map<std::string, uint64_t> name_store_;
};

}  // namespace memgraph::query::plan::v2
