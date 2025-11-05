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

#include "query/plan_v2/egraph.hpp"

#include "planner/core/egraph.hpp"

#include <cstdint>

#include "private_analysis.hpp"
#include "private_symbol.hpp"

namespace memgraph::query::plan::v2 {

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

}  // namespace memgraph::query::plan::v2

using memgraph::planner::core::EGraph;

namespace memgraph::query::plan::v2 {

/// EGRAPH
struct egraph::impl {
  impl() = default;
  impl(impl &&) = default;
  impl &operator=(impl &&) = default;
  ~impl() = default;

  template <symbol S, typename... Args>
  requires(!symbol_traits<S>::is_disambiguated && symbol_traits<S>::has_children) auto make(Args &&...args) -> eclass {
    auto res = std::array{std::forward<Args>(args)...} |
               std::ranges::views::transform([](eclass ec) -> planner::core::EClassId { return ec.value_of(); }) |
               std::ranges::to<utils::small_vector<planner::core::EClassId>>();
    return eclass(egraph_.emplace(S, std::move(res)));
  }

  template <symbol S>
  requires(!symbol_traits<S>::is_disambiguated &&
           symbol_traits<S>::has_children) auto make(std::vector<eclass> children) -> eclass {
    auto res = children |
               std::ranges::views::transform([](eclass ec) -> planner::core::EClassId { return ec.value_of(); }) |
               std::ranges::to<utils::small_vector<planner::core::EClassId>>();
    return eclass(egraph_.emplace(S, std::move(res)));
  }

  template <symbol S>
  requires(symbol_traits<S>::is_disambiguated &&
           !symbol_traits<S>::has_children) auto make(uint64_t const disambiguator) -> eclass {
    return eclass(egraph_.emplace(S, utils::small_vector<planner::core::EClassId>{}, disambiguator));
  }

  template <symbol S, typename... Args>
  requires(symbol_traits<S>::is_disambiguated &&symbol_traits<S>::has_children) auto make(uint64_t const disambiguator,
                                                                                          Args &&...args) -> eclass {
    auto res = std::array{std::forward<Args>(args)...} |
               std::ranges::views::transform([](eclass ec) -> planner::core::EClassId { return ec.value_of(); }) |
               std::ranges::to<utils::small_vector<planner::core::EClassId>>();
    return eclass(egraph_.emplace(S, std::move(res), disambiguator));
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

  EGraph<symbol, analysis> egraph_;
  uint64_t next_literal_disambiguator_ = 0;
  uint64_t next_once_disambiguator_ = 0;
  uint64_t next_name_disambiguator_ = 0;
  std::map<storage::ExternalPropertyValue, uint64_t> literal_store_;
  std::map<std::string, uint64_t> name_store_;
};

egraph::egraph() : pimpl_(std::make_unique<impl>()) {}
egraph::egraph(egraph &&other) noexcept : pimpl_(std::exchange(other.pimpl_, std::make_unique<impl>())) {}
egraph &egraph::operator=(egraph &&other) noexcept {
  std::swap(pimpl_, other.pimpl_);
  return *this;
}
egraph::~egraph() = default;  // required because pimpl

// NOLINTNEXTLINE(readability-make-member-function-const)
auto egraph::MakeSymbol(int32_t sym_pos) -> eclass { return pimpl_->make<symbol::Symbol>(sym_pos); }

// NOLINTNEXTLINE(readability-make-member-function-const)
auto egraph::MakeBind(eclass input, eclass sym, eclass expr) -> eclass {
  return pimpl_->make<symbol::Bind>(input, sym, expr);
}

// NOLINTNEXTLINE(readability-make-member-function-const)
auto egraph::MakeLiteral(storage::ExternalPropertyValue const &value) -> eclass {
  auto const disambiguator = pimpl_->store_literal(value);
  return pimpl_->make<symbol::Literal>(disambiguator);
}

// NOLINTNEXTLINE(readability-make-member-function-const)
auto egraph::MakeOnce() -> eclass {
  auto const disambiguator = pimpl_->next_once_disambiguator_++;
  return pimpl_->make<symbol::Once>(disambiguator);
}

// NOLINTNEXTLINE(readability-make-member-function-const)
auto egraph::MakeParameterLookup(int32_t param_pos) -> eclass { return pimpl_->make<symbol::ParamLookup>(param_pos); }

// NOLINTNEXTLINE(readability-make-member-function-const)
auto egraph::MakeNamedOutput(std::string_view name, eclass sym, eclass expr) -> eclass {
  auto disambiguator = pimpl_->store_name(name);
  return pimpl_->make<symbol::NamedOutput>(disambiguator, sym, expr);
}

// NOLINTNEXTLINE(readability-make-member-function-const)
auto egraph::MakeOutputs(eclass input, std::vector<eclass> named_outputs) -> eclass {
  named_outputs.insert(named_outputs.begin(), input);
  return pimpl_->make<symbol::Output>(std::move(named_outputs));
}

// NOLINTNEXTLINE(readability-make-member-function-const)
auto egraph::MakeIdentifier(eclass sym) -> eclass { return pimpl_->make<symbol::Identifier>(sym); }

}  // namespace memgraph::query::plan::v2
