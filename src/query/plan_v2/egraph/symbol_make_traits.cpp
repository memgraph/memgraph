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

#include "query/plan_v2/egraph/symbol_make_traits.hpp"

namespace memgraph::query::plan::v2 {

using enum symbol;

auto symbol_make_traits<Once>::make(storage_type &s) -> seeded_node {
  return {.lowered = {.children = {}, .disambiguator = s.counter++}, .seed = default_analysis_seed<Once>()};
}

auto symbol_make_traits<Symbol>::make(storage_type &s, int32_t pos, std::string_view name) -> seeded_node {
  s.store.try_emplace(pos, std::string{name});
  return {.lowered = {.children = {}, .disambiguator = static_cast<uint64_t>(pos)},
          .seed = default_analysis_seed<Symbol>()};
}

auto symbol_make_traits<Literal>::make(storage_type &s, storage::ExternalPropertyValue const &value) -> seeded_node {
  auto [it, inserted] = s.store.try_emplace(value, s.info.size());
  if (inserted) s.info.push_back(&it->first);

  auto const is_list = value.IsList() || value.IsIntList() || value.IsDoubleList() || value.IsNumericList();
  return {.lowered = {.children = {}, .disambiguator = it->second},
          .seed = analysis{
              ExpressionAnalysis{.known_constant_value = value,
                                 .known_list_length = is_list ? std::optional{GetListSize(value)} : std::nullopt}}};
}

auto symbol_make_traits<ParamLookup>::make(storage_type & /*s*/, int32_t pos) -> seeded_node {
  return {.lowered = {.children = {}, .disambiguator = static_cast<uint64_t>(pos)},
          .seed = default_analysis_seed<ParamLookup>()};
}

auto symbol_make_traits<Bind>::make(storage_type & /*s*/, planner::core::EClassId input, planner::core::EClassId sym,
                                    planner::core::EClassId expr) -> seeded_node {
  return {.lowered = {.children = utils::small_vector{input, sym, expr}, .disambiguator = std::nullopt},
          .seed = default_analysis_seed<Bind>()};
}

auto symbol_make_traits<Identifier>::make(storage_type & /*s*/, planner::core::EClassId sym) -> seeded_node {
  return {.lowered = {.children = utils::small_vector{sym}, .disambiguator = std::nullopt},
          .seed = default_analysis_seed<Identifier>()};
}

auto symbol_make_traits<Output>::make(storage_type & /*s*/, utils::small_vector<planner::core::EClassId> children)
    -> seeded_node {
  return {.lowered = {.children = std::move(children), .disambiguator = std::nullopt},
          .seed = default_analysis_seed<Output>()};
}

auto symbol_make_traits<NamedOutput>::make(storage_type &s, std::string_view name, planner::core::EClassId sym,
                                           planner::core::EClassId expr) -> seeded_node {
  auto [it, inserted] = s.store.try_emplace(std::string{name}, s.info.size());
  if (inserted) s.info.emplace_back(it->first);
  return {.lowered = {.children = utils::small_vector{sym, expr}, .disambiguator = it->second},
          .seed = default_analysis_seed<NamedOutput>()};
}

auto symbol_make_traits<Function>::storage_type::intern(std::string_view name, bool is_pure) -> uint64_t {
  auto [it, inserted] = store.try_emplace(std::string{name}, info.size());
  if (inserted) {
    info.push_back(FunctionInfo{.name = it->first, .kind = BuiltinKindFor(name), .is_pure = is_pure});
  }
  return it->second;
}

auto symbol_make_traits<Function>::make(storage_type &s, std::string_view name,
                                        utils::small_vector<planner::core::EClassId> args, ExpressionAnalysis seed,
                                        bool is_pure) -> seeded_node {
  return {.lowered = {.children = std::move(args), .disambiguator = s.intern(name, is_pure)},
          .seed = analysis{std::move(seed)}};
}

auto symbol_make_traits<Unwind>::make(storage_type & /*s*/, planner::core::EClassId input, planner::core::EClassId sym,
                                      planner::core::EClassId list_expr) -> seeded_node {
  return {.lowered = {.children = utils::small_vector{input, sym, list_expr}, .disambiguator = std::nullopt},
          .seed = default_analysis_seed<Unwind>()};
}

auto symbol_make_traits<Subquery>::make(storage_type & /*s*/, utils::small_vector<planner::core::EClassId> children)
    -> seeded_node {
  return {.lowered = {.children = std::move(children), .disambiguator = std::nullopt},
          .seed = default_analysis_seed<Subquery>()};
}

}  // namespace memgraph::query::plan::v2
