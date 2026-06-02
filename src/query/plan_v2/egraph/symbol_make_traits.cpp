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

auto symbol_make_traits<Once>::make(storage_type &s) -> planner::core::LoweredNode {
  return {.children = {}, .disambiguator = s.counter++};
}

auto symbol_make_traits<Symbol>::make(storage_type &s, int32_t pos, std::string_view name)
    -> planner::core::LoweredNode {
  s.store.try_emplace(pos, std::string{name});
  return {.children = {}, .disambiguator = static_cast<uint64_t>(pos)};
}

auto symbol_make_traits<Literal>::make(storage_type &s, storage::ExternalPropertyValue const &value)
    -> planner::core::LoweredNode {
  auto [it, inserted] = s.store.try_emplace(value, s.info.size());
  if (inserted) s.info.push_back(&it->first);
  return {.children = {}, .disambiguator = it->second};
}

auto symbol_make_traits<ParamLookup>::make(storage_type & /*s*/, int32_t pos) -> planner::core::LoweredNode {
  return {.children = {}, .disambiguator = static_cast<uint64_t>(pos)};
}

auto symbol_make_traits<Bind>::make(storage_type & /*s*/, planner::core::EClassId input, planner::core::EClassId sym,
                                    planner::core::EClassId expr) -> planner::core::LoweredNode {
  return {.children = utils::small_vector{input, sym, expr}, .disambiguator = std::nullopt};
}

auto symbol_make_traits<Identifier>::make(storage_type & /*s*/, planner::core::EClassId sym)
    -> planner::core::LoweredNode {
  return {.children = utils::small_vector{sym}, .disambiguator = std::nullopt};
}

auto symbol_make_traits<Output>::make(storage_type & /*s*/, utils::small_vector<planner::core::EClassId> children)
    -> planner::core::LoweredNode {
  return {.children = std::move(children), .disambiguator = std::nullopt};
}

auto symbol_make_traits<NamedOutput>::make(storage_type &s, std::string_view name, planner::core::EClassId sym,
                                           planner::core::EClassId expr) -> planner::core::LoweredNode {
  auto [it, inserted] = s.store.try_emplace(std::string{name}, s.info.size());
  if (inserted) s.info.emplace_back(it->first);
  return {.children = utils::small_vector{sym, expr}, .disambiguator = it->second};
}

auto symbol_make_traits<Function>::storage_type::intern(std::string_view name) -> uint64_t {
  auto [it, inserted] = store.try_emplace(std::string{name}, info.size());
  if (inserted) {
    info.push_back(FunctionInfo{.name = it->first, .kind = BuiltinKindFor(name)});
  }
  return it->second;
}

auto symbol_make_traits<Function>::make(storage_type &s, std::string_view name,
                                        utils::small_vector<planner::core::EClassId> args)
    -> planner::core::LoweredNode {
  return {.children = std::move(args), .disambiguator = s.intern(name)};
}

auto symbol_make_traits<Unwind>::make(storage_type & /*s*/, planner::core::EClassId input, planner::core::EClassId sym,
                                      planner::core::EClassId list_expr) -> planner::core::LoweredNode {
  return {.children = utils::small_vector{input, sym, list_expr}, .disambiguator = std::nullopt};
}

auto symbol_make_traits<Subquery>::make(storage_type & /*s*/, utils::small_vector<planner::core::EClassId> children)
    -> planner::core::LoweredNode {
  return {.children = std::move(children), .disambiguator = std::nullopt};
}

}  // namespace memgraph::query::plan::v2
