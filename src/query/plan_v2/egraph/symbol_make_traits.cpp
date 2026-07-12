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

auto symbol_make_traits<Symbol>::make(storage_type &s, int32_t pos, std::string_view name, int64_t token_position)
    -> seeded_node {
  s.store.try_emplace(pos, SymbolInfo{.name = std::string{name}, .token_position = token_position});
  return {.lowered = {.children = {}, .disambiguator = static_cast<uint64_t>(pos)},
          .seed = default_analysis_seed<Symbol>()};
}

auto symbol_make_traits<Literal>::make(storage_type &s, storage::ExternalPropertyValue const &value) -> seeded_node {
  auto const id = s.values.intern(value, [](auto const &key) { return &key; });

  return {.lowered = {.children = {}, .disambiguator = id},
          .seed = analysis{ExpressionAnalysis{
              .known_constant_value = value,
              .known_length = value.IsAnyList()
                                  ? std::optional{KnownLength{.of = LengthOf::List, .value = value.ListSize()}}
                                  : std::nullopt}}};
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
  auto const id = s.names.intern(name, [](auto const &key) { return std::string_view{key}; });
  return {.lowered = {.children = utils::small_vector{sym, expr}, .disambiguator = id},
          .seed = default_analysis_seed<NamedOutput>()};
}

auto symbol_make_traits<Function>::make(storage_type &s, std::string_view name,
                                        utils::small_vector<planner::core::EClassId> args, bool is_pure)
    -> seeded_node {
  auto const id = s.functions.intern(name, [is_pure](auto const &key) {
    return FunctionInfo{.name = key, .kind = BuiltinKindFor(key), .is_pure = is_pure};
  });
  return {.lowered = {.children = std::move(args), .disambiguator = id}, .seed = default_analysis_seed<Function>()};
}

namespace {

/// The provably-exact length of `range(start, end)` when both bounds are
/// statically-known integers (`nullopt` otherwise). Cypher range is inclusive
/// on both ends; reversed bounds give an empty list.
///
/// Integer bounds only, which `known_int` already enforces: `range()` rejects a
/// non-integer bound at runtime, so claiming a length off an integral double
/// (e.g. 5.0) would elide an evaluation that is supposed to throw.
auto ProvableRangeLength(planner::core::EGraph<symbol, analysis> const &eg,
                         std::span<planner::core::EClassId const> args) -> std::optional<std::size_t> {
  if (args.size() != 2) return std::nullopt;
  auto const a = eg.analysis_of(args[0]).known_int();
  auto const b = eg.analysis_of(args[1]).known_int();
  if (!a || !b) return std::nullopt;
  if (*b < *a) return std::size_t{0};
  // Difference in unsigned to avoid signed-overflow UB for extreme bounds; +1
  // for the inclusive end. A full-int64-span range can't fit, so decline it.
  auto const span = static_cast<std::uint64_t>(*b) - static_cast<std::uint64_t>(*a);
  if (span == UINT64_MAX) return std::nullopt;
  return static_cast<std::size_t>(span + 1);
}

}  // namespace

auto symbol_make_traits<Function>::update(planner::core::EGraph<symbol, analysis> const &eg, storage_type const &s,
                                          std::span<planner::core::EClassId const> args, std::uint64_t function_id,
                                          analysis &out) -> bool {
  auto *expr = out.expression_for_update();
  if (expr == nullptr) return false;
  auto const *entry = s.functions.find(function_id);
  if (entry == nullptr) return false;
  auto const &info = *entry;
  // Only a pure function's output is a statically-known fact, so an impure call
  // establishes nothing regardless of kind.
  if (!info.is_pure) return false;

  switch (info.kind) {
    case BuiltinKind::Range: {
      if (expr->known_length) return false;
      auto const length = ProvableRangeLength(eg, args);
      if (!length) return false;
      expr->known_length = KnownLength{.of = LengthOf::List, .value = *length};
      return true;
    }
    case BuiltinKind::Size: {
      if (expr->known_constant_value || args.size() != 1) return false;
      auto const length = eg.analysis_of(args[0]).known_list_length();
      if (!length) return false;
      expr->known_constant_value = storage::ExternalPropertyValue{static_cast<int64_t>(*length)};
      return true;
    }
    case BuiltinKind::Unknown:
      return false;
  }
  return false;
}

auto symbol_make_traits<Add>::update(planner::core::EGraph<symbol, analysis> const &eg, storage_type const & /*s*/,
                                     std::span<planner::core::EClassId const> children, std::uint64_t /*disambiguator*/,
                                     analysis &out) -> bool {
  auto *expr = out.expression_for_update();
  if (expr == nullptr || expr->known_length || children.size() != 2) return false;
  // Both reads answer only for a list's length, so this sums two list lengths
  // rather than inferring list-ness from the presence of a count.
  auto const lhs = eg.analysis_of(children[0]).known_list_length();
  auto const rhs = eg.analysis_of(children[1]).known_list_length();
  if (!lhs || !rhs) return false;
  expr->known_length = KnownLength{.of = LengthOf::List, .value = *lhs + *rhs};
  return true;
}

auto symbol_make_traits<Unwind>::make(storage_type & /*s*/, planner::core::EClassId input, planner::core::EClassId sym,
                                      planner::core::EClassId list_expr) -> seeded_node {
  return {.lowered = {.children = utils::small_vector{input, sym, list_expr}, .disambiguator = std::nullopt},
          .seed = default_analysis_seed<Unwind>()};
}

auto symbol_make_traits<Filter>::make(storage_type & /*s*/, planner::core::EClassId input,
                                      planner::core::EClassId predicate) -> seeded_node {
  return {.lowered = {.children = utils::small_vector{input, predicate}, .disambiguator = std::nullopt},
          .seed = default_analysis_seed<Filter>()};
}

auto symbol_make_traits<Subquery>::make(storage_type & /*s*/, utils::small_vector<planner::core::EClassId> children)
    -> seeded_node {
  return {.lowered = {.children = std::move(children), .disambiguator = std::nullopt},
          .seed = default_analysis_seed<Subquery>()};
}

auto symbol_make_traits<Distinct>::make(storage_type & /*s*/, utils::small_vector<planner::core::EClassId> children)
    -> seeded_node {
  return {.lowered = {.children = std::move(children), .disambiguator = std::nullopt},
          .seed = default_analysis_seed<Distinct>()};
}

auto symbol_make_traits<Skip>::make(storage_type & /*s*/, planner::core::EClassId input, planner::core::EClassId count)
    -> seeded_node {
  return {.lowered = {.children = utils::small_vector{input, count}, .disambiguator = std::nullopt},
          .seed = default_analysis_seed<Skip>()};
}

auto symbol_make_traits<Limit>::make(storage_type & /*s*/, planner::core::EClassId input, planner::core::EClassId count)
    -> seeded_node {
  return {.lowered = {.children = utils::small_vector{input, count}, .disambiguator = std::nullopt},
          .seed = default_analysis_seed<Limit>()};
}

auto symbol_make_traits<OrderBy>::make(storage_type &s, planner::core::EClassId input,
                                       utils::small_vector<planner::core::EClassId> children_after_input,
                                       std::vector<Ordering> orderings) -> seeded_node {
  // Intern the ordering vector: same directions -> same disambiguator (hash-cons),
  // different directions -> distinct e-node even with identical sort-key children.
  auto [it, inserted] = s.store.try_emplace(std::move(orderings), s.info.size());
  if (inserted) s.info.push_back(it->first);

  auto children = utils::small_vector<planner::core::EClassId>{};
  children.reserve(1 + children_after_input.size());
  children.push_back(input);
  for (auto c : children_after_input) children.push_back(c);
  return {.lowered = {.children = std::move(children), .disambiguator = it->second},
          .seed = default_analysis_seed<OrderBy>()};
}

}  // namespace memgraph::query::plan::v2
