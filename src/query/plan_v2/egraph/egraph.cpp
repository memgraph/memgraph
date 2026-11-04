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

#include "query/plan_v2/egraph/egraph.hpp"

#include <cstdint>
#include <optional>
#include <span>

#include "query/plan_v2/egraph/builtin_functions.hpp"
#include "query/plan_v2/egraph/egraph_internal.hpp"

namespace memgraph::query::plan::v2 {

using enum symbol;
using EGraph = planner::core::EGraph<symbol, analysis>;

egraph::egraph() : pimpl_(std::make_unique<impl>()) {}

egraph::egraph(egraph &&other) noexcept : pimpl_(std::exchange(other.pimpl_, std::make_unique<impl>())) {}

egraph &egraph::operator=(egraph &&other) noexcept {
  std::swap(pimpl_, other.pimpl_);
  return *this;
}

egraph::~egraph() = default;  // required because pimpl

// ========================================================================
// Public API implementations - convert eclass <-> core::EClassId at the
// boundary, then delegate to the inherited TypedEGraph::Make<S>().
// ========================================================================

auto egraph::MakeOnce() -> eclass { return from_core(pimpl_->graph.Make<Once>().eclass_id); }

auto egraph::MakeSymbol(int32_t position, std::string_view name) -> eclass {
  return from_core(pimpl_->graph.Make<Symbol>(position, name).eclass_id);
}

auto egraph::MakeLiteral(storage::ExternalPropertyValue const &value) -> eclass {
  return from_core(pimpl_->graph.Make<Literal>(value).eclass_id);
}

auto egraph::MakeParameterLookup(int32_t position) -> eclass {
  return from_core(pimpl_->graph.Make<ParamLookup>(position).eclass_id);
}

auto egraph::MakeBind(eclass input, eclass sym, eclass expr) -> eclass {
  return from_core(pimpl_->graph.Make<Bind>(to_core(input), to_core(sym), to_core(expr)).eclass_id);
}

auto egraph::MakeIdentifier(eclass sym) -> eclass {
  return from_core(pimpl_->graph.Make<Identifier>(to_core(sym)).eclass_id);
}

auto egraph::MakeOutput(eclass input, std::span<eclass const> named_outputs) -> eclass {
  auto children = utils::small_vector<planner::core::EClassId>{};
  children.reserve(1 + named_outputs.size());
  children.push_back(to_core(input));
  for (auto e : named_outputs) children.push_back(to_core(e));
  return from_core(pimpl_->graph.Make<Output>(std::move(children)).eclass_id);
}

auto egraph::MakeNamedOutput(std::string_view name, eclass sym, eclass expr) -> eclass {
  return from_core(pimpl_->graph.Make<NamedOutput>(name, to_core(sym), to_core(expr)).eclass_id);
}

namespace {

// The provably-exact length of `range(start, end)` when both bounds are
// statically-known integers (`nullopt` otherwise).  Cypher range is inclusive
// on both ends; reversed bounds give an empty list.  Computed once here, at the
// make-time seed; the search plane reads the resulting fact rather than
// recomputing.
//
// Integer bounds only, which `known_int` already enforces: `range()` rejects a
// non-integer bound at runtime, so seeding a length off an integral double
// (e.g. 5.0) would elide an evaluation that is supposed to throw.
auto ProvableRangeLength(EGraph const &eg, std::span<planner::core::EClassId const> args)
    -> std::optional<std::size_t> {
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

// Analysis facts a builtin's semantics establish at plan time. `range` over
// constant integer bounds has a known length; `size` of a known-length list
// folds to that length as a constant, with no evaluation or materialisation.
// Purity gate: only a pure function's output is a statically-known fact, so an
// impure call seeds nothing regardless of kind.
auto BuiltinAnalysis(EGraph const &eg, std::string_view name, std::span<planner::core::EClassId const> args,
                     bool is_pure) -> ExpressionAnalysis {
  if (!is_pure) return {};
  switch (BuiltinKindFor(name)) {
    case BuiltinKind::Range:
      return {.known_list_length = ProvableRangeLength(eg, args)};
    case BuiltinKind::Size:
      if (args.size() == 1) {
        if (auto const length = eg.analysis_of(args[0]).known_list_length()) {
          return {.known_constant_value = storage::ExternalPropertyValue{static_cast<int64_t>(*length)}};
        }
      }
      return {};
    case BuiltinKind::Unknown:
      return {};
  }
  return {};
}

}  // namespace

auto egraph::MakeFunction(std::string_view name, std::span<eclass const> args, bool is_pure) -> eclass {
  auto core_args = to_core(args);
  auto seed = BuiltinAnalysis(pimpl_->graph.core(), name, core_args, is_pure);
  return from_core(pimpl_->graph.Make<Function>(name, std::move(core_args), std::move(seed), is_pure).eclass_id);
}

auto egraph::MakeUnwind(eclass input, eclass sym, eclass list_expr) -> eclass {
  return from_core(pimpl_->graph.Make<Unwind>(to_core(input), to_core(sym), to_core(list_expr)).eclass_id);
}

auto egraph::MakeSubquery(eclass outer_input, eclass inner_root, std::span<eclass const> exposed_syms) -> eclass {
  auto children = utils::small_vector<planner::core::EClassId>{};
  children.reserve(2 + exposed_syms.size());
  children.push_back(to_core(outer_input));
  children.push_back(to_core(inner_root));
  for (auto e : exposed_syms) children.push_back(to_core(e));
  return from_core(pimpl_->graph.Make<Subquery>(std::move(children)).eclass_id);
}

auto egraph::FunctionInfoById(std::uint64_t id) const -> FunctionInfo const * {
  auto const &info = pimpl_->graph.storage<Function>().info;
  if (id >= info.size()) return nullptr;
  return &info[id];
}

// Binary / unary public-API definitions - generated from EGRAPH_*_OPS.
namespace {
/// Length a binary operator's result is statically known to have, or nullopt.
/// Most operators contribute none; the primary template is that default.
template <symbol S>
auto BinaryKnownListLength(EGraph const & /*eg*/, planner::core::EClassId /*lhs*/, planner::core::EClassId /*rhs*/)
    -> std::optional<std::size_t> {
  return std::nullopt;
}

/// List concatenation of two known-length lists has a known length: their sum.
/// `known_list_length` is set only for lists, so both operands carrying it means
/// list + list (Cypher concatenation), not numeric addition.
template <>
auto BinaryKnownListLength<symbol::Add>(EGraph const &eg, planner::core::EClassId lhs, planner::core::EClassId rhs)
    -> std::optional<std::size_t> {
  return eg.analysis_of(lhs).known_list_length().and_then([&](std::size_t lhs_length) {
    return eg.analysis_of(rhs).known_list_length().transform(
        [lhs_length](std::size_t rhs_length) { return lhs_length + rhs_length; });
  });
}
}  // namespace

// NOLINTBEGIN(cppcoreguidelines-macro-usage)
#define MG_DEFN_MAKE_BINARY(Name, ...)                                                                      \
  auto egraph::Make##Name(eclass lhs, eclass rhs)->eclass {                                                 \
    auto const l = to_core(lhs);                                                                            \
    auto const r = to_core(rhs);                                                                            \
    return from_core(                                                                                       \
        pimpl_->graph.Make<Name>(l, r, BinaryKnownListLength<Name>(pimpl_->graph.core(), l, r)).eclass_id); \
  }
EGRAPH_BINARY_OPS(MG_DEFN_MAKE_BINARY)
#undef MG_DEFN_MAKE_BINARY

#define MG_DEFN_MAKE_UNARY(Name, ...)                                       \
  auto egraph::Make##Name(eclass operand)->eclass {                         \
    return from_core(pimpl_->graph.Make<Name>(to_core(operand)).eclass_id); \
  }
EGRAPH_UNARY_OPS(MG_DEFN_MAKE_UNARY)
#undef MG_DEFN_MAKE_UNARY

// NOLINTEND(cppcoreguidelines-macro-usage)

// ========================================================================
// Pimpl back-door for plan_v2-internal TUs.
// ========================================================================

auto impl_of(egraph &e) -> egraph::impl & { return *e.pimpl_; }

auto impl_of(egraph const &e) -> egraph::impl const & { return *e.pimpl_; }

}  // namespace memgraph::query::plan::v2
