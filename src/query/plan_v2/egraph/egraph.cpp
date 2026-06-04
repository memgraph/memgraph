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

#include <optional>

#include "query/plan_v2/cost/builtin_estimator.hpp"
#include "query/plan_v2/egraph/builtin_functions.hpp"
#include "query/plan_v2/egraph/egraph_internal.hpp"

namespace memgraph::query::plan::v2 {

using enum symbol;

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

auto egraph::MakeFunction(std::string_view name, std::span<eclass const> args) -> eclass {
  auto core_args = to_core(args);
  // For builtins whose semantics fix the produced list's length over constant
  // args (range), seed it as an analysis fact so a later Unwind can elide an
  // unused binding into a CardinalityScale.
  std::optional<std::size_t> known_list_length;
  if (BuiltinKindFor(name) == BuiltinKind::Range) {
    known_list_length = ProvableRangeLength(pimpl_->graph.core(), core_args);
  }
  return from_core(pimpl_->graph.Make<Function>(name, std::move(core_args), known_list_length).eclass_id);
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
// NOLINTBEGIN(cppcoreguidelines-macro-usage)
#define MG_DEFN_MAKE_BINARY(Name, ...)                                                \
  auto egraph::Make##Name(eclass lhs, eclass rhs)->eclass {                           \
    return from_core(pimpl_->graph.Make<Name>(to_core(lhs), to_core(rhs)).eclass_id); \
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
