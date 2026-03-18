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

#include "query/plan_v2/egraph.hpp"
#include "query/plan_v2/egraph_internal.hpp"

namespace memgraph::query::plan::v2 {

egraph::egraph() : pimpl_(std::make_unique<impl>()) {}

egraph::egraph(egraph &&other) noexcept : pimpl_(std::exchange(other.pimpl_, std::make_unique<impl>())) {}

egraph &egraph::operator=(egraph &&other) noexcept {
  std::swap(pimpl_, other.pimpl_);
  return *this;
}

egraph::~egraph() = default;  // required because pimpl

// ========================================================================
// Public API implementations - delegate to impl::Make<S>()
// ========================================================================

auto egraph::MakeOnce() -> eclass { return pimpl_->Make<symbol::Once>(); }

auto egraph::MakeSymbol(int32_t position, std::string_view name) -> eclass {
  return pimpl_->Make<symbol::Symbol>(position, name);
}

auto egraph::MakeLiteral(storage::ExternalPropertyValue const &value) -> eclass {
  return pimpl_->Make<symbol::Literal>(value);
}

auto egraph::MakeParameterLookup(int32_t position) -> eclass { return pimpl_->Make<symbol::ParamLookup>(position); }

auto egraph::MakeBind(eclass input, eclass sym, eclass expr) -> eclass {
  return pimpl_->Make<symbol::Bind>(input, sym, expr);
}

auto egraph::MakeIdentifier(eclass sym) -> eclass { return pimpl_->Make<symbol::Identifier>(sym); }

auto egraph::MakeOutputs(eclass input, std::vector<eclass> named_outputs) -> eclass {
  return pimpl_->Make<symbol::Output>(input, std::move(named_outputs));
}

auto egraph::MakeNamedOutput(std::string_view name, eclass sym, eclass expr) -> eclass {
  return pimpl_->Make<symbol::NamedOutput>(name, sym, expr);
}

// Arithmetic operators
auto egraph::MakeAdd(eclass lhs, eclass rhs) -> eclass { return pimpl_->Make<symbol::Add>(lhs, rhs); }

auto egraph::MakeSub(eclass lhs, eclass rhs) -> eclass { return pimpl_->Make<symbol::Sub>(lhs, rhs); }

auto egraph::MakeMul(eclass lhs, eclass rhs) -> eclass { return pimpl_->Make<symbol::Mul>(lhs, rhs); }

auto egraph::MakeDiv(eclass lhs, eclass rhs) -> eclass { return pimpl_->Make<symbol::Div>(lhs, rhs); }

auto egraph::MakeMod(eclass lhs, eclass rhs) -> eclass { return pimpl_->Make<symbol::Mod>(lhs, rhs); }

auto egraph::MakeExp(eclass lhs, eclass rhs) -> eclass { return pimpl_->Make<symbol::Exp>(lhs, rhs); }

// Comparison operators
auto egraph::MakeEq(eclass lhs, eclass rhs) -> eclass { return pimpl_->Make<symbol::Eq>(lhs, rhs); }

auto egraph::MakeNeq(eclass lhs, eclass rhs) -> eclass { return pimpl_->Make<symbol::Neq>(lhs, rhs); }

auto egraph::MakeLt(eclass lhs, eclass rhs) -> eclass { return pimpl_->Make<symbol::Lt>(lhs, rhs); }

auto egraph::MakeLte(eclass lhs, eclass rhs) -> eclass { return pimpl_->Make<symbol::Lte>(lhs, rhs); }

auto egraph::MakeGt(eclass lhs, eclass rhs) -> eclass { return pimpl_->Make<symbol::Gt>(lhs, rhs); }

auto egraph::MakeGte(eclass lhs, eclass rhs) -> eclass { return pimpl_->Make<symbol::Gte>(lhs, rhs); }

// Boolean operators
auto egraph::MakeAnd(eclass lhs, eclass rhs) -> eclass { return pimpl_->Make<symbol::And>(lhs, rhs); }

auto egraph::MakeOr(eclass lhs, eclass rhs) -> eclass { return pimpl_->Make<symbol::Or>(lhs, rhs); }

auto egraph::MakeXor(eclass lhs, eclass rhs) -> eclass { return pimpl_->Make<symbol::Xor>(lhs, rhs); }

auto egraph::MakeNot(eclass operand) -> eclass { return pimpl_->Make<symbol::Not>(operand); }

// Unary operators
auto egraph::MakeUnaryMinus(eclass operand) -> eclass { return pimpl_->Make<symbol::UnaryMinus>(operand); }

auto egraph::MakeUnaryPlus(eclass operand) -> eclass { return pimpl_->Make<symbol::UnaryPlus>(operand); }

// ========================================================================
// Internal accessor implementations
// ========================================================================

auto internal::get_impl(egraph const &e) -> egraph::impl const & { return *e.pimpl_; }

auto internal::get_impl(egraph &e) -> egraph::impl & { return *e.pimpl_; }

}  // namespace memgraph::query::plan::v2
