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
#include "query/plan_v2/egraph_internal.hpp"

namespace memgraph::query::plan::v2 {

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

// ========================================================================
// Internal accessor implementations
// ========================================================================

auto internal::get_impl(egraph const &e) -> egraph::impl const & { return *e.pimpl_; }

auto internal::get_impl(egraph &e) -> egraph::impl & { return *e.pimpl_; }

}  // namespace memgraph::query::plan::v2
