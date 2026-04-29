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

// Binary / unary public-API definitions — generated from EGRAPH_*_OPS.
// NOLINTBEGIN(cppcoreguidelines-macro-usage)
#define MG_DEFN_MAKE_BINARY(Name, ...) \
  auto egraph::Make##Name(eclass lhs, eclass rhs)->eclass { return pimpl_->Make<symbol::Name>(lhs, rhs); }
EGRAPH_BINARY_OPS(MG_DEFN_MAKE_BINARY)
#undef MG_DEFN_MAKE_BINARY

#define MG_DEFN_MAKE_UNARY(Name, ...) \
  auto egraph::Make##Name(eclass operand)->eclass { return pimpl_->Make<symbol::Name>(operand); }
EGRAPH_UNARY_OPS(MG_DEFN_MAKE_UNARY)
#undef MG_DEFN_MAKE_UNARY

// Cross-check: every entry in the X-lists must be the right arity, and the
// list count must match the count of binary/unary symbols in AllSymbolsSeq.
// Forward + count together imply set equality, so a missing or extra entry
// breaks the build at this site.
#define MG_ASSERT_BINARY(Name, ...) \
  static_assert(is_binary_expr_op_v<symbol::Name>, "EGRAPH_BINARY_OPS contains non-binary-expr symbol: " #Name);
EGRAPH_BINARY_OPS(MG_ASSERT_BINARY)
#undef MG_ASSERT_BINARY

#define MG_ASSERT_UNARY(Name, ...) \
  static_assert(is_unary_expr_op_v<symbol::Name>, "EGRAPH_UNARY_OPS contains non-unary-expr symbol: " #Name);
EGRAPH_UNARY_OPS(MG_ASSERT_UNARY)
#undef MG_ASSERT_UNARY

#define MG_COUNT_ONE(...) +1
static_assert((0 EGRAPH_BINARY_OPS(MG_COUNT_ONE)) == binary_expr_op_count_v,
              "EGRAPH_BINARY_OPS count does not match binary expr symbols in AllSymbolsSeq");
static_assert((0 EGRAPH_UNARY_OPS(MG_COUNT_ONE)) == unary_expr_op_count_v,
              "EGRAPH_UNARY_OPS count does not match unary expr symbols in AllSymbolsSeq");
#undef MG_COUNT_ONE

// NOLINTEND(cppcoreguidelines-macro-usage)

// ========================================================================
// Internal accessor implementations
// ========================================================================

auto internal::get_impl(egraph const &e) -> egraph::impl const & { return *e.pimpl_; }

auto internal::get_impl(egraph &e) -> egraph::impl & { return *e.pimpl_; }

}  // namespace memgraph::query::plan::v2
