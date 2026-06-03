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

#pragma once

#include <cstdint>
#include <initializer_list>
#include <memory>
#include <span>
#include <string_view>

#include "query/plan_v2/egraph/builtin_functions.hpp"
#include "query/plan_v2/egraph/op_ast_lists.hpp"
#include "storage/v2/property_value.hpp"
#include "strong_type/strong_type.hpp"

namespace memgraph::query::plan {
class LogicalOperator;
}

namespace memgraph::query::plan::v2 {

using eclass = strong::type<std::uint32_t, struct eclass_>;

struct egraph {
  egraph();
  egraph(egraph &&) noexcept;
  egraph &operator=(egraph &&) noexcept;
  ~egraph();  // required because pimpl

  // Public API for creating egraph nodes
  auto MakeOnce() -> eclass;
  auto MakeSymbol(int32_t position, std::string_view name) -> eclass;
  auto MakeLiteral(storage::ExternalPropertyValue const &value) -> eclass;
  auto MakeParameterLookup(int32_t position) -> eclass;
  auto MakeBind(eclass input, eclass sym, eclass expr) -> eclass;
  auto MakeIdentifier(eclass sym) -> eclass;
  auto MakeOutput(eclass input, std::span<eclass const> named_outputs) -> eclass;

  auto MakeOutput(eclass input, std::initializer_list<eclass> named_outputs) -> eclass {
    return MakeOutput(input, std::span<eclass const>{named_outputs});
  }

  auto MakeNamedOutput(std::string_view name, eclass sym, eclass expr) -> eclass;
  auto MakeFunction(std::string_view name, std::span<eclass const> args) -> eclass;

  auto MakeFunction(std::string_view name, std::initializer_list<eclass> args) -> eclass {
    return MakeFunction(name, std::span<eclass const>{args});
  }

  auto MakeUnwind(eclass input, eclass sym, eclass list_expr) -> eclass;
  auto MakeSubquery(eclass outer_input, eclass inner_root, std::span<eclass const> exposed_syms) -> eclass;

  auto MakeSubquery(eclass outer_input, eclass inner_root, std::initializer_list<eclass> exposed_syms) -> eclass {
    return MakeSubquery(outer_input, inner_root, std::span<eclass const>{exposed_syms});
  }

  /// Look up the FunctionInfo (name + cached BuiltinKind) for a function id
  /// previously assigned by MakeFunction.  Used by the cost-model estimator
  /// and the builder to translate a Function e-node back to its name without
  /// materialising the whole interner.  Returns nullptr if the id is unknown.
  auto FunctionInfoById(std::uint64_t id) const -> FunctionInfo const *;

  // Binary operators (arithmetic / comparison / boolean) - generated from EGRAPH_BINARY_OPS.
#define MG_DECL_MAKE_BINARY(Name, ...) auto Make##Name(eclass lhs, eclass rhs) -> eclass;
  EGRAPH_BINARY_OPS(MG_DECL_MAKE_BINARY)
#undef MG_DECL_MAKE_BINARY

  // Unary operators - generated from EGRAPH_UNARY_OPS.
#define MG_DECL_MAKE_UNARY(Name, ...) auto Make##Name(eclass operand) -> eclass;
  EGRAPH_UNARY_OPS(MG_DECL_MAKE_UNARY)
#undef MG_DECL_MAKE_UNARY

 private:
  struct impl;
  std::unique_ptr<impl> pimpl_;

  friend auto impl_of(egraph &e) -> impl &;
  friend auto impl_of(egraph const &e) -> impl const &;
};
}  // namespace memgraph::query::plan::v2
