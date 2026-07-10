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
#include <functional>
#include <map>
#include <memory>
#include <span>
#include <string>
#include <string_view>
#include <variant>
#include <vector>

#include "frontend/ast/ast_storage.hpp"
#include "query/frontend/semantic/symbol_table.hpp"
#include "query/plan/operator.hpp"
#include "query/plan_v2/egraph/builtin_functions.hpp"
#include "query/plan_v2/egraph/symbol.hpp"
#include "query/plan_v2/resolve/extraction_env.hpp"

namespace memgraph::query::plan::v2 {

// BuildState + symbol_build_traits<S>: turn the resolver's topological order
// into the LogicalOperator / Expression tree the executor runs. The per-symbol
// build bodies are file-local in builder.cpp; this header is just the contract.

// Operators -> used once in the build. Expressions -> can be reused.
using LogicalOperatorPtr = std::shared_ptr<LogicalOperator>;
using BuildResult = std::variant<LogicalOperatorPtr, Expression *, Symbol, NamedExpression *>;
using ENodeRef = planner::core::ENode<symbol> const &;
using ChildRef = std::reference_wrapper<BuildResult const>;
using ChildrenRef = std::span<ChildRef const>;

/// Build-phase state: read-only input refs supplied by the orchestrator,
/// plus owned outputs (`ast_storage`, `symbol_table`) moved out at the end.
struct BuildState {
  // NOLINTBEGIN(cppcoreguidelines-avoid-const-or-ref-data-members)
  std::vector<storage::ExternalPropertyValue const *> const &literal_info;
  std::vector<std::string_view> const &named_output_info;
  std::map<std::int32_t, std::string> const &symbol_store;
  std::vector<FunctionInfo> const &function_info;
  // NOLINTEND(cppcoreguidelines-avoid-const-or-ref-data-members)

  AstStorage ast_storage;
  SymbolTable symbol_table;

  /// Dispatch on `node.symbol()` to the per-symbol build body and wrap the
  /// result in a BuildResult. Defined in builder.cpp.
  auto Build(ENodeRef node, ChildrenRef children) -> BuildResult;
};

}  // namespace memgraph::query::plan::v2
