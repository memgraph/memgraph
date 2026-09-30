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

#include <memory>
#include <tuple>

#include "frontend/ast/ast_storage.hpp"
#include "query/frontend/semantic/symbol_table.hpp"
#include "query/plan_v2/egraph/egraph.hpp"
#include "query/plan_v2/frontend/query_planner_context.hpp"

namespace memgraph::query::plan {
class LogicalOperator;
}

namespace memgraph::query::plan::v2 {

/// Result of a successful ConvertToLogicalOperator call.
struct ExtractionResult {
  std::unique_ptr<LogicalOperator> plan;
  double cost;
  /// Cardinality of the root alt selected by extraction.  Surfaces the
  /// per-query result so callers / tests can pin cardinality semantics
  /// directly, without round-tripping through cost arithmetic.
  double cardinality;
  AstStorage ast_storage;
  SymbolTable symbol_table;
};

/// Returns the extracted plan together with the SymbolTable to use for
/// downstream lookups, in place of the parse-time SymbolTable.
///
/// `planner_context` is required: callers must own one and pass it in.
/// Long-lived owners (Interpreter) get amortised buffer allocations across
/// queries; short-lived callers (triggers, tests) just declare a local one.
auto ConvertToLogicalOperator(egraph const &e, eclass root, QueryPlannerContext &planner_context) -> ExtractionResult;
}  // namespace memgraph::query::plan::v2
