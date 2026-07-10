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

#include <string>
#include <string_view>

#include "query/exceptions.hpp"
#include "query/plan_v2/egraph/symbol.hpp"
#include "query/plan_v2/resolve/analysis.hpp"
#include "query/plan_v2/resolve/variable_index.hpp"
#include "query/plan_v2/resolve/variable_set.hpp"

import memgraph.planner.core.egraph;

namespace memgraph::query::plan::v2 {

using EGraph = planner::core::EGraph<symbol, analysis>;

/// Read-only state shared across one extraction pass.
struct SymbolContext {
  // NOLINTBEGIN(cppcoreguidelines-avoid-const-or-ref-data-members)
  EGraph const &egraph;
  VariableIndex const &variable_index;
  // NOLINTEND(cppcoreguidelines-avoid-const-or-ref-data-members)
  VariableSet outer_scope;
  /// Symbol e-classes referenced by some Identifier e-node anywhere in the
  /// e-graph: the egraph-wide demand filter Bind's cost case uses to decide
  /// whether to emit an alive alt.
  VariableSet referenced_syms;
};

[[noreturn]] inline void ThrowPlannerBug(std::string_view detail) {
  throw PlannerBug{std::string{"Plan extraction failed: "} + std::string{detail} +
                   " This is a planner bug - please report it at "
                   "https://github.com/memgraph/memgraph/issues"};
}

}  // namespace memgraph::query::plan::v2
