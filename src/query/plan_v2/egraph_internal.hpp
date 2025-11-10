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

#pragma once

#include "plan_v2/private_analysis.hpp"
#include "plan_v2/private_symbol.hpp"
#include "planner/core/egraph.hpp"
#include "query/plan_v2/egraph.hpp"

namespace memgraph::query::plan::v2 {

/**
 * @brief Internal accessor for egraph implementation details
 *
 * This struct provides controlled access to the internal egraph_ member
 * for functions that need it (like ConvertToLogicalOperator).
 *
 * NOTE: This header should NOT be included by public API consumers.
 * It is only for internal implementation files that need access to
 * the underlying EGraph<symbol, analysis> instance.
 */
struct internal {
  static auto get_egraph(egraph const &e) -> memgraph::planner::core::EGraph<symbol, analysis> const &;
  static auto get_egraph(egraph &e) -> memgraph::planner::core::EGraph<symbol, analysis> &;

  // Conversion helpers between eclass and planner::core::EClassId
  static auto to_core_id(eclass id) -> memgraph::planner::core::EClassId {
    return memgraph::planner::core::EClassId{id.value_of()};
  }

  static auto from_core_id(memgraph::planner::core::EClassId id) -> eclass { return eclass{id.value_of()}; }
};

}  // namespace memgraph::query::plan::v2
