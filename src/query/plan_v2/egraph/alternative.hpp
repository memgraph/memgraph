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

#include <compare>

#include "planner/extract/pareto_frontier.hpp"
#include "query/plan_v2/resolve/variable_set.hpp"

import memgraph.planner.core.egraph;

namespace memgraph::query::plan::v2 {

/// One alternative way to realise an e-class: one of the equivalent forms it
/// denotes, with the cost and symbol flow of choosing it.
struct Alternative {
  /// Estimated cost of realising this alternative; lower is better.
  double cost;
  /// Number of values flowing through this point; 1 for a scalar value.
  double cardinality = 1.0;
  /// Symbols that must be bound, and so active in scope, for this alternative
  /// to be evaluated.
  VariableSet required;
  /// Symbols this alternative makes active in scope for whatever consumes it.
  VariableSet introduces;
  /// The e-node that realises this alternative.
  planner::core::ENodeId enode_id;
};

/// Pareto dimensions (each policy type below gives its direction). Two are
/// worth a note:
///   - cardinality : a consumer multiplies its per-row cost by it, so a
///                   narrower row pipe wins when the other axes tie.
///   - introduces  : larger is better, unlike a usual cost axis. An alternative
///                   that makes more symbols active lets a consumer meet its
///                   demand from this input, so a cheap alternative that binds
///                   nothing must not dominate one that binds a symbol a
///                   consumer needs.
using AlternativeDim_Cost = planner::core::extract::Dim<&Alternative::cost, planner::core::extract::LowerIsBetter>;
using AlternativeDim_Cardinality =
    planner::core::extract::Dim<&Alternative::cardinality, planner::core::extract::LowerIsBetter>;
using AlternativeDim_Required =
    planner::core::extract::Dim<&Alternative::required, planner::core::extract::SmallerSubsetIsBetter>;
using AlternativeDim_Introduces =
    planner::core::extract::Dim<&Alternative::introduces, planner::core::extract::LargerSubsetIsBetter>;

}  // namespace memgraph::query::plan::v2
