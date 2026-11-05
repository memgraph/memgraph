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

#include <span>

#include "planner/extract/extract.hpp"
#include "query/plan_v2/cost/extraction_env.hpp"
#include "query/plan_v2/egraph/alternative.hpp"
#include "query/plan_v2/scope/variable_set.hpp"

namespace memgraph::query::plan::v2 {

/// CostFrontier: ParetoFrontier with resolve/min_cost for the extraction contract.
struct CostFrontier
    : planner::core::extract::CostResultBase<Alternative, AlternativeDim_Cost, AlternativeDim_Cardinality,
                                             AlternativeDim_Required, AlternativeDim_Introduces> {
  using CostResultBase::CostResultBase;
};

/// Read-only state the cost model reads while scoring an e-node.
/// Row count assumed for a list whose size is not statically known. A small
/// guess on purpose: an unknown list is assumed short so it does not dominate
/// the row cardinality an Unwind derives from it. The exact figure is not
/// empirically tuned - any small constant serves until estimates are backed by
/// stored statistics.
inline constexpr double kDefaultListSize = 12.0;

struct CostCtx {
  using CostResult = CostFrontier;

  // NOLINTBEGIN(cppcoreguidelines-avoid-const-or-ref-data-members)
  SymbolContext const &syms;
  // NOLINTEND(cppcoreguidelines-avoid-const-or-ref-data-members)

  auto operator()(planner::core::ENode<symbol> const &current, planner::core::ENodeId enode_id,
                  std::span<CostResult const *const> children) const -> CostResult;
};

/// Collect the LHS-sym e-classes of every NamedOutput child of an Output enode,
/// as a VariableSet of bit positions translated through `idx`.
auto ExtractOutputOwnSyms(planner::core::ENode<symbol> const &output_enode, EGraph const &egraph,
                          VariableIndex const &idx) -> VariableSet;

}  // namespace memgraph::query::plan::v2
