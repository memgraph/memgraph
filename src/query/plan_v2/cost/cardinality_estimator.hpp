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
#include <span>

#include "query/plan_v2/egraph/symbol.hpp"
#include "query/plan_v2/resolve/analysis.hpp"

import memgraph.planner.core.egraph;

namespace memgraph::query::plan::v2 {

using EGraph = planner::core::EGraph<symbol, analysis>;

/// Default per-call cardinality when nothing better is known.
///
/// Placeholder for storage-stats-backed estimates; deliberately not 1.0
/// because that would silently make every unknown look free and every
/// plan-shape decision tip toward "evaluate per row".  1000.0 is large
/// enough that an unknown row pipe out-costs a known scalar.
inline constexpr double kDefaultCardinalityEstimate = 1000.0;

/// Pluggable cardinality estimator.
///
/// The cost model calls this once per e-node that needs a cardinality estimate
/// (currently: Function).  The enode, its argument e-classes, and the full
/// e-graph are passed so implementations can walk the graph for constant
/// deduction.  Non-Function callers receive kDefaultCardinalityEstimate by default.
///
/// Concrete implementations:
///   - BuiltinEstimator  - constant deduction for builtins (e.g. range(0,5)
///                         → a 6-element list); falls back to kDefaultListSize
///                         otherwise.  Production default.
struct CardinalityEstimator {
  explicit CardinalityEstimator(EGraph const &eg) : eg_(eg) {}

  CardinalityEstimator(CardinalityEstimator const &) = delete;
  CardinalityEstimator(CardinalityEstimator &&) = delete;
  auto operator=(CardinalityEstimator const &) -> CardinalityEstimator & = delete;
  auto operator=(CardinalityEstimator &&) -> CardinalityEstimator & = delete;
  virtual ~CardinalityEstimator() = default;

  virtual auto Estimate(planner::core::ENode<symbol> const &enode,
                        std::span<planner::core::EClassId const> children) const -> double = 0;

 protected:
  // NOLINTNEXTLINE(cppcoreguidelines-avoid-const-or-ref-data-members)
  EGraph const &eg_;
};

}  // namespace memgraph::query::plan::v2
