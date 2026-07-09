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

#include "query/plan_v2/cost/cardinality_estimator.hpp"
#include "query/plan_v2/egraph/builtin_functions.hpp"

namespace memgraph::query::plan::v2 {

struct egraph;  // public e-graph facade

/// Default list size when a built-in's produced size can't be deduced
/// statically.  A small guess on purpose: an unknown list is assumed short so
/// it doesn't dominate the row cardinality an Unwind later derives from it. The
/// exact figure is not empirically tuned - any small constant serves until the
/// stats-backed estimator lands.
inline constexpr double kDefaultListSize = 12.0;

/// Estimates the produced size of a built-in whose size is *not* statically
/// known.  A statically-known size (e.g. `range` over constant bounds) is
/// carried as a `known_list_length` analysis fact and read by the cost model
/// directly; the estimator is the fallback for the genuinely-unknown, and is
/// the seam a future storage-stats-backed estimator slots into.  Today every
/// Function falls back to kDefaultListSize.
///
/// The estimator looks up FunctionInfo through the public e-graph facade (which
/// the cost model passes alongside the EGraph for the e-class walk).
struct BuiltinEstimator final : CardinalityEstimator {
  // Reserved for the future storage-stats-backed estimator: the FunctionInfo
  // lookup facade. Unused today - known sizes are analysis facts the cost model
  // reads directly, and Estimate only returns kDefaultListSize - but kept so
  // that seam needs no signature change later.
  // NOLINTNEXTLINE(cppcoreguidelines-avoid-const-or-ref-data-members)
  egraph const &facade;

  explicit BuiltinEstimator(egraph const &f);

  auto Estimate(planner::core::ENode<symbol> const &enode, std::span<planner::core::EClassId const> children) const
      -> double override;
};

}  // namespace memgraph::query::plan::v2
