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

#include "query/plan_v2/cost/builtin_estimator.hpp"

#include <span>

#include "query/plan_v2/egraph/egraph_internal.hpp"

namespace memgraph::query::plan::v2 {

using enum symbol;

BuiltinEstimator::BuiltinEstimator(egraph const &f) : CardinalityEstimator(impl_of(f).graph.core()), facade(f) {}

auto BuiltinEstimator::Estimate(planner::core::ENode<symbol> const &enode,
                                std::span<planner::core::EClassId const> /*children*/) const -> double {
  switch (enode.symbol()) {
    case Function:
      // Statically-known list sizes are carried as analysis facts and read by
      // the cost model; the estimator handles only the genuinely-unknown, which
      // currently means a default for every function.
      return kDefaultListSize;
    default:
      return kDefaultCardinalityEstimate;
  }
}

}  // namespace memgraph::query::plan::v2
