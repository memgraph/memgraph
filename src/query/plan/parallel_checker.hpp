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

#include "query/plan/operator.hpp"

namespace memgraph::query::plan {

// NOTE Currently just checks if the plan is parallelized, but its setup to be easily extended in the future
struct ParallelChecker : public virtual HierarchicalLogicalOperatorVisitor {
 public:
  ParallelChecker() = default;
  ~ParallelChecker() override = default;

  ParallelChecker(const ParallelChecker &) = delete;
  ParallelChecker(ParallelChecker &&) = delete;

  ParallelChecker &operator=(const ParallelChecker &) = delete;
  ParallelChecker &operator=(ParallelChecker &&) = delete;

  using HierarchicalLogicalOperatorVisitor::PostVisit;
  using HierarchicalLogicalOperatorVisitor::PreVisit;
  using HierarchicalLogicalOperatorVisitor::Visit;

  bool is_parallelized_{false};
  void CheckParallelized(const LogicalOperator &root);

  // Parallel operators - return false to stop traversal once we find one
  bool PreVisit(AggregateParallel &) override;

  bool Visit(Once &) override;
};

}  // namespace memgraph::query::plan
