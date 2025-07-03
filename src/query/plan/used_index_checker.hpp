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
#include "storage/v2/indices/active_indices.hpp"

namespace memgraph::query::plan {

struct UsedIndexChecker : public virtual HierarchicalLogicalOperatorVisitor {
  UsedIndexChecker() = default;

  UsedIndexChecker(const UsedIndexChecker &) = delete;
  UsedIndexChecker(UsedIndexChecker &&) = delete;

  UsedIndexChecker &operator=(const UsedIndexChecker &) = delete;
  UsedIndexChecker &operator=(UsedIndexChecker &&) = delete;

  using HierarchicalLogicalOperatorVisitor::PostVisit;
  using HierarchicalLogicalOperatorVisitor::PreVisit;
  using HierarchicalLogicalOperatorVisitor::Visit;

  bool PreVisit(CreateNode &) override;
  bool PreVisit(CreateExpand &) override;
  bool PreVisit(Delete &) override;

  bool PreVisit(SetProperty &) override;
  bool PreVisit(SetProperties &) override;
  bool PreVisit(SetLabels &) override;

  bool PreVisit(RemoveProperty &) override;
  bool PreVisit(RemoveLabels &) override;

  bool PreVisit(ScanAll &) override;
  bool PreVisit(ScanAllByLabel &) override;
  bool PreVisit(ScanAllByLabelProperties &) override;
  bool PreVisit(ScanAllById &) override;

  bool PreVisit(ScanAllByEdge &) override;
  bool PreVisit(ScanAllByEdgeType &) override;
  bool PreVisit(ScanAllByEdgeTypeProperty &) override;
  bool PreVisit(ScanAllByEdgeTypePropertyValue &) override;
  bool PreVisit(ScanAllByEdgeTypePropertyRange &) override;
  bool PreVisit(ScanAllByEdgeProperty &) override;
  bool PreVisit(ScanAllByEdgePropertyValue &) override;
  bool PreVisit(ScanAllByEdgePropertyRange &) override;
  bool PreVisit(ScanAllByEdgeId &) override;

  bool PreVisit(Expand &) override;
  bool PreVisit(ExpandVariable &) override;

  bool PreVisit(ConstructNamedPath &) override;

  bool PreVisit(Filter &) override;
  bool PreVisit(EdgeUniquenessFilter &) override;

  bool PreVisit(Merge &) override;
  bool PreVisit(Optional &) override;
  bool PreVisit(Cartesian &) override;

  bool PreVisit(EmptyResult &) override;
  bool PreVisit(Produce &) override;
  bool PreVisit(Accumulate &) override;
  bool PreVisit(Aggregate &) override;
  bool PreVisit(Skip &) override;
  bool PreVisit(Limit &) override;
  bool PreVisit(OrderBy &) override;
  bool PreVisit(Distinct &) override;
  bool PreVisit(Union &) override;

  bool PreVisit(Unwind &) override;
  bool PreVisit(CallProcedure &) override;
  bool PreVisit(Foreach &) override;

  bool PreVisit(Apply &) override;
  bool PreVisit(IndexedJoin &) override;
  bool PreVisit(HashJoin &) override;
  bool PreVisit(RollUpApply &) override;
  bool PreVisit(PeriodicSubquery &) override;
  bool PreVisit(PeriodicCommit &) override;

  bool Visit(Once &) override;

  storage::IndicesCollection required_indices_;
};
}  // namespace memgraph::query::plan
