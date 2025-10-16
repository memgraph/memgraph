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

/// @file
#pragma once

#include <iosfwd>

#include <nlohmann/json_fwd.hpp>

#include "query/plan/operator.hpp"

namespace memgraph::query {
class DbAccessor;

namespace plan {
class LogicalOperator;

/// Pretty print a `LogicalOperator` plan to a `std::ostream`.
/// DbAccessor is needed for resolving label and property names.
/// Note that `plan_root` isn't modified, but we can't take it as a const
/// because we don't have support for visiting a const LogicalOperator.
void PrettyPrint(const DbAccessor &dba, const LogicalOperator *plan_root, std::ostream *out);

/// Convert a `LogicalOperator` plan to a JSON representation.
/// DbAccessor is needed for resolving label and property names.
nlohmann::json PlanToJson(const DbAccessor &dba, const LogicalOperator *plan_root);

struct PlanPrinter final : virtual HierarchicalLogicalOperatorVisitor {
  using HierarchicalLogicalOperatorVisitor::PostVisit;
  using HierarchicalLogicalOperatorVisitor::PreVisit;
  using HierarchicalLogicalOperatorVisitor::Visit;

  PlanPrinter(const DbAccessor *dba, std::ostream *out);

  bool DefaultPreVisit() override;

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
  bool PreVisit(ScanAllByPointDistance &) override;
  bool PreVisit(ScanAllByPointWithinbbox &) override;
  bool PreVisit(ScanAllByEdgeId &) override;

  bool PreVisit(Expand &) override;
  bool PreVisit(ExpandVariable &) override;

  bool PreVisit(ConstructNamedPath &) override;

  bool PreVisit(Filter &) override;
  bool PreVisit(EvaluatePatternFilter & /*unused*/) override;
  bool PreVisit(EdgeUniquenessFilter &) override;

  bool PreVisit(Merge &) override;
  bool PreVisit(Optional &) override;
  bool PreVisit(Cartesian &) override;
  bool PreVisit(HashJoin &) override;

  bool PreVisit(EmptyResult &) override;
  bool PreVisit(Produce &) override;
  bool PreVisit(Accumulate &) override;
  bool PreVisit(Aggregate &) override;
  bool PreVisit(Skip &) override;
  bool PreVisit(Limit &) override;
  bool PreVisit(OrderBy &) override;
  bool PreVisit(Distinct &) override;
  bool PreVisit(Union &) override;
  bool PreVisit(RollUpApply &) override;
  bool PreVisit(PeriodicCommit &) override;
  bool PreVisit(PeriodicSubquery &) override;
  bool PreVisit(SetNestedProperty &) override;
  bool PreVisit(RemoveNestedProperty &) override;

  bool PreVisit(Unwind &) override;
  bool PreVisit(CallProcedure &) override;
  bool PreVisit(LoadCsv &) override;
  bool PreVisit(Foreach &) override;
  bool PreVisit(Apply & /*unused*/) override;
  bool PreVisit(IndexedJoin & /*unused*/) override;

  bool Visit(Once &) override;

  /// Call fun with output stream. The stream is prefixed with amount of spaces
  /// corresponding to the current depth_.
  template <class TFun>
  void WithPrintLn(TFun fun) {
    *out_ << " ";
    for (int64_t i = 0; i < depth_; ++i) {
      *out_ << "| ";
    }
    fun(*out_);
    *out_ << std::endl;
  }

  /// Forward this printer to another operator branch by incrementing the depth
  /// and printing the branch name.
  void Branch(LogicalOperator &op, const std::string &branch_name = "");

  int64_t depth_{0};
  const DbAccessor *dba_{nullptr};
  std::ostream *out_{nullptr};
};

}  // namespace plan
}  // namespace memgraph::query
