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

  bool PreVisit(CreateNode & /*unused*/) override;
  bool PreVisit(CreateExpand & /*unused*/) override;
  bool PreVisit(Delete & /*unused*/) override;

  bool PreVisit(SetProperty & /*unused*/) override;
  bool PreVisit(SetProperties & /*unused*/) override;
  bool PreVisit(SetLabels & /*unused*/) override;

  bool PreVisit(RemoveProperty & /*unused*/) override;
  bool PreVisit(RemoveLabels & /*unused*/) override;

  bool PreVisit(ScanAll & /*unused*/) override;
  bool PreVisit(ScanAllByLabel & /*unused*/) override;
  bool PreVisit(ScanAllByLabelProperties & /*unused*/) override;
  bool PreVisit(ScanAllById & /*unused*/) override;
  bool PreVisit(ScanAllByEdge & /*unused*/) override;
  bool PreVisit(ScanAllByEdgeType & /*unused*/) override;
  bool PreVisit(ScanAllByEdgeTypeProperty & /*unused*/) override;
  bool PreVisit(ScanAllByEdgeTypePropertyValue & /*unused*/) override;
  bool PreVisit(ScanAllByEdgeTypePropertyRange & /*unused*/) override;
  bool PreVisit(ScanAllByEdgeProperty & /*unused*/) override;
  bool PreVisit(ScanAllByEdgePropertyValue & /*unused*/) override;
  bool PreVisit(ScanAllByEdgePropertyRange & /*unused*/) override;
  bool PreVisit(ScanAllByPointDistance & /*unused*/) override;
  bool PreVisit(ScanAllByPointWithinbbox & /*unused*/) override;
  bool PreVisit(ScanAllByEdgeId & /*unused*/) override;

  bool PreVisit(Expand & /*unused*/) override;
  bool PreVisit(ExpandVariable & /*unused*/) override;

  bool PreVisit(ConstructNamedPath & /*unused*/) override;

  bool PreVisit(Filter & /*unused*/) override;
  bool PreVisit(EvaluatePatternFilter & /*unused*/) override;
  bool PreVisit(EdgeUniquenessFilter & /*unused*/) override;

  bool PreVisit(Merge & /*unused*/) override;
  bool PreVisit(Optional & /*unused*/) override;
  bool PreVisit(Cartesian & /*unused*/) override;
  bool PreVisit(HashJoin & /*unused*/) override;

  bool PreVisit(EmptyResult & /*unused*/) override;
  bool PreVisit(Produce & /*unused*/) override;
  bool PreVisit(Accumulate & /*unused*/) override;
  bool PreVisit(Aggregate & /*unused*/) override;
  bool PreVisit(Skip & /*unused*/) override;
  bool PreVisit(Limit & /*unused*/) override;
  bool PreVisit(OrderBy & /*unused*/) override;
  bool PreVisit(Distinct & /*unused*/) override;
  bool PreVisit(Union & /*unused*/) override;
  bool PreVisit(RollUpApply & /*unused*/) override;
  bool PreVisit(PeriodicCommit & /*unused*/) override;
  bool PreVisit(PeriodicSubquery & /*unused*/) override;
  bool PreVisit(SetNestedProperty & /*unused*/) override;
  bool PreVisit(RemoveNestedProperty & /*unused*/) override;

  bool PreVisit(Unwind & /*unused*/) override;
  bool PreVisit(CallProcedure & /*unused*/) override;
  bool PreVisit(LoadCsv & /*unused*/) override;
  bool PreVisit(LoadParquet &) override;
  bool PreVisit(Foreach & /*unused*/) override;
  bool PreVisit(Apply & /*unused*/) override;
  bool PreVisit(IndexedJoin & /*unused*/) override;

  bool Visit(Once & /*unused*/) override;

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