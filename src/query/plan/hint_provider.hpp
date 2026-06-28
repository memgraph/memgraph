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

/// @file
/// This file provides textual information about possible inefficiencies in the query planner.
/// An inefficiency is for example having a sequential scan with filtering, without the usage of indices.

#pragma once

#include <algorithm>
#include <vector>

#include <boost/algorithm/string.hpp>
#include <range/v3/view.hpp>

#include "query/plan/operator.hpp"
#include "query/plan/preprocess.hpp"
#include "utils/logging.hpp"
#include "utils/string.hpp"

namespace memgraph::query::plan {

struct PlanHintsResult {
  std::vector<std::string> hints;
  // True if the plan has a label/label-property-filtered node scan that a matching index could have served.
  bool has_unindexed_scan{false};
};

[[nodiscard]] PlanHintsResult ProvidePlanHints(const LogicalOperator *plan_root, const SymbolTable &symbol_table);

class PlanHintsProvider final : public HierarchicalLogicalOperatorVisitor {
  // Accessors are valid only post-traversal; ProvidePlanHints owns the construct -> Accept -> read lifecycle.
  friend PlanHintsResult ProvidePlanHints(const LogicalOperator *plan_root, const SymbolTable &symbol_table);

 public:
  explicit PlanHintsProvider(const SymbolTable &symbol_table) : symbol_table_(symbol_table) {}

  using HierarchicalLogicalOperatorVisitor::PostVisit;
  using HierarchicalLogicalOperatorVisitor::PreVisit;
  using HierarchicalLogicalOperatorVisitor::Visit;

  bool Visit(Once & /*unused*/) override { return true; }

  bool PreVisit(Filter & /*unused*/) override { return true; }

  bool PostVisit(Filter &op) override {
    HintIndexUsage(op);
    return true;
  }

  bool PreVisit(ScanAll & /*unused*/) override { return true; }

  bool PostVisit(ScanAll & /*unused*/) override { return true; }

  bool PreVisit(Expand & /*unused*/) override { return true; }

  bool PostVisit(Expand & /*expand*/) override { return true; }

  bool PreVisit(ExpandVariable & /*unused*/) override { return true; }

  bool PostVisit(ExpandVariable & /*unused*/) override { return true; }

  bool PreVisit(Merge &op) override {
    op.input()->Accept(*this);
    op.merge_match_->Accept(*this);
    return false;
  }

  bool PostVisit(Merge & /*unused*/) override { return true; }

  bool PreVisit(Optional &op) override {
    op.input()->Accept(*this);
    op.optional_->Accept(*this);
    return false;
  }

  bool PostVisit(Optional & /*unused*/) override { return true; }

  bool PreVisit(Cartesian &op) override {
    op.left_op_->Accept(*this);
    op.right_op_->Accept(*this);
    return false;
  }

  bool PostVisit(Cartesian & /*unused*/) override { return true; }

  bool PreVisit(Union &op) override {
    op.left_op_->Accept(*this);
    op.right_op_->Accept(*this);
    return false;
  }

  bool PostVisit(Union & /*unused*/) override { return true; }

  bool PreVisit(CreateNode & /*unused*/) override { return true; }

  bool PostVisit(CreateNode & /*unused*/) override { return true; }

  bool PreVisit(CreateExpand & /*unused*/) override { return true; }

  bool PostVisit(CreateExpand & /*unused*/) override { return true; }

  bool PreVisit(ScanAllByLabel & /*unused*/) override { return true; }

  bool PostVisit(ScanAllByLabel & /*unused*/) override { return true; }

  bool PreVisit(ScanAllByLabelProperties & /*unused*/) override { return true; }

  bool PostVisit(ScanAllByLabelProperties & /*unused*/) override { return true; }

  bool PreVisit(ScanAllById & /*unused*/) override { return true; }

  bool PostVisit(ScanAllById & /*unused*/) override { return true; }

  bool PreVisit(ScanAllByEdge & /*unused*/) override { return true; }

  bool PostVisit(ScanAllByEdge & /*unused*/) override { return true; }

  bool PreVisit(ScanAllByEdgeType & /*unused*/) override { return true; }

  bool PostVisit(ScanAllByEdgeType & /*unused*/) override { return true; }

  bool PreVisit(ScanAllByEdgeTypeProperty & /*unused*/) override { return true; }

  bool PostVisit(ScanAllByEdgeTypeProperty & /*unused*/) override { return true; }

  bool PreVisit(ScanAllByEdgeTypePropertyValue & /*unused*/) override { return true; }

  bool PostVisit(ScanAllByEdgeTypePropertyValue & /*unused*/) override { return true; }

  bool PreVisit(ScanAllByEdgeTypePropertyRange & /*unused*/) override { return true; }

  bool PostVisit(ScanAllByEdgeTypePropertyRange & /*unused*/) override { return true; }

  bool PreVisit(ScanAllByEdgeProperty & /*unused*/) override { return true; }

  bool PostVisit(ScanAllByEdgeProperty & /*unused*/) override { return true; }

  bool PreVisit(ScanAllByEdgePropertyValue & /*unused*/) override { return true; }

  bool PostVisit(ScanAllByEdgePropertyValue & /*unused*/) override { return true; }

  bool PreVisit(ScanAllByEdgePropertyRange & /*unused*/) override { return true; }

  bool PostVisit(ScanAllByEdgePropertyRange & /*unused*/) override { return true; }

  bool PreVisit(ScanAllByEdgeId & /*unused*/) override { return true; }

  bool PostVisit(ScanAllByEdgeId & /*unused*/) override { return true; }

  bool PreVisit(ScanAllByPointDistance & /*unused*/) override { return true; }

  bool PostVisit(ScanAllByPointDistance & /*unused*/) override { return true; }

  bool PreVisit(ScanAllByPointWithinbbox & /*unused*/) override { return true; }

  bool PostVisit(ScanAllByPointWithinbbox & /*unused*/) override { return true; }

  bool PreVisit(ScanChunk & /*unused*/) override { return true; }

  bool PostVisit(ScanChunk & /*unused*/) override { return true; }

  bool PreVisit(ScanChunkByEdge & /*unused*/) override { return true; }

  bool PostVisit(ScanChunkByEdge & /*unused*/) override { return true; }

  bool PreVisit(ConstructNamedPath & /*unused*/) override { return true; }

  bool PostVisit(ConstructNamedPath & /*unused*/) override { return true; }

  bool PreVisit(Produce & /*unused*/) override { return true; }

  bool PostVisit(Produce & /*unused*/) override { return true; }

  bool PreVisit(EmptyResult & /*unused*/) override { return true; }

  bool PostVisit(EmptyResult & /*unused*/) override { return true; }

  bool PreVisit(Delete & /*unused*/) override { return true; }

  bool PostVisit(Delete & /*unused*/) override { return true; }

  bool PreVisit(SetProperty & /*unused*/) override { return true; }

  bool PostVisit(SetProperty & /*unused*/) override { return true; }

  bool PreVisit(SetProperties & /*unused*/) override { return true; }

  bool PostVisit(SetProperties & /*unused*/) override { return true; }

  bool PreVisit(SetLabels & /*unused*/) override { return true; }

  bool PostVisit(SetLabels & /*unused*/) override { return true; }

  bool PreVisit(RemoveProperty & /*unused*/) override { return true; }

  bool PostVisit(RemoveProperty & /*unused*/) override { return true; }

  bool PreVisit(RemoveLabels & /*unused*/) override { return true; }

  bool PostVisit(RemoveLabels & /*unused*/) override { return true; }

  bool PreVisit(EdgeUniquenessFilter & /*unused*/) override { return true; }

  bool PostVisit(EdgeUniquenessFilter & /*unused*/) override { return true; }

  bool PreVisit(Accumulate & /*unused*/) override { return true; }

  bool PostVisit(Accumulate & /*unused*/) override { return true; }

  bool PreVisit(Aggregate & /*unused*/) override { return true; }

  bool PostVisit(Aggregate & /*unused*/) override { return true; }

  bool PreVisit(AggregateParallel & /*unused*/) override { return true; }

  bool PostVisit(AggregateParallel & /*unused*/) override { return true; }

  bool PreVisit(ParallelMerge & /*unused*/) override { return true; }

  bool PostVisit(ParallelMerge & /*unused*/) override { return true; }

  bool PreVisit(ScanParallel & /*unused*/) override { return true; }

  bool PostVisit(ScanParallel & /*unused*/) override { return true; }

  bool PreVisit(ScanParallelByLabel & /*unused*/) override { return true; }

  bool PostVisit(ScanParallelByLabel & /*unused*/) override { return true; }

  bool PreVisit(ScanParallelByLabelProperties & /*unused*/) override { return true; }

  bool PostVisit(ScanParallelByLabelProperties & /*unused*/) override { return true; }

  bool PreVisit(ScanParallelByEdge & /*unused*/) override { return true; }

  bool PostVisit(ScanParallelByEdge & /*unused*/) override { return true; }

  bool PreVisit(ScanParallelByEdgeType & /*unused*/) override { return true; }

  bool PostVisit(ScanParallelByEdgeType & /*unused*/) override { return true; }

  bool PreVisit(ScanParallelByEdgeTypeProperty & /*unused*/) override { return true; }

  bool PostVisit(ScanParallelByEdgeTypeProperty & /*unused*/) override { return true; }

  bool PreVisit(ScanParallelByEdgeTypePropertyValue & /*unused*/) override { return true; }

  bool PostVisit(ScanParallelByEdgeTypePropertyValue & /*unused*/) override { return true; }

  bool PreVisit(ScanParallelByEdgeTypePropertyRange & /*unused*/) override { return true; }

  bool PostVisit(ScanParallelByEdgeTypePropertyRange & /*unused*/) override { return true; }

  bool PreVisit(ScanParallelByEdgeProperty & /*unused*/) override { return true; }

  bool PostVisit(ScanParallelByEdgeProperty & /*unused*/) override { return true; }

  bool PreVisit(ScanParallelByEdgePropertyValue & /*unused*/) override { return true; }

  bool PostVisit(ScanParallelByEdgePropertyValue & /*unused*/) override { return true; }

  bool PreVisit(ScanParallelByEdgePropertyRange & /*unused*/) override { return true; }

  bool PostVisit(ScanParallelByEdgePropertyRange & /*unused*/) override { return true; }

  bool PreVisit(Skip & /*unused*/) override { return true; }

  bool PostVisit(Skip & /*unused*/) override { return true; }

  bool PreVisit(Limit & /*unused*/) override { return true; }

  bool PostVisit(Limit & /*unused*/) override { return true; }

  bool PreVisit(OrderBy & /*unused*/) override { return true; }

  bool PostVisit(OrderBy & /*unused*/) override { return true; }

  bool PreVisit(OrderByParallel & /*unused*/) override { return true; }

  bool PostVisit(OrderByParallel & /*unused*/) override { return true; }

  bool PreVisit(Unwind & /*unused*/) override { return true; }

  bool PostVisit(Unwind & /*unused*/) override { return true; }

  bool PreVisit(Distinct & /*unused*/) override { return true; }

  bool PostVisit(Distinct & /*unused*/) override { return true; }

  bool PreVisit(CallProcedure & /*unused*/) override { return true; }

  bool PostVisit(CallProcedure & /*unused*/) override { return true; }

  bool PreVisit(Foreach &op) override {
    op.input()->Accept(*this);
    op.update_clauses_->Accept(*this);
    return false;
  }

  bool PostVisit(Foreach & /*unused*/) override { return true; }

  bool PreVisit(EvaluatePatternFilter & /*unused*/) override { return true; }

  bool PostVisit(EvaluatePatternFilter & /*op*/) override { return true; }

  bool PreVisit(Apply &op) override {
    op.input()->Accept(*this);
    op.subquery_->Accept(*this);
    return false;
  }

  bool PostVisit(Apply & /*op*/) override { return true; }

  bool PreVisit(LoadCsv & /*unused*/) override { return true; }

  bool PostVisit(LoadCsv & /*unused*/) override { return true; }

  bool PreVisit(LoadParquet & /*unused*/) override { return true; }

  bool PostVisit(LoadParquet & /*unused*/) override { return true; }

  bool PreVisit(LoadJsonl & /*unused*/) override { return true; }

  bool PostVisit(LoadJsonl & /*unused*/) override { return true; }

  bool PreVisit(HashJoin &op) override {
    op.left_op_->Accept(*this);
    op.right_op_->Accept(*this);
    return false;
  }

  bool PostVisit(HashJoin & /*unused*/) override { return true; }

  bool PreVisit(IndexedJoin &op) override {
    op.main_branch_->Accept(*this);
    op.sub_branch_->Accept(*this);
    return false;
  }

  bool PostVisit(IndexedJoin & /*unused*/) override { return true; }

  bool PreVisit(RollUpApply &op) override {
    op.input()->Accept(*this);
    op.list_collection_branch_->Accept(*this);
    return false;
  }

  bool PostVisit(RollUpApply & /*unused*/) override { return true; }

  bool PreVisit(PeriodicCommit & /*unused*/) override { return true; }

  bool PostVisit(PeriodicCommit & /*unused*/) override { return true; }

  bool PreVisit(PeriodicSubquery &op) override {
    op.input()->Accept(*this);
    op.subquery_->Accept(*this);
    return false;
  }

  bool PostVisit(PeriodicSubquery & /*op*/) override { return true; }

  bool PreVisit(SetNestedProperty & /*unused*/) override { return true; }

  bool PostVisit(SetNestedProperty & /*op*/) override { return true; }

  bool PreVisit(RemoveNestedProperty & /*unused*/) override { return true; }

  bool PostVisit(RemoveNestedProperty & /*op*/) override { return true; }

 private:
  const SymbolTable &symbol_table_;
  std::vector<std::string> hints_;
  bool has_unindexed_scan_{false};

  [[nodiscard]] std::vector<std::string> take_hints() { return std::move(hints_); }

  [[nodiscard]] bool has_unindexed_scan() const { return has_unindexed_scan_; }

  bool DefaultPreVisit() override { LOG_FATAL("Operator not implemented for providing plan hints!"); }

  // Records a missing-/suboptimal-index hint for a Filter and sets has_unindexed_scan_ accordingly.
  void HintIndexUsage(Filter &op);

  template <typename Func>
  std::string ExtractAndJoin(auto &&collection, Func &&projection) {
    auto elements = collection | ranges::views::transform(std::forward<Func>(projection));
    return utils::Join(elements, ", ");
  }
};

}  // namespace memgraph::query::plan
