// Copyright 2024 Memgraph Ltd.
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

std::vector<std::string> ProvidePlanHints(const LogicalOperator *plan_root, const SymbolTable &symbol_table);

class PlanHintsProvider final : public HierarchicalLogicalOperatorVisitor {
 public:
  explicit PlanHintsProvider(const SymbolTable &symbol_table) : symbol_table_(symbol_table) {}

  std::vector<std::string> &hints() { return hints_; }

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

  bool PreVisit(ScanAllByLabelPropertyRange & /*unused*/) override { return true; }
  bool PostVisit(ScanAllByLabelPropertyRange & /*unused*/) override { return true; }

  bool PreVisit(ScanAllByLabelPropertyValue & /*unused*/) override { return true; }
  bool PostVisit(ScanAllByLabelPropertyValue & /*unused*/) override { return true; }

  bool PreVisit(ScanAllByLabelProperty & /*unused*/) override { return true; }
  bool PostVisit(ScanAllByLabelProperty & /*unused*/) override { return true; }

  bool PreVisit(ScanAllById & /*unused*/) override { return true; }
  bool PostVisit(ScanAllById & /*unused*/) override { return true; }

  bool PreVisit(ScanAllByEdgeType & /*unused*/) override { return true; }
  bool PostVisit(ScanAllByEdgeType & /*unused*/) override { return true; }

  bool PreVisit(ScanAllByEdgeTypeProperty & /*unused*/) override { return true; }
  bool PostVisit(ScanAllByEdgeTypeProperty & /*unused*/) override { return true; }

  bool PreVisit(ScanAllByEdgeId & /*unused*/) override { return true; }
  bool PostVisit(ScanAllByEdgeId & /*unused*/) override { return true; }

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

  bool PreVisit(Skip & /*unused*/) override { return true; }
  bool PostVisit(Skip & /*unused*/) override { return true; }

  bool PreVisit(Limit & /*unused*/) override { return true; }
  bool PostVisit(Limit & /*unused*/) override { return true; }

  bool PreVisit(OrderBy & /*unused*/) override { return true; }
  bool PostVisit(OrderBy & /*unused*/) override { return true; }

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

 private:
  const SymbolTable &symbol_table_;
  std::vector<std::string> hints_;

  bool DefaultPreVisit() override { LOG_FATAL("Operator not implemented for providing plan hints!"); }

  void HintIndexUsage(Filter &op) {
    if (auto *maybe_scan_operator = dynamic_cast<ScanAll *>(op.input().get()); !maybe_scan_operator) {
      return;
    }

    auto const scan_symbol = dynamic_cast<ScanAll *>(op.input().get())->output_symbol_;
    auto const scan_type = op.input()->GetTypeInfo();

    Filters filters;
    filters.CollectFilterExpression(op.expression_, symbol_table_);
    const std::string filtered_labels = ExtractAndJoin(filters.FilteredLabels(scan_symbol),
                                                       [](const auto &item) { return fmt::format(":{0}", item.name); });
    const std::string filtered_properties =
        ExtractAndJoin(filters.FilteredProperties(scan_symbol), [](const auto &item) { return item.name; });

    if (filtered_labels.empty() && filtered_properties.empty()) {
      return;
    }

    if (scan_type == ScanAll::kType) {
      if (!filtered_labels.empty() && !filtered_properties.empty()) {
        hints_.push_back(
            fmt::format("Sequential scan will be used on symbol `{0}` although there is a filter on labels {1} and "
                        "properties {2}. Consider "
                        "creating a label-property index.",
                        scan_symbol.name(), filtered_labels, filtered_properties));
        return;
      }

      if (!filtered_labels.empty()) {
        hints_.push_back(fmt::format(
            "Sequential scan will be used on symbol `{0}` although there is a filter on labels {1}. Consider "
            "creating a label index.",
            scan_symbol.name(), filtered_labels));
        return;
      }
      return;
    }

    if (scan_type == ScanAllByLabel::kType && !filtered_properties.empty()) {
      hints_.push_back(fmt::format(
          "Label index will be used on symbol `{0}` although there is also a filter on properties {1}. Consider "
          "creating a label-property index.",
          scan_symbol.name(), filtered_properties));
      return;
    }
  }

  std::string ExtractAndJoin(auto &&collection, auto &&projection) {
    auto elements = collection | ranges::views::transform(projection);
    return boost::algorithm::join(elements, ", ");
  }
};

}  // namespace memgraph::query::plan
