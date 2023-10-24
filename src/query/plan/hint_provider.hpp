// Copyright 2023 Memgraph Ltd.
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
  PlanHintsProvider(const SymbolTable &symbol_table) : symbol_table_(symbol_table) {}

  std::vector<std::string> &hints() { return hints_; }

  using HierarchicalLogicalOperatorVisitor::PostVisit;
  using HierarchicalLogicalOperatorVisitor::PreVisit;
  using HierarchicalLogicalOperatorVisitor::Visit;

  bool Visit(Once &) override { return true; }

  bool PreVisit(Filter &op) override { return true; }

  bool PostVisit(Filter &op) override {
    HintIndexUsage(op);
    return true;
  }

  bool PreVisit(ScanAll &op) override { return true; }

  bool PostVisit(ScanAll &scan) override { return true; }

  bool PreVisit(Expand &op) override { return true; }

  bool PostVisit(Expand & /*expand*/) override { return true; }

  bool PreVisit(ExpandVariable &op) override { return true; }

  bool PostVisit(ExpandVariable &expand) override { return true; }

  bool PreVisit(Merge &op) override {
    op.input()->Accept(*this);
    op.merge_match_->Accept(*this);
    return false;
  }

  bool PostVisit(Merge &) override { return true; }

  bool PreVisit(Optional &op) override {
    op.input()->Accept(*this);
    op.optional_->Accept(*this);
    return false;
  }

  bool PostVisit(Optional &) override { return true; }

  bool PreVisit(Cartesian &op) override {
    op.left_op_->Accept(*this);
    op.right_op_->Accept(*this);
    return false;
  }

  bool PostVisit(Cartesian &) override { return true; }

  bool PreVisit(Union &op) override {
    op.left_op_->Accept(*this);
    op.right_op_->Accept(*this);
    return false;
  }

  bool PostVisit(Union &) override { return true; }

  bool PreVisit(CreateNode &op) override { return true; }
  bool PostVisit(CreateNode &) override { return true; }

  bool PreVisit(CreateExpand &op) override { return true; }
  bool PostVisit(CreateExpand &) override { return true; }

  bool PreVisit(ScanAllByLabel &op) override { return true; }
  bool PostVisit(ScanAllByLabel &) override { return true; }

  bool PreVisit(ScanAllByLabelPropertyRange &op) override { return true; }
  bool PostVisit(ScanAllByLabelPropertyRange &) override { return true; }

  bool PreVisit(ScanAllByLabelPropertyValue &op) override { return true; }
  bool PostVisit(ScanAllByLabelPropertyValue &) override { return true; }

  bool PreVisit(ScanAllByLabelProperty &op) override { return true; }
  bool PostVisit(ScanAllByLabelProperty &) override { return true; }

  bool PreVisit(ScanAllById &op) override { return true; }
  bool PostVisit(ScanAllById &) override { return true; }

  bool PreVisit(ConstructNamedPath &op) override { return true; }
  bool PostVisit(ConstructNamedPath &) override { return true; }

  bool PreVisit(Produce &op) override { return true; }
  bool PostVisit(Produce &) override { return true; }

  bool PreVisit(EmptyResult &op) override { return true; }
  bool PostVisit(EmptyResult &) override { return true; }

  bool PreVisit(Delete &op) override { return true; }
  bool PostVisit(Delete &) override { return true; }

  bool PreVisit(SetProperty &op) override { return true; }
  bool PostVisit(SetProperty &) override { return true; }

  bool PreVisit(SetProperties &op) override { return true; }
  bool PostVisit(SetProperties &) override { return true; }

  bool PreVisit(SetLabels &op) override { return true; }
  bool PostVisit(SetLabels &) override { return true; }

  bool PreVisit(RemoveProperty &op) override { return true; }
  bool PostVisit(RemoveProperty &) override { return true; }

  bool PreVisit(RemoveLabels &op) override { return true; }
  bool PostVisit(RemoveLabels &) override { return true; }

  bool PreVisit(EdgeUniquenessFilter &op) override { return true; }
  bool PostVisit(EdgeUniquenessFilter &) override { return true; }

  bool PreVisit(Accumulate &op) override { return true; }
  bool PostVisit(Accumulate &) override { return true; }

  bool PreVisit(Aggregate &op) override { return true; }
  bool PostVisit(Aggregate &) override { return true; }

  bool PreVisit(Skip &op) override { return true; }
  bool PostVisit(Skip &) override { return true; }

  bool PreVisit(Limit &op) override { return true; }
  bool PostVisit(Limit &) override { return true; }

  bool PreVisit(OrderBy &op) override { return true; }
  bool PostVisit(OrderBy &) override { return true; }

  bool PreVisit(Unwind &op) override { return true; }
  bool PostVisit(Unwind &) override { return true; }

  bool PreVisit(Distinct &op) override { return true; }
  bool PostVisit(Distinct &) override { return true; }

  bool PreVisit(CallProcedure &op) override { return true; }
  bool PostVisit(CallProcedure &) override { return true; }

  bool PreVisit(Foreach &op) override {
    op.input()->Accept(*this);
    op.update_clauses_->Accept(*this);
    return false;
  }

  bool PostVisit(Foreach &) override { return true; }

  bool PreVisit(EvaluatePatternFilter &op) override { return true; }

  bool PostVisit(EvaluatePatternFilter & /*op*/) override { return true; }

  bool PreVisit(Apply &op) override {
    op.input()->Accept(*this);
    op.subquery_->Accept(*this);
    return false;
  }

  bool PostVisit(Apply & /*op*/) override { return true; }

  bool PreVisit(LoadCsv &op) override { return true; }

  bool PostVisit(LoadCsv & /*op*/) override { return true; }

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
