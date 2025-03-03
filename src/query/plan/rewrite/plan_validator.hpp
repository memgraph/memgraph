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

#include <memory>
#include "query/plan/operator.hpp"

namespace memgraph::query::plan {

namespace impl {

class PlanValidator final : public HierarchicalLogicalOperatorVisitor {
 public:
  PlanValidator(const SymbolTable &symbol_table) : symbol_table_(symbol_table) {}

  ~PlanValidator() override = default;

  using HierarchicalLogicalOperatorVisitor::PostVisit;
  using HierarchicalLogicalOperatorVisitor::PreVisit;
  using HierarchicalLogicalOperatorVisitor::Visit;

  bool Visit(Once &t) override { return true; }

  bool PreVisit(ScanAllByEdge &op) override {
    is_valid_plan_ = false;
    return false;
  }

  bool PreVisit(ScanAllByEdgeType &op) override {
    if (scope_.should_forbid_scanallbyedge_) {
      is_valid_plan_ = false;
      return false;
    }
    return true;
  }

  bool PreVisit(ScanAllByEdgeTypeProperty &op) override {
    if (scope_.should_forbid_scanallbyedge_) {
      is_valid_plan_ = false;
      return false;
    }
    return true;
  }

  bool PreVisit(ScanAllByEdgeTypePropertyValue &op) override {
    if (scope_.should_forbid_scanallbyedge_) {
      is_valid_plan_ = false;
      return false;
    }
    return true;
  }

  bool PreVisit(ScanAllByEdgeTypePropertyRange &op) override {
    if (scope_.should_forbid_scanallbyedge_) {
      is_valid_plan_ = false;
      return false;
    }
    return true;
  }

  bool PreVisit(Optional &op) override {
    // create new plan validator that will go into the input branch so we don't recurse
    // and invalidate the optional_expand flag early
    if (!IsBranchValid(&op.input_)) {
      is_valid_plan_ = false;
      return false;
    }

    auto input_modified_symbols = op.input_->ModifiedSymbols(symbol_table_);
    auto optional_modified_symbols = op.optional_->ModifiedSymbols(symbol_table_);

    std::unordered_set<Symbol> input_symbols(input_modified_symbols.begin(), input_modified_symbols.end());
    std::vector<Symbol> intersection;
    for (auto symbol : optional_modified_symbols) {
      if (input_symbols.count(symbol)) {
        intersection.push_back(symbol);
        break;
      }
    }

    if (!intersection.empty()) {
      // we should now reject plans which use indexed edge scan since we would like to rather expand from existing
      scope_.should_forbid_scanallbyedge_ = true;
    }

    op.optional_->Accept(*this);

    scope_.should_forbid_scanallbyedge_ = false;

    // we have manually gotten through the input and the optional so we can quit the execution
    return false;
  }

  bool IsValidPlan() { return is_valid_plan_; }

 private:
  struct Scope {
    bool should_forbid_scanallbyedge_{false};
  };
  const SymbolTable &symbol_table_;
  bool is_valid_plan_{true};
  Scope scope_;

  bool IsBranchValid(std::shared_ptr<LogicalOperator> *branch) {
    auto rewriter = PlanValidator{symbol_table_};
    (*branch)->Accept(rewriter);
    return rewriter.IsValidPlan();
  }
};

}  // namespace impl

inline bool ValidatePlan(LogicalOperator &root_op, const SymbolTable &symbol_table) {
  auto rewriter = impl::PlanValidator{symbol_table};
  root_op.Accept(rewriter);
  return rewriter.IsValidPlan();
}

}  // namespace memgraph::query::plan
