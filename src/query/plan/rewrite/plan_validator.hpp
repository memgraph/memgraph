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
    if (scope_.in_optional_expand_) {
      is_valid_plan_ = false;
      return false;
    }
    return true;
  }

  bool PreVisit(ScanAllByEdgeTypeProperty &op) override {
    if (scope_.in_optional_expand_) {
      is_valid_plan_ = false;
      return false;
    }
    return true;
  }

  bool PreVisit(ScanAllByEdgeTypePropertyValue &op) override {
    if (scope_.in_optional_expand_) {
      is_valid_plan_ = false;
      return false;
    }
    return true;
  }

  bool PreVisit(ScanAllByEdgeTypePropertyRange &op) override {
    if (scope_.in_optional_expand_) {
      is_valid_plan_ = false;
      return false;
    }
    return true;
  }

  bool PreVisit(Optional &op) override {
    auto io_symbols = op.input_->OutputSymbols(symbol_table_);
    auto im_symbols = op.input_->ModifiedSymbols(symbol_table_);
    auto oo_symbols = op.optional_->OutputSymbols(symbol_table_);
    auto om_symbols = op.optional_->ModifiedSymbols(symbol_table_);
    if (op.input_->GetTypeInfo() != Once::kType) {
      scope_.in_optional_expand_ = true;
    }
    return true;
  }

  bool PostVisit(Optional &op) override {
    scope_.in_optional_expand_ = false;
    return true;
  }

  bool IsValidPlan() { return is_valid_plan_; }

 private:
  struct Scope {
    bool in_optional_expand_{false};
  };
  const SymbolTable &symbol_table_;
  bool is_valid_plan_{true};
  Scope scope_;
};

}  // namespace impl

inline bool ValidatePlan(LogicalOperator &root_op, const SymbolTable &symbol_table) {
  auto rewriter = impl::PlanValidator{symbol_table};
  root_op.Accept(rewriter);
  return rewriter.IsValidPlan();
}

}  // namespace memgraph::query::plan
