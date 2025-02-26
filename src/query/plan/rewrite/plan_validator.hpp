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
  PlanValidator() {}

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
    if (scope_.in_optional) {
      is_valid_plan_ = false;
      return false;
    }
    return true;
  }

  bool PreVisit(ScanAllByEdgeTypeProperty &op) override {
    if (scope_.in_optional) {
      is_valid_plan_ = false;
      return false;
    }
    return true;
  }

  bool PreVisit(ScanAllByEdgeTypePropertyValue &op) override {
    if (scope_.in_optional) {
      is_valid_plan_ = false;
      return false;
    }
    return true;
  }

  bool PreVisit(ScanAllByEdgeTypePropertyRange &op) override {
    if (scope_.in_optional) {
      is_valid_plan_ = false;
      return false;
    }
    return true;
  }

  bool PreVisit(Optional &op) override {
    scope_.in_optional = true;
    return true;
  }

  bool PostVisit(Optional &op) override {
    scope_.in_optional = false;
    return true;
  }

  bool IsValidPlan() { return is_valid_plan_; }

 private:
  struct Scope {
    bool in_optional{false};
  };
  bool is_valid_plan_{true};
  Scope scope_;
};

}  // namespace impl

inline bool ValidatePlan(LogicalOperator &root_op) {
  auto rewriter = impl::PlanValidator{};
  root_op.Accept(rewriter);
  return rewriter.IsValidPlan();
}

}  // namespace memgraph::query::plan
