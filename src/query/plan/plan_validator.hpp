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

#pragma once

#include "query/plan/operator.hpp"

namespace memgraph::query::plan {

struct PlanValidatorParameters {
  bool is_explicit_transaction{false};
  bool is_profile_query{false};
};

class PlanValidator final : public HierarchicalLogicalOperatorVisitor {
 public:
  PlanValidator() = default;

  PlanValidator(const PlanValidator &) = delete;
  PlanValidator(PlanValidator &&) = delete;

  PlanValidator &operator=(const PlanValidator &) = delete;
  PlanValidator &operator=(PlanValidator &&) = delete;

  void Validate(LogicalOperator &root, PlanValidatorParameters parameters);

  using HierarchicalLogicalOperatorVisitor::PostVisit;
  using HierarchicalLogicalOperatorVisitor::PreVisit;
  using HierarchicalLogicalOperatorVisitor::Visit;

  bool Visit(Once &) override;

  bool PreVisit(PeriodicCommit &) override;
  bool PreVisit(PeriodicSubquery &) override;

 private:
  bool has_periodic_execution_{false};
};

}  // namespace memgraph::query::plan
