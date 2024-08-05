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

#include "query/plan/plan_validator.hpp"

#include "query/exceptions.hpp"

namespace memgraph::query::plan {

bool PlanValidator::Visit(Once & /*op*/) { return true; }

bool PlanValidator::PreVisit(PeriodicCommit & /*op*/) {
  has_periodic_execution_ = true;
  return true;
}

bool PlanValidator::PreVisit(PeriodicSubquery & /*op*/) {
  has_periodic_execution_ = true;
  return true;
}

void PlanValidator::Validate(LogicalOperator &root, PlanValidatorParameters parameters) {
  root.Accept(*this);

  if (has_periodic_execution_ && parameters.is_profile_query) {
    throw SemanticException("Periodic execution not allowed inside profile query!");
  }

  if (has_periodic_execution_ && parameters.is_explicit_transaction) {
    throw SemanticException("Periodic execution not allowed inside an explicit transaction!");
  }
}
}  // namespace memgraph::query::plan
