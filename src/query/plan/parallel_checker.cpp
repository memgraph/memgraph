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

#include "query/plan/parallel_checker.hpp"
namespace memgraph::query::plan {
bool ParallelChecker::PreVisit(AggregateParallel &) {
  is_parallelized_ = true;
  return false;
}

bool ParallelChecker::Visit(Once &) { return false; }  // NOLINT(hicpp-named-parameter)

void ParallelChecker::CheckParallelized(const LogicalOperator &root) {
  is_parallelized_ = false;
  // NOLINTNEXTLINE(cppcoreguidelines-pro-type-const-cast)
  const_cast<LogicalOperator *>(&root)->Accept(*this);
}

}  // namespace memgraph::query::plan
