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

// NOLINTNEXTLINE(cppcoreguidelines-macro-usage)
#define PRE_VISIT_PARALLEL(TOp)                                                  \
  bool ParallelChecker::PreVisit(TOp &) { /*NOLINT(bugprone-macro-parentheses)*/ \
    is_parallelized_ = true;                                                     \
    return false; /* Stop traversal once we find a parallel operator */          \
  }

namespace memgraph::query::plan {

PRE_VISIT_PARALLEL(AggregateParallel)
PRE_VISIT_PARALLEL(ParallelMerge)
PRE_VISIT_PARALLEL(ScanParallel)
PRE_VISIT_PARALLEL(ScanParallelById)
PRE_VISIT_PARALLEL(ScanParallelByLabel)
PRE_VISIT_PARALLEL(ScanParallelByLabelProperties)
PRE_VISIT_PARALLEL(ScanParallelByPointDistance)
PRE_VISIT_PARALLEL(ScanParallelByWithinbbox)
PRE_VISIT_PARALLEL(ScanParallelByEdge)
PRE_VISIT_PARALLEL(ScanParallelByEdgeType)
PRE_VISIT_PARALLEL(ScanParallelByEdgeTypeProperty)
PRE_VISIT_PARALLEL(ScanParallelByEdgeTypePropertyValue)
PRE_VISIT_PARALLEL(ScanParallelByEdgeTypePropertyRange)
PRE_VISIT_PARALLEL(ScanParallelByEdgeProperty)
PRE_VISIT_PARALLEL(ScanParallelByEdgePropertyValue)
PRE_VISIT_PARALLEL(ScanParallelByEdgePropertyRange)
PRE_VISIT_PARALLEL(ScanParallelByEdgeId)

#undef PRE_VISIT_PARALLEL

bool ParallelChecker::Visit(Once &) { return false; }  // NOLINT(hicpp-named-parameter)

void ParallelChecker::CheckParallelized(const LogicalOperator &root) {
  is_parallelized_ = false;
  // NOLINTNEXTLINE(cppcoreguidelines-pro-type-const-cast)
  const_cast<LogicalOperator *>(&root)->Accept(*this);
}

}  // namespace memgraph::query::plan
