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

#pragma once

#include <cstdint>

namespace memgraph::query::plan {

/// Why a return body cannot leave its projection until after the sort and the limit.
///
/// A projection emits one row per row it reads and keeps their order, so sorting and limiting
/// underneath it selects the same rows while leaving the projected expressions to run only for
/// the survivors. It is wrong whenever a row reaching the projection could not be rebuilt from
/// what the sort replays. Carrying the reason rather than a yes or no is what lets a test name
/// the rule it checks.
enum class NoDeferReason : uint8_t {
  kDeferred,
  kNoOrderBy,
  /// Every row is projected either way, so deferring saves nothing.
  kNoLimit,
  /// DISTINCT or a trailing WHERE: both change how many rows reach the limit.
  kCardinalityChanges,
  /// A branch under the ORDER BY reads what the projection writes.
  kOrderByBranch,
  /// A branch under the projection writes its answer per row, and the sort cannot replay it.
  kProjectionBranch,
  /// An aggregate writes one row per group, and the sort cannot replay it.
  kAggregation,
  /// A sort key names something not bound when the clause began, such as a projected alias.
  kSortKeyNotBound,
  /// A projected expression reads something not bound when the clause began.
  kProjectionReadsUnbound,
};

/// What a return body looks like to the decision below, gathered by the planner. Kept apart from
/// the decision so that one stays a total function and can be checked without planning a query.
struct ProjectionFacts {
  bool has_order_by{false};
  bool has_limit{false};
  bool changes_cardinality{false};
  bool has_order_by_branch{false};
  bool has_projection_branch{false};
  bool has_aggregation{false};
  bool sort_keys_all_bound{true};
  bool projection_reads_all_bound{true};
};

/// The first reason that applies. Cheap facts are checked before ones needing a walk over the
/// expressions, and the answer does not depend on what else is also true.
constexpr NoDeferReason WhyProjectionCannotDefer(ProjectionFacts const &facts) {
  if (!facts.has_order_by) return NoDeferReason::kNoOrderBy;
  if (!facts.has_limit) return NoDeferReason::kNoLimit;
  if (facts.changes_cardinality) return NoDeferReason::kCardinalityChanges;
  if (facts.has_order_by_branch) return NoDeferReason::kOrderByBranch;
  if (facts.has_projection_branch) return NoDeferReason::kProjectionBranch;
  if (facts.has_aggregation) return NoDeferReason::kAggregation;
  if (!facts.sort_keys_all_bound) return NoDeferReason::kSortKeyNotBound;
  if (!facts.projection_reads_all_bound) return NoDeferReason::kProjectionReadsUnbound;
  return NoDeferReason::kDeferred;
}

}  // namespace memgraph::query::plan
