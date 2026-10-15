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

/// Whether a return body may leave its projection until after the sort and the limit, and when it
/// may not, which single thing stops it.
///
/// A projection emits one row for every row it reads and keeps their order, so sorting and
/// limiting underneath it selects the same rows in the same order while leaving the projected
/// expressions to run only for the survivors. That is worth doing when a projected expression is
/// costly, and it is wrong whenever a row reaching the projection could not be rebuilt from what
/// the sort replays.
///
/// The reason is carried rather than reduced to a yes or no so that a test can name the rule it
/// is checking. A test that asserts only the resulting plan passes whichever rule refused, which
/// has twice let a rule ship with no test behind it.
enum class NoDeferReason : uint8_t {
  /// The projection may be deferred.
  kDeferred,
  /// No limit, so every row is projected either way and deferring saves nothing.
  kNoLimit,
  /// DISTINCT or a trailing WHERE changes how many rows reach the limit.
  kCardinalityChanges,
  /// A branch under the ORDER BY reads what the projection writes.
  kOrderByBranch,
  /// A branch under the projection writes its answer per row, and the sort cannot replay it.
  kProjectionBranch,
  /// An aggregate writes one row per group, and the sort cannot replay it.
  kAggregation,
  /// A sort key names something not yet bound when the clause began, such as a projected alias.
  kSortKeyNotBound,
  /// A projected expression reads something not yet bound when the clause began.
  kProjectionReadsUnbound,
};

/// What a return body looks like to the decision above. Gathered by the planner, which owns the
/// shapes these are read off; kept apart from the decision so the decision stays a total function
/// over its inputs and can be checked without planning a query.
struct ProjectionFacts {
  bool has_order_by{false};
  bool has_limit{false};
  /// DISTINCT or a trailing WHERE: either changes how many rows reach the limit.
  bool changes_cardinality{false};
  bool has_order_by_branch{false};
  bool has_projection_branch{false};
  bool has_aggregation{false};
  bool sort_keys_all_bound{true};
  bool projection_reads_all_bound{true};
};

/// The first reason that applies, in the order the fields above are listed. The order is not
/// arbitrary: a cheaper fact is checked before one that needs a walk over the expressions, and a
/// caller reporting the reason gets the same answer whatever else is also true.
constexpr NoDeferReason WhyProjectionCannotDefer(ProjectionFacts const &facts) {
  if (!facts.has_order_by || !facts.has_limit) return NoDeferReason::kNoLimit;
  if (facts.changes_cardinality) return NoDeferReason::kCardinalityChanges;
  if (facts.has_order_by_branch) return NoDeferReason::kOrderByBranch;
  if (facts.has_projection_branch) return NoDeferReason::kProjectionBranch;
  if (facts.has_aggregation) return NoDeferReason::kAggregation;
  if (!facts.sort_keys_all_bound) return NoDeferReason::kSortKeyNotBound;
  if (!facts.projection_reads_all_bound) return NoDeferReason::kProjectionReadsUnbound;
  return NoDeferReason::kDeferred;
}

}  // namespace memgraph::query::plan
