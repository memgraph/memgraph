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
#include <expected>

namespace memgraph::query::plan {

/// The one thing stopping a return body from leaving its projection until after the sort and the
/// limit.
enum class DeferBlocker : uint8_t {
  /// No sort for the projection to move past.
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

/// Whether the projection can wait until after the sort and the limit, and the first blocker when
/// it cannot.
///
/// A projection emits one row per row it reads and keeps their order, so sorting and limiting
/// underneath it selects the same rows while leaving the projected expressions to run only for
/// the survivors. Waiting is wrong whenever a row reaching the projection could not be rebuilt
/// from what the sort replays.
///
/// Cheap facts are checked before ones needing a walk over the expressions, and the blocker
/// reported does not depend on what else is also true. Naming the blocker rather than answering
/// yes or no is what lets a test name the rule it checks.
constexpr std::expected<void, DeferBlocker> CanDeferProjection(ProjectionFacts const &facts) {
  if (!facts.has_order_by) return std::unexpected{DeferBlocker::kNoOrderBy};
  if (!facts.has_limit) return std::unexpected{DeferBlocker::kNoLimit};
  if (facts.changes_cardinality) return std::unexpected{DeferBlocker::kCardinalityChanges};
  if (facts.has_order_by_branch) return std::unexpected{DeferBlocker::kOrderByBranch};
  if (facts.has_projection_branch) return std::unexpected{DeferBlocker::kProjectionBranch};
  if (facts.has_aggregation) return std::unexpected{DeferBlocker::kAggregation};
  if (!facts.sort_keys_all_bound) return std::unexpected{DeferBlocker::kSortKeyNotBound};
  if (!facts.projection_reads_all_bound) return std::unexpected{DeferBlocker::kProjectionReadsUnbound};
  return {};
}

}  // namespace memgraph::query::plan
