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

#include <algorithm>
#include <vector>

#include <gtest/gtest.h>

#include "query/plan/deferred_projection.hpp"

using memgraph::query::plan::NoDeferReason;
using memgraph::query::plan::ProjectionFacts;
using memgraph::query::plan::WhyProjectionCannotDefer;

namespace {

/// A body that may defer, which each test below spoils in exactly one way.
constexpr ProjectionFacts Deferrable() {
  return ProjectionFacts{
      .has_order_by = true,
      .has_limit = true,
      .changes_cardinality = false,
      .has_order_by_branch = false,
      .has_projection_branch = false,
      .has_aggregation = false,
      .sort_keys_all_bound = true,
      .projection_reads_all_bound = true,
  };
}

TEST(DeferredProjection, DefersWhenNothingStopsIt) {
  EXPECT_EQ(WhyProjectionCannotDefer(Deferrable()), NoDeferReason::kDeferred);
}

TEST(DeferredProjection, EachFactRefusesInItsOwnName) {
  auto without_order_by = Deferrable();
  without_order_by.has_order_by = false;
  EXPECT_EQ(WhyProjectionCannotDefer(without_order_by), NoDeferReason::kNoOrderBy);

  auto without_limit = Deferrable();
  without_limit.has_limit = false;
  EXPECT_EQ(WhyProjectionCannotDefer(without_limit), NoDeferReason::kNoLimit);

  auto cardinality = Deferrable();
  cardinality.changes_cardinality = true;
  EXPECT_EQ(WhyProjectionCannotDefer(cardinality), NoDeferReason::kCardinalityChanges);

  auto order_by_branch = Deferrable();
  order_by_branch.has_order_by_branch = true;
  EXPECT_EQ(WhyProjectionCannotDefer(order_by_branch), NoDeferReason::kOrderByBranch);

  auto projection_branch = Deferrable();
  projection_branch.has_projection_branch = true;
  EXPECT_EQ(WhyProjectionCannotDefer(projection_branch), NoDeferReason::kProjectionBranch);

  auto aggregation = Deferrable();
  aggregation.has_aggregation = true;
  EXPECT_EQ(WhyProjectionCannotDefer(aggregation), NoDeferReason::kAggregation);

  auto unbound_key = Deferrable();
  unbound_key.sort_keys_all_bound = false;
  EXPECT_EQ(WhyProjectionCannotDefer(unbound_key), NoDeferReason::kSortKeyNotBound);

  auto unbound_projection = Deferrable();
  unbound_projection.projection_reads_all_bound = false;
  EXPECT_EQ(WhyProjectionCannotDefer(unbound_projection), NoDeferReason::kProjectionReadsUnbound);
}

// Every field has to be able to refuse on its own. A field whose reason is unreachable because an
// earlier one always covers it would be a rule nothing can be written to exercise, which is how a
// guard ends up shipping with no test behind it.
TEST(DeferredProjection, NoReasonIsUnreachable) {
  constexpr NoDeferReason kEveryReason[] = {
      NoDeferReason::kDeferred,
      NoDeferReason::kNoOrderBy,
      NoDeferReason::kNoLimit,
      NoDeferReason::kCardinalityChanges,
      NoDeferReason::kOrderByBranch,
      NoDeferReason::kProjectionBranch,
      NoDeferReason::kAggregation,
      NoDeferReason::kSortKeyNotBound,
      NoDeferReason::kProjectionReadsUnbound,
  };

  // Spoil one field at a time, collecting the reasons that come back.
  std::vector<NoDeferReason> reached{WhyProjectionCannotDefer(Deferrable())};
  for (auto field : {&ProjectionFacts::changes_cardinality,
                     &ProjectionFacts::has_order_by_branch,
                     &ProjectionFacts::has_projection_branch,
                     &ProjectionFacts::has_aggregation}) {
    auto facts = Deferrable();
    facts.*field = true;
    reached.push_back(WhyProjectionCannotDefer(facts));
  }
  for (auto field : {&ProjectionFacts::has_order_by,
                     &ProjectionFacts::has_limit,
                     &ProjectionFacts::sort_keys_all_bound,
                     &ProjectionFacts::projection_reads_all_bound}) {
    auto facts = Deferrable();
    facts.*field = false;
    reached.push_back(WhyProjectionCannotDefer(facts));
  }

  for (auto const reason : kEveryReason) {
    EXPECT_NE(std::ranges::find(reached, reason), reached.end())
        << "no single fact produces reason " << static_cast<int>(reason)
        << ", so nothing can be written to exercise it";
  }
}

// An earlier reason wins, so a caller reporting one gets the same answer however many apply.
TEST(DeferredProjection, ReportsTheFirstReasonWhenSeveralApply) {
  auto everything_wrong = Deferrable();
  everything_wrong.changes_cardinality = true;
  everything_wrong.has_projection_branch = true;
  everything_wrong.has_aggregation = true;
  everything_wrong.sort_keys_all_bound = false;
  EXPECT_EQ(WhyProjectionCannotDefer(everything_wrong), NoDeferReason::kCardinalityChanges);

  auto no_limit_and_more = Deferrable();
  no_limit_and_more.has_limit = false;
  no_limit_and_more.has_aggregation = true;
  EXPECT_EQ(WhyProjectionCannotDefer(no_limit_and_more), NoDeferReason::kNoLimit);

  auto neither_sort_nor_limit = Deferrable();
  neither_sort_nor_limit.has_order_by = false;
  neither_sort_nor_limit.has_limit = false;
  EXPECT_EQ(WhyProjectionCannotDefer(neither_sort_nor_limit), NoDeferReason::kNoOrderBy);
}

}  // namespace
