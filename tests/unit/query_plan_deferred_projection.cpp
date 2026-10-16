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

using memgraph::query::plan::CanDeferProjection;
using memgraph::query::plan::DeferBlocker;
using memgraph::query::plan::ProjectionFacts;

namespace {

/// A body that can defer, which each test below spoils in exactly one way.
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

/// The blocker a body reports, for a test that expects one.
constexpr DeferBlocker BlockerFor(ProjectionFacts const &facts) { return CanDeferProjection(facts).error(); }

TEST(DeferredProjection, DefersWhenNothingBlocksIt) { EXPECT_TRUE(CanDeferProjection(Deferrable())); }

TEST(DeferredProjection, EachFactBlocksInItsOwnName) {
  auto without_order_by = Deferrable();
  without_order_by.has_order_by = false;
  EXPECT_EQ(BlockerFor(without_order_by), DeferBlocker::kNoOrderBy);

  auto without_limit = Deferrable();
  without_limit.has_limit = false;
  EXPECT_EQ(BlockerFor(without_limit), DeferBlocker::kNoLimit);

  auto cardinality = Deferrable();
  cardinality.changes_cardinality = true;
  EXPECT_EQ(BlockerFor(cardinality), DeferBlocker::kCardinalityChanges);

  auto order_by_branch = Deferrable();
  order_by_branch.has_order_by_branch = true;
  EXPECT_EQ(BlockerFor(order_by_branch), DeferBlocker::kOrderByBranch);

  auto projection_branch = Deferrable();
  projection_branch.has_projection_branch = true;
  EXPECT_EQ(BlockerFor(projection_branch), DeferBlocker::kProjectionBranch);

  auto aggregation = Deferrable();
  aggregation.has_aggregation = true;
  EXPECT_EQ(BlockerFor(aggregation), DeferBlocker::kAggregation);

  auto unbound_key = Deferrable();
  unbound_key.sort_keys_all_bound = false;
  EXPECT_EQ(BlockerFor(unbound_key), DeferBlocker::kSortKeyNotBound);

  auto unbound_projection = Deferrable();
  unbound_projection.projection_reads_all_bound = false;
  EXPECT_EQ(BlockerFor(unbound_projection), DeferBlocker::kProjectionReadsUnbound);
}

// Every field has to be able to block on its own. A blocker that is unreachable because an earlier
// one always covers it is a rule nothing can be written to exercise, which is how a guard ends up
// shipping with no test behind it.
TEST(DeferredProjection, NoBlockerIsUnreachable) {
  constexpr DeferBlocker kEveryBlocker[] = {
      DeferBlocker::kNoOrderBy,
      DeferBlocker::kNoLimit,
      DeferBlocker::kCardinalityChanges,
      DeferBlocker::kOrderByBranch,
      DeferBlocker::kProjectionBranch,
      DeferBlocker::kAggregation,
      DeferBlocker::kSortKeyNotBound,
      DeferBlocker::kProjectionReadsUnbound,
  };

  // Spoil one field at a time, collecting the blockers that come back.
  std::vector<DeferBlocker> reached;
  for (auto field : {&ProjectionFacts::changes_cardinality,
                     &ProjectionFacts::has_order_by_branch,
                     &ProjectionFacts::has_projection_branch,
                     &ProjectionFacts::has_aggregation}) {
    auto facts = Deferrable();
    facts.*field = true;
    reached.push_back(BlockerFor(facts));
  }
  for (auto field : {&ProjectionFacts::has_order_by,
                     &ProjectionFacts::has_limit,
                     &ProjectionFacts::sort_keys_all_bound,
                     &ProjectionFacts::projection_reads_all_bound}) {
    auto facts = Deferrable();
    facts.*field = false;
    reached.push_back(BlockerFor(facts));
  }

  for (auto const blocker : kEveryBlocker) {
    EXPECT_NE(std::ranges::find(reached, blocker), reached.end())
        << "no single fact produces blocker " << static_cast<int>(blocker)
        << ", so nothing can be written to exercise it";
  }
}

// An earlier blocker wins, so a caller reporting one gets the same answer however many apply.
TEST(DeferredProjection, ReportsTheFirstBlockerWhenSeveralApply) {
  auto everything_wrong = Deferrable();
  everything_wrong.changes_cardinality = true;
  everything_wrong.has_projection_branch = true;
  everything_wrong.has_aggregation = true;
  everything_wrong.sort_keys_all_bound = false;
  EXPECT_EQ(BlockerFor(everything_wrong), DeferBlocker::kCardinalityChanges);

  auto no_limit_and_more = Deferrable();
  no_limit_and_more.has_limit = false;
  no_limit_and_more.has_aggregation = true;
  EXPECT_EQ(BlockerFor(no_limit_and_more), DeferBlocker::kNoLimit);

  auto neither_sort_nor_limit = Deferrable();
  neither_sort_nor_limit.has_order_by = false;
  neither_sort_nor_limit.has_limit = false;
  EXPECT_EQ(BlockerFor(neither_sort_nor_limit), DeferBlocker::kNoOrderBy);
}

}  // namespace
