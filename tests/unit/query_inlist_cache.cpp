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

#include <gtest/gtest.h>

#include "helpers/query_test_utils.hpp"

using namespace memgraph::query;
using namespace memgraph::query::test;

class InListCacheTest : public testing::Test {
 protected:
  ScopedLogLevel scoped_log_level;  // Disable logging first
  StorageComponent storage_component;
  QueryBuildingComponent builder;
  QueryEvaluationComponent evaluator{builder, storage_component};
};

// Test: x IN range(1,100) creates and uses cache
TEST_F(InListCacheTest, RangeInListCaching) {
  // Build: x IN range(1, 100)
  auto [x_id, x_symbol] = builder.CreateIdentifier("x");
  auto *range_func = builder.CreateRangeFunction(1, 100);
  auto *in_list = builder.CreateInListOperator(x_id, range_func);

  // Prepare caching
  evaluator.PrepareInListCaching();

  // Verify it's tracked for caching
  evaluator.ExpectTrackedForCaching(in_list);

  // Evaluate with x = 50
  auto result = evaluator.Eval(in_list, {{x_symbol, 50_tv}});
  EXPECT_TRUE(result.IsBool());
  EXPECT_TRUE(result.ValueBool()) << "50 should be in range(1, 100)";

  // Verify cache was populated with correct values
  evaluator.ExpectCacheContains(in_list, {1_tv, 50_tv, 100_tv}, {0_tv, 101_tv});
}

// Test: Identifier-based list also supports caching
TEST_F(InListCacheTest, IdentifierListCaching) {
  // Build: x IN mylist (where mylist = [1..10])
  auto [mylist_id, mylist_symbol] = builder.CreateIdentifier("mylist");
  auto [x_id, x_symbol] = builder.CreateIdentifier("x");
  auto *in_list = builder.CreateInListOperator(x_id, mylist_id);

  // Prepare caching
  evaluator.PrepareInListCaching();

  // Verify it's tracked for caching
  evaluator.ExpectTrackedForCaching(in_list);

  // Evaluate with mylist = [1..10] and x = 5
  auto result = evaluator.Eval(in_list, {{mylist_symbol, CreateIntList(1, 10)}, {x_symbol, 5_tv}});
  EXPECT_TRUE(result.IsBool());
  EXPECT_TRUE(result.ValueBool()) << "5 should be in the list";

  // Verify cache was populated with correct values
  evaluator.ExpectCacheContains(in_list, {1_tv, 5_tv, 10_tv}, {11_tv});
}

// Test: Cache invalidation
TEST_F(InListCacheTest, CacheInvalidation) {
  // Build: x IN mylist (where mylist = [1..5])
  auto [mylist_id, mylist_symbol] = builder.CreateIdentifier("mylist");
  auto [x_id, x_symbol] = builder.CreateIdentifier("x");
  auto *in_list = builder.CreateInListOperator(x_id, mylist_id);

  // Prepare caching
  evaluator.PrepareInListCaching();

  // Verify it's tracked for caching
  evaluator.ExpectTrackedForCaching(in_list);

  // Evaluate with mylist = [1..5] and x = 3
  auto result = evaluator.Eval(in_list, {{mylist_symbol, CreateIntList(1, 5)}, {x_symbol, 3_tv}});
  EXPECT_TRUE(result.ValueBool());
  evaluator.ExpectCachePopulated(in_list);

  // Invalidate the cache
  evaluator.frame_change_collector.ResetInListCache(mylist_symbol);

  // Verify cache was cleared
  evaluator.ExpectCacheNotPopulated(in_list, "Cache should be cleared after invalidation");
  // But still tracked
  evaluator.ExpectTrackedForCaching(in_list);
}

// Test: Non-list expressions should throw QueryRuntimeException
TEST_F(InListCacheTest, NonListExpressionThrows) {
  // Build: x IN 5 (integer literal) -> should throw
  auto [x_id, x_symbol] = builder.CreateIdentifier("x");
  auto *int_literal = builder.CreateLiteral(5);
  auto *in_list = builder.CreateInListOperator(x_id, int_literal);

  // Should throw QueryRuntimeException when evaluated with x = 5
  EXPECT_THROW(evaluator.Eval(in_list, {{x_symbol, 5_tv}}), QueryRuntimeException);
}

// Test: Non-pure expressions should not be cached
TEST_F(InListCacheTest, NonPureExpressionsNotCached) {
  // Build: x IN [rand()] - non-deterministic, should NOT be cached
  auto [x_id, x_symbol] = builder.CreateIdentifier("x");
  auto *rand_func = builder.CreateFunction("rand");
  auto *list_literal = builder.CreateListLiteral(rand_func);
  auto *in_list = builder.CreateInListOperator(x_id, list_literal);

  // Prepare caching - should detect this is not cacheable
  evaluator.PrepareInListCaching();

  // Verify it was NOT tracked (because rand() is non-pure)
  evaluator.ExpectTrackedForCaching(in_list, false);

  // Evaluate with x = 0.5 - should work but not create cache
  const auto result = evaluator.Eval(in_list, {{x_symbol, 0.5_tv}});
  EXPECT_TRUE(result.IsBool());

  // Verify cache was NOT populated
  evaluator.ExpectCacheNotPopulated(in_list, "Non-pure expressions should not be cached");
}
