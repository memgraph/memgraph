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

class CacheTest : public testing::Test {
 protected:
  ScopedLogLevel scoped_log_level;  // Disable logging first
  StorageComponent storage_component;
  QueryBuildingComponent builder;
  QueryEvaluationComponent evaluator{builder, storage_component};
};

// ============================================================================
// InList Cache Tests
// ============================================================================

TEST_F(CacheTest, RangeInListCaching) {
  // Build: x IN range(1, 100)
  auto [x_id, x_symbol] = builder.CreateIdentifier("x");
  auto *range_func = builder.CreateRangeFunction(1, 100);
  auto *in_list = builder.CreateInListOperator(x_id, range_func);

  evaluator.PrepareCaching();
  evaluator.ExpectTrackedForCaching(in_list);

  // Evaluate with x = 50
  auto result = evaluator.Eval(in_list, {{x_symbol, 50_tv}});
  EXPECT_TRUE(result.IsBool());
  EXPECT_TRUE(result.ValueBool()) << "50 should be in range(1, 100)";

  evaluator.ExpectInListCacheContains(in_list, {1_tv, 50_tv, 100_tv}, {0_tv, 101_tv});
}

TEST_F(CacheTest, IdentifierListCaching) {
  // Build: x IN mylist (where mylist = [1..10])
  auto [mylist_id, mylist_symbol] = builder.CreateIdentifier("mylist");
  auto [x_id, x_symbol] = builder.CreateIdentifier("x");
  auto *in_list = builder.CreateInListOperator(x_id, mylist_id);

  evaluator.PrepareCaching();
  evaluator.ExpectTrackedForCaching(in_list);

  // Evaluate with mylist = [1..10] and x = 5
  auto result = evaluator.Eval(in_list, {{mylist_symbol, CreateIntList(1, 10)}, {x_symbol, 5_tv}});
  EXPECT_TRUE(result.IsBool());
  EXPECT_TRUE(result.ValueBool()) << "5 should be in the list";

  // Verify cache was populated with correct values
  evaluator.ExpectInListCacheContains(in_list, {1_tv, 5_tv, 10_tv}, {11_tv});
}

TEST_F(CacheTest, InListCacheInvalidation) {
  // Build: x IN mylist (where mylist = [1..5])
  auto [mylist_id, mylist_symbol] = builder.CreateIdentifier("mylist");
  auto [x_id, x_symbol] = builder.CreateIdentifier("x");
  auto *in_list = builder.CreateInListOperator(x_id, mylist_id);

  evaluator.PrepareCaching();
  evaluator.ExpectTrackedForCaching(in_list);

  // Evaluate with mylist = [1..5] and x = 3
  auto result = evaluator.Eval(in_list, {{mylist_symbol, CreateIntList(1, 5)}, {x_symbol, 3_tv}});
  EXPECT_TRUE(result.ValueBool());
  evaluator.ExpectInListCachePopulated(in_list);

  evaluator.frame_change_collector.ResetInListCache(mylist_symbol);

  evaluator.ExpectInListCacheNotPopulated(in_list, "Cache should be cleared after invalidation");
  evaluator.ExpectTrackedForCaching(in_list);
}

TEST_F(CacheTest, NonListExpressionThrows) {
  // Build: x IN 5 (integer literal) -> should throw
  auto [x_id, x_symbol] = builder.CreateIdentifier("x");
  auto *int_literal = builder.CreateLiteral(5);
  auto *in_list = builder.CreateInListOperator(x_id, int_literal);

  // Should throw QueryRuntimeException when evaluated with x = 5
  EXPECT_THROW(evaluator.Eval(in_list, {{x_symbol, 5_tv}}), QueryRuntimeException);
}

TEST_F(CacheTest, NonPureInListExpressionsNotCached) {
  // Build: x IN [rand()] - non-deterministic, should NOT be cached
  auto [x_id, x_symbol] = builder.CreateIdentifier("x");
  auto *rand_func = builder.CreateFunction("rand");
  auto *list_literal = builder.CreateListLiteral(rand_func);
  auto *in_list = builder.CreateInListOperator(x_id, list_literal);

  evaluator.PrepareCaching();
  evaluator.ExpectTrackedForCaching(in_list, false);

  // Evaluate with x = 0.5 - should work but not create cache
  const auto result = evaluator.Eval(in_list, {{x_symbol, 0.5_tv}});
  EXPECT_TRUE(result.IsBool());

  // Verify cache was NOT populated
  evaluator.ExpectInListCacheNotPopulated(in_list, "Non-pure expressions should not be cached");
}

// ============================================================================
// Regex Cache Tests
// ============================================================================

TEST_F(CacheTest, LiteralRegexCaching) {
  // Build: "text" =~ ".*ext"
  auto *string_literal = builder.CreateLiteral("text");
  auto *regex_literal = builder.CreateLiteral(".*ext");
  auto *regex_match = builder.CreateRegexMatch(string_literal, regex_literal);

  evaluator.PrepareCaching();
  evaluator.ExpectTrackedForCaching(regex_match);

  auto result = evaluator.Eval(regex_match);
  EXPECT_TRUE(result.IsBool());
  EXPECT_TRUE(result.ValueBool());

  evaluator.ExpectCachePopulated(regex_match);

  // Evaluate again with different string - cache should be reused
  auto *string_literal2 = builder.CreateLiteral("next");
  auto *regex_match2 = builder.CreateRegexMatch(string_literal2, regex_literal);
  auto result2 = evaluator.Eval(regex_match2);
  EXPECT_TRUE(result2.IsBool());
  EXPECT_TRUE(result2.ValueBool());
  evaluator.ExpectCachePopulated(regex_match2);
}

TEST_F(CacheTest, IdentifierRegexCaching) {
  // Build: text_var =~ regex_var (where regex_var = ".*ext")
  auto [text_id, text_symbol] = builder.CreateIdentifier("text_var");
  auto [regex_id, regex_symbol] = builder.CreateIdentifier("regex_var");
  auto *regex_match = builder.CreateRegexMatch(text_id, regex_id);

  evaluator.PrepareCaching();
  evaluator.ExpectTrackedForCaching(regex_match);

  auto result = evaluator.Eval(regex_match, {{text_symbol, TypedValue("text")}, {regex_symbol, TypedValue(".*ext")}});
  EXPECT_TRUE(result.IsBool());
  EXPECT_TRUE(result.ValueBool());

  evaluator.ExpectCachePopulated(regex_match);
}

TEST_F(CacheTest, RegexCacheInvalidation) {
  // Build: text_var =~ regex_var
  auto [text_id, text_symbol] = builder.CreateIdentifier("text_var");
  auto [regex_id, regex_symbol] = builder.CreateIdentifier("regex_var");
  auto *regex_match = builder.CreateRegexMatch(text_id, regex_id);

  evaluator.PrepareCaching();
  evaluator.ExpectTrackedForCaching(regex_match);

  // Evaluate with text_var = "text" and regex_var = ".*ext"
  auto result = evaluator.Eval(regex_match, {{text_symbol, TypedValue("text")}, {regex_symbol, TypedValue(".*ext")}});
  EXPECT_TRUE(result.ValueBool());
  evaluator.ExpectCachePopulated(regex_match);

  evaluator.frame_change_collector.ResetCache(regex_symbol);

  evaluator.ExpectCacheNotPopulated(regex_match, "Regex cache should be cleared after invalidation");
  evaluator.ExpectTrackedForCaching(regex_match);

  auto result2 = evaluator.Eval(regex_match, {{text_symbol, TypedValue("test")}, {regex_symbol, TypedValue(".*est")}});
  EXPECT_TRUE(result2.ValueBool());
  evaluator.ExpectCachePopulated(regex_match);
}

TEST_F(CacheTest, SharedRegexCache) {
  // Build: text1 =~ regex_var AND text2 =~ regex_var
  auto [text1_id, text1_symbol] = builder.CreateIdentifier("text1");
  auto [text2_id, text2_symbol] = builder.CreateIdentifier("text2");
  auto [regex_id, regex_symbol] = builder.CreateIdentifier("regex_var");
  auto *regex_match1 = builder.CreateRegexMatch(text1_id, regex_id);
  auto *regex_match2 = builder.CreateRegexMatch(text2_id, regex_id);

  evaluator.PrepareCaching();

  evaluator.ExpectTrackedForCaching(regex_match1);
  evaluator.ExpectTrackedForCaching(regex_match2);

  auto result1 =
      evaluator.Eval(regex_match1, {{text1_symbol, TypedValue("text")}, {regex_symbol, TypedValue(".*ext")}});
  EXPECT_TRUE(result1.ValueBool());
  evaluator.ExpectCachePopulated(regex_match1);

  auto result2 =
      evaluator.Eval(regex_match2, {{text2_symbol, TypedValue("next")}, {regex_symbol, TypedValue(".*ext")}});
  EXPECT_TRUE(result2.ValueBool());
  evaluator.ExpectCachePopulated(regex_match2);

  evaluator.frame_change_collector.ResetCache(regex_symbol);
  evaluator.ExpectCacheNotPopulated(regex_match1, "First regex cache should be cleared");
  evaluator.ExpectCacheNotPopulated(regex_match2, "Second regex cache should be cleared");
}

TEST_F(CacheTest, NonPureRegexExpressionsNotCached) {
  // build: "text" =~ rand() - non-deterministic, should NOT be cached
  // this won't actually work because rand() doesn't return a string, but its important that it's not tracked for
  // caching
  auto *string_literal = builder.CreateLiteral("text");
  auto *rand_func = builder.CreateFunction("rand");
  auto *regex_match = builder.CreateRegexMatch(string_literal, rand_func);

  evaluator.PrepareCaching();
  evaluator.ExpectTrackedForCaching(regex_match, false);
}

TEST_F(CacheTest, InvalidRegexThrows) {
  // Build: "text" =~ "*ext" (invalid regex)
  auto *string_literal = builder.CreateLiteral("text");
  auto *invalid_regex = builder.CreateLiteral("*ext");
  auto *regex_match = builder.CreateRegexMatch(string_literal, invalid_regex);

  // Should throw QueryRuntimeException when evaluated
  EXPECT_THROW(evaluator.Eval(regex_match), QueryRuntimeException);
}

TEST_F(CacheTest, NonStringRegexThrows) {
  // Build: "text" =~ 42 (integer literal) -> should throw
  auto *string_literal = builder.CreateLiteral("text");
  auto *int_literal = builder.CreateLiteral(42);
  auto *regex_match = builder.CreateRegexMatch(string_literal, int_literal);

  // Should throw QueryRuntimeException when evaluated
  EXPECT_THROW(evaluator.Eval(regex_match), QueryRuntimeException);
}
