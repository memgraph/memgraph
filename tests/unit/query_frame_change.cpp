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

#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include "query/frame_change.hpp"
#include "query/frontend/semantic/symbol.hpp"
#include "query/typed_value.hpp"
#include "utils/frame_change_id.hpp"
#include "utils/memory.hpp"

using namespace memgraph::query;
using namespace memgraph::utils;

class FrameChangeCollectorTest : public ::testing::Test {
 protected:
  // Use symbol positions to create FrameChangeId keys
  static FrameChangeId MakeKey(Symbol::Position_t pos) { return FrameChangeId{pos}; }
};

TEST_F(FrameChangeCollectorTest, MergeFromInlistCache) {
  FrameChangeCollector main_collector{NewDeleteResource()};
  FrameChangeCollector branch_collector{NewDeleteResource()};

  // Use symbol positions as keys
  auto key1 = MakeKey(1);
  auto key2 = MakeKey(2);
  auto key3 = MakeKey(3);

  auto &main_cache1 = main_collector.AddInListKey(key1);
  main_cache1.SetValue(TypedValue(std::vector<TypedValue>{TypedValue(1), TypedValue(2)}));

  auto &branch_cache2 = branch_collector.AddInListKey(key2);
  branch_cache2.SetValue(TypedValue(std::vector<TypedValue>{TypedValue(3), TypedValue(4)}));

  // Add a common key to both with different values
  auto &main_cache3 = main_collector.AddInListKey(key3);
  main_cache3.SetValue(TypedValue(std::vector<TypedValue>{TypedValue(5)}));

  auto &branch_cache3 = branch_collector.AddInListKey(key3);
  branch_cache3.SetValue(TypedValue(std::vector<TypedValue>{TypedValue(6)}));

  // Merge branch into main
  main_collector.MergeFrom(std::move(branch_collector));

  // Check that main has all keys
  EXPECT_TRUE(main_collector.IsInlistKeyTracked(key1));
  EXPECT_TRUE(main_collector.IsInlistKeyTracked(key2));
  EXPECT_TRUE(main_collector.IsInlistKeyTracked(key3));

  // Check key1 has original values
  auto cached1 = main_collector.TryGetInlistCachedValue(key1);
  ASSERT_TRUE(cached1.has_value());
  EXPECT_TRUE(cached1->get().Contains(TypedValue(1)));
  EXPECT_TRUE(cached1->get().Contains(TypedValue(2)));

  // Check key2 was merged from branch
  auto cached2 = main_collector.TryGetInlistCachedValue(key2);
  ASSERT_TRUE(cached2.has_value());
  EXPECT_TRUE(cached2->get().Contains(TypedValue(3)));
  EXPECT_TRUE(cached2->get().Contains(TypedValue(4)));

  // Check key3 has union of both sets
  auto cached3 = main_collector.TryGetInlistCachedValue(key3);
  ASSERT_TRUE(cached3.has_value());
  EXPECT_TRUE(cached3->get().Contains(TypedValue(5)));
  EXPECT_TRUE(cached3->get().Contains(TypedValue(6)));
}

TEST_F(FrameChangeCollectorTest, MergeFromRegexCache) {
  FrameChangeCollector main_collector{NewDeleteResource()};
  FrameChangeCollector branch_collector{NewDeleteResource()};

  auto key1 = MakeKey(1);
  auto key2 = MakeKey(2);
  auto key3 = MakeKey(3);

  // Main has key1 with a regex
  main_collector.AddRegexKey(key1);
  main_collector.AddRegexKey(key1, std::regex("pattern1"));

  // Branch has key2 with a regex
  branch_collector.AddRegexKey(key2);
  branch_collector.AddRegexKey(key2, std::regex("pattern2"));

  // Both have key3, but only branch has a populated regex
  main_collector.AddRegexKey(key3);  // tracked but not populated
  branch_collector.AddRegexKey(key3);
  branch_collector.AddRegexKey(key3, std::regex("pattern3"));

  // Merge branch into main
  main_collector.MergeFrom(std::move(branch_collector));

  // Check that main has all keys
  EXPECT_TRUE(main_collector.IsRegexKeyTracked(key1));
  EXPECT_TRUE(main_collector.IsRegexKeyTracked(key2));
  EXPECT_TRUE(main_collector.IsRegexKeyTracked(key3));

  // Check key1 still has its original regex
  EXPECT_TRUE(main_collector.TryGetRegexCachedValue(key1).has_value());

  // Check key2 was merged from branch
  EXPECT_TRUE(main_collector.TryGetRegexCachedValue(key2).has_value());

  // Check key3 now has branch's regex (since main didn't have a value)
  EXPECT_TRUE(main_collector.TryGetRegexCachedValue(key3).has_value());
}

TEST_F(FrameChangeCollectorTest, MergeFromInvalidators) {
  FrameChangeCollector main_collector{NewDeleteResource()};
  FrameChangeCollector branch_collector{NewDeleteResource()};

  auto key1 = MakeKey(1);
  auto key2 = MakeKey(2);

  Symbol::Position_t symbol_pos1 = 100;
  Symbol::Position_t symbol_pos2 = 200;

  // Set up main with one invalidator
  main_collector.AddInListKey(key1);
  main_collector.AddInvalidator(key1, symbol_pos1);

  // Set up branch with another invalidator for a different symbol
  branch_collector.AddInListKey(key2);
  branch_collector.AddInvalidator(key2, symbol_pos2);

  // Also add invalidator for same symbol but different key in branch
  branch_collector.AddInvalidator(key2, symbol_pos1);

  // Populate the cache values
  auto &main_cache1 = main_collector.GetInlistCachedValue(key1);
  main_cache1.SetValue(TypedValue(std::vector<TypedValue>{TypedValue(1)}));

  auto &branch_cache2 = branch_collector.GetInlistCachedValue(key2);
  branch_cache2.SetValue(TypedValue(std::vector<TypedValue>{TypedValue(2)}));

  // Merge branch into main
  main_collector.MergeFrom(std::move(branch_collector));

  // Create a symbol with position symbol_pos1 to test invalidation
  Symbol sym1{"test_sym1", static_cast<int>(symbol_pos1), false};

  // Before reset, both caches should have values
  EXPECT_TRUE(main_collector.TryGetInlistCachedValue(key1).has_value());
  EXPECT_TRUE(main_collector.TryGetInlistCachedValue(key2).has_value());

  // Reset cache for symbol_pos1 - should invalidate both key1 (from main) and key2 (from branch)
  main_collector.ResetInListCache(sym1);

  // After reset, both key1 and key2 should be invalidated (key1 was originally tied to symbol_pos1,
  // key2 was added to symbol_pos1 via merge from branch)
  EXPECT_FALSE(main_collector.TryGetInlistCachedValue(key1).has_value());
  EXPECT_FALSE(main_collector.TryGetInlistCachedValue(key2).has_value());
}

TEST_F(FrameChangeCollectorTest, MergeFromEmptyCollectors) {
  FrameChangeCollector main_collector{NewDeleteResource()};
  FrameChangeCollector branch_collector{NewDeleteResource()};

  // Both are empty, merge should work without issues
  main_collector.MergeFrom(std::move(branch_collector));

  EXPECT_FALSE(main_collector.AnyCaches());
}

TEST_F(FrameChangeCollectorTest, MergeFromPreservesMainValues) {
  FrameChangeCollector main_collector{NewDeleteResource()};
  FrameChangeCollector branch_collector{NewDeleteResource()};

  auto key1 = MakeKey(1);

  // Main has key1 with regex populated
  main_collector.AddRegexKey(key1);
  main_collector.AddRegexKey(key1, std::regex("main_pattern"));

  // Branch also has key1 with regex populated
  branch_collector.AddRegexKey(key1);
  branch_collector.AddRegexKey(key1, std::regex("branch_pattern"));

  // Merge branch into main
  main_collector.MergeFrom(std::move(branch_collector));

  // Main should keep its original regex (first one wins if both have values)
  auto cached = main_collector.TryGetRegexCachedValue(key1);
  ASSERT_TRUE(cached.has_value());
  // Main pattern should be preserved (we can verify it matches expected string)
  EXPECT_TRUE(std::regex_match("main_pattern", cached->get()));
}

TEST_F(FrameChangeCollectorTest, AnyCachesAfterMerge) {
  FrameChangeCollector main_collector{NewDeleteResource()};
  FrameChangeCollector branch_collector{NewDeleteResource()};

  EXPECT_FALSE(main_collector.AnyCaches());
  EXPECT_FALSE(main_collector.AnyInListCaches());
  EXPECT_FALSE(main_collector.AnyRegexCaches());

  // Add to branch only
  auto key1 = MakeKey(1);
  branch_collector.AddInListKey(key1);

  // Merge
  main_collector.MergeFrom(std::move(branch_collector));

  // Main should now have caches
  EXPECT_TRUE(main_collector.AnyCaches());
  EXPECT_TRUE(main_collector.AnyInListCaches());
  EXPECT_FALSE(main_collector.AnyRegexCaches());
}
