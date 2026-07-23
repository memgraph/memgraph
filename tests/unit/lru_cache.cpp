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

#include "utils/lru_cache.hpp"
#include <optional>
#include "gtest/gtest.h"

TEST(LRUCacheTest, BasicTest) {
  memgraph::utils::LRUCache<int, int> cache(2);
  cache.put(1, 1);
  cache.put(2, 2);

  std::optional<int> value;
  value = cache.get(1);
  EXPECT_TRUE(value.has_value());
  EXPECT_EQ(value.value(), 1);

  cache.put(3, 3);

  value = cache.get(2);
  EXPECT_FALSE(value.has_value());

  cache.put(4, 4);

  value = cache.get(1);
  EXPECT_FALSE(value.has_value());

  value = cache.get(3);
  EXPECT_TRUE(value.has_value());
  EXPECT_EQ(value.value(), 3);

  value = cache.get(4);
  EXPECT_TRUE(value.has_value());
  EXPECT_EQ(value.value(), 4);

  EXPECT_EQ(cache.size(), 2);
}

TEST(LRUCacheTest, DuplicatePutTest) {
  memgraph::utils::LRUCache<int, int> cache(2);
  cache.put(1, 1);
  cache.put(2, 2);
  cache.put(1, 10);

  std::optional<int> value;
  value = cache.get(1);
  EXPECT_TRUE(value.has_value());
  EXPECT_EQ(value.value(), 10);

  value = cache.get(2);
  EXPECT_TRUE(value.has_value());
  EXPECT_EQ(value.value(), 2);
}

TEST(LRUCacheTest, ResizeTest) {
  memgraph::utils::LRUCache<int, int> cache(2);
  cache.put(1, 1);
  cache.put(2, 2);
  cache.put(3, 3);

  std::optional<int> value;
  value = cache.get(1);
  EXPECT_FALSE(value.has_value());

  value = cache.get(2);
  EXPECT_TRUE(value.has_value());
  EXPECT_EQ(value.value(), 2);

  value = cache.get(3);
  EXPECT_TRUE(value.has_value());
  EXPECT_EQ(value.value(), 3);
}

TEST(LRUCacheTest, EmptyCacheTest) {
  memgraph::utils::LRUCache<int, int> cache(2);

  std::optional<int> value;
  value = cache.get(1);
  EXPECT_FALSE(value.has_value());

  cache.put(1, 1);
  value = cache.get(1);
  EXPECT_TRUE(value.has_value());
  EXPECT_EQ(value.value(), 1);
}

TEST(LRUCacheTest, LargeCacheTest) {
  const int CACHE_SIZE = 10'000;
  memgraph::utils::LRUCache<int, int> cache(CACHE_SIZE);

  std::optional<int> value;
  for (int i = 0; i < CACHE_SIZE; i++) {
    value = cache.get(i);
    EXPECT_FALSE(value.has_value());
    cache.put(i, i);
  }

  for (int i = 0; i < CACHE_SIZE; i++) {
    value = cache.get(i);
    EXPECT_TRUE(value.has_value());
    EXPECT_EQ(value.value(), i);
  }
}

TEST(LRUCacheTest, InvalidateTest) {
  memgraph::utils::LRUCache<int, int> cache(3);
  cache.put(1, 100);
  cache.put(2, 200);
  cache.put(3, 300);

  // Ensure all elements are present
  EXPECT_TRUE(cache.get(1).has_value());
  EXPECT_TRUE(cache.get(2).has_value());
  EXPECT_TRUE(cache.get(3).has_value());

  // Invalidate one key
  cache.invalidate(2);

  // Key 2 should be removed
  std::optional<int> value = cache.get(2);
  EXPECT_FALSE(value.has_value());

  // Other keys should still be present
  EXPECT_TRUE(cache.get(1).has_value());
  EXPECT_EQ(cache.get(1).value(), 100);

  EXPECT_TRUE(cache.get(3).has_value());
  EXPECT_EQ(cache.get(3).value(), 300);

  // Cache size should be 2 now
  EXPECT_EQ(cache.size(), 2);

  // Invalidate a non-existent key (should not crash or change state)
  cache.invalidate(42);
  EXPECT_EQ(cache.size(), 2);
}

TEST(LRUCacheTest, PeekReturnsPresentAndAbsent) {
  memgraph::utils::LRUCache<int, int> cache(2);
  cache.put(1, 100);
  cache.put(2, 200);

  auto const &const_cache = cache;

  std::optional<int> value = const_cache.peek(1);
  EXPECT_TRUE(value.has_value());
  EXPECT_EQ(value.value(), 100);

  value = const_cache.peek(2);
  EXPECT_TRUE(value.has_value());
  EXPECT_EQ(value.value(), 200);

  // Absent key
  value = const_cache.peek(42);
  EXPECT_FALSE(value.has_value());
}

TEST(LRUCacheTest, PeekDoesNotBumpRecencyUnlikeGet) {
  // With size 2, inserting 3 evicts the LRU of {1, 2}. peek() must not count as a use, so peeking
  // key 1 repeatedly still lets it be evicted (get() would have kept it alive).
  memgraph::utils::LRUCache<int, int> peek_cache(2);
  peek_cache.put(1, 100);
  peek_cache.put(2, 200);

  auto const &const_peek_cache = peek_cache;
  // Repeatedly peek at key 1 - must not affect recency.
  for (int i = 0; i < 5; ++i) {
    auto const value = const_peek_cache.peek(1);
    EXPECT_TRUE(value.has_value());
  }

  // Without a recency bump, 1 (put before 2) is LRU and evicted when 3 is inserted.
  peek_cache.put(3, 300);
  EXPECT_FALSE(const_peek_cache.peek(1).has_value());
  EXPECT_TRUE(const_peek_cache.peek(2).has_value());
  EXPECT_TRUE(const_peek_cache.peek(3).has_value());

  // Control: get() on key 1 bumps its recency, so 2 (never re-touched) is evicted instead.
  memgraph::utils::LRUCache<int, int> get_cache(2);
  get_cache.put(1, 100);
  get_cache.put(2, 200);
  for (int i = 0; i < 5; ++i) {
    auto const value = get_cache.get(1);
    EXPECT_TRUE(value.has_value());
  }
  get_cache.put(3, 300);
  EXPECT_TRUE(get_cache.get(1).has_value());
  EXPECT_FALSE(get_cache.get(2).has_value());
  EXPECT_TRUE(get_cache.get(3).has_value());
}
