// Copyright 2024 Memgraph Ltd.
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
  const int CACHE_SIZE = 10000;
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
