// Copyright 2023 Memgraph Ltd.
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
#include "gtest/gtest.h"

TEST(LRUCacheTest, BasicTest) {
  memgraph::utils::LRUCache<int, int> cache(2);
  cache.put(1, 1);
  cache.put(2, 2);

  int value;
  EXPECT_TRUE(cache.get(1, value));
  EXPECT_EQ(value, 1);

  cache.put(3, 3);

  EXPECT_FALSE(cache.get(2, value));

  cache.put(4, 4);

  EXPECT_FALSE(cache.get(1, value));

  EXPECT_TRUE(cache.get(3, value));
  EXPECT_EQ(value, 3);

  EXPECT_TRUE(cache.get(4, value));
  EXPECT_EQ(value, 4);

  EXPECT_EQ(cache.size(), 2);
}

TEST(LRUCacheTest, DuplicatePutTest) {
  memgraph::utils::LRUCache<int, int> cache(2);
  cache.put(1, 1);
  cache.put(2, 2);
  cache.put(1, 10);

  int value;
  EXPECT_TRUE(cache.get(1, value));
  EXPECT_EQ(value, 10);

  EXPECT_TRUE(cache.get(2, value));
  EXPECT_EQ(value, 2);
}

TEST(LRUCacheTest, ResizeTest) {
  memgraph::utils::LRUCache<int, int> cache(2);
  cache.put(1, 1);
  cache.put(2, 2);
  cache.put(3, 3);

  int value;
  EXPECT_FALSE(cache.get(1, value));

  EXPECT_TRUE(cache.get(2, value));
  EXPECT_EQ(value, 2);

  EXPECT_TRUE(cache.get(3, value));
  EXPECT_EQ(value, 3);
}

TEST(LRUCacheTest, EmptyCacheTest) {
  memgraph::utils::LRUCache<int, int> cache(2);

  int value;
  EXPECT_FALSE(cache.get(1, value));

  cache.put(1, 1);
  EXPECT_TRUE(cache.get(1, value));
  EXPECT_EQ(value, 1);
}

TEST(LRUCacheTest, LargeCacheTest) {
  const int CACHE_SIZE = 10000;
  memgraph::utils::LRUCache<int, int> cache(CACHE_SIZE);

  for (int i = 0; i < CACHE_SIZE; i++) {
    cache.put(i, i);
  }

  int value;
  for (int i = 0; i < CACHE_SIZE; i++) {
    EXPECT_TRUE(cache.get(i, value));
    EXPECT_EQ(value, i);
  }
}
