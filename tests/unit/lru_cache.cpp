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

namespace {
// A key that counts both its live instances and its total constructions, so a
// test can assert how many times the cache materializes a key.
struct CountingKey {
  static inline int live = 0;
  static inline int constructed = 0;

  explicit CountingKey(int value) : value{value} {
    ++live;
    ++constructed;
  }

  CountingKey(const CountingKey &other) : value{other.value} {
    ++live;
    ++constructed;
  }

  CountingKey(CountingKey &&other) noexcept : value{other.value} {
    ++live;
    ++constructed;
  }

  CountingKey &operator=(const CountingKey &) = default;
  CountingKey &operator=(CountingKey &&) = default;

  ~CountingKey() { --live; }

  bool operator==(const CountingKey &other) const { return value == other.value; }

  int value;
};
}  // namespace

template <>
struct std::hash<CountingKey> {
  size_t operator()(const CountingKey &key) const noexcept { return std::hash<int>{}(key.value); }
};

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

// An entry is immutable once cached: a put for a key already present keeps the
// stored value rather than overwriting it.
TEST(LRUCacheTest, DuplicatePutKeepsExistingValue) {
  memgraph::utils::LRUCache<int, int> cache(2);
  cache.put(1, 1);
  cache.put(2, 2);
  cache.put(1, 10);

  EXPECT_EQ(cache.get(1).value(), 1);
  EXPECT_EQ(cache.get(2).value(), 2);
}

// A put for a present key, though it does not change the value, still refreshes
// recency: the entry becomes most-recently-used and so survives the next
// eviction that a fresh insert triggers.
TEST(LRUCacheTest, DuplicatePutPromotesToMru) {
  memgraph::utils::LRUCache<int, int> cache(2);
  cache.put(1, 1);
  cache.put(2, 2);   // order MRU..LRU: 2, 1
  cache.put(1, 99);  // key present: value kept, 1 promoted -> 1, 2
  cache.put(3, 3);   // capacity reached: evicts LRU, which is now 2

  EXPECT_FALSE(cache.get(2).has_value());
  EXPECT_EQ(cache.get(1).value(), 1);
  EXPECT_TRUE(cache.get(3).has_value());
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

TEST(LRUCacheTest, KeyMaterializedOncePerEntry) {
  constexpr int kEntries = 100;
  ASSERT_EQ(CountingKey::live, 0);
  {
    memgraph::utils::LRUCache<CountingKey, int> cache(kEntries);
    for (int i = 0; i < kEntries; ++i) {
      cache.put(CountingKey{i}, i);
    }
    // Each resident entry materializes exactly one key: the list node owns it
    // and the index stores only an iterator, not a second copy.
    EXPECT_EQ(CountingKey::live, kEntries);

    // A get() resolves the bare key through transparent lookup, constructing no
    // key of its own on either a hit or a miss.
    const int constructed_after_fill = CountingKey::constructed;
    for (int i = 0; i < kEntries; ++i) {
      EXPECT_TRUE(cache.get(CountingKey{i}).has_value());  // hit
    }
    EXPECT_FALSE(cache.get(CountingKey{-1}).has_value());  // miss
    // The only keys built here are the throwaway lookup arguments: one per call,
    // none retained.
    EXPECT_EQ(CountingKey::live, kEntries);
    EXPECT_EQ(CountingKey::constructed, constructed_after_fill + kEntries + 1);
  }
  EXPECT_EQ(CountingKey::live, 0);
}

TEST(LRUCacheTest, InvalidateTest) {
  memgraph::utils::LRUCache<int, int> cache(3);
  cache.put(1, 100);
  cache.put(2, 200);
  cache.put(3, 300);

  EXPECT_TRUE(cache.get(1).has_value());
  EXPECT_TRUE(cache.get(2).has_value());
  EXPECT_TRUE(cache.get(3).has_value());

  cache.invalidate(2);

  EXPECT_FALSE(cache.get(2).has_value());
  EXPECT_EQ(cache.get(1).value(), 100);
  EXPECT_EQ(cache.get(3).value(), 300);
  EXPECT_EQ(cache.size(), 2);

  // Invalidating an absent key is a no-op.
  cache.invalidate(42);
  EXPECT_EQ(cache.size(), 2);
}
