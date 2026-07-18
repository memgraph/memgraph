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
  // With cache_size == 2, filling with 1, 2 then inserting 3 evicts whichever
  // of {1, 2} is least-recently-used. peek() must NOT count as a use: even
  // after peeking at key 1 many times, it should be evicted just like a
  // plain unaccessed entry, whereas get() would have kept it alive.
  memgraph::utils::LRUCache<int, int> peek_cache(2);
  peek_cache.put(1, 100);
  peek_cache.put(2, 200);

  auto const &const_peek_cache = peek_cache;
  // Repeatedly peek at key 1 - must not affect recency.
  for (int i = 0; i < 5; ++i) {
    auto const value = const_peek_cache.peek(1);
    EXPECT_TRUE(value.has_value());
  }

  // 2 was put after 1, so without any recency bump, 1 is the least-recently-used
  // and must be the one evicted when 3 is inserted.
  peek_cache.put(3, 300);
  EXPECT_FALSE(const_peek_cache.peek(1).has_value());
  EXPECT_TRUE(const_peek_cache.peek(2).has_value());
  EXPECT_TRUE(const_peek_cache.peek(3).has_value());

  // Control: the same sequence but using get() on key 1 instead of peek()
  // must bump its recency, so 2 (never re-touched) is evicted instead.
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
