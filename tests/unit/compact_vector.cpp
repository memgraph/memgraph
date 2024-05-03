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

#include <string>

#include "gtest/gtest.h"

#include "storage/v2/tco_vector.hpp"

TEST(CompactVector, BasicTest) {
  const int kMaxElements = 10;

  memgraph::storage::TcoVector<int> vec;

  for (int i = 0; i < kMaxElements; ++i) {
    vec.push_back(i);
  }

  for (int i = 0; i < kMaxElements; ++i) {
    EXPECT_EQ(vec[i], i);
  }
}

TEST(CompactVector, Clear) {
  const int kMaxElements = 10;
  memgraph::storage::TcoVector<int> vec;

  for (int i = 0; i < kMaxElements + 1; ++i) {
    vec.push_back(i);
  }

  auto capacity = vec.capacity();
  EXPECT_EQ(vec.size(), kMaxElements + 1);
  vec.clear();
  EXPECT_EQ(vec.size(), 0);
  EXPECT_EQ(capacity, vec.capacity());
}

TEST(CompactVector, Resize) {
  const int kSmallStorageSize = 5;
  const int kTwiceTheSmallStorage = 2 * kSmallStorageSize;
  memgraph::storage::TcoVector<int> vec;

  for (int i = 0; i < kTwiceTheSmallStorage; ++i) {
    vec.push_back(i);
  }

  EXPECT_EQ(vec.size(), kTwiceTheSmallStorage);
  vec.resize(kSmallStorageSize);
  EXPECT_EQ(vec.size(), kSmallStorageSize);

  vec.resize(kTwiceTheSmallStorage);
  for (int i = kSmallStorageSize; i < kTwiceTheSmallStorage; ++i) EXPECT_NE(vec[i], i);
}

TEST(CompactVector, Reserve) {
  const int kMaxElements = 1000;
  memgraph::storage::TcoVector<int> vec;
  EXPECT_EQ(vec.capacity(), 0);
  vec.reserve(kMaxElements);
  EXPECT_EQ(vec.capacity(), kMaxElements);
  vec.reserve(1);
  EXPECT_EQ(vec.capacity(), kMaxElements);
}

TEST(CompactVector, EraseFirst) {
  memgraph::storage::TcoVector<std::string> vec = {"a", "b", "c"};
  auto next = vec.erase(vec.begin());
  EXPECT_EQ(2, vec.size());
  EXPECT_EQ("b", vec[0]);
  EXPECT_EQ("c", vec[1]);
  EXPECT_EQ("b", *next);
}

TEST(CompactVector, EraseThird) {
  memgraph::storage::TcoVector<std::string> vec = {"a", "b", "c", "d"};
  auto next = vec.begin();
  ++next;  // "b"
  ++next;  // "c"
  next = vec.erase(next);

  EXPECT_EQ(3, vec.size());
  EXPECT_EQ("a", vec[0]);
  EXPECT_EQ("b", vec[1]);
  EXPECT_EQ("d", vec[2]);
  EXPECT_EQ("d", *next);
}

TEST(CompactVector, EraseLast) {
  memgraph::storage::TcoVector<std::string> vec = {"a", "b", "c"};
  auto next = vec.begin();
  ++next;  // "b"
  ++next;  // "c"
  next = vec.erase(next);

  EXPECT_EQ(2, vec.size());
  EXPECT_EQ("a", vec[0]);
  EXPECT_EQ("b", vec[1]);
  EXPECT_EQ(vec.end(), next);
}

TEST(CompactVector, ErasePart) {
  memgraph::storage::TcoVector<std::string> vec = {"a", "b", "c", "d", "e", "f"};
  auto beg = vec.begin();
  ++beg;  // "b"
  auto end = beg;
  ++end;
  ++end;  // "d"

  end = vec.erase(beg, end);
  EXPECT_EQ(4, vec.size());
  EXPECT_EQ("a", vec[0]);
  EXPECT_EQ("d", vec[1]);
  EXPECT_EQ("e", vec[2]);
  EXPECT_EQ("f", vec[3]);
  EXPECT_EQ(*end, "d");
}

TEST(CompactVector, ErasePartTillEnd) {
  memgraph::storage::TcoVector<std::string> vec = {"a", "b", "c", "d", "e", "f"};
  auto it = vec.begin();
  ++it;  // "b"

  it = vec.erase(it, vec.end());
  EXPECT_EQ(1, vec.size());
  EXPECT_EQ("a", vec[0]);
  EXPECT_EQ(it, vec.end());
}

TEST(CompactVector, EraseFull) {
  memgraph::storage::TcoVector<std::string> vec = {"a", "b", "c", "d", "e", "f"};

  auto it = vec.erase(vec.begin(), vec.end());
  EXPECT_EQ(0, vec.size());
  EXPECT_EQ(it, vec.end());
}

TEST(CompactVector, EraseOneEndIterator) {
  memgraph::storage::TcoVector<std::string> vec = {"a", "b", "c"};

  auto it = vec.erase(vec.end());
  EXPECT_EQ(3, vec.size());
  EXPECT_EQ(it, vec.end());
}

TEST(CompactVector, EraseRangeEndIterator) {
  memgraph::storage::TcoVector<std::string> vec = {"a", "b", "c"};

  auto it = vec.erase(vec.end(), vec.end());
  EXPECT_EQ(3, vec.size());
  EXPECT_EQ(it, vec.end());
}

TEST(CompactVector, EraseOneEndIteratorOnEmptyVector) {
  memgraph::storage::TcoVector<std::string> vec;

  auto it = vec.erase(vec.end());
  EXPECT_EQ(0, vec.size());
  EXPECT_EQ(it, vec.end());
}

TEST(CompactVector, EraseRangeEndIteratorOnEmptyVector) {
  memgraph::storage::TcoVector<std::string> vec;

  auto it = vec.erase(vec.end(), vec.end());
  EXPECT_EQ(0, vec.size());
  EXPECT_EQ(it, vec.end());
}

TEST(CompactVector, EmplaceBack) {
  memgraph::storage::TcoVector<std::string> vec = {"a", "b", "c", "d", "e", "f"};
  vec.emplace_back("g");
  EXPECT_EQ("g", vec.back());
}

TEST(CompactVector, PushPopBack) {
  const int kElemNum = 10000;
  memgraph::storage::TcoVector<int> vec;
  EXPECT_EQ(0, vec.size());
  for (int i = 0; i < kElemNum; ++i) {
    vec.push_back(i);
  }
  EXPECT_EQ(kElemNum, vec.size());
  for (int i = 0; i < kElemNum; ++i) {
    vec.pop_back();
  }
  EXPECT_EQ(0, vec.size());
}

TEST(CompactVector, Capacity) {
  const int kElemNum = 10000;
  memgraph::storage::TcoVector<int> vec;
  EXPECT_EQ(0, vec.size());
  EXPECT_EQ(0, vec.capacity());

  for (int i = 0; i < kElemNum; ++i) {
    vec.push_back(i);
  }
  EXPECT_EQ(kElemNum, vec.size());
  EXPECT_LE(kElemNum, vec.capacity());
}

TEST(CompactVector, Empty) {
  const int kElemNum = 10000;
  memgraph::storage::TcoVector<int> vec;
  EXPECT_TRUE(vec.empty());
  for (int i = 0; i < kElemNum; ++i) {
    vec.push_back(i);
  }
  EXPECT_FALSE(vec.empty());
  for (int i = 0; i < kElemNum; ++i) {
    vec.pop_back();
  }
  EXPECT_TRUE(vec.empty());
}
