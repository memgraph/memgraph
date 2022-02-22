// Copyright 2022 Memgraph Ltd.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt; by using this file, you agree to be bound by the terms of the Business Source
// License, and you may not use this file except in compliance with the Business Source License.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

#include <atomic>
#include <chrono>
#include <optional>
#include <string>
#include <thread>
#include <utility>
#include <vector>

#include "gtest/gtest.h"

#include "utils/small_vector.hpp"

TEST(SmallVector, BasicTest) {
  const int kMaxElements = 10;

  memgraph::utils::SmallVector<int, kMaxElements> small_vector;

  for (int i = 0; i < kMaxElements; ++i) {
    small_vector.push_back(i);
  }

  for (int i = 0; i < kMaxElements; ++i) {
    EXPECT_EQ(small_vector[i], i);
  }
}

TEST(SmallVector, Clear) {
  const int kMaxElements = 10;
  memgraph::utils::SmallVector<int, kMaxElements> small_vector;

  for (int i = 0; i < kMaxElements + 1; ++i) {
    small_vector.push_back(i);
  }

  auto capacity = small_vector.capacity();
  EXPECT_EQ(small_vector.size(), kMaxElements + 1);
  small_vector.clear();
  EXPECT_EQ(small_vector.size(), 0);
  EXPECT_EQ(capacity, small_vector.capacity());
}

TEST(SmallVector, Resize) {
  const int kSmallStorageSize = 5;
  const int kTwiceTheSmallStorage = 2 * kSmallStorageSize;
  const int kSomeRandomConst = 505;
  memgraph::utils::SmallVector<int, kSmallStorageSize> small_vector;

  for (int i = 0; i < kTwiceTheSmallStorage; ++i) {
    small_vector.push_back(i);
  }

  EXPECT_EQ(small_vector.size(), kTwiceTheSmallStorage);
  small_vector.resize(kSmallStorageSize);
  EXPECT_EQ(small_vector.size(), kSmallStorageSize);

  small_vector.resize(kTwiceTheSmallStorage, kSomeRandomConst);
  for (int i = kSmallStorageSize; i < kTwiceTheSmallStorage; ++i) EXPECT_EQ(small_vector[i], kSomeRandomConst);
}

TEST(SmallVector, Reserve) {
  const int kMaxElements = 1000;
  const int kMaxElementsAfter = 1000;
  memgraph::utils::SmallVector<int, kMaxElements> small_vector;
  EXPECT_EQ(small_vector.capacity(), kMaxElements);
  small_vector.reserve(kMaxElementsAfter);
  EXPECT_EQ(small_vector.capacity(), kMaxElementsAfter);
  small_vector.reserve(1);
  EXPECT_EQ(small_vector.capacity(), kMaxElementsAfter);
}

TEST(SmallVector, PopBackVal) {
  const int kMaxElements = 10;
  memgraph::utils::SmallVector<std::string, kMaxElements> small_vector;
  for (int i = 0; i < kMaxElements; ++i) {
    small_vector.push_back(std::to_string(i));
  }
  for (int i = kMaxElements - 1; i >= 0; --i) {
    EXPECT_EQ(small_vector.pop_back_val(), std::to_string(i));
  }

  EXPECT_EQ(small_vector.size(), 0);
}

TEST(SmallVector, Swap) {
  const int kSize1 = 10;
  const int kSize2 = 1000;

  memgraph::utils::SmallVector<int, kSize1> vector1;
  memgraph::utils::SmallVector<int, kSize2> vector2;

  memgraph::utils::SmallVector<int, kSize1> vector3;
  memgraph::utils::SmallVector<int, kSize2> vector4;

  memgraph::utils::SmallVector<int, kSize2> ref_vector1;
  memgraph::utils::SmallVector<int, kSize2> ref_vector2;
  memgraph::utils::SmallVector<int, kSize2> ref_vector3;
  memgraph::utils::SmallVector<int, kSize2> ref_vector4;

  for (int i = 0; i < kSize1; ++i) {
    int value = i % 3;
    ref_vector1.push_back(value);
    vector1.push_back(value);
    ref_vector3.push_back(value + 1);
    vector3.push_back(value + 1);
  }

  for (int i = 0; i < 2 * kSize2; ++i) {
    ref_vector2.push_back(i);
    vector2.push_back(i);
    ref_vector4.push_back(i + 1);
    vector4.push_back(i + 1);
  }

  EXPECT_EQ(ref_vector1, vector1);
  EXPECT_EQ(ref_vector2, vector2);
  EXPECT_EQ(ref_vector3, vector3);
  EXPECT_EQ(ref_vector4, vector4);

  EXPECT_NE(vector1, vector2);
  EXPECT_NE(vector3, vector4);

  vector1.swap(vector2);
  vector4.swap(vector3);

  EXPECT_EQ(ref_vector1, vector2);
  EXPECT_EQ(ref_vector2, vector1);
  EXPECT_EQ(ref_vector3, vector4);
  EXPECT_EQ(ref_vector4, vector3);
}

TEST(SmallVector, Append1) {
  const int kMaxElements = 100;
  const int kSize = 10;
  std::vector<int> test_vector;
  for (int i = 0; i < kMaxElements; ++i) {
    test_vector.push_back(i);
  }
  memgraph::utils::SmallVector<int, kSize> small_vector = {20};
  small_vector.append(test_vector.begin(), test_vector.end());
  EXPECT_EQ(20, small_vector[0]);
  for (int i = 0; i < kMaxElements; ++i) {
    EXPECT_EQ(test_vector[i], small_vector[i + 1]);
  }
}

TEST(SmallVector, Append2) {
  const int kSize = 10;
  const std::string kElement = "dolje na koljena, reci mi moja voljena";
  memgraph::utils::SmallVector<std::string, 0> test_vector(kSize, kElement);
  memgraph::utils::SmallVector<std::string, kSize> small_vector;
  small_vector.append(kSize, kElement);
  EXPECT_EQ(small_vector.size(), kSize);
  EXPECT_EQ(test_vector, small_vector);
}

TEST(SmallVector, Append3) {
  const int kSize = 3;
  memgraph::utils::SmallVector<int, kSize> test_vector = {1, 2, 3, 4, 5, 1, 2, 3, 4, 5};
  memgraph::utils::SmallVector<int, kSize> small_vector = {1, 2, 3, 4, 5};
  small_vector.append({1, 2, 3, 4, 5});
  EXPECT_EQ(test_vector, small_vector);
}

TEST(SmallVector, Assign1) {
  const int kSize = 3;
  const int kElemNum = 100;
  const std::string kElement = "brate samo loudam malo toga sejvam";
  memgraph::utils::SmallVector<std::string, kSize> test_vector;
  memgraph::utils::SmallVector<std::string, kSize> small_vector = {"a", "b", "c", "d", "e", "f"};
  for (int i = 0; i < kElemNum; ++i) {
    test_vector.push_back(kElement);
  }

  small_vector.assign(100, kElement);
  EXPECT_EQ(test_vector, small_vector);
}

TEST(SmallVector, Assign2) {
  const int kSize = 3;
  const std::string kElement = "preko tjedna gospoda";
  memgraph::utils::SmallVector<std::string, kSize> test_vector = {kElement};
  memgraph::utils::SmallVector<std::string, kSize> small_vector = {"a", "b", "c", "d", "e", "f"};
  small_vector.assign({kElement});
  EXPECT_EQ(test_vector, small_vector);
}

TEST(SmallVector, Erase) {
  const int kSize = 3;
  memgraph::utils::SmallVector<std::string, kSize> small_vector = {"a", "b", "c", "d", "e", "f"};
  small_vector.erase(small_vector.begin() + 1, small_vector.end());
  EXPECT_EQ(1, small_vector.size());
  EXPECT_EQ("a", small_vector[0]);
  small_vector.erase(small_vector.begin());
  EXPECT_EQ(0, small_vector.size());
}

TEST(SmallVector, Insert) {
  const int kSize = 3;
  const std::string kXXX = "xxx";

  memgraph::utils::SmallVector<std::string, kSize> test_vector = {"1", "2", "3", "4", "5"};
  memgraph::utils::SmallVector<std::string, kSize> small_vector = {"a", "b", "c", "d", "e", "f"};
  small_vector.insert(small_vector.begin(), kXXX);
  EXPECT_EQ(kXXX, small_vector[0]);

  small_vector.insert(small_vector.begin() + 1, test_vector.begin(), test_vector.end());

  for (int i = 0; i < 5; ++i) {
    EXPECT_EQ(test_vector[i], small_vector[i + 1]);
  }

  small_vector.insert(small_vector.end(), 10, kXXX);

  for (int i = small_vector.size() - 1; i >= small_vector.size() - 10; --i) {
    EXPECT_EQ(kXXX, small_vector[i]);
  }

  small_vector.insert(small_vector.begin(), {"www", "abc"});
  EXPECT_EQ("www", small_vector[0]);
  EXPECT_EQ("abc", small_vector[1]);
}

TEST(SmallVector, EmplaceBack) {
  const int kSize = 3;
  memgraph::utils::SmallVector<std::string, kSize> small_vector = {"a", "b", "c", "d", "e", "f"};
  small_vector.emplace_back("g");
  EXPECT_EQ("g", small_vector.back());
}

TEST(SmallVector, PushPopBack) {
  const int kSize = 3;
  const int kElemNum = 10000;
  memgraph::utils::SmallVector<int, kSize> small_vector;
  EXPECT_EQ(0, small_vector.size());
  for (int i = 0; i < kElemNum; ++i) {
    small_vector.push_back(i);
  }
  EXPECT_EQ(kElemNum, small_vector.size());
  for (int i = 0; i < kElemNum; ++i) {
    small_vector.pop_back();
  }
  EXPECT_EQ(0, small_vector.size());
}

TEST(SmallVector, Capacity) {
  const int kSize = 3;
  const int kElemNum = 10000;
  memgraph::utils::SmallVector<int, kSize> small_vector;
  EXPECT_EQ(0, small_vector.size());
  EXPECT_EQ(3, small_vector.capacity());
  for (int i = 0; i < kElemNum; ++i) {
    small_vector.push_back(i);
  }
  EXPECT_EQ(kElemNum, small_vector.size());
  EXPECT_LE(kElemNum, small_vector.capacity());
}

TEST(SmallVector, Data) {
  const int kSize = 3;
  const int kElemNum = 100;
  memgraph::utils::SmallVector<int, kSize> small_vector;
  for (int i = 0; i < kElemNum; ++i) {
    small_vector.push_back(i);
  }
  int *p = small_vector.data();
  for (int i = 0; i < kElemNum; ++i) {
    EXPECT_EQ(i, *(p + i));
  }
}

TEST(SmallVector, Empty) {
  const int kSize = 3;
  const int kElemNum = 10000;
  memgraph::utils::SmallVector<int, kSize> small_vector;
  EXPECT_TRUE(small_vector.empty());
  for (int i = 0; i < kElemNum; ++i) {
    small_vector.push_back(i);
  }
  EXPECT_FALSE(small_vector.empty());
  for (int i = 0; i < kElemNum; ++i) {
    small_vector.pop_back();
  }
  EXPECT_TRUE(small_vector.empty());
}

TEST(SmallVector, Operators) {
  const int kSize = 3;
  const int kElemNum = 10000;
  memgraph::utils::SmallVector<int, kSize> small_vector_1;
  memgraph::utils::SmallVector<int, kSize> small_vector_2;

  for (int i = 0; i < kElemNum; ++i) {
    small_vector_1.push_back(i);
    small_vector_2.push_back(i + 1);
  }

  EXPECT_NE(small_vector_1, small_vector_2);
  EXPECT_FALSE(small_vector_1 == small_vector_2);
  EXPECT_TRUE(small_vector_1 < small_vector_2);
}
