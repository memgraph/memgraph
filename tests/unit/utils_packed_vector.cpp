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

#include <gtest/gtest.h>

#include "utils/packed_vector.hpp"

#include <vector>

using memgraph::utils::PackedVarintVector;

TEST(PackedVarintVector, DefaultConstruction) {
  PackedVarintVector vec;
  EXPECT_EQ(vec.count(), 0);
  EXPECT_EQ(vec.byte_size(), 0);
  vec.for_each([](uint32_t /* val */) { ADD_FAILURE() << "empty vector should not call func"; });
}

TEST(PackedVarintVector, PushBackSingleValue) {
  PackedVarintVector vec;
  vec.push_back(42);
  EXPECT_EQ(vec.count(), 1);
  EXPECT_EQ(vec.byte_size(), 1);

  std::vector<uint32_t> seen;
  vec.for_each([&seen](uint32_t val) { seen.push_back(val); });
  EXPECT_EQ(seen, std::vector<uint32_t>{42});
}

TEST(PackedVarintVector, PushBackZero) {
  PackedVarintVector vec;
  vec.push_back(0);
  EXPECT_EQ(vec.count(), 1);
  EXPECT_EQ(vec.byte_size(), 1);

  std::vector<uint32_t> seen;
  vec.for_each([&seen](uint32_t val) { seen.push_back(val); });
  EXPECT_EQ(seen, std::vector<uint32_t>{0});
}

TEST(PackedVarintVector, PushBackOneByteMax) {
  PackedVarintVector vec;
  vec.push_back(127);
  EXPECT_EQ(vec.count(), 1);
  EXPECT_EQ(vec.byte_size(), 1);

  std::vector<uint32_t> seen;
  vec.for_each([&seen](uint32_t val) { seen.push_back(val); });
  EXPECT_EQ(seen, std::vector<uint32_t>{127});
}

TEST(PackedVarintVector, PushBackTwoBytesMin) {
  PackedVarintVector vec;
  vec.push_back(128);
  EXPECT_EQ(vec.count(), 1);
  EXPECT_EQ(vec.byte_size(), 2);

  std::vector<uint32_t> seen;
  vec.for_each([&seen](uint32_t val) { seen.push_back(val); });
  EXPECT_EQ(seen, std::vector<uint32_t>{128});
}

TEST(PackedVarintVector, PushBackMaxUint32) {
  PackedVarintVector vec;
  vec.push_back(UINT32_MAX);
  EXPECT_EQ(vec.count(), 1);
  EXPECT_EQ(vec.byte_size(), 5);

  std::vector<uint32_t> seen;
  vec.for_each([&seen](uint32_t val) { seen.push_back(val); });
  EXPECT_EQ(seen, std::vector<uint32_t>{UINT32_MAX});
}

TEST(PackedVarintVector, PushBackMultipleValues) {
  PackedVarintVector vec;
  std::vector<uint32_t> expected = {0, 1, 127, 128, 255, 256, 1000, 1000000, UINT32_MAX};
  for (uint32_t val : expected) {
    vec.push_back(val);
  }
  EXPECT_EQ(vec.count(), static_cast<uint32_t>(expected.size()));

  std::vector<uint32_t> seen;
  vec.for_each([&seen](uint32_t val) { seen.push_back(val); });
  EXPECT_EQ(seen, expected);
}

TEST(PackedVarintVector, ByteSizeAndCount) {
  PackedVarintVector vec;
  EXPECT_EQ(vec.byte_size(), 0);
  EXPECT_EQ(vec.count(), 0);

  vec.push_back(0);
  EXPECT_EQ(vec.byte_size(), 1);
  EXPECT_EQ(vec.count(), 1);

  vec.push_back(128);
  EXPECT_EQ(vec.byte_size(), 3);  // 1 + 2
  EXPECT_EQ(vec.count(), 2);

  vec.push_back(16384);
  EXPECT_EQ(vec.byte_size(), 6);  // 1 + 2 + 3
  EXPECT_EQ(vec.count(), 3);
}

TEST(PackedVarintVector, SmallBufferStaysInline) {
  PackedVarintVector vec;
  for (int idx = 0; idx < 8; ++idx) {
    vec.push_back(static_cast<uint32_t>(idx));
  }
  EXPECT_EQ(vec.count(), 8);
  EXPECT_EQ(vec.byte_size(), 8);

  std::vector<uint32_t> seen;
  vec.for_each([&seen](uint32_t val) { seen.push_back(val); });
  EXPECT_EQ(seen.size(), 8);
  for (int idx = 0; idx < 8; ++idx) {
    EXPECT_EQ(seen[static_cast<size_t>(idx)], static_cast<uint32_t>(idx));
  }
}

TEST(PackedVarintVector, GrowsToHeap) {
  PackedVarintVector vec;
  for (int idx = 0; idx < 20; ++idx) {
    vec.push_back(128);
  }
  EXPECT_EQ(vec.count(), 20);
  EXPECT_GE(vec.byte_size(), 40);

  std::vector<uint32_t> seen;
  vec.for_each([&seen](uint32_t val) { seen.push_back(val); });
  EXPECT_EQ(seen.size(), 20);
  for (uint32_t val : seen) {
    EXPECT_EQ(val, 128);
  }
}

TEST(PackedVarintVector, MoveConstructFromSmallBuffer) {
  PackedVarintVector source;
  source.push_back(1);
  source.push_back(2);

  PackedVarintVector dest(std::move(source));

  EXPECT_EQ(source.count(), 0);
  EXPECT_EQ(source.byte_size(), 0);

  EXPECT_EQ(dest.count(), 2);
  std::vector<uint32_t> seen;
  dest.for_each([&seen](uint32_t val) { seen.push_back(val); });
  EXPECT_EQ(seen, std::vector<uint32_t>({1, 2}));
}

TEST(PackedVarintVector, MoveConstructFromHeap) {
  PackedVarintVector source;
  for (int idx = 0; idx < 20; ++idx) {
    source.push_back(static_cast<uint32_t>(idx));
  }

  PackedVarintVector dest(std::move(source));

  EXPECT_EQ(source.count(), 0);
  EXPECT_EQ(source.byte_size(), 0);

  EXPECT_EQ(dest.count(), 20);
  std::vector<uint32_t> seen;
  dest.for_each([&seen](uint32_t val) { seen.push_back(val); });
  EXPECT_EQ(seen.size(), 20);
  for (int idx = 0; idx < 20; ++idx) {
    EXPECT_EQ(seen[static_cast<size_t>(idx)], static_cast<uint32_t>(idx));
  }
}

TEST(PackedVarintVector, MoveAssignFromSmallToEmpty) {
  PackedVarintVector source;
  source.push_back(10);
  PackedVarintVector dest;

  dest = std::move(source);

  EXPECT_EQ(source.count(), 0);
  EXPECT_EQ(dest.count(), 1);
  std::vector<uint32_t> seen;
  dest.for_each([&seen](uint32_t val) { seen.push_back(val); });
  EXPECT_EQ(seen, std::vector<uint32_t>{10});
}

TEST(PackedVarintVector, MoveAssignFromHeapToSmall) {
  PackedVarintVector source;
  source.push_back(1);
  PackedVarintVector dest;
  for (int idx = 0; idx < 20; ++idx) {
    dest.push_back(static_cast<uint32_t>(idx));
  }

  source = std::move(dest);

  EXPECT_EQ(dest.count(), 0);
  EXPECT_EQ(source.count(), 20);
  std::vector<uint32_t> seen;
  source.for_each([&seen](uint32_t val) { seen.push_back(val); });
  EXPECT_EQ(seen.size(), 20);
}

TEST(PackedVarintVector, MoveAssignSelf) {
  PackedVarintVector vec;
  vec.push_back(1);
  vec = std::move(vec);  // NOLINT(clang-diagnostic-self-move) -- intentional: self-move must leave object valid
  EXPECT_EQ(vec.count(), 1);
  std::vector<uint32_t> seen;
  vec.for_each([&seen](uint32_t val) { seen.push_back(val); });
  EXPECT_EQ(seen, std::vector<uint32_t>{1});
}

TEST(PackedVarintVector, CopyConstruct) {
  PackedVarintVector source;
  source.push_back(1);
  source.push_back(128);

  PackedVarintVector dest(source);

  EXPECT_EQ(source.count(), 2);
  EXPECT_EQ(dest.count(), 2);
  std::vector<uint32_t> seen;
  dest.for_each([&seen](uint32_t val) { seen.push_back(val); });
  EXPECT_EQ(seen, std::vector<uint32_t>({1, 128}));
}

TEST(PackedVarintVector, CopyConstructFromHeap) {
  PackedVarintVector source;
  for (int idx = 0; idx < 20; ++idx) {
    source.push_back(static_cast<uint32_t>(idx));
  }

  PackedVarintVector dest(source);

  EXPECT_EQ(source.count(), 20);
  EXPECT_EQ(dest.count(), 20);
  std::vector<uint32_t> seen;
  dest.for_each([&seen](uint32_t val) { seen.push_back(val); });
  EXPECT_EQ(seen.size(), 20);
}

TEST(PackedVarintVector, CopyAssign) {
  PackedVarintVector source;
  source.push_back(10);
  source.push_back(20);
  PackedVarintVector dest;
  dest.push_back(99);

  dest = source;

  EXPECT_EQ(source.count(), 2);
  EXPECT_EQ(dest.count(), 2);
  std::vector<uint32_t> seen;
  dest.for_each([&seen](uint32_t val) { seen.push_back(val); });
  EXPECT_EQ(seen, std::vector<uint32_t>({10, 20}));
}

TEST(PackedVarintVector, MoveNoexcept) {
  static_assert(std::is_nothrow_move_constructible_v<PackedVarintVector>);
  static_assert(std::is_nothrow_move_assignable_v<PackedVarintVector>);
}

// --- Edge cases: copy assign self, iterators, erase, varint boundaries, buffer state combinations ---

TEST(PackedVarintVector, CopyAssignSelf) {
  PackedVarintVector vec;
  vec.push_back(1);
  vec.push_back(128);
  vec = vec;  // NOLINT(clang-diagnostic-self-assign-overloaded) -- intentional: must remain valid
  EXPECT_EQ(vec.count(), 2);
  std::vector<uint32_t> seen;
  vec.for_each([&seen](uint32_t val) { seen.push_back(val); });
  EXPECT_EQ(seen, std::vector<uint32_t>({1, 128}));
}

TEST(PackedVarintVector, CopyAssignSelfHeap) {
  PackedVarintVector vec;
  for (int idx = 0; idx < 20; ++idx) {
    vec.push_back(static_cast<uint32_t>(idx));
  }
  vec = vec;  // NOLINT(clang-diagnostic-self-assign-overloaded) -- intentional
  EXPECT_EQ(vec.count(), 20);
  std::vector<uint32_t> seen;
  vec.for_each([&seen](uint32_t val) { seen.push_back(val); });
  EXPECT_EQ(seen.size(), 20);
}

TEST(PackedVarintVector, IteratorsEmpty) {
  PackedVarintVector vec;
  EXPECT_TRUE(vec.begin() == vec.end());
  EXPECT_FALSE(vec.begin() != vec.end());
}

TEST(PackedVarintVector, IteratorsRangeFor) {
  PackedVarintVector vec;
  std::vector<uint32_t> expected = {0, 1, 127, 128, 255, 256, 1000, 1000000, UINT32_MAX};
  for (uint32_t val : expected) {
    vec.push_back(val);
  }
  std::vector<uint32_t> via_range_for;
  for (uint32_t val : vec) {
    via_range_for.push_back(val);
  }
  EXPECT_EQ(via_range_for, expected);
}

TEST(PackedVarintVector, IteratorsPostIncrement) {
  PackedVarintVector vec;
  vec.push_back(10);
  vec.push_back(20);
  vec.push_back(30);
  auto it = vec.begin();
  EXPECT_EQ(*it++, 10);
  EXPECT_EQ(*it++, 20);
  EXPECT_EQ(*it++, 30);
  EXPECT_TRUE(it == vec.end());
}

TEST(PackedVarintVector, EraseFirst) {
  PackedVarintVector vec;
  vec.push_back(1);
  vec.push_back(2);
  vec.push_back(3);
  auto it = vec.erase(vec.begin());
  EXPECT_EQ(vec.count(), 2);
  std::vector<uint32_t> seen;
  vec.for_each([&seen](uint32_t val) { seen.push_back(val); });
  EXPECT_EQ(seen, std::vector<uint32_t>({2, 3}));
  EXPECT_EQ(*it, 2);
}

TEST(PackedVarintVector, EraseLast) {
  PackedVarintVector vec;
  vec.push_back(1);
  vec.push_back(2);
  vec.push_back(3);
  auto it = vec.begin();
  ++it;
  ++it;
  vec.erase(it);
  EXPECT_EQ(vec.count(), 2);
  std::vector<uint32_t> seen;
  vec.for_each([&seen](uint32_t val) { seen.push_back(val); });
  EXPECT_EQ(seen, std::vector<uint32_t>({1, 2}));
}

TEST(PackedVarintVector, EraseMiddle) {
  PackedVarintVector vec;
  vec.push_back(1);
  vec.push_back(128);  // 2 bytes
  vec.push_back(3);
  auto it = vec.begin();
  ++it;
  vec.erase(it);
  EXPECT_EQ(vec.count(), 2);
  std::vector<uint32_t> seen;
  vec.for_each([&seen](uint32_t val) { seen.push_back(val); });
  EXPECT_EQ(seen, std::vector<uint32_t>({1, 3}));
}

TEST(PackedVarintVector, EraseUntilEmpty) {
  PackedVarintVector vec;
  vec.push_back(1);
  vec.push_back(2);
  while (vec.begin() != vec.end()) {
    vec.erase(vec.begin());
  }
  EXPECT_EQ(vec.count(), 0);
  EXPECT_EQ(vec.byte_size(), 0);
  vec.for_each([](uint32_t /* val */) { ADD_FAILURE() << "should be empty"; });
}

TEST(PackedVarintVector, VarintBoundaryThreeBytes) {
  PackedVarintVector vec;
  vec.push_back(16383);  // max 2-byte
  vec.push_back(16384);  // min 3-byte
  EXPECT_EQ(vec.count(), 2);
  EXPECT_EQ(vec.byte_size(), 2 + 3);
  std::vector<uint32_t> seen;
  vec.for_each([&seen](uint32_t val) { seen.push_back(val); });
  EXPECT_EQ(seen, std::vector<uint32_t>({16383, 16384}));
}

TEST(PackedVarintVector, VarintBoundaryFourBytes) {
  PackedVarintVector vec;
  vec.push_back(2097151);  // max 3-byte
  vec.push_back(2097152);  // min 4-byte
  EXPECT_EQ(vec.count(), 2);
  EXPECT_EQ(vec.byte_size(), 3 + 4);
  std::vector<uint32_t> seen;
  vec.for_each([&seen](uint32_t val) { seen.push_back(val); });
  EXPECT_EQ(seen, std::vector<uint32_t>({2097151, 2097152}));
}

TEST(PackedVarintVector, MoveAssignFromSmallToHeap) {
  PackedVarintVector dest;
  for (int idx = 0; idx < 20; ++idx) {
    dest.push_back(200);  // heap
  }
  PackedVarintVector source;
  source.push_back(1);
  source.push_back(2);

  dest = std::move(source);

  EXPECT_EQ(source.count(), 0);
  EXPECT_EQ(dest.count(), 2);
  std::vector<uint32_t> seen;
  dest.for_each([&seen](uint32_t val) { seen.push_back(val); });
  EXPECT_EQ(seen, std::vector<uint32_t>({1, 2}));
}

TEST(PackedVarintVector, CopyAssignSmallToHeap) {
  PackedVarintVector dest;
  for (int idx = 0; idx < 20; ++idx) {
    dest.push_back(200);
  }
  PackedVarintVector source;
  source.push_back(10);
  source.push_back(20);

  dest = source;

  EXPECT_EQ(source.count(), 2);
  EXPECT_EQ(dest.count(), 2);
  std::vector<uint32_t> seen;
  dest.for_each([&seen](uint32_t val) { seen.push_back(val); });
  EXPECT_EQ(seen, std::vector<uint32_t>({10, 20}));
}

TEST(PackedVarintVector, CopyAssignHeapToSmall) {
  PackedVarintVector dest;
  dest.push_back(99);
  PackedVarintVector source;
  for (int idx = 0; idx < 20; ++idx) {
    source.push_back(static_cast<uint32_t>(idx));
  }

  dest = source;

  EXPECT_EQ(source.count(), 20);
  EXPECT_EQ(dest.count(), 20);
  std::vector<uint32_t> seen;
  dest.for_each([&seen](uint32_t val) { seen.push_back(val); });
  EXPECT_EQ(seen.size(), 20);
}

TEST(PackedVarintVector, CountMatchesForEach) {
  PackedVarintVector vec;
  std::vector<uint32_t> expected = {0, 1, 127, 128, 255, 1000, UINT32_MAX};
  for (uint32_t val : expected) {
    vec.push_back(val);
  }
  uint32_t for_each_count = 0;
  vec.for_each([&for_each_count](uint32_t /* val */) { for_each_count++; });
  EXPECT_EQ(vec.count(), for_each_count);
  EXPECT_EQ(for_each_count, static_cast<uint32_t>(expected.size()));
}

TEST(PackedVarintVector, ManyElements) {
  PackedVarintVector vec;
  constexpr int N = 500;
  for (int i = 0; i < N; ++i) {
    vec.push_back(static_cast<uint32_t>(i));
  }
  EXPECT_EQ(vec.count(), static_cast<uint32_t>(N));
  int idx = 0;
  vec.for_each([&idx](uint32_t val) {
    EXPECT_EQ(val, static_cast<uint32_t>(idx));
    idx++;
  });
  EXPECT_EQ(idx, N);
}

// --- empty() and clear() ---

TEST(PackedVarintVector, EmptyOnDefault) {
  PackedVarintVector vec;
  EXPECT_TRUE(vec.empty());
  vec.push_back(1);
  EXPECT_FALSE(vec.empty());
}

TEST(PackedVarintVector, ClearSmallBuffer) {
  PackedVarintVector vec;
  vec.push_back(1);
  vec.push_back(2);
  vec.clear();
  EXPECT_TRUE(vec.empty());
  EXPECT_EQ(vec.byte_size(), 0);
  EXPECT_EQ(vec.count(), 0);
  vec.for_each([](uint32_t) { ADD_FAILURE() << "should be empty after clear"; });
}

TEST(PackedVarintVector, ClearHeapBuffer) {
  PackedVarintVector vec;
  for (int i = 0; i < 20; ++i) {
    vec.push_back(128);
  }
  vec.clear();
  EXPECT_TRUE(vec.empty());
  EXPECT_EQ(vec.count(), 0);
}

TEST(PackedVarintVector, ClearThenPushBack) {
  PackedVarintVector vec;
  vec.push_back(10);
  vec.push_back(20);
  vec.clear();
  vec.push_back(30);
  EXPECT_EQ(vec.count(), 1);
  std::vector<uint32_t> seen;
  vec.for_each([&seen](uint32_t val) { seen.push_back(val); });
  EXPECT_EQ(seen, std::vector<uint32_t>{30});
}

// --- push_back after erase ---

TEST(PackedVarintVector, PushBackAfterErase) {
  PackedVarintVector vec;
  vec.push_back(1);
  vec.push_back(2);
  vec.push_back(3);
  vec.erase(vec.begin());
  vec.push_back(4);
  EXPECT_EQ(vec.count(), 3);
  std::vector<uint32_t> seen;
  vec.for_each([&seen](uint32_t val) { seen.push_back(val); });
  EXPECT_EQ(seen, std::vector<uint32_t>({2, 3, 4}));
}

TEST(PackedVarintVector, PushBackAfterEraseAll) {
  PackedVarintVector vec;
  vec.push_back(1);
  vec.push_back(2);
  while (!vec.empty()) {
    vec.erase(vec.begin());
  }
  vec.push_back(99);
  EXPECT_EQ(vec.count(), 1);
  EXPECT_EQ(*vec.begin(), 99);
}

// --- erase while iterating (erase-and-continue pattern) ---

TEST(PackedVarintVector, EraseWhileIteratingEvens) {
  PackedVarintVector vec;
  for (uint32_t i = 0; i < 10; ++i) {
    vec.push_back(i);
  }
  auto it = vec.begin();
  while (it != vec.end()) {
    if (*it % 2 == 0) {
      it = vec.erase(it);
    } else {
      ++it;
    }
  }
  std::vector<uint32_t> seen;
  vec.for_each([&seen](uint32_t val) { seen.push_back(val); });
  EXPECT_EQ(seen, std::vector<uint32_t>({1, 3, 5, 7, 9}));
}

TEST(PackedVarintVector, EraseWhileIteratingAll) {
  PackedVarintVector vec;
  vec.push_back(10);
  vec.push_back(20);
  vec.push_back(30);
  auto it = vec.begin();
  while (it != vec.end()) {
    it = vec.erase(it);
  }
  EXPECT_TRUE(vec.empty());
  EXPECT_EQ(vec.count(), 0);
}

// --- mixed varint widths: interleaved large and small values ---

TEST(PackedVarintVector, MixedVarintWidths) {
  std::vector<uint32_t> expected = {1, UINT32_MAX, 2, 128, 0, 2097152, 127, 16384};
  PackedVarintVector vec;
  for (uint32_t val : expected) {
    vec.push_back(val);
  }
  std::vector<uint32_t> seen;
  vec.for_each([&seen](uint32_t val) { seen.push_back(val); });
  EXPECT_EQ(seen, expected);

  std::vector<uint32_t> via_iter;
  for (auto val : vec) {
    via_iter.push_back(val);
  }
  EXPECT_EQ(via_iter, expected);
}

// --- copy-assign reuses heap buffer ---

TEST(PackedVarintVector, CopyAssignReusesHeapBuffer) {
  PackedVarintVector dest;
  for (int i = 0; i < 30; ++i) {
    dest.push_back(128);
  }
  PackedVarintVector source;
  source.push_back(1);
  source.push_back(2);

  dest = source;

  EXPECT_EQ(dest.count(), 2);
  std::vector<uint32_t> seen;
  dest.for_each([&seen](uint32_t val) { seen.push_back(val); });
  EXPECT_EQ(seen, std::vector<uint32_t>({1, 2}));

  dest.push_back(3);
  EXPECT_EQ(dest.count(), 3);
}
