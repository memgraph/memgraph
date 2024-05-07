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

#include "gtest/gtest.h"

#include "storage/v2/compact_vector.hpp"

#include <ranges>

using namespace memgraph::storage;
namespace rv = std::ranges::views;

///// TEST HELPERS

struct AlwaysAlloc {
  AlwaysAlloc() : AlwaysAlloc(-1) {}
  explicit AlwaysAlloc(int i) : ptr{std::make_unique<int>(i)} {}
  AlwaysAlloc(AlwaysAlloc &&) = default;
  AlwaysAlloc &operator=(AlwaysAlloc &&) = default;

  friend bool operator==(AlwaysAlloc const &lhs, AlwaysAlloc const &rhs) { return *lhs.ptr == *rhs.ptr; }

  explicit(false) operator int() const { return *ptr; }

 private:
  std::unique_ptr<int> ptr;
};

template <typename T>
struct CompactVectorCommon;

template <>
struct CompactVectorCommon<int> : public testing::Test {
  int default_value() const { return int{}; }
  int make_value(int i) const { return i; }
};

template <>
struct CompactVectorCommon<AlwaysAlloc> : public testing::Test {
  auto default_value() const -> AlwaysAlloc const & {
    static auto instance = AlwaysAlloc{};
    return instance;
  }
  AlwaysAlloc make_value(int i) const { return AlwaysAlloc{i}; }
};

auto maker(auto &src) {
  return [&src](int i) { return src.make_value(i); };
}

using MyTypes = ::testing::Types<int, AlwaysAlloc>;
TYPED_TEST_SUITE(CompactVectorCommon, MyTypes);

///// TESTS START HERE

TYPED_TEST(CompactVectorCommon, NeverInsertedTest) {
  CompactVector<TypeParam> vec;
  EXPECT_EQ(vec.size(), 0);
  EXPECT_TRUE(vec.empty());
  EXPECT_EQ(vec.capacity(), 0);
  EXPECT_EQ(vec.begin(), vec.end());
}

TYPED_TEST(CompactVectorCommon, BasicTest) {
  const int kMaxElements = 10;

  CompactVector<TypeParam> vec;

  for (auto i : rv::iota(0, kMaxElements)) {
    vec.push_back(this->make_value(i));
  }

  for (auto i : rv::iota(0, kMaxElements)) {
    EXPECT_EQ(std::as_const(vec)[i], i);
  }
}

TYPED_TEST(CompactVectorCommon, Clear) {
  constexpr auto kMaxElements = 10;
  CompactVector<TypeParam> vec;

  for (auto i : rv::iota(0, kMaxElements)) {
    vec.push_back(this->make_value(i));
  }

  auto const capacity = vec.capacity();
  EXPECT_NE(capacity, 0);
  EXPECT_EQ(std::as_const(vec).size(), kMaxElements);
  vec.clear();
  EXPECT_EQ(std::as_const(vec).capacity(), capacity);
  EXPECT_EQ(std::as_const(vec).size(), 0);
}

TYPED_TEST(CompactVectorCommon, Resize) {
  constexpr auto kSmallStorageSize = 5;
  constexpr auto kTwiceTheSmallStorage = 2 * kSmallStorageSize;
  CompactVector<TypeParam> vec;

  for (auto i : rv::iota(0, kTwiceTheSmallStorage)) {
    vec.push_back(this->make_value(i));
  }

  EXPECT_EQ(std::as_const(vec).size(), kTwiceTheSmallStorage);
  vec.resize(kSmallStorageSize);
  EXPECT_EQ(std::as_const(vec).size(), kSmallStorageSize);
  for (auto i : rv::iota(0, kSmallStorageSize)) {
    EXPECT_EQ(vec[i], i);
  }

  vec.resize(kTwiceTheSmallStorage);
  EXPECT_EQ(std::as_const(vec).size(), kTwiceTheSmallStorage);
  for (int i = kSmallStorageSize; i < kTwiceTheSmallStorage; ++i) {
    EXPECT_EQ(vec[i], this->default_value());
  }
}

TYPED_TEST(CompactVectorCommon, Reserve) {
  const int kMaxElements = 1000;
  CompactVector<TypeParam> vec;
  EXPECT_EQ(vec.capacity(), 0);
  vec.reserve(kMaxElements);
  EXPECT_EQ(vec.capacity(), kMaxElements);
  vec.reserve(1);
  EXPECT_EQ(vec.capacity(), kMaxElements);
}

TYPED_TEST(CompactVectorCommon, EraseFirst) {
  auto rng = rv::iota(0, 3) | rv::transform(maker(*this));
  auto vec = CompactVector<TypeParam>{rng.begin(), rng.end()};

  auto after_erase = vec.erase(vec.begin());
  EXPECT_EQ(vec.begin(), after_erase);

  EXPECT_EQ(2, vec.size());
  EXPECT_EQ(3, vec.capacity());
  EXPECT_EQ(this->make_value(1), vec[0]);
  EXPECT_EQ(this->make_value(2), vec[1]);
}

TYPED_TEST(CompactVectorCommon, EraseMiddle) {
  auto rng = rv::iota(0, 5) | rv::transform(maker(*this));
  auto vec = CompactVector<TypeParam>{rng.begin(), rng.end()};

  auto middle = std::next(vec.begin(), 2);
  auto after_erase = vec.erase(middle);
  EXPECT_EQ(std::next(vec.begin(), 2), after_erase);

  EXPECT_EQ(4, vec.size());
  EXPECT_EQ(5, vec.capacity());
  // first 2 are the same
  EXPECT_EQ(this->make_value(0), vec[0]);
  EXPECT_EQ(this->make_value(1), vec[1]);
  // next 2 are shifted down from original position
  EXPECT_EQ(this->make_value(3), vec[2]);
  EXPECT_EQ(this->make_value(4), vec[3]);
}

TYPED_TEST(CompactVectorCommon, EraseBack) {
  auto rng = rv::iota(0, 3) | rv::transform(maker(*this));
  auto vec = CompactVector<TypeParam>{rng.begin(), rng.end()};

  auto back = std::next(vec.begin(), 2);
  auto after_erase = vec.erase(back);
  EXPECT_EQ(vec.end(), after_erase);

  EXPECT_EQ(2, vec.size());
  EXPECT_EQ(3, vec.capacity());
  EXPECT_EQ(this->make_value(0), vec[0]);
  EXPECT_EQ(this->make_value(1), vec[1]);
}

TYPED_TEST(CompactVectorCommon, EraseRangeMiddle) {
  auto rng = rv::iota(0, 6) | rv::transform(maker(*this));
  auto vec = CompactVector<TypeParam>{rng.begin(), rng.end()};

  auto const first = std::next(vec.begin(), 2);
  auto const last = std::next(vec.begin(), 4);
  auto after_erase = vec.erase(first, last);
  EXPECT_EQ(std::next(vec.begin(), 2), after_erase);

  EXPECT_EQ(4, vec.size());
  EXPECT_EQ(6, vec.capacity());
  EXPECT_EQ(this->make_value(0), vec[0]);
  EXPECT_EQ(this->make_value(1), vec[1]);

  EXPECT_EQ(this->make_value(4), vec[2]);
  EXPECT_EQ(this->make_value(5), vec[3]);
}

TYPED_TEST(CompactVectorCommon, EraseRangeTillEnd) {
  auto rng = rv::iota(0, 6) | rv::transform(maker(*this));
  auto vec = CompactVector<TypeParam>{rng.begin(), rng.end()};

  auto const first = std::next(vec.begin(), 2);
  auto const last = vec.end();
  auto after_erase = vec.erase(first, last);
  EXPECT_EQ(vec.end(), after_erase);

  EXPECT_EQ(2, vec.size());
  EXPECT_EQ(6, vec.capacity());
  EXPECT_EQ(this->make_value(0), vec[0]);
  EXPECT_EQ(this->make_value(1), vec[1]);
}

TYPED_TEST(CompactVectorCommon, EraseRangeFull) {
  auto rng = rv::iota(0, 6) | rv::transform(maker(*this));
  auto vec = CompactVector<TypeParam>{rng.begin(), rng.end()};

  auto it = vec.erase(vec.begin(), vec.end());
  EXPECT_EQ(0, vec.size());
  EXPECT_EQ(it, vec.end());
}

TYPED_TEST(CompactVectorCommon, EraseEndIterator) {
  auto rng = rv::iota(0, 3) | rv::transform(maker(*this));
  auto vec = CompactVector<TypeParam>{rng.begin(), rng.end()};

  auto it = vec.erase(vec.end());
  EXPECT_EQ(3, vec.size());
  EXPECT_EQ(it, vec.end());
}

TYPED_TEST(CompactVectorCommon, EraseRangeEndIterator) {
  auto rng = rv::iota(0, 3) | rv::transform(maker(*this));
  auto vec = CompactVector<TypeParam>{rng.begin(), rng.end()};

  auto it = vec.erase(vec.end(), vec.end());
  EXPECT_EQ(3, vec.size());
  EXPECT_EQ(it, vec.end());
}

TYPED_TEST(CompactVectorCommon, EraseEndIteratorOnEmptyVector) {
  CompactVector<TypeParam> vec;

  auto it = vec.erase(vec.end());
  EXPECT_EQ(0, vec.size());
  EXPECT_EQ(it, vec.end());
}

TYPED_TEST(CompactVectorCommon, EraseRangeEndIteratorOnEmptyVector) {
  CompactVector<TypeParam> vec;

  auto it = vec.erase(vec.end(), vec.end());
  EXPECT_EQ(0, vec.size());
  EXPECT_EQ(it, vec.end());
}

TYPED_TEST(CompactVectorCommon, EmplaceBack) {
  auto rng = rv::iota(0, 3) | rv::transform(maker(*this));
  auto vec = CompactVector<TypeParam>{rng.begin(), rng.end()};
  vec.emplace_back(this->make_value(42));
  EXPECT_EQ(42, std::as_const(vec).back());
  EXPECT_EQ(4, vec.size());
}

TYPED_TEST(CompactVectorCommon, PushPopBack) {
  constexpr auto kElemNum = 10'000;
  auto rng = rv::iota(0, kElemNum) | rv::transform(maker(*this));
  auto vec = CompactVector<TypeParam>{rng.begin(), rng.end()};
  EXPECT_EQ(kElemNum, vec.size());
  for ([[maybe_unused]] auto _ : rv::iota(0, kElemNum)) {
    vec.pop_back();
  }
  EXPECT_EQ(0, vec.size());
  EXPECT_TRUE(vec.empty());
}

TYPED_TEST(CompactVectorCommon, Capacity) {
  constexpr auto kElemNum = 10'000;
  auto rng = rv::iota(0, kElemNum) | rv::transform(maker(*this));
  auto vec = CompactVector<TypeParam>{rng.begin(), rng.end()};

  EXPECT_EQ(kElemNum, vec.size());
  EXPECT_LE(kElemNum, vec.capacity());
}

TYPED_TEST(CompactVectorCommon, Empty) {
  constexpr auto kElemNum = 10'000;
  CompactVector<TypeParam> vec;
  EXPECT_TRUE(vec.empty());
  for (auto i : rv::iota(0, kElemNum)) {
    vec.push_back(this->make_value(i));
  }
  EXPECT_FALSE(vec.empty());
  for ([[maybe_unused]] auto _ : rv::iota(0, kElemNum)) {
    vec.pop_back();
  }
  EXPECT_TRUE(vec.empty());
}

TYPED_TEST(CompactVectorCommon, ReverseIteration) {
  auto rng = rv::iota(0, 10) | rv::transform(maker(*this));
  auto vec = CompactVector<TypeParam>{rng.begin(), rng.end()};

  auto rbegin = vec.rbegin();
  auto rend = vec.rend();
  EXPECT_EQ(std::distance(rbegin, rend), 10);

  // 9 down to 0
  auto expected = 9;
  for (auto it = rbegin; it != rend; ++it) {
    EXPECT_EQ(*it, expected);
    --expected;
  }
}

template <typename It, typename ConstIt>
concept CompatibleIterators = std::forward_iterator<It> && std::forward_iterator<ConstIt> &&
    requires(It it, ConstIt cit) {
  { it == cit } -> std::same_as<bool>;
  { it != cit } -> std::same_as<bool>;
  { cit == it } -> std::same_as<bool>;
  { cit != it } -> std::same_as<bool>;
};

using sut_t = CompactVector<int>;
static_assert(CompatibleIterators<sut_t::iterator, sut_t::const_iterator>);
static_assert(std::contiguous_iterator<sut_t::iterator>);
static_assert(std::contiguous_iterator<sut_t::const_iterator>);
