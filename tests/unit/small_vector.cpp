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

#include "storage/v2/small_vector.hpp"

#include <ranges>

using namespace memgraph::storage;
namespace rv = std::ranges::views;

///// TEST HELPERS

struct AlwaysAlloc {
  AlwaysAlloc() : AlwaysAlloc(-1) {}
  explicit AlwaysAlloc(int i) : ptr{std::make_unique<int>(i)} {}
  AlwaysAlloc(AlwaysAlloc const &other) : ptr{std::make_unique<int>(*other.ptr)} {}
  AlwaysAlloc &operator=(AlwaysAlloc const &other) {
    ptr = std::make_unique<int>(*other.ptr);
    return *this;
  }

  AlwaysAlloc(AlwaysAlloc &&) = default;
  AlwaysAlloc &operator=(AlwaysAlloc &&) = default;

  friend bool operator==(AlwaysAlloc const &lhs, AlwaysAlloc const &rhs) { return *lhs.ptr == *rhs.ptr; }

  explicit(false) operator int() const { return *ptr; }

 private:
  std::unique_ptr<int> ptr;
};

struct LargeType {
  LargeType() : LargeType(-1) {}
  explicit LargeType(int i) : values{i} {}
  LargeType(LargeType const &other) = default;
  LargeType &operator=(LargeType const &other) = default;

  friend bool operator==(LargeType const &lhs, LargeType const &rhs) { return lhs.values[0] == rhs.values[0]; }

  explicit(false) operator int() const { return values[0]; }

 private:
  static constexpr auto N = (8 / sizeof(int)) + 1;
  std::array<int, N> values;  // must be larger than 8B
};
static_assert(8 < sizeof(LargeType), "must be larger than 8B so that we don't use SBO");

template <typename T>
struct SmallVectorCommon;

template <>
struct SmallVectorCommon<int> : public testing::Test {
  int make_value(int i) const { return int{i}; }
};

template <>
struct SmallVectorCommon<AlwaysAlloc> : public testing::Test {
  AlwaysAlloc make_value(int i) const { return AlwaysAlloc{i}; }
};

template <>
struct SmallVectorCommon<LargeType> : public testing::Test {
  LargeType make_value(int i) const { return LargeType{i}; }
};

auto maker(auto &src) {
  return [&src](int i) { return src.make_value(i); };
}

template <typename T>
auto make_seq(auto *src, int ub) {
  auto rng = rv::iota(0, ub) | rv::transform(maker(*src));
  return small_vector<T>{rng.begin(), rng.end()};
}

using MyTypes = ::testing::Types<int, AlwaysAlloc, LargeType>;
TYPED_TEST_SUITE(SmallVectorCommon, MyTypes);

///// TESTS START HERE

TYPED_TEST(SmallVectorCommon, NeverInsertedTest) {
  small_vector<TypeParam> vec;
  EXPECT_EQ(vec.size(), 0);
  EXPECT_TRUE(vec.empty());
  EXPECT_EQ(vec.begin(), vec.end());
}

TYPED_TEST(SmallVectorCommon, BasicTest) {
  const int kMaxElements = 10;

  small_vector<TypeParam> vec;

  for (auto i : rv::iota(0, kMaxElements)) {
    vec.push_back(this->make_value(i));
  }

  for (auto i : rv::iota(0, kMaxElements)) {
    EXPECT_EQ(std::as_const(vec)[i], i);
  }
}

TYPED_TEST(SmallVectorCommon, Clear) {
  constexpr auto kMaxElements = 10;
  auto vec = make_seq<TypeParam>(this, kMaxElements);

  auto const capacity = vec.capacity();
  EXPECT_LE(vec.size(), capacity);
  EXPECT_EQ(std::as_const(vec).size(), kMaxElements);
  vec.clear();
  EXPECT_EQ(std::as_const(vec).capacity(), capacity);
  EXPECT_EQ(std::as_const(vec).size(), 0);
}

TYPED_TEST(SmallVectorCommon, Resize) {
  constexpr auto kSmallStorageSize = 5;
  constexpr auto kTwiceTheSmallStorage = 2 * kSmallStorageSize;
  auto vec = make_seq<TypeParam>(this, kTwiceTheSmallStorage);

  EXPECT_EQ(std::as_const(vec).size(), kTwiceTheSmallStorage);
  vec.resize(kSmallStorageSize);
  EXPECT_EQ(std::as_const(vec).size(), kSmallStorageSize);
  for (auto i : rv::iota(0, kSmallStorageSize)) {
    EXPECT_EQ(vec[i], i);
  }

  vec.resize(kTwiceTheSmallStorage);
  EXPECT_EQ(std::as_const(vec).size(), kTwiceTheSmallStorage);
}

TYPED_TEST(SmallVectorCommon, ResizeToSame) {
  auto vec = make_seq<TypeParam>(this, 10);
  vec.resize(10);
  EXPECT_EQ(std::as_const(vec).size(), 10);
}

TYPED_TEST(SmallVectorCommon, Reserve) {
  const int kMaxElements = 1000;
  small_vector<TypeParam> vec;
  vec.reserve(kMaxElements);
  EXPECT_EQ(vec.capacity(), kMaxElements);
  vec.reserve(1);
  EXPECT_EQ(vec.capacity(), kMaxElements);
}

TYPED_TEST(SmallVectorCommon, EmplaceAfterReserve) {
  constexpr auto initialCapacity = small_vector<TypeParam>::kSmallCapacity;

  small_vector<TypeParam> vec;
  EXPECT_EQ(vec.capacity(), initialCapacity);
  EXPECT_EQ(vec.size(), 0);

  // reserve
  auto new_capacity = initialCapacity + 100;
  vec.reserve(new_capacity);
  EXPECT_EQ(vec.capacity(), new_capacity);
  EXPECT_NE(vec.size(), new_capacity);

  // now emplace
  vec.emplace_back(this->make_value(1));
  EXPECT_EQ(vec.capacity(), new_capacity);
}

TYPED_TEST(SmallVectorCommon, PushBackCanIncreaseCapacity) {
  constexpr auto initialCapacity = small_vector<TypeParam>::kSmallCapacity;
  if constexpr (initialCapacity == 0) {
    small_vector<TypeParam> vec;
    vec.push_back(this->make_value(1));
    EXPECT_EQ(vec.capacity(), 1);
  }
}

TYPED_TEST(SmallVectorCommon, EmplaceBackCanIncreaseCapacity) {
  constexpr auto initialCapacity = small_vector<TypeParam>::kSmallCapacity;
  if constexpr (initialCapacity == 0) {
    small_vector<TypeParam> vec;
    vec.emplace_back(this->make_value(1));
    EXPECT_EQ(vec.capacity(), 1);
  }
}

TYPED_TEST(SmallVectorCommon, PushAfterReserve) {
  constexpr auto initialCapacity = small_vector<TypeParam>::kSmallCapacity;

  small_vector<TypeParam> vec;
  EXPECT_EQ(vec.capacity(), initialCapacity);

  // reserve
  auto new_capacity = initialCapacity + 100;
  vec.reserve(new_capacity);
  EXPECT_EQ(vec.capacity(), new_capacity);
  EXPECT_NE(vec.size(), new_capacity);

  // now push back
  vec.push_back(this->make_value(1));
  EXPECT_EQ(vec.capacity(), new_capacity);
}

TYPED_TEST(SmallVectorCommon, EraseFirst) {
  auto vec = make_seq<TypeParam>(this, 3);

  auto after_erase = vec.erase(vec.begin());
  EXPECT_EQ(vec.begin(), after_erase);

  EXPECT_EQ(2, vec.size());
  EXPECT_EQ(3, vec.capacity());
  EXPECT_EQ(this->make_value(1), vec[0]);
  EXPECT_EQ(this->make_value(2), vec[1]);
}

TYPED_TEST(SmallVectorCommon, EraseMiddle) {
  auto vec = make_seq<TypeParam>(this, 5);

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

TYPED_TEST(SmallVectorCommon, EraseBack) {
  auto vec = make_seq<TypeParam>(this, 3);

  auto back = std::next(vec.begin(), 2);
  auto after_erase = vec.erase(back);
  EXPECT_EQ(vec.end(), after_erase);

  EXPECT_EQ(2, vec.size());
  EXPECT_EQ(3, vec.capacity());
  EXPECT_EQ(this->make_value(0), vec[0]);
  EXPECT_EQ(this->make_value(1), vec[1]);
}

TYPED_TEST(SmallVectorCommon, EraseRangeMiddle) {
  auto vec = make_seq<TypeParam>(this, 6);

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

TYPED_TEST(SmallVectorCommon, EraseRangeTillEnd) {
  auto vec = make_seq<TypeParam>(this, 6);

  auto const first = std::next(vec.begin(), 2);
  auto const last = vec.end();
  auto after_erase = vec.erase(first, last);
  EXPECT_EQ(vec.end(), after_erase);

  EXPECT_EQ(2, vec.size());
  EXPECT_EQ(6, vec.capacity());
  EXPECT_EQ(this->make_value(0), vec[0]);
  EXPECT_EQ(this->make_value(1), vec[1]);
}

TYPED_TEST(SmallVectorCommon, EraseRangeFull) {
  auto vec = make_seq<TypeParam>(this, 6);

  auto it = vec.erase(vec.begin(), vec.end());
  EXPECT_EQ(0, vec.size());
  EXPECT_EQ(it, vec.end());
}

TYPED_TEST(SmallVectorCommon, EraseEndIterator) {
  auto vec = make_seq<TypeParam>(this, 3);

  auto it = vec.erase(vec.end());
  EXPECT_EQ(3, vec.size());
  EXPECT_EQ(it, vec.end());
}

TYPED_TEST(SmallVectorCommon, EraseRangeEndIterator) {
  auto vec = make_seq<TypeParam>(this, 3);

  auto it = vec.erase(vec.end(), vec.end());
  EXPECT_EQ(3, vec.size());
  EXPECT_EQ(it, vec.end());
}

TYPED_TEST(SmallVectorCommon, EraseEndIteratorOnEmptyVector) {
  small_vector<TypeParam> vec;

  auto it = vec.erase(vec.end());
  EXPECT_EQ(0, vec.size());
  EXPECT_EQ(it, vec.end());
}

TYPED_TEST(SmallVectorCommon, EraseRangeEndIteratorOnEmptyVector) {
  small_vector<TypeParam> vec;

  auto it = vec.erase(vec.end(), vec.end());
  EXPECT_EQ(0, vec.size());
  EXPECT_EQ(it, vec.end());
}

TYPED_TEST(SmallVectorCommon, EmplaceBack) {
  auto vec = make_seq<TypeParam>(this, 3);
  vec.emplace_back(this->make_value(42));
  EXPECT_EQ(42, std::as_const(vec).back());
  EXPECT_EQ(4, vec.size());
}

TYPED_TEST(SmallVectorCommon, PopBack) {
  constexpr auto kElemNum = 10;
  auto vec = make_seq<TypeParam>(this, kElemNum);
  EXPECT_EQ(kElemNum, vec.size());
  for ([[maybe_unused]] auto i : rv::iota(0, kElemNum) | rv::reverse) {
    vec.pop_back();
    EXPECT_EQ(i, vec.size());
  }
  EXPECT_EQ(0, vec.size());
  EXPECT_TRUE(vec.empty());
}

TYPED_TEST(SmallVectorCommon, Capacity) {
  constexpr auto kElemNum = 10;
  auto rng = rv::iota(0, kElemNum) | rv::transform(maker(*this));
  auto vec = small_vector<TypeParam>{rng.begin(), rng.end()};

  EXPECT_EQ(kElemNum, vec.size());
  EXPECT_LE(kElemNum, vec.capacity());
}

TYPED_TEST(SmallVectorCommon, Empty) {
  constexpr auto kElemNum = 10;
  small_vector<TypeParam> vec;
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

TYPED_TEST(SmallVectorCommon, ReverseIteration) {
  auto vec = make_seq<TypeParam>(this, 10);

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

TYPED_TEST(SmallVectorCommon, CopyConstruct) {
  for (auto src_size : rv::iota(0, 10)) {
    auto src = make_seq<TypeParam>(this, src_size);
    auto dst = src;  // copy construct
    EXPECT_EQ(dst, src);
    EXPECT_EQ(dst.size(), src_size);
  }
}

TYPED_TEST(SmallVectorCommon, CopyAssign) {
  for (auto src_size : rv::iota(0, 10)) {
    auto src = make_seq<TypeParam>(this, src_size);
    for (auto dst_size : rv::iota(0, 10)) {
      auto dst = make_seq<TypeParam>(this, dst_size);
      dst = src;  // copy assign
      EXPECT_EQ(dst, src);
      EXPECT_EQ(dst.size(), src_size);
    }
  }
}

TYPED_TEST(SmallVectorCommon, CopySelfAssign) {
  for (auto src_size : rv::iota(0, 10)) {
    auto src = make_seq<TypeParam>(this, src_size);
    src = src;  // copy assign
    EXPECT_EQ(src.size(), src_size);
  }
}

TYPED_TEST(SmallVectorCommon, MoveConstruct) {
  for (auto src_size : rv::iota(0, 10)) {
    auto src = make_seq<TypeParam>(this, src_size);
    auto dst = std::move(src);  // move construct
    EXPECT_EQ(dst.size(), src_size);
  }
}

TYPED_TEST(SmallVectorCommon, MoveAssign) {
  for (auto src_size : rv::iota(0, 10)) {
    for (auto dst_size : rv::iota(0, 10)) {
      auto src = make_seq<TypeParam>(this, src_size);
      auto dst = make_seq<TypeParam>(this, dst_size);
      dst = std::move(src);  // move assign
      EXPECT_EQ(dst.size(), src_size);
    }
  }
}

TYPED_TEST(SmallVectorCommon, MoveSelfAssign) {
  for (auto src_size : rv::iota(0, 10)) {
    auto src = make_seq<TypeParam>(this, src_size);
    src = std::move(src);  // move assign
    EXPECT_EQ(src.size(), src_size);
  }
}

TYPED_TEST(SmallVectorCommon, AtOutOfBounds) {
  // mutable at
  auto sut = make_seq<TypeParam>(this, 3);
  EXPECT_NO_THROW(sut.at(2));
  EXPECT_THROW(sut.at(3), std::out_of_range);

  // immutable at
  auto const &const_sut = sut;
  EXPECT_NO_THROW(auto x = const_sut.at(2));
  EXPECT_THROW(auto x = const_sut.at(3), std::out_of_range);
}

TYPED_TEST(SmallVectorCommon, PopBackWhenEmpty) {
  auto sut = make_seq<TypeParam>(this, 3);
  sut.pop_back();
  sut.pop_back();
  sut.pop_back();

  EXPECT_TRUE(sut.empty());
  EXPECT_EQ(sut.size(), 0);
  EXPECT_EQ(std::distance(sut.begin(), sut.end()), 0);

  sut.pop_back();

  EXPECT_TRUE(sut.empty());
  EXPECT_EQ(sut.size(), 0);
  EXPECT_EQ(std::distance(sut.begin(), sut.end()), 0);
}

TYPED_TEST(SmallVectorCommon, GrowShrink) {
  auto vec = small_vector<TypeParam>{};
  for (auto i : rv::iota(0, 30)) {
    vec.push_back(this->make_value(i));
    EXPECT_EQ(vec.back(), i);
    EXPECT_EQ(vec.size(), i + 1);
    for (auto j : rv::iota(0, i)) {
      EXPECT_EQ(vec[j], j);
    }
  }

  auto capacity = vec.capacity();

  for (auto i : rv::iota(1, 30) | rv::reverse) {
    vec.pop_back();
    EXPECT_EQ(vec.back(), i - 1);
    EXPECT_EQ(vec.size(), i);
    EXPECT_EQ(vec.capacity(), capacity);
    for (auto j : rv::iota(0, i)) {
      EXPECT_EQ(vec[j], j);
    }
  }
  vec.pop_back();
  EXPECT_TRUE(vec.empty());
}

template <typename It, typename ConstIt>
concept CompatibleIterators = std::forward_iterator<It> && std::forward_iterator<ConstIt> &&
    requires(It it, ConstIt cit) {
  { it == cit } -> std::same_as<bool>;
  { it != cit } -> std::same_as<bool>;
  { cit == it } -> std::same_as<bool>;
  { cit != it } -> std::same_as<bool>;
};

using sut_t = small_vector<int>;
static_assert(CompatibleIterators<sut_t::iterator, sut_t::const_iterator>);
static_assert(std::contiguous_iterator<sut_t::iterator>);
static_assert(std::contiguous_iterator<sut_t::const_iterator>);
