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

#include "utils/static_vector.hpp"

#include <string>

using namespace memgraph::utils;

static_assert(std::is_trivially_destructible_v<static_vector<int, 1>>);
static_assert(!std::is_trivially_destructible_v<static_vector<std::string, 1>>);

static_assert(std::contiguous_iterator<static_vector_iterator<int>>);

TEST(StaticVectorTest, DefaultConstruction) {
  constexpr auto N = 3;
  using sut_t = static_vector<int, N>;
  static_assert(std::is_nothrow_default_constructible_v<sut_t>, "Must be default constructible.");
  sut_t vec;
  EXPECT_EQ(vec.size(), 0);
  EXPECT_EQ(vec.capacity(), N);
}

TEST(StaticVector, CopyFromArray) {
  constexpr auto data = std::array{1, 2, 3};
  auto vec = static_vector{data};
  EXPECT_EQ(vec, data);
  EXPECT_EQ(vec.size(), data.size());
  EXPECT_EQ(vec.capacity(), data.size());
  EXPECT_EQ(vec[1], data[1]);
}

TEST(StaticVector, Copy) {
  constexpr auto data = std::array{1, 2, 3};
  auto vec = static_vector{data};
  auto cpy = vec;
  EXPECT_EQ(vec, cpy);
  EXPECT_EQ(cpy.size(), vec.size());
  EXPECT_EQ(cpy.capacity(), vec.size());
  EXPECT_EQ(cpy[1], vec[1]);
}

TEST(StaticVector, Emplace) {
  auto vec = static_vector<int, 5>{};
  vec.emplace(42);
  EXPECT_EQ(vec.size(), 1);
  EXPECT_EQ(vec.capacity(), 5);
  EXPECT_EQ(vec[0], 42);
}
