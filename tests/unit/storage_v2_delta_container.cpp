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

#include "storage/v2/delta_container.hpp"

#include <ranges>

using namespace memgraph::storage;

TEST(DeltaContainer, Empty) {
  auto container = delta_container{};
  EXPECT_TRUE(std::ranges::empty(container));
  EXPECT_EQ(container.size(), 0);
  EXPECT_EQ(std::distance(container.begin(), container.end()), 0);
}

TEST(DeltaContainer, Emplace) {
  auto container = delta_container{};
  container.emplace(Delta::DeleteObjectTag{}, (std::atomic<uint64_t> *)nullptr, 0);
  EXPECT_FALSE(std::ranges::empty(container));
  EXPECT_EQ(container.size(), 1);
  EXPECT_EQ(std::distance(container.begin(), container.end()), 1);
}

TEST(DeltaContainer, Move) {
  auto container = delta_container{};
  container.emplace(Delta::DeleteObjectTag{}, (std::atomic<uint64_t> *)nullptr, 0);
  auto container2 = std::move(container);

  EXPECT_TRUE(std::ranges::empty(container));
  EXPECT_EQ(container.size(), 0);
  EXPECT_EQ(std::distance(container.begin(), container.end()), 0);

  EXPECT_FALSE(std::ranges::empty(container2));
  EXPECT_EQ(container2.size(), 1);
  EXPECT_EQ(std::distance(container2.begin(), container2.end()), 1);
}
