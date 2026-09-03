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

#include "gtest/gtest.h"

#include "utils/priority_thread_pool.hpp"

namespace {
constexpr auto kGroupSize = 64U;
constexpr auto kMaxN = memgraph::utils::HotMask::kMaxElements;
}  // namespace

TEST(HotMask, OneElement) {
  using namespace memgraph::utils;
  HotMask mask(1);
  // At the start no hot elements
  EXPECT_FALSE(mask.GetHotElement());
  // Using oob ids will cause a DMG_ASSERT, not testing it here
  mask.Reset(0);
  EXPECT_FALSE(mask.GetHotElement());
  mask.Set(0);
  EXPECT_EQ(mask.GetHotElement(), 0);
  // Getting will reset it
  EXPECT_FALSE(mask.GetHotElement());

  mask.Set(0);
  mask.Reset(0);
  EXPECT_FALSE(mask.GetHotElement());
}

TEST(HotMask, WholeGroup) {
  using namespace memgraph::utils;
  HotMask mask(kGroupSize);
  // At the start no hot elements
  EXPECT_FALSE(mask.GetHotElement());

  // Using oob ids will cause a DMG_ASSERT, not testing it here
  mask.Reset(0);
  EXPECT_FALSE(mask.GetHotElement());
  mask.Set(0);
  EXPECT_EQ(mask.GetHotElement(), 0);
  // Getting will reset it
  EXPECT_FALSE(mask.GetHotElement());

  mask.Set(0);
  mask.Set(1);
  mask.Reset(0);
  EXPECT_EQ(mask.GetHotElement(), 1);
  mask.Set(1);
  mask.Set(0);
  mask.Reset(0);
  EXPECT_EQ(mask.GetHotElement(), 1);
  mask.Set(0);
  mask.Set(1);
  EXPECT_EQ(mask.GetHotElement(), 0);
  EXPECT_EQ(mask.GetHotElement(), 1);
  mask.Set(1);
  mask.Set(0);
  EXPECT_EQ(mask.GetHotElement(), 0);
  EXPECT_EQ(mask.GetHotElement(), 1);

  // Fill the whole group and check the get order
  for (uint16_t i = 0; i < kGroupSize; ++i) mask.Set(i);
  for (uint16_t i = 0; i < kGroupSize; ++i) EXPECT_EQ(mask.GetHotElement(), i);
  EXPECT_FALSE(mask.GetHotElement());  // All read

  // Fill the whole group, reset half and check the get order
  for (uint16_t i = 0; i < kGroupSize; ++i) mask.Set(i);
  for (uint16_t i = 0; i < kGroupSize / 2; ++i) mask.Reset(i * 2);
  for (uint16_t i = 0; i < kGroupSize / 2; ++i) EXPECT_EQ(mask.GetHotElement(), i * 2 + 1);
  EXPECT_FALSE(mask.GetHotElement());  // All read

  // Fill every other element in group and check the get order
  for (uint16_t i = 0; i < kGroupSize / 2; ++i) mask.Set(i * 2);
  for (uint16_t i = 0; i < kGroupSize / 2; ++i) EXPECT_EQ(mask.GetHotElement(), i * 2);
  EXPECT_FALSE(mask.GetHotElement());  // All read
}

TEST(HotMask, MultipleGroups) {
  using namespace memgraph::utils;
  constexpr auto kN = 200U;
  HotMask mask(kN);
  // At the start no hot elements
  EXPECT_FALSE(mask.GetHotElement());

  // Fill the second group and check the get order
  for (uint16_t i = 0; i < kGroupSize; ++i) mask.Set(i + kGroupSize);
  for (uint16_t i = 0; i < kGroupSize; ++i) EXPECT_EQ(mask.GetHotElement(), i + kGroupSize);
  EXPECT_FALSE(mask.GetHotElement());  // All read

  // Fill the whole mask, reset half and check the get order
  for (uint16_t i = 0; i < kN; ++i) mask.Set(i);
  for (uint16_t i = 0; i < kN / 2; ++i) mask.Reset(i * 2);
  for (uint16_t i = 0; i < kN / 2; ++i) EXPECT_EQ(mask.GetHotElement(), i * 2 + 1);
  EXPECT_FALSE(mask.GetHotElement());  // All read

  // Fill every other element and check the get order
  for (uint16_t i = 0; i < kN / 2; ++i) mask.Set(i * 2);
  for (uint16_t i = 0; i < kN / 2; ++i) EXPECT_EQ(mask.GetHotElement(), i * 2);
  EXPECT_FALSE(mask.GetHotElement());  // All read
}

TEST(HotMask, Full) {
  using namespace memgraph::utils;
  HotMask mask(kMaxN);
  // At the start no hot elements
  EXPECT_FALSE(mask.GetHotElement());

  // Fill the last 3 groups and check the get order
  for (uint16_t i = 0; i < 3 * kGroupSize; ++i) mask.Set(i + kMaxN - 3 * kGroupSize);
  for (uint16_t i = 0; i < 3 * kGroupSize; ++i) EXPECT_EQ(mask.GetHotElement(), i + kMaxN - 3 * kGroupSize);
  EXPECT_FALSE(mask.GetHotElement());  // All read

  // Fill the whole mask, reset half and check the get order
  for (uint16_t i = 0; i < kMaxN; ++i) mask.Set(i);
  for (uint16_t i = 0; i < kMaxN / 2; ++i) mask.Reset(i * 2);
  for (uint16_t i = 0; i < kMaxN / 2; ++i) EXPECT_EQ(mask.GetHotElement(), i * 2 + 1);
  EXPECT_FALSE(mask.GetHotElement());  // All read

  // Fill every other element and check the get order
  for (uint16_t i = 0; i < kMaxN / 2; ++i) mask.Set(i * 2);
  for (uint16_t i = 0; i < kMaxN / 2; ++i) EXPECT_EQ(mask.GetHotElement(), i * 2);
  EXPECT_FALSE(mask.GetHotElement());  // All read
}

TEST(HotMask, Count) {
  using namespace memgraph::utils;
  constexpr auto kN = 200U;  // spans multiple 64-bit groups
  HotMask mask(kN);
  EXPECT_EQ(mask.Count(), 0U);

  // Set bits across group boundaries.
  mask.Set(0);
  mask.Set(63);   // end of group 0
  mask.Set(64);   // start of group 1
  mask.Set(199);  // last element
  EXPECT_EQ(mask.Count(), 4U);

  // Setting an already-set bit is idempotent.
  mask.Set(64);
  EXPECT_EQ(mask.Count(), 4U);

  // Reset lowers the count.
  mask.Reset(63);
  EXPECT_EQ(mask.Count(), 3U);

  // GetHotElement clears the bit it returns, so it also lowers the count.
  EXPECT_TRUE(mask.GetHotElement().has_value());
  EXPECT_EQ(mask.Count(), 2U);

  // Draining the rest returns the count to zero.
  while (mask.GetHotElement()) {
  }
  EXPECT_EQ(mask.Count(), 0U);

  // Every element set -> Count() == size.
  for (uint16_t i = 0; i < kN; ++i) mask.Set(i);
  EXPECT_EQ(mask.Count(), kN);
}
