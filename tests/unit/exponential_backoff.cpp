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

#include "utils/exponential_backoff.hpp"

#include <gtest/gtest.h>
#include <chrono>
#include <range/v3/all.hpp>

class ExponentialBackoffTest : public ::testing::Test {
 protected:
  void SetUp() override {}

  void TearDown() override {}
};

TEST_F(ExponentialBackoffTest, TestTimings) {
  ExponentialBackoffInternals backoff_internals{std::chrono::milliseconds(1000), std::chrono::seconds(5)};

  ASSERT_EQ(std::chrono::milliseconds{1000}, backoff_internals.calculate_delay());
  ASSERT_EQ(std::chrono::milliseconds{2000}, backoff_internals.calculate_delay());
  ASSERT_EQ(std::chrono::milliseconds{4000}, backoff_internals.calculate_delay());
  ASSERT_EQ(std::chrono::milliseconds{5000}, backoff_internals.calculate_delay());
  ASSERT_EQ(std::chrono::milliseconds{5000}, backoff_internals.calculate_delay());

  for (auto const delay : ranges::views::iota(1, 1000) | ranges::views::transform([&backoff_internals](auto /*param*/) {
                            return backoff_internals.calculate_delay();
                          })) {
    ASSERT_EQ(delay, backoff_internals.calculate_delay());
  }
}
