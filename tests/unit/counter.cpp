// Copyright 2025 Memgraph Ltd.
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

import memgraph.utils.counter;

using memgraph::utils::ResettableAtomicCounter;
using memgraph::utils::ResettableCounter;

TEST(Counter, RuntimeCounterInc) {
  auto cnt = ResettableAtomicCounter{2};
  ASSERT_FALSE(cnt(1));
  ASSERT_TRUE(cnt(1));
  ASSERT_FALSE(cnt(1));
  ASSERT_TRUE(cnt(1));
}

TEST(Counter, RuntimeCounterLarge) {
  auto cnt = ResettableAtomicCounter{100};
  ASSERT_FALSE(cnt(50));
  ASSERT_FALSE(cnt(40));
  ASSERT_TRUE(cnt(10));
}

TEST(Counter, RuntimeCounterLarge2) {
  auto cnt = ResettableAtomicCounter{100};
  ASSERT_TRUE(cnt(101));
}

TEST(Counter, RuntimeCounterLarge3) {
  auto cnt = ResettableAtomicCounter{100};
  ASSERT_FALSE(cnt(20));
  ASSERT_FALSE(cnt(30));
  ASSERT_FALSE(cnt(30));
  ASSERT_FALSE(cnt(19));
  ASSERT_TRUE(cnt(2));
}

TEST(Counter, CompiletimeCounter) {
  auto cnt = ResettableCounter(2);
  ASSERT_FALSE(cnt());
  ASSERT_TRUE(cnt());
  ASSERT_FALSE(cnt());
  ASSERT_TRUE(cnt());
}
