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

#include <gtest/gtest.h>

#include "utils/stack.hpp"

TEST(Stack, EraseIfSomeElements) {
  memgraph::utils::Stack<int, 1020, false> stack;
  for (int i = 0; i < 20; ++i) {
    stack.Push(i);
  }

  stack.EraseIf([](int val) { return val % 2 == 0; });

  // Verify only odd numbers remain
  std::vector<int> remaining;
  std::optional<int> item;
  while ((item = stack.Pop())) {
    remaining.push_back(*item);
  }

  ASSERT_EQ(remaining.size(), 10);
  for (int val : remaining) {
    ASSERT_EQ(val % 2, 1);
  }
}

TEST(Stack, EraseIfAllElements) {
  memgraph::utils::Stack<int, 1020, false> stack;
  for (int i = 0; i < 10; ++i) {
    stack.Push(i);
  }

  stack.EraseIf([](int) { return true; });

  // Verify stack is empty
  ASSERT_FALSE(stack.Pop().has_value());
}

TEST(Stack, EraseIfNoElements) {
  memgraph::utils::Stack<int, 1020, false> stack;
  for (int i = 0; i < 10; ++i) {
    stack.Push(i);
  }

  stack.EraseIf([](int val) { return val < 0; });

  // Verify all elements remain
  int count = 0;
  std::optional<int> item;
  while ((item = stack.Pop())) {
    ++count;
  }

  ASSERT_EQ(count, 10);
}

TEST(Stack, EraseIfEmptyStack) {
  memgraph::utils::Stack<int, 1020, false> stack;
  stack.EraseIf([](int val) { return val > 5; });
  ASSERT_FALSE(stack.Pop().has_value());
}

TEST(Stack, EraseIfWithDeleter) {
  memgraph::utils::Stack<int, 1020, false> stack;
  for (int i = 0; i < 10; ++i) {
    stack.Push(i);
  }

  std::vector<int> deleted_values;
  stack.EraseIf([](int val) { return val % 2 == 0; }, [&deleted_values](int val) { deleted_values.push_back(val); });

  // Verify deletion callback was called for even numbers
  ASSERT_EQ(deleted_values.size(), 5);
  for (int val : deleted_values) {
    ASSERT_EQ(val % 2, 0);
  }

  // Verify only odd numbers remain
  std::vector<int> remaining;
  std::optional<int> item;
  while ((item = stack.Pop())) {
    remaining.push_back(*item);
  }

  ASSERT_EQ(remaining.size(), 5);
  for (int val : remaining) {
    ASSERT_EQ(val % 2, 1);
  }
}
