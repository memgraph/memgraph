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

static constexpr uint64_t kStackSize = 1016;

TEST(Stack, EraseIfSomeElements) {
  memgraph::utils::Stack<int, kStackSize, false> stack;
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
  memgraph::utils::Stack<int, kStackSize, false> stack;
  for (int i = 0; i < 10; ++i) {
    stack.Push(i);
  }

  stack.EraseIf([](int) { return true; });

  // Verify stack is empty
  ASSERT_FALSE(stack.Pop().has_value());
}

TEST(Stack, EraseIfNoElements) {
  memgraph::utils::Stack<int, kStackSize, false> stack;
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
  memgraph::utils::Stack<int, kStackSize, false> stack;
  stack.EraseIf([](int val) { return val > 5; });
  ASSERT_FALSE(stack.Pop().has_value());
}

TEST(Stack, EraseIfWithDeleter) {
  memgraph::utils::Stack<int, kStackSize, false> stack;
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

TEST(Stack, EraseIfMultipleBlocks) {
  // Use smaller block size to easily create multiple blocks
  memgraph::utils::Stack<int, kStackSize, false> stack;
  // Push enough elements to fill more than one block (kStackSize per block)
  const int total_elements = 2500;  // Will create 3 blocks
  for (int i = 0; i < total_elements; ++i) {
    stack.Push(i);
  }

  // Erase all even numbers (should span multiple blocks)
  stack.EraseIf([](int val) { return val % 2 == 0; });

  // Verify only odd numbers remain
  std::vector<int> remaining;
  std::optional<int> item;
  while ((item = stack.Pop())) {
    remaining.push_back(*item);
    ASSERT_EQ(*item % 2, 1);
  }

  ASSERT_EQ(remaining.size(), total_elements / 2);
}

TEST(Stack, EraseIfMultipleBlocksPartial) {
  memgraph::utils::Stack<int, kStackSize, false> stack;
  const int total_elements = 2500;
  for (int i = 0; i < total_elements; ++i) {
    stack.Push(i);
  }

  // Erase elements in a specific range that spans multiple blocks
  stack.EraseIf([](int val) { return val >= 500 && val < 2000; });

  // Verify elements outside the range remain
  std::vector<int> remaining;
  std::optional<int> item;
  while ((item = stack.Pop())) {
    remaining.push_back(*item);
    ASSERT_TRUE(*item < 500 || *item >= 2000);
  }

  ASSERT_EQ(remaining.size(), 1000);  // 0-499 and 2000-2499
}

TEST(Stack, EraseIfMultipleBlocksWithDeleter) {
  memgraph::utils::Stack<int, kStackSize, false> stack;
  const int total_elements = 2500;
  for (int i = 0; i < total_elements; ++i) {
    stack.Push(i);
  }

  std::vector<int> deleted_values;
  stack.EraseIf([](int val) { return val % 3 == 0; }, [&deleted_values](int val) { deleted_values.push_back(val); });

  // Verify deletion callback was called for all erased elements
  ASSERT_EQ(deleted_values.size(), (total_elements + 2) / 3);  // Approximately 1/3

  // Verify remaining elements are not divisible by 3
  std::vector<int> remaining;
  std::optional<int> item;
  while ((item = stack.Pop())) {
    remaining.push_back(*item);
    ASSERT_NE(*item % 3, 0);
  }

  ASSERT_EQ(remaining.size() + deleted_values.size(), total_elements);
}

TEST(Stack, EraseIfAllElementsMultipleBlocks) {
  memgraph::utils::Stack<int, kStackSize, false> stack;
  const int total_elements = 2500;
  for (int i = 0; i < total_elements; ++i) {
    stack.Push(i);
  }

  // Erase all elements across multiple blocks
  stack.EraseIf([](int) { return true; });

  // Verify stack is empty
  ASSERT_FALSE(stack.Pop().has_value());
}

TEST(Stack, IteratorForward) {
  memgraph::utils::Stack<int, kStackSize, false> stack;
  for (int i = 0; i < 10; ++i) {
    stack.Push(i);
  }

  std::vector<int> iterated;
  for (auto it = stack.begin(); it != stack.end(); ++it) {
    iterated.push_back(*it);
  }

  ASSERT_EQ(iterated.size(), 10);
  for (size_t i = 0; i < iterated.size(); ++i) {
    ASSERT_EQ(iterated[i], 9 - static_cast<int>(i));
  }
}

TEST(Stack, IteratorBidirectional) {
  memgraph::utils::Stack<int, kStackSize, false> stack;
  for (int i = 0; i < 5; ++i) {
    stack.Push(i);
  }

  auto it = stack.begin();
  ASSERT_EQ(*it, 4);
  ++it;
  ASSERT_EQ(*it, 3);
  --it;
  ASSERT_EQ(*it, 4);
  ++it;
  ++it;
  ASSERT_EQ(*it, 2);
  --it;
  ASSERT_EQ(*it, 3);
}

TEST(Stack, IteratorEmptyStack) {
  memgraph::utils::Stack<int, kStackSize, false> stack;
  ASSERT_EQ(stack.begin(), stack.end());
}
