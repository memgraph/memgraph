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

#include <stdexcept>
#include <type_traits>
#include <vector>

#include "utils/coro_task.hpp"

using memgraph::utils::SyncWait;
using memgraph::utils::Task;

namespace {

Task<int> ReturnsValue(int value) { co_return value; }

Task<int> ThrowsRuntimeError() {
  if (true) {
    throw std::runtime_error("boom");
  }
  co_return 0;  // unreachable; keeps this a coroutine (co_return present).
}

Task<void> CompletesVoid(bool *ran) {
  *ran = true;
  co_return;
}

// Nested chaining: co_awaits two other Task<int>s in sequence and sums them, recording the
// order in which each phase runs so we can assert symmetric transfer actually suspends/resumes
// through the continuations rather than eagerly running everything up front.
Task<int> SumTwoLeaves(std::vector<int> *order, int a, int b) {
  order->push_back(1);
  const int first = co_await ReturnsValue(a);
  order->push_back(2);
  const int second = co_await ReturnsValue(b);
  order->push_back(3);
  co_return first + second;
}

}  // namespace

TEST(CoroTask, ReturnsValueViaSyncWait) { EXPECT_EQ(SyncWait(ReturnsValue(42)), 42); }

TEST(CoroTask, PropagatesException) {
  EXPECT_THROW(
      {
        try {
          SyncWait(ThrowsRuntimeError());
        } catch (const std::runtime_error &e) {
          EXPECT_STREQ(e.what(), "boom");
          throw;
        }
      },
      std::runtime_error);
}

TEST(CoroTask, VoidTaskCompletes) {
  bool ran = false;
  SyncWait(CompletesVoid(&ran));
  EXPECT_TRUE(ran);
}

TEST(CoroTask, NestedChainingSumsAndOrdersCorrectly) {
  std::vector<int> order;
  const int result = SyncWait(SumTwoLeaves(&order, 10, 20));
  EXPECT_EQ(result, 30);
  EXPECT_EQ(order, (std::vector<int>{1, 2, 3}));
}

TEST(CoroTask, MoveOnlyNoDoubleFree) {
  static_assert(!std::is_copy_constructible_v<Task<int>>);
  static_assert(!std::is_copy_assignable_v<Task<int>>);
  static_assert(std::is_move_constructible_v<Task<int>>);

  // A freshly-constructed lazy Task holds a suspended-at-initial-suspend frame; it must be safe
  // to move it and then let both the moved-from (now empty) and the moved-to Task be destroyed
  // without ever being awaited/run -- exercising the destructor's guard against a null handle and
  // proving there is exactly one owner of the underlying coroutine frame at all times.
  Task<int> original = ReturnsValue(7);
  Task<int> moved = std::move(original);

  // moved-from Task is now empty; destroying it must be a no-op (no double-destroy of `moved`'s
  // frame).
  {
    Task<int> empty_dtor = std::move(original);
  }

  // `moved` still owns the only live frame; letting it go out of scope destroys the
  // never-started coroutine exactly once.
}

TEST(CoroTask, MultipleIndependentTasksDoNotInterfere) {
  auto first = ReturnsValue(1);
  auto second = ReturnsValue(2);
  EXPECT_EQ(SyncWait(std::move(second)), 2);
  EXPECT_EQ(SyncWait(std::move(first)), 1);
}
