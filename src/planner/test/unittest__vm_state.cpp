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

// VMState unit tests: deduplication of yielded binding tuples.

#include <array>

#include <gtest/gtest.h>

#include "planner/pattern/vm/state.hpp"
#include "planner/pattern/vm/types.hpp"

namespace memgraph::planner::core {

using namespace pattern::vm;

namespace {

const std::array<SlotIdx, 2> kOrder2 = {SlotIdx{0}, SlotIdx{1}};
constexpr std::array<uint8_t, 2> kSlotToOrder2 = {0, 1};

auto TwoSlotState() -> VMState {
  VMState state;
  state.reset({.num_eclass_regs = 1, .num_enode_regs = 1, .binding_order = kOrder2, .slot_to_order = kSlotToOrder2});
  return state;
}

}  // namespace

TEST(PatternVM_State, BindingAlwaysSucceeds) {
  auto state = TwoSlotState();
  EXPECT_TRUE(state.try_bind(0, EClassId{100}));
  state.mark_seen(0);
  EXPECT_TRUE(state.try_bind(0, EClassId{100}));
  EXPECT_EQ(state.get(0), EClassId{100});
}

TEST(PatternVM_State, YieldsEachTupleOnce) {
  auto state = TwoSlotState();
  pattern::MatchArena arena;
  (void)state.try_bind(0, EClassId{100});
  (void)state.try_bind(1, EClassId{200});
  EXPECT_TRUE(state.yield_into(arena).has_value());
  EXPECT_FALSE(state.yield_into(arena).has_value());

  (void)state.try_bind(1, EClassId{201});
  EXPECT_TRUE(state.yield_into(arena).has_value());
  (void)state.try_bind(1, EClassId{200});
  EXPECT_FALSE(state.yield_into(arena).has_value());
}

TEST(PatternVM_State, TuplesCompareSlotBySlot) {
  auto state = TwoSlotState();
  pattern::MatchArena arena;
  (void)state.try_bind(0, EClassId{100});
  (void)state.try_bind(1, EClassId{200});
  EXPECT_TRUE(state.yield_into(arena).has_value());
  (void)state.try_bind(0, EClassId{200});
  (void)state.try_bind(1, EClassId{100});
  EXPECT_TRUE(state.yield_into(arena).has_value());
}

TEST(PatternVM_State, ARepeatedTupleLeavesTheArenaUnchanged) {
  auto state = TwoSlotState();
  pattern::MatchArena arena;
  (void)state.try_bind(0, EClassId{100});
  (void)state.try_bind(1, EClassId{200});
  ASSERT_TRUE(state.yield_into(arena).has_value());
  EXPECT_EQ(arena.size(), 2u);
  EXPECT_FALSE(state.yield_into(arena).has_value());
  EXPECT_EQ(arena.size(), 2u);
}

TEST(PatternVM_State, ResetForgetsYieldedTuples) {
  auto state = TwoSlotState();
  pattern::MatchArena arena;
  (void)state.try_bind(0, EClassId{100});
  (void)state.try_bind(1, EClassId{200});
  EXPECT_TRUE(state.yield_into(arena).has_value());
  state.reset({.num_eclass_regs = 1, .num_enode_regs = 1, .binding_order = kOrder2, .slot_to_order = kSlotToOrder2});
  (void)state.try_bind(0, EClassId{100});
  (void)state.try_bind(1, EClassId{200});
  EXPECT_TRUE(state.yield_into(arena).has_value());
}

}  // namespace memgraph::planner::core
