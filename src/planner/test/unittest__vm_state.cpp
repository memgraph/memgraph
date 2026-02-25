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

// VMState unit tests: direct state manipulation for deduplication logic.

#include <gtest/gtest.h>

#include "planner/pattern/vm/state.hpp"

namespace memgraph::planner::core {

using namespace vm;

// ============================================================================
// VMState Deduplication Tests
// ============================================================================

// Test: Global deduplication across candidates via bind-time dedup.
TEST(VMStateTest, GlobalDeduplicationAcrossCandidates) {
  VMState state;
  state.reset(2, 1, 1);  // 2 slots, 1 eclass reg, 1 enode reg

  // Simulate first "candidate" execution:
  EXPECT_TRUE(state.try_bind_dedup(0, EClassId{100})) << "First bind of slot 0 should succeed";
  EXPECT_TRUE(state.try_bind_dedup(1, EClassId{200})) << "First bind of slot 1 should succeed";

  // Mark slots as seen
  state.mark_seen(1);
  state.mark_seen(0);

  // Simulate second candidate with same values
  state.pc = 0;

  // Same value should be deduplicated (global dedup)
  bool bind0_result = state.try_bind_dedup(0, EClassId{100});
  EXPECT_FALSE(bind0_result) << "Second bind of slot 0 with same value should be deduplicated";
}

// Test: Verify correct behavior when slot value changes
TEST(VMStateTest, BindSlotValueChange) {
  VMState state;
  state.reset(2, 1, 1);

  // First candidate: (100, 200)
  EXPECT_TRUE(state.try_bind_dedup(0, EClassId{100})) << "First bind of slot 0 should succeed";
  EXPECT_TRUE(state.try_bind_dedup(1, EClassId{200})) << "First bind of slot 1 should succeed";

  state.pc = 0;

  // Different value for slot 0 - should clear seen_per_slot[1]
  EXPECT_TRUE(state.try_bind_dedup(0, EClassId{101})) << "New value for slot 0 should succeed";
  // Same value 200 for slot 1, but prefix changed
  EXPECT_TRUE(state.try_bind_dedup(1, EClassId{200})) << "Same slot 1 value with different prefix should succeed";
}

// Test: Deduplication within same candidate
TEST(VMStateTest, DeduplicationWithinCandidate) {
  VMState state;
  state.reset(2, 1, 1);

  EXPECT_TRUE(state.try_bind_dedup(0, EClassId{100})) << "First bind of slot 0 should succeed";
  EXPECT_TRUE(state.try_bind_dedup(1, EClassId{200})) << "First bind of slot 1 should succeed";

  state.mark_seen(0);

  // Same value should be deduplicated after mark_seen
  EXPECT_FALSE(state.try_bind_dedup(0, EClassId{100})) << "Second bind of slot 0 (same value) should be deduplicated";
}

// Test: Binding order clears correct seen sets
TEST(VMStateTest, BindingOrderClearsSlotsCorrectly) {
  // Simulate binding order [1, 0, 2]
  std::vector<std::vector<uint8_t>> slots_after_storage = {
      {2},     // slot 0: clear slot 2 when changed
      {0, 2},  // slot 1: clear slots 0 and 2 when changed
      {}       // slot 2: clear nothing (last in order)
  };

  VMState state;
  state.reset(3, 1, 1, [&slots_after_storage](std::size_t slot) -> std::span<uint8_t const> {
    return slots_after_storage[slot];
  });

  // Bind in order: slot 1, slot 0, slot 2
  EXPECT_TRUE(state.try_bind_dedup(1, EClassId{100})) << "First bind of slot 1 should succeed";
  EXPECT_TRUE(state.try_bind_dedup(0, EClassId{200})) << "First bind of slot 0 should succeed";
  EXPECT_TRUE(state.try_bind_dedup(2, EClassId{300})) << "First bind of slot 2 should succeed";

  state.mark_seen(2);

  // Same slot 2 value should be deduplicated
  EXPECT_FALSE(state.try_bind_dedup(2, EClassId{300})) << "Same slot 2 value should be deduplicated";

  // Change slot 0 - should clear seen_per_slot[2]
  EXPECT_TRUE(state.try_bind_dedup(0, EClassId{201})) << "New value for slot 0 should succeed";

  // Re-bind slot 2 with same value - should now succeed
  EXPECT_TRUE(state.try_bind_dedup(2, EClassId{300})) << "After prefix change, same slot[2] value should be new";

  state.mark_seen(2);

  // Change slot 1 - should clear seen_per_slot[0] and seen_per_slot[2]
  EXPECT_TRUE(state.try_bind_dedup(1, EClassId{101})) << "New value for slot 1 should succeed";

  // Original values should be new again
  EXPECT_TRUE(state.try_bind_dedup(0, EClassId{200})) << "Original slot 0 value should be new after slot 1 changed";
  EXPECT_TRUE(state.try_bind_dedup(2, EClassId{300})) << "After root prefix change, tuple should be new";
}

}  // namespace memgraph::planner::core
