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

import memgraph.planner.core.eids;

namespace memgraph::planner::core {

using namespace vm;

// ============================================================================
// VMState Deduplication Tests
// ============================================================================

// Identity binding order for simple 2-slot tests
constexpr std::array<uint8_t, 2> kIdentityOrder2 = {0, 1};

// Test: Global deduplication across candidates via bind-time dedup.
TEST(VMStateTest, GlobalDeduplicationAcrossCandidates) {
  VMState state;
  state.reset(
      {.num_eclass_regs = 1, .num_enode_regs = 1, .binding_order = kIdentityOrder2, .slot_to_order = kIdentityOrder2});

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
  state.reset(
      {.num_eclass_regs = 1, .num_enode_regs = 1, .binding_order = kIdentityOrder2, .slot_to_order = kIdentityOrder2});

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
  state.reset(
      {.num_eclass_regs = 1, .num_enode_regs = 1, .binding_order = kIdentityOrder2, .slot_to_order = kIdentityOrder2});

  EXPECT_TRUE(state.try_bind_dedup(0, EClassId{100})) << "First bind of slot 0 should succeed";
  EXPECT_TRUE(state.try_bind_dedup(1, EClassId{200})) << "First bind of slot 1 should succeed";

  state.mark_seen(0);

  // Same value should be deduplicated after mark_seen
  EXPECT_FALSE(state.try_bind_dedup(0, EClassId{100})) << "Second bind of slot 0 (same value) should be deduplicated";
}

// Test: Binding order clears correct seen sets
TEST(VMStateTest, BindingOrderClearsSlotsCorrectly) {
  // Binding order [1, 0, 2] means: slot 1 first, then slot 0, then slot 2
  // When slot 1 changes (pos 0), clear slots at positions 1, 2 = slots 0, 2
  // When slot 0 changes (pos 1), clear slots at positions 2 = slot 2
  // When slot 2 changes (pos 2), clear nothing (last in order)
  std::vector<uint8_t> binding_order = {1, 0, 2};
  std::vector<uint8_t> slot_to_order = {1, 0, 2};  // slot 0 at pos 1, slot 1 at pos 0, slot 2 at pos 2

  VMState state;
  state.reset(
      {.num_eclass_regs = 1, .num_enode_regs = 1, .binding_order = binding_order, .slot_to_order = slot_to_order});

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

// Test: Complex 4-slot permutation where binding order differs significantly from slot indices
// Simulates: ?w:0 ?x:1 ?y:2 ?z:3 with binding order ?x, ?y, ?w, ?z
//   binding_order (pos -> slot): [1, 2, 0, 3]
//   slot_to_order (slot -> pos): [2, 0, 1, 3]
// Expected clearing behavior:
//   When ?x (slot 1, pos 0) changes: clear ?y, ?w, ?z (slots 2, 0, 3)
//   When ?y (slot 2, pos 1) changes: clear ?w, ?z (slots 0, 3)
//   When ?w (slot 0, pos 2) changes: clear ?z (slot 3)
//   When ?z (slot 3, pos 3) changes: clear nothing
TEST(VMStateTest, FourSlotComplexPermutation) {
  std::vector<uint8_t> binding_order = {1, 2, 0, 3};  // pos -> slot
  std::vector<uint8_t> slot_to_order = {2, 0, 1, 3};  // slot -> pos

  VMState state;
  state.reset(
      {.num_eclass_regs = 1, .num_enode_regs = 1, .binding_order = binding_order, .slot_to_order = slot_to_order});

  // Bind all slots in binding order: ?x, ?y, ?w, ?z
  EXPECT_TRUE(state.try_bind_dedup(1, EClassId{10}));  // ?x
  EXPECT_TRUE(state.try_bind_dedup(2, EClassId{20}));  // ?y
  EXPECT_TRUE(state.try_bind_dedup(0, EClassId{30}));  // ?w
  EXPECT_TRUE(state.try_bind_dedup(3, EClassId{40}));  // ?z

  // Mark all as seen
  state.mark_seen(3);
  state.mark_seen(0);
  state.mark_seen(2);
  state.mark_seen(1);

  // All should now be deduplicated
  EXPECT_FALSE(state.try_bind_dedup(1, EClassId{10}));
  EXPECT_FALSE(state.try_bind_dedup(2, EClassId{20}));
  EXPECT_FALSE(state.try_bind_dedup(0, EClassId{30}));
  EXPECT_FALSE(state.try_bind_dedup(3, EClassId{40}));

  // Change ?w (slot 0, pos 2) - should only clear ?z (slot 3)
  EXPECT_TRUE(state.try_bind_dedup(0, EClassId{31}));

  // ?x and ?y should still be deduplicated (bound before ?w)
  EXPECT_FALSE(state.try_bind_dedup(1, EClassId{10}));
  EXPECT_FALSE(state.try_bind_dedup(2, EClassId{20}));
  // ?z should now be fresh (bound after ?w)
  EXPECT_TRUE(state.try_bind_dedup(3, EClassId{40}));

  state.mark_seen(3);
  state.mark_seen(0);

  // Change ?y (slot 2, pos 1) - should clear ?w (slot 0) and ?z (slot 3)
  EXPECT_TRUE(state.try_bind_dedup(2, EClassId{21}));

  // ?x should still be deduplicated
  EXPECT_FALSE(state.try_bind_dedup(1, EClassId{10}));
  // ?w and ?z should be fresh
  EXPECT_TRUE(state.try_bind_dedup(0, EClassId{30}));
  EXPECT_TRUE(state.try_bind_dedup(3, EClassId{40}));

  state.mark_seen(3);
  state.mark_seen(0);
  state.mark_seen(2);

  // Change ?x (slot 1, pos 0) - should clear ?y, ?w, ?z (all others)
  EXPECT_TRUE(state.try_bind_dedup(1, EClassId{11}));

  // All should be fresh
  EXPECT_TRUE(state.try_bind_dedup(2, EClassId{20}));
  EXPECT_TRUE(state.try_bind_dedup(0, EClassId{30}));
  EXPECT_TRUE(state.try_bind_dedup(3, EClassId{40}));
}

}  // namespace memgraph::planner::core
