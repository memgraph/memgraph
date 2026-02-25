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

#include <gtest/gtest.h>

#include "planner/pattern/match.hpp"

import memgraph.planner.core.eids;

using namespace memgraph::planner::core;

// ============================================================================
// MatchArena Tests
// ============================================================================
//
// MatchArena is a monotonic allocator for storing match bindings efficiently.

TEST(MatchArena, InternAndRetrieve) {
  MatchArena arena;

  std::array bindings = {EClassId{10}, EClassId{20}, EClassId{30}};
  auto offset = arena.intern(bindings);

  EXPECT_EQ(arena.get(offset, 0), EClassId{10});
  EXPECT_EQ(arena.get(offset, 1), EClassId{20});
  EXPECT_EQ(arena.get(offset, 2), EClassId{30});
}

TEST(MatchArena, MultipleAllocationsAreIndependent) {
  MatchArena arena;

  auto offset1 = arena.intern(std::array{EClassId{1}, EClassId{2}});
  auto offset2 = arena.intern(std::array{EClassId{10}, EClassId{20}, EClassId{30}});

  // Separate allocations
  EXPECT_NE(offset1, offset2);

  // Each retrieves correct values
  EXPECT_EQ(arena.get(offset1, 0), EClassId{1});
  EXPECT_EQ(arena.get(offset2, 2), EClassId{30});
}

TEST(MatchArena, ClearResetsForReuse) {
  MatchArena arena;

  arena.intern(std::array{EClassId{1}, EClassId{2}, EClassId{3}});
  EXPECT_EQ(arena.size(), 3);

  arena.clear();
  EXPECT_EQ(arena.size(), 0);

  // Can reuse after clear
  arena.intern(std::array{EClassId{10}});
  EXPECT_EQ(arena.size(), 1);
}
