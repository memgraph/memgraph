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

#include <algorithm>
#include <array>
#include <set>
#include <vector>

#include "test_rewriter_fixture.hpp"

namespace memgraph::planner::core {

using namespace test;

// ============================================================================
// Join Order Computation Tests
// ============================================================================
//
// These tests verify the join ordering algorithm used when building multi-pattern
// rules. The algorithm computes an optimal order for joining pattern matches to
// minimize Cartesian products.
//
// Note: These tests examine rule.join_steps() directly rather than running the
// rewriter, since they're testing the join order computation logic itself.

TEST(JoinOrder, PrefersHubPatterns) {
  // Tests that hub patterns (high connectivity) are processed before leaf patterns,
  // regardless of the declaration order of patterns.
  //
  // Given: X(?a), Y(?b), A(?a,?b), B(?b,?c), Z(?c), C(?c,?a)
  // Variable-sharing graph forms a triangle A-B-C with leaves X,Y,Z attached.
  //
  //       X(?a)           Y(?b)           Z(?c)
  //         \               |               /
  //          \              |              /
  //           A(?a,?b)----B(?b,?c)----C(?c,?a)
  //                  \      |      /
  //                   \     |     /
  //                    [triangle]
  //
  // Properties we verify for ALL permutations of pattern declaration order:
  // 1. Hub patterns (A,B,C with degree 2) are in the first 3 join steps
  // 2. Leaf patterns (X,Y,Z with degree 1) are in the last 3 join steps
  // 3. No Cartesian products: every step after first has shared variables

  // Pattern indices for tracking: 0-2 = hubs (A,B,C), 3-5 = leaves (X,Y,Z)
  constexpr std::size_t kIdxA = 0, kIdxB = 1, kIdxC = 2;
  constexpr std::size_t kIdxX = 3, kIdxY = 4, kIdxZ = 5;

  auto const patterns = std::array{
      TestPattern::build(Op::F, {Var{kVarA}, Var{kVarB}}),  // A(?a,?b)
      TestPattern::build(Op::F, {Var{kVarB}, Var{kVarC}}),  // B(?b,?c)
      TestPattern::build(Op::F, {Var{kVarC}, Var{kVarA}}),  // C(?c,?a)
      TestPattern::build(Op::F2, {Var{kVarA}}),             // X(?a)
      TestPattern::build(Op::F2, {Var{kVarB}}),             // Y(?b)
      TestPattern::build(Op::F2, {Var{kVarC}}),             // Z(?c)
  };

  // All permutations of pattern indices
  auto perm = std::array{0, 1, 2, 3, 4, 5};
  std::size_t permutation_count = 0;

  do {
    // Build rule with patterns in permuted order
    auto builder = TestRewriteRule::Builder{"perm_test"};
    for (auto idx : perm) {
      builder = std::move(builder).pattern(patterns[idx]);
    }
    auto rule = std::move(builder).apply([](TestRuleContext &, Match const &) {});

    // Get the computed join steps
    auto steps = rule.join_steps();
    ASSERT_EQ(steps.size(), 6) << "Permutation " << permutation_count;

    // Map join step position -> original pattern index
    // steps[i].pattern_index is the index in the permuted order
    // perm[steps[i].pattern_index] is the original index (0-5)
    std::vector<std::size_t> join_order;
    for (auto const &step : steps) {
      join_order.push_back(perm[step.pattern_index]);
    }

    // Property 1: First 3 steps should be hub patterns (indices 0,1,2)
    std::set first_three(join_order.begin(), join_order.begin() + 3);
    std::set expected_hubs = {kIdxA, kIdxB, kIdxC};
    EXPECT_EQ(first_three, expected_hubs) << "Permutation " << permutation_count << ": hubs should be processed first";

    // Property 2: Last 3 steps should be leaf patterns (indices 3,4,5)
    std::set last_three(join_order.begin() + 3, join_order.end());
    std::set expected_leaves = {kIdxX, kIdxY, kIdxZ};
    EXPECT_EQ(last_three, expected_leaves)
        << "Permutation " << permutation_count << ": leaves should be processed last";

    // Property 3: No Cartesian products - every step after first has shared vars
    for (std::size_t i = 1; i < steps.size(); ++i) {
      EXPECT_FALSE(steps[i].shared_vars.empty())
          << "Permutation " << permutation_count << ", step " << i << ": should have shared vars";
    }

    ++permutation_count;
  } while (std::ranges::next_permutation(perm).found);

  // 6! = 720 permutations
  EXPECT_EQ(permutation_count, 720);
}

}  // namespace memgraph::planner::core
