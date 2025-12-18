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

#include <algorithm>
#include <chrono>
#include <random>

#include <gtest/gtest.h>
#include <mg_generate.hpp>
#include <mg_graph.hpp>

#include "algorithm/cycles.hpp"

namespace {
bool CheckCycles(std::vector<std::vector<mg_graph::Node<>>> user, std::vector<std::vector<std::uint64_t>> correct) {
  // normalize cycles
  for (auto &cycle : correct) {
    std::rotate(cycle.begin(), std::min_element(cycle.begin(), cycle.end()), cycle.end());
  }

  std::vector<std::vector<std::uint64_t>> user_cycles;
  for (const auto &cycle : user) {
    std::vector<std::uint64_t> user_cycle;
    for (const auto &node : cycle) {
      user_cycle.push_back(node.id);
    }
    std::rotate(user_cycle.begin(), std::min_element(user_cycle.begin(), user_cycle.end()), user_cycle.end());
    user_cycles.push_back(user_cycle);
  }

  if (user_cycles.size() != correct.size()) {
    return false;
  }

  for (std::size_t i = 0; i < user_cycles.size(); ++i) {
    user_cycles[i].push_back(user_cycles[i][0]);
    correct[i].push_back(correct[i][0]);

    if (user_cycles[i][1] > user_cycles[i][user_cycles[i].size() - 2]) {
      std::reverse(user_cycles[i].begin(), user_cycles[i].end());
    }
    if (correct[i][1] > correct[i][correct[i].size() - 2]) std::reverse(correct[i].begin(), correct[i].end());
  }

  std::sort(correct.begin(), correct.end());
  std::sort(user_cycles.begin(), user_cycles.end());

  return user_cycles == correct;
}

}  // namespace

TEST(Cycles, EmptyGraph) {
  auto graph = mg_generate::BuildGraph(0, {});
  auto cycles = cycles_alg::GetCycles(*graph);
  ASSERT_TRUE(CheckCycles(cycles, {}));
}

TEST(Cycles, SingleNode) {
  auto graph = mg_generate::BuildGraph(1, {});
  auto cycles = cycles_alg::GetCycles(*graph);
  ASSERT_TRUE(CheckCycles(cycles, {}));
}

TEST(Cycles, DisconnectedNodes) {
  auto graph = mg_generate::BuildGraph(100, {});
  auto cycles = cycles_alg::GetCycles(*graph);
  ASSERT_TRUE(CheckCycles(cycles, {}));
}

TEST(Cycles, SmallTree) {
  //    (4)
  //   /   \
  // (2)   (1)
  //  |   /   \
  // (0)(3)   (5)
  auto graph = mg_generate::BuildGraph(6, {{2, 4}, {1, 4}, {0, 2}, {1, 3}, {1, 5}});
  auto cycles = cycles_alg::GetCycles(*graph);
  ASSERT_TRUE(CheckCycles(cycles, {}));
}

TEST(Cycles, RandomTree) {
  auto graph = mg_generate::GenRandomTree(10000);
  auto cycles = cycles_alg::GetCycles(*graph);
  ASSERT_TRUE(CheckCycles(cycles, {}));
}

TEST(Cycles, Triangle) {
  auto graph = mg_generate::BuildGraph(3, {{0, 1}, {1, 2}, {0, 2}});
  auto cycles = cycles_alg::GetCycles(*graph);
  ASSERT_TRUE(CheckCycles(cycles, {{0, 1, 2}}));
}

TEST(Cycles, BigCycle) {
  int nodes = 1000;
  std::vector<std::pair<uint64_t, uint64_t>> edges;
  for (int i = 1; i < nodes; ++i) {
    edges.emplace_back(i - 1, i);
  }
  edges.emplace_back(0, nodes - 1);

  auto graph = mg_generate::BuildGraph(nodes, edges);

  auto cycles = cycles_alg::GetCycles(*graph);

  std::vector<std::vector<uint64_t>> correct;
  correct.push_back({});
  for (int i = 0; i < nodes; ++i) correct.back().push_back(i);

  ASSERT_TRUE(CheckCycles(cycles, correct));
}

TEST(Cycles, HandmadeConnectedGraph1) {
  //    (1)--(3)--(7)
  //   / |    |
  // (0) |   (4)--(5)
  //   \ |     \  /
  //    (2)     (6)
  auto graph = mg_generate::BuildGraph(8, {{0, 1}, {0, 2}, {1, 2}, {1, 3}, {3, 4}, {3, 7}, {4, 5}, {4, 6}, {5, 6}});
  auto cycles = cycles_alg::GetCycles(*graph);
  ASSERT_TRUE(CheckCycles(cycles, {{0, 1, 2}, {4, 5, 6}}));
}

TEST(Cycles, HandmadeConnectedGraph2) {
  //    (1)--(3)--(7)     (10)     (14)
  //   / |    |    |     /    \     |
  // (0) |   (4)--(5)--(9)   (12)--(13)
  //   \ |     \  / \    \   /
  //    (2)     (6) (8)   (11)
  auto graph = mg_generate::BuildGraph(15, {{0, 1},
                                            {0, 2},
                                            {1, 2},
                                            {1, 3},
                                            {3, 4},
                                            {3, 7},
                                            {4, 5},
                                            {4, 6},
                                            {5, 6},
                                            {5, 7},
                                            {5, 8},
                                            {5, 9},
                                            {9, 10},
                                            {9, 11},
                                            {10, 12},
                                            {11, 12},
                                            {12, 13},
                                            {13, 14}});
  auto cycles = cycles_alg::GetCycles(*graph);
  ASSERT_TRUE(CheckCycles(cycles, {{0, 1, 2}, {3, 7, 5, 4}, {4, 5, 6}, {3, 7, 5, 6, 4}, {9, 10, 12, 11}}));
}

TEST(Cycles, DisconnectedCycles) {
  //    (1)  (3)---(4)
  //   / |    |     |
  // (0) |    |     |
  //   \ |    |     |
  //    (2)  (5)---(6)
  auto graph = mg_generate::BuildGraph(7, {{0, 1}, {0, 2}, {1, 2}, {3, 4}, {3, 5}, {4, 6}, {5, 6}});
  auto cycles = cycles_alg::GetCycles(*graph);
  ASSERT_TRUE(CheckCycles(cycles, {{0, 1, 2}, {3, 4, 6, 5}}));
}

TEST(Cycles, HandmadeDisconnectedGraph) {
  //    (4)--(5)                   (12)         (19)         (23)
  //     |    |                   /    \        /           /   \
  //    (1)--(3)               (11)    (13)--(18)--(20)--(22)--(24)
  //   /                        |       |       \
  // (0)             (8)--(9)  (10)----(14)      (21)--(25)
  //   \                        |
  //    (2)--(6)               (15)----(16)
  //      \  /                    \    /
  //       (7)                     (17)
  auto graph = mg_generate::BuildGraph(
      26, {{0, 1},   {0, 2},   {1, 4},   {1, 3},   {3, 5},   {4, 5},   {2, 6},   {2, 7},   {6, 7},   {8, 9},
           {10, 11}, {11, 12}, {12, 13}, {13, 14}, {10, 14}, {10, 15}, {15, 16}, {16, 17}, {15, 17}, {13, 18},
           {18, 19}, {18, 21}, {18, 20}, {21, 25}, {20, 22}, {22, 23}, {23, 24}, {22, 24}});
  auto cycles = cycles_alg::GetCycles(*graph);
  ASSERT_TRUE(CheckCycles(cycles, {{1, 3, 5, 4}, {2, 6, 7}, {10, 11, 12, 13, 14}, {15, 16, 17}, {22, 23, 24}}));
}

TEST(Cycles, HandmadeArticulationPoint) {
  // (0)     (3)
  //  | \   / |
  //  |  (2)  |
  //  | /   \ |
  // (1)     (4)   (9)
  //        /   \ /  \
  //      (5)---(6)--(10)
  //        \   /
  //         (8)
  auto graph = mg_generate::BuildGraph(11, {{0, 1},
                                            {0, 2},
                                            {1, 2},
                                            {2, 3},
                                            {2, 4},
                                            {3, 4},
                                            {4, 5},
                                            {5, 8},
                                            {6, 8},
                                            {4, 6},
                                            {5, 6},
                                            {6, 9},
                                            {9, 10},
                                            {6, 10}});
  auto cycles = cycles_alg::GetCycles(*graph);
  ASSERT_TRUE(CheckCycles(cycles, {{0, 1, 2}, {2, 3, 4}, {4, 5, 6}, {5, 6, 8}, {4, 5, 8, 6}, {6, 9, 10}}));
}

TEST(Cycles, HandmadeComplexCycle) {
  //       (2)--(3)
  //      /        \
  //    (1)---------(4)
  //   / |            \
  // (0)-+------------(5)
  //  |  |             |
  // (11)+------------(6)
  //   \ |            /
  //    (10)        (7)
  //      \        /
  //       (9)--(8)
  int nodes = 12;
  std::vector<std::pair<std::uint64_t, std::uint64_t>> edges;
  for (int i = 1; i < nodes; ++i) {
    edges.emplace_back(i - 1, i);
  }
  edges.emplace_back(0, nodes - 1);

  edges.emplace_back(1, 4);
  edges.emplace_back(0, 5);
  edges.emplace_back(6, 11);
  edges.emplace_back(1, 10);

  auto graph = mg_generate::BuildGraph(nodes, edges);
  auto cycles = cycles_alg::GetCycles(*graph);
  ASSERT_TRUE(CheckCycles(cycles, {{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11},
                                   {1, 2, 3, 4},
                                   {0, 1, 4, 5},
                                   {11, 0, 5, 6},
                                   {11, 6, 7, 8, 9, 10},
                                   {0, 1, 2, 3, 4, 5},
                                   {11, 0, 1, 4, 5, 6},
                                   {9, 10, 11, 0, 5, 6, 7, 8},
                                   {11, 0, 1, 2, 3, 4, 5, 6},
                                   {9, 10, 11, 0, 1, 4, 5, 6, 7, 8},
                                   {0, 1, 10, 11},
                                   {1, 2, 3, 4, 5, 6, 7, 8, 9, 10},
                                   {10, 1, 4, 5, 0, 11},
                                   {10, 1, 4, 5, 6, 11},
                                   {10, 1, 0, 5, 6, 7, 8, 9},
                                   {10, 1, 0, 11, 6, 7, 8, 9},
                                   {10, 1, 4, 5, 0, 11, 6, 7, 8, 9},
                                   {10, 1, 0, 5, 6, 11},
                                   {10, 1, 2, 3, 4, 5, 0, 11},
                                   {10, 1, 2, 3, 4, 5, 0, 11, 6, 7, 8, 9},
                                   {10, 1, 2, 3, 4, 5, 6, 11},
                                   {10, 1, 4, 5, 6, 7, 8, 9}}));
}

TEST(Cycles, NeighbouringCycles) {
  auto graph = mg_generate::BuildGraph(4, {{0, 1}, {0, 1}, {0, 3}, {1, 2}, {1, 2}, {1, 2}});
  auto cycles = cycles_alg::GetCycles(*graph);
  ASSERT_TRUE(CheckCycles(cycles, {{0, 1}, {1, 2}}));
}

TEST(Cycles, SelfLoops) {
  auto graph = mg_generate::BuildGraph(4, {{0, 1}, {1, 2}, {2, 3}, {1, 1}});
  auto cycles = cycles_alg::GetCycles(*graph);
  ASSERT_TRUE(CheckCycles(cycles, {{1, 1}}));
}

int main(int argc, char **argv) {
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
