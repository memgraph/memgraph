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

#include <queue>

#include <gtest/gtest.h>
#include <mg_generate.hpp>
#include <mg_graph.hpp>
#include <mg_test_utils.hpp>

#include "algorithm/bridges.hpp"

namespace {

std::uint64_t CountComponents(const mg_graph::Graph<> &graph) {
  std::uint64_t n_components = 0;
  std::vector<bool> visited(graph.Nodes().size());

  for (const auto &node : graph.Nodes()) {
    if (visited[node.id]) continue;
    ++n_components;

    std::queue<std::uint64_t> queue;
    visited[node.id] = true;
    queue.push(node.id);
    while (!queue.empty()) {
      auto curr_id = queue.front();
      queue.pop();
      for (const auto &neigh : graph.Neighbours(curr_id)) {
        auto next_id = neigh.node_id;
        if (visited[next_id]) continue;
        visited[next_id] = true;
        queue.push(next_id);
      }
    }
  }
  return n_components;
}

std::vector<std::pair<std::uint64_t, std::uint64_t>> GetBridgesBruteforce(mg_graph::Graph<> *graph) {
  using IntPair = std::pair<std::uint64_t, std::uint64_t>;

  const std::uint64_t comp_cnt = CountComponents(*graph);
  std::vector<IntPair> bridges;
  auto edges = graph->ExistingEdges();
  for (const auto [id, from, to] : edges) {
    graph->EraseEdge(from, to);
    auto new_comp_cnt = CountComponents(*graph);

    if (new_comp_cnt > comp_cnt) bridges.emplace_back(from, to);
    graph->CreateEdge(from, to, mg_graph::GraphType::kUndirectedGraph);
  }
  return bridges;
}

/// Checks if obtained list of bridges is correct.
bool CheckBridges(std::vector<mg_graph::Edge<>> user, std::vector<std::pair<std::uint64_t, std::uint64_t>> correct) {
  std::set<std::pair<std::uint64_t, std::uint64_t>> user_bridge_set;
  std::set<std::pair<std::uint64_t, std::uint64_t>> correct_bridge_set;
  for (auto [from, to] : correct) {
    correct_bridge_set.emplace(from, to);
    correct_bridge_set.emplace(to, from);
  }
  for (auto [id, from, to] : user) {
    user_bridge_set.emplace(from, to);
    user_bridge_set.emplace(to, from);
  }
  return user_bridge_set == correct_bridge_set;
}

}  // namespace

TEST(Bridges, EmptyGraph) {
  auto graph = mg_generate::BuildGraph(0, {});
  auto bridges = bridges_alg::GetBridges(*graph);
  ASSERT_TRUE(CheckBridges(bridges, {}));
}

TEST(Bridges, SingleNode) {
  auto graph = mg_generate::BuildGraph(1, {});
  auto bridges = bridges_alg::GetBridges(*graph);
  ASSERT_TRUE(CheckBridges(bridges, {}));
}

TEST(Bridges, DisconnectedNodes) {
  auto graph = mg_generate::BuildGraph(100, {});
  auto bridges = bridges_alg::GetBridges(*graph);
  ASSERT_TRUE(CheckBridges(bridges, {}));
}

TEST(Bridges, Cycle) {
  const std::uint64_t n = 100;
  std::vector<std::pair<std::uint64_t, std::uint64_t>> edges;
  edges.reserve(n);
  for (std::uint64_t i = 0; i < n; ++i) {
    edges.emplace_back(i, (i + 1) % n);
  }
  auto graph = mg_generate::BuildGraph(n, edges);
  auto bridges = bridges_alg::GetBridges(*graph);
  ASSERT_TRUE(CheckBridges(bridges, {}));
}

TEST(Bridges, SmallTree) {
  //    (4)
  //   /   \
  // (2)   (1)
  //  |   /   \
  // (0)(3)   (5)
  auto graph = mg_generate::BuildGraph(6, {{2, 4}, {1, 4}, {0, 2}, {1, 3}, {1, 5}});
  auto bridges = bridges_alg::GetBridges(*graph);
  ASSERT_TRUE(CheckBridges(bridges, {{2, 4}, {1, 4}, {0, 2}, {1, 3}, {1, 5}}));
}

TEST(Bridges, RandomTree) {
  auto graph = mg_generate::GenRandomTree(10000);
  std::vector<std::pair<std::uint64_t, std::uint64_t>> edges;

  for (const auto &edge : graph->Edges()) edges.emplace_back(edge.from, edge.to);

  auto bridges = bridges_alg::GetBridges(*graph);
  ASSERT_TRUE(CheckBridges(bridges, edges));
}

TEST(Bridges, HandmadeConnectedGraph1) {
  //    (1)--(3)--(7)
  //   / |    |
  // (0) |   (4)--(5)
  //   \ |     \  /
  //    (2)     (6)
  auto graph = mg_generate::BuildGraph(8, {{0, 1}, {0, 2}, {1, 2}, {1, 3}, {3, 4}, {3, 7}, {4, 5}, {4, 6}, {5, 6}});
  auto bridges = bridges_alg::GetBridges(*graph);
  ASSERT_TRUE(CheckBridges(bridges, {{1, 3}, {3, 4}, {3, 7}}));
}

TEST(Bridges, HandmadeConnectedGraph2) {
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
  auto bridges = bridges_alg::GetBridges(*graph);
  ASSERT_TRUE(CheckBridges(bridges, {{1, 3}, {5, 8}, {5, 9}, {12, 13}, {13, 14}}));
}

TEST(Bridges, HandmadeDisconnectedGraph) {
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
  auto bridges = bridges_alg::GetBridges(*graph);
  ASSERT_TRUE(CheckBridges(
      bridges, {{0, 1}, {0, 2}, {8, 9}, {10, 15}, {13, 18}, {18, 19}, {18, 20}, {18, 21}, {21, 25}, {20, 22}}));
}

TEST(Bridges, SimpleNeighborCycle) {
  auto graph = mg_generate::BuildGraph(2, {{0, 1}, {1, 0}});
  auto bridges = bridges_alg::GetBridges(*graph);
  ASSERT_EQ(0, bridges.size());
}

TEST(Bridges, NeighborCycle) {
  auto graph = mg_generate::BuildGraph(6, {{0, 1}, {1, 2}, {2, 3}, {3, 0}, {3, 4}, {3, 4}, {4, 5}});
  auto bridges = bridges_alg::GetBridges(*graph);
  ASSERT_EQ(1, bridges.size());
  ASSERT_EQ(bridges[0].id, 6);
}

TEST(Bridges, Random100) {
  for (std::uint16_t t = 0; t < 100; ++t) {
    auto graph = mg_generate::GenRandomGraph(10, 20);
    auto algo_bridges = bridges_alg::GetBridges(*graph);
    auto bf_bridges = GetBridgesBruteforce(graph.get());
    ASSERT_TRUE(CheckBridges(algo_bridges, bf_bridges));
  }
}

TEST(Bridges, Performance) {
  auto graph = mg_generate::GenRandomGraph(10000, 25000);

  const mg_test_utility::Timer timer;
  auto bridges = bridges_alg::GetBridges(*graph);
  ASSERT_TRUE(timer.Elapsed() < std::chrono::seconds(100));
}

int main(int argc, char **argv) {
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
