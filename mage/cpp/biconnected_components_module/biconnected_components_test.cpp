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

#include <chrono>
#include <random>
#include <set>
#include <vector>

#include <gtest/gtest.h>
#include <mg_generate.hpp>
#include <mg_graph.hpp>
#include <mg_test_utils.hpp>

#include "algorithm/biconnected_components.hpp"

namespace {

bool CheckBCC(std::vector<std::vector<mg_graph::Edge<>>> user,
              std::vector<std::vector<std::pair<std::uint64_t, std::uint64_t>>> correct_bcc_edges) {
  std::vector<std::set<std::pair<std::uint64_t, std::uint64_t>>> user_bcc_edges;
  std::vector<std::set<std::pair<std::uint64_t, std::uint64_t>>> correct_bcc_edges_set;

  for (auto &bcc : correct_bcc_edges) {
    correct_bcc_edges_set.emplace_back();
    for (auto &p : bcc) {
      correct_bcc_edges_set.back().insert({p.first, p.second});
      correct_bcc_edges_set.back().insert({p.second, p.first});
    }
  }

  for (auto &bcc : user) {
    user_bcc_edges.emplace_back();
    for (const auto &edge : bcc) {
      user_bcc_edges.back().insert({edge.from, edge.to});
      user_bcc_edges.back().insert({edge.to, edge.from});
    }
  }

  std::ranges::sort(correct_bcc_edges_set);
  std::ranges::sort(user_bcc_edges);

  return user_bcc_edges == correct_bcc_edges_set;
}
}  // namespace

TEST(BCC, EmptyGraph) {
  auto G = mg_generate::BuildGraph(0, {});

  std::unordered_set<std::uint64_t> articulation_points;
  std::vector<std::unordered_set<std::uint64_t>> bcc_nodes;
  auto BCC = bcc_algorithm::GetBiconnectedComponents(*G, articulation_points, bcc_nodes);

  ASSERT_TRUE(CheckBCC(BCC, {}));
}

TEST(BCC, SingleNode) {
  auto G = mg_generate::BuildGraph(1, {});

  std::unordered_set<std::uint64_t> articulation_points;
  std::vector<std::unordered_set<std::uint64_t>> bcc_nodes;
  auto BCC = bcc_algorithm::GetBiconnectedComponents(*G, articulation_points, bcc_nodes);

  ASSERT_TRUE(CheckBCC(BCC, {}));
}

TEST(BCC, DisconnectedNodes) {
  auto G = mg_generate::BuildGraph(100, {});

  std::unordered_set<std::uint64_t> articulation_points;
  std::vector<std::unordered_set<std::uint64_t>> bcc_nodes;
  auto BCC = bcc_algorithm::GetBiconnectedComponents(*G, articulation_points, bcc_nodes);

  ASSERT_TRUE(CheckBCC(BCC, {}));
}

TEST(BCC, Cycle) {
  const std::uint64_t n = 100;
  std::vector<std::pair<std::uint64_t, std::uint64_t>> E;
  E.reserve(n);
  for (std::uint64_t i = 0; i < n; ++i) {
    E.emplace_back(i, (i + 1) % n);
  }
  auto G = mg_generate::BuildGraph(n, E);

  std::unordered_set<std::uint64_t> articulation_points;
  std::vector<std::unordered_set<std::uint64_t>> bcc_nodes;
  auto BCC = bcc_algorithm::GetBiconnectedComponents(*G, articulation_points, bcc_nodes);

  ASSERT_TRUE(CheckBCC(BCC, {E}));
}

///    (4)
///   /   \
/// (2)   (1)
///  |   /   \
/// (0)(3)   (5)
TEST(BCC, SmallTree) {
  auto G = mg_generate::BuildGraph(6, {{2, 4}, {1, 4}, {0, 2}, {1, 3}, {1, 5}});

  std::unordered_set<std::uint64_t> articulation_points;
  std::vector<std::unordered_set<std::uint64_t>> bcc_nodes;
  auto BCC = bcc_algorithm::GetBiconnectedComponents(*G, articulation_points, bcc_nodes);

  ASSERT_TRUE(CheckBCC(BCC, {{{2, 4}}, {{1, 4}}, {{0, 2}}, {{1, 3}}, {{1, 5}}}));
}

TEST(BCC, RandomTree) {
  auto G = mg_generate::GenRandomTree(10000);
  std::vector<std::vector<std::pair<std::uint64_t, std::uint64_t>>> correct_bcc;
  for (const auto &edge : G->Edges()) {
    if (edge.from < edge.to) correct_bcc.push_back({{edge.from, edge.to}});
  }

  std::unordered_set<std::uint64_t> articulation_points;
  std::vector<std::unordered_set<std::uint64_t>> bcc_nodes;
  auto BCC = bcc_algorithm::GetBiconnectedComponents(*G, articulation_points, bcc_nodes);

  ASSERT_TRUE(CheckBCC(BCC, correct_bcc));
}

///    (1)--(3)--(7)
///   / |    |
/// (0) |   (4)--(5)
///   \ |     \  /
///    (2)     (6)
TEST(BCC, HandmadeConnectedGraph1) {
  auto G = mg_generate::BuildGraph(8, {{0, 1}, {0, 2}, {1, 2}, {1, 3}, {3, 4}, {3, 7}, {4, 5}, {4, 6}, {5, 6}});

  std::unordered_set<std::uint64_t> articulation_points;
  std::vector<std::unordered_set<std::uint64_t>> bcc_nodes;
  auto BCC = bcc_algorithm::GetBiconnectedComponents(*G, articulation_points, bcc_nodes);

  ASSERT_TRUE(CheckBCC(BCC, {{{0, 1}, {1, 2}, {2, 0}}, {{1, 3}}, {{3, 7}}, {{3, 4}}, {{4, 5}, {5, 6}, {4, 6}}}));
}

///    (1)--(3)--(7)     (10)     (14)
///   / |    |    |     /    \     |
/// (0) |   (4)--(5)--(9)   (12)--(13)
///   \ |     \  / \    \   /
///    (2)     (6) (8)   (11)
TEST(BCC, HandmadeConnectedGraph2) {
  auto G = mg_generate::BuildGraph(15, {{0, 1},
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

  std::unordered_set<std::uint64_t> articulation_points;
  std::vector<std::unordered_set<std::uint64_t>> bcc_nodes;
  auto BCC = bcc_algorithm::GetBiconnectedComponents(*G, articulation_points, bcc_nodes);

  ASSERT_TRUE(CheckBCC(BCC, {{{0, 1}, {1, 2}, {2, 0}},
                             {{1, 3}},
                             {{3, 7}, {7, 5}, {5, 4}, {3, 4}, {4, 6}, {5, 6}},
                             {{5, 8}},
                             {{5, 9}},
                             {{9, 10}, {10, 12}, {11, 12}, {9, 11}},
                             {{12, 13}},
                             {{13, 14}}}));
}

///    (4)--(5)                   (12)         (19)         (23)
///     |    |                   /    \        /           /   \
///    (1)--(3)               (11)    (13)--(18)--(20)--(22)--(24)
///   /                        |       |       \
/// (0)             (8)--(9)  (10)----(14)      (21)--(25)
///   \                        |
///    (2)--(6)               (15)----(16)
///      \  /                    \    /
///       (7)                     (17)
TEST(BCC, HandmadeDisconnectedGraph) {
  auto G = mg_generate::BuildGraph(
      26, {{0, 1},   {0, 2},   {1, 4},   {1, 3},   {3, 5},   {4, 5},   {2, 6},   {2, 7},   {6, 7},   {8, 9},
           {10, 11}, {11, 12}, {12, 13}, {13, 14}, {10, 14}, {10, 15}, {15, 16}, {16, 17}, {15, 17}, {13, 18},
           {18, 19}, {18, 21}, {18, 20}, {21, 25}, {20, 22}, {22, 23}, {23, 24}, {22, 24}});

  std::unordered_set<std::uint64_t> articulation_points;
  std::vector<std::unordered_set<std::uint64_t>> bcc_nodes;
  auto BCC = bcc_algorithm::GetBiconnectedComponents(*G, articulation_points, bcc_nodes);

  ASSERT_TRUE(CheckBCC(BCC, {{{0, 1}},
                             {{0, 2}},
                             {{1, 4}, {1, 3}, {4, 5}, {3, 5}},
                             {{2, 6}, {6, 7}, {2, 7}},
                             {{8, 9}},
                             {{10, 11}, {11, 12}, {12, 13}, {13, 14}, {14, 10}},
                             {{10, 15}},
                             {{15, 16}, {15, 17}, {16, 17}},
                             {{13, 18}},
                             {{18, 19}},
                             {{18, 21}},
                             {{18, 20}},
                             {{21, 25}},
                             {{20, 22}},
                             {{22, 23}, {22, 24}, {23, 24}}}));
}

///     (1)--(2)--(5)--(6)        (13)
///    / |\  /|    |  / | \      /    \
/// (0)  | \/ |    | /  | (7)--(10)--(11)
///    \ | /\ |    |/   |/       \    /
///     (3)--(4)  (9)--(8)        (12)
TEST(BCC, HandmadeCrossEdge) {
  auto G = mg_generate::BuildGraph(
      14, {{0, 1}, {0, 3}, {1, 3}, {1, 4}, {1, 2}, {3, 4},  {2, 4},   {2, 3},   {2, 5},   {5, 9},   {9, 8},
           {8, 7}, {6, 7}, {6, 8}, {9, 6}, {5, 6}, {7, 10}, {10, 13}, {10, 11}, {10, 12}, {13, 11}, {11, 12}});

  std::unordered_set<std::uint64_t> articulation_points;
  std::vector<std::unordered_set<std::uint64_t>> bcc_nodes;
  auto BCC = bcc_algorithm::GetBiconnectedComponents(*G, articulation_points, bcc_nodes);

  ASSERT_TRUE(CheckBCC(BCC, {{{0, 1}, {1, 3}, {0, 3}, {1, 4}, {1, 2}, {2, 4}, {3, 4}, {2, 3}},
                             {{2, 5}},
                             {{5, 6}, {6, 7}, {7, 8}, {8, 9}, {9, 5}, {9, 6}, {6, 8}},
                             {{7, 10}},
                             {{10, 11}, {10, 12}, {10, 13}, {11, 12}, {11, 13}}}));
}

/// (0)     (3)
///  | \   / |
///  |  (2)  |
///  | /   \ |
/// (1)     (4)   (9)
///        /   \ /  \
///      (5)---(6)--(10)
///        \   /
///         (8)
TEST(BCC, HandmadeArticulationPoint) {
  auto G = mg_generate::BuildGraph(11, {{0, 1},
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

  std::unordered_set<std::uint64_t> articulation_points;
  std::vector<std::unordered_set<std::uint64_t>> bcc_nodes;
  auto BCC = bcc_algorithm::GetBiconnectedComponents(*G, articulation_points, bcc_nodes);

  ASSERT_TRUE(CheckBCC(BCC, {{{0, 1}, {1, 2}, {2, 0}},
                             {{2, 3}, {2, 4}, {3, 4}},
                             {{4, 5}, {4, 6}, {5, 6}, {5, 8}, {6, 8}},
                             {{6, 9}, {9, 10}, {6, 10}}}));
}

TEST(BCC, Performance) {
  auto G = mg_generate::GenRandomGraph(10000, 25000);
  const mg_test_utility::Timer timer;

  std::unordered_set<std::uint64_t> articulation_points;
  std::vector<std::unordered_set<std::uint64_t>> bcc_nodes;
  auto BCC = bcc_algorithm::GetBiconnectedComponents(*G, articulation_points, bcc_nodes);

  auto time_elapsed = timer.Elapsed();
  ASSERT_TRUE(time_elapsed < std::chrono::seconds(1));
}

int main(int argc, char **argv) {
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
