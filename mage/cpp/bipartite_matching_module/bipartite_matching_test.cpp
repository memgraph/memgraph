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

#include <gtest/gtest.h>
#include <mg_generate.hpp>
#include <mg_graph.hpp>
#include <mg_test_utils.hpp>

#include "algorithm/bipartite_matching.hpp"

TEST(BipartiteMatching, RandomCompleteBipartiteGraphs) {
  auto seed = 42;
  auto num_of_experiments = 200;
  std::mt19937 rng(seed);

  auto max_size = 250;
  std::uniform_int_distribution<> dist(1, max_size);

  for (int t = 0; t < num_of_experiments; ++t) {
    auto size_a = dist(rng);
    auto size_b = dist(rng);

    std::vector<std::pair<uint64_t, uint64_t>> bipartite_edges;
    for (int i = 1; i <= size_a; ++i)
      for (int j = 1; j <= size_b; ++j) bipartite_edges.emplace_back(i, j);

    auto max_match = bipartite_matching_util::MaximumMatching(bipartite_edges);
    ASSERT_EQ(max_match, std::min(size_a, size_b));
  }
}

TEST(BipartiteMatching, NotBipartiteGraph) {
  auto graph =
      mg_generate::BuildGraph(5, {{0, 1}, {0, 2}, {0, 3}, {0, 4}, {1, 2}, {1, 3}, {1, 4}, {2, 3}, {2, 4}, {3, 4}});

  std::vector<std::int8_t> colors(graph->Nodes().size(), -1);
  auto is_graph_bipartite = bipartite_matching_util::IsGraphBipartiteColoring(*graph, colors);
  ASSERT_FALSE(is_graph_bipartite);
}

TEST(BipartiteMatching, BipartiteGraph) {
  auto graph = mg_generate::BuildGraph(4, {{0, 1}, {1, 2}, {2, 3}, {3, 0}});

  std::vector<std::int8_t> colors(graph->Nodes().size(), -1);
  auto is_graph_bipartite = bipartite_matching_util::IsGraphBipartiteColoring(*graph, colors);
  ASSERT_TRUE(is_graph_bipartite);
}

TEST(BipartiteMatching, BipartiteGraphWith2Components) {
  auto graph = mg_generate::BuildGraph(5, {{0, 1}, {1, 2}, {3, 4}});

  std::vector<std::int8_t> colors(graph->Nodes().size(), -1);
  auto is_graph_bipartite = bipartite_matching_util::IsGraphBipartiteColoring(*graph, colors);
  ASSERT_TRUE(is_graph_bipartite);
}

TEST(BipartiteMatching, NotBipartiteGraphWithSelfLoop) {
  auto graph = mg_generate::BuildGraph(5, {{0, 1}, {1, 2}, {2, 3}, {0, 0}});

  std::vector<std::int8_t> colors(graph->Nodes().size(), -1);
  auto is_graph_bipartite = bipartite_matching_util::IsGraphBipartiteColoring(*graph, colors);
  ASSERT_FALSE(is_graph_bipartite);
}

TEST(BipartiteMatching, Handmade1) {
  const std::vector<std::pair<uint64_t, uint64_t>> bipartite_edges = {{1, 2}, {1, 3}, {2, 1}, {2, 4},
                                                                      {3, 3}, {4, 3}, {4, 4}, {5, 5}};
  auto max_match = bipartite_matching_util::MaximumMatching(bipartite_edges);
  ASSERT_EQ(max_match, 5);
}

TEST(BipartiteMatching, HandmadeGraph1) {
  auto graph = mg_generate::BuildGraph(10, {{0, 6}, {0, 7}, {1, 5}, {1, 8}, {2, 7}, {3, 7}, {3, 8}, {4, 9}});
  auto max_match = bipartite_matching_alg::BipartiteMatching(*graph);
  ASSERT_EQ(max_match, 5);
}

TEST(BipartiteMatching, Handmade2) {
  const std::vector<std::pair<uint64_t, uint64_t>> bipartite_edges = {{5, 2}, {1, 2}, {4, 3}, {3, 1}, {2, 2}, {4, 4}};
  auto max_match = bipartite_matching_util::MaximumMatching(bipartite_edges);
  ASSERT_EQ(max_match, 3);
}

TEST(BipartiteMatching, HandmadeGraph2) {
  auto graph = mg_generate::BuildGraph(9, {{4, 6}, {0, 6}, {3, 7}, {2, 5}, {1, 6}, {3, 8}});
  auto max_match = bipartite_matching_alg::BipartiteMatching(*graph);
  ASSERT_EQ(max_match, 3);
}

TEST(BipartiteMatching, Handmade3) {
  const std::vector<std::pair<uint64_t, uint64_t>> bipartite_edges = {{1, 2}, {1, 5}, {2, 3}, {3, 2},
                                                                      {4, 3}, {4, 4}, {5, 1}};
  auto max_match = bipartite_matching_util::MaximumMatching(bipartite_edges);
  ASSERT_EQ(max_match, 5);
}

TEST(BipartiteMatching, HandmadeGraph3) {
  auto graph = mg_generate::BuildGraph(10, {{0, 6}, {0, 9}, {1, 7}, {2, 6}, {3, 7}, {3, 8}, {4, 5}});
  auto max_match = bipartite_matching_alg::BipartiteMatching(*graph);
  ASSERT_EQ(max_match, 5);
}

TEST(BipartiteMatching, Handmade4) {
  const std::vector<std::pair<uint64_t, uint64_t>> bipartite_edges = {
      {1, 4}, {1, 10}, {2, 6}, {2, 8}, {2, 9}, {3, 9}, {4, 6}, {4, 8}, {5, 1},  {5, 3},  {5, 9}, {6, 1},
      {6, 5}, {6, 7},  {7, 4}, {7, 7}, {8, 2}, {8, 8}, {9, 3}, {9, 5}, {10, 1}, {10, 2}, {10, 7}};
  auto max_match = bipartite_matching_util::MaximumMatching(bipartite_edges);
  ASSERT_EQ(max_match, 10);
}

TEST(BipartiteMatching, HandmadeGraph4) {
  auto graph = mg_generate::BuildGraph(
      20, {{0, 13}, {0, 19}, {1, 15}, {1, 17}, {1, 18}, {2, 18}, {3, 15}, {3, 17}, {4, 10}, {4, 12}, {4, 18}, {5, 10},
           {5, 14}, {5, 16}, {6, 13}, {6, 16}, {7, 11}, {7, 17}, {8, 12}, {8, 14}, {9, 10}, {9, 11}, {9, 17}});
  auto max_match = bipartite_matching_alg::BipartiteMatching(*graph);
  ASSERT_EQ(max_match, 10);
}

TEST(BipartiteMatching, Handmade5) {
  const std::vector<std::pair<uint64_t, uint64_t>> bipartite_edges = {
      {1, 7}, {2, 1}, {2, 6}, {2, 8}, {3, 4}, {3, 5}, {3, 6}, {3, 7}, {4, 2}, {4, 4},  {4, 5},  {5, 5},  {5, 6},
      {6, 3}, {6, 4}, {6, 7}, {7, 5}, {7, 7}, {8, 4}, {8, 5}, {9, 3}, {9, 6}, {9, 10}, {10, 1}, {10, 8}, {10, 9}};
  auto max_match = bipartite_matching_util::MaximumMatching(bipartite_edges);
  ASSERT_EQ(max_match, 9);
}

TEST(BipartiteMatching, HandmadeGraph5) {
  auto graph =
      mg_generate::BuildGraph(20, {{0, 16}, {1, 10}, {1, 15}, {1, 17}, {2, 13}, {2, 14}, {2, 15}, {2, 16}, {3, 11},
                                   {3, 13}, {3, 14}, {4, 14}, {4, 15}, {5, 12}, {5, 13}, {5, 16}, {6, 14}, {6, 16},
                                   {7, 13}, {7, 14}, {8, 12}, {8, 15}, {8, 19}, {9, 10}, {9, 17}, {9, 18}});
  auto max_match = bipartite_matching_alg::BipartiteMatching(*graph);
  ASSERT_EQ(max_match, 9);
}

TEST(BipartiteMatching, ChainGraph) {
  auto graph = mg_generate::BuildGraph(5, {{0, 1}, {1, 2}, {2, 3}, {3, 4}});
  auto max_match = bipartite_matching_alg::BipartiteMatching(*graph);
  ASSERT_EQ(max_match, 2);
}

TEST(BipartiteMatching, CycleGraph) {
  auto graph = mg_generate::BuildGraph(6, {{0, 1}, {1, 2}, {2, 3}, {3, 4}, {4, 5}, {5, 0}});
  auto max_match = bipartite_matching_alg::BipartiteMatching(*graph);
  ASSERT_EQ(max_match, 3);
}

TEST(BipartiteMatching, Performance) {
  auto seed = 42;
  std::mt19937 rng(seed);

  auto num_of_nodes = 250;
  std::size_t num_of_edges = num_of_nodes * num_of_nodes / 5;

  std::uniform_int_distribution<> dist(1, num_of_nodes);
  std::set<std::pair<uint64_t, uint64_t>> bipartite_edges;
  for (std::size_t i = 0; i < num_of_edges; ++i) {
    auto from = dist(rng);
    auto to = dist(rng);
    bipartite_edges.insert({from, to});
  }

  const mg_test_utility::Timer timer;
  bipartite_matching_util::MaximumMatching(
      std::vector<std::pair<uint64_t, uint64_t>>(bipartite_edges.begin(), bipartite_edges.end()));
  auto time_elapsed = timer.Elapsed();
  ASSERT_TRUE(time_elapsed < std::chrono::seconds(1));
}

int main(int argc, char **argv) {
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
