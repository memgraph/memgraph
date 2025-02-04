// Copyright 2025 Memgraph Ltd.
//
// Licensed as a Memgraph Enterprise file under the Memgraph Enterprise
// License (the "License"); by using this file, you agree to be bound by the terms of the License, and you may not use
// this file except in compliance with the License. You may obtain a copy of the License at https://memgraph.com/legal.

#include <gtest/gtest.h>

#include <mg_generate.hpp>
#include <mg_test_utils.hpp>

#include "algorithm/katz.hpp"

namespace {
bool CompareRankingSort(const std::vector<std::pair<uint64_t, double>> &result,
                        const std::vector<std::uint64_t> &ranking, double threshold = 1e-6) {
  if (result.size() != ranking.size()) return false;

  std::vector<std::pair<std::uint64_t, double>> result_cp(result);

  std::sort(result_cp.begin(), result_cp.end(), [threshold](const auto &a, const auto &b) -> bool {
    auto [key_a, value_a] = a;
    auto [key_b, value_b] = b;

    auto diff = abs(value_b - value_a);
    return (value_a > value_b && diff > threshold) || (diff < threshold && key_a < key_b);
  });

  for (std::size_t i = 0; i < ranking.size(); i++) {
    auto [node_id, _] = result_cp[i];
    if (ranking[i] != node_id) return false;
  }
  return true;
}
}  // namespace

TEST(KatzCentrality, AddNodesAndEdges) {
  auto graph = mg_generate::BuildGraph({0, 1, 2, 3, 4, 5, 6},
                                       {{0, 5}, {0, 1}, {1, 4}, {2, 1}, {3, 1}, {4, 0}, {4, 3}, {4, 1}, {5, 0}, {5, 4}},
                                       mg_graph::GraphType::kDirectedGraph);
  auto katz_centrality = katz_alg::SetKatz(*graph);
  ASSERT_TRUE(CompareRankingSort(katz_centrality, {1, 4, 0, 3, 5, 2, 6}));

  graph = mg_generate::BuildGraph({0, 1, 2, 3, 4, 5, 6, 13},
                                  {{0, 5}, {0, 1}, {1, 4}, {2, 1}, {3, 1}, {4, 0}, {4, 3}, {4, 1}, {5, 0}, {5, 4}},
                                  mg_graph::GraphType::kDirectedGraph);
  katz_centrality = katz_alg::UpdateKatz(*graph, {13}, {}, {}, {}, {});
  ASSERT_TRUE(CompareRankingSort(katz_centrality, {1, 4, 0, 3, 5, 2, 6, 13}));

  graph = mg_generate::BuildGraph({0, 1, 2, 3, 4, 5, 6, 13},
                                  {{0, 5},
                                   {0, 1},
                                   {1, 4},
                                   {2, 1},
                                   {3, 1},
                                   {4, 0},
                                   {4, 3},
                                   {4, 1},
                                   {5, 0},
                                   {5, 4},
                                   {13, 1},
                                   {2, 13},
                                   {13, 5},
                                   {4, 13}},
                                  mg_graph::GraphType::kDirectedGraph);
  katz_centrality = katz_alg::UpdateKatz(*graph, {}, {{13, 1}, {2, 13}, {13, 5}, {4, 13}}, {10, 11, 12, 13}, {}, {});
  ASSERT_TRUE(CompareRankingSort(katz_centrality, {1, 4, 0, 5, 13, 3, 2, 6}));
}

TEST(KatzCentrality, AddEdges) {
  auto graph = mg_generate::BuildGraph({0, 1, 2, 3, 4, 5, 6},
                                       {{0, 5}, {0, 1}, {1, 4}, {2, 1}, {3, 1}, {4, 0}, {4, 3}, {4, 1}, {5, 0}, {5, 4}},
                                       mg_graph::GraphType::kDirectedGraph);
  auto katz_centrality = katz_alg::SetKatz(*graph);
  ASSERT_TRUE(CompareRankingSort(katz_centrality, {1, 4, 0, 3, 5, 2, 6}));

  graph = mg_generate::BuildGraph(
      {0, 1, 2, 3, 4, 5, 6},
      {{0, 5}, {0, 1}, {1, 4}, {2, 1}, {3, 1}, {4, 0}, {4, 3}, {4, 1}, {5, 0}, {5, 4}, {3, 5}, {2, 5}},
      mg_graph::GraphType::kDirectedGraph);
  katz_centrality = katz_alg::UpdateKatz(*graph, {}, {{3, 5}, {2, 5}}, {10, 11}, {}, {});
  ASSERT_TRUE(CompareRankingSort(katz_centrality, {1, 5, 4, 0, 3, 2, 6}));
}

TEST(KatzCentrality, AddEdgesGradually) {
  auto graph = mg_generate::BuildGraph({0, 1, 2, 3, 4, 5, 6}, {{3, 1}, {4, 0}}, mg_graph::GraphType::kDirectedGraph);
  auto katz_centrality = katz_alg::SetKatz(*graph);
  ASSERT_TRUE(CompareRankingSort(katz_centrality, {0, 1, 2, 3, 4, 5, 6}));

  graph = mg_generate::BuildGraph({0, 1, 2, 3, 4, 5, 6}, {{3, 1}, {4, 0}, {4, 3}, {4, 1}, {5, 0}},
                                  mg_graph::GraphType::kDirectedGraph);
  katz_centrality = katz_alg::UpdateKatz(*graph, {}, {{4, 3}, {4, 1}, {5, 0}}, {2, 3, 4}, {}, {});
  ASSERT_TRUE(CompareRankingSort(katz_centrality, {1, 0, 3, 2, 4, 5, 6}));

  graph = mg_generate::BuildGraph({0, 1, 2, 3, 4, 5, 6},
                                  {{3, 1}, {4, 0}, {4, 3}, {4, 1}, {5, 0}, {0, 5}, {0, 1}, {1, 4}, {2, 1}},
                                  mg_graph::GraphType::kDirectedGraph);
  katz_centrality = katz_alg::UpdateKatz(*graph, {}, {{0, 5}, {0, 1}, {1, 4}, {2, 1}}, {5, 6, 7, 8}, {}, {});
  ASSERT_TRUE(CompareRankingSort(katz_centrality, {1, 0, 4, 5, 3, 2, 6}));

  graph = mg_generate::BuildGraph({0, 1, 2, 3, 4, 5, 6},
                                  {{3, 1}, {4, 0}, {4, 3}, {4, 1}, {5, 0}, {0, 5}, {0, 1}, {1, 4}, {2, 1}, {5, 4}},
                                  mg_graph::GraphType::kDirectedGraph);
  katz_centrality = katz_alg::UpdateKatz(*graph, {}, {{5, 4}}, {9}, {}, {});
  ASSERT_TRUE(CompareRankingSort(katz_centrality, {1, 4, 0, 3, 5, 2, 6}));
}

TEST(KatzCentrality, DeleteEdges) {
  auto graph = mg_generate::BuildGraph({0, 1, 2, 3, 4, 5, 6},
                                       {{0, 5}, {0, 1}, {1, 4}, {2, 1}, {3, 1}, {4, 0}, {4, 3}, {4, 1}, {5, 0}, {5, 4}},
                                       mg_graph::GraphType::kDirectedGraph);
  auto katz_centrality = katz_alg::SetKatz(*graph);
  ASSERT_TRUE(CompareRankingSort(katz_centrality, {1, 4, 0, 3, 5, 2, 6}));

  graph = mg_generate::BuildGraph({0, 1, 2, 3, 4, 5, 6}, {{0, 5}, {0, 1}, {1, 4}, {4, 0}, {4, 3}, {5, 0}, {5, 4}},
                                  mg_graph::GraphType::kDirectedGraph);
  katz_centrality = katz_alg::UpdateKatz(*graph, {}, {}, {}, {}, {{2, 1}, {3, 1}, {4, 1}});
  ASSERT_TRUE(CompareRankingSort(katz_centrality, {0, 4, 1, 5, 3, 2, 6}));
}

TEST(KatzCentrality, DeleteAllEdges) {
  auto graph = mg_generate::BuildGraph({0, 1, 2, 3, 4, 5, 6},
                                       {{0, 5}, {0, 1}, {1, 4}, {2, 1}, {3, 1}, {4, 0}, {4, 3}, {4, 1}, {5, 0}, {5, 4}},
                                       mg_graph::GraphType::kDirectedGraph);
  auto katz_centrality = katz_alg::SetKatz(*graph);
  ASSERT_TRUE(CompareRankingSort(katz_centrality, {1, 4, 0, 3, 5, 2, 6}));

  graph = mg_generate::BuildGraph({0, 1, 2, 3, 4, 5, 6}, {}, mg_graph::GraphType::kDirectedGraph);
  katz_centrality = katz_alg::UpdateKatz(
      *graph, {}, {}, {}, {}, {{0, 5}, {0, 1}, {1, 4}, {2, 1}, {3, 1}, {4, 0}, {4, 3}, {4, 1}, {5, 0}, {5, 4}});
  ASSERT_TRUE(CompareRankingSort(katz_centrality, {0, 1, 2, 3, 4, 5, 6}));
}

TEST(KatzCentrality, DeleteAllAndRevert) {
  auto graph = mg_generate::BuildGraph({0, 1, 2, 3, 4, 5, 6},
                                       {{0, 5}, {0, 1}, {1, 4}, {2, 1}, {3, 1}, {4, 0}, {4, 3}, {4, 1}, {5, 0}, {5, 4}},
                                       mg_graph::GraphType::kDirectedGraph);
  auto katz_centrality = katz_alg::SetKatz(*graph);
  ASSERT_TRUE(CompareRankingSort(katz_centrality, {1, 4, 0, 3, 5, 2, 6}));

  graph = mg_generate::BuildGraph({0, 1, 2, 3, 4, 5, 6}, {}, mg_graph::GraphType::kDirectedGraph);
  katz_centrality = katz_alg::UpdateKatz(
      *graph, {}, {}, {}, {}, {{0, 5}, {0, 1}, {1, 4}, {2, 1}, {3, 1}, {4, 0}, {4, 3}, {4, 1}, {5, 0}, {5, 4}});
  ASSERT_TRUE(CompareRankingSort(katz_centrality, {0, 1, 2, 3, 4, 5, 6}));

  graph = mg_generate::BuildGraph({0, 1, 2, 3, 4, 5, 6},
                                  {{0, 5}, {0, 1}, {1, 4}, {2, 1}, {3, 1}, {4, 0}, {4, 3}, {4, 1}, {5, 0}, {5, 4}},
                                  mg_graph::GraphType::kDirectedGraph);
  katz_centrality =
      katz_alg::UpdateKatz(*graph, {}, {{0, 5}, {0, 1}, {1, 4}, {2, 1}, {3, 1}, {4, 0}, {4, 3}, {4, 1}, {5, 0}, {5, 4}},
                           {0, 1, 2, 3, 4, 5, 6, 7, 8, 9}, {}, {});
  ASSERT_TRUE(CompareRankingSort(katz_centrality, {1, 4, 0, 3, 5, 2, 6}));
}

TEST(KatzCentrality, DeleteNodes) {
  auto graph = mg_generate::BuildGraph({0, 1, 2, 3, 4, 5, 6},
                                       {{0, 5}, {0, 1}, {1, 4}, {2, 1}, {3, 1}, {4, 0}, {4, 3}, {4, 1}, {5, 0}, {5, 4}},
                                       mg_graph::GraphType::kDirectedGraph);
  auto katz_centrality = katz_alg::SetKatz(*graph);
  ASSERT_TRUE(CompareRankingSort(katz_centrality, {1, 4, 0, 3, 5, 2, 6}));

  graph = mg_generate::BuildGraph({0, 1, 2, 3, 5, 6}, {{0, 5}, {0, 1}, {2, 1}, {3, 1}, {5, 0}},
                                  mg_graph::GraphType::kDirectedGraph);
  katz_centrality = katz_alg::UpdateKatz(*graph, {}, {}, {}, {4}, {{1, 4}, {4, 0}, {4, 3}, {4, 1}, {5, 4}});
  ASSERT_TRUE(CompareRankingSort(katz_centrality, {1, 0, 5, 2, 3, 6}));
}

TEST(KatzCentrality, KatzRankingExample_15) {
  auto graph = mg_generate::BuildGraph({0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14},
                                       {{0, 11}, {1, 9}, {3, 2}, {4, 3}, {10, 1}, {10, 11}, {11, 1}, {14, 8}},
                                       mg_graph::GraphType::kDirectedGraph);
  auto katz_centrality = katz_alg::SetKatz(*graph);
  ASSERT_TRUE(CompareRankingSort(katz_centrality, {1, 11, 9, 2, 3, 8, 0, 4, 5, 6, 7, 10, 12, 13, 14}));

  graph = mg_generate::BuildGraph(
      {0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 25},
      {{0, 11}, {0, 25}, {1, 9}, {1, 25}, {4, 3}, {4, 25}, {10, 11}, {11, 1}, {11, 3}, {12, 2}, {13, 25}, {14, 8}},
      mg_graph::GraphType::kDirectedGraph);
  katz_centrality = katz_alg::UpdateKatz(*graph, {25}, {{0, 25}, {1, 25}, {4, 25}, {13, 25}, {12, 2}, {11, 3}},
                                         {1, 3, 5, 10, 9, 8}, {}, {{10, 1}, {3, 2}});
  ASSERT_TRUE(CompareRankingSort(katz_centrality, {25, 3, 11, 1, 9, 2, 8, 0, 4, 5, 6, 7, 10, 12, 13, 14}));
}

TEST(KatzCentrality, KatzRankingExample_16) {
  auto graph =
      mg_generate::BuildGraph({0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15},
                              {{0, 2},  {0, 6},  {0, 5},  {1, 7},  {3, 0},  {3, 2},   {5, 11}, {6, 7},  {6, 10},
                               {6, 2},  {7, 4},  {7, 3},  {8, 7},  {8, 4},  {8, 1},   {9, 11}, {9, 2},  {11, 6},
                               {12, 3}, {12, 2}, {12, 8}, {13, 1}, {13, 7}, {13, 10}, {14, 8}, {14, 3}, {14, 11}},
                              mg_graph::GraphType::kDirectedGraph);
  auto katz_centrality = katz_alg::SetKatz(*graph);
  ASSERT_TRUE(CompareRankingSort(katz_centrality, {2, 7, 3, 4, 11, 6, 10, 1, 8, 0, 5, 9, 12, 13, 14, 15}));

  graph = mg_generate::BuildGraph({0, 1, 2, 3, 5, 6, 7, 8, 9, 10, 12, 13, 14, 15},
                                  {{0, 2},
                                   {0, 5},
                                   {1, 7},
                                   {3, 0},
                                   {6, 7},
                                   {6, 10},
                                   {7, 3},
                                   {9, 2},
                                   {12, 3},
                                   {12, 2},
                                   {12, 8},
                                   {13, 1},
                                   {13, 7},
                                   {14, 8},
                                   {14, 13}},
                                  mg_graph::GraphType::kDirectedGraph);
  katz_centrality = katz_alg::UpdateKatz(
      *graph, {}, {{6, 7}, {14, 13}}, {4, 14}, {4, 11},
      {{7, 4}, {8, 4}, {11, 6}, {9, 11}, {14, 11}, {5, 11}, {0, 6}, {3, 2}, {8, 7}, {6, 2}, {8, 1}, {13, 10}, {14, 3}});
  ASSERT_TRUE(CompareRankingSort(katz_centrality, {7, 2, 3, 8, 0, 5, 1, 10, 13, 6, 9, 12, 14, 15}));
}

TEST(KatzCentrality, KatzRankingExample_18) {
  auto graph = mg_generate::BuildGraph({0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17},
                                       {{0, 17},
                                        {0, 5},
                                        {1, 0},
                                        {2, 13},
                                        {2, 6},
                                        {3, 0},
                                        {3, 17},
                                        {6, 17},
                                        {6, 10},
                                        {7, 4},
                                        {7, 16},
                                        {8, 7},
                                        {8, 4},
                                        {13, 7},
                                        {13, 10},
                                        {14, 8}},
                                       mg_graph::GraphType::kDirectedGraph);
  auto katz_centrality = katz_alg::SetKatz(*graph);
  ASSERT_TRUE(CompareRankingSort(katz_centrality, {17, 4, 7, 10, 0, 16, 5, 6, 8, 13, 1, 2, 3, 9, 11, 12, 14, 15}));

  graph = mg_generate::BuildGraph(
      {0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17},
      {{0, 17}, {0, 5}, {2, 6}, {3, 17}, {6, 17}, {6, 10}, {6, 16}, {7, 4}, {7, 16}, {8, 4}, {9, 12}, {13, 7}, {14, 8}},
      mg_graph::GraphType::kDirectedGraph);
  katz_centrality =
      katz_alg::UpdateKatz(*graph, {}, {{9, 12}, {6, 16}}, {10, 6}, {}, {{8, 7}, {13, 10}, {2, 13}, {3, 0}, {1, 0}});
  ASSERT_TRUE(CompareRankingSort(katz_centrality, {17, 4, 16, 10, 5, 6, 7, 8, 12, 0, 1, 2, 3, 9, 11, 13, 14, 15}));
}

TEST(KatzCentrality, KatzRankingExample_19) {
  auto graph = mg_generate::BuildGraph(
      {0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18},
      {{0, 2}, {0, 5}, {2, 18}, {3, 0},  {3, 17}, {3, 2},   {3, 12},  {6, 7},  {6, 10}, {7, 4},   {7, 14},
       {8, 7}, {8, 4}, {8, 1},  {12, 3}, {13, 1}, {13, 10}, {14, 17}, {16, 0}, {17, 6}, {17, 13}, {18, 8}},
      mg_graph::GraphType::kDirectedGraph);
  auto katz_centrality = katz_alg::SetKatz(*graph);
  ASSERT_TRUE(CompareRankingSort(katz_centrality, {4, 2, 10, 1, 7, 17, 0, 18, 6, 13, 14, 5, 8, 3, 12, 9, 11, 15, 16}));

  graph = mg_generate::BuildGraph({0, 1, 2, 3, 4, 5, 6, 7, 9, 11, 12, 13, 15, 16, 17, 18},
                                  {{0, 2},
                                   {0, 5},
                                   {0, 9},
                                   {1, 15},
                                   {2, 18},
                                   {3, 0},
                                   {3, 17},
                                   {3, 2},
                                   {3, 12},
                                   {6, 7},
                                   {7, 4},
                                   {12, 3},
                                   {13, 1},
                                   {16, 0},
                                   {17, 6},
                                   {17, 13},
                                   {18, 7}},
                                  mg_graph::GraphType::kDirectedGraph);
  katz_centrality = katz_alg::UpdateKatz(*graph, {}, {{0, 9}, {1, 15}, {18, 7}}, {2, 3, 16}, {14, 10, 8},
                                         {{14, 17}, {7, 14}, {13, 10}, {6, 10}, {8, 7}, {8, 4}, {8, 1}, {18, 8}});
  ASSERT_TRUE(CompareRankingSort(katz_centrality, {2, 7, 0, 18, 4, 5, 9, 1, 3, 6, 12, 13, 15, 17, 11, 16}));
}

TEST(KatzCentrality, SetAndGet) {
  auto graph = mg_generate::BuildGraph({0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14},
                                       {{0, 11}, {1, 9}, {3, 2}, {4, 3}, {10, 1}, {10, 11}, {11, 1}, {14, 8}},
                                       mg_graph::GraphType::kDirectedGraph);
  auto katz_centrality = katz_alg::SetKatz(*graph);
  ASSERT_TRUE(CompareRankingSort(katz_centrality, {1, 11, 9, 2, 3, 8, 0, 4, 5, 6, 7, 10, 12, 13, 14}));

  graph = mg_generate::BuildGraph({0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14},
                                  {{0, 11}, {1, 9}, {3, 2}, {4, 3}, {10, 1}, {10, 11}, {11, 1}, {14, 8}},
                                  mg_graph::GraphType::kDirectedGraph);
  katz_centrality = katz_alg::SetKatz(*graph);
  ASSERT_TRUE(CompareRankingSort(katz_centrality, {1, 11, 9, 2, 3, 8, 0, 4, 5, 6, 7, 10, 12, 13, 14}));
}

TEST(KatzCentrality, GetOnInconsistedGraph) {
  auto graph = mg_generate::BuildGraph({0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14},
                                       {{0, 11}, {1, 9}, {3, 2}, {4, 3}, {10, 1}, {10, 11}, {11, 1}, {14, 8}},
                                       mg_graph::GraphType::kDirectedGraph);
  auto katz_centrality = katz_alg::SetKatz(*graph);
  ASSERT_TRUE(CompareRankingSort(katz_centrality, {1, 11, 9, 2, 3, 8, 0, 4, 5, 6, 7, 10, 12, 13, 14}));

  graph = mg_generate::BuildGraph(
      {0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 25},
      {{0, 11}, {0, 25}, {1, 9}, {1, 25}, {4, 3}, {4, 25}, {10, 11}, {11, 1}, {11, 3}, {12, 2}, {13, 25}, {14, 8}},
      mg_graph::GraphType::kDirectedGraph);
  EXPECT_ANY_THROW(katz_alg::GetKatz(*graph));
}

TEST(KatzCentrality, GetOnNonInitialized) {
  auto graph = mg_generate::BuildGraph({0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14},
                                       {{0, 11}, {1, 9}, {3, 2}, {4, 3}, {10, 1}, {10, 11}, {11, 1}, {14, 8}},
                                       mg_graph::GraphType::kDirectedGraph);
  auto katz_centrality = katz_alg::GetKatz(*graph);
  ASSERT_TRUE(CompareRankingSort(katz_centrality, {1, 11, 9, 2, 3, 8, 0, 4, 5, 6, 7, 10, 12, 13, 14}));
}
