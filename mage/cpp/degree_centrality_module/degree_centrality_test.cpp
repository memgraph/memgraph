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

#include <tuple>

#include <gtest/gtest.h>
#include <mg_generate.hpp>
#include <mg_graph.hpp>
#include <mg_test_utils.hpp>

#include "algorithm/degree_centrality.hpp"

class DegreeCentralityTest
    : public testing::TestWithParam<
          std::tuple<mg_graph::Graph<>, std::vector<double>, degree_centrality_alg::AlgorithmType>> {};

TEST_P(DegreeCentralityTest, ParametrizedTest) {
  auto graph = std::get<0>(GetParam());
  auto expected = std::get<1>(GetParam());
  auto algorithm_type = std::get<2>(GetParam());
  auto results = degree_centrality_alg::GetDegreeCentrality(graph, algorithm_type);
  ASSERT_TRUE(mg_test_utility::TestEqualVectors(results, expected));
}

INSTANTIATE_TEST_SUITE_P(
    DegreeCentrality, DegreeCentralityTest,
    testing::Values(
        std::make_tuple(*mg_generate::BuildGraph(0, {}, mg_graph::GraphType::kDirectedGraph), std::vector<double>{},
                        degree_centrality_alg::AlgorithmType::kUndirected),
        std::make_tuple(*mg_generate::BuildGraph(5, {{0, 4}, {2, 3}}, mg_graph::GraphType::kDirectedGraph),
                        std::vector<double>{0.2500, 0.0000, 0.2500, 0.2500, 0.2500},
                        degree_centrality_alg::AlgorithmType::kUndirected),
        std::make_tuple(
            *mg_generate::BuildGraph(
                10, {{0, 4}, {0, 8}, {1, 5}, {1, 8}, {2, 6}, {3, 5}, {4, 7}, {5, 6}, {5, 8}, {6, 8}, {7, 9}, {8, 9}},
                mg_graph::GraphType::kDirectedGraph),
            std::vector<double>{0.2222, 0.2222, 0.1111, 0.1111, 0.2222, 0.4444, 0.3333, 0.2222, 0.5556, 0.2222},
            degree_centrality_alg::AlgorithmType::kUndirected),
        std::make_tuple(*mg_generate::BuildGraph(15, {{0, 4},  {0, 8},  {0, 13}, {1, 3},   {1, 8},   {1, 13}, {2, 8},
                                                      {2, 11}, {2, 13}, {3, 5},  {3, 8},   {3, 9},   {3, 11}, {3, 13},
                                                      {4, 7},  {4, 8},  {4, 11}, {5, 13},  {6, 7},   {6, 8},  {6, 9},
                                                      {6, 13}, {7, 14}, {8, 10}, {10, 13}, {12, 13}, {13, 14}},
                                                 mg_graph::GraphType::kDirectedGraph),
                        std::vector<double>{0.2143, 0.2143, 0.2143, 0.4286, 0.2857, 0.1429, 0.2857, 0.2143, 0.5000,
                                            0.1429, 0.1429, 0.2143, 0.0714, 0.6429, 0.1429},
                        degree_centrality_alg::AlgorithmType::kUndirected)));

INSTANTIATE_TEST_SUITE_P(
    InDegreeCentrality, DegreeCentralityTest,
    testing::Values(
        std::make_tuple(*mg_generate::BuildGraph(0, {}, mg_graph::GraphType::kDirectedGraph), std::vector<double>{},
                        degree_centrality_alg::AlgorithmType::kIn),
        std::make_tuple(
            *mg_generate::BuildGraph(5, {{0, 4}, {1, 4}, {3, 0}, {3, 4}}, mg_graph::GraphType::kDirectedGraph),
            std::vector<double>{0.2500, 0.0000, 0.0000, 0.0000, 0.7500}, degree_centrality_alg::AlgorithmType::kIn),
        std::make_tuple(*mg_generate::BuildGraph(10, {{0, 4}, {0, 8}, {1, 4}, {1, 7}, {2, 3}, {2, 8}, {3, 6}, {3, 9},
                                                      {4, 1}, {4, 5}, {4, 8}, {4, 9}, {5, 1}, {5, 3}, {5, 8}, {5, 9},
                                                      {6, 2}, {7, 4}, {7, 6}, {7, 8}, {7, 9}, {8, 3}, {9, 2}, {9, 4}},
                                                 mg_graph::GraphType::kDirectedGraph),
                        std::vector<double>{0.0000, 0.2222, 0.2222, 0.3333, 0.4444, 0.1111, 0.2222, 0.1111, 0.5556,
                                            0.4444},
                        degree_centrality_alg::AlgorithmType::kIn),

        std::make_tuple(*mg_generate::BuildGraph(
                            15, {{0, 4},  {0, 8},   {0, 13},  {1, 2},   {1, 7},  {1, 12}, {2, 5},   {2, 8},  {2, 10},
                                 {2, 13}, {3, 1},   {3, 2},   {3, 5},   {3, 7},  {3, 11}, {3, 12},  {4, 0},  {4, 12},
                                 {4, 14}, {5, 0},   {5, 1},   {5, 6},   {5, 14}, {6, 1},  {6, 14},  {7, 4},  {7, 6},
                                 {7, 8},  {7, 10},  {7, 12},  {7, 13},  {8, 0},  {8, 5},  {8, 10},  {8, 11}, {8, 13},
                                 {8, 14}, {9, 1},   {9, 2},   {9, 3},   {9, 6},  {9, 8},  {9, 10},  {9, 11}, {9, 13},
                                 {9, 14}, {10, 1},  {10, 2},  {10, 4},  {10, 5}, {10, 9}, {10, 13}, {11, 1}, {11, 2},
                                 {11, 5}, {11, 10}, {11, 12}, {11, 14}, {12, 1}, {12, 3}, {12, 4},  {12, 5}, {12, 6},
                                 {12, 8}, {12, 9},  {12, 10}, {13, 1},  {13, 4}, {13, 5}, {13, 6},  {13, 7}, {13, 9},
                                 {14, 3}, {14, 5},  {14, 6},  {14, 13}},
                            mg_graph::GraphType::kDirectedGraph),
                        std::vector<double>{0.2143, 0.5714, 0.3571, 0.2143, 0.3571, 0.5714, 0.4286, 0.2143, 0.3571,
                                            0.2143, 0.4286, 0.2143, 0.3571, 0.5000, 0.4286},
                        degree_centrality_alg::AlgorithmType::kIn)));

INSTANTIATE_TEST_SUITE_P(
    OutDegreeCentrality, DegreeCentralityTest,
    testing::Values(
        std::make_tuple(*mg_generate::BuildGraph(0, {}, mg_graph::GraphType::kDirectedGraph), std::vector<double>{},
                        degree_centrality_alg::AlgorithmType::kOut),
        std::make_tuple(*mg_generate::BuildGraph(5, {{0, 1}, {0, 4}, {2, 0}, {2, 1}, {3, 1}, {4, 0}, {4, 3}},
                                                 mg_graph::GraphType::kDirectedGraph),
                        std::vector<double>{0.5000, 0.0000, 0.5000, 0.2500, 0.5000},
                        degree_centrality_alg::AlgorithmType::kOut),
        std::make_tuple(
            *mg_generate::BuildGraph(10, {{0, 1}, {0, 4}, {0, 9}, {1, 0}, {1, 5}, {1, 8}, {2, 1}, {2, 3}, {2, 7},
                                          {2, 9}, {3, 0}, {3, 4}, {3, 5}, {3, 6}, {3, 8}, {3, 9}, {4, 3}, {4, 7},
                                          {6, 2}, {6, 3}, {6, 7}, {7, 9}, {8, 0}, {8, 5}, {9, 2}, {9, 7}},
                                     mg_graph::GraphType::kDirectedGraph),
            std::vector<double>{0.3333, 0.3333, 0.4444, 0.6667, 0.2222, 0.0000, 0.3333, 0.1111, 0.2222, 0.2222},
            degree_centrality_alg::AlgorithmType::kOut),
        std::make_tuple(*mg_generate::BuildGraph(
                            15, {{0, 1},   {0, 4},   {0, 9},   {0, 10},  {0, 14},  {1, 3},   {1, 6},  {1, 7},  {1, 11},
                                 {1, 13},  {1, 14},  {2, 3},   {2, 4},   {2, 5},   {2, 7},   {2, 8},  {2, 12}, {3, 0},
                                 {4, 0},   {4, 1},   {4, 5},   {5, 1},   {5, 2},   {5, 8},   {5, 14}, {6, 4},  {6, 8},
                                 {7, 2},   {7, 5},   {8, 0},   {8, 1},   {8, 2},   {8, 9},   {8, 12}, {8, 13}, {8, 14},
                                 {9, 1},   {9, 2},   {9, 5},   {9, 6},   {9, 8},   {9, 10},  {10, 0}, {10, 3}, {10, 4},
                                 {10, 7},  {10, 11}, {10, 12}, {10, 13}, {11, 0},  {11, 4},  {11, 8}, {12, 0}, {12, 1},
                                 {12, 10}, {12, 11}, {12, 14}, {13, 3},  {13, 10}, {13, 14}, {14, 0}, {14, 2}, {14, 7},
                                 {14, 8},  {14, 13}},
                            mg_graph::GraphType::kDirectedGraph),
                        std::vector<double>{0.3571, 0.4286, 0.4286, 0.0714, 0.2143, 0.2857, 0.1429, 0.1429, 0.5000,
                                            0.4286, 0.5000, 0.2143, 0.3571, 0.2143, 0.3571},
                        degree_centrality_alg::AlgorithmType::kOut)

            ));

int main(int argc, char **argv) {
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
