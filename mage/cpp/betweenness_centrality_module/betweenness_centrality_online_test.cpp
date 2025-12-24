// Copyright 2025 Memgraph Ltd.
//
// Licensed as a Memgraph Enterprise file under the Memgraph Enterprise
// License (the "License"); by using this file, you agree to be bound by the terms of the License, and you may not use
// this file except in compliance with the License. You may obtain a copy of the License at https://memgraph.com/legal.

#include <gtest/gtest.h>

#include <mg_generate.hpp>
#include <mg_test_utils.hpp>

#include "../biconnected_components_module/algorithm/biconnected_components.hpp"
#include "algorithm/betweenness_centrality.hpp"
#include "algorithm_online/betweenness_centrality_online.hpp"

TEST(OnlineBC, SetBC) {
  const auto example_graph = mg_generate::BuildGraph(
      20, {{0, 1},   {0, 3},   {0, 9},   {0, 10},  {1, 2},   {2, 3},   {2, 4},  {3, 5},  {4, 7},
           {4, 14},  {4, 18},  {5, 6},   {5, 11},  {5, 13},  {6, 7},   {8, 9},  {8, 10}, {11, 12},
           {12, 13}, {14, 16}, {15, 16}, {15, 17}, {16, 18}, {16, 19}, {17, 19}},
      mg_graph::GraphType::kUndirectedGraph);

  auto algorithm = online_bc::OnlineBC();
  const auto computed_BC = algorithm.Set(*example_graph, false);

  const std::unordered_map<std::uint64_t, double> correct_BC = {{0, 51.33333333333333},
                                                                {1, 17.333333333333336},
                                                                {2, 63.33333333333333},
                                                                {3, 61.833333333333336},
                                                                {4, 84.5},
                                                                {5, 56.5},
                                                                {6, 19.833333333333336},
                                                                {7, 21.833333333333332},
                                                                {8, 0.5},
                                                                {9, 8.5},
                                                                {10, 8.5},
                                                                {11, 8.5},
                                                                {12, 0.5},
                                                                {13, 8.5},
                                                                {14, 28.0},
                                                                {15, 8.5},
                                                                {16, 49.0},
                                                                {17, 0.5},
                                                                {18, 28.0},
                                                                {19, 8.5}};

  ASSERT_TRUE(mg_test_utility::TestEqualUnorderedMaps(computed_BC, correct_BC));
}

TEST(OnlineBC, GetBC) {
  const auto example_graph = mg_generate::BuildGraph(
      20, {{0, 1},   {0, 3},   {0, 9},   {0, 10},  {1, 2},   {2, 3},   {2, 4},  {3, 5},  {4, 7},
           {4, 14},  {4, 18},  {5, 6},   {5, 11},  {5, 13},  {6, 7},   {8, 9},  {8, 10}, {11, 12},
           {12, 13}, {14, 16}, {15, 16}, {15, 17}, {16, 18}, {16, 19}, {17, 19}},
      mg_graph::GraphType::kUndirectedGraph);

  auto algorithm = online_bc::OnlineBC();
  algorithm.Set(*example_graph, false);

  const auto computed_BC = algorithm.Get(*example_graph, false);

  const std::unordered_map<std::uint64_t, double> correct_BC = {{0, 51.33333333333333},
                                                                {1, 17.333333333333336},
                                                                {2, 63.33333333333333},
                                                                {3, 61.833333333333336},
                                                                {4, 84.5},
                                                                {5, 56.5},
                                                                {6, 19.833333333333336},
                                                                {7, 21.833333333333332},
                                                                {8, 0.5},
                                                                {9, 8.5},
                                                                {10, 8.5},
                                                                {11, 8.5},
                                                                {12, 0.5},
                                                                {13, 8.5},
                                                                {14, 28.0},
                                                                {15, 8.5},
                                                                {16, 49.0},
                                                                {17, 0.5},
                                                                {18, 28.0},
                                                                {19, 8.5}};

  ASSERT_TRUE(mg_test_utility::TestEqualUnorderedMaps(computed_BC, correct_BC));
}

TEST(OnlineBC, GetBCInconsistent) {
  const auto example_graph = mg_generate::BuildGraph(6, {{0, 1}, {0, 2}, {1, 2}, {2, 3}, {3, 4}, {3, 5}, {4, 5}},
                                                     mg_graph::GraphType::kUndirectedGraph);

  auto algorithm = online_bc::OnlineBC();
  algorithm.Set(*example_graph, false);

  const auto current_graph =
      mg_generate::BuildGraph(5, {{0, 1}, {0, 2}, {1, 2}, {2, 3}, {3, 4}}, mg_graph::GraphType::kUndirectedGraph);

  bool exception_caught = false;
  try {
    algorithm.Get(*current_graph, false);
  } catch (const std::exception &e) {
    exception_caught = true;
  }

  ASSERT_TRUE(exception_caught);
}

TEST(OnlineBC, UpdateBCInsertEdge) {
  const auto prior_graph = mg_generate::BuildGraph(
      20, {{0, 1},   {0, 3},   {0, 9},   {0, 10},  {1, 2},   {2, 3},   {2, 4},  {3, 5},  {4, 7},
           {4, 14},  {4, 18},  {5, 6},   {5, 11},  {5, 13},  {6, 7},   {8, 9},  {8, 10}, {11, 12},
           {12, 13}, {14, 16}, {15, 16}, {15, 17}, {16, 18}, {16, 19}, {17, 19}},
      mg_graph::GraphType::kUndirectedGraph);

  auto algorithm = online_bc::OnlineBC();
  algorithm.Set(*prior_graph, false);

  const auto current_graph = mg_generate::BuildGraph(
      20, {{0, 1},   {0, 3},   {0, 9},   {0, 10},  {1, 2},   {2, 3},   {2, 4},   {3, 5},  {4, 7},
           {4, 14},  {4, 18},  {5, 6},   {5, 11},  {5, 13},  {6, 7},   {8, 9},   {8, 10}, {11, 12},
           {12, 13}, {14, 16}, {15, 16}, {15, 17}, {16, 18}, {16, 19}, {17, 19}, {3, 7}},
      mg_graph::GraphType::kUndirectedGraph);

  const auto updated_BC =
      algorithm.EdgeUpdate(*prior_graph, *current_graph, online_bc::Operation::CREATE_EDGE, {3, 7}, false);

  const std::unordered_map<std::uint64_t, double> correct_BC = {{0, 51.733333333333334},
                                                                {1, 11.33333333333333},
                                                                {2, 42.266666666666666},
                                                                {3, 75.9666666666667},
                                                                {4, 79.86666666666667},
                                                                {5, 51.733333333333334},
                                                                {6, 11.33333333333333},
                                                                {7, 42.266666666666666},
                                                                {8, 0.5},
                                                                {9, 8.5},
                                                                {10, 8.5},
                                                                {11, 8.5},
                                                                {12, 0.5},
                                                                {13, 8.5},
                                                                {14, 28.0},
                                                                {15, 8.5},
                                                                {16, 49.0},
                                                                {17, 0.5},
                                                                {18, 28.0},
                                                                {19, 8.5}};

  ASSERT_TRUE(mg_test_utility::TestEqualUnorderedMaps(updated_BC, correct_BC));
}

TEST(OnlineBC, UpdateBCInsertEdgeAnother) {
  const auto prior_graph = mg_generate::BuildGraph(
      15, {{0, 1},  {0, 2},   {0, 3},   {1, 2},   {1, 4},   {2, 3},   {2, 9},   {3, 13}, {4, 5},  {4, 6},
           {4, 7},  {4, 8},   {5, 7},   {5, 8},   {6, 7},   {6, 8},   {8, 10},  {9, 10}, {9, 12}, {9, 13},
           {9, 14}, {10, 11}, {10, 13}, {10, 14}, {11, 12}, {11, 13}, {11, 14}, {12, 14}},
      mg_graph::GraphType::kUndirectedGraph);

  auto algorithm = online_bc::OnlineBC();
  algorithm.Set(*prior_graph, false);

  const auto current_graph = mg_generate::BuildGraph(
      15, {{0, 1},  {0, 2},   {0, 3},   {1, 2},   {1, 4},   {2, 3},   {2, 9},   {3, 13},  {4, 5},  {4, 6},
           {4, 7},  {4, 8},   {5, 7},   {5, 8},   {6, 7},   {6, 8},   {8, 10},  {9, 10},  {9, 12}, {9, 13},
           {9, 14}, {10, 11}, {10, 13}, {10, 14}, {11, 12}, {11, 13}, {11, 14}, {12, 14}, {0, 13}},
      mg_graph::GraphType::kUndirectedGraph);

  const auto updated_BC =
      algorithm.EdgeUpdate(*prior_graph, *current_graph, online_bc::Operation::CREATE_EDGE, {0, 13}, false);

  const std::unordered_map<std::uint64_t, double> correct_BC = {{0, 5.75},
                                                                {1, 14.183333333333335},
                                                                {2, 8.766666666666667},
                                                                {3, 0.5},
                                                                {4, 16.733333333333334},
                                                                {5, 2.1333333333333333},
                                                                {6, 2.1333333333333333},
                                                                {7, 0.3333333333333333},
                                                                {8, 23.483333333333334},
                                                                {9, 12.233333333333334},
                                                                {10, 27.066666666666666},
                                                                {11, 3.883333333333333},
                                                                {12, 0.41666666666666663},
                                                                {13, 12.083333333333332},
                                                                {14, 2.3}};

  ASSERT_TRUE(mg_test_utility::TestEqualUnorderedMaps(updated_BC, correct_BC));
}

TEST(OnlineBC, UpdateBCDeleteEdge) {
  const auto prior_graph = mg_generate::BuildGraph(
      20, {{0, 1},   {0, 3},   {0, 9},   {0, 10},  {1, 2},   {2, 3},   {2, 4},   {3, 5},  {4, 7},
           {4, 14},  {4, 18},  {5, 6},   {5, 11},  {5, 13},  {6, 7},   {8, 9},   {8, 10}, {11, 12},
           {12, 13}, {14, 16}, {15, 16}, {15, 17}, {16, 18}, {16, 19}, {17, 19}, {3, 7}},
      mg_graph::GraphType::kUndirectedGraph);

  auto algorithm = online_bc::OnlineBC();
  algorithm.Set(*prior_graph, false);

  const auto current_graph = mg_generate::BuildGraph(
      20, {{0, 1},   {0, 3},   {0, 9},   {0, 10},  {1, 2},   {2, 3},   {2, 4},  {3, 5},  {4, 7},
           {4, 14},  {4, 18},  {5, 6},   {5, 11},  {5, 13},  {6, 7},   {8, 9},  {8, 10}, {11, 12},
           {12, 13}, {14, 16}, {15, 16}, {15, 17}, {16, 18}, {16, 19}, {17, 19}},
      mg_graph::GraphType::kUndirectedGraph);

  const auto updated_BC =
      algorithm.EdgeUpdate(*prior_graph, *current_graph, online_bc::Operation::DELETE_EDGE, {3, 7}, false);

  const std::unordered_map<std::uint64_t, double> correct_BC = {{0, 51.33333333333333},
                                                                {1, 17.333333333333336},
                                                                {2, 63.33333333333333},
                                                                {3, 61.833333333333336},
                                                                {4, 84.5},
                                                                {5, 56.5},
                                                                {6, 19.833333333333336},
                                                                {7, 21.833333333333332},
                                                                {8, 0.5},
                                                                {9, 8.5},
                                                                {10, 8.5},
                                                                {11, 8.5},
                                                                {12, 0.5},
                                                                {13, 8.5},
                                                                {14, 28.0},
                                                                {15, 8.5},
                                                                {16, 49.0},
                                                                {17, 0.5},
                                                                {18, 28.0},
                                                                {19, 8.5}};

  ASSERT_TRUE(mg_test_utility::TestEqualUnorderedMaps(updated_BC, correct_BC));
}

TEST(OnlineBC, UpdateBCInsertNode) {
  const auto prior_graph = mg_generate::BuildGraph(
      19,
      {{0, 1},  {0, 3},  {0, 9}, {0, 10}, {1, 2},  {2, 3},   {2, 4},   {3, 5},   {4, 7},   {4, 14},  {4, 18}, {5, 6},
       {5, 11}, {5, 13}, {6, 7}, {8, 9},  {8, 10}, {11, 12}, {12, 13}, {14, 16}, {15, 16}, {15, 17}, {16, 18}},
      mg_graph::GraphType::kUndirectedGraph);

  auto algorithm = online_bc::OnlineBC();
  algorithm.Set(*prior_graph, false);

  const auto current_graph = mg_generate::BuildGraph(
      20,
      {{0, 1},  {0, 3},  {0, 9}, {0, 10}, {1, 2},  {2, 3},   {2, 4},   {3, 5},   {4, 7},   {4, 14},  {4, 18}, {5, 6},
       {5, 11}, {5, 13}, {6, 7}, {8, 9},  {8, 10}, {11, 12}, {12, 13}, {14, 16}, {15, 16}, {15, 17}, {16, 18}},
      mg_graph::GraphType::kUndirectedGraph);

  const auto updated_BC = algorithm.NodeUpdate(online_bc::Operation::CREATE_NODE, 19, false);

  const std::unordered_map<std::uint64_t, double> correct_BC = {{0, 48.33333333333333},
                                                                {1, 15.333333333333334},
                                                                {2, 55.33333333333333},
                                                                {3, 57.83333333333333},
                                                                {4, 71.5},
                                                                {5, 53.5},
                                                                {6, 17.833333333333336},
                                                                {7, 18.833333333333332},
                                                                {8, 0.5},
                                                                {9, 8.0},
                                                                {10, 8.0},
                                                                {11, 8.0},
                                                                {12, 0.5},
                                                                {13, 8.0},
                                                                {14, 21.0},
                                                                {15, 17.0},
                                                                {16, 32.5},
                                                                {17, 0.0},
                                                                {18, 21.0},
                                                                {19, 0.0}};

  ASSERT_TRUE(mg_test_utility::TestEqualUnorderedMaps(updated_BC, correct_BC));
}

TEST(OnlineBC, UpdateBCDeleteNode) {
  const auto prior_graph = mg_generate::BuildGraph(
      20,
      {{0, 1},  {0, 3},  {0, 9}, {0, 10}, {1, 2},  {2, 3},   {2, 4},   {3, 5},   {4, 7},   {4, 14},  {4, 18}, {5, 6},
       {5, 11}, {5, 13}, {6, 7}, {8, 9},  {8, 10}, {11, 12}, {12, 13}, {14, 16}, {15, 16}, {15, 17}, {16, 18}},
      mg_graph::GraphType::kUndirectedGraph);

  auto algorithm = online_bc::OnlineBC();
  algorithm.Set(*prior_graph, false);

  const auto current_graph = mg_generate::BuildGraph(
      19,
      {{0, 1},  {0, 3},  {0, 9}, {0, 10}, {1, 2},  {2, 3},   {2, 4},   {3, 5},   {4, 7},   {4, 14},  {4, 18}, {5, 6},
       {5, 11}, {5, 13}, {6, 7}, {8, 9},  {8, 10}, {11, 12}, {12, 13}, {14, 16}, {15, 16}, {15, 17}, {16, 18}},
      mg_graph::GraphType::kUndirectedGraph);

  const auto updated_BC = algorithm.NodeUpdate(online_bc::Operation::DELETE_NODE, 19, false);

  const std::unordered_map<std::uint64_t, double> correct_BC = {{0, 48.33333333333333},
                                                                {1, 15.333333333333334},
                                                                {2, 55.33333333333333},
                                                                {3, 57.83333333333333},
                                                                {4, 71.5},
                                                                {5, 53.5},
                                                                {6, 17.833333333333336},
                                                                {7, 18.833333333333332},
                                                                {8, 0.5},
                                                                {9, 8.0},
                                                                {10, 8.0},
                                                                {11, 8.0},
                                                                {12, 0.5},
                                                                {13, 8.0},
                                                                {14, 21.0},
                                                                {15, 17.0},
                                                                {16, 32.5},
                                                                {17, 0.0},
                                                                {18, 21.0}};

  ASSERT_TRUE(mg_test_utility::TestEqualUnorderedMaps(updated_BC, correct_BC));
}

TEST(OnlineBC, UpdateBCCreateAttachNode) {
  const auto prior_graph = mg_generate::BuildGraph(
      19,
      {{0, 1},  {0, 3},  {0, 9}, {0, 10}, {1, 2},  {2, 3},   {2, 4},   {3, 5},   {4, 7},   {4, 14},  {4, 18}, {5, 6},
       {5, 11}, {5, 13}, {6, 7}, {8, 9},  {8, 10}, {11, 12}, {12, 13}, {14, 16}, {15, 16}, {15, 17}, {16, 18}},
      mg_graph::GraphType::kUndirectedGraph);

  auto algorithm = online_bc::OnlineBC();
  algorithm.Set(*prior_graph, false);

  const auto current_graph =
      mg_generate::BuildGraph(20, {{0, 1},  {0, 3},   {0, 9},   {0, 10},  {1, 2},   {2, 3},   {2, 4},   {3, 5},
                                   {4, 7},  {4, 14},  {4, 18},  {5, 6},   {5, 11},  {5, 13},  {6, 7},   {8, 9},
                                   {8, 10}, {11, 12}, {12, 13}, {14, 16}, {15, 16}, {15, 17}, {16, 18}, {17, 19}},
                              mg_graph::GraphType::kUndirectedGraph);

  const auto updated_BC =
      algorithm.NodeEdgeUpdate(*current_graph, online_bc::Operation::CREATE_ATTACH_NODE, 19, {17, 19}, false);

  const std::unordered_map<std::uint64_t, double> correct_BC = {{0, 51.33333333333333},
                                                                {1, 17.333333333333336},
                                                                {2, 63.33333333333333},
                                                                {3, 61.833333333333336},
                                                                {4, 84.5},
                                                                {5, 56.5},
                                                                {6, 19.833333333333336},
                                                                {7, 21.833333333333332},
                                                                {8, 0.5},
                                                                {9, 8.5},
                                                                {10, 8.5},
                                                                {11, 8.5},
                                                                {12, 0.5},
                                                                {13, 8.5},
                                                                {14, 28.0},
                                                                {15, 34.0},
                                                                {16, 48.5},
                                                                {17, 18.0},
                                                                {18, 28.0},
                                                                {19, 0.0}};

  ASSERT_TRUE(mg_test_utility::TestEqualUnorderedMaps(updated_BC, correct_BC));
}

TEST(OnlineBC, UpdateBCDetachDeleteNode) {
  const auto prior_graph =
      mg_generate::BuildGraph(20, {{0, 1},  {0, 3},   {0, 9},   {0, 10},  {1, 2},   {2, 3},   {2, 4},   {3, 5},
                                   {4, 7},  {4, 14},  {4, 18},  {5, 6},   {5, 11},  {5, 13},  {6, 7},   {8, 9},
                                   {8, 10}, {11, 12}, {12, 13}, {14, 16}, {15, 16}, {15, 17}, {16, 18}, {16, 19}},
                              mg_graph::GraphType::kUndirectedGraph);

  auto algorithm = online_bc::OnlineBC();
  algorithm.Set(*prior_graph, false);

  const auto current_graph = mg_generate::BuildGraph(
      19,
      {{0, 1},  {0, 3},  {0, 9}, {0, 10}, {1, 2},  {2, 3},   {2, 4},   {3, 5},   {4, 7},   {4, 14},  {4, 18}, {5, 6},
       {5, 11}, {5, 13}, {6, 7}, {8, 9},  {8, 10}, {11, 12}, {12, 13}, {14, 16}, {15, 16}, {15, 17}, {16, 18}},
      mg_graph::GraphType::kUndirectedGraph);

  const auto updated_BC =
      algorithm.NodeEdgeUpdate(*current_graph, online_bc::Operation::DETACH_DELETE_NODE, 19, {16, 19}, false);

  const std::unordered_map<std::uint64_t, double> correct_BC = {{0, 48.33333333333333},
                                                                {1, 15.333333333333334},
                                                                {2, 55.33333333333333},
                                                                {3, 57.83333333333333},
                                                                {4, 71.5},
                                                                {5, 53.5},
                                                                {6, 17.833333333333336},
                                                                {7, 18.833333333333332},
                                                                {8, 0.5},
                                                                {9, 8.0},
                                                                {10, 8.0},
                                                                {11, 8.0},
                                                                {12, 0.5},
                                                                {13, 8.0},
                                                                {14, 21.0},
                                                                {15, 17.0},
                                                                {16, 32.5},
                                                                {17, 0.0},
                                                                {18, 21.0}};

  ASSERT_TRUE(mg_test_utility::TestEqualUnorderedMaps(updated_BC, correct_BC));
}

TEST(OnlineBC, UpdateBCInsertEdgeBothNodesIsolated) {
  const auto prior_graph = mg_generate::BuildGraph(2, {}, mg_graph::GraphType::kUndirectedGraph);

  auto algorithm = online_bc::OnlineBC();
  const auto computed_BC = algorithm.Set(*prior_graph, false);

  const auto current_graph = mg_generate::BuildGraph(2, {{0, 1}}, mg_graph::GraphType::kUndirectedGraph);

  const auto updated_BC =
      algorithm.EdgeUpdate(*prior_graph, *current_graph, online_bc::Operation::CREATE_EDGE, {0, 1}, false);

  const std::unordered_map<std::uint64_t, double> correct_BC = {{0, 0}, {1, 0}};

  ASSERT_TRUE(mg_test_utility::TestEqualUnorderedMaps(updated_BC, correct_BC));
}

TEST(OnlineBC, UpdateBCInsertEdgeOneNodeIsolated) {
  const auto prior_graph = mg_generate::BuildGraph(7, {{0, 1}, {0, 2}, {1, 2}, {2, 3}, {3, 4}, {3, 5}, {4, 5}},
                                                   mg_graph::GraphType::kUndirectedGraph);

  auto algorithm = online_bc::OnlineBC();
  const auto computed_BC = algorithm.Set(*prior_graph, false);

  const auto current_graph = mg_generate::BuildGraph(
      7, {{0, 1}, {0, 2}, {1, 2}, {2, 3}, {3, 4}, {3, 5}, {4, 5}, {4, 6}}, mg_graph::GraphType::kUndirectedGraph);

  const auto updated_BC =
      algorithm.EdgeUpdate(*prior_graph, *current_graph, online_bc::Operation::CREATE_EDGE, {4, 6}, false);

  const std::unordered_map<std::uint64_t, double> correct_BC = {{0, 0}, {1, 0}, {2, 8}, {3, 9}, {4, 5}, {5, 0}, {6, 0}};

  ASSERT_TRUE(mg_test_utility::TestEqualUnorderedMaps(updated_BC, correct_BC));
}

TEST(OnlineBC, UpdateBCInsertEdgeConnectDisconnected) {
  const auto prior_graph = mg_generate::BuildGraph(6, {{0, 1}, {0, 2}, {1, 2}, {3, 4}, {3, 5}, {4, 5}},
                                                   mg_graph::GraphType::kUndirectedGraph);

  auto algorithm = online_bc::OnlineBC();
  const auto computed_BC = algorithm.Set(*prior_graph, false);

  const auto current_graph = mg_generate::BuildGraph(6, {{0, 1}, {0, 2}, {1, 2}, {2, 3}, {3, 4}, {3, 5}, {4, 5}},
                                                     mg_graph::GraphType::kUndirectedGraph);

  const auto updated_BC =
      algorithm.EdgeUpdate(*prior_graph, *current_graph, online_bc::Operation::CREATE_EDGE, {2, 3}, false);

  const std::unordered_map<std::uint64_t, double> correct_BC = {{0, 0}, {1, 0}, {2, 6}, {3, 6}, {4, 0}, {5, 0}};

  ASSERT_TRUE(mg_test_utility::TestEqualUnorderedMaps(updated_BC, correct_BC));
}

TEST(OnlineBC, Normalize) {
  const auto example_graph = mg_generate::BuildGraph(
      20, {{0, 1},   {0, 3},   {0, 9},   {0, 10},  {1, 2},   {2, 3},   {2, 4},  {3, 5},  {4, 7},
           {4, 14},  {4, 18},  {5, 6},   {5, 11},  {5, 13},  {6, 7},   {8, 9},  {8, 10}, {11, 12},
           {12, 13}, {14, 16}, {15, 16}, {15, 17}, {16, 18}, {16, 19}, {17, 19}},
      mg_graph::GraphType::kUndirectedGraph);

  auto algorithm = online_bc::OnlineBC();
  const auto computed_BC = algorithm.Set(*example_graph, true);

  const std::unordered_map<std::uint64_t, double> correct_BC = {
      {0, 0.3001949317738791},     {1, 0.101364522417154},      {2, 0.37037037037037035},   {3, 0.361598440545809},
      {4, 0.4941520467836257},     {5, 0.33040935672514615},    {6, 0.11598440545808968},   {7, 0.1276803118908382},
      {8, 0.0029239766081871343},  {9, 0.049707602339181284},   {10, 0.049707602339181284}, {11, 0.049707602339181284},
      {12, 0.0029239766081871343}, {13, 0.049707602339181284},  {14, 0.16374269005847952},  {15, 0.049707602339181284},
      {16, 0.28654970760233917},   {17, 0.0029239766081871343}, {18, 0.16374269005847952},  {19, 0.049707602339181284}};

  ASSERT_TRUE(mg_test_utility::TestEqualUnorderedMaps(computed_BC, correct_BC));
}

TEST(OnlineBC, NonParallel) {
  const auto example_graph = mg_generate::BuildGraph(
      20, {{0, 1},   {0, 3},   {0, 9},   {0, 10},  {1, 2},   {2, 3},   {2, 4},  {3, 5},  {4, 7},
           {4, 14},  {4, 18},  {5, 6},   {5, 11},  {5, 13},  {6, 7},   {8, 9},  {8, 10}, {11, 12},
           {12, 13}, {14, 16}, {15, 16}, {15, 17}, {16, 18}, {16, 19}, {17, 19}},
      mg_graph::GraphType::kUndirectedGraph);

  auto algorithm = online_bc::OnlineBC();
  const auto computed_BC = algorithm.Set(*example_graph, false, 1);

  const std::unordered_map<std::uint64_t, double> correct_BC = {{0, 51.33333333333333},
                                                                {1, 17.333333333333336},
                                                                {2, 63.33333333333333},
                                                                {3, 61.833333333333336},
                                                                {4, 84.5},
                                                                {5, 56.5},
                                                                {6, 19.833333333333336},
                                                                {7, 21.833333333333332},
                                                                {8, 0.5},
                                                                {9, 8.5},
                                                                {10, 8.5},
                                                                {11, 8.5},
                                                                {12, 0.5},
                                                                {13, 8.5},
                                                                {14, 28.0},
                                                                {15, 8.5},
                                                                {16, 49.0},
                                                                {17, 0.5},
                                                                {18, 28.0},
                                                                {19, 8.5}};

  ASSERT_TRUE(mg_test_utility::TestEqualUnorderedMaps(computed_BC, correct_BC));
}

int main(int argc, char **argv) {
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
