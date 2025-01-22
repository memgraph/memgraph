// Copyright 2025 Memgraph Ltd.
//
// Licensed as a Memgraph Enterprise file under the Memgraph Enterprise
// License (the "License"); by using this file, you agree to be bound by the terms of the License, and you may not use
// this file except in compliance with the License. You may obtain a copy of the License at https://memgraph.com/legal.

#include <gtest/gtest.h>

#include <mg_generate.hpp>

#include "algorithm_online/community_detection.hpp"

TEST(LabelRankT, EmptyGraph) {
  auto empty_graph = mg_generate::BuildGraph(0, {});

  LabelRankT::LabelRankT algorithm = LabelRankT::LabelRankT();
  auto labels = algorithm.SetLabels(std::move(empty_graph));

  std::unordered_map<std::uint64_t, std::int64_t> correct_labels = {};

  ASSERT_TRUE(labels == correct_labels);
}

TEST(LabelRankT, SmallGraph) {
  auto small_graph = mg_generate::BuildGraph(6, {{0, 1}, {0, 2}, {1, 2}, {2, 3}, {3, 4}, {3, 5}, {4, 5}});

  LabelRankT::LabelRankT algorithm = LabelRankT::LabelRankT();
  auto labels = algorithm.SetLabels(std::move(small_graph));

  std::unordered_map<std::uint64_t, std::int64_t> correct_labels = {{0, 1}, {1, 1}, {2, 1}, {3, 2}, {4, 2}, {5, 2}};

  ASSERT_TRUE(labels == correct_labels);
}

TEST(LabelRankT, DisconnectedGraph) {
  auto disconnected_graph = mg_generate::BuildGraph(
      16, {
              {0, 1},  {0, 2},   {0, 3},   {1, 2},   {1, 4},   {2, 3},   {2, 9},   {3, 13},  {4, 5},  {4, 6},
              {4, 7},  {4, 8},   {5, 7},   {5, 8},   {6, 7},   {6, 8},   {8, 10},  {9, 10},  {9, 12}, {9, 13},
              {9, 14}, {10, 11}, {10, 13}, {10, 14}, {11, 12}, {11, 13}, {11, 14}, {12, 14},
          });

  LabelRankT::LabelRankT algorithm = LabelRankT::LabelRankT();
  auto labels = algorithm.SetLabels(std::move(disconnected_graph));

  std::unordered_map<std::uint64_t, std::int64_t> correct_labels = {{0, 1},  {1, 1},  {2, 1},  {3, 1}, {4, 2},  {5, 2},
                                                                    {6, 2},  {7, 2},  {8, 2},  {9, 3}, {10, 3}, {11, 3},
                                                                    {12, 3}, {13, 3}, {14, 3}, {15, 4}};

  ASSERT_TRUE(labels == correct_labels);
}

TEST(LabelRankT, GetLabels) {
  auto example_graph = mg_generate::BuildGraph(
      15, {
              {0, 1},  {0, 2},   {0, 3},   {1, 2},   {1, 4},   {2, 3},   {2, 9},   {3, 13},  {4, 5},  {4, 6},
              {4, 7},  {4, 8},   {5, 7},   {5, 8},   {6, 7},   {6, 8},   {8, 10},  {9, 10},  {9, 12}, {9, 13},
              {9, 14}, {10, 11}, {10, 13}, {10, 14}, {11, 12}, {11, 13}, {11, 14}, {12, 14},
          });

  LabelRankT::LabelRankT algorithm = LabelRankT::LabelRankT();
  algorithm.SetLabels(std::move(example_graph));
  auto labels = algorithm.GetLabels(std::move(example_graph));

  std::unordered_map<std::uint64_t, std::int64_t> correct_labels = {{0, 1},  {1, 1},  {2, 1},  {3, 1},  {4, 2},
                                                                    {5, 2},  {6, 2},  {7, 2},  {8, 2},  {9, 3},
                                                                    {10, 3}, {11, 3}, {12, 3}, {13, 3}, {14, 3}};

  ASSERT_TRUE(labels == correct_labels);
}

TEST(LabelRankT, GetLabelsUninitialized) {
  auto example_graph = mg_generate::BuildGraph(
      15, {
              {0, 1},  {0, 2},   {0, 3},   {1, 2},   {1, 4},   {2, 3},   {2, 9},   {3, 13},  {4, 5},  {4, 6},
              {4, 7},  {4, 8},   {5, 7},   {5, 8},   {6, 7},   {6, 8},   {8, 10},  {9, 10},  {9, 12}, {9, 13},
              {9, 14}, {10, 11}, {10, 13}, {10, 14}, {11, 12}, {11, 13}, {11, 14}, {12, 14},
          });

  LabelRankT::LabelRankT algorithm = LabelRankT::LabelRankT();
  auto labels = algorithm.GetLabels(std::move(example_graph));

  std::unordered_map<std::uint64_t, std::int64_t> correct_labels = {{0, 1},  {1, 1},  {2, 1},  {3, 1},  {4, 2},
                                                                    {5, 2},  {6, 2},  {7, 2},  {8, 2},  {9, 3},
                                                                    {10, 3}, {11, 3}, {12, 3}, {13, 3}, {14, 3}};

  ASSERT_TRUE(labels == correct_labels);
}

TEST(LabelRankT, SetLabels) {
  auto example_graph = mg_generate::BuildGraph(
      15, {
              {0, 1},  {0, 2},   {0, 3},   {1, 2},   {1, 4},   {2, 3},   {2, 9},   {3, 13},  {4, 5},  {4, 6},
              {4, 7},  {4, 8},   {5, 7},   {5, 8},   {6, 7},   {6, 8},   {8, 10},  {9, 10},  {9, 12}, {9, 13},
              {9, 14}, {10, 11}, {10, 13}, {10, 14}, {11, 12}, {11, 13}, {11, 14}, {12, 14},
          });

  LabelRankT::LabelRankT algorithm = LabelRankT::LabelRankT();
  auto labels = algorithm.SetLabels(std::move(example_graph));

  std::unordered_map<std::uint64_t, std::int64_t> correct_labels = {{0, 1},  {1, 1},  {2, 1},  {3, 1},  {4, 2},
                                                                    {5, 2},  {6, 2},  {7, 2},  {8, 2},  {9, 3},
                                                                    {10, 3}, {11, 3}, {12, 3}, {13, 3}, {14, 3}};

  ASSERT_TRUE(labels == correct_labels);
}

TEST(LabelRankT, UpdateLabelsEdgesChanged) {
  auto example_graph = mg_generate::BuildGraph(
      15, {
              {0, 1},  {0, 2},   {0, 3},   {1, 2},   {1, 4},   {2, 3},   {2, 9},   {3, 13},  {4, 5},  {4, 6},
              {4, 7},  {4, 8},   {5, 7},   {5, 8},   {6, 7},   {6, 8},   {8, 10},  {9, 10},  {9, 12}, {9, 13},
              {9, 14}, {10, 11}, {10, 13}, {10, 14}, {11, 12}, {11, 13}, {11, 14}, {12, 14},

          });

  LabelRankT::LabelRankT algorithm = LabelRankT::LabelRankT();
  auto labels = algorithm.SetLabels(std::move(example_graph));

  auto updated_graph = mg_generate::BuildGraph(
      15, {
              {0, 1},  {0, 2},   {0, 3},   {0, 13},  {1, 2},   {1, 4},   {2, 3},   {2, 9},   {3, 9},  {3, 13},
              {4, 5},  {4, 6},   {4, 7},   {4, 8},   {5, 7},   {5, 8},   {6, 7},   {6, 8},   {8, 10}, {9, 10},
              {9, 13}, {10, 11}, {10, 12}, {10, 14}, {11, 12}, {11, 13}, {11, 14}, {12, 14},

          });

  labels = algorithm.UpdateLabels(std::move(updated_graph), {}, {{0, 13}, {3, 9}, {10, 12}}, {},
                                  {{9, 12}, {9, 14}, {10, 13}});

  std::unordered_map<std::uint64_t, std::int64_t> correct_labels = {{0, 1},  {1, 1},  {2, 1},  {3, 1},  {4, 2},
                                                                    {5, 2},  {6, 2},  {7, 2},  {8, 2},  {9, 1},
                                                                    {10, 3}, {11, 3}, {12, 3}, {13, 1}, {14, 3}};

  ASSERT_TRUE(labels == correct_labels);
}

TEST(LabelRankT, UpdateLabelsNodesChanged) {
  auto example_graph = mg_generate::BuildGraph(
      15, {
              {0, 1},  {0, 2},   {0, 3},   {1, 2},   {1, 4},   {2, 3},   {2, 9},   {3, 13},  {4, 5},  {4, 6},
              {4, 7},  {4, 8},   {5, 7},   {5, 8},   {6, 7},   {6, 8},   {8, 10},  {9, 10},  {9, 12}, {9, 13},
              {9, 14}, {10, 11}, {10, 13}, {10, 14}, {11, 12}, {11, 13}, {11, 14}, {12, 14},

          });

  LabelRankT::LabelRankT algorithm = LabelRankT::LabelRankT();
  auto labels = algorithm.SetLabels(std::move(example_graph));

  auto updated_graph = mg_generate::BuildGraph(
      15, {
              {0, 1},  {0, 2},   {0, 3},   {0, 13},  {1, 2},   {1, 4},   {2, 3},   {2, 9},   {3, 9},  {3, 13},
              {4, 5},  {4, 6},   {4, 7},   {4, 8},   {5, 7},   {5, 8},   {6, 7},   {6, 8},   {8, 10}, {9, 10},
              {9, 13}, {10, 11}, {10, 12}, {10, 14}, {11, 12}, {11, 13}, {11, 14}, {12, 14},

          });

  labels = algorithm.UpdateLabels(std::move(updated_graph), {}, {{0, 13}, {3, 9}, {10, 12}}, {},
                                  {{9, 12}, {9, 14}, {10, 13}});

  updated_graph = mg_generate::BuildGraph(
      16, {{0, 1},   {0, 2},   {0, 3},   {0, 13},  {1, 2},   {1, 4},   {1, 7},   {2, 3},  {2, 4},  {2, 6},  {2, 9},
           {3, 9},   {3, 13},  {4, 6},   {4, 7},   {4, 8},   {6, 7},   {8, 10},  {8, 15}, {9, 10}, {9, 13}, {10, 11},
           {10, 12}, {10, 14}, {10, 15}, {11, 12}, {11, 13}, {11, 14}, {12, 14}, {14, 15}

          });

  labels = algorithm.UpdateLabels(std::move(updated_graph), {15}, {{8, 15}, {10, 15}, {14, 15}, {1, 7}, {2, 4}, {2, 6}},
                                  {5}, {{4, 5}, {5, 7}, {5, 8}, {6, 8}});

  std::unordered_map<std::uint64_t, std::int64_t> correct_labels = {{0, 1},  {1, 1},  {2, 1},  {3, 1},  {4, 1},
                                                                    {6, 1},  {7, 1},  {8, 2},  {9, 1},  {10, 2},
                                                                    {11, 2}, {12, 2}, {13, 1}, {14, 2}, {15, 2}};

  ASSERT_TRUE(labels == correct_labels);
}

TEST(LabelRankT, NoLabelsDetected) {
  auto small_graph = mg_generate::BuildGraph(6, {{0, 1}, {0, 2}, {1, 2}, {2, 3}, {3, 4}, {3, 5}, {4, 5}});

  LabelRankT::LabelRankT algorithm = LabelRankT::LabelRankT();
  auto labels = algorithm.SetLabels(std::move(small_graph), false, false, 0.7, 4.0, 0.99, "weight", 1.0, 100, 5);

  std::unordered_map<std::uint64_t, std::int64_t> correct_labels = {{0, -1}, {1, -1}, {2, -1},
                                                                    {3, -1}, {4, -1}, {5, -1}};

  ASSERT_TRUE(labels == correct_labels);
}

TEST(LabelRankT, DirectedGraph) {
  auto directed_graph = mg_generate::BuildGraph(
      6,
      {{0, 1}, {0, 2}, {1, 0}, {1, 2}, {2, 0}, {2, 1}, {2, 3}, {3, 2}, {3, 4}, {3, 5}, {4, 3}, {4, 5}, {5, 3}, {5, 4}},
      mg_graph::GraphType::kDirectedGraph);

  LabelRankT::LabelRankT algorithm = LabelRankT::LabelRankT();
  auto labels = algorithm.SetLabels(std::move(directed_graph), true, false);

  std::unordered_map<std::uint64_t, std::int64_t> correct_labels = {{0, 1}, {1, 1}, {2, 1}, {3, 2}, {4, 2}, {5, 2}};

  ASSERT_TRUE(labels == correct_labels);
}

TEST(LabelRankT, WeightedGraph) {
  auto weighted_graph = mg_generate::BuildWeightedGraph(
      6, {{{0, 1}, 1}, {{0, 2}, 1}, {{1, 2}, 1}, {{2, 3}, 2}, {{3, 4}, 1}, {{3, 5}, 1}, {{4, 5}, 1}},
      mg_graph::GraphType::kUndirectedGraph);

  LabelRankT::LabelRankT algorithm = LabelRankT::LabelRankT();
  auto labels = algorithm.SetLabels(std::move(weighted_graph), false, true);

  std::unordered_map<std::uint64_t, std::int64_t> correct_labels = {{0, 1}, {1, 1}, {2, 1}, {3, 2}, {4, 2}, {5, 2}};

  ASSERT_TRUE(labels == correct_labels);
}

TEST(LabelRankT, DirectedWeightedGraph) {
  auto directed_weighted_graph = mg_generate::BuildWeightedGraph(6,
                                                                 {{{0, 1}, 1},
                                                                  {{0, 2}, 1},
                                                                  {{1, 0}, 1},
                                                                  {{1, 2}, 1},
                                                                  {{2, 0}, 1},
                                                                  {{2, 1}, 1},
                                                                  {{2, 3}, 2},
                                                                  {{3, 2}, 2},
                                                                  {{3, 4}, 1},
                                                                  {{3, 5}, 1},
                                                                  {{4, 3}, 1},
                                                                  {{4, 5}, 1},
                                                                  {{5, 3}, 1},
                                                                  {{5, 4}, 1}},
                                                                 mg_graph::GraphType::kDirectedGraph);

  LabelRankT::LabelRankT algorithm = LabelRankT::LabelRankT();
  auto labels = algorithm.SetLabels(std::move(directed_weighted_graph), true, true);

  std::unordered_map<std::uint64_t, std::int64_t> correct_labels = {{0, 1}, {1, 1}, {2, 1}, {3, 2}, {4, 2}, {5, 2}};

  ASSERT_TRUE(labels == correct_labels);
}
