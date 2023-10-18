// Copyright 2023 Memgraph Ltd.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt; by using this file, you agree to be bound by the terms of the Business Source
// License, and you may not use this file except in compliance with the Business Source License.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

#include <gmock/gmock.h>
#include <gtest/gtest-death-test.h>
#include <gtest/gtest.h>
#include <sys/types.h>
#include <sys/wait.h>
#include <unistd.h>

#include <algorithm>
#include <chrono>
#include <csignal>
#include <filesystem>
#include <iostream>
#include <thread>

#include "dbms/database.hpp"
#include "replication/state.hpp"
#include "storage/v2/config.hpp"
#include "storage/v2/durability/marker.hpp"
#include "storage/v2/durability/paths.hpp"
#include "storage/v2/durability/snapshot.hpp"
#include "storage/v2/durability/version.hpp"
#include "storage/v2/durability/wal.hpp"
#include "storage/v2/edge_accessor.hpp"
#include "storage/v2/indices/label_index_stats.hpp"
#include "storage/v2/inmemory/storage.hpp"
#include "storage/v2/vertex_accessor.hpp"
#include "utils/file.hpp"
#include "utils/logging.hpp"
#include "utils/timer.hpp"

using testing::Contains;
using testing::UnorderedElementsAre;

class DurabilityTest : public ::testing::TestWithParam<bool> {
 protected:
  const uint64_t kNumBaseVertices = 1000;
  const uint64_t kNumBaseEdges = 10000;
  const uint64_t kNumExtendedVertices = 100;
  const uint64_t kNumExtendedEdges = 1000;

  // We don't want to flush the WAL while we are doing operations because the
  // flushing adds a large overhead that slows down execution.
  const uint64_t kFlushWalEvery = (kNumBaseVertices + kNumBaseEdges + kNumExtendedVertices + kNumExtendedEdges) * 2;

  enum class DatasetType {
    ONLY_BASE,
    ONLY_BASE_WITH_EXTENDED_INDICES_AND_CONSTRAINTS,
    ONLY_EXTENDED,
    ONLY_EXTENDED_WITH_BASE_INDICES_AND_CONSTRAINTS,
    BASE_WITH_EXTENDED,
  };

 public:
  DurabilityTest()
      : base_vertex_gids_(kNumBaseVertices, memgraph::storage::Gid::FromUint(std::numeric_limits<uint64_t>::max())),
        base_edge_gids_(kNumBaseEdges, memgraph::storage::Gid::FromUint(std::numeric_limits<uint64_t>::max())),
        extended_vertex_gids_(kNumExtendedVertices,
                              memgraph::storage::Gid::FromUint(std::numeric_limits<uint64_t>::max())),
        extended_edge_gids_(kNumExtendedEdges, memgraph::storage::Gid::FromUint(std::numeric_limits<uint64_t>::max())) {
  }

  void SetUp() override { Clear(); }

  void TearDown() override { Clear(); }

  void CreateBaseDataset(memgraph::storage::Storage *store, bool properties_on_edges) {
    auto label_indexed = store->NameToLabel("base_indexed");
    auto label_unindexed = store->NameToLabel("base_unindexed");
    auto property_id = store->NameToProperty("id");
    auto property_extra = store->NameToProperty("extra");
    auto et1 = store->NameToEdgeType("base_et1");
    auto et2 = store->NameToEdgeType("base_et2");

    {
      // Create label index.
      auto unique_acc = store->UniqueAccess();
      ASSERT_FALSE(unique_acc->CreateIndex(label_unindexed).HasError());
      ASSERT_FALSE(unique_acc->Commit().HasError());
    }
    {
      // Create label index statistics.
      auto acc = store->Access();
      acc->SetIndexStats(label_unindexed, memgraph::storage::LabelIndexStats{1, 2});
      ASSERT_TRUE(acc->GetIndexStats(label_unindexed));
      ASSERT_FALSE(acc->Commit().HasError());
    }
    {
      // Create label+property index.
      auto unique_acc = store->UniqueAccess();
      ASSERT_FALSE(unique_acc->CreateIndex(label_indexed, property_id).HasError());
      ASSERT_FALSE(unique_acc->Commit().HasError());
    }
    {
      // Create label+property index statistics.
      auto acc = store->Access();
      acc->SetIndexStats(label_indexed, property_id, memgraph::storage::LabelPropertyIndexStats{1, 2, 3.4, 5.6, 0.0});
      ASSERT_TRUE(acc->GetIndexStats(label_indexed, property_id));
      ASSERT_FALSE(acc->Commit().HasError());
    }

    {
      // Create existence constraint.
      auto unique_acc = store->UniqueAccess();
      ASSERT_FALSE(unique_acc->CreateExistenceConstraint(label_unindexed, property_id).HasError());
      ASSERT_FALSE(unique_acc->Commit().HasError());
    }
    {
      // Create unique constraint.
      auto unique_acc = store->UniqueAccess();
      ASSERT_FALSE(unique_acc->CreateUniqueConstraint(label_unindexed, {property_id, property_extra}).HasError());
      ASSERT_FALSE(unique_acc->Commit().HasError());
    }

    // Create vertices.
    for (uint64_t i = 0; i < kNumBaseVertices; ++i) {
      auto acc = store->Access();
      auto vertex = acc->CreateVertex();
      base_vertex_gids_[i] = vertex.Gid();
      if (i < kNumBaseVertices / 2) {
        ASSERT_TRUE(vertex.AddLabel(label_indexed).HasValue());
      } else {
        ASSERT_TRUE(vertex.AddLabel(label_unindexed).HasValue());
      }
      if (i < kNumBaseVertices / 3 || i >= kNumBaseVertices / 2) {
        ASSERT_TRUE(
            vertex.SetProperty(property_id, memgraph::storage::PropertyValue(static_cast<int64_t>(i))).HasValue());
      }
      ASSERT_FALSE(acc->Commit().HasError());
    }

    // Create edges.
    for (uint64_t i = 0; i < kNumBaseEdges; ++i) {
      auto acc = store->Access();
      auto vertex1 = acc->FindVertex(base_vertex_gids_[(i / 2) % kNumBaseVertices], memgraph::storage::View::OLD);
      ASSERT_TRUE(vertex1);
      auto vertex2 = acc->FindVertex(base_vertex_gids_[(i / 3) % kNumBaseVertices], memgraph::storage::View::OLD);
      ASSERT_TRUE(vertex2);
      memgraph::storage::EdgeTypeId et;
      if (i < kNumBaseEdges / 2) {
        et = et1;
      } else {
        et = et2;
      }
      auto edgeRes = acc->CreateEdge(&*vertex1, &*vertex2, et);
      ASSERT_TRUE(edgeRes.HasValue());
      auto edge = std::move(edgeRes.GetValue());
      base_edge_gids_[i] = edge.Gid();
      if (properties_on_edges) {
        ASSERT_TRUE(
            edge.SetProperty(property_id, memgraph::storage::PropertyValue(static_cast<int64_t>(i))).HasValue());
      } else {
        auto ret = edge.SetProperty(property_id, memgraph::storage::PropertyValue(static_cast<int64_t>(i)));
        ASSERT_TRUE(ret.HasError());
        ASSERT_EQ(ret.GetError(), memgraph::storage::Error::PROPERTIES_DISABLED);
      }
      ASSERT_FALSE(acc->Commit().HasError());
    }
  }

  void CreateExtendedDataset(memgraph::storage::Storage *store, bool single_transaction = false) {
    auto label_indexed = store->NameToLabel("extended_indexed");
    auto label_unused = store->NameToLabel("extended_unused");
    auto property_count = store->NameToProperty("count");
    auto et3 = store->NameToEdgeType("extended_et3");
    auto et4 = store->NameToEdgeType("extended_et4");

    {
      // Create label index.
      auto unique_acc = store->UniqueAccess();
      ASSERT_FALSE(unique_acc->CreateIndex(label_unused).HasError());
      ASSERT_FALSE(unique_acc->Commit().HasError());
    }
    {
      // Create label index statistics.
      auto acc = store->Access();
      acc->SetIndexStats(label_unused, memgraph::storage::LabelIndexStats{123, 9.87});
      ASSERT_TRUE(acc->GetIndexStats(label_unused));
      ASSERT_FALSE(acc->Commit().HasError());
    }
    {
      // Create label+property index.
      auto unique_acc = store->UniqueAccess();
      ASSERT_FALSE(unique_acc->CreateIndex(label_indexed, property_count).HasError());
      ASSERT_FALSE(unique_acc->Commit().HasError());
    }
    {
      // Create label+property index statistics.
      auto acc = store->Access();
      acc->SetIndexStats(label_indexed, property_count,
                         memgraph::storage::LabelPropertyIndexStats{456798, 312345, 12312312.2, 123123.2, 67876.9});
      ASSERT_TRUE(acc->GetIndexStats(label_indexed, property_count));
      ASSERT_FALSE(acc->Commit().HasError());
    }

    {
      // Create existence constraint.
      auto unique_acc = store->UniqueAccess();
      ASSERT_FALSE(unique_acc->CreateExistenceConstraint(label_unused, property_count).HasError());
      ASSERT_FALSE(unique_acc->Commit().HasError());
    }

    {
      // Create unique constraint.
      auto unique_acc = store->UniqueAccess();
      ASSERT_FALSE(unique_acc->CreateUniqueConstraint(label_unused, {property_count}).HasError());
      ASSERT_FALSE(unique_acc->Commit().HasError());
    }

    // Storage accessor.
    std::unique_ptr<memgraph::storage::Storage::Accessor> acc;
    if (single_transaction) acc = store->Access();

    // Create vertices.
    for (uint64_t i = 0; i < kNumExtendedVertices; ++i) {
      if (!single_transaction) acc = store->Access();
      auto vertex = acc->CreateVertex();
      extended_vertex_gids_[i] = vertex.Gid();
      if (i < kNumExtendedVertices / 2) {
        ASSERT_TRUE(vertex.AddLabel(label_indexed).HasValue());
      }
      if (i < kNumExtendedVertices / 3 || i >= kNumExtendedVertices / 2) {
        ASSERT_TRUE(vertex.SetProperty(property_count, memgraph::storage::PropertyValue("nandare")).HasValue());
      }
      if (!single_transaction) ASSERT_FALSE(acc->Commit().HasError());
    }

    // Create edges.
    for (uint64_t i = 0; i < kNumExtendedEdges; ++i) {
      if (!single_transaction) acc = store->Access();
      auto vertex1 =
          acc->FindVertex(extended_vertex_gids_[(i / 5) % kNumExtendedVertices], memgraph::storage::View::NEW);
      ASSERT_TRUE(vertex1);
      auto vertex2 =
          acc->FindVertex(extended_vertex_gids_[(i / 6) % kNumExtendedVertices], memgraph::storage::View::NEW);
      ASSERT_TRUE(vertex2);
      memgraph::storage::EdgeTypeId et;
      if (i < kNumExtendedEdges / 4) {
        et = et3;
      } else {
        et = et4;
      }
      auto edgeRes = acc->CreateEdge(&*vertex1, &*vertex2, et);
      ASSERT_TRUE(edgeRes.HasValue());
      auto edge = std::move(edgeRes.GetValue());
      extended_edge_gids_[i] = edge.Gid();
      if (!single_transaction) ASSERT_FALSE(acc->Commit().HasError());
    }

    if (single_transaction) ASSERT_FALSE(acc->Commit().HasError());
  }

  void VerifyDataset(memgraph::storage::Storage *store, DatasetType type, bool properties_on_edges,
                     bool verify_info = true) {
    auto base_label_indexed = store->NameToLabel("base_indexed");
    auto base_label_unindexed = store->NameToLabel("base_unindexed");
    auto property_id = store->NameToProperty("id");
    auto property_extra = store->NameToProperty("extra");
    auto et1 = store->NameToEdgeType("base_et1");
    auto et2 = store->NameToEdgeType("base_et2");

    auto extended_label_indexed = store->NameToLabel("extended_indexed");
    auto extended_label_unused = store->NameToLabel("extended_unused");
    auto property_count = store->NameToProperty("count");
    auto et3 = store->NameToEdgeType("extended_et3");
    auto et4 = store->NameToEdgeType("extended_et4");

    // Create storage accessor.
    auto acc = store->Access();

    // Verify indices info.
    {
      auto info = acc->ListAllIndices();
      switch (type) {
        case DatasetType::ONLY_BASE:
          ASSERT_THAT(info.label, UnorderedElementsAre(base_label_unindexed));
          ASSERT_THAT(info.label_property, UnorderedElementsAre(std::make_pair(base_label_indexed, property_id)));
          break;
        case DatasetType::ONLY_EXTENDED:
          ASSERT_THAT(info.label, UnorderedElementsAre(extended_label_unused));
          ASSERT_THAT(info.label_property,
                      UnorderedElementsAre(std::make_pair(base_label_indexed, property_id),
                                           std::make_pair(extended_label_indexed, property_count)));
          break;
        case DatasetType::ONLY_BASE_WITH_EXTENDED_INDICES_AND_CONSTRAINTS:
        case DatasetType::ONLY_EXTENDED_WITH_BASE_INDICES_AND_CONSTRAINTS:
        case DatasetType::BASE_WITH_EXTENDED:
          ASSERT_THAT(info.label, UnorderedElementsAre(base_label_unindexed, extended_label_unused));
          ASSERT_THAT(info.label_property,
                      UnorderedElementsAre(std::make_pair(base_label_indexed, property_id),
                                           std::make_pair(extended_label_indexed, property_count)));
          break;
      }
    }

    // Verify index statistics
    {
      switch (type) {
        case DatasetType::ONLY_BASE: {
          const auto l_stats = acc->GetIndexStats(base_label_unindexed);
          ASSERT_TRUE(l_stats);
          ASSERT_EQ(l_stats->count, 1);
          ASSERT_EQ(l_stats->avg_degree, 2);
          const auto lp_stats = acc->GetIndexStats(base_label_indexed, property_id);
          ASSERT_TRUE(lp_stats);
          ASSERT_EQ(lp_stats->count, 1);
          ASSERT_EQ(lp_stats->distinct_values_count, 2);
          ASSERT_EQ(lp_stats->statistic, 3.4);
          ASSERT_EQ(lp_stats->avg_group_size, 5.6);
          ASSERT_EQ(lp_stats->avg_degree, 0.0);
          break;
        }
        case DatasetType::ONLY_EXTENDED: {
          const auto l_stats = acc->GetIndexStats(extended_label_unused);
          ASSERT_TRUE(l_stats);
          ASSERT_EQ(l_stats->count, 123);
          ASSERT_EQ(l_stats->avg_degree, 9.87);
          const auto lp_stats = acc->GetIndexStats(extended_label_indexed, property_count);
          ASSERT_TRUE(lp_stats);
          ASSERT_EQ(lp_stats->count, 456798);
          ASSERT_EQ(lp_stats->distinct_values_count, 312345);
          ASSERT_EQ(lp_stats->statistic, 12312312.2);
          ASSERT_EQ(lp_stats->avg_group_size, 123123.2);
          ASSERT_EQ(lp_stats->avg_degree, 67876.9);
          break;
        }
        case DatasetType::ONLY_BASE_WITH_EXTENDED_INDICES_AND_CONSTRAINTS:
        case DatasetType::ONLY_EXTENDED_WITH_BASE_INDICES_AND_CONSTRAINTS:
        case DatasetType::BASE_WITH_EXTENDED: {
          const auto l_stats = acc->GetIndexStats(base_label_unindexed);
          ASSERT_TRUE(l_stats);
          ASSERT_EQ(l_stats->count, 1);
          ASSERT_EQ(l_stats->avg_degree, 2);
          const auto lp_stats = acc->GetIndexStats(base_label_indexed, property_id);
          ASSERT_TRUE(lp_stats);
          ASSERT_EQ(lp_stats->count, 1);
          ASSERT_EQ(lp_stats->distinct_values_count, 2);
          ASSERT_EQ(lp_stats->statistic, 3.4);
          ASSERT_EQ(lp_stats->avg_group_size, 5.6);
          ASSERT_EQ(lp_stats->avg_degree, 0.0);
          const auto l_stats_ex = acc->GetIndexStats(extended_label_unused);
          ASSERT_TRUE(l_stats_ex);
          ASSERT_EQ(l_stats_ex->count, 123);
          ASSERT_EQ(l_stats_ex->avg_degree, 9.87);
          const auto lp_stats_ex = acc->GetIndexStats(extended_label_indexed, property_count);
          ASSERT_TRUE(lp_stats_ex);
          ASSERT_EQ(lp_stats_ex->count, 456798);
          ASSERT_EQ(lp_stats_ex->distinct_values_count, 312345);
          ASSERT_EQ(lp_stats_ex->statistic, 12312312.2);
          ASSERT_EQ(lp_stats_ex->avg_group_size, 123123.2);
          ASSERT_EQ(lp_stats_ex->avg_degree, 67876.9);
          break;
        }
      }
    }

    // Verify constraints info.
    {
      auto info = acc->ListAllConstraints();
      switch (type) {
        case DatasetType::ONLY_BASE:
          ASSERT_THAT(info.existence, UnorderedElementsAre(std::make_pair(base_label_unindexed, property_id)));
          ASSERT_THAT(info.unique, UnorderedElementsAre(
                                       std::make_pair(base_label_unindexed, std::set{property_id, property_extra})));
          break;
        case DatasetType::ONLY_EXTENDED:
          ASSERT_THAT(info.existence, UnorderedElementsAre(std::make_pair(extended_label_unused, property_count)));
          ASSERT_THAT(info.unique,
                      UnorderedElementsAre(std::make_pair(extended_label_unused, std::set{property_count})));
          break;
        case DatasetType::ONLY_BASE_WITH_EXTENDED_INDICES_AND_CONSTRAINTS:
        case DatasetType::ONLY_EXTENDED_WITH_BASE_INDICES_AND_CONSTRAINTS:
        case DatasetType::BASE_WITH_EXTENDED:
          ASSERT_THAT(info.existence, UnorderedElementsAre(std::make_pair(base_label_unindexed, property_id),
                                                           std::make_pair(extended_label_unused, property_count)));
          ASSERT_THAT(info.unique,
                      UnorderedElementsAre(std::make_pair(base_label_unindexed, std::set{property_id, property_extra}),
                                           std::make_pair(extended_label_unused, std::set{property_count})));
          break;
      }
    }

    bool have_base_dataset = false;
    bool have_extended_dataset = false;
    switch (type) {
      case DatasetType::ONLY_BASE:
      case DatasetType::ONLY_BASE_WITH_EXTENDED_INDICES_AND_CONSTRAINTS:
        have_base_dataset = true;
        break;
      case DatasetType::ONLY_EXTENDED:
      case DatasetType::ONLY_EXTENDED_WITH_BASE_INDICES_AND_CONSTRAINTS:
        have_extended_dataset = true;
        break;
      case DatasetType::BASE_WITH_EXTENDED:
        have_base_dataset = true;
        have_extended_dataset = true;
        break;
    }

    // Verify base dataset.
    if (have_base_dataset) {
      // Verify vertices.
      for (uint64_t i = 0; i < kNumBaseVertices; ++i) {
        auto vertex = acc->FindVertex(base_vertex_gids_[i], memgraph::storage::View::OLD);
        ASSERT_TRUE(vertex);
        auto labels = vertex->Labels(memgraph::storage::View::OLD);
        ASSERT_TRUE(labels.HasValue());
        if (i < kNumBaseVertices / 2) {
          ASSERT_THAT(*labels, UnorderedElementsAre(base_label_indexed));
        } else {
          ASSERT_THAT(*labels, UnorderedElementsAre(base_label_unindexed));
        }
        auto properties = vertex->Properties(memgraph::storage::View::OLD);
        ASSERT_TRUE(properties.HasValue());
        if (i < kNumBaseVertices / 3 || i >= kNumBaseVertices / 2) {
          ASSERT_EQ(properties->size(), 1);
          ASSERT_EQ((*properties)[property_id], memgraph::storage::PropertyValue(static_cast<int64_t>(i)));
        } else {
          ASSERT_EQ(properties->size(), 0);
        }
      }

      // Verify edges.
      for (uint64_t i = 0; i < kNumBaseEdges; ++i) {
        auto find_edge = [&](auto &edges) -> std::optional<memgraph::storage::EdgeAccessor> {
          for (auto &edge : edges) {
            if (edge.Gid() == base_edge_gids_[i]) {
              return edge;
            }
          }
          return {};
        };

        {
          auto vertex1 = acc->FindVertex(base_vertex_gids_[(i / 2) % kNumBaseVertices], memgraph::storage::View::OLD);
          ASSERT_TRUE(vertex1);
          auto out_edges = vertex1->OutEdges(memgraph::storage::View::OLD);
          ASSERT_TRUE(out_edges.HasValue());
          auto edge1 = find_edge(out_edges->edges);
          ASSERT_TRUE(edge1);
          if (i < kNumBaseEdges / 2) {
            ASSERT_EQ(edge1->EdgeType(), et1);
          } else {
            ASSERT_EQ(edge1->EdgeType(), et2);
          }
          auto properties = edge1->Properties(memgraph::storage::View::OLD);
          ASSERT_TRUE(properties.HasValue());
          if (properties_on_edges) {
            ASSERT_EQ(properties->size(), 1);
            ASSERT_EQ((*properties)[property_id], memgraph::storage::PropertyValue(static_cast<int64_t>(i)));
          } else {
            ASSERT_EQ(properties->size(), 0);
          }
        }

        {
          auto vertex2 = acc->FindVertex(base_vertex_gids_[(i / 3) % kNumBaseVertices], memgraph::storage::View::OLD);
          ASSERT_TRUE(vertex2);
          auto in_edges = vertex2->InEdges(memgraph::storage::View::OLD);
          ASSERT_TRUE(in_edges.HasValue());
          auto edge2 = find_edge(in_edges->edges);
          ASSERT_TRUE(edge2);
          if (i < kNumBaseEdges / 2) {
            ASSERT_EQ(edge2->EdgeType(), et1);
          } else {
            ASSERT_EQ(edge2->EdgeType(), et2);
          }
          auto properties = edge2->Properties(memgraph::storage::View::OLD);
          ASSERT_TRUE(properties.HasValue());
          if (properties_on_edges) {
            ASSERT_EQ(properties->size(), 1);
            ASSERT_EQ((*properties)[property_id], memgraph::storage::PropertyValue(static_cast<int64_t>(i)));
          } else {
            ASSERT_EQ(properties->size(), 0);
          }
        }
      }

      // Verify label indices.
      {
        std::vector<memgraph::storage::VertexAccessor> vertices;
        vertices.reserve(kNumBaseVertices / 2);
        for (auto vertex : acc->Vertices(base_label_unindexed, memgraph::storage::View::OLD)) {
          vertices.push_back(vertex);
        }
        ASSERT_EQ(vertices.size(), kNumBaseVertices / 2);
        std::sort(vertices.begin(), vertices.end(), [](const auto &a, const auto &b) { return a.Gid() < b.Gid(); });
        for (uint64_t i = 0; i < kNumBaseVertices / 2; ++i) {
          ASSERT_EQ(vertices[i].Gid(), base_vertex_gids_[kNumBaseVertices / 2 + i]);
        }
      }

      // Verify label+property index.
      {
        std::vector<memgraph::storage::VertexAccessor> vertices;
        vertices.reserve(kNumBaseVertices / 3);
        for (auto vertex : acc->Vertices(base_label_indexed, property_id, memgraph::storage::View::OLD)) {
          vertices.push_back(vertex);
        }
        ASSERT_EQ(vertices.size(), kNumBaseVertices / 3);
        std::sort(vertices.begin(), vertices.end(), [](const auto &a, const auto &b) { return a.Gid() < b.Gid(); });
        for (uint64_t i = 0; i < kNumBaseVertices / 3; ++i) {
          ASSERT_EQ(vertices[i].Gid(), base_vertex_gids_[i]);
        }
      }
    } else {
      // Verify vertices.
      for (uint64_t i = 0; i < kNumBaseVertices; ++i) {
        auto vertex = acc->FindVertex(base_vertex_gids_[i], memgraph::storage::View::OLD);
        ASSERT_FALSE(vertex);
      }

      if (type == DatasetType::ONLY_EXTENDED_WITH_BASE_INDICES_AND_CONSTRAINTS) {
        // Verify label indices.
        {
          uint64_t count = 0;
          auto iterable = acc->Vertices(base_label_unindexed, memgraph::storage::View::OLD);
          for (auto it = iterable.begin(); it != iterable.end(); ++it) {
            ++count;
          }
          ASSERT_EQ(count, 0);
        }

        // Verify label+property index.
        {
          uint64_t count = 0;
          auto iterable = acc->Vertices(base_label_indexed, property_id, memgraph::storage::View::OLD);
          for (auto it = iterable.begin(); it != iterable.end(); ++it) {
            ++count;
          }
          ASSERT_EQ(count, 0);
        }
      }
    }

    // Verify extended dataset.
    if (have_extended_dataset) {
      // Verify vertices.
      for (uint64_t i = 0; i < kNumExtendedVertices; ++i) {
        auto vertex = acc->FindVertex(extended_vertex_gids_[i], memgraph::storage::View::OLD);
        ASSERT_TRUE(vertex);
        auto labels = vertex->Labels(memgraph::storage::View::OLD);
        ASSERT_TRUE(labels.HasValue());
        if (i < kNumExtendedVertices / 2) {
          ASSERT_THAT(*labels, UnorderedElementsAre(extended_label_indexed));
        }
        auto properties = vertex->Properties(memgraph::storage::View::OLD);
        ASSERT_TRUE(properties.HasValue());
        if (i < kNumExtendedVertices / 3 || i >= kNumExtendedVertices / 2) {
          ASSERT_EQ(properties->size(), 1);
          ASSERT_EQ((*properties)[property_count], memgraph::storage::PropertyValue("nandare"));
        } else {
          ASSERT_EQ(properties->size(), 0);
        }
      }

      // Verify edges.
      for (uint64_t i = 0; i < kNumExtendedEdges; ++i) {
        auto find_edge = [&](auto &edges) -> std::optional<memgraph::storage::EdgeAccessor> {
          for (auto &edge : edges) {
            if (edge.Gid() == extended_edge_gids_[i]) {
              return edge;
            }
          }
          return {};
        };

        {
          auto vertex1 =
              acc->FindVertex(extended_vertex_gids_[(i / 5) % kNumExtendedVertices], memgraph::storage::View::OLD);
          ASSERT_TRUE(vertex1);
          auto out_edges = vertex1->OutEdges(memgraph::storage::View::OLD);
          ASSERT_TRUE(out_edges.HasValue());
          auto edge1 = find_edge(out_edges->edges);
          ASSERT_TRUE(edge1);
          if (i < kNumExtendedEdges / 4) {
            ASSERT_EQ(edge1->EdgeType(), et3);
          } else {
            ASSERT_EQ(edge1->EdgeType(), et4);
          }
          auto properties = edge1->Properties(memgraph::storage::View::OLD);
          ASSERT_TRUE(properties.HasValue());
          ASSERT_EQ(properties->size(), 0);
        }

        {
          auto vertex2 =
              acc->FindVertex(extended_vertex_gids_[(i / 6) % kNumExtendedVertices], memgraph::storage::View::OLD);
          ASSERT_TRUE(vertex2);
          auto in_edges = vertex2->InEdges(memgraph::storage::View::OLD);
          ASSERT_TRUE(in_edges.HasValue());
          auto edge2 = find_edge(in_edges->edges);
          ASSERT_TRUE(edge2);
          if (i < kNumExtendedEdges / 4) {
            ASSERT_EQ(edge2->EdgeType(), et3);
          } else {
            ASSERT_EQ(edge2->EdgeType(), et4);
          }
          auto properties = edge2->Properties(memgraph::storage::View::OLD);
          ASSERT_TRUE(properties.HasValue());
          ASSERT_EQ(properties->size(), 0);
        }
      }

      // Verify label indices.
      {
        std::vector<memgraph::storage::VertexAccessor> vertices;
        vertices.reserve(kNumExtendedVertices / 2);
        for (auto vertex : acc->Vertices(extended_label_unused, memgraph::storage::View::OLD)) {
          vertices.emplace_back(vertex);
        }
        ASSERT_EQ(vertices.size(), 0);
      }

      // Verify label+property index.
      {
        std::vector<memgraph::storage::VertexAccessor> vertices;
        vertices.reserve(kNumExtendedVertices / 3);
        for (auto vertex : acc->Vertices(extended_label_indexed, property_count, memgraph::storage::View::OLD)) {
          vertices.emplace_back(vertex);
        }
        ASSERT_EQ(vertices.size(), kNumExtendedVertices / 3);
        std::sort(vertices.begin(), vertices.end(), [](const auto &a, const auto &b) { return a.Gid() < b.Gid(); });
        for (uint64_t i = 0; i < kNumExtendedVertices / 3; ++i) {
          ASSERT_EQ(vertices[i].Gid(), extended_vertex_gids_[i]);
        }
      }
    } else {
      // Verify vertices.
      for (uint64_t i = 0; i < kNumExtendedVertices; ++i) {
        auto vertex = acc->FindVertex(extended_vertex_gids_[i], memgraph::storage::View::OLD);
        ASSERT_FALSE(vertex);
      }

      if (type == DatasetType::ONLY_BASE_WITH_EXTENDED_INDICES_AND_CONSTRAINTS) {
        // Verify label indices.
        {
          uint64_t count = 0;
          auto iterable = acc->Vertices(extended_label_unused, memgraph::storage::View::OLD);
          for (auto it = iterable.begin(); it != iterable.end(); ++it) {
            ++count;
          }
          ASSERT_EQ(count, 0);
        }

        // Verify label+property index.
        {
          uint64_t count = 0;
          auto iterable = acc->Vertices(extended_label_indexed, property_count, memgraph::storage::View::OLD);
          for (auto it = iterable.begin(); it != iterable.end(); ++it) {
            ++count;
          }
          ASSERT_EQ(count, 0);
        }
      }
    }

    if (verify_info) {
      auto info = store->GetBaseInfo();
      if (have_base_dataset) {
        if (have_extended_dataset) {
          ASSERT_EQ(info.vertex_count, kNumBaseVertices + kNumExtendedVertices);
          ASSERT_EQ(info.edge_count, kNumBaseEdges + kNumExtendedEdges);
        } else {
          ASSERT_EQ(info.vertex_count, kNumBaseVertices);
          ASSERT_EQ(info.edge_count, kNumBaseEdges);
        }
      } else {
        if (have_extended_dataset) {
          ASSERT_EQ(info.vertex_count, kNumExtendedVertices);
          ASSERT_EQ(info.edge_count, kNumExtendedEdges);
        } else {
          ASSERT_EQ(info.vertex_count, 0);
          ASSERT_EQ(info.edge_count, 0);
        }
      }
    }
  }

  std::vector<std::filesystem::path> GetSnapshotsList() {
    return GetFilesList(storage_directory / memgraph::storage::durability::kSnapshotDirectory);
  }

  std::vector<std::filesystem::path> GetBackupSnapshotsList() {
    return GetFilesList(storage_directory / memgraph::storage::durability::kBackupDirectory /
                        memgraph::storage::durability::kSnapshotDirectory);
  }

  std::vector<std::filesystem::path> GetWalsList() {
    return GetFilesList(storage_directory / memgraph::storage::durability::kWalDirectory);
  }

  std::vector<std::filesystem::path> GetBackupWalsList() {
    return GetFilesList(storage_directory / memgraph::storage::durability::kBackupDirectory /
                        memgraph::storage::durability::kWalDirectory);
  }

  void RestoreBackups() {
    {
      auto backup_snapshots = GetBackupSnapshotsList();
      for (const auto &item : backup_snapshots) {
        std::filesystem::rename(
            item, storage_directory / memgraph::storage::durability::kSnapshotDirectory / item.filename());
      }
    }
    {
      auto backup_wals = GetBackupWalsList();
      for (const auto &item : backup_wals) {
        std::filesystem::rename(item,
                                storage_directory / memgraph::storage::durability::kWalDirectory / item.filename());
      }
    }
  }

  std::filesystem::path storage_directory{std::filesystem::temp_directory_path() /
                                          "MG_test_unit_storage_v2_durability"};

 private:
  std::vector<std::filesystem::path> GetFilesList(const std::filesystem::path &path) {
    std::vector<std::filesystem::path> ret;
    std::error_code ec;  // For exception suppression.
    for (auto &item : std::filesystem::directory_iterator(path, ec)) {
      ret.push_back(item.path());
    }
    std::sort(ret.begin(), ret.end());
    std::reverse(ret.begin(), ret.end());
    return ret;
  }

  void Clear() {
    if (!std::filesystem::exists(storage_directory)) return;
    std::filesystem::remove_all(storage_directory);
  }

  std::vector<memgraph::storage::Gid> base_vertex_gids_;
  std::vector<memgraph::storage::Gid> base_edge_gids_;
  std::vector<memgraph::storage::Gid> extended_vertex_gids_;
  std::vector<memgraph::storage::Gid> extended_edge_gids_;
};

void CorruptSnapshot(const std::filesystem::path &path) {
  auto info = memgraph::storage::durability::ReadSnapshotInfo(path);
  spdlog::info("Destroying snapshot {}", path);
  memgraph::utils::OutputFile file;
  file.Open(path, memgraph::utils::OutputFile::Mode::OVERWRITE_EXISTING);
  file.SetPosition(memgraph::utils::OutputFile::Position::SET, info.offset_vertices);
  auto value = static_cast<uint8_t>(memgraph::storage::durability::Marker::TYPE_MAP);
  file.Write(&value, sizeof(value));
  file.Sync();
  file.Close();
}

void DestroyWalFirstDelta(const std::filesystem::path &path) {
  auto info = memgraph::storage::durability::ReadWalInfo(path);
  spdlog::info("Destroying WAL {}", path);
  memgraph::utils::OutputFile file;
  file.Open(path, memgraph::utils::OutputFile::Mode::OVERWRITE_EXISTING);
  file.SetPosition(memgraph::utils::OutputFile::Position::SET, info.offset_deltas);
  auto value = static_cast<uint8_t>(memgraph::storage::durability::Marker::TYPE_MAP);
  file.Write(&value, sizeof(value));
  file.Sync();
  file.Close();
}

void DestroyWalSuffix(const std::filesystem::path &path) {
  auto info = memgraph::storage::durability::ReadWalInfo(path);
  spdlog::info("Destroying WAL {}", path);
  memgraph::utils::OutputFile file;
  file.Open(path, memgraph::utils::OutputFile::Mode::OVERWRITE_EXISTING);
  ASSERT_LT(info.offset_deltas, file.SetPosition(memgraph::utils::OutputFile::Position::RELATIVE_TO_END, -100));
  uint8_t value = 0;
  for (size_t i = 0; i < 100; ++i) {
    file.Write(&value, sizeof(value));
  }
  file.Sync();
  file.Close();
}

INSTANTIATE_TEST_CASE_P(EdgesWithProperties, DurabilityTest, ::testing::Values(true));
INSTANTIATE_TEST_CASE_P(EdgesWithoutProperties, DurabilityTest, ::testing::Values(false));

// NOLINTNEXTLINE(hicpp-special-member-functions)
TEST_P(DurabilityTest, SnapshotOnExit) {
  // Create snapshot.
  {
    memgraph::storage::Config config{.items = {.properties_on_edges = GetParam()},
                                     .durability = {.storage_directory = storage_directory, .snapshot_on_exit = true}};
    memgraph::replication::ReplicationState repl_state{memgraph::storage::ReplicationStateHelper(config)};
    memgraph::dbms::Database db{config, repl_state};
    CreateBaseDataset(db.storage(), GetParam());
    VerifyDataset(db.storage(), DatasetType::ONLY_BASE, GetParam());
    CreateExtendedDataset(db.storage());
    VerifyDataset(db.storage(), DatasetType::BASE_WITH_EXTENDED, GetParam());
  }

  ASSERT_EQ(GetSnapshotsList().size(), 1);
  ASSERT_EQ(GetBackupSnapshotsList().size(), 0);
  ASSERT_EQ(GetWalsList().size(), 0);
  ASSERT_EQ(GetBackupWalsList().size(), 0);

  // Recover snapshot.
  memgraph::storage::Config config{.items = {.properties_on_edges = GetParam()},
                                   .durability = {.storage_directory = storage_directory, .recover_on_startup = true}};
  memgraph::replication::ReplicationState repl_state{memgraph::storage::ReplicationStateHelper(config)};
  memgraph::dbms::Database db{config, repl_state};
  VerifyDataset(db.storage(), DatasetType::BASE_WITH_EXTENDED, GetParam());

  // Try to use the storage.
  {
    auto acc = db.storage()->Access();
    auto vertex = acc->CreateVertex();
    auto edge = acc->CreateEdge(&vertex, &vertex, db.storage()->NameToEdgeType("et"));
    ASSERT_TRUE(edge.HasValue());
    ASSERT_FALSE(acc->Commit().HasError());
  }
}

// NOLINTNEXTLINE(hicpp-special-member-functions)
TEST_P(DurabilityTest, SnapshotPeriodic) {
  // Create snapshot.
  {
    memgraph::storage::Config config{
        .items = {.properties_on_edges = GetParam()},
        .durability = {.storage_directory = storage_directory,
                       .snapshot_wal_mode = memgraph::storage::Config::Durability::SnapshotWalMode::PERIODIC_SNAPSHOT,
                       .snapshot_interval = std::chrono::milliseconds(2000)}};
    memgraph::replication::ReplicationState repl_state{memgraph::storage::ReplicationStateHelper(config)};
    memgraph::dbms::Database db{config, repl_state};
    CreateBaseDataset(db.storage(), GetParam());
    std::this_thread::sleep_for(std::chrono::milliseconds(2500));
  }

  ASSERT_GE(GetSnapshotsList().size(), 1);
  ASSERT_EQ(GetBackupSnapshotsList().size(), 0);
  ASSERT_EQ(GetWalsList().size(), 0);
  ASSERT_EQ(GetBackupWalsList().size(), 0);

  // Recover snapshot.
  memgraph::storage::Config config{.items = {.properties_on_edges = GetParam()},
                                   .durability = {.storage_directory = storage_directory, .recover_on_startup = true}};
  memgraph::replication::ReplicationState repl_state{memgraph::storage::ReplicationStateHelper(config)};
  memgraph::dbms::Database db{config, repl_state};
  VerifyDataset(db.storage(), DatasetType::ONLY_BASE, GetParam());

  // Try to use the storage.
  {
    auto acc = db.storage()->Access();
    auto vertex = acc->CreateVertex();
    auto edge = acc->CreateEdge(&vertex, &vertex, db.storage()->NameToEdgeType("et"));
    ASSERT_TRUE(edge.HasValue());
    ASSERT_FALSE(acc->Commit().HasError());
  }
}

// NOLINTNEXTLINE(hicpp-special-member-functions)
TEST_P(DurabilityTest, SnapshotFallback) {
  // Create snapshot.
  std::size_t number_to_save;
  {
    // DEVNOTE_1: assumes that snapshot disk write takes less than this
    auto const expected_write_time = std::chrono::milliseconds(750);
    auto const snapshot_interval = std::chrono::milliseconds(3000);

    memgraph::storage::Config config{
        .items = {.properties_on_edges = GetParam()},
        .durability = {
            .storage_directory = storage_directory,
            .snapshot_wal_mode = memgraph::storage::Config::Durability::SnapshotWalMode::PERIODIC_SNAPSHOT,
            .snapshot_interval = snapshot_interval,
            .snapshot_retention_count = 10,  // We don't anticipate that we make this many
        }};
    memgraph::replication::ReplicationState repl_state{memgraph::storage::ReplicationStateHelper(config)};
    memgraph::dbms::Database db{config, repl_state};

    auto const ensure_snapshot_is_written = [&](auto &&func) {
      auto const pre_count = GetSnapshotsList().size();
      func();
      // wait long enough to ensure at least one CreateSnapshot has been invoked
      // DEVNOTE_2: no guarantee that it completed, see DEVNOTE_1
      std::this_thread::sleep_for(snapshot_interval + expected_write_time);
      auto const post_count = GetSnapshotsList().size();
      // validate at least one snapshot has happened...hence must have written the writes from func
      ASSERT_GT(post_count, pre_count) << "No snapshot exists to capture the last transaction";
      // TODO: maybe double check by looking at InMemoryStorage's commit log,
      // its oldest active should be newer than the transaction used when running `func`
    };

    ensure_snapshot_is_written([&]() { CreateBaseDataset(db.storage(), GetParam()); });
    number_to_save = GetSnapshotsList().size();
    ensure_snapshot_is_written([&]() { CreateExtendedDataset(db.storage()); });
  }

  ASSERT_EQ(GetBackupSnapshotsList().size(), 0);
  ASSERT_EQ(GetWalsList().size(), 0);
  ASSERT_EQ(GetBackupWalsList().size(), 0);

  // Destroy snapshots.
  {
    auto snapshots = GetSnapshotsList();
    // snapshots order newest first, destroy the newest, preserve number_to_save so that we ONLY_BASE
    auto it = snapshots.begin();
    auto const e = snapshots.end() - number_to_save;
    for (; it != e; ++it) {
      CorruptSnapshot(*it);
    }
  }

  // Recover snapshot.
  memgraph::storage::Config config{.items = {.properties_on_edges = GetParam()},
                                   .durability = {.storage_directory = storage_directory, .recover_on_startup = true}};
  memgraph::replication::ReplicationState repl_state{memgraph::storage::ReplicationStateHelper(config)};
  memgraph::dbms::Database db{config, repl_state};
  VerifyDataset(db.storage(), DatasetType::ONLY_BASE, GetParam());

  // Try to use the storage.
  {
    auto acc = db.storage()->Access();
    auto vertex = acc->CreateVertex();
    auto edge = acc->CreateEdge(&vertex, &vertex, db.storage()->NameToEdgeType("et"));
    ASSERT_TRUE(edge.HasValue());
    ASSERT_FALSE(acc->Commit().HasError());
  }
}

// NOLINTNEXTLINE(hicpp-special-member-functions)
TEST_P(DurabilityTest, SnapshotEverythingCorrupt) {
  // Create unrelated snapshot.
  {
    memgraph::storage::Config config{.items = {.properties_on_edges = GetParam()},
                                     .durability = {.storage_directory = storage_directory, .snapshot_on_exit = true}};
    memgraph::replication::ReplicationState repl_state{memgraph::storage::ReplicationStateHelper(config)};
    memgraph::dbms::Database db{config, repl_state};

    auto acc = db.storage()->Access();
    for (uint64_t i = 0; i < 1000; ++i) {
      acc->CreateVertex();
    }
    ASSERT_FALSE(acc->Commit().HasError());
  }

  ASSERT_EQ(GetSnapshotsList().size(), 1);
  ASSERT_EQ(GetBackupSnapshotsList().size(), 0);
  ASSERT_EQ(GetWalsList().size(), 0);
  ASSERT_EQ(GetBackupWalsList().size(), 0);

  // Get unrelated UUID.
  std::string unrelated_uuid;
  {
    auto snapshots = GetSnapshotsList();
    ASSERT_EQ(snapshots.size(), 1);
    auto info = memgraph::storage::durability::ReadSnapshotInfo(*snapshots.begin());
    unrelated_uuid = info.uuid;
  }

  // Create snapshot.
  {
    memgraph::storage::Config config{
        .items = {.properties_on_edges = GetParam()},
        .durability = {.storage_directory = storage_directory,
                       .snapshot_wal_mode = memgraph::storage::Config::Durability::SnapshotWalMode::PERIODIC_SNAPSHOT,
                       .snapshot_interval = std::chrono::milliseconds(2000)}};
    memgraph::replication::ReplicationState repl_state{memgraph::storage::ReplicationStateHelper(config)};
    memgraph::dbms::Database db{config, repl_state};

    CreateBaseDataset(db.storage(), GetParam());
    std::this_thread::sleep_for(std::chrono::milliseconds(2500));
    CreateExtendedDataset(db.storage());
    std::this_thread::sleep_for(std::chrono::milliseconds(2500));
  }

  ASSERT_GE(GetSnapshotsList().size(), 1);
  ASSERT_EQ(GetBackupSnapshotsList().size(), 1);
  ASSERT_EQ(GetWalsList().size(), 0);
  ASSERT_EQ(GetBackupWalsList().size(), 0);

  // Restore unrelated snapshots.
  RestoreBackups();

  ASSERT_GE(GetSnapshotsList().size(), 2);
  ASSERT_EQ(GetBackupSnapshotsList().size(), 0);
  ASSERT_EQ(GetWalsList().size(), 0);
  ASSERT_EQ(GetBackupWalsList().size(), 0);

  // Destroy all current snapshots.
  {
    auto snapshots = GetSnapshotsList();
    ASSERT_GE(snapshots.size(), 2);
    for (const auto &snapshot : snapshots) {
      auto info = memgraph::storage::durability::ReadSnapshotInfo(snapshot);
      if (info.uuid == unrelated_uuid) {
        spdlog::info("Skipping snapshot {}", snapshot);
        continue;
      }
      CorruptSnapshot(snapshot);
    }
  }

  // Recover snapshot.
  ASSERT_DEATH(([&]() {
                 memgraph::storage::Config config{
                     .items = {.properties_on_edges = GetParam()},
                     .durability = {.storage_directory = storage_directory, .recover_on_startup = true}};
                 memgraph::replication::ReplicationState repl_state{memgraph::storage::ReplicationStateHelper(config)};
                 memgraph::dbms::Database db{config, repl_state};
               }())  // iile
               ,
               "");
}

// NOLINTNEXTLINE(hicpp-special-member-functions)
TEST_P(DurabilityTest, SnapshotRetention) {
  // Create unrelated snapshot.
  {
    memgraph::storage::Config config{.items = {.properties_on_edges = GetParam()},
                                     .durability = {.storage_directory = storage_directory, .snapshot_on_exit = true}};
    memgraph::replication::ReplicationState repl_state{memgraph::storage::ReplicationStateHelper(config)};
    memgraph::dbms::Database db{config, repl_state};
    auto acc = db.storage()->Access();
    for (uint64_t i = 0; i < 1000; ++i) {
      acc->CreateVertex();
    }
    ASSERT_FALSE(acc->Commit().HasError());
  }

  ASSERT_GE(GetSnapshotsList().size(), 1);
  ASSERT_EQ(GetBackupSnapshotsList().size(), 0);
  ASSERT_EQ(GetWalsList().size(), 0);
  ASSERT_EQ(GetBackupWalsList().size(), 0);

  // Create snapshot.
  {
    memgraph::storage::Config config{
        .items = {.properties_on_edges = GetParam()},
        .durability = {.storage_directory = storage_directory,
                       .snapshot_wal_mode = memgraph::storage::Config::Durability::SnapshotWalMode::PERIODIC_SNAPSHOT,
                       .snapshot_interval = std::chrono::milliseconds(2000),
                       .snapshot_retention_count = 3}};
    memgraph::replication::ReplicationState repl_state{memgraph::storage::ReplicationStateHelper(config)};
    memgraph::dbms::Database db{config, repl_state};
    // Restore unrelated snapshots after the database has been started.
    RestoreBackups();
    CreateBaseDataset(db.storage(), GetParam());
    // Allow approximately 5 snapshots to be created.
    std::this_thread::sleep_for(std::chrono::milliseconds(10000));
  }

  ASSERT_EQ(GetSnapshotsList().size(), 1 + 3);
  ASSERT_EQ(GetBackupSnapshotsList().size(), 0);
  ASSERT_EQ(GetWalsList().size(), 0);
  ASSERT_EQ(GetBackupWalsList().size(), 0);

  // Verify that exactly 3 snapshots and 1 unrelated snapshot exist.
  {
    auto snapshots = GetSnapshotsList();
    ASSERT_EQ(snapshots.size(), 1 + 3);
    std::string uuid;
    for (size_t i = 0; i < snapshots.size(); ++i) {
      const auto &path = snapshots[i];
      // This shouldn't throw.
      auto info = memgraph::storage::durability::ReadSnapshotInfo(path);
      if (i == 0) uuid = info.uuid;
      if (i < snapshots.size() - 1) {
        ASSERT_EQ(info.uuid, uuid);
      } else {
        ASSERT_NE(info.uuid, uuid);
      }
    }
  }

  // Recover snapshot.
  memgraph::storage::Config config{.items = {.properties_on_edges = GetParam()},
                                   .durability = {.storage_directory = storage_directory, .recover_on_startup = true}};
  memgraph::replication::ReplicationState repl_state{memgraph::storage::ReplicationStateHelper(config)};
  memgraph::dbms::Database db{config, repl_state};
  VerifyDataset(db.storage(), DatasetType::ONLY_BASE, GetParam());

  // Try to use the storage.
  {
    auto acc = db.storage()->Access();
    auto vertex = acc->CreateVertex();
    auto edge = acc->CreateEdge(&vertex, &vertex, db.storage()->NameToEdgeType("et"));
    ASSERT_TRUE(edge.HasValue());
    ASSERT_FALSE(acc->Commit().HasError());
  }
}

// NOLINTNEXTLINE(hicpp-special-member-functions)
TEST_P(DurabilityTest, SnapshotMixedUUID) {
  // Create snapshot.
  {
    memgraph::storage::Config config{.items = {.properties_on_edges = GetParam()},
                                     .durability = {.storage_directory = storage_directory, .snapshot_on_exit = true}};
    memgraph::replication::ReplicationState repl_state{memgraph::storage::ReplicationStateHelper(config)};
    memgraph::dbms::Database db{config, repl_state};
    CreateBaseDataset(db.storage(), GetParam());
    VerifyDataset(db.storage(), DatasetType::ONLY_BASE, GetParam());
    CreateExtendedDataset(db.storage());
    VerifyDataset(db.storage(), DatasetType::BASE_WITH_EXTENDED, GetParam());
  }

  ASSERT_EQ(GetSnapshotsList().size(), 1);
  ASSERT_EQ(GetBackupSnapshotsList().size(), 0);
  ASSERT_EQ(GetWalsList().size(), 0);
  ASSERT_EQ(GetBackupWalsList().size(), 0);

  // Recover snapshot.
  {
    memgraph::storage::Config config{
        .items = {.properties_on_edges = GetParam()},
        .durability = {.storage_directory = storage_directory, .recover_on_startup = true}};
    memgraph::replication::ReplicationState repl_state{memgraph::storage::ReplicationStateHelper(config)};
    memgraph::dbms::Database db{config, repl_state};
    VerifyDataset(db.storage(), DatasetType::BASE_WITH_EXTENDED, GetParam());
  }

  // Create another snapshot.
  {
    memgraph::storage::Config config{.items = {.properties_on_edges = GetParam()},
                                     .durability = {.storage_directory = storage_directory, .snapshot_on_exit = true}};
    memgraph::replication::ReplicationState repl_state{memgraph::storage::ReplicationStateHelper(config)};
    memgraph::dbms::Database db{config, repl_state};
    CreateBaseDataset(db.storage(), GetParam());
    VerifyDataset(db.storage(), DatasetType::ONLY_BASE, GetParam());
  }

  ASSERT_EQ(GetSnapshotsList().size(), 1);
  ASSERT_EQ(GetBackupSnapshotsList().size(), 1);
  ASSERT_EQ(GetWalsList().size(), 0);
  ASSERT_EQ(GetBackupWalsList().size(), 0);

  // Restore unrelated snapshot.
  RestoreBackups();

  ASSERT_EQ(GetSnapshotsList().size(), 2);
  ASSERT_EQ(GetBackupSnapshotsList().size(), 0);
  ASSERT_EQ(GetWalsList().size(), 0);
  ASSERT_EQ(GetBackupWalsList().size(), 0);

  // Recover snapshot.
  memgraph::storage::Config config{.items = {.properties_on_edges = GetParam()},
                                   .durability = {.storage_directory = storage_directory, .recover_on_startup = true}};
  memgraph::replication::ReplicationState repl_state{memgraph::storage::ReplicationStateHelper(config)};
  memgraph::dbms::Database db{config, repl_state};
  VerifyDataset(db.storage(), DatasetType::ONLY_BASE, GetParam());

  // Try to use the storage.
  {
    auto acc = db.storage()->Access();
    auto vertex = acc->CreateVertex();
    auto edge = acc->CreateEdge(&vertex, &vertex, db.storage()->NameToEdgeType("et"));
    ASSERT_TRUE(edge.HasValue());
    ASSERT_FALSE(acc->Commit().HasError());
  }
}

// NOLINTNEXTLINE(hicpp-special-member-functions)
TEST_P(DurabilityTest, SnapshotBackup) {
  // Create snapshot.
  {
    memgraph::storage::Config config{.items = {.properties_on_edges = GetParam()},
                                     .durability = {.storage_directory = storage_directory, .snapshot_on_exit = true}};
    memgraph::replication::ReplicationState repl_state{memgraph::storage::ReplicationStateHelper(config)};
    memgraph::dbms::Database db{config, repl_state};
    auto acc = db.storage()->Access();
    for (uint64_t i = 0; i < 1000; ++i) {
      acc->CreateVertex();
    }
    ASSERT_FALSE(acc->Commit().HasError());
  }

  ASSERT_EQ(GetSnapshotsList().size(), 1);
  ASSERT_EQ(GetBackupSnapshotsList().size(), 0);
  ASSERT_EQ(GetWalsList().size(), 0);
  ASSERT_EQ(GetBackupWalsList().size(), 0);

  // Start storage without recovery.
  {
    memgraph::storage::Config config{
        .items = {.properties_on_edges = GetParam()},
        .durability = {.storage_directory = storage_directory,
                       .snapshot_wal_mode = memgraph::storage::Config::Durability::SnapshotWalMode::PERIODIC_SNAPSHOT,
                       .snapshot_interval = std::chrono::minutes(20)}};
    memgraph::replication::ReplicationState repl_state{memgraph::storage::ReplicationStateHelper(config)};
    memgraph::dbms::Database db{config, repl_state};
  }

  ASSERT_EQ(GetSnapshotsList().size(), 0);
  ASSERT_EQ(GetBackupSnapshotsList().size(), 1);
  ASSERT_EQ(GetWalsList().size(), 0);
  ASSERT_EQ(GetBackupWalsList().size(), 0);
}

// NOLINTNEXTLINE(hicpp-special-member-functions)
TEST_F(DurabilityTest, SnapshotWithoutPropertiesOnEdgesRecoveryWithPropertiesOnEdges) {
  // Create snapshot.
  {
    memgraph::storage::Config config{.items = {.properties_on_edges = false},
                                     .durability = {.storage_directory = storage_directory, .snapshot_on_exit = true}};
    memgraph::replication::ReplicationState repl_state{memgraph::storage::ReplicationStateHelper(config)};
    memgraph::dbms::Database db{config, repl_state};
    CreateBaseDataset(db.storage(), false);
    VerifyDataset(db.storage(), DatasetType::ONLY_BASE, false);
    CreateExtendedDataset(db.storage());
    VerifyDataset(db.storage(), DatasetType::BASE_WITH_EXTENDED, false);
  }

  ASSERT_EQ(GetSnapshotsList().size(), 1);
  ASSERT_EQ(GetBackupSnapshotsList().size(), 0);
  ASSERT_EQ(GetWalsList().size(), 0);
  ASSERT_EQ(GetBackupWalsList().size(), 0);

  // Recover snapshot.
  memgraph::storage::Config config{.items = {.properties_on_edges = true},
                                   .durability = {.storage_directory = storage_directory, .recover_on_startup = true}};
  memgraph::replication::ReplicationState repl_state{memgraph::storage::ReplicationStateHelper(config)};
  memgraph::dbms::Database db{config, repl_state};
  VerifyDataset(db.storage(), DatasetType::BASE_WITH_EXTENDED, false);

  // Try to use the storage.
  {
    auto acc = db.storage()->Access();
    auto vertex = acc->CreateVertex();
    auto edge = acc->CreateEdge(&vertex, &vertex, db.storage()->NameToEdgeType("et"));
    ASSERT_TRUE(edge.HasValue());
    ASSERT_FALSE(acc->Commit().HasError());
  }
}

// NOLINTNEXTLINE(hicpp-special-member-functions)
TEST_F(DurabilityTest, SnapshotWithPropertiesOnEdgesRecoveryWithoutPropertiesOnEdges) {
  // Create snapshot.
  {
    memgraph::storage::Config config{.items = {.properties_on_edges = true},
                                     .durability = {.storage_directory = storage_directory, .snapshot_on_exit = true}};
    memgraph::replication::ReplicationState repl_state{memgraph::storage::ReplicationStateHelper(config)};
    memgraph::dbms::Database db{config, repl_state};
    CreateBaseDataset(db.storage(), true);
    VerifyDataset(db.storage(), DatasetType::ONLY_BASE, true);
    CreateExtendedDataset(db.storage());
    VerifyDataset(db.storage(), DatasetType::BASE_WITH_EXTENDED, true);
  }

  ASSERT_EQ(GetSnapshotsList().size(), 1);
  ASSERT_EQ(GetBackupSnapshotsList().size(), 0);
  ASSERT_EQ(GetWalsList().size(), 0);
  ASSERT_EQ(GetBackupWalsList().size(), 0);

  // Recover snapshot.
  ASSERT_DEATH(([&]() {
                 memgraph::storage::Config config{
                     .items = {.properties_on_edges = false},
                     .durability = {.storage_directory = storage_directory, .recover_on_startup = true}};
                 memgraph::replication::ReplicationState repl_state{memgraph::storage::ReplicationStateHelper(config)};
                 memgraph::dbms::Database db{config, repl_state};
               }())  // iile
               ,
               "");
}

// NOLINTNEXTLINE(hicpp-special-member-functions)
TEST_F(DurabilityTest, SnapshotWithPropertiesOnEdgesButUnusedRecoveryWithoutPropertiesOnEdges) {
  // Create snapshot.
  {
    memgraph::storage::Config config{.items = {.properties_on_edges = true},
                                     .durability = {.storage_directory = storage_directory, .snapshot_on_exit = true}};
    memgraph::replication::ReplicationState repl_state{memgraph::storage::ReplicationStateHelper(config)};
    memgraph::dbms::Database db{config, repl_state};
    CreateBaseDataset(db.storage(), true);
    VerifyDataset(db.storage(), DatasetType::ONLY_BASE, true);
    CreateExtendedDataset(db.storage());
    VerifyDataset(db.storage(), DatasetType::BASE_WITH_EXTENDED, true);
    // Remove properties from edges.
    {
      auto acc = db.storage()->Access();
      for (auto vertex : acc->Vertices(memgraph::storage::View::OLD)) {
        auto in_edges = vertex.InEdges(memgraph::storage::View::OLD);
        ASSERT_TRUE(in_edges.HasValue());
        for (auto &edge : in_edges->edges) {
          // TODO (mferencevic): Replace with `ClearProperties()`
          auto props = edge.Properties(memgraph::storage::View::NEW);
          ASSERT_TRUE(props.HasValue());
          for (const auto &prop : *props) {
            ASSERT_TRUE(edge.SetProperty(prop.first, memgraph::storage::PropertyValue()).HasValue());
          }
        }
        auto out_edges = vertex.InEdges(memgraph::storage::View::OLD);
        ASSERT_TRUE(out_edges.HasValue());
        for (auto &edge : out_edges->edges) {
          // TODO (mferencevic): Replace with `ClearProperties()`
          auto props = edge.Properties(memgraph::storage::View::NEW);
          ASSERT_TRUE(props.HasValue());
          for (const auto &prop : *props) {
            ASSERT_TRUE(edge.SetProperty(prop.first, memgraph::storage::PropertyValue()).HasValue());
          }
        }
      }
      ASSERT_FALSE(acc->Commit().HasError());
    }
  }

  ASSERT_EQ(GetSnapshotsList().size(), 1);
  ASSERT_EQ(GetBackupSnapshotsList().size(), 0);
  ASSERT_EQ(GetWalsList().size(), 0);
  ASSERT_EQ(GetBackupWalsList().size(), 0);

  // Recover snapshot.
  memgraph::storage::Config config{.items = {.properties_on_edges = false},
                                   .durability = {.storage_directory = storage_directory, .recover_on_startup = true}};
  memgraph::replication::ReplicationState repl_state{memgraph::storage::ReplicationStateHelper(config)};
  memgraph::dbms::Database db{config, repl_state};
  VerifyDataset(db.storage(), DatasetType::BASE_WITH_EXTENDED, false);

  // Try to use the storage.
  {
    auto acc = db.storage()->Access();
    auto vertex = acc->CreateVertex();
    auto edge = acc->CreateEdge(&vertex, &vertex, db.storage()->NameToEdgeType("et"));
    ASSERT_TRUE(edge.HasValue());
    ASSERT_FALSE(acc->Commit().HasError());
  }
}

// NOLINTNEXTLINE(hicpp-special-member-functions)
TEST_P(DurabilityTest, WalBasic) {
  // Create WALs.
  {
    memgraph::storage::Config config{
        .items = {.properties_on_edges = GetParam()},
        .durability = {
            .storage_directory = storage_directory,
            .snapshot_wal_mode = memgraph::storage::Config::Durability::SnapshotWalMode::PERIODIC_SNAPSHOT_WITH_WAL,
            .snapshot_interval = std::chrono::minutes(20),
            .wal_file_flush_every_n_tx = kFlushWalEvery}};
    memgraph::replication::ReplicationState repl_state{memgraph::storage::ReplicationStateHelper(config)};
    memgraph::dbms::Database db{config, repl_state};
    CreateBaseDataset(db.storage(), GetParam());
    CreateExtendedDataset(db.storage());
  }

  ASSERT_EQ(GetSnapshotsList().size(), 0);
  ASSERT_EQ(GetBackupSnapshotsList().size(), 0);
  ASSERT_GE(GetWalsList().size(), 1);
  ASSERT_EQ(GetBackupWalsList().size(), 0);

  // Recover WALs.
  memgraph::storage::Config config{.items = {.properties_on_edges = GetParam()},
                                   .durability = {.storage_directory = storage_directory, .recover_on_startup = true}};
  memgraph::replication::ReplicationState repl_state{memgraph::storage::ReplicationStateHelper(config)};
  memgraph::dbms::Database db{config, repl_state};
  VerifyDataset(db.storage(), DatasetType::BASE_WITH_EXTENDED, GetParam());

  // Try to use the storage.
  {
    auto acc = db.storage()->Access();
    auto vertex = acc->CreateVertex();
    auto edge = acc->CreateEdge(&vertex, &vertex, db.storage()->NameToEdgeType("et"));
    ASSERT_TRUE(edge.HasValue());
    ASSERT_FALSE(acc->Commit().HasError());
  }
}

// NOLINTNEXTLINE(hicpp-special-member-functions)
TEST_P(DurabilityTest, WalBackup) {
  // Create WALs.
  {
    memgraph::storage::Config config{
        .items = {.properties_on_edges = GetParam()},
        .durability = {
            .storage_directory = storage_directory,
            .snapshot_wal_mode = memgraph::storage::Config::Durability::SnapshotWalMode::PERIODIC_SNAPSHOT_WITH_WAL,
            .snapshot_interval = std::chrono::minutes(20),
            .wal_file_size_kibibytes = 1,
            .wal_file_flush_every_n_tx = kFlushWalEvery}};
    memgraph::replication::ReplicationState repl_state{memgraph::storage::ReplicationStateHelper(config)};
    memgraph::dbms::Database db{config, repl_state};
    auto acc = db.storage()->Access();
    for (uint64_t i = 0; i < 1000; ++i) {
      acc->CreateVertex();
    }
    ASSERT_FALSE(acc->Commit().HasError());
  }

  ASSERT_EQ(GetSnapshotsList().size(), 0);
  ASSERT_EQ(GetBackupSnapshotsList().size(), 0);
  auto num_wals = GetWalsList().size();
  ASSERT_GE(num_wals, 1);
  ASSERT_EQ(GetBackupWalsList().size(), 0);

  // Start storage without recovery.
  {
    memgraph::storage::Config config{
        .items = {.properties_on_edges = GetParam()},
        .durability = {
            .storage_directory = storage_directory,
            .snapshot_wal_mode = memgraph::storage::Config::Durability::SnapshotWalMode::PERIODIC_SNAPSHOT_WITH_WAL,
            .snapshot_interval = std::chrono::minutes(20)}};
    memgraph::replication::ReplicationState repl_state{memgraph::storage::ReplicationStateHelper(config)};
    memgraph::dbms::Database db{config, repl_state};
  }

  ASSERT_EQ(GetSnapshotsList().size(), 0);
  ASSERT_EQ(GetBackupSnapshotsList().size(), 0);
  ASSERT_EQ(GetWalsList().size(), 0);
  ASSERT_EQ(GetBackupWalsList().size(), num_wals);
}

// NOLINTNEXTLINE(hicpp-special-member-functions)
TEST_P(DurabilityTest, WalAppendToExisting) {
  // Create WALs.
  {
    memgraph::storage::Config config{
        .items = {.properties_on_edges = GetParam()},
        .durability = {
            .storage_directory = storage_directory,
            .snapshot_wal_mode = memgraph::storage::Config::Durability::SnapshotWalMode::PERIODIC_SNAPSHOT_WITH_WAL,
            .snapshot_interval = std::chrono::minutes(20),
            .wal_file_flush_every_n_tx = kFlushWalEvery}};
    memgraph::replication::ReplicationState repl_state{memgraph::storage::ReplicationStateHelper(config)};
    memgraph::dbms::Database db{config, repl_state};
    CreateBaseDataset(db.storage(), GetParam());
  }

  ASSERT_EQ(GetSnapshotsList().size(), 0);
  ASSERT_EQ(GetBackupSnapshotsList().size(), 0);
  ASSERT_GE(GetWalsList().size(), 1);
  ASSERT_EQ(GetBackupWalsList().size(), 0);

  // Recover WALs.
  {
    memgraph::storage::Config config{
        .items = {.properties_on_edges = GetParam()},
        .durability = {.storage_directory = storage_directory, .recover_on_startup = true}};
    memgraph::replication::ReplicationState repl_state{memgraph::storage::ReplicationStateHelper(config)};
    memgraph::dbms::Database db{config, repl_state};
    VerifyDataset(db.storage(), DatasetType::ONLY_BASE, GetParam());
  }

  // Recover WALs and create more WALs.
  {
    memgraph::storage::Config config{
        .items = {.properties_on_edges = GetParam()},
        .durability = {
            .storage_directory = storage_directory,
            .recover_on_startup = true,
            .snapshot_wal_mode = memgraph::storage::Config::Durability::SnapshotWalMode::PERIODIC_SNAPSHOT_WITH_WAL,
            .snapshot_interval = std::chrono::minutes(20),
            .wal_file_flush_every_n_tx = kFlushWalEvery}};
    memgraph::replication::ReplicationState repl_state{memgraph::storage::ReplicationStateHelper(config)};
    memgraph::dbms::Database db{config, repl_state};
    CreateExtendedDataset(db.storage());
  }

  ASSERT_EQ(GetSnapshotsList().size(), 0);
  ASSERT_EQ(GetBackupSnapshotsList().size(), 0);
  ASSERT_GE(GetWalsList().size(), 2);
  ASSERT_EQ(GetBackupWalsList().size(), 0);

  // Recover WALs.
  memgraph::storage::Config config{.items = {.properties_on_edges = GetParam()},
                                   .durability = {.storage_directory = storage_directory, .recover_on_startup = true}};
  memgraph::replication::ReplicationState repl_state{memgraph::storage::ReplicationStateHelper(config)};
  memgraph::dbms::Database db{config, repl_state};
  VerifyDataset(db.storage(), DatasetType::BASE_WITH_EXTENDED, GetParam());

  // Try to use the storage.
  {
    auto acc = db.storage()->Access();
    auto vertex = acc->CreateVertex();
    auto edge = acc->CreateEdge(&vertex, &vertex, db.storage()->NameToEdgeType("et"));
    ASSERT_TRUE(edge.HasValue());
    ASSERT_FALSE(acc->Commit().HasError());
  }
}

// NOLINTNEXTLINE(hicpp-special-member-functions)
TEST_P(DurabilityTest, WalCreateInSingleTransaction) {
  // NOLINTNEXTLINE(readability-isolate-declaration)
  memgraph::storage::Gid gid_v1, gid_v2, gid_e1, gid_v3;

  // Create WALs.
  {
    memgraph::storage::Config config{
        .items = {.properties_on_edges = GetParam()},
        .durability = {
            .storage_directory = storage_directory,
            .snapshot_wal_mode = memgraph::storage::Config::Durability::SnapshotWalMode::PERIODIC_SNAPSHOT_WITH_WAL,
            .snapshot_interval = std::chrono::minutes(20),
            .wal_file_flush_every_n_tx = kFlushWalEvery}};
    memgraph::replication::ReplicationState repl_state{memgraph::storage::ReplicationStateHelper(config)};
    memgraph::dbms::Database db{config, repl_state};
    auto acc = db.storage()->Access();
    auto v1 = acc->CreateVertex();
    gid_v1 = v1.Gid();
    auto v2 = acc->CreateVertex();
    gid_v2 = v2.Gid();
    auto e1Res = acc->CreateEdge(&v1, &v2, db.storage()->NameToEdgeType("e1"));
    ASSERT_TRUE(e1Res.HasValue());
    auto e1 = std::move(e1Res.GetValue());
    gid_e1 = e1.Gid();
    ASSERT_TRUE(v1.AddLabel(db.storage()->NameToLabel("l11")).HasValue());
    ASSERT_TRUE(v1.AddLabel(db.storage()->NameToLabel("l12")).HasValue());
    ASSERT_TRUE(v1.AddLabel(db.storage()->NameToLabel("l13")).HasValue());
    if (GetParam()) {
      ASSERT_TRUE(
          e1.SetProperty(db.storage()->NameToProperty("test"), memgraph::storage::PropertyValue("nandare")).HasValue());
    }
    ASSERT_TRUE(v2.AddLabel(db.storage()->NameToLabel("l21")).HasValue());
    ASSERT_TRUE(
        v2.SetProperty(db.storage()->NameToProperty("hello"), memgraph::storage::PropertyValue("world")).HasValue());
    auto v3 = acc->CreateVertex();
    gid_v3 = v3.Gid();
    ASSERT_TRUE(v3.SetProperty(db.storage()->NameToProperty("v3"), memgraph::storage::PropertyValue(42)).HasValue());
    ASSERT_FALSE(acc->Commit().HasError());
  }

  ASSERT_EQ(GetSnapshotsList().size(), 0);
  ASSERT_EQ(GetBackupSnapshotsList().size(), 0);
  ASSERT_GE(GetWalsList().size(), 1);
  ASSERT_EQ(GetBackupWalsList().size(), 0);

  // Recover WALs.
  memgraph::storage::Config config{.items = {.properties_on_edges = GetParam()},
                                   .durability = {.storage_directory = storage_directory, .recover_on_startup = true}};
  memgraph::replication::ReplicationState repl_state{memgraph::storage::ReplicationStateHelper(config)};
  memgraph::dbms::Database db{config, repl_state};
  {
    auto acc = db.storage()->Access();

    auto indices = acc->ListAllIndices();
    ASSERT_EQ(indices.label.size(), 0);
    ASSERT_EQ(indices.label_property.size(), 0);
    auto constraints = acc->ListAllConstraints();
    ASSERT_EQ(constraints.existence.size(), 0);
    ASSERT_EQ(constraints.unique.size(), 0);
    {
      auto v1 = acc->FindVertex(gid_v1, memgraph::storage::View::OLD);
      ASSERT_TRUE(v1);
      auto labels = v1->Labels(memgraph::storage::View::OLD);
      ASSERT_TRUE(labels.HasValue());
      ASSERT_THAT(*labels, UnorderedElementsAre(db.storage()->NameToLabel("l11"), db.storage()->NameToLabel("l12"),
                                                db.storage()->NameToLabel("l13")));
      auto props = v1->Properties(memgraph::storage::View::OLD);
      ASSERT_TRUE(props.HasValue());
      ASSERT_EQ(props->size(), 0);
      auto in_edges = v1->InEdges(memgraph::storage::View::OLD);
      ASSERT_TRUE(in_edges.HasValue());
      ASSERT_EQ(in_edges->edges.size(), 0);
      auto out_edges = v1->OutEdges(memgraph::storage::View::OLD);
      ASSERT_TRUE(out_edges.HasValue());
      ASSERT_EQ(out_edges->edges.size(), 1);
      const auto &edge = out_edges->edges[0];
      ASSERT_EQ(edge.Gid(), gid_e1);
      auto edge_props = edge.Properties(memgraph::storage::View::OLD);
      ASSERT_TRUE(edge_props.HasValue());
      if (GetParam()) {
        ASSERT_THAT(*edge_props, UnorderedElementsAre(std::make_pair(db.storage()->NameToProperty("test"),
                                                                     memgraph::storage::PropertyValue("nandare"))));
      } else {
        ASSERT_EQ(edge_props->size(), 0);
      }
    }
    {
      auto v2 = acc->FindVertex(gid_v2, memgraph::storage::View::OLD);
      ASSERT_TRUE(v2);
      auto labels = v2->Labels(memgraph::storage::View::OLD);
      ASSERT_TRUE(labels.HasValue());
      ASSERT_THAT(*labels, UnorderedElementsAre(db.storage()->NameToLabel("l21")));
      auto props = v2->Properties(memgraph::storage::View::OLD);
      ASSERT_TRUE(props.HasValue());
      ASSERT_THAT(*props, UnorderedElementsAre(std::make_pair(db.storage()->NameToProperty("hello"),
                                                              memgraph::storage::PropertyValue("world"))));
      auto in_edges = v2->InEdges(memgraph::storage::View::OLD);
      ASSERT_TRUE(in_edges.HasValue());
      ASSERT_EQ(in_edges->edges.size(), 1);
      const auto &edge = in_edges->edges[0];
      ASSERT_EQ(edge.Gid(), gid_e1);
      auto edge_props = edge.Properties(memgraph::storage::View::OLD);
      ASSERT_TRUE(edge_props.HasValue());
      if (GetParam()) {
        ASSERT_THAT(*edge_props, UnorderedElementsAre(std::make_pair(db.storage()->NameToProperty("test"),
                                                                     memgraph::storage::PropertyValue("nandare"))));
      } else {
        ASSERT_EQ(edge_props->size(), 0);
      }
      auto out_edges = v2->OutEdges(memgraph::storage::View::OLD);
      ASSERT_TRUE(out_edges.HasValue());
      ASSERT_EQ(out_edges->edges.size(), 0);
    }
    {
      auto v3 = acc->FindVertex(gid_v3, memgraph::storage::View::OLD);
      ASSERT_TRUE(v3);
      auto labels = v3->Labels(memgraph::storage::View::OLD);
      ASSERT_TRUE(labels.HasValue());
      ASSERT_EQ(labels->size(), 0);
      auto props = v3->Properties(memgraph::storage::View::OLD);
      ASSERT_TRUE(props.HasValue());
      ASSERT_THAT(*props, UnorderedElementsAre(std::make_pair(db.storage()->NameToProperty("v3"),
                                                              memgraph::storage::PropertyValue(42))));
      auto in_edges = v3->InEdges(memgraph::storage::View::OLD);
      ASSERT_TRUE(in_edges.HasValue());
      ASSERT_EQ(in_edges->edges.size(), 0);
      auto out_edges = v3->OutEdges(memgraph::storage::View::OLD);
      ASSERT_TRUE(out_edges.HasValue());
      ASSERT_EQ(out_edges->edges.size(), 0);
    }
  }

  // Try to use the storage.
  {
    auto acc = db.storage()->Access();
    auto vertex = acc->CreateVertex();
    auto edge = acc->CreateEdge(&vertex, &vertex, db.storage()->NameToEdgeType("et"));
    ASSERT_TRUE(edge.HasValue());
    ASSERT_FALSE(acc->Commit().HasError());
  }
}

// NOLINTNEXTLINE(hicpp-special-member-functions)
TEST_P(DurabilityTest, WalCreateAndRemoveEverything) {
  // Create WALs.
  {
    memgraph::storage::Config config{
        .items = {.properties_on_edges = GetParam()},
        .durability = {
            .storage_directory = storage_directory,
            .snapshot_wal_mode = memgraph::storage::Config::Durability::SnapshotWalMode::PERIODIC_SNAPSHOT_WITH_WAL,
            .snapshot_interval = std::chrono::minutes(20),
            .wal_file_flush_every_n_tx = kFlushWalEvery}};
    memgraph::replication::ReplicationState repl_state{memgraph::storage::ReplicationStateHelper(config)};
    memgraph::dbms::Database db{config, repl_state};
    CreateBaseDataset(db.storage(), GetParam());
    CreateExtendedDataset(db.storage());
    auto indices = [&] {
      auto acc = db.storage()->Access();
      auto res = acc->ListAllIndices();
      acc->Commit();
      return res;
    }();  // iile
    for (const auto &index : indices.label) {
      auto unique_acc = db.storage()->UniqueAccess();
      ASSERT_FALSE(unique_acc->DropIndex(index).HasError());
      ASSERT_FALSE(unique_acc->Commit().HasError());
    }
    for (const auto &index : indices.label_property) {
      auto unique_acc = db.storage()->UniqueAccess();
      ASSERT_FALSE(unique_acc->DropIndex(index.first, index.second).HasError());
      ASSERT_FALSE(unique_acc->Commit().HasError());
    }
    auto constraints = [&] {
      auto acc = db.storage()->Access();
      auto res = acc->ListAllConstraints();
      acc->Commit();
      return res;
    }();  // iile
    for (const auto &constraint : constraints.existence) {
      auto unique_acc = db.storage()->UniqueAccess();
      ASSERT_FALSE(unique_acc->DropExistenceConstraint(constraint.first, constraint.second).HasError());
      ASSERT_FALSE(unique_acc->Commit().HasError());
    }
    for (const auto &constraint : constraints.unique) {
      auto unique_acc = db.storage()->UniqueAccess();
      ASSERT_EQ(unique_acc->DropUniqueConstraint(constraint.first, constraint.second),
                memgraph::storage::UniqueConstraints::DeletionStatus::SUCCESS);
      ASSERT_FALSE(unique_acc->Commit().HasError());
    }
    auto acc = db.storage()->Access();
    for (auto vertex : acc->Vertices(memgraph::storage::View::OLD)) {
      ASSERT_TRUE(acc->DetachDeleteVertex(&vertex).HasValue());
    }
    ASSERT_FALSE(acc->Commit().HasError());
  }

  ASSERT_EQ(GetSnapshotsList().size(), 0);
  ASSERT_EQ(GetBackupSnapshotsList().size(), 0);
  ASSERT_GE(GetWalsList().size(), 1);
  ASSERT_EQ(GetBackupWalsList().size(), 0);

  // Recover WALs.
  memgraph::storage::Config config{.items = {.properties_on_edges = GetParam()},
                                   .durability = {.storage_directory = storage_directory, .recover_on_startup = true}};
  memgraph::replication::ReplicationState repl_state{memgraph::storage::ReplicationStateHelper(config)};
  memgraph::dbms::Database db{config, repl_state};
  {
    auto acc = db.storage()->Access();
    auto indices = acc->ListAllIndices();
    ASSERT_EQ(indices.label.size(), 0);
    ASSERT_EQ(indices.label_property.size(), 0);
    auto constraints = acc->ListAllConstraints();
    ASSERT_EQ(constraints.existence.size(), 0);
    ASSERT_EQ(constraints.unique.size(), 0);
    uint64_t count = 0;
    auto iterable = acc->Vertices(memgraph::storage::View::OLD);
    for (auto it = iterable.begin(); it != iterable.end(); ++it) {
      ++count;
    }
    ASSERT_EQ(count, 0);
  }

  // Try to use the storage.
  {
    auto acc = db.storage()->Access();
    auto vertex = acc->CreateVertex();
    auto edge = acc->CreateEdge(&vertex, &vertex, db.storage()->NameToEdgeType("et"));
    ASSERT_TRUE(edge.HasValue());
    ASSERT_FALSE(acc->Commit().HasError());
  }
}

// NOLINTNEXTLINE(hicpp-special-member-functions)
TEST_P(DurabilityTest, WalTransactionOrdering) {
  // NOLINTNEXTLINE(readability-isolate-declaration)
  memgraph::storage::Gid gid1, gid2, gid3;

  // Create WAL.
  {
    memgraph::storage::Config config{
        .items = {.properties_on_edges = GetParam()},
        .durability = {
            .storage_directory = storage_directory,
            .snapshot_wal_mode = memgraph::storage::Config::Durability::SnapshotWalMode::PERIODIC_SNAPSHOT_WITH_WAL,
            .snapshot_interval = std::chrono::minutes(20),
            .wal_file_size_kibibytes = 100000,
            .wal_file_flush_every_n_tx = kFlushWalEvery,
        }};
    memgraph::replication::ReplicationState repl_state{memgraph::storage::ReplicationStateHelper(config)};
    memgraph::dbms::Database db{config, repl_state};
    auto acc1 = db.storage()->Access();
    auto acc2 = db.storage()->Access();

    // Create vertex in transaction 2.
    {
      auto vertex2 = acc2->CreateVertex();
      gid2 = vertex2.Gid();
      ASSERT_TRUE(
          vertex2.SetProperty(db.storage()->NameToProperty("id"), memgraph::storage::PropertyValue(2)).HasValue());
    }

    auto acc3 = db.storage()->Access();

    // Create vertex in transaction 3.
    {
      auto vertex3 = acc3->CreateVertex();
      gid3 = vertex3.Gid();
      ASSERT_TRUE(
          vertex3.SetProperty(db.storage()->NameToProperty("id"), memgraph::storage::PropertyValue(3)).HasValue());
    }

    // Create vertex in transaction 1.
    {
      auto vertex1 = acc1->CreateVertex();
      gid1 = vertex1.Gid();
      ASSERT_TRUE(
          vertex1.SetProperty(db.storage()->NameToProperty("id"), memgraph::storage::PropertyValue(1)).HasValue());
    }

    // Commit transaction 3, then 1, then 2.
    ASSERT_FALSE(acc3->Commit().HasError());
    ASSERT_FALSE(acc1->Commit().HasError());
    ASSERT_FALSE(acc2->Commit().HasError());
  }

  ASSERT_EQ(GetSnapshotsList().size(), 0);
  ASSERT_EQ(GetBackupSnapshotsList().size(), 0);
  ASSERT_EQ(GetWalsList().size(), 1);
  ASSERT_EQ(GetBackupWalsList().size(), 0);

  // Verify WAL data.
  {
    auto path = GetWalsList().front();
    auto info = memgraph::storage::durability::ReadWalInfo(path);
    memgraph::storage::durability::Decoder wal;
    wal.Initialize(path, memgraph::storage::durability::kWalMagic);
    wal.SetPosition(info.offset_deltas);
    ASSERT_EQ(info.num_deltas, 9);
    std::vector<std::pair<uint64_t, memgraph::storage::durability::WalDeltaData>> data;
    for (uint64_t i = 0; i < info.num_deltas; ++i) {
      auto timestamp = memgraph::storage::durability::ReadWalDeltaHeader(&wal);
      data.emplace_back(timestamp, memgraph::storage::durability::ReadWalDeltaData(&wal));
    }
    // Verify timestamps.
    ASSERT_EQ(data[1].first, data[0].first);
    ASSERT_EQ(data[2].first, data[1].first);
    ASSERT_GT(data[3].first, data[2].first);
    ASSERT_EQ(data[4].first, data[3].first);
    ASSERT_EQ(data[5].first, data[4].first);
    ASSERT_GT(data[6].first, data[5].first);
    ASSERT_EQ(data[7].first, data[6].first);
    ASSERT_EQ(data[8].first, data[7].first);
    // Verify transaction 3.
    ASSERT_EQ(data[0].second.type, memgraph::storage::durability::WalDeltaData::Type::VERTEX_CREATE);
    ASSERT_EQ(data[0].second.vertex_create_delete.gid, gid3);
    ASSERT_EQ(data[1].second.type, memgraph::storage::durability::WalDeltaData::Type::VERTEX_SET_PROPERTY);
    ASSERT_EQ(data[1].second.vertex_edge_set_property.gid, gid3);
    ASSERT_EQ(data[1].second.vertex_edge_set_property.property, "id");
    ASSERT_EQ(data[1].second.vertex_edge_set_property.value, memgraph::storage::PropertyValue(3));
    ASSERT_EQ(data[2].second.type, memgraph::storage::durability::WalDeltaData::Type::TRANSACTION_END);
    // Verify transaction 1.
    ASSERT_EQ(data[3].second.type, memgraph::storage::durability::WalDeltaData::Type::VERTEX_CREATE);
    ASSERT_EQ(data[3].second.vertex_create_delete.gid, gid1);
    ASSERT_EQ(data[4].second.type, memgraph::storage::durability::WalDeltaData::Type::VERTEX_SET_PROPERTY);
    ASSERT_EQ(data[4].second.vertex_edge_set_property.gid, gid1);
    ASSERT_EQ(data[4].second.vertex_edge_set_property.property, "id");
    ASSERT_EQ(data[4].second.vertex_edge_set_property.value, memgraph::storage::PropertyValue(1));
    ASSERT_EQ(data[5].second.type, memgraph::storage::durability::WalDeltaData::Type::TRANSACTION_END);
    // Verify transaction 2.
    ASSERT_EQ(data[6].second.type, memgraph::storage::durability::WalDeltaData::Type::VERTEX_CREATE);
    ASSERT_EQ(data[6].second.vertex_create_delete.gid, gid2);
    ASSERT_EQ(data[7].second.type, memgraph::storage::durability::WalDeltaData::Type::VERTEX_SET_PROPERTY);
    ASSERT_EQ(data[7].second.vertex_edge_set_property.gid, gid2);
    ASSERT_EQ(data[7].second.vertex_edge_set_property.property, "id");
    ASSERT_EQ(data[7].second.vertex_edge_set_property.value, memgraph::storage::PropertyValue(2));
    ASSERT_EQ(data[8].second.type, memgraph::storage::durability::WalDeltaData::Type::TRANSACTION_END);
  }

  // Recover WALs.
  memgraph::storage::Config config{.items = {.properties_on_edges = GetParam()},
                                   .durability = {.storage_directory = storage_directory, .recover_on_startup = true}};
  memgraph::replication::ReplicationState repl_state{memgraph::storage::ReplicationStateHelper(config)};
  memgraph::dbms::Database db{config, repl_state};
  {
    auto acc = db.storage()->Access();
    for (auto [gid, id] : std::vector<std::pair<memgraph::storage::Gid, int64_t>>{{gid1, 1}, {gid2, 2}, {gid3, 3}}) {
      auto vertex = acc->FindVertex(gid, memgraph::storage::View::OLD);
      ASSERT_TRUE(vertex);
      auto labels = vertex->Labels(memgraph::storage::View::OLD);
      ASSERT_TRUE(labels.HasValue());
      ASSERT_EQ(labels->size(), 0);
      auto props = vertex->Properties(memgraph::storage::View::OLD);
      ASSERT_TRUE(props.HasValue());
      ASSERT_EQ(props->size(), 1);
      ASSERT_EQ(props->at(db.storage()->NameToProperty("id")), memgraph::storage::PropertyValue(id));
    }
  }

  // Try to use the storage.
  {
    auto acc = db.storage()->Access();
    auto vertex = acc->CreateVertex();
    auto edge = acc->CreateEdge(&vertex, &vertex, db.storage()->NameToEdgeType("et"));
    ASSERT_TRUE(edge.HasValue());
    ASSERT_FALSE(acc->Commit().HasError());
  }
}

// NOLINTNEXTLINE(hicpp-special-member-functions)
TEST_P(DurabilityTest, WalCreateAndRemoveOnlyBaseDataset) {
  // Create WALs.
  {
    memgraph::storage::Config config{
        .items = {.properties_on_edges = GetParam()},
        .durability = {
            .storage_directory = storage_directory,
            .snapshot_wal_mode = memgraph::storage::Config::Durability::SnapshotWalMode::PERIODIC_SNAPSHOT_WITH_WAL,
            .snapshot_interval = std::chrono::minutes(20),
            .wal_file_flush_every_n_tx = kFlushWalEvery}};
    memgraph::replication::ReplicationState repl_state{memgraph::storage::ReplicationStateHelper(config)};
    memgraph::dbms::Database db{config, repl_state};
    CreateBaseDataset(db.storage(), GetParam());
    CreateExtendedDataset(db.storage());
    auto label_indexed = db.storage()->NameToLabel("base_indexed");
    auto label_unindexed = db.storage()->NameToLabel("base_unindexed");
    auto acc = db.storage()->Access();
    for (auto vertex : acc->Vertices(memgraph::storage::View::OLD)) {
      auto has_indexed = vertex.HasLabel(label_indexed, memgraph::storage::View::OLD);
      ASSERT_TRUE(has_indexed.HasValue());
      auto has_unindexed = vertex.HasLabel(label_unindexed, memgraph::storage::View::OLD);
      if (!*has_indexed && !*has_unindexed) continue;
      ASSERT_TRUE(acc->DetachDeleteVertex(&vertex).HasValue());
    }
    ASSERT_FALSE(acc->Commit().HasError());
  }

  ASSERT_EQ(GetSnapshotsList().size(), 0);
  ASSERT_EQ(GetBackupSnapshotsList().size(), 0);
  ASSERT_GE(GetWalsList().size(), 1);
  ASSERT_EQ(GetBackupWalsList().size(), 0);

  // Recover WALs.
  memgraph::storage::Config config{.items = {.properties_on_edges = GetParam()},
                                   .durability = {.storage_directory = storage_directory, .recover_on_startup = true}};
  memgraph::replication::ReplicationState repl_state{memgraph::storage::ReplicationStateHelper(config)};
  memgraph::dbms::Database db{config, repl_state};
  VerifyDataset(db.storage(), DatasetType::ONLY_EXTENDED_WITH_BASE_INDICES_AND_CONSTRAINTS, GetParam());

  // Try to use the storage.
  {
    auto acc = db.storage()->Access();
    auto vertex = acc->CreateVertex();
    auto edge = acc->CreateEdge(&vertex, &vertex, db.storage()->NameToEdgeType("et"));
    ASSERT_TRUE(edge.HasValue());
    ASSERT_FALSE(acc->Commit().HasError());
  }
}

// NOLINTNEXTLINE(hicpp-special-member-functions)
TEST_P(DurabilityTest, WalDeathResilience) {
  pid_t pid = fork();
  if (pid == 0) {
    // Create WALs.
    {
      memgraph::storage::Config config{
          .items = {.properties_on_edges = GetParam()},
          .durability = {
              .storage_directory = storage_directory,
              .snapshot_wal_mode = memgraph::storage::Config::Durability::SnapshotWalMode::PERIODIC_SNAPSHOT_WITH_WAL,
              .snapshot_interval = std::chrono::minutes(20),
              .wal_file_flush_every_n_tx = kFlushWalEvery}};
      memgraph::replication::ReplicationState repl_state{memgraph::storage::ReplicationStateHelper(config)};
      memgraph::dbms::Database db{config, repl_state};
      // Create one million vertices.
      for (uint64_t i = 0; i < 1000000; ++i) {
        auto acc = db.storage()->Access();
        acc->CreateVertex();
        MG_ASSERT(!acc->Commit().HasError(), "Couldn't commit transaction!");
      }
    }
  } else if (pid > 0) {
    // Wait for WALs to be created.
    std::this_thread::sleep_for(std::chrono::seconds(2));
    int status;
    EXPECT_EQ(waitpid(pid, &status, WNOHANG), 0);
    EXPECT_EQ(kill(pid, SIGKILL), 0);
    EXPECT_EQ(waitpid(pid, &status, 0), pid);
    EXPECT_NE(status, 0);
  } else {
    LOG_FATAL("Couldn't create process to execute test!");
  }

  ASSERT_EQ(GetSnapshotsList().size(), 0);
  ASSERT_EQ(GetBackupSnapshotsList().size(), 0);
  ASSERT_GE(GetWalsList().size(), 1);
  ASSERT_EQ(GetBackupWalsList().size(), 0);

  // Recover WALs and create more WALs.
  const uint64_t kExtraItems = 1000;
  uint64_t count = 0;
  {
    memgraph::storage::Config config{
        .items = {.properties_on_edges = GetParam()},
        .durability = {
            .storage_directory = storage_directory,
            .recover_on_startup = true,
            .snapshot_wal_mode = memgraph::storage::Config::Durability::SnapshotWalMode::PERIODIC_SNAPSHOT_WITH_WAL,
            .snapshot_interval = std::chrono::minutes(20),
            .wal_file_flush_every_n_tx = kFlushWalEvery,
        }};
    memgraph::replication::ReplicationState repl_state{memgraph::storage::ReplicationStateHelper(config)};
    memgraph::dbms::Database db{config, repl_state};
    {
      auto acc = db.storage()->Access();
      auto iterable = acc->Vertices(memgraph::storage::View::OLD);
      for (auto it = iterable.begin(); it != iterable.end(); ++it) {
        ++count;
      }
      ASSERT_GT(count, 0);
    }

    {
      auto acc = db.storage()->Access();
      for (uint64_t i = 0; i < kExtraItems; ++i) {
        acc->CreateVertex();
      }
      ASSERT_FALSE(acc->Commit().HasError());
    }
  }

  ASSERT_EQ(GetSnapshotsList().size(), 0);
  ASSERT_EQ(GetBackupSnapshotsList().size(), 0);
  ASSERT_GE(GetWalsList().size(), 2);
  ASSERT_EQ(GetBackupWalsList().size(), 0);

  // Recover WALs.
  memgraph::storage::Config config{.items = {.properties_on_edges = GetParam()},
                                   .durability = {.storage_directory = storage_directory, .recover_on_startup = true}};
  memgraph::replication::ReplicationState repl_state{memgraph::storage::ReplicationStateHelper(config)};
  memgraph::dbms::Database db{config, repl_state};
  {
    uint64_t current = 0;
    auto acc = db.storage()->Access();
    auto iterable = acc->Vertices(memgraph::storage::View::OLD);
    for (auto it = iterable.begin(); it != iterable.end(); ++it) {
      ++current;
    }
    ASSERT_EQ(count + kExtraItems, current);
  }

  // Try to use the storage.
  {
    auto acc = db.storage()->Access();
    auto vertex = acc->CreateVertex();
    auto edge = acc->CreateEdge(&vertex, &vertex, db.storage()->NameToEdgeType("et"));
    ASSERT_TRUE(edge.HasValue());
    ASSERT_FALSE(acc->Commit().HasError());
  }
}

// NOLINTNEXTLINE(hicpp-special-member-functions)
TEST_P(DurabilityTest, WalMissingSecond) {
  // Create unrelated WALs.
  {
    memgraph::storage::Config config{
        .items = {.properties_on_edges = GetParam()},
        .durability = {
            .storage_directory = storage_directory,
            .snapshot_wal_mode = memgraph::storage::Config::Durability::SnapshotWalMode::PERIODIC_SNAPSHOT_WITH_WAL,
            .snapshot_interval = std::chrono::minutes(20),
            .wal_file_size_kibibytes = 1,
            .wal_file_flush_every_n_tx = kFlushWalEvery}};
    memgraph::replication::ReplicationState repl_state{memgraph::storage::ReplicationStateHelper(config)};
    memgraph::dbms::Database db{config, repl_state};
    auto acc = db.storage()->Access();
    for (uint64_t i = 0; i < 1000; ++i) {
      acc->CreateVertex();
    }
    ASSERT_FALSE(acc->Commit().HasError());
  }

  ASSERT_EQ(GetSnapshotsList().size(), 0);
  ASSERT_EQ(GetBackupSnapshotsList().size(), 0);
  ASSERT_GE(GetWalsList().size(), 1);
  ASSERT_EQ(GetBackupWalsList().size(), 0);

  uint64_t unrelated_wals = GetWalsList().size();

  // Create WALs.
  {
    memgraph::storage::Config config{
        .items = {.properties_on_edges = GetParam()},
        .durability = {
            .storage_directory = storage_directory,
            .snapshot_wal_mode = memgraph::storage::Config::Durability::SnapshotWalMode::PERIODIC_SNAPSHOT_WITH_WAL,
            .snapshot_interval = std::chrono::minutes(20),
            .wal_file_size_kibibytes = 1,
            .wal_file_flush_every_n_tx = kFlushWalEvery}};
    memgraph::replication::ReplicationState repl_state{memgraph::storage::ReplicationStateHelper(config)};
    memgraph::dbms::Database db{config, repl_state};
    const uint64_t kNumVertices = 1000;
    std::vector<memgraph::storage::Gid> gids;
    gids.reserve(kNumVertices);
    for (uint64_t i = 0; i < kNumVertices; ++i) {
      auto acc = db.storage()->Access();
      auto vertex = acc->CreateVertex();
      gids.push_back(vertex.Gid());
      ASSERT_FALSE(acc->Commit().HasError());
    }
    for (uint64_t i = 0; i < kNumVertices; ++i) {
      auto acc = db.storage()->Access();
      auto vertex = acc->FindVertex(gids[i], memgraph::storage::View::OLD);
      ASSERT_TRUE(vertex);
      ASSERT_TRUE(
          vertex->SetProperty(db.storage()->NameToProperty("nandare"), memgraph::storage::PropertyValue("haihaihai!"))
              .HasValue());
      ASSERT_FALSE(acc->Commit().HasError());
    }
  }

  ASSERT_EQ(GetSnapshotsList().size(), 0);
  ASSERT_EQ(GetBackupSnapshotsList().size(), 0);
  ASSERT_GE(GetWalsList().size(), 1);
  ASSERT_GE(GetBackupWalsList().size(), 1);

  // Restore unrelated WALs.
  RestoreBackups();

  ASSERT_EQ(GetSnapshotsList().size(), 0);
  ASSERT_EQ(GetBackupSnapshotsList().size(), 0);
  ASSERT_GE(GetWalsList().size(), 2);
  ASSERT_EQ(GetBackupWalsList().size(), 0);

  // Remove second WAL.
  {
    auto wals = GetWalsList();
    ASSERT_GT(wals.size(), unrelated_wals + 2);
    const auto &wal_file = wals[wals.size() - unrelated_wals - 2];
    spdlog::info("Deleting WAL file {}", wal_file);
    ASSERT_TRUE(std::filesystem::remove(wal_file));
  }

  // Recover WALs.
  ASSERT_DEATH(([&]() {
                 memgraph::storage::Config config{
                     .items = {.properties_on_edges = GetParam()},
                     .durability = {.storage_directory = storage_directory, .recover_on_startup = true}};
                 memgraph::replication::ReplicationState repl_state{memgraph::storage::ReplicationStateHelper(config)};
                 memgraph::dbms::Database db{config, repl_state};
               }())  // iile
               ,
               "");
}

// NOLINTNEXTLINE(hicpp-special-member-functions)
TEST_P(DurabilityTest, WalCorruptSecond) {
  // Create unrelated WALs.
  {
    memgraph::storage::Config config{
        .items = {.properties_on_edges = GetParam()},
        .durability = {
            .storage_directory = storage_directory,
            .snapshot_wal_mode = memgraph::storage::Config::Durability::SnapshotWalMode::PERIODIC_SNAPSHOT_WITH_WAL,
            .snapshot_interval = std::chrono::minutes(20),
            .wal_file_size_kibibytes = 1,
            .wal_file_flush_every_n_tx = kFlushWalEvery}};
    memgraph::replication::ReplicationState repl_state{memgraph::storage::ReplicationStateHelper(config)};
    memgraph::dbms::Database db{config, repl_state};
    auto acc = db.storage()->Access();
    for (uint64_t i = 0; i < 1000; ++i) {
      acc->CreateVertex();
    }
    ASSERT_FALSE(acc->Commit().HasError());
  }

  ASSERT_EQ(GetSnapshotsList().size(), 0);
  ASSERT_EQ(GetBackupSnapshotsList().size(), 0);
  ASSERT_GE(GetWalsList().size(), 1);
  ASSERT_EQ(GetBackupWalsList().size(), 0);

  uint64_t unrelated_wals = GetWalsList().size();

  // Create WALs.
  {
    memgraph::storage::Config config{
        .items = {.properties_on_edges = GetParam()},
        .durability = {
            .storage_directory = storage_directory,
            .snapshot_wal_mode = memgraph::storage::Config::Durability::SnapshotWalMode::PERIODIC_SNAPSHOT_WITH_WAL,
            .snapshot_interval = std::chrono::minutes(20),
            .wal_file_size_kibibytes = 1,
            .wal_file_flush_every_n_tx = kFlushWalEvery}};
    memgraph::replication::ReplicationState repl_state{memgraph::storage::ReplicationStateHelper(config)};
    memgraph::dbms::Database db{config, repl_state};
    const uint64_t kNumVertices = 1000;
    std::vector<memgraph::storage::Gid> gids;
    gids.reserve(kNumVertices);
    for (uint64_t i = 0; i < kNumVertices; ++i) {
      auto acc = db.storage()->Access();
      auto vertex = acc->CreateVertex();
      gids.push_back(vertex.Gid());
      ASSERT_FALSE(acc->Commit().HasError());
    }
    for (uint64_t i = 0; i < kNumVertices; ++i) {
      auto acc = db.storage()->Access();
      auto vertex = acc->FindVertex(gids[i], memgraph::storage::View::OLD);
      ASSERT_TRUE(vertex);
      ASSERT_TRUE(
          vertex->SetProperty(db.storage()->NameToProperty("nandare"), memgraph::storage::PropertyValue("haihaihai!"))
              .HasValue());
      ASSERT_FALSE(acc->Commit().HasError());
    }
  }

  ASSERT_EQ(GetSnapshotsList().size(), 0);
  ASSERT_EQ(GetBackupSnapshotsList().size(), 0);
  ASSERT_GE(GetWalsList().size(), 1);
  ASSERT_GE(GetBackupWalsList().size(), 1);

  // Restore unrelated WALs.
  RestoreBackups();

  ASSERT_EQ(GetSnapshotsList().size(), 0);
  ASSERT_EQ(GetBackupSnapshotsList().size(), 0);
  ASSERT_GE(GetWalsList().size(), 2);
  ASSERT_EQ(GetBackupWalsList().size(), 0);

  // Destroy second WAL.
  {
    auto wals = GetWalsList();
    ASSERT_GT(wals.size(), unrelated_wals + 2);
    const auto &wal_file = wals[wals.size() - unrelated_wals - 2];
    DestroyWalFirstDelta(wal_file);
  }

  // Recover WALs.
  ASSERT_DEATH(([&]() {
                 memgraph::storage::Config config{
                     .items = {.properties_on_edges = GetParam()},
                     .durability = {.storage_directory = storage_directory, .recover_on_startup = true}};
                 memgraph::replication::ReplicationState repl_state{memgraph::storage::ReplicationStateHelper(config)};
                 memgraph::dbms::Database db{config, repl_state};
               }())  // iile
               ,
               "");
}

// NOLINTNEXTLINE(hicpp-special-member-functions)
TEST_P(DurabilityTest, WalCorruptLastTransaction) {
  // Create WALs
  {
    memgraph::storage::Config config{
        .items = {.properties_on_edges = GetParam()},
        .durability = {
            .storage_directory = storage_directory,
            .snapshot_wal_mode = memgraph::storage::Config::Durability::SnapshotWalMode::PERIODIC_SNAPSHOT_WITH_WAL,
            .snapshot_interval = std::chrono::minutes(20),
            .wal_file_size_kibibytes = 1,
            .wal_file_flush_every_n_tx = kFlushWalEvery}};
    memgraph::replication::ReplicationState repl_state{memgraph::storage::ReplicationStateHelper(config)};
    memgraph::dbms::Database db{config, repl_state};
    CreateBaseDataset(db.storage(), GetParam());
    CreateExtendedDataset(db.storage(), /* single_transaction = */ true);
  }

  ASSERT_EQ(GetSnapshotsList().size(), 0);
  ASSERT_EQ(GetBackupSnapshotsList().size(), 0);
  ASSERT_GE(GetWalsList().size(), 2);
  ASSERT_EQ(GetBackupWalsList().size(), 0);

  // Destroy last transaction in the latest WAL.
  {
    auto wals = GetWalsList();
    ASSERT_GE(wals.size(), 2);
    const auto &wal_file = wals.front();
    DestroyWalSuffix(wal_file);
  }

  // Recover WALs.
  memgraph::storage::Config config{.items = {.properties_on_edges = GetParam()},
                                   .durability = {.storage_directory = storage_directory, .recover_on_startup = true}};
  memgraph::replication::ReplicationState repl_state{memgraph::storage::ReplicationStateHelper(config)};
  memgraph::dbms::Database db{config, repl_state};
  // The extended dataset shouldn't be recovered because its WAL transaction was
  // corrupt.
  VerifyDataset(db.storage(), DatasetType::ONLY_BASE_WITH_EXTENDED_INDICES_AND_CONSTRAINTS, GetParam());

  // Try to use the storage.
  {
    auto acc = db.storage()->Access();
    auto vertex = acc->CreateVertex();
    auto edge = acc->CreateEdge(&vertex, &vertex, db.storage()->NameToEdgeType("et"));
    ASSERT_TRUE(edge.HasValue());
    ASSERT_FALSE(acc->Commit().HasError());
  }
}

// NOLINTNEXTLINE(hicpp-special-member-functions)
TEST_P(DurabilityTest, WalAllOperationsInSingleTransaction) {
  // Create WALs
  {
    memgraph::storage::Config config{
        .items = {.properties_on_edges = GetParam()},
        .durability = {
            .storage_directory = storage_directory,
            .snapshot_wal_mode = memgraph::storage::Config::Durability::SnapshotWalMode::PERIODIC_SNAPSHOT_WITH_WAL,
            .snapshot_interval = std::chrono::minutes(20),
            .wal_file_size_kibibytes = 1,
            .wal_file_flush_every_n_tx = kFlushWalEvery}};
    memgraph::replication::ReplicationState repl_state{memgraph::storage::ReplicationStateHelper(config)};
    memgraph::dbms::Database db{config, repl_state};
    auto acc = db.storage()->Access();
    auto vertex1 = acc->CreateVertex();
    auto vertex2 = acc->CreateVertex();
    ASSERT_TRUE(vertex1.AddLabel(acc->NameToLabel("nandare")).HasValue());
    ASSERT_TRUE(vertex2.SetProperty(acc->NameToProperty("haihai"), memgraph::storage::PropertyValue(42)).HasValue());
    ASSERT_TRUE(vertex1.RemoveLabel(acc->NameToLabel("nandare")).HasValue());
    auto edge1Res = acc->CreateEdge(&vertex1, &vertex2, acc->NameToEdgeType("et1"));
    ASSERT_TRUE(edge1Res.HasValue());
    auto edge1 = std::move(edge1Res.GetValue());

    ASSERT_TRUE(vertex2.SetProperty(acc->NameToProperty("haihai"), memgraph::storage::PropertyValue()).HasValue());
    auto vertex3 = acc->CreateVertex();
    auto edge2Res = acc->CreateEdge(&vertex3, &vertex3, acc->NameToEdgeType("et2"));
    ASSERT_TRUE(edge2Res.HasValue());
    auto edge2 = std::move(edge2Res.GetValue());
    if (GetParam()) {
      ASSERT_TRUE(edge2.SetProperty(acc->NameToProperty("meaning"), memgraph::storage::PropertyValue(true)).HasValue());
      ASSERT_TRUE(
          edge1.SetProperty(acc->NameToProperty("hello"), memgraph::storage::PropertyValue("world")).HasValue());
      ASSERT_TRUE(edge2.SetProperty(acc->NameToProperty("meaning"), memgraph::storage::PropertyValue()).HasValue());
    }
    ASSERT_TRUE(vertex3.AddLabel(acc->NameToLabel("test")).HasValue());
    ASSERT_TRUE(vertex3.SetProperty(acc->NameToProperty("nonono"), memgraph::storage::PropertyValue(-1)).HasValue());
    ASSERT_TRUE(vertex3.SetProperty(acc->NameToProperty("nonono"), memgraph::storage::PropertyValue()).HasValue());
    if (GetParam()) {
      ASSERT_TRUE(edge1.SetProperty(acc->NameToProperty("hello"), memgraph::storage::PropertyValue()).HasValue());
    }
    ASSERT_TRUE(vertex3.RemoveLabel(acc->NameToLabel("test")).HasValue());
    ASSERT_TRUE(acc->DetachDeleteVertex(&vertex1).HasValue());
    ASSERT_TRUE(acc->DeleteEdge(&edge2).HasValue());
    ASSERT_TRUE(acc->DeleteVertex(&vertex2).HasValue());
    ASSERT_TRUE(acc->DeleteVertex(&vertex3).HasValue());
    ASSERT_FALSE(acc->Commit().HasError());
  }

  ASSERT_EQ(GetSnapshotsList().size(), 0);
  ASSERT_EQ(GetBackupSnapshotsList().size(), 0);
  ASSERT_GE(GetWalsList().size(), 1);
  ASSERT_EQ(GetBackupWalsList().size(), 0);

  // Recover WALs.
  memgraph::storage::Config config{.items = {.properties_on_edges = GetParam()},
                                   .durability = {.storage_directory = storage_directory, .recover_on_startup = true}};
  memgraph::replication::ReplicationState repl_state{memgraph::storage::ReplicationStateHelper(config)};
  memgraph::dbms::Database db{config, repl_state};
  {
    auto acc = db.storage()->Access();
    uint64_t count = 0;
    auto iterable = acc->Vertices(memgraph::storage::View::OLD);
    for (auto it = iterable.begin(); it != iterable.end(); ++it) {
      ++count;
    }
    ASSERT_EQ(count, 0);
  }

  // Try to use the storage.
  {
    auto acc = db.storage()->Access();
    auto vertex = acc->CreateVertex();
    auto edge = acc->CreateEdge(&vertex, &vertex, db.storage()->NameToEdgeType("et"));
    ASSERT_TRUE(edge.HasValue());
    ASSERT_FALSE(acc->Commit().HasError());
  }
}

// NOLINTNEXTLINE(hicpp-special-member-functions)
TEST_P(DurabilityTest, WalAndSnapshot) {
  // Create snapshot and WALs.
  {
    memgraph::storage::Config config{
        .items = {.properties_on_edges = GetParam()},
        .durability = {
            .storage_directory = storage_directory,
            .snapshot_wal_mode = memgraph::storage::Config::Durability::SnapshotWalMode::PERIODIC_SNAPSHOT_WITH_WAL,
            .snapshot_interval = std::chrono::milliseconds(2000),
            .wal_file_flush_every_n_tx = kFlushWalEvery}};
    memgraph::replication::ReplicationState repl_state{memgraph::storage::ReplicationStateHelper(config)};
    memgraph::dbms::Database db{config, repl_state};
    CreateBaseDataset(db.storage(), GetParam());
    std::this_thread::sleep_for(std::chrono::milliseconds(2500));
    CreateExtendedDataset(db.storage());
  }

  ASSERT_GE(GetSnapshotsList().size(), 1);
  ASSERT_EQ(GetBackupSnapshotsList().size(), 0);
  ASSERT_GE(GetWalsList().size(), 1);
  ASSERT_EQ(GetBackupWalsList().size(), 0);

  // Recover snapshot and WALs.
  memgraph::storage::Config config{.items = {.properties_on_edges = GetParam()},
                                   .durability = {.storage_directory = storage_directory, .recover_on_startup = true}};
  memgraph::replication::ReplicationState repl_state{memgraph::storage::ReplicationStateHelper(config)};
  memgraph::dbms::Database db{config, repl_state};
  VerifyDataset(db.storage(), DatasetType::BASE_WITH_EXTENDED, GetParam());

  // Try to use the storage.
  {
    auto acc = db.storage()->Access();
    auto vertex = acc->CreateVertex();
    auto edge = acc->CreateEdge(&vertex, &vertex, db.storage()->NameToEdgeType("et"));
    ASSERT_TRUE(edge.HasValue());
    ASSERT_FALSE(acc->Commit().HasError());
  }
}

// NOLINTNEXTLINE(hicpp-special-member-functions)
TEST_P(DurabilityTest, WalAndSnapshotAppendToExistingSnapshot) {
  // Create snapshot.
  {
    memgraph::storage::Config config{.items = {.properties_on_edges = GetParam()},
                                     .durability = {.storage_directory = storage_directory, .snapshot_on_exit = true}};
    memgraph::replication::ReplicationState repl_state{memgraph::storage::ReplicationStateHelper(config)};
    memgraph::dbms::Database db{config, repl_state};
    CreateBaseDataset(db.storage(), GetParam());
  }

  ASSERT_EQ(GetSnapshotsList().size(), 1);
  ASSERT_EQ(GetBackupSnapshotsList().size(), 0);
  ASSERT_EQ(GetWalsList().size(), 0);
  ASSERT_EQ(GetBackupWalsList().size(), 0);

  // Recover snapshot.
  {
    memgraph::storage::Config config{
        .items = {.properties_on_edges = GetParam()},
        .durability = {.storage_directory = storage_directory, .recover_on_startup = true}};
    memgraph::replication::ReplicationState repl_state{memgraph::storage::ReplicationStateHelper(config)};
    memgraph::dbms::Database db{config, repl_state};
    VerifyDataset(db.storage(), DatasetType::ONLY_BASE, GetParam());
  }

  // Recover snapshot and create WALs.
  {
    memgraph::storage::Config config{
        .items = {.properties_on_edges = GetParam()},
        .durability = {
            .storage_directory = storage_directory,
            .recover_on_startup = true,
            .snapshot_wal_mode = memgraph::storage::Config::Durability::SnapshotWalMode::PERIODIC_SNAPSHOT_WITH_WAL,
            .snapshot_interval = std::chrono::minutes(20),
            .wal_file_flush_every_n_tx = kFlushWalEvery}};
    memgraph::replication::ReplicationState repl_state{memgraph::storage::ReplicationStateHelper(config)};
    memgraph::dbms::Database db{config, repl_state};
    CreateExtendedDataset(db.storage());
  }

  ASSERT_EQ(GetSnapshotsList().size(), 1);
  ASSERT_EQ(GetBackupSnapshotsList().size(), 0);
  ASSERT_GE(GetWalsList().size(), 1);
  ASSERT_EQ(GetBackupWalsList().size(), 0);

  // Recover snapshot and WALs.
  memgraph::storage::Config config{.items = {.properties_on_edges = GetParam()},
                                   .durability = {.storage_directory = storage_directory, .recover_on_startup = true}};
  memgraph::replication::ReplicationState repl_state{memgraph::storage::ReplicationStateHelper(config)};
  memgraph::dbms::Database db{config, repl_state};
  VerifyDataset(db.storage(), DatasetType::BASE_WITH_EXTENDED, GetParam());

  // Try to use the storage.
  {
    auto acc = db.storage()->Access();
    auto vertex = acc->CreateVertex();
    auto edge = acc->CreateEdge(&vertex, &vertex, db.storage()->NameToEdgeType("et"));
    ASSERT_TRUE(edge.HasValue());
    ASSERT_FALSE(acc->Commit().HasError());
  }
}

// NOLINTNEXTLINE(hicpp-special-member-functions)
TEST_P(DurabilityTest, WalAndSnapshotAppendToExistingSnapshotAndWal) {
  // Create snapshot.
  {
    memgraph::storage::Config config{.items = {.properties_on_edges = GetParam()},
                                     .durability = {.storage_directory = storage_directory, .snapshot_on_exit = true}};
    memgraph::replication::ReplicationState repl_state{memgraph::storage::ReplicationStateHelper(config)};
    memgraph::dbms::Database db{config, repl_state};
    CreateBaseDataset(db.storage(), GetParam());
  }

  ASSERT_EQ(GetSnapshotsList().size(), 1);
  ASSERT_EQ(GetBackupSnapshotsList().size(), 0);
  ASSERT_EQ(GetWalsList().size(), 0);
  ASSERT_EQ(GetBackupWalsList().size(), 0);

  // Recover snapshot.
  {
    memgraph::storage::Config config{
        .items = {.properties_on_edges = GetParam()},
        .durability = {.storage_directory = storage_directory, .recover_on_startup = true}};
    memgraph::replication::ReplicationState repl_state{memgraph::storage::ReplicationStateHelper(config)};
    memgraph::dbms::Database db{config, repl_state};
    VerifyDataset(db.storage(), DatasetType::ONLY_BASE, GetParam());
  }

  // Recover snapshot and create WALs.
  {
    memgraph::storage::Config config{
        .items = {.properties_on_edges = GetParam()},
        .durability = {
            .storage_directory = storage_directory,
            .recover_on_startup = true,
            .snapshot_wal_mode = memgraph::storage::Config::Durability::SnapshotWalMode::PERIODIC_SNAPSHOT_WITH_WAL,
            .snapshot_interval = std::chrono::minutes(20),
            .wal_file_flush_every_n_tx = kFlushWalEvery}};
    memgraph::replication::ReplicationState repl_state{memgraph::storage::ReplicationStateHelper(config)};
    memgraph::dbms::Database db{config, repl_state};
    CreateExtendedDataset(db.storage());
  }

  ASSERT_EQ(GetSnapshotsList().size(), 1);
  ASSERT_EQ(GetBackupSnapshotsList().size(), 0);
  ASSERT_GE(GetWalsList().size(), 1);
  ASSERT_EQ(GetBackupWalsList().size(), 0);

  // Recover snapshot and WALs and create more WALs.
  memgraph::storage::Gid vertex_gid;
  {
    memgraph::storage::Config config{
        .items = {.properties_on_edges = GetParam()},
        .durability = {
            .storage_directory = storage_directory,
            .recover_on_startup = true,
            .snapshot_wal_mode = memgraph::storage::Config::Durability::SnapshotWalMode::PERIODIC_SNAPSHOT_WITH_WAL,
            .snapshot_interval = std::chrono::minutes(20),
            .wal_file_flush_every_n_tx = kFlushWalEvery}};
    memgraph::replication::ReplicationState repl_state{memgraph::storage::ReplicationStateHelper(config)};
    memgraph::dbms::Database db{config, repl_state};
    VerifyDataset(db.storage(), DatasetType::BASE_WITH_EXTENDED, GetParam());
    auto acc = db.storage()->Access();
    auto vertex = acc->CreateVertex();
    vertex_gid = vertex.Gid();
    if (GetParam()) {
      ASSERT_TRUE(
          vertex.SetProperty(db.storage()->NameToProperty("meaning"), memgraph::storage::PropertyValue(42)).HasValue());
    }
    ASSERT_FALSE(acc->Commit().HasError());
  }

  ASSERT_EQ(GetSnapshotsList().size(), 1);
  ASSERT_EQ(GetBackupSnapshotsList().size(), 0);
  ASSERT_GE(GetWalsList().size(), 2);
  ASSERT_EQ(GetBackupWalsList().size(), 0);

  // Recover snapshot and WALs.
  memgraph::storage::Config config{.items = {.properties_on_edges = GetParam()},
                                   .durability = {.storage_directory = storage_directory, .recover_on_startup = true}};
  memgraph::replication::ReplicationState repl_state{memgraph::storage::ReplicationStateHelper(config)};
  memgraph::dbms::Database db{config, repl_state};
  VerifyDataset(db.storage(), DatasetType::BASE_WITH_EXTENDED, GetParam(),
                /* verify_info = */ false);
  {
    auto acc = db.storage()->Access();
    auto vertex = acc->FindVertex(vertex_gid, memgraph::storage::View::OLD);
    ASSERT_TRUE(vertex);
    auto labels = vertex->Labels(memgraph::storage::View::OLD);
    ASSERT_TRUE(labels.HasValue());
    ASSERT_EQ(labels->size(), 0);
    auto props = vertex->Properties(memgraph::storage::View::OLD);
    ASSERT_TRUE(props.HasValue());
    if (GetParam()) {
      ASSERT_THAT(*props, UnorderedElementsAre(std::make_pair(db.storage()->NameToProperty("meaning"),
                                                              memgraph::storage::PropertyValue(42))));
    } else {
      ASSERT_EQ(props->size(), 0);
    }
  }

  // Try to use the storage.
  {
    auto acc = db.storage()->Access();
    auto vertex = acc->CreateVertex();
    auto edge = acc->CreateEdge(&vertex, &vertex, db.storage()->NameToEdgeType("et"));
    ASSERT_TRUE(edge.HasValue());
    ASSERT_FALSE(acc->Commit().HasError());
  }
}

// NOLINTNEXTLINE(hicpp-special-member-functions)
TEST_P(DurabilityTest, WalAndSnapshotWalRetention) {
  // Create unrelated WALs.
  {
    memgraph::storage::Config config{
        .items = {.properties_on_edges = GetParam()},
        .durability = {
            .storage_directory = storage_directory,
            .snapshot_wal_mode = memgraph::storage::Config::Durability::SnapshotWalMode::PERIODIC_SNAPSHOT_WITH_WAL,
            .snapshot_interval = std::chrono::minutes(20),
            .wal_file_size_kibibytes = 1,
            .wal_file_flush_every_n_tx = kFlushWalEvery}};
    memgraph::replication::ReplicationState repl_state{memgraph::storage::ReplicationStateHelper(config)};
    memgraph::dbms::Database db{config, repl_state};
    auto acc = db.storage()->Access();
    for (uint64_t i = 0; i < 1000; ++i) {
      acc->CreateVertex();
    }
    ASSERT_FALSE(acc->Commit().HasError());
  }

  ASSERT_EQ(GetSnapshotsList().size(), 0);
  ASSERT_EQ(GetBackupSnapshotsList().size(), 0);
  ASSERT_GE(GetWalsList().size(), 1);
  ASSERT_EQ(GetBackupWalsList().size(), 0);

  uint64_t unrelated_wals = GetWalsList().size();

  uint64_t items_created = 0;

  // Create snapshot and WALs.
  {
    memgraph::storage::Config config{
        .items = {.properties_on_edges = GetParam()},
        .durability = {
            .storage_directory = storage_directory,
            .snapshot_wal_mode = memgraph::storage::Config::Durability::SnapshotWalMode::PERIODIC_SNAPSHOT_WITH_WAL,
            .snapshot_interval = std::chrono::seconds(2),
            .wal_file_size_kibibytes = 1,
            .wal_file_flush_every_n_tx = 1}};
    memgraph::replication::ReplicationState repl_state{memgraph::storage::ReplicationStateHelper(config)};
    memgraph::dbms::Database db{config, repl_state};
    // Restore unrelated snapshots after the database has been started.
    RestoreBackups();
    memgraph::utils::Timer timer;
    // Allow at least 6 snapshots to be created.
    while (timer.Elapsed().count() < 13.0) {
      auto acc = db.storage()->Access();
      acc->CreateVertex();
      ASSERT_FALSE(acc->Commit().HasError());
      ++items_created;
    }
  }

  ASSERT_EQ(GetSnapshotsList().size(), 3);
  ASSERT_EQ(GetBackupSnapshotsList().size(), 0);
  ASSERT_GE(GetWalsList().size(), unrelated_wals + 1);
  ASSERT_EQ(GetBackupWalsList().size(), 0);

  auto snapshots = GetSnapshotsList();
  ASSERT_EQ(snapshots.size(), 3);

  for (uint64_t i = 0; i < snapshots.size(); ++i) {
    spdlog::info("Recovery attempt {}", i);

    // Recover and verify data.
    {
      memgraph::storage::Config config{
          .items = {.properties_on_edges = GetParam()},
          .durability = {.storage_directory = storage_directory, .recover_on_startup = true}};
      memgraph::replication::ReplicationState repl_state{memgraph::storage::ReplicationStateHelper(config)};
      memgraph::dbms::Database db{config, repl_state};
      auto acc = db.storage()->Access();
      for (uint64_t j = 0; j < items_created; ++j) {
        auto vertex = acc->FindVertex(memgraph::storage::Gid::FromUint(j), memgraph::storage::View::OLD);
        ASSERT_TRUE(vertex);
      }
    }

    // Destroy current snapshot.
    CorruptSnapshot(snapshots[i]);
  }

  // Recover data after all of the snapshots have been destroyed. The recovery
  // shouldn't be possible because the initial WALs are already deleted.
  ASSERT_DEATH(([&]() {
                 memgraph::storage::Config config{
                     .items = {.properties_on_edges = GetParam()},
                     .durability = {.storage_directory = storage_directory, .recover_on_startup = true}};
                 memgraph::replication::ReplicationState repl_state{memgraph::storage::ReplicationStateHelper(config)};
                 memgraph::dbms::Database db{config, repl_state};
               }())  // iile
               ,
               "");
}

// NOLINTNEXTLINE(hicpp-special-member-functions)
TEST_P(DurabilityTest, SnapshotAndWalMixedUUID) {
  // Create unrelated snapshot and WALs.
  {
    memgraph::storage::Config config{
        .items = {.properties_on_edges = GetParam()},
        .durability = {
            .storage_directory = storage_directory,
            .snapshot_wal_mode = memgraph::storage::Config::Durability::SnapshotWalMode::PERIODIC_SNAPSHOT_WITH_WAL,
            .snapshot_interval = std::chrono::seconds(2)}};
    memgraph::replication::ReplicationState repl_state{memgraph::storage::ReplicationStateHelper(config)};
    memgraph::dbms::Database db{config, repl_state};
    auto acc = db.storage()->Access();
    for (uint64_t i = 0; i < 1000; ++i) {
      acc->CreateVertex();
    }
    ASSERT_FALSE(acc->Commit().HasError());
    std::this_thread::sleep_for(std::chrono::milliseconds(2500));
  }

  ASSERT_GE(GetSnapshotsList().size(), 1);
  ASSERT_EQ(GetBackupSnapshotsList().size(), 0);
  ASSERT_GE(GetWalsList().size(), 1);
  ASSERT_EQ(GetBackupWalsList().size(), 0);

  // Create snapshot and WALs.
  {
    memgraph::storage::Config config{
        .items = {.properties_on_edges = GetParam()},
        .durability = {
            .storage_directory = storage_directory,
            .snapshot_wal_mode = memgraph::storage::Config::Durability::SnapshotWalMode::PERIODIC_SNAPSHOT_WITH_WAL,
            .snapshot_interval = std::chrono::seconds(2)}};
    memgraph::replication::ReplicationState repl_state{memgraph::storage::ReplicationStateHelper(config)};
    memgraph::dbms::Database db{config, repl_state};
    CreateBaseDataset(db.storage(), GetParam());
    std::this_thread::sleep_for(std::chrono::milliseconds(2500));
    CreateExtendedDataset(db.storage());
    std::this_thread::sleep_for(std::chrono::milliseconds(2500));
  }

  ASSERT_GE(GetSnapshotsList().size(), 1);
  ASSERT_GE(GetBackupSnapshotsList().size(), 1);
  ASSERT_GE(GetWalsList().size(), 1);
  ASSERT_GE(GetBackupWalsList().size(), 1);

  // Restore unrelated snapshots and WALs.
  RestoreBackups();

  ASSERT_GE(GetSnapshotsList().size(), 2);
  ASSERT_EQ(GetBackupSnapshotsList().size(), 0);
  ASSERT_GE(GetWalsList().size(), 2);
  ASSERT_EQ(GetBackupWalsList().size(), 0);

  // Recover snapshot and WALs.
  memgraph::storage::Config config{.items = {.properties_on_edges = GetParam()},
                                   .durability = {.storage_directory = storage_directory, .recover_on_startup = true}};
  memgraph::replication::ReplicationState repl_state{memgraph::storage::ReplicationStateHelper(config)};
  memgraph::dbms::Database db{config, repl_state};
  VerifyDataset(db.storage(), DatasetType::BASE_WITH_EXTENDED, GetParam());

  // Try to use the storage.
  {
    auto acc = db.storage()->Access();
    auto vertex = acc->CreateVertex();
    auto edge = acc->CreateEdge(&vertex, &vertex, db.storage()->NameToEdgeType("et"));
    ASSERT_TRUE(edge.HasValue());
    ASSERT_FALSE(acc->Commit().HasError());
  }
}
