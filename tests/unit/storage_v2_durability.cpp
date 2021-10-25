// Copyright 2021 Memgraph Ltd.
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

#include "storage/v2/durability/paths.hpp"
#include "storage/v2/durability/snapshot.hpp"
#include "storage/v2/durability/version.hpp"
#include "storage/v2/storage.hpp"
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
      : base_vertex_gids_(kNumBaseVertices, storage::Gid::FromUint(std::numeric_limits<uint64_t>::max())),
        base_edge_gids_(kNumBaseEdges, storage::Gid::FromUint(std::numeric_limits<uint64_t>::max())),
        extended_vertex_gids_(kNumExtendedVertices, storage::Gid::FromUint(std::numeric_limits<uint64_t>::max())),
        extended_edge_gids_(kNumExtendedEdges, storage::Gid::FromUint(std::numeric_limits<uint64_t>::max())) {}

  void SetUp() override { Clear(); }

  void TearDown() override { Clear(); }

  void CreateBaseDataset(storage::Storage *store, bool properties_on_edges) {
    auto label_indexed = store->NameToLabel("base_indexed");
    auto label_unindexed = store->NameToLabel("base_unindexed");
    auto property_id = store->NameToProperty("id");
    auto property_extra = store->NameToProperty("extra");
    auto et1 = store->NameToEdgeType("base_et1");
    auto et2 = store->NameToEdgeType("base_et2");

    // Create label index.
    ASSERT_TRUE(store->CreateIndex(label_unindexed));

    // Create label+property index.
    ASSERT_TRUE(store->CreateIndex(label_indexed, property_id));

    // Create existence constraint.
    ASSERT_FALSE(store->CreateExistenceConstraint(label_unindexed, property_id).HasError());

    // Create unique constraint.
    ASSERT_FALSE(store->CreateUniqueConstraint(label_unindexed, {property_id, property_extra}).HasError());

    // Create vertices.
    for (uint64_t i = 0; i < kNumBaseVertices; ++i) {
      auto acc = store->Access();
      auto vertex = acc.CreateVertex();
      base_vertex_gids_[i] = vertex.Gid();
      if (i < kNumBaseVertices / 2) {
        ASSERT_TRUE(vertex.AddLabel(label_indexed).HasValue());
      } else {
        ASSERT_TRUE(vertex.AddLabel(label_unindexed).HasValue());
      }
      if (i < kNumBaseVertices / 3 || i >= kNumBaseVertices / 2) {
        ASSERT_TRUE(vertex.SetProperty(property_id, storage::PropertyValue(static_cast<int64_t>(i))).HasValue());
      }
      ASSERT_FALSE(acc.Commit().HasError());
    }

    // Create edges.
    for (uint64_t i = 0; i < kNumBaseEdges; ++i) {
      auto acc = store->Access();
      auto vertex1 = acc.FindVertex(base_vertex_gids_[(i / 2) % kNumBaseVertices], storage::View::OLD);
      ASSERT_TRUE(vertex1);
      auto vertex2 = acc.FindVertex(base_vertex_gids_[(i / 3) % kNumBaseVertices], storage::View::OLD);
      ASSERT_TRUE(vertex2);
      storage::EdgeTypeId et;
      if (i < kNumBaseEdges / 2) {
        et = et1;
      } else {
        et = et2;
      }
      auto edge = acc.CreateEdge(&*vertex1, &*vertex2, et);
      ASSERT_TRUE(edge.HasValue());
      base_edge_gids_[i] = edge->Gid();
      if (properties_on_edges) {
        ASSERT_TRUE(edge->SetProperty(property_id, storage::PropertyValue(static_cast<int64_t>(i))).HasValue());
      } else {
        auto ret = edge->SetProperty(property_id, storage::PropertyValue(static_cast<int64_t>(i)));
        ASSERT_TRUE(ret.HasError());
        ASSERT_EQ(ret.GetError(), storage::Error::PROPERTIES_DISABLED);
      }
      ASSERT_FALSE(acc.Commit().HasError());
    }
  }

  void CreateExtendedDataset(storage::Storage *store, bool single_transaction = false) {
    auto label_indexed = store->NameToLabel("extended_indexed");
    auto label_unused = store->NameToLabel("extended_unused");
    auto property_count = store->NameToProperty("count");
    auto et3 = store->NameToEdgeType("extended_et3");
    auto et4 = store->NameToEdgeType("extended_et4");

    // Create label index.
    ASSERT_TRUE(store->CreateIndex(label_unused));

    // Create label+property index.
    ASSERT_TRUE(store->CreateIndex(label_indexed, property_count));

    // Create existence constraint.
    ASSERT_FALSE(store->CreateExistenceConstraint(label_unused, property_count).HasError());

    // Create unique constraint.
    ASSERT_FALSE(store->CreateUniqueConstraint(label_unused, {property_count}).HasError());

    // Storage accessor.
    std::optional<storage::Storage::Accessor> acc;
    if (single_transaction) acc.emplace(store->Access());

    // Create vertices.
    for (uint64_t i = 0; i < kNumExtendedVertices; ++i) {
      if (!single_transaction) acc.emplace(store->Access());
      auto vertex = acc->CreateVertex();
      extended_vertex_gids_[i] = vertex.Gid();
      if (i < kNumExtendedVertices / 2) {
        ASSERT_TRUE(vertex.AddLabel(label_indexed).HasValue());
      }
      if (i < kNumExtendedVertices / 3 || i >= kNumExtendedVertices / 2) {
        ASSERT_TRUE(vertex.SetProperty(property_count, storage::PropertyValue("nandare")).HasValue());
      }
      if (!single_transaction) ASSERT_FALSE(acc->Commit().HasError());
    }

    // Create edges.
    for (uint64_t i = 0; i < kNumExtendedEdges; ++i) {
      if (!single_transaction) acc.emplace(store->Access());
      auto vertex1 = acc->FindVertex(extended_vertex_gids_[(i / 5) % kNumExtendedVertices], storage::View::NEW);
      ASSERT_TRUE(vertex1);
      auto vertex2 = acc->FindVertex(extended_vertex_gids_[(i / 6) % kNumExtendedVertices], storage::View::NEW);
      ASSERT_TRUE(vertex2);
      storage::EdgeTypeId et;
      if (i < kNumExtendedEdges / 4) {
        et = et3;
      } else {
        et = et4;
      }
      auto edge = acc->CreateEdge(&*vertex1, &*vertex2, et);
      ASSERT_TRUE(edge.HasValue());
      extended_edge_gids_[i] = edge->Gid();
      if (!single_transaction) ASSERT_FALSE(acc->Commit().HasError());
    }

    if (single_transaction) ASSERT_FALSE(acc->Commit().HasError());
  }

  void VerifyDataset(storage::Storage *store, DatasetType type, bool properties_on_edges, bool verify_info = true) {
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

    // Verify indices info.
    {
      auto info = store->ListAllIndices();
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

    // Verify constraints info.
    {
      auto info = store->ListAllConstraints();
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

    // Create storage accessor.
    auto acc = store->Access();

    // Verify base dataset.
    if (have_base_dataset) {
      // Verify vertices.
      for (uint64_t i = 0; i < kNumBaseVertices; ++i) {
        auto vertex = acc.FindVertex(base_vertex_gids_[i], storage::View::OLD);
        ASSERT_TRUE(vertex);
        auto labels = vertex->Labels(storage::View::OLD);
        ASSERT_TRUE(labels.HasValue());
        if (i < kNumBaseVertices / 2) {
          ASSERT_THAT(*labels, UnorderedElementsAre(base_label_indexed));
        } else {
          ASSERT_THAT(*labels, UnorderedElementsAre(base_label_unindexed));
        }
        auto properties = vertex->Properties(storage::View::OLD);
        ASSERT_TRUE(properties.HasValue());
        if (i < kNumBaseVertices / 3 || i >= kNumBaseVertices / 2) {
          ASSERT_EQ(properties->size(), 1);
          ASSERT_EQ((*properties)[property_id], storage::PropertyValue(static_cast<int64_t>(i)));
        } else {
          ASSERT_EQ(properties->size(), 0);
        }
      }

      // Verify edges.
      for (uint64_t i = 0; i < kNumBaseEdges; ++i) {
        auto find_edge = [&](const auto &edges) -> std::optional<storage::EdgeAccessor> {
          for (const auto &edge : edges) {
            if (edge.Gid() == base_edge_gids_[i]) {
              return edge;
            }
          }
          return std::nullopt;
        };

        {
          auto vertex1 = acc.FindVertex(base_vertex_gids_[(i / 2) % kNumBaseVertices], storage::View::OLD);
          ASSERT_TRUE(vertex1);
          auto out_edges = vertex1->OutEdges(storage::View::OLD);
          ASSERT_TRUE(out_edges.HasValue());
          auto edge1 = find_edge(*out_edges);
          ASSERT_TRUE(edge1);
          if (i < kNumBaseEdges / 2) {
            ASSERT_EQ(edge1->EdgeType(), et1);
          } else {
            ASSERT_EQ(edge1->EdgeType(), et2);
          }
          auto properties = edge1->Properties(storage::View::OLD);
          ASSERT_TRUE(properties.HasValue());
          if (properties_on_edges) {
            ASSERT_EQ(properties->size(), 1);
            ASSERT_EQ((*properties)[property_id], storage::PropertyValue(static_cast<int64_t>(i)));
          } else {
            ASSERT_EQ(properties->size(), 0);
          }
        }

        {
          auto vertex2 = acc.FindVertex(base_vertex_gids_[(i / 3) % kNumBaseVertices], storage::View::OLD);
          ASSERT_TRUE(vertex2);
          auto in_edges = vertex2->InEdges(storage::View::OLD);
          ASSERT_TRUE(in_edges.HasValue());
          auto edge2 = find_edge(*in_edges);
          ASSERT_TRUE(edge2);
          if (i < kNumBaseEdges / 2) {
            ASSERT_EQ(edge2->EdgeType(), et1);
          } else {
            ASSERT_EQ(edge2->EdgeType(), et2);
          }
          auto properties = edge2->Properties(storage::View::OLD);
          ASSERT_TRUE(properties.HasValue());
          if (properties_on_edges) {
            ASSERT_EQ(properties->size(), 1);
            ASSERT_EQ((*properties)[property_id], storage::PropertyValue(static_cast<int64_t>(i)));
          } else {
            ASSERT_EQ(properties->size(), 0);
          }
        }
      }

      // Verify label indices.
      {
        std::vector<storage::VertexAccessor> vertices;
        vertices.reserve(kNumBaseVertices / 2);
        for (auto vertex : acc.Vertices(base_label_unindexed, storage::View::OLD)) {
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
        std::vector<storage::VertexAccessor> vertices;
        vertices.reserve(kNumBaseVertices / 3);
        for (auto vertex : acc.Vertices(base_label_indexed, property_id, storage::View::OLD)) {
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
        auto vertex = acc.FindVertex(base_vertex_gids_[i], storage::View::OLD);
        ASSERT_FALSE(vertex);
      }

      if (type == DatasetType::ONLY_EXTENDED_WITH_BASE_INDICES_AND_CONSTRAINTS) {
        // Verify label indices.
        {
          uint64_t count = 0;
          auto iterable = acc.Vertices(base_label_unindexed, storage::View::OLD);
          for (auto it = iterable.begin(); it != iterable.end(); ++it) {
            ++count;
          }
          ASSERT_EQ(count, 0);
        }

        // Verify label+property index.
        {
          uint64_t count = 0;
          auto iterable = acc.Vertices(base_label_indexed, property_id, storage::View::OLD);
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
        auto vertex = acc.FindVertex(extended_vertex_gids_[i], storage::View::OLD);
        ASSERT_TRUE(vertex);
        auto labels = vertex->Labels(storage::View::OLD);
        ASSERT_TRUE(labels.HasValue());
        if (i < kNumExtendedVertices / 2) {
          ASSERT_THAT(*labels, UnorderedElementsAre(extended_label_indexed));
        }
        auto properties = vertex->Properties(storage::View::OLD);
        ASSERT_TRUE(properties.HasValue());
        if (i < kNumExtendedVertices / 3 || i >= kNumExtendedVertices / 2) {
          ASSERT_EQ(properties->size(), 1);
          ASSERT_EQ((*properties)[property_count], storage::PropertyValue("nandare"));
        } else {
          ASSERT_EQ(properties->size(), 0);
        }
      }

      // Verify edges.
      for (uint64_t i = 0; i < kNumExtendedEdges; ++i) {
        auto find_edge = [&](const auto &edges) -> std::optional<storage::EdgeAccessor> {
          for (const auto &edge : edges) {
            if (edge.Gid() == extended_edge_gids_[i]) {
              return edge;
            }
          }
          return std::nullopt;
        };

        {
          auto vertex1 = acc.FindVertex(extended_vertex_gids_[(i / 5) % kNumExtendedVertices], storage::View::OLD);
          ASSERT_TRUE(vertex1);
          auto out_edges = vertex1->OutEdges(storage::View::OLD);
          ASSERT_TRUE(out_edges.HasValue());
          auto edge1 = find_edge(*out_edges);
          ASSERT_TRUE(edge1);
          if (i < kNumExtendedEdges / 4) {
            ASSERT_EQ(edge1->EdgeType(), et3);
          } else {
            ASSERT_EQ(edge1->EdgeType(), et4);
          }
          auto properties = edge1->Properties(storage::View::OLD);
          ASSERT_TRUE(properties.HasValue());
          ASSERT_EQ(properties->size(), 0);
        }

        {
          auto vertex2 = acc.FindVertex(extended_vertex_gids_[(i / 6) % kNumExtendedVertices], storage::View::OLD);
          ASSERT_TRUE(vertex2);
          auto in_edges = vertex2->InEdges(storage::View::OLD);
          ASSERT_TRUE(in_edges.HasValue());
          auto edge2 = find_edge(*in_edges);
          ASSERT_TRUE(edge2);
          if (i < kNumExtendedEdges / 4) {
            ASSERT_EQ(edge2->EdgeType(), et3);
          } else {
            ASSERT_EQ(edge2->EdgeType(), et4);
          }
          auto properties = edge2->Properties(storage::View::OLD);
          ASSERT_TRUE(properties.HasValue());
          ASSERT_EQ(properties->size(), 0);
        }
      }

      // Verify label indices.
      {
        std::vector<storage::VertexAccessor> vertices;
        vertices.reserve(kNumExtendedVertices / 2);
        for (auto vertex : acc.Vertices(extended_label_unused, storage::View::OLD)) {
          vertices.push_back(vertex);
        }
        ASSERT_EQ(vertices.size(), 0);
      }

      // Verify label+property index.
      {
        std::vector<storage::VertexAccessor> vertices;
        vertices.reserve(kNumExtendedVertices / 3);
        for (auto vertex : acc.Vertices(extended_label_indexed, property_count, storage::View::OLD)) {
          vertices.push_back(vertex);
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
        auto vertex = acc.FindVertex(extended_vertex_gids_[i], storage::View::OLD);
        ASSERT_FALSE(vertex);
      }

      if (type == DatasetType::ONLY_BASE_WITH_EXTENDED_INDICES_AND_CONSTRAINTS) {
        // Verify label indices.
        {
          uint64_t count = 0;
          auto iterable = acc.Vertices(extended_label_unused, storage::View::OLD);
          for (auto it = iterable.begin(); it != iterable.end(); ++it) {
            ++count;
          }
          ASSERT_EQ(count, 0);
        }

        // Verify label+property index.
        {
          uint64_t count = 0;
          auto iterable = acc.Vertices(extended_label_indexed, property_count, storage::View::OLD);
          for (auto it = iterable.begin(); it != iterable.end(); ++it) {
            ++count;
          }
          ASSERT_EQ(count, 0);
        }
      }
    }

    if (verify_info) {
      auto info = store->GetInfo();
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
    return GetFilesList(storage_directory / storage::durability::kSnapshotDirectory);
  }

  std::vector<std::filesystem::path> GetBackupSnapshotsList() {
    return GetFilesList(storage_directory / storage::durability::kBackupDirectory /
                        storage::durability::kSnapshotDirectory);
  }

  std::vector<std::filesystem::path> GetWalsList() {
    return GetFilesList(storage_directory / storage::durability::kWalDirectory);
  }

  std::vector<std::filesystem::path> GetBackupWalsList() {
    return GetFilesList(storage_directory / storage::durability::kBackupDirectory / storage::durability::kWalDirectory);
  }

  void RestoreBackups() {
    {
      auto backup_snapshots = GetBackupSnapshotsList();
      for (const auto &item : backup_snapshots) {
        std::filesystem::rename(item, storage_directory / storage::durability::kSnapshotDirectory / item.filename());
      }
    }
    {
      auto backup_wals = GetBackupWalsList();
      for (const auto &item : backup_wals) {
        std::filesystem::rename(item, storage_directory / storage::durability::kWalDirectory / item.filename());
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

  std::vector<storage::Gid> base_vertex_gids_;
  std::vector<storage::Gid> base_edge_gids_;
  std::vector<storage::Gid> extended_vertex_gids_;
  std::vector<storage::Gid> extended_edge_gids_;
};

void DestroySnapshot(const std::filesystem::path &path) {
  auto info = storage::durability::ReadSnapshotInfo(path);
  spdlog::info("Destroying snapshot {}", path);
  utils::OutputFile file;
  file.Open(path, utils::OutputFile::Mode::OVERWRITE_EXISTING);
  file.SetPosition(utils::OutputFile::Position::SET, info.offset_vertices);
  auto value = static_cast<uint8_t>(storage::durability::Marker::TYPE_MAP);
  file.Write(&value, sizeof(value));
  file.Sync();
  file.Close();
}

void DestroyWalFirstDelta(const std::filesystem::path &path) {
  auto info = storage::durability::ReadWalInfo(path);
  spdlog::info("Destroying WAL {}", path);
  utils::OutputFile file;
  file.Open(path, utils::OutputFile::Mode::OVERWRITE_EXISTING);
  file.SetPosition(utils::OutputFile::Position::SET, info.offset_deltas);
  auto value = static_cast<uint8_t>(storage::durability::Marker::TYPE_MAP);
  file.Write(&value, sizeof(value));
  file.Sync();
  file.Close();
}

void DestroyWalSuffix(const std::filesystem::path &path) {
  auto info = storage::durability::ReadWalInfo(path);
  spdlog::info("Destroying WAL {}", path);
  utils::OutputFile file;
  file.Open(path, utils::OutputFile::Mode::OVERWRITE_EXISTING);
  ASSERT_LT(info.offset_deltas, file.SetPosition(utils::OutputFile::Position::RELATIVE_TO_END, -100));
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
    storage::Storage store({.items = {.properties_on_edges = GetParam()},
                            .durability = {.storage_directory = storage_directory, .snapshot_on_exit = true}});
    CreateBaseDataset(&store, GetParam());
    VerifyDataset(&store, DatasetType::ONLY_BASE, GetParam());
    CreateExtendedDataset(&store);
    VerifyDataset(&store, DatasetType::BASE_WITH_EXTENDED, GetParam());
  }

  ASSERT_EQ(GetSnapshotsList().size(), 1);
  ASSERT_EQ(GetBackupSnapshotsList().size(), 0);
  ASSERT_EQ(GetWalsList().size(), 0);
  ASSERT_EQ(GetBackupWalsList().size(), 0);

  // Recover snapshot.
  storage::Storage store({.items = {.properties_on_edges = GetParam()},
                          .durability = {.storage_directory = storage_directory, .recover_on_startup = true}});
  VerifyDataset(&store, DatasetType::BASE_WITH_EXTENDED, GetParam());

  // Try to use the storage.
  {
    auto acc = store.Access();
    auto vertex = acc.CreateVertex();
    auto edge = acc.CreateEdge(&vertex, &vertex, store.NameToEdgeType("et"));
    ASSERT_TRUE(edge.HasValue());
    ASSERT_FALSE(acc.Commit().HasError());
  }
}

// NOLINTNEXTLINE(hicpp-special-member-functions)
TEST_P(DurabilityTest, SnapshotPeriodic) {
  // Create snapshot.
  {
    storage::Storage store(
        {.items = {.properties_on_edges = GetParam()},
         .durability = {.storage_directory = storage_directory,
                        .snapshot_wal_mode = storage::Config::Durability::SnapshotWalMode::PERIODIC_SNAPSHOT,
                        .snapshot_interval = std::chrono::milliseconds(2000)}});
    CreateBaseDataset(&store, GetParam());
    std::this_thread::sleep_for(std::chrono::milliseconds(2500));
  }

  ASSERT_GE(GetSnapshotsList().size(), 1);
  ASSERT_EQ(GetBackupSnapshotsList().size(), 0);
  ASSERT_EQ(GetWalsList().size(), 0);
  ASSERT_EQ(GetBackupWalsList().size(), 0);

  // Recover snapshot.
  storage::Storage store({.items = {.properties_on_edges = GetParam()},
                          .durability = {.storage_directory = storage_directory, .recover_on_startup = true}});
  VerifyDataset(&store, DatasetType::ONLY_BASE, GetParam());

  // Try to use the storage.
  {
    auto acc = store.Access();
    auto vertex = acc.CreateVertex();
    auto edge = acc.CreateEdge(&vertex, &vertex, store.NameToEdgeType("et"));
    ASSERT_TRUE(edge.HasValue());
    ASSERT_FALSE(acc.Commit().HasError());
  }
}

// NOLINTNEXTLINE(hicpp-special-member-functions)
TEST_P(DurabilityTest, SnapshotFallback) {
  // Create snapshot.
  {
    storage::Storage store(
        {.items = {.properties_on_edges = GetParam()},
         .durability = {.storage_directory = storage_directory,
                        .snapshot_wal_mode = storage::Config::Durability::SnapshotWalMode::PERIODIC_SNAPSHOT,
                        .snapshot_interval = std::chrono::milliseconds(3000)}});
    CreateBaseDataset(&store, GetParam());
    std::this_thread::sleep_for(std::chrono::milliseconds(3500));
    ASSERT_EQ(GetSnapshotsList().size(), 1);
    CreateExtendedDataset(&store);
    std::this_thread::sleep_for(std::chrono::milliseconds(3000));
  }

  ASSERT_EQ(GetSnapshotsList().size(), 2);
  ASSERT_EQ(GetBackupSnapshotsList().size(), 0);
  ASSERT_EQ(GetWalsList().size(), 0);
  ASSERT_EQ(GetBackupWalsList().size(), 0);

  // Destroy last snapshot.
  {
    auto snapshots = GetSnapshotsList();
    ASSERT_EQ(snapshots.size(), 2);
    DestroySnapshot(*snapshots.begin());
  }

  // Recover snapshot.
  storage::Storage store({.items = {.properties_on_edges = GetParam()},
                          .durability = {.storage_directory = storage_directory, .recover_on_startup = true}});
  VerifyDataset(&store, DatasetType::ONLY_BASE, GetParam());

  // Try to use the storage.
  {
    auto acc = store.Access();
    auto vertex = acc.CreateVertex();
    auto edge = acc.CreateEdge(&vertex, &vertex, store.NameToEdgeType("et"));
    ASSERT_TRUE(edge.HasValue());
    ASSERT_FALSE(acc.Commit().HasError());
  }
}

// NOLINTNEXTLINE(hicpp-special-member-functions)
TEST_P(DurabilityTest, SnapshotEverythingCorrupt) {
  // Create unrelated snapshot.
  {
    storage::Storage store({.items = {.properties_on_edges = GetParam()},
                            .durability = {.storage_directory = storage_directory, .snapshot_on_exit = true}});
    auto acc = store.Access();
    for (uint64_t i = 0; i < 1000; ++i) {
      acc.CreateVertex();
    }
    ASSERT_FALSE(acc.Commit().HasError());
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
    auto info = storage::durability::ReadSnapshotInfo(*snapshots.begin());
    unrelated_uuid = info.uuid;
  }

  // Create snapshot.
  {
    storage::Storage store(
        {.items = {.properties_on_edges = GetParam()},
         .durability = {.storage_directory = storage_directory,
                        .snapshot_wal_mode = storage::Config::Durability::SnapshotWalMode::PERIODIC_SNAPSHOT,
                        .snapshot_interval = std::chrono::milliseconds(2000)}});
    CreateBaseDataset(&store, GetParam());
    std::this_thread::sleep_for(std::chrono::milliseconds(2500));
    CreateExtendedDataset(&store);
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
      auto info = storage::durability::ReadSnapshotInfo(snapshot);
      if (info.uuid == unrelated_uuid) {
        spdlog::info("Skipping snapshot {}", snapshot);
        continue;
      }
      DestroySnapshot(snapshot);
    }
  }

  // Recover snapshot.
  ASSERT_DEATH(
      {
        storage::Storage store({.items = {.properties_on_edges = GetParam()},
                                .durability = {.storage_directory = storage_directory, .recover_on_startup = true}});
      },
      "");
}

// NOLINTNEXTLINE(hicpp-special-member-functions)
TEST_P(DurabilityTest, SnapshotRetention) {
  // Create unrelated snapshot.
  {
    storage::Storage store({.items = {.properties_on_edges = GetParam()},
                            .durability = {.storage_directory = storage_directory, .snapshot_on_exit = true}});
    auto acc = store.Access();
    for (uint64_t i = 0; i < 1000; ++i) {
      acc.CreateVertex();
    }
    ASSERT_FALSE(acc.Commit().HasError());
  }

  ASSERT_GE(GetSnapshotsList().size(), 1);
  ASSERT_EQ(GetBackupSnapshotsList().size(), 0);
  ASSERT_EQ(GetWalsList().size(), 0);
  ASSERT_EQ(GetBackupWalsList().size(), 0);

  // Create snapshot.
  {
    storage::Storage store(
        {.items = {.properties_on_edges = GetParam()},
         .durability = {.storage_directory = storage_directory,
                        .snapshot_wal_mode = storage::Config::Durability::SnapshotWalMode::PERIODIC_SNAPSHOT,
                        .snapshot_interval = std::chrono::milliseconds(2000),
                        .snapshot_retention_count = 3}});
    // Restore unrelated snapshots after the database has been started.
    RestoreBackups();
    CreateBaseDataset(&store, GetParam());
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
      auto info = storage::durability::ReadSnapshotInfo(path);
      if (i == 0) uuid = info.uuid;
      if (i < snapshots.size() - 1) {
        ASSERT_EQ(info.uuid, uuid);
      } else {
        ASSERT_NE(info.uuid, uuid);
      }
    }
  }

  // Recover snapshot.
  storage::Storage store({.items = {.properties_on_edges = GetParam()},
                          .durability = {.storage_directory = storage_directory, .recover_on_startup = true}});
  VerifyDataset(&store, DatasetType::ONLY_BASE, GetParam());

  // Try to use the storage.
  {
    auto acc = store.Access();
    auto vertex = acc.CreateVertex();
    auto edge = acc.CreateEdge(&vertex, &vertex, store.NameToEdgeType("et"));
    ASSERT_TRUE(edge.HasValue());
    ASSERT_FALSE(acc.Commit().HasError());
  }
}

// NOLINTNEXTLINE(hicpp-special-member-functions)
TEST_P(DurabilityTest, SnapshotMixedUUID) {
  // Create snapshot.
  {
    storage::Storage store({.items = {.properties_on_edges = GetParam()},
                            .durability = {.storage_directory = storage_directory, .snapshot_on_exit = true}});
    CreateBaseDataset(&store, GetParam());
    VerifyDataset(&store, DatasetType::ONLY_BASE, GetParam());
    CreateExtendedDataset(&store);
    VerifyDataset(&store, DatasetType::BASE_WITH_EXTENDED, GetParam());
  }

  ASSERT_EQ(GetSnapshotsList().size(), 1);
  ASSERT_EQ(GetBackupSnapshotsList().size(), 0);
  ASSERT_EQ(GetWalsList().size(), 0);
  ASSERT_EQ(GetBackupWalsList().size(), 0);

  // Recover snapshot.
  {
    storage::Storage store({.items = {.properties_on_edges = GetParam()},
                            .durability = {.storage_directory = storage_directory, .recover_on_startup = true}});
    VerifyDataset(&store, DatasetType::BASE_WITH_EXTENDED, GetParam());
  }

  // Create another snapshot.
  {
    storage::Storage store({.items = {.properties_on_edges = GetParam()},
                            .durability = {.storage_directory = storage_directory, .snapshot_on_exit = true}});
    CreateBaseDataset(&store, GetParam());
    VerifyDataset(&store, DatasetType::ONLY_BASE, GetParam());
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
  storage::Storage store({.items = {.properties_on_edges = GetParam()},
                          .durability = {.storage_directory = storage_directory, .recover_on_startup = true}});
  VerifyDataset(&store, DatasetType::ONLY_BASE, GetParam());

  // Try to use the storage.
  {
    auto acc = store.Access();
    auto vertex = acc.CreateVertex();
    auto edge = acc.CreateEdge(&vertex, &vertex, store.NameToEdgeType("et"));
    ASSERT_TRUE(edge.HasValue());
    ASSERT_FALSE(acc.Commit().HasError());
  }
}

// NOLINTNEXTLINE(hicpp-special-member-functions)
TEST_P(DurabilityTest, SnapshotBackup) {
  // Create snapshot.
  {
    storage::Storage store({.items = {.properties_on_edges = GetParam()},
                            .durability = {.storage_directory = storage_directory, .snapshot_on_exit = true}});
    auto acc = store.Access();
    for (uint64_t i = 0; i < 1000; ++i) {
      acc.CreateVertex();
    }
    ASSERT_FALSE(acc.Commit().HasError());
  }

  ASSERT_EQ(GetSnapshotsList().size(), 1);
  ASSERT_EQ(GetBackupSnapshotsList().size(), 0);
  ASSERT_EQ(GetWalsList().size(), 0);
  ASSERT_EQ(GetBackupWalsList().size(), 0);

  // Start storage without recovery.
  {
    storage::Storage store(
        {.items = {.properties_on_edges = GetParam()},
         .durability = {.storage_directory = storage_directory,
                        .snapshot_wal_mode = storage::Config::Durability::SnapshotWalMode::PERIODIC_SNAPSHOT,
                        .snapshot_interval = std::chrono::minutes(20)}});
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
    storage::Storage store({.items = {.properties_on_edges = false},
                            .durability = {.storage_directory = storage_directory, .snapshot_on_exit = true}});
    CreateBaseDataset(&store, false);
    VerifyDataset(&store, DatasetType::ONLY_BASE, false);
    CreateExtendedDataset(&store);
    VerifyDataset(&store, DatasetType::BASE_WITH_EXTENDED, false);
  }

  ASSERT_EQ(GetSnapshotsList().size(), 1);
  ASSERT_EQ(GetBackupSnapshotsList().size(), 0);
  ASSERT_EQ(GetWalsList().size(), 0);
  ASSERT_EQ(GetBackupWalsList().size(), 0);

  // Recover snapshot.
  storage::Storage store({.items = {.properties_on_edges = true},
                          .durability = {.storage_directory = storage_directory, .recover_on_startup = true}});
  VerifyDataset(&store, DatasetType::BASE_WITH_EXTENDED, false);

  // Try to use the storage.
  {
    auto acc = store.Access();
    auto vertex = acc.CreateVertex();
    auto edge = acc.CreateEdge(&vertex, &vertex, store.NameToEdgeType("et"));
    ASSERT_TRUE(edge.HasValue());
    ASSERT_FALSE(acc.Commit().HasError());
  }
}

// NOLINTNEXTLINE(hicpp-special-member-functions)
TEST_F(DurabilityTest, SnapshotWithPropertiesOnEdgesRecoveryWithoutPropertiesOnEdges) {
  // Create snapshot.
  {
    storage::Storage store({.items = {.properties_on_edges = true},
                            .durability = {.storage_directory = storage_directory, .snapshot_on_exit = true}});
    CreateBaseDataset(&store, true);
    VerifyDataset(&store, DatasetType::ONLY_BASE, true);
    CreateExtendedDataset(&store);
    VerifyDataset(&store, DatasetType::BASE_WITH_EXTENDED, true);
  }

  ASSERT_EQ(GetSnapshotsList().size(), 1);
  ASSERT_EQ(GetBackupSnapshotsList().size(), 0);
  ASSERT_EQ(GetWalsList().size(), 0);
  ASSERT_EQ(GetBackupWalsList().size(), 0);

  // Recover snapshot.
  ASSERT_DEATH(
      {
        storage::Storage store({.items = {.properties_on_edges = false},
                                .durability = {.storage_directory = storage_directory, .recover_on_startup = true}});
      },
      "");
}

// NOLINTNEXTLINE(hicpp-special-member-functions)
TEST_F(DurabilityTest, SnapshotWithPropertiesOnEdgesButUnusedRecoveryWithoutPropertiesOnEdges) {
  // Create snapshot.
  {
    storage::Storage store({.items = {.properties_on_edges = true},
                            .durability = {.storage_directory = storage_directory, .snapshot_on_exit = true}});
    CreateBaseDataset(&store, true);
    VerifyDataset(&store, DatasetType::ONLY_BASE, true);
    CreateExtendedDataset(&store);
    VerifyDataset(&store, DatasetType::BASE_WITH_EXTENDED, true);
    // Remove properties from edges.
    {
      auto acc = store.Access();
      for (auto vertex : acc.Vertices(storage::View::OLD)) {
        auto in_edges = vertex.InEdges(storage::View::OLD);
        ASSERT_TRUE(in_edges.HasValue());
        for (auto edge : *in_edges) {
          // TODO (mferencevic): Replace with `ClearProperties()`
          auto props = edge.Properties(storage::View::NEW);
          ASSERT_TRUE(props.HasValue());
          for (const auto &prop : *props) {
            ASSERT_TRUE(edge.SetProperty(prop.first, storage::PropertyValue()).HasValue());
          }
        }
        auto out_edges = vertex.InEdges(storage::View::OLD);
        ASSERT_TRUE(out_edges.HasValue());
        for (auto edge : *out_edges) {
          // TODO (mferencevic): Replace with `ClearProperties()`
          auto props = edge.Properties(storage::View::NEW);
          ASSERT_TRUE(props.HasValue());
          for (const auto &prop : *props) {
            ASSERT_TRUE(edge.SetProperty(prop.first, storage::PropertyValue()).HasValue());
          }
        }
      }
      ASSERT_FALSE(acc.Commit().HasError());
    }
  }

  ASSERT_EQ(GetSnapshotsList().size(), 1);
  ASSERT_EQ(GetBackupSnapshotsList().size(), 0);
  ASSERT_EQ(GetWalsList().size(), 0);
  ASSERT_EQ(GetBackupWalsList().size(), 0);

  // Recover snapshot.
  storage::Storage store({.items = {.properties_on_edges = false},
                          .durability = {.storage_directory = storage_directory, .recover_on_startup = true}});
  VerifyDataset(&store, DatasetType::BASE_WITH_EXTENDED, false);

  // Try to use the storage.
  {
    auto acc = store.Access();
    auto vertex = acc.CreateVertex();
    auto edge = acc.CreateEdge(&vertex, &vertex, store.NameToEdgeType("et"));
    ASSERT_TRUE(edge.HasValue());
    ASSERT_FALSE(acc.Commit().HasError());
  }
}

// NOLINTNEXTLINE(hicpp-special-member-functions)
TEST_P(DurabilityTest, WalBasic) {
  // Create WALs.
  {
    storage::Storage store(
        {.items = {.properties_on_edges = GetParam()},
         .durability = {.storage_directory = storage_directory,
                        .snapshot_wal_mode = storage::Config::Durability::SnapshotWalMode::PERIODIC_SNAPSHOT_WITH_WAL,
                        .snapshot_interval = std::chrono::minutes(20),
                        .wal_file_flush_every_n_tx = kFlushWalEvery}});
    CreateBaseDataset(&store, GetParam());
    CreateExtendedDataset(&store);
  }

  ASSERT_EQ(GetSnapshotsList().size(), 0);
  ASSERT_EQ(GetBackupSnapshotsList().size(), 0);
  ASSERT_GE(GetWalsList().size(), 1);
  ASSERT_EQ(GetBackupWalsList().size(), 0);

  // Recover WALs.
  storage::Storage store({.items = {.properties_on_edges = GetParam()},
                          .durability = {.storage_directory = storage_directory, .recover_on_startup = true}});
  VerifyDataset(&store, DatasetType::BASE_WITH_EXTENDED, GetParam());

  // Try to use the storage.
  {
    auto acc = store.Access();
    auto vertex = acc.CreateVertex();
    auto edge = acc.CreateEdge(&vertex, &vertex, store.NameToEdgeType("et"));
    ASSERT_TRUE(edge.HasValue());
    ASSERT_FALSE(acc.Commit().HasError());
  }
}

// NOLINTNEXTLINE(hicpp-special-member-functions)
TEST_P(DurabilityTest, WalBackup) {
  // Create WALs.
  {
    storage::Storage store(
        {.items = {.properties_on_edges = GetParam()},
         .durability = {.storage_directory = storage_directory,
                        .snapshot_wal_mode = storage::Config::Durability::SnapshotWalMode::PERIODIC_SNAPSHOT_WITH_WAL,
                        .snapshot_interval = std::chrono::minutes(20),
                        .wal_file_size_kibibytes = 1,
                        .wal_file_flush_every_n_tx = kFlushWalEvery}});
    auto acc = store.Access();
    for (uint64_t i = 0; i < 1000; ++i) {
      acc.CreateVertex();
    }
    ASSERT_FALSE(acc.Commit().HasError());
  }

  ASSERT_EQ(GetSnapshotsList().size(), 0);
  ASSERT_EQ(GetBackupSnapshotsList().size(), 0);
  auto num_wals = GetWalsList().size();
  ASSERT_GE(num_wals, 1);
  ASSERT_EQ(GetBackupWalsList().size(), 0);

  // Start storage without recovery.
  {
    storage::Storage store(
        {.items = {.properties_on_edges = GetParam()},
         .durability = {.storage_directory = storage_directory,
                        .snapshot_wal_mode = storage::Config::Durability::SnapshotWalMode::PERIODIC_SNAPSHOT_WITH_WAL,
                        .snapshot_interval = std::chrono::minutes(20)}});
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
    storage::Storage store(
        {.items = {.properties_on_edges = GetParam()},
         .durability = {.storage_directory = storage_directory,
                        .snapshot_wal_mode = storage::Config::Durability::SnapshotWalMode::PERIODIC_SNAPSHOT_WITH_WAL,
                        .snapshot_interval = std::chrono::minutes(20),
                        .wal_file_flush_every_n_tx = kFlushWalEvery}});
    CreateBaseDataset(&store, GetParam());
  }

  ASSERT_EQ(GetSnapshotsList().size(), 0);
  ASSERT_EQ(GetBackupSnapshotsList().size(), 0);
  ASSERT_GE(GetWalsList().size(), 1);
  ASSERT_EQ(GetBackupWalsList().size(), 0);

  // Recover WALs.
  {
    storage::Storage store({.items = {.properties_on_edges = GetParam()},
                            .durability = {.storage_directory = storage_directory, .recover_on_startup = true}});
    VerifyDataset(&store, DatasetType::ONLY_BASE, GetParam());
  }

  // Recover WALs and create more WALs.
  {
    storage::Storage store(
        {.items = {.properties_on_edges = GetParam()},
         .durability = {.storage_directory = storage_directory,
                        .recover_on_startup = true,
                        .snapshot_wal_mode = storage::Config::Durability::SnapshotWalMode::PERIODIC_SNAPSHOT_WITH_WAL,
                        .snapshot_interval = std::chrono::minutes(20),
                        .wal_file_flush_every_n_tx = kFlushWalEvery}});
    CreateExtendedDataset(&store);
  }

  ASSERT_EQ(GetSnapshotsList().size(), 0);
  ASSERT_EQ(GetBackupSnapshotsList().size(), 0);
  ASSERT_GE(GetWalsList().size(), 2);
  ASSERT_EQ(GetBackupWalsList().size(), 0);

  // Recover WALs.
  storage::Storage store({.items = {.properties_on_edges = GetParam()},
                          .durability = {.storage_directory = storage_directory, .recover_on_startup = true}});
  VerifyDataset(&store, DatasetType::BASE_WITH_EXTENDED, GetParam());

  // Try to use the storage.
  {
    auto acc = store.Access();
    auto vertex = acc.CreateVertex();
    auto edge = acc.CreateEdge(&vertex, &vertex, store.NameToEdgeType("et"));
    ASSERT_TRUE(edge.HasValue());
    ASSERT_FALSE(acc.Commit().HasError());
  }
}

// NOLINTNEXTLINE(hicpp-special-member-functions)
TEST_P(DurabilityTest, WalCreateInSingleTransaction) {
  // NOLINTNEXTLINE(readability-isolate-declaration)
  storage::Gid gid_v1, gid_v2, gid_e1, gid_v3;

  // Create WALs.
  {
    storage::Storage store(
        {.items = {.properties_on_edges = GetParam()},
         .durability = {.storage_directory = storage_directory,
                        .snapshot_wal_mode = storage::Config::Durability::SnapshotWalMode::PERIODIC_SNAPSHOT_WITH_WAL,
                        .snapshot_interval = std::chrono::minutes(20),
                        .wal_file_flush_every_n_tx = kFlushWalEvery}});
    auto acc = store.Access();
    auto v1 = acc.CreateVertex();
    gid_v1 = v1.Gid();
    auto v2 = acc.CreateVertex();
    gid_v2 = v2.Gid();
    auto e1 = acc.CreateEdge(&v1, &v2, store.NameToEdgeType("e1"));
    ASSERT_TRUE(e1.HasValue());
    gid_e1 = e1->Gid();
    ASSERT_TRUE(v1.AddLabel(store.NameToLabel("l11")).HasValue());
    ASSERT_TRUE(v1.AddLabel(store.NameToLabel("l12")).HasValue());
    ASSERT_TRUE(v1.AddLabel(store.NameToLabel("l13")).HasValue());
    if (GetParam()) {
      ASSERT_TRUE(e1->SetProperty(store.NameToProperty("test"), storage::PropertyValue("nandare")).HasValue());
    }
    ASSERT_TRUE(v2.AddLabel(store.NameToLabel("l21")).HasValue());
    ASSERT_TRUE(v2.SetProperty(store.NameToProperty("hello"), storage::PropertyValue("world")).HasValue());
    auto v3 = acc.CreateVertex();
    gid_v3 = v3.Gid();
    ASSERT_TRUE(v3.SetProperty(store.NameToProperty("v3"), storage::PropertyValue(42)).HasValue());
    ASSERT_FALSE(acc.Commit().HasError());
  }

  ASSERT_EQ(GetSnapshotsList().size(), 0);
  ASSERT_EQ(GetBackupSnapshotsList().size(), 0);
  ASSERT_GE(GetWalsList().size(), 1);
  ASSERT_EQ(GetBackupWalsList().size(), 0);

  // Recover WALs.
  storage::Storage store({.items = {.properties_on_edges = GetParam()},
                          .durability = {.storage_directory = storage_directory, .recover_on_startup = true}});
  {
    auto indices = store.ListAllIndices();
    ASSERT_EQ(indices.label.size(), 0);
    ASSERT_EQ(indices.label_property.size(), 0);
    auto constraints = store.ListAllConstraints();
    ASSERT_EQ(constraints.existence.size(), 0);
    ASSERT_EQ(constraints.unique.size(), 0);
    auto acc = store.Access();
    {
      auto v1 = acc.FindVertex(gid_v1, storage::View::OLD);
      ASSERT_TRUE(v1);
      auto labels = v1->Labels(storage::View::OLD);
      ASSERT_TRUE(labels.HasValue());
      ASSERT_THAT(*labels,
                  UnorderedElementsAre(store.NameToLabel("l11"), store.NameToLabel("l12"), store.NameToLabel("l13")));
      auto props = v1->Properties(storage::View::OLD);
      ASSERT_TRUE(props.HasValue());
      ASSERT_EQ(props->size(), 0);
      auto in_edges = v1->InEdges(storage::View::OLD);
      ASSERT_TRUE(in_edges.HasValue());
      ASSERT_EQ(in_edges->size(), 0);
      auto out_edges = v1->OutEdges(storage::View::OLD);
      ASSERT_TRUE(out_edges.HasValue());
      ASSERT_EQ(out_edges->size(), 1);
      const auto &edge = (*out_edges)[0];
      ASSERT_EQ(edge.Gid(), gid_e1);
      auto edge_props = edge.Properties(storage::View::OLD);
      ASSERT_TRUE(edge_props.HasValue());
      if (GetParam()) {
        ASSERT_THAT(*edge_props, UnorderedElementsAre(
                                     std::make_pair(store.NameToProperty("test"), storage::PropertyValue("nandare"))));
      } else {
        ASSERT_EQ(edge_props->size(), 0);
      }
    }
    {
      auto v2 = acc.FindVertex(gid_v2, storage::View::OLD);
      ASSERT_TRUE(v2);
      auto labels = v2->Labels(storage::View::OLD);
      ASSERT_TRUE(labels.HasValue());
      ASSERT_THAT(*labels, UnorderedElementsAre(store.NameToLabel("l21")));
      auto props = v2->Properties(storage::View::OLD);
      ASSERT_TRUE(props.HasValue());
      ASSERT_THAT(*props,
                  UnorderedElementsAre(std::make_pair(store.NameToProperty("hello"), storage::PropertyValue("world"))));
      auto in_edges = v2->InEdges(storage::View::OLD);
      ASSERT_TRUE(in_edges.HasValue());
      ASSERT_EQ(in_edges->size(), 1);
      const auto &edge = (*in_edges)[0];
      ASSERT_EQ(edge.Gid(), gid_e1);
      auto edge_props = edge.Properties(storage::View::OLD);
      ASSERT_TRUE(edge_props.HasValue());
      if (GetParam()) {
        ASSERT_THAT(*edge_props, UnorderedElementsAre(
                                     std::make_pair(store.NameToProperty("test"), storage::PropertyValue("nandare"))));
      } else {
        ASSERT_EQ(edge_props->size(), 0);
      }
      auto out_edges = v2->OutEdges(storage::View::OLD);
      ASSERT_TRUE(out_edges.HasValue());
      ASSERT_EQ(out_edges->size(), 0);
    }
    {
      auto v3 = acc.FindVertex(gid_v3, storage::View::OLD);
      ASSERT_TRUE(v3);
      auto labels = v3->Labels(storage::View::OLD);
      ASSERT_TRUE(labels.HasValue());
      ASSERT_EQ(labels->size(), 0);
      auto props = v3->Properties(storage::View::OLD);
      ASSERT_TRUE(props.HasValue());
      ASSERT_THAT(*props, UnorderedElementsAre(std::make_pair(store.NameToProperty("v3"), storage::PropertyValue(42))));
      auto in_edges = v3->InEdges(storage::View::OLD);
      ASSERT_TRUE(in_edges.HasValue());
      ASSERT_EQ(in_edges->size(), 0);
      auto out_edges = v3->OutEdges(storage::View::OLD);
      ASSERT_TRUE(out_edges.HasValue());
      ASSERT_EQ(out_edges->size(), 0);
    }
  }

  // Try to use the storage.
  {
    auto acc = store.Access();
    auto vertex = acc.CreateVertex();
    auto edge = acc.CreateEdge(&vertex, &vertex, store.NameToEdgeType("et"));
    ASSERT_TRUE(edge.HasValue());
    ASSERT_FALSE(acc.Commit().HasError());
  }
}

// NOLINTNEXTLINE(hicpp-special-member-functions)
TEST_P(DurabilityTest, WalCreateAndRemoveEverything) {
  // Create WALs.
  {
    storage::Storage store(
        {.items = {.properties_on_edges = GetParam()},
         .durability = {.storage_directory = storage_directory,
                        .snapshot_wal_mode = storage::Config::Durability::SnapshotWalMode::PERIODIC_SNAPSHOT_WITH_WAL,
                        .snapshot_interval = std::chrono::minutes(20),
                        .wal_file_flush_every_n_tx = kFlushWalEvery}});
    CreateBaseDataset(&store, GetParam());
    CreateExtendedDataset(&store);
    auto indices = store.ListAllIndices();
    for (const auto &index : indices.label) {
      ASSERT_TRUE(store.DropIndex(index));
    }
    for (const auto &index : indices.label_property) {
      ASSERT_TRUE(store.DropIndex(index.first, index.second));
    }
    auto constraints = store.ListAllConstraints();
    for (const auto &constraint : constraints.existence) {
      ASSERT_TRUE(store.DropExistenceConstraint(constraint.first, constraint.second));
    }
    for (const auto &constraint : constraints.unique) {
      ASSERT_EQ(store.DropUniqueConstraint(constraint.first, constraint.second),
                storage::UniqueConstraints::DeletionStatus::SUCCESS);
    }
    auto acc = store.Access();
    for (auto vertex : acc.Vertices(storage::View::OLD)) {
      ASSERT_TRUE(acc.DetachDeleteVertex(&vertex).HasValue());
    }
    ASSERT_FALSE(acc.Commit().HasError());
  }

  ASSERT_EQ(GetSnapshotsList().size(), 0);
  ASSERT_EQ(GetBackupSnapshotsList().size(), 0);
  ASSERT_GE(GetWalsList().size(), 1);
  ASSERT_EQ(GetBackupWalsList().size(), 0);

  // Recover WALs.
  storage::Storage store({.items = {.properties_on_edges = GetParam()},
                          .durability = {.storage_directory = storage_directory, .recover_on_startup = true}});
  {
    auto indices = store.ListAllIndices();
    ASSERT_EQ(indices.label.size(), 0);
    ASSERT_EQ(indices.label_property.size(), 0);
    auto constraints = store.ListAllConstraints();
    ASSERT_EQ(constraints.existence.size(), 0);
    ASSERT_EQ(constraints.unique.size(), 0);
    auto acc = store.Access();
    uint64_t count = 0;
    auto iterable = acc.Vertices(storage::View::OLD);
    for (auto it = iterable.begin(); it != iterable.end(); ++it) {
      ++count;
    }
    ASSERT_EQ(count, 0);
  }

  // Try to use the storage.
  {
    auto acc = store.Access();
    auto vertex = acc.CreateVertex();
    auto edge = acc.CreateEdge(&vertex, &vertex, store.NameToEdgeType("et"));
    ASSERT_TRUE(edge.HasValue());
    ASSERT_FALSE(acc.Commit().HasError());
  }
}

// NOLINTNEXTLINE(hicpp-special-member-functions)
TEST_P(DurabilityTest, WalTransactionOrdering) {
  // NOLINTNEXTLINE(readability-isolate-declaration)
  storage::Gid gid1, gid2, gid3;

  // Create WAL.
  {
    storage::Storage store(
        {.items = {.properties_on_edges = GetParam()},
         .durability = {
             .storage_directory = storage_directory,
             .snapshot_wal_mode = storage::Config::Durability::SnapshotWalMode::PERIODIC_SNAPSHOT_WITH_WAL,
             .snapshot_interval = std::chrono::minutes(20),
             .wal_file_size_kibibytes = 100000,
             .wal_file_flush_every_n_tx = kFlushWalEvery,
         }});
    auto acc1 = store.Access();
    auto acc2 = store.Access();

    // Create vertex in transaction 2.
    {
      auto vertex2 = acc2.CreateVertex();
      gid2 = vertex2.Gid();
      ASSERT_TRUE(vertex2.SetProperty(store.NameToProperty("id"), storage::PropertyValue(2)).HasValue());
    }

    auto acc3 = store.Access();

    // Create vertex in transaction 3.
    {
      auto vertex3 = acc3.CreateVertex();
      gid3 = vertex3.Gid();
      ASSERT_TRUE(vertex3.SetProperty(store.NameToProperty("id"), storage::PropertyValue(3)).HasValue());
    }

    // Create vertex in transaction 1.
    {
      auto vertex1 = acc1.CreateVertex();
      gid1 = vertex1.Gid();
      ASSERT_TRUE(vertex1.SetProperty(store.NameToProperty("id"), storage::PropertyValue(1)).HasValue());
    }

    // Commit transaction 3, then 1, then 2.
    ASSERT_FALSE(acc3.Commit().HasError());
    ASSERT_FALSE(acc1.Commit().HasError());
    ASSERT_FALSE(acc2.Commit().HasError());
  }

  ASSERT_EQ(GetSnapshotsList().size(), 0);
  ASSERT_EQ(GetBackupSnapshotsList().size(), 0);
  ASSERT_EQ(GetWalsList().size(), 1);
  ASSERT_EQ(GetBackupWalsList().size(), 0);

  // Verify WAL data.
  {
    auto path = GetWalsList().front();
    auto info = storage::durability::ReadWalInfo(path);
    storage::durability::Decoder wal;
    wal.Initialize(path, storage::durability::kWalMagic);
    wal.SetPosition(info.offset_deltas);
    ASSERT_EQ(info.num_deltas, 9);
    std::vector<std::pair<uint64_t, storage::durability::WalDeltaData>> data;
    for (uint64_t i = 0; i < info.num_deltas; ++i) {
      auto timestamp = storage::durability::ReadWalDeltaHeader(&wal);
      data.emplace_back(timestamp, storage::durability::ReadWalDeltaData(&wal));
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
    ASSERT_EQ(data[0].second.type, storage::durability::WalDeltaData::Type::VERTEX_CREATE);
    ASSERT_EQ(data[0].second.vertex_create_delete.gid, gid3);
    ASSERT_EQ(data[1].second.type, storage::durability::WalDeltaData::Type::VERTEX_SET_PROPERTY);
    ASSERT_EQ(data[1].second.vertex_edge_set_property.gid, gid3);
    ASSERT_EQ(data[1].second.vertex_edge_set_property.property, "id");
    ASSERT_EQ(data[1].second.vertex_edge_set_property.value, storage::PropertyValue(3));
    ASSERT_EQ(data[2].second.type, storage::durability::WalDeltaData::Type::TRANSACTION_END);
    // Verify transaction 1.
    ASSERT_EQ(data[3].second.type, storage::durability::WalDeltaData::Type::VERTEX_CREATE);
    ASSERT_EQ(data[3].second.vertex_create_delete.gid, gid1);
    ASSERT_EQ(data[4].second.type, storage::durability::WalDeltaData::Type::VERTEX_SET_PROPERTY);
    ASSERT_EQ(data[4].second.vertex_edge_set_property.gid, gid1);
    ASSERT_EQ(data[4].second.vertex_edge_set_property.property, "id");
    ASSERT_EQ(data[4].second.vertex_edge_set_property.value, storage::PropertyValue(1));
    ASSERT_EQ(data[5].second.type, storage::durability::WalDeltaData::Type::TRANSACTION_END);
    // Verify transaction 2.
    ASSERT_EQ(data[6].second.type, storage::durability::WalDeltaData::Type::VERTEX_CREATE);
    ASSERT_EQ(data[6].second.vertex_create_delete.gid, gid2);
    ASSERT_EQ(data[7].second.type, storage::durability::WalDeltaData::Type::VERTEX_SET_PROPERTY);
    ASSERT_EQ(data[7].second.vertex_edge_set_property.gid, gid2);
    ASSERT_EQ(data[7].second.vertex_edge_set_property.property, "id");
    ASSERT_EQ(data[7].second.vertex_edge_set_property.value, storage::PropertyValue(2));
    ASSERT_EQ(data[8].second.type, storage::durability::WalDeltaData::Type::TRANSACTION_END);
  }

  // Recover WALs.
  storage::Storage store({.items = {.properties_on_edges = GetParam()},
                          .durability = {.storage_directory = storage_directory, .recover_on_startup = true}});
  {
    auto acc = store.Access();
    for (auto [gid, id] : std::vector<std::pair<storage::Gid, int64_t>>{{gid1, 1}, {gid2, 2}, {gid3, 3}}) {
      auto vertex = acc.FindVertex(gid, storage::View::OLD);
      ASSERT_TRUE(vertex);
      auto labels = vertex->Labels(storage::View::OLD);
      ASSERT_TRUE(labels.HasValue());
      ASSERT_EQ(labels->size(), 0);
      auto props = vertex->Properties(storage::View::OLD);
      ASSERT_TRUE(props.HasValue());
      ASSERT_EQ(props->size(), 1);
      ASSERT_EQ(props->at(store.NameToProperty("id")), storage::PropertyValue(id));
    }
  }

  // Try to use the storage.
  {
    auto acc = store.Access();
    auto vertex = acc.CreateVertex();
    auto edge = acc.CreateEdge(&vertex, &vertex, store.NameToEdgeType("et"));
    ASSERT_TRUE(edge.HasValue());
    ASSERT_FALSE(acc.Commit().HasError());
  }
}

// NOLINTNEXTLINE(hicpp-special-member-functions)
TEST_P(DurabilityTest, WalCreateAndRemoveOnlyBaseDataset) {
  // Create WALs.
  {
    storage::Storage store(
        {.items = {.properties_on_edges = GetParam()},
         .durability = {.storage_directory = storage_directory,
                        .snapshot_wal_mode = storage::Config::Durability::SnapshotWalMode::PERIODIC_SNAPSHOT_WITH_WAL,
                        .snapshot_interval = std::chrono::minutes(20),
                        .wal_file_flush_every_n_tx = kFlushWalEvery}});
    CreateBaseDataset(&store, GetParam());
    CreateExtendedDataset(&store);
    auto label_indexed = store.NameToLabel("base_indexed");
    auto label_unindexed = store.NameToLabel("base_unindexed");
    auto acc = store.Access();
    for (auto vertex : acc.Vertices(storage::View::OLD)) {
      auto has_indexed = vertex.HasLabel(label_indexed, storage::View::OLD);
      ASSERT_TRUE(has_indexed.HasValue());
      auto has_unindexed = vertex.HasLabel(label_unindexed, storage::View::OLD);
      if (!*has_indexed && !*has_unindexed) continue;
      ASSERT_TRUE(acc.DetachDeleteVertex(&vertex).HasValue());
    }
    ASSERT_FALSE(acc.Commit().HasError());
  }

  ASSERT_EQ(GetSnapshotsList().size(), 0);
  ASSERT_EQ(GetBackupSnapshotsList().size(), 0);
  ASSERT_GE(GetWalsList().size(), 1);
  ASSERT_EQ(GetBackupWalsList().size(), 0);

  // Recover WALs.
  storage::Storage store({.items = {.properties_on_edges = GetParam()},
                          .durability = {.storage_directory = storage_directory, .recover_on_startup = true}});
  VerifyDataset(&store, DatasetType::ONLY_EXTENDED_WITH_BASE_INDICES_AND_CONSTRAINTS, GetParam());

  // Try to use the storage.
  {
    auto acc = store.Access();
    auto vertex = acc.CreateVertex();
    auto edge = acc.CreateEdge(&vertex, &vertex, store.NameToEdgeType("et"));
    ASSERT_TRUE(edge.HasValue());
    ASSERT_FALSE(acc.Commit().HasError());
  }
}

// NOLINTNEXTLINE(hicpp-special-member-functions)
TEST_P(DurabilityTest, WalDeathResilience) {
  pid_t pid = fork();
  if (pid == 0) {
    // Create WALs.
    {
      storage::Storage store(
          {.items = {.properties_on_edges = GetParam()},
           .durability = {.storage_directory = storage_directory,
                          .snapshot_wal_mode = storage::Config::Durability::SnapshotWalMode::PERIODIC_SNAPSHOT_WITH_WAL,
                          .snapshot_interval = std::chrono::minutes(20),
                          .wal_file_flush_every_n_tx = kFlushWalEvery}});
      // Create one million vertices.
      for (uint64_t i = 0; i < 1000000; ++i) {
        auto acc = store.Access();
        acc.CreateVertex();
        MG_ASSERT(!acc.Commit().HasError(), "Couldn't commit transaction!");
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
    storage::Storage store(
        {.items = {.properties_on_edges = GetParam()},
         .durability = {
             .storage_directory = storage_directory,
             .recover_on_startup = true,
             .snapshot_wal_mode = storage::Config::Durability::SnapshotWalMode::PERIODIC_SNAPSHOT_WITH_WAL,
             .snapshot_interval = std::chrono::minutes(20),
             .wal_file_flush_every_n_tx = kFlushWalEvery,
         }});
    {
      auto acc = store.Access();
      auto iterable = acc.Vertices(storage::View::OLD);
      for (auto it = iterable.begin(); it != iterable.end(); ++it) {
        ++count;
      }
      ASSERT_GT(count, 0);
    }

    {
      auto acc = store.Access();
      for (uint64_t i = 0; i < kExtraItems; ++i) {
        acc.CreateVertex();
      }
      ASSERT_FALSE(acc.Commit().HasError());
    }
  }

  ASSERT_EQ(GetSnapshotsList().size(), 0);
  ASSERT_EQ(GetBackupSnapshotsList().size(), 0);
  ASSERT_GE(GetWalsList().size(), 2);
  ASSERT_EQ(GetBackupWalsList().size(), 0);

  // Recover WALs.
  storage::Storage store({.items = {.properties_on_edges = GetParam()},
                          .durability = {.storage_directory = storage_directory, .recover_on_startup = true}});
  {
    uint64_t current = 0;
    auto acc = store.Access();
    auto iterable = acc.Vertices(storage::View::OLD);
    for (auto it = iterable.begin(); it != iterable.end(); ++it) {
      ++current;
    }
    ASSERT_EQ(count + kExtraItems, current);
  }

  // Try to use the storage.
  {
    auto acc = store.Access();
    auto vertex = acc.CreateVertex();
    auto edge = acc.CreateEdge(&vertex, &vertex, store.NameToEdgeType("et"));
    ASSERT_TRUE(edge.HasValue());
    ASSERT_FALSE(acc.Commit().HasError());
  }
}

// NOLINTNEXTLINE(hicpp-special-member-functions)
TEST_P(DurabilityTest, WalMissingSecond) {
  // Create unrelated WALs.
  {
    storage::Storage store(
        {.items = {.properties_on_edges = GetParam()},
         .durability = {.storage_directory = storage_directory,
                        .snapshot_wal_mode = storage::Config::Durability::SnapshotWalMode::PERIODIC_SNAPSHOT_WITH_WAL,
                        .snapshot_interval = std::chrono::minutes(20),
                        .wal_file_size_kibibytes = 1,
                        .wal_file_flush_every_n_tx = kFlushWalEvery}});
    auto acc = store.Access();
    for (uint64_t i = 0; i < 1000; ++i) {
      acc.CreateVertex();
    }
    ASSERT_FALSE(acc.Commit().HasError());
  }

  ASSERT_EQ(GetSnapshotsList().size(), 0);
  ASSERT_EQ(GetBackupSnapshotsList().size(), 0);
  ASSERT_GE(GetWalsList().size(), 1);
  ASSERT_EQ(GetBackupWalsList().size(), 0);

  uint64_t unrelated_wals = GetWalsList().size();

  // Create WALs.
  {
    storage::Storage store(
        {.items = {.properties_on_edges = GetParam()},
         .durability = {.storage_directory = storage_directory,
                        .snapshot_wal_mode = storage::Config::Durability::SnapshotWalMode::PERIODIC_SNAPSHOT_WITH_WAL,
                        .snapshot_interval = std::chrono::minutes(20),
                        .wal_file_size_kibibytes = 1,
                        .wal_file_flush_every_n_tx = kFlushWalEvery}});
    const uint64_t kNumVertices = 1000;
    std::vector<storage::Gid> gids;
    gids.reserve(kNumVertices);
    for (uint64_t i = 0; i < kNumVertices; ++i) {
      auto acc = store.Access();
      auto vertex = acc.CreateVertex();
      gids.push_back(vertex.Gid());
      ASSERT_FALSE(acc.Commit().HasError());
    }
    for (uint64_t i = 0; i < kNumVertices; ++i) {
      auto acc = store.Access();
      auto vertex = acc.FindVertex(gids[i], storage::View::OLD);
      ASSERT_TRUE(vertex);
      ASSERT_TRUE(
          vertex->SetProperty(store.NameToProperty("nandare"), storage::PropertyValue("haihaihai!")).HasValue());
      ASSERT_FALSE(acc.Commit().HasError());
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
  ASSERT_DEATH(
      {
        storage::Storage store({.items = {.properties_on_edges = GetParam()},
                                .durability = {.storage_directory = storage_directory, .recover_on_startup = true}});
      },
      "");
}

// NOLINTNEXTLINE(hicpp-special-member-functions)
TEST_P(DurabilityTest, WalCorruptSecond) {
  // Create unrelated WALs.
  {
    storage::Storage store(
        {.items = {.properties_on_edges = GetParam()},
         .durability = {.storage_directory = storage_directory,
                        .snapshot_wal_mode = storage::Config::Durability::SnapshotWalMode::PERIODIC_SNAPSHOT_WITH_WAL,
                        .snapshot_interval = std::chrono::minutes(20),
                        .wal_file_size_kibibytes = 1,
                        .wal_file_flush_every_n_tx = kFlushWalEvery}});
    auto acc = store.Access();
    for (uint64_t i = 0; i < 1000; ++i) {
      acc.CreateVertex();
    }
    ASSERT_FALSE(acc.Commit().HasError());
  }

  ASSERT_EQ(GetSnapshotsList().size(), 0);
  ASSERT_EQ(GetBackupSnapshotsList().size(), 0);
  ASSERT_GE(GetWalsList().size(), 1);
  ASSERT_EQ(GetBackupWalsList().size(), 0);

  uint64_t unrelated_wals = GetWalsList().size();

  // Create WALs.
  {
    storage::Storage store(
        {.items = {.properties_on_edges = GetParam()},
         .durability = {.storage_directory = storage_directory,
                        .snapshot_wal_mode = storage::Config::Durability::SnapshotWalMode::PERIODIC_SNAPSHOT_WITH_WAL,
                        .snapshot_interval = std::chrono::minutes(20),
                        .wal_file_size_kibibytes = 1,
                        .wal_file_flush_every_n_tx = kFlushWalEvery}});
    const uint64_t kNumVertices = 1000;
    std::vector<storage::Gid> gids;
    gids.reserve(kNumVertices);
    for (uint64_t i = 0; i < kNumVertices; ++i) {
      auto acc = store.Access();
      auto vertex = acc.CreateVertex();
      gids.push_back(vertex.Gid());
      ASSERT_FALSE(acc.Commit().HasError());
    }
    for (uint64_t i = 0; i < kNumVertices; ++i) {
      auto acc = store.Access();
      auto vertex = acc.FindVertex(gids[i], storage::View::OLD);
      ASSERT_TRUE(vertex);
      ASSERT_TRUE(
          vertex->SetProperty(store.NameToProperty("nandare"), storage::PropertyValue("haihaihai!")).HasValue());
      ASSERT_FALSE(acc.Commit().HasError());
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
  ASSERT_DEATH(
      {
        storage::Storage store({.items = {.properties_on_edges = GetParam()},
                                .durability = {.storage_directory = storage_directory, .recover_on_startup = true}});
      },
      "");
}

// NOLINTNEXTLINE(hicpp-special-member-functions)
TEST_P(DurabilityTest, WalCorruptLastTransaction) {
  // Create WALs
  {
    storage::Storage store(
        {.items = {.properties_on_edges = GetParam()},
         .durability = {.storage_directory = storage_directory,
                        .snapshot_wal_mode = storage::Config::Durability::SnapshotWalMode::PERIODIC_SNAPSHOT_WITH_WAL,
                        .snapshot_interval = std::chrono::minutes(20),
                        .wal_file_size_kibibytes = 1,
                        .wal_file_flush_every_n_tx = kFlushWalEvery}});
    CreateBaseDataset(&store, GetParam());
    CreateExtendedDataset(&store, /* single_transaction = */ true);
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
  storage::Storage store({.items = {.properties_on_edges = GetParam()},
                          .durability = {.storage_directory = storage_directory, .recover_on_startup = true}});
  // The extended dataset shouldn't be recovered because its WAL transaction was
  // corrupt.
  VerifyDataset(&store, DatasetType::ONLY_BASE_WITH_EXTENDED_INDICES_AND_CONSTRAINTS, GetParam());

  // Try to use the storage.
  {
    auto acc = store.Access();
    auto vertex = acc.CreateVertex();
    auto edge = acc.CreateEdge(&vertex, &vertex, store.NameToEdgeType("et"));
    ASSERT_TRUE(edge.HasValue());
    ASSERT_FALSE(acc.Commit().HasError());
  }
}

// NOLINTNEXTLINE(hicpp-special-member-functions)
TEST_P(DurabilityTest, WalAllOperationsInSingleTransaction) {
  // Create WALs
  {
    storage::Storage store(
        {.items = {.properties_on_edges = GetParam()},
         .durability = {.storage_directory = storage_directory,
                        .snapshot_wal_mode = storage::Config::Durability::SnapshotWalMode::PERIODIC_SNAPSHOT_WITH_WAL,
                        .snapshot_interval = std::chrono::minutes(20),
                        .wal_file_size_kibibytes = 1,
                        .wal_file_flush_every_n_tx = kFlushWalEvery}});
    auto acc = store.Access();
    auto vertex1 = acc.CreateVertex();
    auto vertex2 = acc.CreateVertex();
    ASSERT_TRUE(vertex1.AddLabel(acc.NameToLabel("nandare")).HasValue());
    ASSERT_TRUE(vertex2.SetProperty(acc.NameToProperty("haihai"), storage::PropertyValue(42)).HasValue());
    ASSERT_TRUE(vertex1.RemoveLabel(acc.NameToLabel("nandare")).HasValue());
    auto edge1 = acc.CreateEdge(&vertex1, &vertex2, acc.NameToEdgeType("et1"));
    ASSERT_TRUE(edge1.HasValue());
    ASSERT_TRUE(vertex2.SetProperty(acc.NameToProperty("haihai"), storage::PropertyValue()).HasValue());
    auto vertex3 = acc.CreateVertex();
    auto edge2 = acc.CreateEdge(&vertex3, &vertex3, acc.NameToEdgeType("et2"));
    ASSERT_TRUE(edge2.HasValue());
    if (GetParam()) {
      ASSERT_TRUE(edge2->SetProperty(acc.NameToProperty("meaning"), storage::PropertyValue(true)).HasValue());
      ASSERT_TRUE(edge1->SetProperty(acc.NameToProperty("hello"), storage::PropertyValue("world")).HasValue());
      ASSERT_TRUE(edge2->SetProperty(acc.NameToProperty("meaning"), storage::PropertyValue()).HasValue());
    }
    ASSERT_TRUE(vertex3.AddLabel(acc.NameToLabel("test")).HasValue());
    ASSERT_TRUE(vertex3.SetProperty(acc.NameToProperty("nonono"), storage::PropertyValue(-1)).HasValue());
    ASSERT_TRUE(vertex3.SetProperty(acc.NameToProperty("nonono"), storage::PropertyValue()).HasValue());
    if (GetParam()) {
      ASSERT_TRUE(edge1->SetProperty(acc.NameToProperty("hello"), storage::PropertyValue()).HasValue());
    }
    ASSERT_TRUE(vertex3.RemoveLabel(acc.NameToLabel("test")).HasValue());
    ASSERT_TRUE(acc.DetachDeleteVertex(&vertex1).HasValue());
    ASSERT_TRUE(acc.DeleteEdge(&*edge2).HasValue());
    ASSERT_TRUE(acc.DeleteVertex(&vertex2).HasValue());
    ASSERT_TRUE(acc.DeleteVertex(&vertex3).HasValue());
    ASSERT_FALSE(acc.Commit().HasError());
  }

  ASSERT_EQ(GetSnapshotsList().size(), 0);
  ASSERT_EQ(GetBackupSnapshotsList().size(), 0);
  ASSERT_GE(GetWalsList().size(), 1);
  ASSERT_EQ(GetBackupWalsList().size(), 0);

  // Recover WALs.
  storage::Storage store({.items = {.properties_on_edges = GetParam()},
                          .durability = {.storage_directory = storage_directory, .recover_on_startup = true}});
  {
    auto acc = store.Access();
    uint64_t count = 0;
    auto iterable = acc.Vertices(storage::View::OLD);
    for (auto it = iterable.begin(); it != iterable.end(); ++it) {
      ++count;
    }
    ASSERT_EQ(count, 0);
  }

  // Try to use the storage.
  {
    auto acc = store.Access();
    auto vertex = acc.CreateVertex();
    auto edge = acc.CreateEdge(&vertex, &vertex, store.NameToEdgeType("et"));
    ASSERT_TRUE(edge.HasValue());
    ASSERT_FALSE(acc.Commit().HasError());
  }
}

// NOLINTNEXTLINE(hicpp-special-member-functions)
TEST_P(DurabilityTest, WalAndSnapshot) {
  // Create snapshot and WALs.
  {
    storage::Storage store(
        {.items = {.properties_on_edges = GetParam()},
         .durability = {.storage_directory = storage_directory,
                        .snapshot_wal_mode = storage::Config::Durability::SnapshotWalMode::PERIODIC_SNAPSHOT_WITH_WAL,
                        .snapshot_interval = std::chrono::milliseconds(2000),
                        .wal_file_flush_every_n_tx = kFlushWalEvery}});
    CreateBaseDataset(&store, GetParam());
    std::this_thread::sleep_for(std::chrono::milliseconds(2500));
    CreateExtendedDataset(&store);
  }

  ASSERT_GE(GetSnapshotsList().size(), 1);
  ASSERT_EQ(GetBackupSnapshotsList().size(), 0);
  ASSERT_GE(GetWalsList().size(), 1);
  ASSERT_EQ(GetBackupWalsList().size(), 0);

  // Recover snapshot and WALs.
  storage::Storage store({.items = {.properties_on_edges = GetParam()},
                          .durability = {.storage_directory = storage_directory, .recover_on_startup = true}});
  VerifyDataset(&store, DatasetType::BASE_WITH_EXTENDED, GetParam());

  // Try to use the storage.
  {
    auto acc = store.Access();
    auto vertex = acc.CreateVertex();
    auto edge = acc.CreateEdge(&vertex, &vertex, store.NameToEdgeType("et"));
    ASSERT_TRUE(edge.HasValue());
    ASSERT_FALSE(acc.Commit().HasError());
  }
}

// NOLINTNEXTLINE(hicpp-special-member-functions)
TEST_P(DurabilityTest, WalAndSnapshotAppendToExistingSnapshot) {
  // Create snapshot.
  {
    storage::Storage store({.items = {.properties_on_edges = GetParam()},
                            .durability = {.storage_directory = storage_directory, .snapshot_on_exit = true}});
    CreateBaseDataset(&store, GetParam());
  }

  ASSERT_EQ(GetSnapshotsList().size(), 1);
  ASSERT_EQ(GetBackupSnapshotsList().size(), 0);
  ASSERT_EQ(GetWalsList().size(), 0);
  ASSERT_EQ(GetBackupWalsList().size(), 0);

  // Recover snapshot.
  {
    storage::Storage store({.items = {.properties_on_edges = GetParam()},
                            .durability = {.storage_directory = storage_directory, .recover_on_startup = true}});
    VerifyDataset(&store, DatasetType::ONLY_BASE, GetParam());
  }

  // Recover snapshot and create WALs.
  {
    storage::Storage store(
        {.items = {.properties_on_edges = GetParam()},
         .durability = {.storage_directory = storage_directory,
                        .recover_on_startup = true,
                        .snapshot_wal_mode = storage::Config::Durability::SnapshotWalMode::PERIODIC_SNAPSHOT_WITH_WAL,
                        .snapshot_interval = std::chrono::minutes(20),
                        .wal_file_flush_every_n_tx = kFlushWalEvery}});
    CreateExtendedDataset(&store);
  }

  ASSERT_EQ(GetSnapshotsList().size(), 1);
  ASSERT_EQ(GetBackupSnapshotsList().size(), 0);
  ASSERT_GE(GetWalsList().size(), 1);
  ASSERT_EQ(GetBackupWalsList().size(), 0);

  // Recover snapshot and WALs.
  storage::Storage store({.items = {.properties_on_edges = GetParam()},
                          .durability = {.storage_directory = storage_directory, .recover_on_startup = true}});
  VerifyDataset(&store, DatasetType::BASE_WITH_EXTENDED, GetParam());

  // Try to use the storage.
  {
    auto acc = store.Access();
    auto vertex = acc.CreateVertex();
    auto edge = acc.CreateEdge(&vertex, &vertex, store.NameToEdgeType("et"));
    ASSERT_TRUE(edge.HasValue());
    ASSERT_FALSE(acc.Commit().HasError());
  }
}

// NOLINTNEXTLINE(hicpp-special-member-functions)
TEST_P(DurabilityTest, WalAndSnapshotAppendToExistingSnapshotAndWal) {
  // Create snapshot.
  {
    storage::Storage store({.items = {.properties_on_edges = GetParam()},
                            .durability = {.storage_directory = storage_directory, .snapshot_on_exit = true}});
    CreateBaseDataset(&store, GetParam());
  }

  ASSERT_EQ(GetSnapshotsList().size(), 1);
  ASSERT_EQ(GetBackupSnapshotsList().size(), 0);
  ASSERT_EQ(GetWalsList().size(), 0);
  ASSERT_EQ(GetBackupWalsList().size(), 0);

  // Recover snapshot.
  {
    storage::Storage store({.items = {.properties_on_edges = GetParam()},
                            .durability = {.storage_directory = storage_directory, .recover_on_startup = true}});
    VerifyDataset(&store, DatasetType::ONLY_BASE, GetParam());
  }

  // Recover snapshot and create WALs.
  {
    storage::Storage store(
        {.items = {.properties_on_edges = GetParam()},
         .durability = {.storage_directory = storage_directory,
                        .recover_on_startup = true,
                        .snapshot_wal_mode = storage::Config::Durability::SnapshotWalMode::PERIODIC_SNAPSHOT_WITH_WAL,
                        .snapshot_interval = std::chrono::minutes(20),
                        .wal_file_flush_every_n_tx = kFlushWalEvery}});
    CreateExtendedDataset(&store);
  }

  ASSERT_EQ(GetSnapshotsList().size(), 1);
  ASSERT_EQ(GetBackupSnapshotsList().size(), 0);
  ASSERT_GE(GetWalsList().size(), 1);
  ASSERT_EQ(GetBackupWalsList().size(), 0);

  // Recover snapshot and WALs and create more WALs.
  storage::Gid vertex_gid;
  {
    storage::Storage store(
        {.items = {.properties_on_edges = GetParam()},
         .durability = {.storage_directory = storage_directory,
                        .recover_on_startup = true,
                        .snapshot_wal_mode = storage::Config::Durability::SnapshotWalMode::PERIODIC_SNAPSHOT_WITH_WAL,
                        .snapshot_interval = std::chrono::minutes(20),
                        .wal_file_flush_every_n_tx = kFlushWalEvery}});
    VerifyDataset(&store, DatasetType::BASE_WITH_EXTENDED, GetParam());
    auto acc = store.Access();
    auto vertex = acc.CreateVertex();
    vertex_gid = vertex.Gid();
    if (GetParam()) {
      ASSERT_TRUE(vertex.SetProperty(store.NameToProperty("meaning"), storage::PropertyValue(42)).HasValue());
    }
    ASSERT_FALSE(acc.Commit().HasError());
  }

  ASSERT_EQ(GetSnapshotsList().size(), 1);
  ASSERT_EQ(GetBackupSnapshotsList().size(), 0);
  ASSERT_GE(GetWalsList().size(), 2);
  ASSERT_EQ(GetBackupWalsList().size(), 0);

  // Recover snapshot and WALs.
  storage::Storage store({.items = {.properties_on_edges = GetParam()},
                          .durability = {.storage_directory = storage_directory, .recover_on_startup = true}});
  VerifyDataset(&store, DatasetType::BASE_WITH_EXTENDED, GetParam(),
                /* verify_info = */ false);
  {
    auto acc = store.Access();
    auto vertex = acc.FindVertex(vertex_gid, storage::View::OLD);
    ASSERT_TRUE(vertex);
    auto labels = vertex->Labels(storage::View::OLD);
    ASSERT_TRUE(labels.HasValue());
    ASSERT_EQ(labels->size(), 0);
    auto props = vertex->Properties(storage::View::OLD);
    ASSERT_TRUE(props.HasValue());
    if (GetParam()) {
      ASSERT_THAT(*props,
                  UnorderedElementsAre(std::make_pair(store.NameToProperty("meaning"), storage::PropertyValue(42))));
    } else {
      ASSERT_EQ(props->size(), 0);
    }
  }

  // Try to use the storage.
  {
    auto acc = store.Access();
    auto vertex = acc.CreateVertex();
    auto edge = acc.CreateEdge(&vertex, &vertex, store.NameToEdgeType("et"));
    ASSERT_TRUE(edge.HasValue());
    ASSERT_FALSE(acc.Commit().HasError());
  }
}

// NOLINTNEXTLINE(hicpp-special-member-functions)
TEST_P(DurabilityTest, WalAndSnapshotWalRetention) {
  // Create unrelated WALs.
  {
    storage::Storage store(
        {.items = {.properties_on_edges = GetParam()},
         .durability = {.storage_directory = storage_directory,
                        .snapshot_wal_mode = storage::Config::Durability::SnapshotWalMode::PERIODIC_SNAPSHOT_WITH_WAL,
                        .snapshot_interval = std::chrono::minutes(20),
                        .wal_file_size_kibibytes = 1,
                        .wal_file_flush_every_n_tx = kFlushWalEvery}});
    auto acc = store.Access();
    for (uint64_t i = 0; i < 1000; ++i) {
      acc.CreateVertex();
    }
    ASSERT_FALSE(acc.Commit().HasError());
  }

  ASSERT_EQ(GetSnapshotsList().size(), 0);
  ASSERT_EQ(GetBackupSnapshotsList().size(), 0);
  ASSERT_GE(GetWalsList().size(), 1);
  ASSERT_EQ(GetBackupWalsList().size(), 0);

  uint64_t unrelated_wals = GetWalsList().size();

  uint64_t items_created = 0;

  // Create snapshot and WALs.
  {
    storage::Storage store(
        {.items = {.properties_on_edges = GetParam()},
         .durability = {.storage_directory = storage_directory,
                        .snapshot_wal_mode = storage::Config::Durability::SnapshotWalMode::PERIODIC_SNAPSHOT_WITH_WAL,
                        .snapshot_interval = std::chrono::seconds(2),
                        .wal_file_size_kibibytes = 1,
                        .wal_file_flush_every_n_tx = 1}});
    // Restore unrelated snapshots after the database has been started.
    RestoreBackups();
    utils::Timer timer;
    // Allow at least 6 snapshots to be created.
    while (timer.Elapsed().count() < 13.0) {
      auto acc = store.Access();
      acc.CreateVertex();
      ASSERT_FALSE(acc.Commit().HasError());
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
      storage::Storage store({.items = {.properties_on_edges = GetParam()},
                              .durability = {.storage_directory = storage_directory, .recover_on_startup = true}});
      auto acc = store.Access();
      for (uint64_t j = 0; j < items_created; ++j) {
        auto vertex = acc.FindVertex(storage::Gid::FromUint(j), storage::View::OLD);
        ASSERT_TRUE(vertex);
      }
    }

    // Destroy current snapshot.
    DestroySnapshot(snapshots[i]);
  }

  // Recover data after all of the snapshots have been destroyed. The recovery
  // shouldn't be possible because the initial WALs are already deleted.
  ASSERT_DEATH(
      {
        storage::Storage store({.items = {.properties_on_edges = GetParam()},
                                .durability = {.storage_directory = storage_directory, .recover_on_startup = true}});
      },
      "");
}

// NOLINTNEXTLINE(hicpp-special-member-functions)
TEST_P(DurabilityTest, SnapshotAndWalMixedUUID) {
  // Create unrelated snapshot and WALs.
  {
    storage::Storage store(
        {.items = {.properties_on_edges = GetParam()},
         .durability = {.storage_directory = storage_directory,
                        .snapshot_wal_mode = storage::Config::Durability::SnapshotWalMode::PERIODIC_SNAPSHOT_WITH_WAL,
                        .snapshot_interval = std::chrono::seconds(2)}});
    auto acc = store.Access();
    for (uint64_t i = 0; i < 1000; ++i) {
      acc.CreateVertex();
    }
    ASSERT_FALSE(acc.Commit().HasError());
    std::this_thread::sleep_for(std::chrono::milliseconds(2500));
  }

  ASSERT_GE(GetSnapshotsList().size(), 1);
  ASSERT_EQ(GetBackupSnapshotsList().size(), 0);
  ASSERT_GE(GetWalsList().size(), 1);
  ASSERT_EQ(GetBackupWalsList().size(), 0);

  // Create snapshot and WALs.
  {
    storage::Storage store(
        {.items = {.properties_on_edges = GetParam()},
         .durability = {.storage_directory = storage_directory,
                        .snapshot_wal_mode = storage::Config::Durability::SnapshotWalMode::PERIODIC_SNAPSHOT_WITH_WAL,
                        .snapshot_interval = std::chrono::seconds(2)}});
    CreateBaseDataset(&store, GetParam());
    std::this_thread::sleep_for(std::chrono::milliseconds(2500));
    CreateExtendedDataset(&store);
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
  storage::Storage store({.items = {.properties_on_edges = GetParam()},
                          .durability = {.storage_directory = storage_directory, .recover_on_startup = true}});
  VerifyDataset(&store, DatasetType::BASE_WITH_EXTENDED, GetParam());

  // Try to use the storage.
  {
    auto acc = store.Access();
    auto vertex = acc.CreateVertex();
    auto edge = acc.CreateEdge(&vertex, &vertex, store.NameToEdgeType("et"));
    ASSERT_TRUE(edge.HasValue());
    ASSERT_FALSE(acc.Commit().HasError());
  }
}
