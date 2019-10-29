#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include <csignal>

#include <sys/types.h>
#include <sys/wait.h>
#include <unistd.h>

#include <algorithm>
#include <chrono>
#include <filesystem>
#include <iostream>
#include <thread>

#include "storage/v2/durability.hpp"
#include "storage/v2/storage.hpp"
#include "utils/file.hpp"
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
  const uint64_t kFlushWalEvery = (kNumBaseVertices + kNumBaseEdges +
                                   kNumExtendedVertices + kNumExtendedEdges) *
                                  2;

 public:
  DurabilityTest()
      : base_vertex_gids_(kNumBaseVertices),
        base_edge_gids_(kNumBaseEdges),
        extended_vertex_gids_(kNumExtendedVertices),
        extended_edge_gids_(kNumExtendedEdges) {}

  void SetUp() override { Clear(); }

  void TearDown() override { Clear(); }

  void CreateBaseDataset(storage::Storage *store, bool properties_on_edges) {
    auto label_indexed = store->NameToLabel("base_indexed");
    auto label_unindexed = store->NameToLabel("base_unindexed");
    auto property_id = store->NameToProperty("id");
    auto et1 = store->NameToEdgeType("base_et1");
    auto et2 = store->NameToEdgeType("base_et2");

    // Create label index.
    ASSERT_TRUE(store->CreateIndex(label_unindexed));

    // Create label+property index.
    ASSERT_TRUE(store->CreateIndex(label_indexed, property_id));

    // Create existence constraint.
    ASSERT_FALSE(store->CreateExistenceConstraint(label_unindexed, property_id)
                     .HasError());

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
        ASSERT_TRUE(vertex
                        .SetProperty(property_id, storage::PropertyValue(
                                                      static_cast<int64_t>(i)))
                        .HasValue());
      }
      ASSERT_FALSE(acc.Commit().HasError());
    }

    // Create edges.
    for (uint64_t i = 0; i < kNumBaseEdges; ++i) {
      auto acc = store->Access();
      auto vertex1 = acc.FindVertex(
          base_vertex_gids_[(i / 2) % kNumBaseVertices], storage::View::OLD);
      ASSERT_TRUE(vertex1);
      auto vertex2 = acc.FindVertex(
          base_vertex_gids_[(i / 3) % kNumBaseVertices], storage::View::OLD);
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
        ASSERT_TRUE(edge->SetProperty(property_id, storage::PropertyValue(
                                                       static_cast<int64_t>(i)))
                        .HasValue());
      } else {
        auto ret = edge->SetProperty(
            property_id, storage::PropertyValue(static_cast<int64_t>(i)));
        ASSERT_TRUE(ret.HasError());
        ASSERT_EQ(ret.GetError(), storage::Error::PROPERTIES_DISABLED);
      }
      ASSERT_FALSE(acc.Commit().HasError());
    }
  }

  void VerifyBaseDataset(storage::Storage *store, bool properties_on_edges,
                         bool extended_dataset_exists) {
    auto label_indexed = store->NameToLabel("base_indexed");
    auto label_unindexed = store->NameToLabel("base_unindexed");
    auto property_id = store->NameToProperty("id");
    auto et1 = store->NameToEdgeType("base_et1");
    auto et2 = store->NameToEdgeType("base_et2");

    // Verify indices info.
    {
      auto info = store->ListAllIndices();
      if (extended_dataset_exists) {
        ASSERT_THAT(info.label, Contains(label_unindexed));
        ASSERT_THAT(info.label_property,
                    Contains(std::make_pair(label_indexed, property_id)));
      } else {
        ASSERT_THAT(info.label, UnorderedElementsAre(label_unindexed));
        ASSERT_THAT(info.label_property, UnorderedElementsAre(std::make_pair(
                                             label_indexed, property_id)));
      }
    }

    // Verify constraints info.
    {
      auto info = store->ListAllConstraints();
      if (extended_dataset_exists) {
        ASSERT_THAT(info.existence,
                    Contains(std::make_pair(label_unindexed, property_id)));
      } else {
        ASSERT_THAT(info.existence, UnorderedElementsAre(std::make_pair(
                                        label_unindexed, property_id)));
      }
    }

    // Create storage accessor.
    auto acc = store->Access();

    // Verify vertices.
    for (uint64_t i = 0; i < kNumBaseVertices; ++i) {
      auto vertex = acc.FindVertex(base_vertex_gids_[i], storage::View::OLD);
      ASSERT_TRUE(vertex);
      auto labels = vertex->Labels(storage::View::OLD);
      ASSERT_TRUE(labels.HasValue());
      if (i < kNumBaseVertices / 2) {
        ASSERT_THAT(*labels, UnorderedElementsAre(label_indexed));
      } else {
        ASSERT_THAT(*labels, UnorderedElementsAre(label_unindexed));
      }
      auto properties = vertex->Properties(storage::View::OLD);
      ASSERT_TRUE(properties.HasValue());
      if (i < kNumBaseVertices / 3 || i >= kNumBaseVertices / 2) {
        ASSERT_EQ(properties->size(), 1);
        ASSERT_EQ((*properties)[property_id],
                  storage::PropertyValue(static_cast<int64_t>(i)));
      } else {
        ASSERT_EQ(properties->size(), 0);
      }
    }

    // Verify edges.
    for (uint64_t i = 0; i < kNumBaseEdges; ++i) {
      auto find_edge =
          [&](const auto &edges) -> std::optional<storage::EdgeAccessor> {
        for (const auto &edge : edges) {
          if (edge.Gid() == base_edge_gids_[i]) {
            return edge;
          }
        }
        return std::nullopt;
      };

      {
        auto vertex1 = acc.FindVertex(
            base_vertex_gids_[(i / 2) % kNumBaseVertices], storage::View::OLD);
        ASSERT_TRUE(vertex1);
        auto out_edges = vertex1->OutEdges({}, storage::View::OLD);
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
          ASSERT_EQ((*properties)[property_id],
                    storage::PropertyValue(static_cast<int64_t>(i)));
        } else {
          ASSERT_EQ(properties->size(), 0);
        }
      }

      {
        auto vertex2 = acc.FindVertex(
            base_vertex_gids_[(i / 3) % kNumBaseVertices], storage::View::OLD);
        ASSERT_TRUE(vertex2);
        auto in_edges = vertex2->InEdges({}, storage::View::OLD);
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
          ASSERT_EQ((*properties)[property_id],
                    storage::PropertyValue(static_cast<int64_t>(i)));
        } else {
          ASSERT_EQ(properties->size(), 0);
        }
      }
    }

    // Verify label indices.
    {
      std::vector<storage::VertexAccessor> vertices;
      vertices.reserve(kNumBaseVertices / 2);
      for (auto vertex : acc.Vertices(label_unindexed, storage::View::OLD)) {
        vertices.push_back(vertex);
      }
      ASSERT_EQ(vertices.size(), kNumBaseVertices / 2);
      std::sort(vertices.begin(), vertices.end(),
                [](const auto &a, const auto &b) { return a.Gid() < b.Gid(); });
      for (uint64_t i = 0; i < kNumBaseVertices / 2; ++i) {
        ASSERT_EQ(vertices[i].Gid(),
                  base_vertex_gids_[kNumBaseVertices / 2 + i]);
      }
    }

    // Verify label+property index.
    {
      std::vector<storage::VertexAccessor> vertices;
      vertices.reserve(kNumBaseVertices / 3);
      for (auto vertex :
           acc.Vertices(label_indexed, property_id, storage::View::OLD)) {
        vertices.push_back(vertex);
      }
      ASSERT_EQ(vertices.size(), kNumBaseVertices / 3);
      std::sort(vertices.begin(), vertices.end(),
                [](const auto &a, const auto &b) { return a.Gid() < b.Gid(); });
      for (uint64_t i = 0; i < kNumBaseVertices / 3; ++i) {
        ASSERT_EQ(vertices[i].Gid(), base_vertex_gids_[i]);
      }
    }
  }

  void CreateExtendedDataset(storage::Storage *store) {
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
    ASSERT_FALSE(store->CreateExistenceConstraint(label_unused, property_count)
                     .HasError());

    // Create vertices.
    for (uint64_t i = 0; i < kNumExtendedVertices; ++i) {
      auto acc = store->Access();
      auto vertex = acc.CreateVertex();
      extended_vertex_gids_[i] = vertex.Gid();
      if (i < kNumExtendedVertices / 2) {
        ASSERT_TRUE(vertex.AddLabel(label_indexed).HasValue());
      }
      if (i < kNumExtendedVertices / 3 || i >= kNumExtendedVertices / 2) {
        ASSERT_TRUE(
            vertex
                .SetProperty(property_count, storage::PropertyValue("nandare"))
                .HasValue());
      }
      ASSERT_FALSE(acc.Commit().HasError());
    }

    // Create edges.
    for (uint64_t i = 0; i < kNumExtendedEdges; ++i) {
      auto acc = store->Access();
      auto vertex1 =
          acc.FindVertex(extended_vertex_gids_[(i / 5) % kNumExtendedVertices],
                         storage::View::OLD);
      ASSERT_TRUE(vertex1);
      auto vertex2 =
          acc.FindVertex(extended_vertex_gids_[(i / 6) % kNumExtendedVertices],
                         storage::View::OLD);
      ASSERT_TRUE(vertex2);
      storage::EdgeTypeId et;
      if (i < kNumExtendedEdges / 4) {
        et = et3;
      } else {
        et = et4;
      }
      auto edge = acc.CreateEdge(&*vertex1, &*vertex2, et);
      ASSERT_TRUE(edge.HasValue());
      extended_edge_gids_[i] = edge->Gid();
      ASSERT_FALSE(acc.Commit().HasError());
    }
  }

  void VerifyExtendedDataset(storage::Storage *store) {
    auto label_indexed = store->NameToLabel("extended_indexed");
    auto label_unused = store->NameToLabel("extended_unused");
    auto property_count = store->NameToProperty("count");
    auto et3 = store->NameToEdgeType("extended_et3");
    auto et4 = store->NameToEdgeType("extended_et4");

    // Verify indices info.
    {
      auto info = store->ListAllIndices();
      auto base_label_indexed = store->NameToLabel("base_indexed");
      auto base_label_unindexed = store->NameToLabel("base_unindexed");
      auto base_property_id = store->NameToProperty("id");
      ASSERT_THAT(info.label,
                  UnorderedElementsAre(base_label_unindexed, label_unused));
      ASSERT_THAT(info.label_property,
                  UnorderedElementsAre(
                      std::make_pair(base_label_indexed, base_property_id),
                      std::make_pair(label_indexed, property_count)));
    }

    // Verify constraints info.
    {
      auto info = store->ListAllConstraints();
      auto base_label_unindexed = store->NameToLabel("base_unindexed");
      auto base_property_id = store->NameToProperty("id");
      ASSERT_THAT(info.existence,
                  UnorderedElementsAre(
                      std::make_pair(base_label_unindexed, base_property_id),
                      std::make_pair(label_unused, property_count)));
    }

    // Create storage accessor.
    auto acc = store->Access();

    // Verify vertices.
    for (uint64_t i = 0; i < kNumExtendedVertices; ++i) {
      auto vertex =
          acc.FindVertex(extended_vertex_gids_[i], storage::View::OLD);
      ASSERT_TRUE(vertex);
      auto labels = vertex->Labels(storage::View::OLD);
      ASSERT_TRUE(labels.HasValue());
      if (i < kNumExtendedVertices / 2) {
        ASSERT_THAT(*labels, UnorderedElementsAre(label_indexed));
      }
      auto properties = vertex->Properties(storage::View::OLD);
      ASSERT_TRUE(properties.HasValue());
      if (i < kNumExtendedVertices / 3 || i >= kNumExtendedVertices / 2) {
        ASSERT_EQ(properties->size(), 1);
        ASSERT_EQ((*properties)[property_count],
                  storage::PropertyValue("nandare"));
      } else {
        ASSERT_EQ(properties->size(), 0);
      }
    }

    // Verify edges.
    for (uint64_t i = 0; i < kNumExtendedEdges; ++i) {
      auto find_edge =
          [&](const auto &edges) -> std::optional<storage::EdgeAccessor> {
        for (const auto &edge : edges) {
          if (edge.Gid() == extended_edge_gids_[i]) {
            return edge;
          }
        }
        return std::nullopt;
      };

      {
        auto vertex1 = acc.FindVertex(
            extended_vertex_gids_[(i / 5) % kNumExtendedVertices],
            storage::View::OLD);
        ASSERT_TRUE(vertex1);
        auto out_edges = vertex1->OutEdges({}, storage::View::OLD);
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
        auto vertex2 = acc.FindVertex(
            extended_vertex_gids_[(i / 6) % kNumExtendedVertices],
            storage::View::OLD);
        ASSERT_TRUE(vertex2);
        auto in_edges = vertex2->InEdges({}, storage::View::OLD);
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
      for (auto vertex : acc.Vertices(label_unused, storage::View::OLD)) {
        vertices.push_back(vertex);
      }
      ASSERT_EQ(vertices.size(), 0);
    }

    // Verify label+property index.
    {
      std::vector<storage::VertexAccessor> vertices;
      vertices.reserve(kNumExtendedVertices / 3);
      for (auto vertex :
           acc.Vertices(label_indexed, property_count, storage::View::OLD)) {
        vertices.push_back(vertex);
      }
      ASSERT_EQ(vertices.size(), kNumExtendedVertices / 3);
      std::sort(vertices.begin(), vertices.end(),
                [](const auto &a, const auto &b) { return a.Gid() < b.Gid(); });
      for (uint64_t i = 0; i < kNumExtendedVertices / 3; ++i) {
        ASSERT_EQ(vertices[i].Gid(), extended_vertex_gids_[i]);
      }
    }
  }

  std::vector<std::filesystem::path> GetSnapshotsList() {
    return GetFilesList(storage_directory / storage::kSnapshotDirectory);
  }

  std::vector<std::filesystem::path> GetWalsList() {
    return GetFilesList(storage_directory / storage::kWalDirectory);
  }

  std::filesystem::path storage_directory{
      std::filesystem::temp_directory_path() /
      "MG_test_unit_storage_v2_durability"};

 private:
  std::vector<std::filesystem::path> GetFilesList(
      const std::filesystem::path &path) {
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

INSTANTIATE_TEST_CASE_P(EdgesWithProperties, DurabilityTest,
                        ::testing::Values(true));
INSTANTIATE_TEST_CASE_P(EdgesWithoutProperties, DurabilityTest,
                        ::testing::Values(false));

// NOLINTNEXTLINE(hicpp-special-member-functions)
TEST_P(DurabilityTest, SnapshotOnExit) {
  // Create snapshot.
  {
    storage::Storage store(
        {.items = {.properties_on_edges = GetParam()},
         .durability = {.storage_directory = storage_directory,
                        .snapshot_on_exit = true}});
    CreateBaseDataset(&store, GetParam());
    VerifyBaseDataset(&store, GetParam(), false);
    CreateExtendedDataset(&store);
    VerifyBaseDataset(&store, GetParam(), true);
    VerifyExtendedDataset(&store);
  }

  ASSERT_EQ(GetSnapshotsList().size(), 1);
  ASSERT_EQ(GetWalsList().size(), 0);

  // Recover snapshot.
  storage::Storage store({.items = {.properties_on_edges = GetParam()},
                          .durability = {.storage_directory = storage_directory,
                                         .recover_on_startup = true}});
  VerifyBaseDataset(&store, GetParam(), true);
  VerifyExtendedDataset(&store);

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
                        .snapshot_wal_mode = storage::Config::Durability::
                            SnapshotWalMode::PERIODIC_SNAPSHOT,
                        .snapshot_interval = std::chrono::milliseconds(2000)}});
    CreateBaseDataset(&store, GetParam());
    std::this_thread::sleep_for(std::chrono::milliseconds(2500));
  }

  ASSERT_GE(GetSnapshotsList().size(), 1);
  ASSERT_EQ(GetWalsList().size(), 0);

  // Recover snapshot.
  storage::Storage store({.items = {.properties_on_edges = GetParam()},
                          .durability = {.storage_directory = storage_directory,
                                         .recover_on_startup = true}});
  VerifyBaseDataset(&store, GetParam(), false);

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
                        .snapshot_wal_mode = storage::Config::Durability::
                            SnapshotWalMode::PERIODIC_SNAPSHOT,
                        .snapshot_interval = std::chrono::milliseconds(2000)}});
    CreateBaseDataset(&store, GetParam());
    std::this_thread::sleep_for(std::chrono::milliseconds(2500));
    CreateExtendedDataset(&store);
    std::this_thread::sleep_for(std::chrono::milliseconds(2500));
  }

  ASSERT_GE(GetSnapshotsList().size(), 2);
  ASSERT_EQ(GetWalsList().size(), 0);

  // Destroy last snapshot.
  {
    auto snapshots = GetSnapshotsList();
    ASSERT_GE(snapshots.size(), 2);

    auto info = storage::ReadSnapshotInfo(*snapshots.begin());

    LOG(INFO) << "Destroying snapshot " << *snapshots.begin();
    utils::OutputFile file;
    file.Open(*snapshots.begin(), utils::OutputFile::Mode::OVERWRITE_EXISTING);
    file.SetPosition(utils::OutputFile::Position::SET, info.offset_vertices);
    auto value = static_cast<uint8_t>(storage::Marker::TYPE_MAP);
    file.Write(&value, sizeof(value));
    file.Sync();
    file.Close();
  }

  // Recover snapshot.
  storage::Storage store({.items = {.properties_on_edges = GetParam()},
                          .durability = {.storage_directory = storage_directory,
                                         .recover_on_startup = true}});
  VerifyBaseDataset(&store, GetParam(), false);

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
TEST_P(DurabilityTest, SnapshotRetention) {
  // Create unrelated snapshot.
  {
    storage::Storage store(
        {.items = {.properties_on_edges = GetParam()},
         .durability = {.storage_directory = storage_directory,
                        .snapshot_on_exit = true}});
    auto acc = store.Access();
    for (uint64_t i = 0; i < 1000; ++i) {
      acc.CreateVertex();
    }
    ASSERT_FALSE(acc.Commit().HasError());
  }

  ASSERT_GE(GetSnapshotsList().size(), 1);
  ASSERT_EQ(GetWalsList().size(), 0);

  // Create snapshot.
  {
    storage::Storage store(
        {.items = {.properties_on_edges = GetParam()},
         .durability = {.storage_directory = storage_directory,
                        .snapshot_wal_mode = storage::Config::Durability::
                            SnapshotWalMode::PERIODIC_SNAPSHOT,
                        .snapshot_interval = std::chrono::milliseconds(2000),
                        .snapshot_retention_count = 3}});
    CreateBaseDataset(&store, GetParam());
    // Allow approximately 5 snapshots to be created.
    std::this_thread::sleep_for(std::chrono::milliseconds(10000));
  }

  ASSERT_EQ(GetSnapshotsList().size(), 1 + 3);
  ASSERT_EQ(GetWalsList().size(), 0);

  // Verify that exactly 3 snapshots and 1 unrelated snapshot exist.
  {
    auto snapshots = GetSnapshotsList();
    ASSERT_EQ(snapshots.size(), 1 + 3);
    std::string uuid;
    for (size_t i = 0; i < snapshots.size(); ++i) {
      const auto &path = snapshots[i];
      // This shouldn't throw.
      auto info = storage::ReadSnapshotInfo(path);
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
                          .durability = {.storage_directory = storage_directory,
                                         .recover_on_startup = true}});
  VerifyBaseDataset(&store, GetParam(), false);

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
TEST_F(DurabilityTest,
       SnapshotWithoutPropertiesOnEdgesRecoveryWithPropertiesOnEdges) {
  // Create snapshot.
  {
    storage::Storage store(
        {.items = {.properties_on_edges = false},
         .durability = {.storage_directory = storage_directory,
                        .snapshot_on_exit = true}});
    CreateBaseDataset(&store, false);
    VerifyBaseDataset(&store, false, false);
    CreateExtendedDataset(&store);
    VerifyBaseDataset(&store, false, true);
    VerifyExtendedDataset(&store);
  }

  ASSERT_EQ(GetSnapshotsList().size(), 1);
  ASSERT_EQ(GetWalsList().size(), 0);

  // Recover snapshot.
  storage::Storage store({.items = {.properties_on_edges = true},
                          .durability = {.storage_directory = storage_directory,
                                         .recover_on_startup = true}});
  VerifyBaseDataset(&store, false, true);
  VerifyExtendedDataset(&store);

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
TEST_F(DurabilityTest,
       SnapshotWithPropertiesOnEdgesRecoveryWithoutPropertiesOnEdges) {
  // Create snapshot.
  {
    storage::Storage store(
        {.items = {.properties_on_edges = true},
         .durability = {.storage_directory = storage_directory,
                        .snapshot_on_exit = true}});
    CreateBaseDataset(&store, true);
    VerifyBaseDataset(&store, true, false);
    CreateExtendedDataset(&store);
    VerifyBaseDataset(&store, true, true);
    VerifyExtendedDataset(&store);
  }

  // Recover snapshot.
  storage::Storage store({.items = {.properties_on_edges = false},
                          .durability = {.storage_directory = storage_directory,
                                         .recover_on_startup = true}});
  {
    std::vector<storage::VertexAccessor> vertices;
    auto acc = store.Access();
    for (auto vertex : acc.Vertices(storage::View::OLD)) {
      vertices.push_back(vertex);
    }
    ASSERT_EQ(vertices.size(), 0);
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
TEST_F(DurabilityTest,
       SnapshotWithPropertiesOnEdgesButUnusedRecoveryWithoutPropertiesOnEdges) {
  // Create snapshot.
  {
    storage::Storage store(
        {.items = {.properties_on_edges = true},
         .durability = {.storage_directory = storage_directory,
                        .snapshot_on_exit = true}});
    CreateBaseDataset(&store, true);
    VerifyBaseDataset(&store, true, false);
    CreateExtendedDataset(&store);
    VerifyBaseDataset(&store, true, true);
    VerifyExtendedDataset(&store);
    // Remove properties from edges.
    {
      auto acc = store.Access();
      for (auto vertex : acc.Vertices(storage::View::OLD)) {
        auto in_edges = vertex.InEdges({}, storage::View::OLD);
        ASSERT_TRUE(in_edges.HasValue());
        for (auto edge : *in_edges) {
          // TODO (mferencevic): Replace with `ClearProperties()`
          auto props = edge.Properties(storage::View::NEW);
          ASSERT_TRUE(props.HasValue());
          for (const auto &prop : *props) {
            ASSERT_TRUE(edge.SetProperty(prop.first, storage::PropertyValue())
                            .HasValue());
          }
        }
        auto out_edges = vertex.InEdges({}, storage::View::OLD);
        ASSERT_TRUE(out_edges.HasValue());
        for (auto edge : *out_edges) {
          // TODO (mferencevic): Replace with `ClearProperties()`
          auto props = edge.Properties(storage::View::NEW);
          ASSERT_TRUE(props.HasValue());
          for (const auto &prop : *props) {
            ASSERT_TRUE(edge.SetProperty(prop.first, storage::PropertyValue())
                            .HasValue());
          }
        }
      }
      ASSERT_FALSE(acc.Commit().HasError());
    }
  }

  ASSERT_EQ(GetSnapshotsList().size(), 1);
  ASSERT_EQ(GetWalsList().size(), 0);

  // Recover snapshot.
  storage::Storage store({.items = {.properties_on_edges = false},
                          .durability = {.storage_directory = storage_directory,
                                         .recover_on_startup = true}});
  VerifyBaseDataset(&store, false, true);
  VerifyExtendedDataset(&store);

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
                        .snapshot_wal_mode = storage::Config::Durability::
                            SnapshotWalMode::PERIODIC_SNAPSHOT_WITH_WAL,
                        .snapshot_interval = std::chrono::minutes(20),
                        .wal_file_flush_every_n_tx = kFlushWalEvery}});
    CreateBaseDataset(&store, GetParam());
    CreateExtendedDataset(&store);
  }

  ASSERT_EQ(GetSnapshotsList().size(), 0);
  ASSERT_GE(GetWalsList().size(), 1);
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
                        .snapshot_wal_mode = storage::Config::Durability::
                            SnapshotWalMode::PERIODIC_SNAPSHOT_WITH_WAL,
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
      ASSERT_TRUE(e1->SetProperty(store.NameToProperty("test"),
                                  storage::PropertyValue("nandare"))
                      .HasValue());
    }
    ASSERT_TRUE(v2.AddLabel(store.NameToLabel("l21")).HasValue());
    ASSERT_TRUE(v2.SetProperty(store.NameToProperty("hello"),
                               storage::PropertyValue("world"))
                    .HasValue());
    auto v3 = acc.CreateVertex();
    gid_v3 = v3.Gid();
    ASSERT_TRUE(
        v3.SetProperty(store.NameToProperty("v3"), storage::PropertyValue(42))
            .HasValue());
    ASSERT_FALSE(acc.Commit().HasError());
  }

  ASSERT_EQ(GetSnapshotsList().size(), 0);
  ASSERT_GE(GetWalsList().size(), 1);
}

// NOLINTNEXTLINE(hicpp-special-member-functions)
TEST_P(DurabilityTest, WalCreateAndRemoveEverything) {
  // Create WALs.
  {
    storage::Storage store(
        {.items = {.properties_on_edges = GetParam()},
         .durability = {.storage_directory = storage_directory,
                        .snapshot_wal_mode = storage::Config::Durability::
                            SnapshotWalMode::PERIODIC_SNAPSHOT_WITH_WAL,
                        .snapshot_interval = std::chrono::minutes(20),
                        .wal_file_flush_every_n_tx = kFlushWalEvery}});
    CreateBaseDataset(&store, GetParam());
    CreateExtendedDataset(&store);
    auto indices = store.ListAllIndices();
    for (auto index : indices.label) {
      ASSERT_TRUE(store.DropIndex(index));
    }
    for (auto index : indices.label_property) {
      ASSERT_TRUE(store.DropIndex(index.first, index.second));
    }
    auto constraints = store.ListAllConstraints();
    for (auto constraint : constraints.existence) {
      ASSERT_TRUE(
          store.DropExistenceConstraint(constraint.first, constraint.second));
    }
    auto acc = store.Access();
    for (auto vertex : acc.Vertices(storage::View::OLD)) {
      ASSERT_TRUE(acc.DetachDeleteVertex(&vertex).HasValue());
    }
    ASSERT_FALSE(acc.Commit().HasError());
  }

  ASSERT_EQ(GetSnapshotsList().size(), 0);
  ASSERT_GE(GetWalsList().size(), 1);
}

// NOLINTNEXTLINE(hicpp-special-member-functions)
TEST_P(DurabilityTest, WalTransactionOrdering) {
  // NOLINTNEXTLINE(readability-isolate-declaration)
  storage::Gid gid1, gid2, gid3;

  // Create WAL.
  {
    storage::Storage store(
        {.items = {.properties_on_edges = GetParam()},
         .durability = {.storage_directory = storage_directory,
                        .snapshot_wal_mode = storage::Config::Durability::
                            SnapshotWalMode::PERIODIC_SNAPSHOT_WITH_WAL,
                        .snapshot_interval = std::chrono::minutes(20),
                        .wal_file_flush_every_n_tx = kFlushWalEvery,
                        .wal_file_size_kibibytes = 100000}});
    auto acc1 = store.Access();
    auto acc2 = store.Access();

    // Create vertex in transaction 2.
    {
      auto vertex2 = acc2.CreateVertex();
      gid2 = vertex2.Gid();
      ASSERT_TRUE(vertex2
                      .SetProperty(store.NameToProperty("id"),
                                   storage::PropertyValue(2))
                      .HasValue());
    }

    auto acc3 = store.Access();

    // Create vertex in transaction 3.
    {
      auto vertex3 = acc3.CreateVertex();
      gid3 = vertex3.Gid();
      ASSERT_TRUE(vertex3
                      .SetProperty(store.NameToProperty("id"),
                                   storage::PropertyValue(3))
                      .HasValue());
    }

    // Create vertex in transaction 1.
    {
      auto vertex1 = acc1.CreateVertex();
      gid1 = vertex1.Gid();
      ASSERT_TRUE(vertex1
                      .SetProperty(store.NameToProperty("id"),
                                   storage::PropertyValue(1))
                      .HasValue());
    }

    // Commit transaction 3, then 1, then 2.
    ASSERT_FALSE(acc3.Commit().HasError());
    ASSERT_FALSE(acc1.Commit().HasError());
    ASSERT_FALSE(acc2.Commit().HasError());
  }

  ASSERT_EQ(GetSnapshotsList().size(), 0);
  ASSERT_EQ(GetWalsList().size(), 1);

  // Verify WAL data.
  {
    auto path = GetWalsList().front();
    auto info = storage::ReadWalInfo(path);
    storage::Decoder wal;
    wal.Initialize(path, storage::kWalMagic);
    wal.SetPosition(info.offset_deltas);
    ASSERT_EQ(info.num_deltas, 9);
    std::vector<std::pair<uint64_t, storage::WalDeltaData>> data;
    for (uint64_t i = 0; i < info.num_deltas; ++i) {
      auto timestamp = storage::ReadWalDeltaHeader(&wal);
      data.emplace_back(timestamp, storage::ReadWalDeltaData(&wal));
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
    ASSERT_EQ(data[0].second.type, storage::WalDeltaData::Type::VERTEX_CREATE);
    ASSERT_EQ(data[0].second.vertex_create_delete.gid, gid3);
    ASSERT_EQ(data[1].second.type,
              storage::WalDeltaData::Type::VERTEX_SET_PROPERTY);
    ASSERT_EQ(data[1].second.vertex_edge_set_property.gid, gid3);
    ASSERT_EQ(data[1].second.vertex_edge_set_property.property, "id");
    ASSERT_EQ(data[1].second.vertex_edge_set_property.value,
              storage::PropertyValue(3));
    ASSERT_EQ(data[2].second.type,
              storage::WalDeltaData::Type::TRANSACTION_END);
    // Verify transaction 1.
    ASSERT_EQ(data[3].second.type, storage::WalDeltaData::Type::VERTEX_CREATE);
    ASSERT_EQ(data[3].second.vertex_create_delete.gid, gid1);
    ASSERT_EQ(data[4].second.type,
              storage::WalDeltaData::Type::VERTEX_SET_PROPERTY);
    ASSERT_EQ(data[4].second.vertex_edge_set_property.gid, gid1);
    ASSERT_EQ(data[4].second.vertex_edge_set_property.property, "id");
    ASSERT_EQ(data[4].second.vertex_edge_set_property.value,
              storage::PropertyValue(1));
    ASSERT_EQ(data[5].second.type,
              storage::WalDeltaData::Type::TRANSACTION_END);
    // Verify transaction 2.
    ASSERT_EQ(data[6].second.type, storage::WalDeltaData::Type::VERTEX_CREATE);
    ASSERT_EQ(data[6].second.vertex_create_delete.gid, gid2);
    ASSERT_EQ(data[7].second.type,
              storage::WalDeltaData::Type::VERTEX_SET_PROPERTY);
    ASSERT_EQ(data[7].second.vertex_edge_set_property.gid, gid2);
    ASSERT_EQ(data[7].second.vertex_edge_set_property.property, "id");
    ASSERT_EQ(data[7].second.vertex_edge_set_property.value,
              storage::PropertyValue(2));
    ASSERT_EQ(data[8].second.type,
              storage::WalDeltaData::Type::TRANSACTION_END);
  }
}

// NOLINTNEXTLINE(hicpp-special-member-functions)
TEST_P(DurabilityTest, WalCreateAndRemoveOnlyBaseDataset) {
  // Create WALs.
  {
    storage::Storage store(
        {.items = {.properties_on_edges = GetParam()},
         .durability = {.storage_directory = storage_directory,
                        .snapshot_wal_mode = storage::Config::Durability::
                            SnapshotWalMode::PERIODIC_SNAPSHOT_WITH_WAL,
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
  ASSERT_GE(GetWalsList().size(), 1);
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
                          .snapshot_wal_mode = storage::Config::Durability::
                              SnapshotWalMode::PERIODIC_SNAPSHOT_WITH_WAL,
                          .snapshot_interval = std::chrono::minutes(20),
                          .wal_file_flush_every_n_tx = kFlushWalEvery}});
      // Create one million vertices.
      for (uint64_t i = 0; i < 1000000; ++i) {
        auto acc = store.Access();
        acc.CreateVertex();
        CHECK(!acc.Commit().HasError()) << "Couldn't commit transaction!";
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
    LOG(FATAL) << "Couldn't create process to execute test!";
  }

  ASSERT_EQ(GetSnapshotsList().size(), 0);
  ASSERT_GE(GetWalsList().size(), 1);
}

// NOLINTNEXTLINE(hicpp-special-member-functions)
TEST_P(DurabilityTest, WalAllOperationsInSingleTransaction) {
  // Create WALs
  {
    storage::Storage store(
        {.items = {.properties_on_edges = GetParam()},
         .durability = {.storage_directory = storage_directory,
                        .snapshot_wal_mode = storage::Config::Durability::
                            SnapshotWalMode::PERIODIC_SNAPSHOT_WITH_WAL,
                        .snapshot_interval = std::chrono::minutes(20),
                        .wal_file_size_kibibytes = 1,
                        .wal_file_flush_every_n_tx = kFlushWalEvery}});
    auto acc = store.Access();
    auto vertex1 = acc.CreateVertex();
    auto vertex2 = acc.CreateVertex();
    ASSERT_TRUE(vertex1.AddLabel(acc.NameToLabel("nandare")).HasValue());
    ASSERT_TRUE(vertex2
                    .SetProperty(acc.NameToProperty("haihai"),
                                 storage::PropertyValue(42))
                    .HasValue());
    ASSERT_TRUE(vertex1.RemoveLabel(acc.NameToLabel("nandare")).HasValue());
    auto edge1 = acc.CreateEdge(&vertex1, &vertex2, acc.NameToEdgeType("et1"));
    ASSERT_TRUE(edge1.HasValue());
    ASSERT_TRUE(
        vertex2
            .SetProperty(acc.NameToProperty("haihai"), storage::PropertyValue())
            .HasValue());
    auto vertex3 = acc.CreateVertex();
    auto edge2 = acc.CreateEdge(&vertex3, &vertex3, acc.NameToEdgeType("et2"));
    ASSERT_TRUE(edge2.HasValue());
    if (GetParam()) {
      ASSERT_TRUE(edge2
                      ->SetProperty(acc.NameToProperty("meaning"),
                                    storage::PropertyValue(true))
                      .HasValue());
      ASSERT_TRUE(edge1
                      ->SetProperty(acc.NameToProperty("hello"),
                                    storage::PropertyValue("world"))
                      .HasValue());
      ASSERT_TRUE(edge2
                      ->SetProperty(acc.NameToProperty("meaning"),
                                    storage::PropertyValue())
                      .HasValue());
    }
    ASSERT_TRUE(vertex3.AddLabel(acc.NameToLabel("test")).HasValue());
    ASSERT_TRUE(vertex3
                    .SetProperty(acc.NameToProperty("nonono"),
                                 storage::PropertyValue(-1))
                    .HasValue());
    ASSERT_TRUE(
        vertex3
            .SetProperty(acc.NameToProperty("nonono"), storage::PropertyValue())
            .HasValue());
    if (GetParam()) {
      ASSERT_TRUE(edge1
                      ->SetProperty(acc.NameToProperty("hello"),
                                    storage::PropertyValue())
                      .HasValue());
    }
    ASSERT_TRUE(vertex3.RemoveLabel(acc.NameToLabel("test")).HasValue());
    ASSERT_TRUE(acc.DetachDeleteVertex(&vertex1).HasValue());
    ASSERT_TRUE(acc.DeleteEdge(&*edge2).HasValue());
    ASSERT_TRUE(acc.DeleteVertex(&vertex2).HasValue());
    ASSERT_TRUE(acc.DeleteVertex(&vertex3).HasValue());
    ASSERT_FALSE(acc.Commit().HasError());
  }

  ASSERT_EQ(GetSnapshotsList().size(), 0);
  ASSERT_GE(GetWalsList().size(), 1);
}

// NOLINTNEXTLINE(hicpp-special-member-functions)
TEST_P(DurabilityTest, WalAndSnapshot) {
  // Create snapshot and WALs.
  {
    storage::Storage store(
        {.items = {.properties_on_edges = GetParam()},
         .durability = {.storage_directory = storage_directory,
                        .snapshot_wal_mode = storage::Config::Durability::
                            SnapshotWalMode::PERIODIC_SNAPSHOT_WITH_WAL,
                        .snapshot_interval = std::chrono::milliseconds(2000),
                        .wal_file_flush_every_n_tx = kFlushWalEvery}});
    CreateBaseDataset(&store, GetParam());
    std::this_thread::sleep_for(std::chrono::milliseconds(2500));
    CreateExtendedDataset(&store);
  }

  ASSERT_GE(GetSnapshotsList().size(), 1);
  ASSERT_GE(GetWalsList().size(), 1);
}

// NOLINTNEXTLINE(hicpp-special-member-functions)
TEST_P(DurabilityTest, WalAndSnapshotAppendToExistingSnapshot) {
  // Create snapshot.
  {
    storage::Storage store(
        {.items = {.properties_on_edges = GetParam()},
         .durability = {.storage_directory = storage_directory,
                        .snapshot_on_exit = true}});
    CreateBaseDataset(&store, GetParam());
  }

  ASSERT_EQ(GetSnapshotsList().size(), 1);
  ASSERT_EQ(GetWalsList().size(), 0);

  // Recover snapshot.
  {
    storage::Storage store(
        {.items = {.properties_on_edges = GetParam()},
         .durability = {.storage_directory = storage_directory,
                        .recover_on_startup = true}});
    VerifyBaseDataset(&store, GetParam(), false);
  }

  // Recover snapshot and create WALs.
  {
    storage::Storage store(
        {.items = {.properties_on_edges = GetParam()},
         .durability = {.storage_directory = storage_directory,
                        .recover_on_startup = true,
                        .snapshot_wal_mode = storage::Config::Durability::
                            SnapshotWalMode::PERIODIC_SNAPSHOT_WITH_WAL,
                        .snapshot_interval = std::chrono::minutes(20),
                        .wal_file_flush_every_n_tx = kFlushWalEvery}});
    CreateExtendedDataset(&store);
  }

  ASSERT_EQ(GetSnapshotsList().size(), 1);
  ASSERT_GE(GetWalsList().size(), 1);
}

// NOLINTNEXTLINE(hicpp-special-member-functions)
TEST_P(DurabilityTest, WalAndSnapshotWalRetention) {
  // Create unrelated WALs.
  {
    storage::Storage store(
        {.items = {.properties_on_edges = GetParam()},
         .durability = {.storage_directory = storage_directory,
                        .snapshot_wal_mode = storage::Config::Durability::
                            SnapshotWalMode::PERIODIC_SNAPSHOT_WITH_WAL,
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
  ASSERT_GE(GetWalsList().size(), 1);

  uint64_t unrelated_wals = GetWalsList().size();

  uint64_t items_created = 0;

  // Create snapshot and WALs.
  {
    storage::Storage store(
        {.items = {.properties_on_edges = GetParam()},
         .durability = {.storage_directory = storage_directory,
                        .snapshot_wal_mode = storage::Config::Durability::
                            SnapshotWalMode::PERIODIC_SNAPSHOT_WITH_WAL,
                        .snapshot_interval = std::chrono::seconds(2),
                        .wal_file_size_kibibytes = 1,
                        .wal_file_flush_every_n_tx = 1}});
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
  ASSERT_GE(GetWalsList().size(), unrelated_wals + 1);
}
