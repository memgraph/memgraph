#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include <algorithm>
#include <chrono>
#include <filesystem>
#include <thread>

#include "storage/v2/durability.hpp"
#include "storage/v2/storage.hpp"
#include "utils/file.hpp"

using testing::Contains;
using testing::UnorderedElementsAre;

class DurabilityTest : public ::testing::TestWithParam<bool> {
 private:
  const uint64_t kNumBaseVertices = 1000;
  const uint64_t kNumBaseEdges = 10000;
  const uint64_t kNumExtendedVertices = 100;
  const uint64_t kNumExtendedEdges = 1000;

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
    std::vector<std::filesystem::path> ret;
    for (auto &item : std::filesystem::directory_iterator(
             storage_directory / storage::kSnapshotDirectory)) {
      ret.push_back(item.path());
    }
    std::sort(ret.begin(), ret.end());
    std::reverse(ret.begin(), ret.end());
    return ret;
  }

  std::filesystem::path storage_directory{
      std::filesystem::temp_directory_path() /
      "MG_test_unit_storage_v2_durability"};

 private:
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
                        .snapshot_type =
                            storage::Config::Durability::SnapshotType::PERIODIC,
                        .snapshot_interval = std::chrono::milliseconds(2000)}});
    CreateBaseDataset(&store, GetParam());
    std::this_thread::sleep_for(std::chrono::milliseconds(2500));
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
TEST_P(DurabilityTest, SnapshotFallback) {
  // Create snapshot.
  {
    storage::Storage store(
        {.items = {.properties_on_edges = GetParam()},
         .durability = {.storage_directory = storage_directory,
                        .snapshot_type =
                            storage::Config::Durability::SnapshotType::PERIODIC,
                        .snapshot_interval = std::chrono::milliseconds(2000)}});
    CreateBaseDataset(&store, GetParam());
    std::this_thread::sleep_for(std::chrono::milliseconds(2500));
    CreateExtendedDataset(&store);
    std::this_thread::sleep_for(std::chrono::milliseconds(2500));
  }

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
  // Create snapshot.
  {
    storage::Storage store(
        {.items = {.properties_on_edges = GetParam()},
         .durability = {.storage_directory = storage_directory,
                        .snapshot_type =
                            storage::Config::Durability::SnapshotType::PERIODIC,
                        .snapshot_interval = std::chrono::milliseconds(2000),
                        .snapshot_retention_count = 3}});
    CreateBaseDataset(&store, GetParam());
    // Allow approximately 5 snapshots to be created.
    std::this_thread::sleep_for(std::chrono::milliseconds(10000));
  }

  // Verify that exactly 3 snapshots exist.
  {
    auto snapshots = GetSnapshotsList();
    ASSERT_EQ(snapshots.size(), 3);
    for (const auto &path : snapshots) {
      // This shouldn't throw.
      storage::ReadSnapshotInfo(path);
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
