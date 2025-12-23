// Copyright 2025 Memgraph Ltd.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt; by using this file, you agree to be bound by the terms of the Business Source
// License, and you may not use this file except in compliance with the Business Source License.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

#include <algorithm>
#include <atomic>
#include <chrono>
#include <iterator>
#include <thread>
#include <utility>
#include <vector>

#include <fmt/format.h>
#include <gtest/gtest.h>

#include "query/db_accessor.hpp"
#include "storage/v2/inmemory/storage.hpp"
#include "storage/v2/property_value.hpp"
#include "storage/v2/storage.hpp"
#include "storage/v2/vertex_accessor.hpp"
#include "tests/test_commit_args_helper.hpp"
#include "utils/bound.hpp"

using memgraph::query::DbAccessor;
using memgraph::storage::Config;
using memgraph::storage::Gid;
using memgraph::storage::LabelId;
using memgraph::storage::PropertyId;
using memgraph::storage::PropertyPath;
using memgraph::storage::PropertyValue;
using memgraph::storage::PropertyValueRange;
using memgraph::storage::Storage;
using memgraph::storage::View;

// Helper function to process chunks in parallel and collect GIDs in a synchronized vector
template <typename ChunkedContainer, typename PerItemProcessor, typename ChunkValidator>
std::vector<Gid> ProcessChunksInParallel(ChunkedContainer &container, PerItemProcessor per_item_processor,
                                         ChunkValidator chunk_validator) {
  std::vector<std::jthread> threads;
  memgraph::utils::Synchronized<std::vector<Gid>> read_gids;
  for (size_t i = 0; i < container.size(); ++i) {
    threads.emplace_back([&container, i, &read_gids, per_item_processor, chunk_validator]() {
      std::vector<Gid> gids;
      int local_count = 0;
      auto chunk = container.get_chunk(i);
      for (auto it = chunk.begin(); it != chunk.end(); ++it) {
        local_count++;
        gids.push_back((*it).Gid());
        per_item_processor(*it);
      }
      chunk_validator(local_count);
      auto locked_gids = read_gids.Lock();
      locked_gids->insert(locked_gids->end(), std::make_move_iterator(gids.begin()),
                          std::make_move_iterator(gids.end()));
    });
  }

  // Wait for all threads to complete
  threads.clear();

  auto locked_gids = read_gids.Lock();
  std::sort(locked_gids->begin(), locked_gids->end());
  return std::move(*locked_gids);
}

// Simplified version for cases that only need basic validation
template <typename ChunkedContainer>
std::vector<Gid> ProcessChunksInParallel(ChunkedContainer &container) {
  return ProcessChunksInParallel(
      container, [](const auto &) {},  // No per-item processing
      [](int) {}                       // No chunk validation
  );
}

// Helper function to process chunks in parallel and collect PropertyValues
template <typename ChunkedContainer, typename PerItemProcessor, typename ChunkValidator>
std::vector<PropertyValue> ProcessChunksInParallelForPropertyValues(ChunkedContainer &container,
                                                                    PerItemProcessor per_item_processor,
                                                                    ChunkValidator chunk_validator) {
  std::vector<std::jthread> threads;
  memgraph::utils::Synchronized<std::vector<PropertyValue>> read_props;
  for (size_t i = 0; i < container.size(); ++i) {
    threads.emplace_back([&container, i, &read_props, per_item_processor, chunk_validator]() {
      std::vector<PropertyValue> props;
      int local_count = 0;
      auto chunk = container.get_chunk(i);
      for (auto it = chunk.begin(); it != chunk.end(); ++it) {
        local_count++;
        per_item_processor(*it, props);
      }
      chunk_validator(local_count);
      auto locked_props = read_props.Lock();
      locked_props->insert(locked_props->end(), std::make_move_iterator(props.begin()),
                           std::make_move_iterator(props.end()));
    });
  }

  // Wait for all threads to complete
  threads.clear();

  auto locked_props = read_props.Lock();
  std::sort(locked_props->begin(), locked_props->end());
  return std::move(*locked_props);
}

class StorageV2ChunkIteratorTest : public ::testing::Test {
 protected:
  void SetUp() override {
    storage_ = std::make_unique<memgraph::storage::InMemoryStorage>(config_);
    {  // Label index
      auto unique_acc = storage_->UniqueAccess();
      label_id_ = unique_acc->NameToLabel("label");
      property_id_ = unique_acc->NameToProperty("property");
      ASSERT_TRUE(unique_acc->CreateIndex(label_id_).has_value());
      ASSERT_TRUE(unique_acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()).has_value());
    }
    {  // Label property index
      auto unique_acc = storage_->UniqueAccess();
      ASSERT_TRUE(unique_acc->CreateIndex(label_id_, {property_id_}).has_value());
      ASSERT_TRUE(unique_acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()).has_value());
    }
    {  // Edge property index
      auto unique_acc = storage_->UniqueAccess();
      edge_type_id_ = unique_acc->NameToEdgeType("EdgeType");
      ASSERT_EQ(property_id_, unique_acc->NameToProperty("property"));
      ASSERT_TRUE(unique_acc->CreateGlobalEdgeIndex(property_id_).has_value());
      ASSERT_TRUE(unique_acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()).has_value());
    }
    {  // Edge type index
      auto unique_acc = storage_->UniqueAccess();
      edge_type_id1_ = unique_acc->NameToEdgeType("EdgeType1");
      edge_type_id2_ = unique_acc->NameToEdgeType("EdgeType2");
      ASSERT_TRUE(unique_acc->CreateIndex(edge_type_id1_).has_value());
      ASSERT_TRUE(unique_acc->CreateIndex(edge_type_id2_).has_value());
      ASSERT_TRUE(unique_acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()).has_value());
    }
    {  // Edge type property index
      auto unique_acc = storage_->UniqueAccess();
      ASSERT_EQ(edge_type_id1_, unique_acc->NameToEdgeType("EdgeType1"));
      ASSERT_EQ(edge_type_id2_, unique_acc->NameToEdgeType("EdgeType2"));
      ASSERT_EQ(property_id_, unique_acc->NameToProperty("property"));
      ASSERT_TRUE(unique_acc->CreateIndex(edge_type_id1_, property_id_).has_value());
      ASSERT_TRUE(unique_acc->CreateIndex(edge_type_id2_, property_id_).has_value());
      ASSERT_TRUE(unique_acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()).has_value());
    }
  }

  void TearDown() override { storage_.reset(); }

  std::unique_ptr<Storage> storage_;
  LabelId label_id_;
  memgraph::storage::EdgeTypeId edge_type_id_;
  memgraph::storage::EdgeTypeId edge_type_id1_;
  memgraph::storage::EdgeTypeId edge_type_id2_;
  PropertyId property_id_;
  Config config_{.salient = {.items = {.properties_on_edges = true}}};
};

// ============================================================================
// ALL VERTICES TESTS
// ============================================================================

TEST_F(StorageV2ChunkIteratorTest, AllVerticesChunkIteratorBasic) {
  // Create vertices with different labels
  auto acc = storage_->Access();
  std::vector<Gid> vertex_gids;

  // Create 100 vertices with the test label
  for (int i = 0; i < 100; ++i) {
    auto vertex = acc->CreateVertex();
    ASSERT_TRUE(vertex.AddLabel(label_id_).has_value());
    vertex_gids.push_back(vertex.Gid());
  }

  // Create 50 vertices without the label
  for (int i = 0; i < 50; ++i) {
    auto vertex = acc->CreateVertex();
    vertex_gids.push_back(vertex.Gid());
  }

  ASSERT_TRUE(acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()).has_value());

  // Test chunking for all vertices
  auto vertices = acc->ChunkedVertices(View::OLD, 4);

  ASSERT_GT(vertices.size(), 0);

  // Count total vertices across all chunks
  auto read_gids = ProcessChunksInParallel(
      vertices, [](const auto &) {},  // No additional per-item processing
      [](int local_count) {
        ASSERT_GT(local_count, 0);
        ASSERT_LT(local_count, 150 / 4 * 2);
      });

  ASSERT_TRUE(std::equal(read_gids.begin(), read_gids.end(), vertex_gids.begin()));
}

TEST_F(StorageV2ChunkIteratorTest, AllVerticesChunkIteratorEdgeCases) {
  // Create vertices for testing
  std::vector<Gid> vertex_gids;

  // Empty storage
  {
    auto acc = storage_->Access();
    auto vertices = acc->ChunkedVertices(View::OLD, 4);
    ASSERT_EQ(vertices.size(), 1);
    auto chunk = vertices.get_chunk(0);
    ASSERT_TRUE(chunk.begin() == chunk.end());
  }

  // Single vertex
  {
    auto acc = storage_->Access();
    auto vertex = acc->CreateVertex();
    vertex_gids.push_back(vertex.Gid());
    ASSERT_TRUE(acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()).has_value());

    auto vertices = acc->ChunkedVertices(View::OLD, 4);
    ASSERT_EQ(vertices.size(), 1);
    auto read_gids = ProcessChunksInParallel(vertices);
    ASSERT_TRUE(std::equal(read_gids.begin(), read_gids.end(), vertex_gids.begin()));
  }

  // Fewer vertices than chunks
  {
    auto acc = storage_->Access();
    for (int i = 0; i < 2; ++i) {  // add 2 new vertices
      auto vertex = acc->CreateVertex();
      vertex_gids.push_back(vertex.Gid());
    }
    ASSERT_TRUE(acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()).has_value());

    auto vertices = acc->ChunkedVertices(View::OLD, 4);
    ASSERT_EQ(vertices.size(), 3);
    auto read_gids = ProcessChunksInParallel(vertices);
    ASSERT_TRUE(std::equal(read_gids.begin(), read_gids.end(), vertex_gids.begin()));
  }

  // Exact number of vertices and chunks
  {
    auto acc = storage_->Access();
    // Add one more vertex
    auto vertex = acc->CreateVertex();
    vertex_gids.push_back(vertex.Gid());
    ASSERT_TRUE(acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()).has_value());
  }
  {
    auto acc = storage_->Access();
    auto vertices = acc->ChunkedVertices(View::OLD, 4);
    ASSERT_EQ(vertices.size(), 4);
    auto read_gids = ProcessChunksInParallel(vertices);
    ASSERT_TRUE(std::equal(read_gids.begin(), read_gids.end(), vertex_gids.begin()));
  }

  // Test with very large chunk size (more than total vertices)
  {
    auto acc = storage_->Access();
    auto vertices = acc->ChunkedVertices(View::OLD, 1000);
    ASSERT_EQ(vertices.size(), vertex_gids.size());
    auto read_gids = ProcessChunksInParallel(vertices);
    ASSERT_TRUE(std::equal(read_gids.begin(), read_gids.end(), vertex_gids.begin()));
  }

  // Test with chunk size of 1
  {
    auto acc = storage_->Access();
    auto vertices = acc->ChunkedVertices(View::OLD, 1);
    ASSERT_EQ(vertices.size(), 1);
    auto read_gids = ProcessChunksInParallel(vertices);
    ASSERT_TRUE(std::equal(read_gids.begin(), read_gids.end(), vertex_gids.begin()));
  }
}

TEST_F(StorageV2ChunkIteratorTest, AllVerticesChunkIteratorBigDataset) {
  // Create a large number of vertices
  std::vector<Gid> vertex_gids;
  {
    auto acc = storage_->Access();
    // Create 100,000 vertices
    for (int i = 0; i < 100'000; ++i) {
      auto vertex = acc->CreateVertex();
      vertex_gids.push_back(vertex.Gid());
    }
    ASSERT_TRUE(acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()).has_value());
  }

  auto acc = storage_->Access();
  // Test chunking with 16 chunks for better distribution
  auto vertices = acc->ChunkedVertices(View::OLD, 16);

  ASSERT_GT(vertices.size(), 0);

  // Count total vertices across all chunks
  std::atomic<int> total_count{0};
  auto read_gids = ProcessChunksInParallel(
      vertices, [](const auto &) {},  // No additional per-item processing
      [&total_count](int local_count) {
        EXPECT_GT(local_count, 0);
        EXPECT_LT(local_count, 100'000 / 16 * 3);  // Allow some variance
        total_count.fetch_add(local_count, std::memory_order_relaxed);
      });

  ASSERT_EQ(total_count.load(), 100'000);
  ASSERT_TRUE(std::equal(read_gids.begin(), read_gids.end(), vertex_gids.begin()));
}

TEST_F(StorageV2ChunkIteratorTest, AllVerticesChunkIteratorConcurrentOperations) {
  // Create initial vertices
  std::vector<Gid> vertex_gids;
  {
    auto acc = storage_->Access();
    // Create 1000 vertices
    for (int i = 0; i < 1000; ++i) {
      auto vertex = acc->CreateVertex();
      vertex_gids.push_back(vertex.Gid());
    }
    ASSERT_TRUE(acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()).has_value());
  }

  // Take the access before modifing threads start
  auto acc = storage_->Access();
  std::atomic<bool> run{true};

  // Start a thread that continuously modifies vertices
  std::thread modify_thread([&]() {
    while (run.load()) {
      auto modify_acc = storage_->Access();
      // Create some new vertices
      for (int i = 0; i < 10; ++i) {
        auto vertex = modify_acc->CreateVertex();
        vertex_gids.push_back(vertex.Gid());
      }
      ASSERT_TRUE(modify_acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()).has_value());
      std::this_thread::sleep_for(std::chrono::microseconds(100));
    }
  });

  std::this_thread::sleep_for(std::chrono::milliseconds(1));  // Wait for the modify thread to start
  auto vertices = acc->ChunkedVertices(View::OLD, 8);

  ASSERT_GT(vertices.size(), 0);

  // Count total vertices across all chunks
  auto read_gids = ProcessChunksInParallel(vertices);

  run.store(false);
  modify_thread.join();

  // The read_gids should contain at least the original 1000 vertices
  ASSERT_GE(read_gids.size(), 1000);
  // Verify that all original vertices are present
  std::set<Gid> original_vertices(vertex_gids.begin(), vertex_gids.begin() + 1000);
  std::set<Gid> read_vertices(read_gids.begin(), read_gids.end());
  for (const auto &gid : original_vertices) {
    ASSERT_TRUE(read_vertices.find(gid) != read_vertices.end());
  }
}

// ============================================================================
// LABEL INDEX TESTS
// ============================================================================

TEST_F(StorageV2ChunkIteratorTest, LabelIndexChunkIteratorBasic) {
  // Create vertices with specific label
  std::vector<Gid> vertex_gids;
  std::vector<Gid> labeled_vertex_gids;

  // Create 200 vertices with the test label
  {
    auto acc = storage_->Access();
    for (int i = 0; i < 200; ++i) {
      auto vertex = acc->CreateVertex();
      if (i % 3 == 0) {
        ASSERT_TRUE(vertex.AddLabel(label_id_).has_value());
        labeled_vertex_gids.push_back(vertex.Gid());
      }
      vertex_gids.push_back(vertex.Gid());
    }

    ASSERT_TRUE(acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()).has_value());
  }

  // Test chunking for labeled vertices
  auto acc = storage_->Access();
  auto vertices = acc->ChunkedVertices(label_id_, View::OLD, 4);

  ASSERT_GT(vertices.size(), 0);

  // Count total vertices across all chunks
  auto read_gids = ProcessChunksInParallel(
      vertices, [](const auto &) {},  // No additional per-item processing
      [](int local_count) {
        ASSERT_GT(local_count, 0);
        ASSERT_LT(local_count, 67 / 4 * 2);
      });

  ASSERT_TRUE(std::equal(read_gids.begin(), read_gids.end(), labeled_vertex_gids.begin()));
}

TEST_F(StorageV2ChunkIteratorTest, LabelIndexChunkIteratorMultipleEntriesEdgeCases) {
  Gid vertex_gid;

  auto add_vertex = [&]() {
    auto acc = storage_->Access();
    auto vertex = acc->CreateVertex();
    vertex_gid = vertex.Gid();
    ASSERT_TRUE(vertex.AddLabel(label_id_).has_value());
    ASSERT_TRUE(vertex.SetProperty(property_id_, PropertyValue{0}).has_value());
    ASSERT_TRUE(acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()).has_value());
  };
  auto update_vertex = [&]() {
    auto acc = storage_->Access();
    auto vertex = acc->FindVertex(vertex_gid, View::OLD);
    ASSERT_TRUE(vertex);
    ASSERT_TRUE(vertex->RemoveLabel(label_id_).has_value());
    ASSERT_TRUE(vertex->AddLabel(label_id_).has_value());
    auto prop = vertex->GetProperty(property_id_, View::OLD);
    ASSERT_TRUE(prop.has_value());
    ASSERT_TRUE(prop->IsInt());
    ASSERT_TRUE(vertex->SetProperty(property_id_, PropertyValue{prop->ValueInt() + 1}).has_value());
    ASSERT_TRUE(acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()).has_value());
  };

  // Create 4 elements in the index and accessors with different timestamps
  auto acc0 = storage_->Access();
  add_vertex();
  auto acc1 = storage_->Access();
  update_vertex();
  auto acc2 = storage_->Access();
  update_vertex();
  auto acc3 = storage_->Access();
  update_vertex();
  auto acc4 = storage_->Access();

  auto read_count = [](auto &chunks) {
    int count = 0;
    for (size_t i = 0; i < chunks.size(); ++i) {
      auto chunk = chunks.get_chunk(i);
      for (const auto &entry : chunk) {
        count++;
        (void)entry;
      }
    }
    return count;
  };

  auto read_prop = [this](auto &chunks) -> int {
    std::optional<int> prop;
    for (size_t i = 0; i < chunks.size(); ++i) {
      auto chunk = chunks.get_chunk(i);
      for (const auto &entry : chunk) {
        if (prop.has_value()) return -1;
        auto v_prop = entry.GetProperty(property_id_, View::OLD);
        if (!v_prop) return -1;
        if (!v_prop->IsInt()) return -1;
        prop = v_prop->ValueInt();
      }
    }
    if (!prop) return -1;
    return *prop;
  };

  // acc0
  {
    auto vertices = acc0->ChunkedVertices(label_id_, View::OLD, 4);
    ASSERT_EQ(read_count(vertices), 0);
  }
  // acc1
  {
    auto vertices = acc1->ChunkedVertices(label_id_, View::OLD, 4);
    ASSERT_EQ(read_count(vertices), 1);
    ASSERT_EQ(read_prop(vertices), 0);
  }
  // acc2
  {
    auto vertices = acc2->ChunkedVertices(label_id_, View::OLD, 4);
    ASSERT_EQ(read_count(vertices), 1);
    ASSERT_EQ(read_prop(vertices), 1);
  }
  // acc3
  {
    auto vertices = acc3->ChunkedVertices(label_id_, View::OLD, 4);
    ASSERT_EQ(read_count(vertices), 1);
    ASSERT_EQ(read_prop(vertices), 2);
  }
  // acc4
  {
    auto vertices = acc4->ChunkedVertices(label_id_, View::OLD, 4);
    ASSERT_EQ(read_count(vertices), 1);
    ASSERT_EQ(read_prop(vertices), 3);
  }
}

TEST_F(StorageV2ChunkIteratorTest, LabelIndexChunkIteratorMultipleEntries) {
  // Create vertices with specific label
  std::vector<Gid> vertex_gids;
  std::vector<Gid> labeled_vertex_gids;

  // Create 200 vertices with the test label
  {
    auto acc = storage_->Access();
    for (int i = 0; i < 200; ++i) {
      auto vertex = acc->CreateVertex();
      if (i % 3 == 0) {
        ASSERT_TRUE(vertex.AddLabel(label_id_).has_value());
        labeled_vertex_gids.push_back(vertex.Gid());
      }
      vertex_gids.push_back(vertex.Gid());
    }

    ASSERT_TRUE(acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()).has_value());
  }

  // Modify labeled vertices to have multiple entries
  {
    auto acc = storage_->Access();
    for (const auto gid : labeled_vertex_gids) {
      auto vertex = acc->FindVertex(gid, View::OLD);
      ASSERT_TRUE(vertex);
      ASSERT_TRUE(vertex->RemoveLabel(label_id_).has_value());
    }
    ASSERT_TRUE(acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()).has_value());
  }
  {
    auto acc = storage_->Access();
    for (const auto gid : labeled_vertex_gids) {
      auto vertex = acc->FindVertex(gid, View::OLD);
      ASSERT_TRUE(vertex);
      ASSERT_TRUE(vertex->AddLabel(label_id_).has_value());
    }
    ASSERT_TRUE(acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()).has_value());
  }

  // Test chunking for labeled vertices
  auto acc = storage_->Access();
  auto vertices = acc->ChunkedVertices(label_id_, View::OLD, 4);

  ASSERT_GT(vertices.size(), 0);

  // Count total vertices across all chunks
  std::atomic<int> total_count{0};
  auto read_gids = ProcessChunksInParallel(
      vertices, [](const auto &) {},  // No additional per-item processing
      [&total_count](int local_count) {
        ASSERT_GT(local_count, 0);
        ASSERT_LT(local_count, 67 / 4 * 2);
        total_count.fetch_add(local_count, std::memory_order_relaxed);
      });

  ASSERT_TRUE(std::equal(read_gids.begin(), read_gids.end(), labeled_vertex_gids.begin()));
}

TEST_F(StorageV2ChunkIteratorTest, LabelIndexChunkIteratorDynamic) {
  // Create vertices with specific label
  std::vector<Gid> vertex_gids;
  std::vector<Gid> labeled_vertex_gids;

  // Create 200 vertices with the test label
  {
    auto acc = storage_->Access();
    for (int i = 0; i < 200; ++i) {
      auto vertex = acc->CreateVertex();
      if (i % 3 == 0) {
        ASSERT_TRUE(vertex.AddLabel(label_id_).has_value());
        labeled_vertex_gids.push_back(vertex.Gid());
      }
      vertex_gids.push_back(vertex.Gid());
    }

    ASSERT_TRUE(acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()).has_value());
  }

  // Test chunking for labeled vertices
  auto acc = storage_->Access();  // Get access before starting the modify thread

  std::atomic_bool run = true;
  auto modify_thread = std::jthread([&]() {
    while (run) {
      int i = 0;
      auto acc = storage_->Access();
      for (const auto gid : vertex_gids) {
        auto vertex = acc->FindVertex(gid, View::OLD);
        if (!vertex) continue;
        ++i;
        if (i % 2 == 0) {
          ASSERT_TRUE(vertex->RemoveLabel(label_id_).has_value());
        } else if (i % 3 == 0) {
          ASSERT_TRUE(vertex->AddLabel(label_id_).has_value());
        } else if (i % 5 == 0) {
          ASSERT_TRUE(acc->DeleteVertex(&*vertex).has_value());
        }
      }
      ASSERT_TRUE(acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()).has_value());
      storage_->FreeMemory();  // Run storage and skiplist gc
      std::this_thread::sleep_for(std::chrono::microseconds(100));
    }
  });

  std::this_thread::sleep_for(std::chrono::milliseconds(1));  // Wait for the modify thread to start
  auto vertices = acc->ChunkedVertices(label_id_, View::OLD, 4);

  ASSERT_GT(vertices.size(), 0);

  // Count total vertices across all chunks
  std::atomic<int> total_count{0};
  auto read_gids = ProcessChunksInParallel(
      vertices, [](const auto &) {},  // No additional per-item processing
      [&total_count](int local_count) {
        ASSERT_GT(local_count, 0);
        ASSERT_LT(local_count, 67 / 4 * 2);
        total_count.fetch_add(local_count, std::memory_order_relaxed);
      });

  run.store(false);
  modify_thread.join();

  ASSERT_TRUE(std::equal(read_gids.begin(), read_gids.end(), labeled_vertex_gids.begin()));
}

TEST_F(StorageV2ChunkIteratorTest, LabelIndexChunkIteratorManyEntries) {
  // Create vertices with specific label
  std::vector<Gid> vertex_gids;
  std::vector<Gid> labeled_vertex_gids;

  // Create 200 vertices with the test label
  {
    auto acc = storage_->Access();
    for (int i = 0; i < 200; ++i) {
      auto vertex = acc->CreateVertex();
      if (i % 3 == 0) {
        ASSERT_TRUE(vertex.AddLabel(label_id_).has_value());
        labeled_vertex_gids.push_back(vertex.Gid());
      }
      vertex_gids.push_back(vertex.Gid());
    }

    ASSERT_TRUE(acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()).has_value());
  }

  // Modify labeled vertices to have multiple entries
  auto modified_gid = labeled_vertex_gids[50];
  for (int i = 0; i < 100; ++i) {
    {
      auto acc = storage_->Access();
      auto vertex = acc->FindVertex(modified_gid, View::OLD);
      ASSERT_TRUE(vertex);
      ASSERT_TRUE(vertex->RemoveLabel(label_id_).has_value());
      ASSERT_TRUE(acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()).has_value());
    }
    {
      auto acc = storage_->Access();
      auto vertex = acc->FindVertex(modified_gid, View::OLD);
      ASSERT_TRUE(vertex);
      ASSERT_TRUE(vertex->AddLabel(label_id_).has_value());
      ASSERT_TRUE(acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()).has_value());
    }
  }
  // Test chunking for labeled vertices
  auto acc = storage_->Access();
  auto vertices = acc->ChunkedVertices(label_id_, View::OLD, 4);

  ASSERT_GT(vertices.size(), 0);

  // Count total vertices across all chunks
  std::atomic<int> total_count{0};
  auto read_gids = ProcessChunksInParallel(
      vertices, [](const auto &) {},  // No additional per-item processing
      [&total_count](int local_count) {
        // NOTE: We don't assert on the count here because we don't know how many same vertices are in the chunk.
        // ASSERT_GT(local_count, 0);
        // ASSERT_LT(local_count, 67 / 4 * 2);
        total_count.fetch_add(local_count, std::memory_order_relaxed);
      });

  ASSERT_TRUE(std::equal(read_gids.begin(), read_gids.end(), labeled_vertex_gids.begin()));
}

TEST_F(StorageV2ChunkIteratorTest, LabelIndexChunkIteratorEdgeCases) {
  // Create vertices with specific label
  std::vector<Gid> vertex_gids;
  std::vector<Gid> labeled_vertex_gids;

  // Empty index
  {
    // Test chunking for labeled vertices
    auto acc = storage_->Access();
    auto vertices = acc->ChunkedVertices(label_id_, View::OLD, 4);
    ASSERT_EQ(vertices.size(), 1);
    auto chunk = vertices.get_chunk(0);
    ASSERT_TRUE(chunk.begin() == chunk.end());
  }

  // Non existing label (Cannot be tested because we just assert at the index level)

  // Fewer vertices than chunks
  {
    auto acc = storage_->Access();
    for (int i = 0; i < 2; ++i) {
      auto vertex = acc->CreateVertex();
      ASSERT_TRUE(vertex.AddLabel(label_id_).has_value());
      labeled_vertex_gids.push_back(vertex.Gid());
    }
    ASSERT_TRUE(acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()).has_value());
    auto vertices = acc->ChunkedVertices(label_id_, View::OLD, 4);
    ASSERT_EQ(vertices.size(), 2);
  }

  // Exact number of vertices and chunks
  {
    auto acc = storage_->Access();
    for (int i = 0; i < 2 * 3; ++i) {
      auto vertex = acc->CreateVertex();
      if (i % 3 == 0) {
        ASSERT_TRUE(vertex.AddLabel(label_id_).has_value());
        labeled_vertex_gids.push_back(vertex.Gid());
      }
      vertex_gids.push_back(vertex.Gid());
    }
    ASSERT_EQ(labeled_vertex_gids.size(), 4);
    ASSERT_TRUE(acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()).has_value());
  }
  {
    auto acc = storage_->Access();
    auto vertices = acc->ChunkedVertices(label_id_, View::OLD, 4);
    ASSERT_EQ(vertices.size(), 4);
    auto read_gids = ProcessChunksInParallel(vertices);
    ASSERT_TRUE(std::equal(read_gids.begin(), read_gids.end(), labeled_vertex_gids.begin()));
  }

  // Same 100 vertex at the beginning
  {
    const auto first_gid = labeled_vertex_gids[0];
    for (int i = 0; i < 100; ++i) {
      auto acc = storage_->Access();
      auto vertex = acc->FindVertex(first_gid, View::OLD);
      ASSERT_TRUE(vertex);
      ASSERT_TRUE(vertex->RemoveLabel(label_id_).has_value());
      ASSERT_TRUE(vertex->AddLabel(label_id_).has_value());
      ASSERT_TRUE(acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()).has_value());
    }
    auto acc = storage_->Access();
    auto vertices = acc->ChunkedVertices(label_id_, View::OLD, 4);
    // NOTE This is an edge case where we need to skip whole chunks because of duplicates in the index.
    // ASSERT_EQ(vertices.size(), 4);
    auto read_gids = ProcessChunksInParallel(vertices);
    ASSERT_TRUE(std::equal(read_gids.begin(), read_gids.end(), labeled_vertex_gids.begin()));
  }

  // Same 200 vertex at the end
  {
    const auto last_gid = labeled_vertex_gids.back();
    for (int i = 0; i < 200; ++i) {
      auto acc = storage_->Access();
      auto vertex = acc->FindVertex(last_gid, View::OLD);
      ASSERT_TRUE(vertex);
      ASSERT_TRUE(vertex->RemoveLabel(label_id_).has_value());
      ASSERT_TRUE(vertex->AddLabel(label_id_).has_value());
      ASSERT_TRUE(acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()).has_value());
    }
    auto acc = storage_->Access();
    auto vertices = acc->ChunkedVertices(label_id_, View::OLD, 4);
    // NOTE This is an edge case where we need to skip whole chunks because of duplicates in the index.
    // ASSERT_EQ(vertices.size(), 4);
    auto read_gids = ProcessChunksInParallel(vertices);
    ASSERT_TRUE(std::equal(read_gids.begin(), read_gids.end(), labeled_vertex_gids.begin()));
  }
}

// ============================================================================
// LABEL PROPERTY INDEX TESTS
// ============================================================================

TEST_F(StorageV2ChunkIteratorTest, LabelPropertyIndexChunkIteratorBasic) {
  // Create vertices with specific label and property
  std::vector<Gid> vertex_gids;
  std::vector<Gid> labeled_property_vertex_gids;

  // Create 200 vertices with the test label and property
  {
    auto acc = storage_->Access();
    for (int i = 0; i < 200; ++i) {
      auto vertex = acc->CreateVertex();
      if (i % 3 == 0) {
        ASSERT_TRUE(vertex.AddLabel(label_id_).has_value());
        ASSERT_TRUE(vertex.SetProperty(property_id_, PropertyValue(i)).has_value());
        labeled_property_vertex_gids.push_back(vertex.Gid());
      }
      vertex_gids.push_back(vertex.Gid());
    }

    ASSERT_TRUE(acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()).has_value());
  }

  // Test chunking for labeled vertices with property
  auto acc = storage_->Access();
  std::vector<PropertyPath> properties = {{property_id_}};
  std::vector<PropertyValueRange> property_ranges = {};
  auto vertices = acc->ChunkedVertices(label_id_, properties, property_ranges, View::OLD, 4);

  ASSERT_GT(vertices.size(), 0);

  // Count total vertices across all chunks
  std::atomic<int> total_count{0};
  auto read_gids = ProcessChunksInParallel(
      vertices, [](const auto &) {},  // No additional per-item processing
      [&total_count](int local_count) {
        ASSERT_GT(local_count, 0);
        ASSERT_LT(local_count, 67 / 4 * 2);
        total_count.fetch_add(local_count, std::memory_order_relaxed);
      });

  ASSERT_TRUE(std::equal(read_gids.begin(), read_gids.end(), labeled_property_vertex_gids.begin()));
}

TEST_F(StorageV2ChunkIteratorTest, LabelPropertyIndexChunkIteratorMultipleEntries) {
  // Create vertices with specific label and property
  std::vector<Gid> vertex_gids;
  std::vector<Gid> labeled_property_vertex_gids;

  // Create 200 vertices with the test label and property
  {
    auto acc = storage_->Access();
    for (int i = 0; i < 200; ++i) {
      auto vertex = acc->CreateVertex();
      if (i % 3 == 0) {
        ASSERT_TRUE(vertex.AddLabel(label_id_).has_value());
        ASSERT_TRUE(vertex.SetProperty(property_id_, PropertyValue(i)).has_value());
        labeled_property_vertex_gids.push_back(vertex.Gid());
      }
      vertex_gids.push_back(vertex.Gid());
    }

    ASSERT_TRUE(acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()).has_value());
  }

  // Modify labeled vertices to have multiple entries
  {
    auto acc = storage_->Access();
    for (const auto gid : labeled_property_vertex_gids) {
      auto vertex = acc->FindVertex(gid, View::OLD);
      ASSERT_TRUE(vertex);
      ASSERT_TRUE(vertex->RemoveLabel(label_id_).has_value());
    }
    ASSERT_TRUE(acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()).has_value());
  }
  {
    auto acc = storage_->Access();
    for (const auto gid : labeled_property_vertex_gids) {
      auto vertex = acc->FindVertex(gid, View::OLD);
      ASSERT_TRUE(vertex);
      ASSERT_TRUE(vertex->AddLabel(label_id_).has_value());
    }
    ASSERT_TRUE(acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()).has_value());
  }

  // Test chunking for labeled vertices with property
  auto acc = storage_->Access();
  std::vector<PropertyPath> properties = {{property_id_}};
  std::vector<PropertyValueRange> property_ranges = {};
  auto vertices = acc->ChunkedVertices(label_id_, properties, property_ranges, View::OLD, 4);

  ASSERT_GT(vertices.size(), 0);

  // Count total vertices across all chunks
  std::atomic<int> total_count{0};
  auto read_gids = ProcessChunksInParallel(
      vertices, [](const auto &) {},  // No additional per-item processing
      [&total_count](int local_count) {
        ASSERT_GT(local_count, 0);
        ASSERT_LT(local_count, 67 / 4 * 2);
        total_count.fetch_add(local_count, std::memory_order_relaxed);
      });

  ASSERT_TRUE(std::equal(read_gids.begin(), read_gids.end(), labeled_property_vertex_gids.begin()));
}

TEST_F(StorageV2ChunkIteratorTest, LabelPropertyIndexChunkIteratorDynamic) {
  // Create vertices with specific label and property
  std::vector<Gid> vertex_gids;
  std::vector<Gid> labeled_property_vertex_gids;

  // Create 200 vertices with the test label and property
  {
    auto acc = storage_->Access();
    for (int i = 0; i < 200; ++i) {
      auto vertex = acc->CreateVertex();
      if (i % 3 == 0) {
        ASSERT_TRUE(vertex.AddLabel(label_id_).has_value());
        ASSERT_TRUE(vertex.SetProperty(property_id_, PropertyValue(i)).has_value());
        labeled_property_vertex_gids.push_back(vertex.Gid());
      }
      vertex_gids.push_back(vertex.Gid());
    }

    ASSERT_TRUE(acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()).has_value());
  }

  // Test chunking for labeled vertices with property
  auto acc = storage_->Access();  // Get access before starting the modify thread

  std::atomic_bool run = true;
  auto modify_thread = std::jthread([&]() {
    while (run) {
      int i = 0;
      auto acc = storage_->Access();
      for (const auto gid : vertex_gids) {
        auto vertex = acc->FindVertex(gid, View::OLD);
        if (!vertex) continue;
        ++i;
        if (i % 2 == 0) {
          ASSERT_TRUE(vertex->RemoveLabel(label_id_).has_value());
        } else if (i % 3 == 0) {
          ASSERT_TRUE(vertex->AddLabel(label_id_).has_value());
          ASSERT_TRUE(vertex->SetProperty(property_id_, PropertyValue(i)).has_value());
        } else if (i % 5 == 0) {
          ASSERT_TRUE(acc->DeleteVertex(&*vertex).has_value());
        }
      }
      ASSERT_TRUE(acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()).has_value());
      storage_->FreeMemory();  // Run storage and skiplist gc
      std::this_thread::sleep_for(std::chrono::microseconds(100));
    }
  });

  std::this_thread::sleep_for(std::chrono::milliseconds(1));  // Wait for the modify thread to start
  std::vector<PropertyPath> properties = {{property_id_}};
  std::vector<PropertyValueRange> property_ranges = {};
  auto vertices = acc->ChunkedVertices(label_id_, properties, property_ranges, View::OLD, 4);

  ASSERT_GT(vertices.size(), 0);

  // Count total vertices across all chunks
  std::atomic<int> total_count{0};
  auto read_gids = ProcessChunksInParallel(
      vertices, [](const auto &) {},  // No additional per-item processing
      [&total_count](int local_count) {
        ASSERT_GT(local_count, 0);
        ASSERT_LT(local_count, 67 / 4 * 2);
        total_count.fetch_add(local_count, std::memory_order_relaxed);
      });

  run.store(false);
  modify_thread.join();

  ASSERT_TRUE(std::equal(read_gids.begin(), read_gids.end(), labeled_property_vertex_gids.begin()));
}

TEST_F(StorageV2ChunkIteratorTest, LabelPropertyIndexChunkIteratorManyEntries) {
  // Create vertices with specific label and property
  std::vector<Gid> vertex_gids;
  std::vector<Gid> labeled_property_vertex_gids;

  // Create 200 vertices with the test label and property
  {
    auto acc = storage_->Access();
    for (int i = 0; i < 200; ++i) {
      auto vertex = acc->CreateVertex();
      if (i % 3 == 0) {
        ASSERT_TRUE(vertex.AddLabel(label_id_).has_value());
        ASSERT_TRUE(vertex.SetProperty(property_id_, PropertyValue(i)).has_value());
        labeled_property_vertex_gids.push_back(vertex.Gid());
      }
      vertex_gids.push_back(vertex.Gid());
    }

    ASSERT_TRUE(acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()).has_value());
  }

  // Modify labeled vertices to have multiple entries
  auto modified_gid = labeled_property_vertex_gids[50];
  for (int i = 0; i < 100; ++i) {
    {
      auto acc = storage_->Access();
      auto vertex = acc->FindVertex(modified_gid, View::OLD);
      ASSERT_TRUE(vertex);
      ASSERT_TRUE(vertex->SetProperty(property_id_, PropertyValue{}).has_value());
      ASSERT_TRUE(acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()).has_value());
    }
    {
      auto acc = storage_->Access();
      auto vertex = acc->FindVertex(modified_gid, View::OLD);
      ASSERT_TRUE(vertex);
      ASSERT_TRUE(vertex->SetProperty(property_id_, PropertyValue{i}).has_value());
      ASSERT_TRUE(acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()).has_value());
    }
  }
  // Test chunking for labeled vertices with property
  auto acc = storage_->Access();
  std::vector<PropertyPath> properties = {{property_id_}};
  std::vector<PropertyValueRange> property_ranges = {};
  auto vertices = acc->ChunkedVertices(label_id_, properties, property_ranges, View::OLD, 4);

  ASSERT_GT(vertices.size(), 0);

  // Count total vertices across all chunks
  std::atomic<int> total_count{0};
  auto read_gids = ProcessChunksInParallel(
      vertices, [](const auto &) {},  // No additional per-item processing
      [&total_count](int local_count) {
        // NOTE: We don't assert on the count here because we don't know how many same vertices are in the chunk.
        // ASSERT_GT(local_count, 0);
        // ASSERT_LT(local_count, 67 / 4 * 2);
        total_count.fetch_add(local_count, std::memory_order_relaxed);
      });

  ASSERT_TRUE(std::equal(read_gids.begin(), read_gids.end(), labeled_property_vertex_gids.begin()));
}

TEST_F(StorageV2ChunkIteratorTest, LabelPropertyIndexChunkIteratorEdgeCases) {
  // Create vertices with specific label and property
  std::vector<Gid> vertex_gids;
  std::vector<Gid> labeled_property_vertex_gids;

  // Empty index
  {
    // Test chunking for labeled vertices with property
    auto acc = storage_->Access();
    std::vector<PropertyPath> properties = {{property_id_}};
    std::vector<PropertyValueRange> property_ranges = {};
    auto vertices = acc->ChunkedVertices(label_id_, properties, property_ranges, View::OLD, 4);
    ASSERT_EQ(vertices.size(), 1);
    auto chunk = vertices.get_chunk(0);
    ASSERT_TRUE(chunk.begin() == chunk.end());
  }

  // Non existing label (Cannot be tested because we just assert at the index level)

  // Fewer vertices than chunks
  {
    auto acc = storage_->Access();
    for (int i = 0; i < 2; ++i) {
      auto vertex = acc->CreateVertex();
      ASSERT_TRUE(vertex.AddLabel(label_id_).has_value());
      ASSERT_TRUE(vertex.SetProperty(property_id_, PropertyValue(i)).has_value());
      labeled_property_vertex_gids.push_back(vertex.Gid());
    }
    ASSERT_TRUE(acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()).has_value());
    std::vector<PropertyPath> properties = {{property_id_}};
    std::vector<PropertyValueRange> property_ranges = {};
    auto vertices = acc->ChunkedVertices(label_id_, properties, property_ranges, View::OLD, 4);
    ASSERT_EQ(vertices.size(), 2);
  }

  // Exact number of vertices and chunks
  {
    auto acc = storage_->Access();
    for (int i = 0; i < 2 * 3; ++i) {
      auto vertex = acc->CreateVertex();
      if (i % 3 == 0) {
        ASSERT_TRUE(vertex.AddLabel(label_id_).has_value());
        ASSERT_TRUE(vertex.SetProperty(property_id_, PropertyValue(i)).has_value());
        labeled_property_vertex_gids.push_back(vertex.Gid());
      }
      vertex_gids.push_back(vertex.Gid());
    }
    ASSERT_EQ(labeled_property_vertex_gids.size(), 4);
    ASSERT_TRUE(acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()).has_value());
  }
  {
    auto acc = storage_->Access();
    std::vector<PropertyPath> properties = {{property_id_}};
    std::vector<PropertyValueRange> property_ranges = {};
    auto vertices = acc->ChunkedVertices(label_id_, properties, property_ranges, View::OLD, 4);
    ASSERT_EQ(vertices.size(), 4);
    auto read_gids = ProcessChunksInParallel(vertices);
    ASSERT_EQ(read_gids.size(), labeled_property_vertex_gids.size());
    std::sort(read_gids.begin(), read_gids.end());
    for (size_t i = 0; i < read_gids.size(); ++i) {
      ASSERT_EQ(read_gids[i], labeled_property_vertex_gids[i]);
    }
  }

  // Same 100 vertex at the beginning
  {
    const auto first_gid = labeled_property_vertex_gids[0];
    for (int i = 0; i < 100; ++i) {
      {
        auto acc = storage_->Access();
        auto vertex = acc->FindVertex(first_gid, View::OLD);
        ASSERT_TRUE(vertex);
        ASSERT_TRUE(vertex->RemoveLabel(label_id_).has_value());
        ASSERT_TRUE(vertex->AddLabel(label_id_).has_value());
        ASSERT_TRUE(acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()).has_value());
      }
      {
        auto acc = storage_->Access();
        auto vertex = acc->FindVertex(first_gid, View::OLD);
        ASSERT_TRUE(vertex);
        ASSERT_TRUE(vertex->SetProperty(property_id_, PropertyValue{}).has_value());
        ASSERT_TRUE(vertex->SetProperty(property_id_, PropertyValue{i}).has_value());
        ASSERT_TRUE(acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()).has_value());
      }
    }
    auto acc = storage_->Access();
    std::vector<PropertyPath> properties = {{property_id_}};
    std::vector<PropertyValueRange> property_ranges = {};
    auto vertices = acc->ChunkedVertices(label_id_, properties, property_ranges, View::OLD, 4);
    // NOTE This is an edge case where we need to skip whole chunks because of duplicates in the index.
    // ASSERT_EQ(vertices.size(), 4);
    auto read_gids = ProcessChunksInParallel(vertices);
    ASSERT_TRUE(std::equal(read_gids.begin(), read_gids.end(), labeled_property_vertex_gids.begin()));
  }

  // Same 200 vertex at the end
  {
    const auto last_gid = labeled_property_vertex_gids.back();
    for (int i = 0; i < 200; ++i) {
      {
        auto acc = storage_->Access();
        auto vertex = acc->FindVertex(last_gid, View::OLD);
        ASSERT_TRUE(vertex);
        ASSERT_TRUE(vertex->RemoveLabel(label_id_).has_value());
        ASSERT_TRUE(vertex->AddLabel(label_id_).has_value());
        ASSERT_TRUE(acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()).has_value());
      }
      {
        auto acc = storage_->Access();
        auto vertex = acc->FindVertex(last_gid, View::OLD);
        ASSERT_TRUE(vertex);
        ASSERT_TRUE(vertex->SetProperty(property_id_, PropertyValue{}).has_value());
        ASSERT_TRUE(vertex->SetProperty(property_id_, PropertyValue{i}).has_value());
        ASSERT_TRUE(acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()).has_value());
      }
    }
    auto acc = storage_->Access();
    std::vector<PropertyPath> properties = {{property_id_}};
    std::vector<PropertyValueRange> property_ranges = {};
    auto vertices = acc->ChunkedVertices(label_id_, properties, property_ranges, View::OLD, 4);
    // NOTE This is an edge case where we need to skip whole chunks because of duplicates in the index.
    // ASSERT_EQ(vertices.size(), 4);
    auto read_gids = ProcessChunksInParallel(vertices);
    ASSERT_TRUE(std::equal(read_gids.begin(), read_gids.end(), labeled_property_vertex_gids.begin()));
  }
}

TEST_F(StorageV2ChunkIteratorTest, LabelPropertyIndexChunkIteratorBasicRange) {
  // Create vertices with specific label and property
  std::vector<Gid> vertex_gids;
  std::vector<Gid> labeled_property_vertex_gids;

  // Create 200 vertices with the test label and property
  {
    auto acc = storage_->Access();
    for (int i = 0; i < 200; ++i) {
      auto vertex = acc->CreateVertex();
      if (i % 3 == 0) {
        ASSERT_TRUE(vertex.AddLabel(label_id_).has_value());
        ASSERT_TRUE(vertex.SetProperty(property_id_, PropertyValue(i)).has_value());
        labeled_property_vertex_gids.push_back(vertex.Gid());
      }
      vertex_gids.push_back(vertex.Gid());
    }

    ASSERT_TRUE(acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()).has_value());
  }

  // Checking lower bound
  {
    auto acc = storage_->Access();
    std::vector<PropertyPath> properties = {{property_id_}};
    std::vector<PropertyValueRange> property_ranges = {
        PropertyValueRange::Bounded(memgraph::utils::MakeBoundExclusive(PropertyValue(131)), std::nullopt)};
    auto vertices = acc->ChunkedVertices(label_id_, properties, property_ranges, View::OLD, 4);
    ASSERT_GT(vertices.size(), 0);
    auto first_chunk = vertices.get_chunk(0);
    ASSERT_EQ((*first_chunk.begin()).GetProperty(property_id_, View::OLD).value().ValueInt(), 132);
    auto last_chunk = vertices.get_chunk(vertices.size() - 1);
    PropertyValue last_pv;
    for (auto it = last_chunk.begin(); it != last_chunk.end(); ++it) {
      last_pv = (*it).GetProperty(property_id_, View::OLD).value();
    }
    ASSERT_EQ(last_pv.ValueInt(), 198);
  }
  {
    auto acc = storage_->Access();
    std::vector<PropertyPath> properties = {{property_id_}};
    std::vector<PropertyValueRange> property_ranges = {
        PropertyValueRange::Bounded(memgraph::utils::MakeBoundExclusive(PropertyValue(132)), std::nullopt)};
    auto vertices = acc->ChunkedVertices(label_id_, properties, property_ranges, View::OLD, 4);
    ASSERT_GT(vertices.size(), 0);
    auto first_chunk = vertices.get_chunk(0);
    ASSERT_EQ((*first_chunk.begin()).GetProperty(property_id_, View::OLD).value().ValueInt(), 135);
    auto last_chunk = vertices.get_chunk(vertices.size() - 1);
    PropertyValue last_pv;
    for (auto it = last_chunk.begin(); it != last_chunk.end(); ++it) {
      last_pv = (*it).GetProperty(property_id_, View::OLD).value();
    }
    ASSERT_EQ(last_pv.ValueInt(), 198);
  }
  {
    auto acc = storage_->Access();
    std::vector<PropertyPath> properties = {{property_id_}};
    std::vector<PropertyValueRange> property_ranges = {
        PropertyValueRange::Bounded(memgraph::utils::MakeBoundInclusive(PropertyValue(132)), std::nullopt)};
    auto vertices = acc->ChunkedVertices(label_id_, properties, property_ranges, View::OLD, 4);
    ASSERT_GT(vertices.size(), 0);
    auto first_chunk = vertices.get_chunk(0);
    ASSERT_EQ((*first_chunk.begin()).GetProperty(property_id_, View::OLD).value().ValueInt(), 132);
    auto last_chunk = vertices.get_chunk(vertices.size() - 1);
    PropertyValue last_pv;
    for (auto it = last_chunk.begin(); it != last_chunk.end(); ++it) {
      last_pv = (*it).GetProperty(property_id_, View::OLD).value();
    }
    ASSERT_EQ(last_pv.ValueInt(), 198);
  }
  {
    auto acc = storage_->Access();
    std::vector<PropertyPath> properties = {{property_id_}};
    std::vector<PropertyValueRange> property_ranges = {
        PropertyValueRange::Bounded(memgraph::utils::MakeBoundInclusive(PropertyValue(-1)), std::nullopt)};
    auto vertices = acc->ChunkedVertices(label_id_, properties, property_ranges, View::OLD, 4);
    ASSERT_EQ(vertices.size(), 4);
    auto first_chunk = vertices.get_chunk(0);
    ASSERT_EQ((*first_chunk.begin()).GetProperty(property_id_, View::OLD).value().ValueInt(), 0);
    auto last_chunk = vertices.get_chunk(vertices.size() - 1);
    PropertyValue last_pv;
    for (auto it = last_chunk.begin(); it != last_chunk.end(); ++it) {
      last_pv = (*it).GetProperty(property_id_, View::OLD).value();
    }
    ASSERT_EQ(last_pv.ValueInt(), 198);
  }
  {
    auto acc = storage_->Access();
    std::vector<PropertyPath> properties = {{property_id_}};
    std::vector<PropertyValueRange> property_ranges = {
        PropertyValueRange::Bounded(memgraph::utils::MakeBoundInclusive(PropertyValue(210)), std::nullopt)};
    auto vertices = acc->ChunkedVertices(label_id_, properties, property_ranges, View::OLD, 4);
    ASSERT_EQ(vertices.size(), 1);
    auto chunk = vertices.get_chunk(0);
    ASSERT_TRUE(chunk.begin() == chunk.end());
  }

  // Checking upper bound
  {
    auto acc = storage_->Access();
    std::vector<PropertyPath> properties = {{property_id_}};
    std::vector<PropertyValueRange> property_ranges = {
        PropertyValueRange::Bounded(std::nullopt, memgraph::utils::MakeBoundExclusive(PropertyValue(143)))};
    auto vertices = acc->ChunkedVertices(label_id_, properties, property_ranges, View::OLD, 4);
    ASSERT_EQ(vertices.size(), 4);
    auto first_chunk = vertices.get_chunk(0);
    ASSERT_EQ((*first_chunk.begin()).GetProperty(property_id_, View::OLD).value().ValueInt(), 0);
    auto last_chunk = vertices.get_chunk(vertices.size() - 1);
    PropertyValue last_pv;
    for (auto it = last_chunk.begin(); it != last_chunk.end(); ++it) {
      last_pv = (*it).GetProperty(property_id_, View::OLD).value();
    }
    ASSERT_EQ(last_pv.ValueInt(), 141);
  }
  {
    auto acc = storage_->Access();
    std::vector<PropertyPath> properties = {{property_id_}};
    std::vector<PropertyValueRange> property_ranges = {
        PropertyValueRange::Bounded(std::nullopt, memgraph::utils::MakeBoundExclusive(PropertyValue(141)))};
    auto vertices = acc->ChunkedVertices(label_id_, properties, property_ranges, View::OLD, 4);
    ASSERT_GT(vertices.size(), 0);
    auto last_chunk = vertices.get_chunk(vertices.size() - 1);
    PropertyValue last_pv;
    for (auto it = last_chunk.begin(); it != last_chunk.end(); ++it) {
      last_pv = (*it).GetProperty(property_id_, View::OLD).value();
    }
    ASSERT_EQ(last_pv.ValueInt(), 138);
  }
  {
    auto acc = storage_->Access();
    std::vector<PropertyPath> properties = {{property_id_}};
    std::vector<PropertyValueRange> property_ranges = {
        PropertyValueRange::Bounded(std::nullopt, memgraph::utils::MakeBoundInclusive(PropertyValue(141)))};
    auto vertices = acc->ChunkedVertices(label_id_, properties, property_ranges, View::OLD, 4);
    ASSERT_GT(vertices.size(), 0);
    auto last_chunk = vertices.get_chunk(vertices.size() - 1);
    PropertyValue last_pv;
    for (auto it = last_chunk.begin(); it != last_chunk.end(); ++it) {
      last_pv = (*it).GetProperty(property_id_, View::OLD).value();
    }
    ASSERT_EQ(last_pv.ValueInt(), 141);
  }
  {
    auto acc = storage_->Access();
    std::vector<PropertyPath> properties = {{property_id_}};
    std::vector<PropertyValueRange> property_ranges = {
        PropertyValueRange::Bounded(std::nullopt, memgraph::utils::MakeBoundInclusive(PropertyValue(210)))};
    auto vertices = acc->ChunkedVertices(label_id_, properties, property_ranges, View::OLD, 4);
    ASSERT_EQ(vertices.size(), 4);
    auto first_chunk = vertices.get_chunk(0);
    ASSERT_EQ((*first_chunk.begin()).GetProperty(property_id_, View::OLD).value().ValueInt(), 0);
    auto last_chunk = vertices.get_chunk(vertices.size() - 1);
    PropertyValue last_pv;
    for (auto it = last_chunk.begin(); it != last_chunk.end(); ++it) {
      last_pv = (*it).GetProperty(property_id_, View::OLD).value();
    }
    ASSERT_EQ(last_pv.ValueInt(), 198);
  }
  {
    auto acc = storage_->Access();
    std::vector<PropertyPath> properties = {{property_id_}};
    std::vector<PropertyValueRange> property_ranges = {
        PropertyValueRange::Bounded(std::nullopt, memgraph::utils::MakeBoundInclusive(PropertyValue(-10)))};
    auto vertices = acc->ChunkedVertices(label_id_, properties, property_ranges, View::OLD, 4);
    ASSERT_EQ(vertices.size(), 1);
    auto chunk = vertices.get_chunk(0);
    ASSERT_TRUE(chunk.begin() == chunk.end());
  }

  // Test chunking for labeled vertices with property range
  auto acc = storage_->Access();
  std::vector<PropertyPath> properties = {{property_id_}};
  std::vector<PropertyValueRange> property_ranges = {PropertyValueRange::Bounded(
      memgraph::utils::MakeBoundExclusive(PropertyValue(10)), memgraph::utils::MakeBoundExclusive(PropertyValue(160)))};
  auto vertices = acc->ChunkedVertices(label_id_, properties, property_ranges, View::OLD, 4);

  ASSERT_GT(vertices.size(), 0);

  // Count total vertices across all chunks using ProcessChunksInParallel
  auto read_props_result = ProcessChunksInParallelForPropertyValues(
      vertices,
      [this](const auto &vertex, std::vector<PropertyValue> &props) {
        const auto prop = vertex.GetProperty(property_id_, View::OLD);
        ASSERT_TRUE(prop.has_value());
        ASSERT_TRUE(prop->IsInt());
        props.push_back(prop.value());
      },
      [](int local_count) {
        ASSERT_GT(local_count, 0);
        ASSERT_LT(local_count, 50 / 4 * 2);
      });

  {
    ASSERT_EQ(read_props_result.size(), 50);  // Every 3rd has the label+property
    for (int i = 0; i < read_props_result.size(); ++i) {
      ASSERT_EQ(read_props_result[i],
                PropertyValue{i * 3 + 12});  // i*3 + 12 just because of the way we create the dataset
    }
  }

  // Edges cases where both bounds are set
  {
    // Non existing value
    auto acc = storage_->Access();
    std::vector<PropertyPath> properties = {{property_id_}};
    std::vector<PropertyValueRange> property_ranges = {
        PropertyValueRange::Bounded(memgraph::utils::MakeBoundInclusive(PropertyValue(130)),
                                    memgraph::utils::MakeBoundInclusive(PropertyValue(130)))};
    auto vertices = acc->ChunkedVertices(label_id_, properties, property_ranges, View::OLD, 4);
    ASSERT_EQ(vertices.size(), 1);
    auto chunk = vertices.get_chunk(0);
    ASSERT_TRUE(chunk.begin() == chunk.end());
  }
  {
    // Exact match
    auto acc = storage_->Access();
    std::vector<PropertyPath> properties = {{property_id_}};
    std::vector<PropertyValueRange> property_ranges = {
        PropertyValueRange::Bounded(memgraph::utils::MakeBoundInclusive(PropertyValue(132)),
                                    memgraph::utils::MakeBoundInclusive(PropertyValue(132)))};
    auto vertices = acc->ChunkedVertices(label_id_, properties, property_ranges, View::OLD, 4);
    ASSERT_EQ(vertices.size(), 1);
    auto chunk = vertices.get_chunk(0);
    auto it = chunk.begin();
    ASSERT_TRUE(it != chunk.end());
    ASSERT_EQ((*it).GetProperty(property_id_, View::OLD).value().ValueInt(), 132);
    ++it;
    ASSERT_TRUE(it == chunk.end());
  }
  {
    // Missed match
    auto acc = storage_->Access();
    std::vector<PropertyPath> properties = {{property_id_}};
    std::vector<PropertyValueRange> property_ranges = {
        PropertyValueRange::Bounded(memgraph::utils::MakeBoundInclusive(PropertyValue(132)),
                                    memgraph::utils::MakeBoundExclusive(PropertyValue(132)))};
    auto vertices = acc->ChunkedVertices(label_id_, properties, property_ranges, View::OLD, 4);
    ASSERT_EQ(vertices.size(), 1);
    auto chunk = vertices.get_chunk(0);
    ASSERT_TRUE(chunk.begin() == chunk.end());
  }
  {
    // One match
    auto acc = storage_->Access();
    std::vector<PropertyPath> properties = {{property_id_}};
    std::vector<PropertyValueRange> property_ranges = {
        PropertyValueRange::Bounded(memgraph::utils::MakeBoundInclusive(PropertyValue(131)),
                                    memgraph::utils::MakeBoundInclusive(PropertyValue(133)))};
    auto vertices = acc->ChunkedVertices(label_id_, properties, property_ranges, View::OLD, 4);
    ASSERT_EQ(vertices.size(), 1);
    auto chunk = vertices.get_chunk(0);
    auto it = chunk.begin();
    ASSERT_TRUE(it != chunk.end());
    ASSERT_EQ((*it).GetProperty(property_id_, View::OLD).value().ValueInt(), 132);
    ++it;
    ASSERT_TRUE(it == chunk.end());
  }
  {
    // Upper bound lower than lower bound
    auto acc = storage_->Access();
    std::vector<PropertyPath> properties = {{property_id_}};
    std::vector<PropertyValueRange> property_ranges = {
        PropertyValueRange::Bounded(memgraph::utils::MakeBoundInclusive(PropertyValue(130)),
                                    memgraph::utils::MakeBoundInclusive(PropertyValue(120)))};
    auto vertices = acc->ChunkedVertices(label_id_, properties, property_ranges, View::OLD, 4);
    ASSERT_EQ(vertices.size(), 1);
    auto chunk = vertices.get_chunk(0);
    ASSERT_TRUE(chunk.begin() == chunk.end());
  }
}

// ============================================================================
// EDGE PROPERTY INDEX TESTS
// ============================================================================

TEST_F(StorageV2ChunkIteratorTest, BasicEdgePropertyIndexChunking) {
  // Create edges with different property values
  std::vector<Gid> edge_gids;
  {
    auto acc = storage_->Access();
    // Create 100 edges with property values from 0 to 99
    for (int i = 0; i < 100; ++i) {
      auto vertex_from = acc->CreateVertex();
      auto vertex_to = acc->CreateVertex();
      auto edge = acc->CreateEdge(&vertex_from, &vertex_to, edge_type_id_);
      ASSERT_TRUE(edge->SetProperty(property_id_, PropertyValue(i)).has_value());
      edge_gids.push_back(edge->Gid());
    }

    ASSERT_TRUE(acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()).has_value());
  }

  // Test chunking for edges with property
  auto acc = storage_->Access();
  auto edges = acc->ChunkedEdges(property_id_, View::OLD, 4);

  ASSERT_GT(edges.size(), 0);

  // Count total edges across all chunks
  auto read_gids = ProcessChunksInParallel(
      edges, [](const auto &) {},  // No additional per-item processing
      [](int local_count) {
        EXPECT_GT(local_count, 0);
        EXPECT_LT(local_count, 100 / 4 * 2);
      });

  ASSERT_TRUE(std::equal(read_gids.begin(), read_gids.end(), edge_gids.begin()));
}

TEST_F(StorageV2ChunkIteratorTest, EdgePropertyIndexChunkingWithRange) {
  // Create edges with property values in a specific range
  std::vector<Gid> edge_gids;

  {
    auto acc = storage_->Access();
    // Create 200 edges with property values from 0 to 199
    for (int i = 0; i < 200; ++i) {
      auto vertex_from = acc->CreateVertex();
      auto vertex_to = acc->CreateVertex();
      auto edge = acc->CreateEdge(&vertex_from, &vertex_to, edge_type_id_);
      ASSERT_TRUE(edge->SetProperty(property_id_, PropertyValue(i)).has_value());
      edge_gids.push_back(edge->Gid());
    }

    ASSERT_TRUE(acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()).has_value());
  }

  auto acc = storage_->Access();
  // Test chunking for edges with property in range [50, 150]
  auto edges = acc->ChunkedEdges(property_id_, memgraph::utils::MakeBoundInclusive(PropertyValue(50)),
                                 memgraph::utils::MakeBoundInclusive(PropertyValue(150)), View::OLD, 4);

  ASSERT_GT(edges.size(), 0);

  // Count total edges across all chunks
  auto read_gids = ProcessChunksInParallel(
      edges,
      [this](const auto &edge) {
        // Verify property value is in range
        auto property_value = edge.GetProperty(this->property_id_, View::OLD);
        ASSERT_TRUE(property_value.has_value());
        ASSERT_GE(property_value->ValueInt(), 50);
        ASSERT_LE(property_value->ValueInt(), 150);
      },
      [](int local_count) {
        EXPECT_GT(local_count, 0);
        EXPECT_LT(local_count, 101 / 4 * 2);
      });

  ASSERT_TRUE(std::equal(read_gids.begin(), read_gids.end(), edge_gids.begin() + 50, edge_gids.begin() + 151));
}

TEST_F(StorageV2ChunkIteratorTest, EdgePropertyIndexChunkingWithLowerBound) {
  // Create edges with property values
  std::vector<Gid> edge_gids;
  {
    auto acc = storage_->Access();
    // Create 100 edges with property values from 0 to 99
    for (int i = 0; i < 100; ++i) {
      auto vertex_from = acc->CreateVertex();
      auto vertex_to = acc->CreateVertex();
      auto edge = acc->CreateEdge(&vertex_from, &vertex_to, edge_type_id_);
      ASSERT_TRUE(edge->SetProperty(property_id_, PropertyValue(i)).has_value());
      edge_gids.push_back(edge->Gid());
    }

    ASSERT_TRUE(acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()).has_value());
  }

  auto acc = storage_->Access();
  // Test chunking for edges with property >= 30
  auto edges = acc->ChunkedEdges(property_id_, memgraph::utils::MakeBoundInclusive(PropertyValue(30)), std::nullopt,
                                 View::OLD, 4);

  ASSERT_GT(edges.size(), 0);

  // Count total edges across all chunks
  std::atomic<int> total_count{0};
  auto read_gids = ProcessChunksInParallel(
      edges,
      [this](const auto &edge) {
        // Verify property value is >= 30
        auto property_value = edge.GetProperty(this->property_id_, View::OLD);
        ASSERT_TRUE(property_value.has_value());
        ASSERT_GE(property_value->ValueInt(), 30);
      },
      [&total_count](int local_count) {
        EXPECT_GT(local_count, 0);
        EXPECT_LT(local_count, 70 / 4 * 2);
        total_count.fetch_add(local_count, std::memory_order_relaxed);
      });

  ASSERT_TRUE(std::equal(read_gids.begin(), read_gids.end(), edge_gids.begin() + 30));
}

TEST_F(StorageV2ChunkIteratorTest, EdgePropertyIndexChunkingWithUpperBound) {
  // Create edges with property values
  std::vector<Gid> edge_gids;
  {
    auto acc = storage_->Access();
    // Create 100 edges with property values from 0 to 99
    for (int i = 0; i < 100; ++i) {
      auto vertex_from = acc->CreateVertex();
      auto vertex_to = acc->CreateVertex();
      auto edge = acc->CreateEdge(&vertex_from, &vertex_to, edge_type_id_);
      ASSERT_TRUE(edge->SetProperty(property_id_, PropertyValue(i)).has_value());
      edge_gids.push_back(edge->Gid());
    }

    ASSERT_TRUE(acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()).has_value());
  }
  auto acc = storage_->Access();
  // Test chunking for edges with property <= 70
  auto edges = acc->ChunkedEdges(property_id_, std::nullopt, memgraph::utils::MakeBoundInclusive(PropertyValue(70)),
                                 View::OLD, 4);

  ASSERT_GT(edges.size(), 0);

  // Count total edges across all chunks
  std::atomic<int> total_count{0};
  auto read_gids = ProcessChunksInParallel(
      edges,
      [this](const auto &edge) {
        // Verify property value is <= 70
        auto property_value = edge.GetProperty(this->property_id_, View::OLD);
        ASSERT_TRUE(property_value.has_value());
        ASSERT_LE(property_value->ValueInt(), 70);
      },
      [&total_count](int local_count) {
        EXPECT_GT(local_count, 0);
        EXPECT_LT(local_count, 71 / 4 * 2);
        total_count.fetch_add(local_count, std::memory_order_relaxed);
      });

  ASSERT_TRUE(std::equal(read_gids.begin(), read_gids.end(), edge_gids.begin(), edge_gids.begin() + 71));
}

TEST_F(StorageV2ChunkIteratorTest, EdgePropertyIndexChunkingEdgeCases) {
  // Create edges with property values
  std::vector<Gid> edge_gids;
  {
    auto acc = storage_->Access();
    // Create 50 edges with property values from 0 to 49
    for (int i = 0; i < 50; ++i) {
      auto vertex_from = acc->CreateVertex();
      auto vertex_to = acc->CreateVertex();
      auto edge = acc->CreateEdge(&vertex_from, &vertex_to, edge_type_id_);
      ASSERT_TRUE(edge->SetProperty(property_id_, PropertyValue(i)).has_value());
      edge_gids.push_back(edge->Gid());
    }

    ASSERT_TRUE(acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()).has_value());
  }

  // Test 1: Empty range (lower > upper)
  {
    auto acc = storage_->Access();
    auto empty_edges = acc->ChunkedEdges(property_id_, memgraph::utils::MakeBoundInclusive(PropertyValue(100)),
                                         memgraph::utils::MakeBoundInclusive(PropertyValue(50)), View::OLD, 4);
    ASSERT_EQ(empty_edges.size(), 0);
  }

  // Test 2: Single element range
  {
    auto acc = storage_->Access();
    auto single_edges = acc->ChunkedEdges(property_id_, memgraph::utils::MakeBoundInclusive(PropertyValue(25)),
                                          memgraph::utils::MakeBoundInclusive(PropertyValue(25)), View::OLD, 4);
    ASSERT_GT(single_edges.size(), 0);

    int single_count = 0;
    for (size_t i = 0; i < single_edges.size(); ++i) {
      auto chunk = single_edges.get_chunk(i);
      for (auto it = chunk.begin(); it != chunk.end(); ++it) {
        single_count++;
        auto property_value = (*it).GetProperty(property_id_, View::OLD);
        ASSERT_TRUE(property_value.has_value());
        ASSERT_EQ(property_value->ValueInt(), 25);
      }
    }
    ASSERT_EQ(single_count, 1);
  }
  // Test 3: Range with no elements (outside existing range)
  {
    auto acc = storage_->Access();
    auto no_elements_edges = acc->ChunkedEdges(property_id_, memgraph::utils::MakeBoundInclusive(PropertyValue(100)),
                                               memgraph::utils::MakeBoundInclusive(PropertyValue(200)), View::OLD, 4);
    ASSERT_GT(no_elements_edges.size(), 0);

    int no_elements_count = 0;
    for (size_t i = 0; i < no_elements_edges.size(); ++i) {
      auto chunk = no_elements_edges.get_chunk(i);
      for (auto it = chunk.begin(); it != chunk.end(); ++it) {
        no_elements_count++;
      }
    }
    ASSERT_EQ(no_elements_count, 0);
  }
}

TEST_F(StorageV2ChunkIteratorTest, EdgePropertyIndexChunkingBigDataset) {
  // Create edges with property values
  std::vector<Gid> edge_gids;
  {
    auto acc = storage_->Access();
    // Create 100'000 edges with property values from 0 to 99'999
    for (int i = 0; i < 100'000; ++i) {
      auto vertex_from = acc->CreateVertex();
      auto vertex_to = acc->CreateVertex();
      auto edge = acc->CreateEdge(&vertex_from, &vertex_to, edge_type_id_);
      ASSERT_TRUE(edge->SetProperty(property_id_, PropertyValue(i)).has_value());
      edge_gids.push_back(edge->Gid());
    }
    ASSERT_TRUE(acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()).has_value());
  }

  auto acc = storage_->Access();
  // Test chunking for edges with property in range [200, 800]
  auto edges = acc->ChunkedEdges(property_id_, memgraph::utils::MakeBoundInclusive(PropertyValue(20'000)),
                                 memgraph::utils::MakeBoundInclusive(PropertyValue(80'000)), View::OLD, 8);

  ASSERT_GT(edges.size(), 0);

  // Count total edges across all chunks
  std::atomic<int> total_count{0};
  auto read_gids = ProcessChunksInParallel(
      edges,
      [this](const auto &edge) {
        // Verify property value is in range
        auto property_value = edge.GetProperty(this->property_id_, View::OLD);
        ASSERT_TRUE(property_value.has_value());
        ASSERT_GE(property_value->ValueInt(), 20'000);
        ASSERT_LE(property_value->ValueInt(), 80'000);
      },
      [&total_count](int local_count) {
        EXPECT_GT(local_count, 0);
        EXPECT_LT(local_count, 60'000 / 8 * 3);
        total_count.fetch_add(local_count, std::memory_order_relaxed);
      });

  ASSERT_TRUE(std::equal(read_gids.begin(), read_gids.end(), edge_gids.begin() + 20'000, edge_gids.begin() + 80'001));
}

TEST_F(StorageV2ChunkIteratorTest, EdgePropertyIndexChunkingConcurrentOperations) {
  // Create initial edges with property values
  std::vector<Gid> edge_gids;

  {
    auto acc = storage_->Access();
    // Create 500 edges with property values from 0 to 499
    for (int i = 0; i < 500; ++i) {
      auto vertex_from = acc->CreateVertex();
      auto vertex_to = acc->CreateVertex();
      auto edge = acc->CreateEdge(&vertex_from, &vertex_to, edge_type_id_);
      ASSERT_TRUE(edge->SetProperty(property_id_, PropertyValue(i)).has_value());
      edge_gids.push_back(edge->Gid());
    }

    ASSERT_TRUE(acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()).has_value());
  }

  auto acc = storage_->Access();
  // Test chunking for edges with property in range [100, 400]
  auto edges = acc->ChunkedEdges(property_id_, memgraph::utils::MakeBoundInclusive(PropertyValue(100)),
                                 memgraph::utils::MakeBoundInclusive(PropertyValue(400)), View::OLD, 6);

  ASSERT_GT(edges.size(), 0);

  // Start concurrent insertion threads
  std::atomic<bool> stop_insertions{false};
  std::atomic<int> inserted_count{0};

  std::vector<std::jthread> insertion_threads;
  for (int t = 0; t < 4; ++t) {
    insertion_threads.emplace_back([&stop_insertions, &inserted_count, t, this]() {
      auto thread_acc = storage_->Access();
      int new_value = 1000 + (t * 100);
      while (!stop_insertions.load(std::memory_order_acquire)) {
        auto vertex_from = thread_acc->CreateVertex();
        auto vertex_to = thread_acc->CreateVertex();
        auto edge = thread_acc->CreateEdge(&vertex_from, &vertex_to, edge_type_id_);
        if (edge->SetProperty(this->property_id_, PropertyValue(new_value++)).has_value()) {
          ++inserted_count;
        }
        std::this_thread::sleep_for(std::chrono::microseconds(1));
      }
    });
  }

  // Let the processing run for a bit
  std::this_thread::sleep_for(std::chrono::milliseconds(100));

  // Process chunks in parallel
  std::atomic<int> total_processed{0};
  std::vector<std::jthread> processing_threads;

  for (size_t i = 0; i < edges.size(); ++i) {
    processing_threads.emplace_back([&edges, i, &total_processed, this]() {
      int local_count = 0;
      auto chunk = edges.get_chunk(i);
      for (auto it = chunk.begin(); it != chunk.end(); ++it) {
        local_count++;
        // Verify property value is within the specified range
        auto property_value = (*it).GetProperty(this->property_id_, View::OLD);
        ASSERT_TRUE(property_value.has_value());
        ASSERT_GE(property_value->ValueInt(), 100);
        ASSERT_LE(property_value->ValueInt(), 400);
        if (local_count % 50 == 0) {
          // Make sure the iterator and insertions are interleaved
          std::this_thread::sleep_for(std::chrono::microseconds(10));
        }
      }
      EXPECT_GT(local_count, 0);
      EXPECT_LT(local_count, 300 / 6 * 3);
      total_processed.fetch_add(local_count, std::memory_order_relaxed);
    });
  }

  // Stop insertions
  stop_insertions.store(true, std::memory_order_release);
  for (auto &thread : insertion_threads) {
    thread.join();
  }

  // Wait for processing threads
  for (auto &thread : processing_threads) {
    thread.join();
  }

  // Verify we processed exactly the elements in the range
  ASSERT_EQ(total_processed.load(), 301);  // 400 - 100 + 1 = 301
}

TEST_F(StorageV2ChunkIteratorTest, EdgePropertyIndexChunkingMultipleEntries) {
  // Create edges with specific property values
  std::vector<Gid> edge_gids;
  std::vector<Gid> property_edge_gids;

  // Create 200 edges with property values
  {
    auto acc = storage_->Access();
    for (int i = 0; i < 200; ++i) {
      auto vertex_from = acc->CreateVertex();
      auto vertex_to = acc->CreateVertex();
      auto edge = acc->CreateEdge(&vertex_from, &vertex_to, edge_type_id_);
      if (i % 3 == 0) {
        ASSERT_TRUE(edge->SetProperty(property_id_, PropertyValue(i)).has_value());
        property_edge_gids.push_back(edge->Gid());
      }
      edge_gids.push_back(edge->Gid());
    }

    ASSERT_TRUE(acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()).has_value());
  }

  // Modify edges to have multiple entries by updating properties
  {
    auto acc = storage_->Access();
    for (const auto gid : property_edge_gids) {
      auto edge = acc->FindEdge(gid, View::OLD);
      ASSERT_TRUE(edge);
      ASSERT_TRUE(edge->SetProperty(property_id_, PropertyValue{}).has_value());
    }
    ASSERT_TRUE(acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()).has_value());
  }
  {
    auto acc = storage_->Access();
    for (const auto gid : property_edge_gids) {
      auto edge = acc->FindEdge(gid, View::OLD);
      ASSERT_TRUE(edge);
      ASSERT_TRUE(edge->SetProperty(property_id_, PropertyValue{edge->Gid().AsInt()}).has_value());
    }
    ASSERT_TRUE(acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()).has_value());
  }

  // Test chunking for edges with property
  auto acc = storage_->Access();
  auto edges = acc->ChunkedEdges(property_id_, View::OLD, 4);

  ASSERT_GT(edges.size(), 0);

  // Count total edges across all chunks using ProcessChunksInParallel
  auto read_gids_result = ProcessChunksInParallel(
      edges, [](const auto &) {},
      [](int local_count) {
        ASSERT_GT(local_count, 0);
        ASSERT_LT(local_count, 67 / 4 * 2);
      });

  ASSERT_TRUE(std::equal(read_gids_result.begin(), read_gids_result.end(), property_edge_gids.begin()));
}

TEST_F(StorageV2ChunkIteratorTest, EdgePropertyIndexChunkingDynamic) {
  // Create edges with specific property values
  std::vector<Gid> edge_gids;
  std::vector<Gid> property_edge_gids;

  // Create 200 edges with property values
  {
    auto acc = storage_->Access();
    for (int i = 0; i < 200; ++i) {
      auto vertex_from = acc->CreateVertex();
      auto vertex_to = acc->CreateVertex();
      auto edge = acc->CreateEdge(&vertex_from, &vertex_to, edge_type_id_);
      if (i % 3 == 0) {
        ASSERT_TRUE(edge->SetProperty(property_id_, PropertyValue(i)).has_value());
        property_edge_gids.push_back(edge->Gid());
      }
      edge_gids.push_back(edge->Gid());
    }

    ASSERT_TRUE(acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()).has_value());
  }

  // Test chunking for edges with property
  auto acc = storage_->Access();  // Get access before starting the modify thread

  std::atomic_bool run = true;
  auto modify_thread = std::jthread([&run, this, &edge_gids]() {
    while (run) {
      int i = 0;
      auto acc = storage_->Access();
      for (const auto gid : edge_gids) {
        auto edge = acc->FindEdge(gid, View::OLD);
        if (!edge) continue;
        ++i;
        if (i % 2 == 0) {
          ASSERT_TRUE(edge->SetProperty(property_id_, PropertyValue{}).has_value());
        } else if (i % 3 == 0) {
          ASSERT_TRUE(edge->SetProperty(property_id_, PropertyValue{i}).has_value());
        } else if (i % 5 == 0) {
          ASSERT_TRUE(acc->DeleteEdge(&*edge).has_value());
        }
      }
      ASSERT_TRUE(acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()).has_value());
      storage_->FreeMemory();  // Run storage and skiplist gc
      std::this_thread::sleep_for(std::chrono::microseconds(100));
    }
  });

  std::this_thread::sleep_for(std::chrono::milliseconds(1));  // Wait for the modify thread to start
  auto edges = acc->ChunkedEdges(property_id_, View::OLD, 4);

  ASSERT_GT(edges.size(), 0);

  // Count total edges across all chunks using ProcessChunksInParallel
  auto read_gids_result = ProcessChunksInParallel(
      edges, [](const auto &edge) {},
      [](int local_count) {
        ASSERT_GT(local_count, 0);
        ASSERT_LT(local_count, 67 / 4 * 2);
      });

  run.store(false);
  modify_thread.join();

  ASSERT_TRUE(std::equal(read_gids_result.begin(), read_gids_result.end(), property_edge_gids.begin()));
}

TEST_F(StorageV2ChunkIteratorTest, EdgePropertyIndexChunkingManyEntries) {
  // Create edges with specific property values
  std::vector<Gid> edge_gids;
  std::vector<Gid> property_edge_gids;

  // Create 200 edges with property values
  {
    auto acc = storage_->Access();
    for (int i = 0; i < 200; ++i) {
      auto vertex_from = acc->CreateVertex();
      auto vertex_to = acc->CreateVertex();
      auto edge = acc->CreateEdge(&vertex_from, &vertex_to, edge_type_id_);
      if (i % 3 == 0) {
        ASSERT_TRUE(edge->SetProperty(property_id_, PropertyValue(i)).has_value());
        property_edge_gids.push_back(edge->Gid());
      }
      edge_gids.push_back(edge->Gid());
    }

    ASSERT_TRUE(acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()).has_value());
  }

  // Modify one edge to have many entries
  auto modified_gid = property_edge_gids[50];
  for (int i = 0; i < 100; ++i) {
    {
      auto acc = storage_->Access();
      auto edge = acc->FindEdge(modified_gid, View::OLD);
      ASSERT_TRUE(edge);
      ASSERT_TRUE(edge->SetProperty(property_id_, PropertyValue{}).has_value());
      ASSERT_TRUE(acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()).has_value());
    }
    {
      auto acc = storage_->Access();
      auto edge = acc->FindEdge(modified_gid, View::OLD);
      ASSERT_TRUE(edge);
      ASSERT_TRUE(edge->SetProperty(property_id_, PropertyValue{i}).has_value());
      ASSERT_TRUE(acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()).has_value());
    }
  }

  // Test chunking for edges with property
  auto acc = storage_->Access();
  auto edges = acc->ChunkedEdges(property_id_, View::OLD, 4);

  ASSERT_GT(edges.size(), 0);

  // Count total edges across all chunks using ProcessChunksInParallel
  auto read_gids_result = ProcessChunksInParallel(
      edges, [](const auto &) {},
      [](int local_count) {
        // NOTE: We don't assert on the count here because we don't know how many same edges are in the chunk. (Since
        // multiple copies of the same edge are in the chunk.)
      });

  ASSERT_TRUE(std::equal(read_gids_result.begin(), read_gids_result.end(), property_edge_gids.begin()));
}

TEST_F(StorageV2ChunkIteratorTest, EdgePropertyIndexChunkingComprehensiveEdgeCases) {
  // Create edges with specific property values
  std::vector<Gid> edge_gids;
  std::vector<Gid> property_edge_gids;

  // Empty index
  {
    // Test chunking for edges with property
    auto acc = storage_->Access();
    auto edges = acc->ChunkedEdges(property_id_, View::OLD, 4);
    ASSERT_EQ(edges.size(), 1);
    auto chunk = edges.get_chunk(0);
    ASSERT_TRUE(chunk.begin() == chunk.end());
  }

  // Fewer edges than chunks
  {
    auto acc = storage_->Access();
    for (int i = 0; i < 2; ++i) {
      auto vertex_from = acc->CreateVertex();
      auto vertex_to = acc->CreateVertex();
      auto edge = acc->CreateEdge(&vertex_from, &vertex_to, edge_type_id_);
      ASSERT_TRUE(edge->SetProperty(property_id_, PropertyValue(i)).has_value());
      property_edge_gids.push_back(edge->Gid());
    }
    ASSERT_TRUE(acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()).has_value());
    auto edges = acc->ChunkedEdges(property_id_, View::OLD, 4);
    ASSERT_EQ(edges.size(), 2);
  }

  // Exact number of edges and chunks
  {
    auto acc = storage_->Access();
    for (int i = 0; i < 2 * 3; ++i) {
      auto vertex_from = acc->CreateVertex();
      auto vertex_to = acc->CreateVertex();
      auto edge = acc->CreateEdge(&vertex_from, &vertex_to, edge_type_id_);
      if (i % 3 == 0) {
        ASSERT_TRUE(edge->SetProperty(property_id_, PropertyValue(i)).has_value());
        property_edge_gids.push_back(edge->Gid());
      }
      edge_gids.push_back(edge->Gid());
    }
    ASSERT_EQ(property_edge_gids.size(), 4);
    ASSERT_TRUE(acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()).has_value());
  }
  {
    auto acc = storage_->Access();
    auto edges = acc->ChunkedEdges(property_id_, View::OLD, 4);
    ASSERT_EQ(edges.size(), 4);
    auto read_gids = ProcessChunksInParallel(edges);
    ASSERT_TRUE(std::equal(read_gids.begin(), read_gids.end(), property_edge_gids.begin()));
  }

  // Same 100 edge at the beginning
  {
    const auto first_gid = property_edge_gids[0];
    for (int i = 0; i < 100; ++i) {
      auto acc = storage_->Access();
      auto edge = acc->FindEdge(first_gid, View::OLD);
      ASSERT_TRUE(edge);
      ASSERT_TRUE(edge->SetProperty(property_id_, PropertyValue{}).has_value());
      ASSERT_TRUE(edge->SetProperty(property_id_, PropertyValue{i}).has_value());
      ASSERT_TRUE(acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()).has_value());
    }
    auto acc = storage_->Access();
    auto edges = acc->ChunkedEdges(property_id_, View::OLD, 4);
    // NOTE This is an edge case where we need to skip whole chunks because of duplicates in the index.
    // ASSERT_EQ(edges.size(), 4);

    auto read_gids_result = ProcessChunksInParallel(edges);
    ASSERT_TRUE(std::equal(read_gids_result.begin(), read_gids_result.end(), property_edge_gids.begin()));
  }

  // Same 200 edge at the end
  {
    const auto last_gid = property_edge_gids.back();
    for (int i = 0; i < 200; ++i) {
      auto acc = storage_->Access();
      auto edge = acc->FindEdge(last_gid, View::OLD);
      ASSERT_TRUE(edge);
      ASSERT_TRUE(edge->SetProperty(property_id_, PropertyValue{}).has_value());
      ASSERT_TRUE(edge->SetProperty(property_id_, PropertyValue{i}).has_value());
      ASSERT_TRUE(acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()).has_value());
    }
    auto acc = storage_->Access();
    auto edges = acc->ChunkedEdges(property_id_, View::OLD, 4);
    // NOTE This is an edge case where we need to skip whole chunks because of duplicates in the index.
    // ASSERT_EQ(edges.size(), 4);

    auto read_gids_result = ProcessChunksInParallel(edges);
    ASSERT_TRUE(std::equal(read_gids_result.begin(), read_gids_result.end(), property_edge_gids.begin()));
  }
}

TEST_F(StorageV2ChunkIteratorTest, EdgePropertyIndexChunkingBasicRange) {
  // Create edges with specific property values
  std::vector<Gid> edge_gids;
  std::vector<Gid> property_edge_gids;

  // Create 200 edges with property values
  {
    auto acc = storage_->Access();
    for (int i = 0; i < 200; ++i) {
      auto vertex_from = acc->CreateVertex();
      auto vertex_to = acc->CreateVertex();
      auto edge = acc->CreateEdge(&vertex_from, &vertex_to, edge_type_id_);
      if (i % 3 == 0) {
        ASSERT_TRUE(edge->SetProperty(property_id_, PropertyValue(i)).has_value());
        property_edge_gids.push_back(edge->Gid());
      }
      edge_gids.push_back(edge->Gid());
    }

    ASSERT_TRUE(acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()).has_value());
  }

  // Checking lower bound
  {
    auto acc = storage_->Access();
    auto edges = acc->ChunkedEdges(property_id_, memgraph::utils::MakeBoundExclusive(PropertyValue(131)), std::nullopt,
                                   View::OLD, 4);
    ASSERT_GT(edges.size(), 0);
    auto first_chunk = edges.get_chunk(0);
    ASSERT_EQ((*first_chunk.begin()).GetProperty(property_id_, View::OLD).value().ValueInt(), 132);
    auto last_chunk = edges.get_chunk(edges.size() - 1);
    PropertyValue last_pv;
    for (auto it = last_chunk.begin(); it != last_chunk.end(); ++it) {
      last_pv = (*it).GetProperty(property_id_, View::OLD).value();
    }
    ASSERT_EQ(last_pv.ValueInt(), 198);
  }
  {
    auto acc = storage_->Access();
    auto edges = acc->ChunkedEdges(property_id_, memgraph::utils::MakeBoundExclusive(PropertyValue(132)), std::nullopt,
                                   View::OLD, 4);
    ASSERT_GT(edges.size(), 0);
    auto first_chunk = edges.get_chunk(0);
    ASSERT_EQ((*first_chunk.begin()).GetProperty(property_id_, View::OLD).value().ValueInt(), 135);
    auto last_chunk = edges.get_chunk(edges.size() - 1);
    PropertyValue last_pv;
    for (auto it = last_chunk.begin(); it != last_chunk.end(); ++it) {
      last_pv = (*it).GetProperty(property_id_, View::OLD).value();
    }
    ASSERT_EQ(last_pv.ValueInt(), 198);
  }
  {
    auto acc = storage_->Access();
    auto edges = acc->ChunkedEdges(property_id_, memgraph::utils::MakeBoundInclusive(PropertyValue(132)), std::nullopt,
                                   View::OLD, 4);
    ASSERT_GT(edges.size(), 0);
    auto first_chunk = edges.get_chunk(0);
    ASSERT_EQ((*first_chunk.begin()).GetProperty(property_id_, View::OLD).value().ValueInt(), 132);
    auto last_chunk = edges.get_chunk(edges.size() - 1);
    PropertyValue last_pv;
    for (auto it = last_chunk.begin(); it != last_chunk.end(); ++it) {
      last_pv = (*it).GetProperty(property_id_, View::OLD).value();
    }
    ASSERT_EQ(last_pv.ValueInt(), 198);
  }
  {
    auto acc = storage_->Access();
    auto edges = acc->ChunkedEdges(property_id_, memgraph::utils::MakeBoundInclusive(PropertyValue(-1)), std::nullopt,
                                   View::OLD, 4);
    ASSERT_EQ(edges.size(), 4);
    auto first_chunk = edges.get_chunk(0);
    ASSERT_EQ((*first_chunk.begin()).GetProperty(property_id_, View::OLD).value().ValueInt(), 0);
    auto last_chunk = edges.get_chunk(edges.size() - 1);
    PropertyValue last_pv;
    for (auto it = last_chunk.begin(); it != last_chunk.end(); ++it) {
      last_pv = (*it).GetProperty(property_id_, View::OLD).value();
    }
    ASSERT_EQ(last_pv.ValueInt(), 198);
  }
  {
    auto acc = storage_->Access();
    auto edges = acc->ChunkedEdges(property_id_, memgraph::utils::MakeBoundInclusive(PropertyValue(210)), std::nullopt,
                                   View::OLD, 4);
    ASSERT_EQ(edges.size(), 1);
    auto chunk = edges.get_chunk(0);
    ASSERT_TRUE(chunk.begin() == chunk.end());
  }

  // Checking upper bound
  {
    auto acc = storage_->Access();
    auto edges = acc->ChunkedEdges(property_id_, std::nullopt, memgraph::utils::MakeBoundExclusive(PropertyValue(143)),
                                   View::OLD, 4);
    ASSERT_EQ(edges.size(), 4);
    auto first_chunk = edges.get_chunk(0);
    ASSERT_EQ((*first_chunk.begin()).GetProperty(property_id_, View::OLD).value().ValueInt(), 0);
    auto last_chunk = edges.get_chunk(edges.size() - 1);
    PropertyValue last_pv;
    for (auto it = last_chunk.begin(); it != last_chunk.end(); ++it) {
      last_pv = (*it).GetProperty(property_id_, View::OLD).value();
    }
    ASSERT_EQ(last_pv.ValueInt(), 141);
  }
  {
    auto acc = storage_->Access();
    auto edges = acc->ChunkedEdges(property_id_, std::nullopt, memgraph::utils::MakeBoundExclusive(PropertyValue(141)),
                                   View::OLD, 4);
    ASSERT_GT(edges.size(), 0);
    auto last_chunk = edges.get_chunk(edges.size() - 1);
    PropertyValue last_pv;
    for (auto it = last_chunk.begin(); it != last_chunk.end(); ++it) {
      last_pv = (*it).GetProperty(property_id_, View::OLD).value();
    }
    ASSERT_EQ(last_pv.ValueInt(), 138);
  }
  {
    auto acc = storage_->Access();
    auto edges = acc->ChunkedEdges(property_id_, std::nullopt, memgraph::utils::MakeBoundInclusive(PropertyValue(141)),
                                   View::OLD, 4);
    ASSERT_GT(edges.size(), 0);
    auto last_chunk = edges.get_chunk(edges.size() - 1);
    PropertyValue last_pv;
    for (auto it = last_chunk.begin(); it != last_chunk.end(); ++it) {
      last_pv = (*it).GetProperty(property_id_, View::OLD).value();
    }
    ASSERT_EQ(last_pv.ValueInt(), 141);
  }
  {
    auto acc = storage_->Access();
    auto edges = acc->ChunkedEdges(property_id_, std::nullopt, memgraph::utils::MakeBoundInclusive(PropertyValue(210)),
                                   View::OLD, 4);
    ASSERT_EQ(edges.size(), 4);
    auto first_chunk = edges.get_chunk(0);
    ASSERT_EQ((*first_chunk.begin()).GetProperty(property_id_, View::OLD).value().ValueInt(), 0);
    auto last_chunk = edges.get_chunk(edges.size() - 1);
    PropertyValue last_pv;
    for (auto it = last_chunk.begin(); it != last_chunk.end(); ++it) {
      last_pv = (*it).GetProperty(property_id_, View::OLD).value();
    }
    ASSERT_EQ(last_pv.ValueInt(), 198);
  }
  {
    auto acc = storage_->Access();
    auto edges = acc->ChunkedEdges(property_id_, std::nullopt, memgraph::utils::MakeBoundInclusive(PropertyValue(-10)),
                                   View::OLD, 4);
    ASSERT_EQ(edges.size(), 1);
    auto chunk = edges.get_chunk(0);
    ASSERT_TRUE(chunk.begin() == chunk.end());
  }

  // Test chunking for edges with property range
  auto acc = storage_->Access();
  auto edges = acc->ChunkedEdges(property_id_, memgraph::utils::MakeBoundExclusive(PropertyValue(10)),
                                 memgraph::utils::MakeBoundExclusive(PropertyValue(160)), View::OLD, 4);

  ASSERT_GT(edges.size(), 0);

  // Count total edges across all chunks using ProcessChunksInParallel
  std::atomic<int> total_count{0};
  memgraph::utils::Synchronized<std::vector<PropertyValue>> read_props;

  auto read_gids_result = ProcessChunksInParallel(
      edges,
      [&read_props, this](const auto &edge) {
        const auto prop = edge.GetProperty(property_id_, View::OLD);
        ASSERT_TRUE(prop.has_value());
        ASSERT_TRUE(prop->IsInt());
        auto locked_props = read_props.Lock();
        locked_props->push_back(prop.value());
      },
      [&total_count](int local_count) {
        ASSERT_GT(local_count, 0);
        ASSERT_LT(local_count, 50 / 4 * 2);
        total_count.fetch_add(local_count, std::memory_order_relaxed);
      });

  {
    auto locked_props = read_props.Lock();
    ASSERT_EQ(locked_props->size(), 50);  // Every 3rd has the property
    std::sort(locked_props->begin(), locked_props->end());
    for (size_t i = 0; i < locked_props->size(); ++i) {
      ASSERT_EQ((*locked_props)[i], PropertyValue{(static_cast<int>(i) * 3) +
                                                  12});  // i*3 + 12 just because of the way we create the dataset
    }
  }

  // Edges cases where both bounds are set
  {
    // Non existing value
    auto acc = storage_->Access();
    auto edges = acc->ChunkedEdges(property_id_, memgraph::utils::MakeBoundInclusive(PropertyValue(130)),
                                   memgraph::utils::MakeBoundInclusive(PropertyValue(130)), View::OLD, 4);
    ASSERT_EQ(edges.size(), 1);
    auto chunk = edges.get_chunk(0);
    ASSERT_TRUE(chunk.begin() == chunk.end());
  }
  {
    // Exact match
    auto acc = storage_->Access();
    auto edges = acc->ChunkedEdges(property_id_, memgraph::utils::MakeBoundInclusive(PropertyValue(132)),
                                   memgraph::utils::MakeBoundInclusive(PropertyValue(132)), View::OLD, 4);
    ASSERT_EQ(edges.size(), 1);
    auto chunk = edges.get_chunk(0);
    auto it = chunk.begin();
    ASSERT_TRUE(it != chunk.end());
    ASSERT_EQ((*it).GetProperty(property_id_, View::OLD).value().ValueInt(), 132);
    ++it;
    ASSERT_TRUE(it == chunk.end());
  }
  {
    // Missed match
    auto acc = storage_->Access();
    auto edges = acc->ChunkedEdges(property_id_, memgraph::utils::MakeBoundInclusive(PropertyValue(132)),
                                   memgraph::utils::MakeBoundExclusive(PropertyValue(132)), View::OLD, 4);
    ASSERT_EQ(edges.size(), 1);
    auto chunk = edges.get_chunk(0);
    ASSERT_TRUE(chunk.begin() == chunk.end());
  }
  {
    // One match
    auto acc = storage_->Access();
    auto edges = acc->ChunkedEdges(property_id_, memgraph::utils::MakeBoundInclusive(PropertyValue(131)),
                                   memgraph::utils::MakeBoundInclusive(PropertyValue(133)), View::OLD, 4);
    ASSERT_EQ(edges.size(), 1);
    auto chunk = edges.get_chunk(0);
    auto it = chunk.begin();
    ASSERT_TRUE(it != chunk.end());
    ASSERT_EQ((*it).GetProperty(property_id_, View::OLD).value().ValueInt(), 132);
    ++it;
    ASSERT_TRUE(it == chunk.end());
  }
  {
    // Upper bound lower than lower bound
    auto acc = storage_->Access();
    auto edges = acc->ChunkedEdges(property_id_, memgraph::utils::MakeBoundInclusive(PropertyValue(130)),
                                   memgraph::utils::MakeBoundInclusive(PropertyValue(120)), View::OLD, 4);
    ASSERT_EQ(edges.size(), 0);
  }
}

// ============================================================================
// EDGE TYPE INDEX TESTS
// ============================================================================

TEST_F(StorageV2ChunkIteratorTest, BasicEdgeTypeIndexChunking) {
  // Create edges with different edge types
  std::vector<Gid> edge_gids;

  {
    auto acc = storage_->Access();
    // Create 100 edges with edge_type_id1_
    for (int i = 0; i < 100; ++i) {
      auto vertex_from = acc->CreateVertex();
      auto vertex_to = acc->CreateVertex();
      auto edge = acc->CreateEdge(&vertex_from, &vertex_to, edge_type_id1_);
      edge_gids.push_back(edge->Gid());
    }

    ASSERT_TRUE(acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()).has_value());
  }

  auto acc = storage_->Access();
  // Test chunking for edges with edge type
  auto edges = acc->ChunkedEdges(edge_type_id1_, View::OLD, 4);

  ASSERT_GT(edges.size(), 0);

  // Count total edges across all chunks using ProcessChunksInParallel
  auto read_gids_result = ProcessChunksInParallel(
      edges, [](const auto &) {},
      [](int local_count) {
        ASSERT_GT(local_count, 0);
        ASSERT_LT(local_count, 100 / 4 * 2);
      });

  ASSERT_TRUE(std::equal(read_gids_result.begin(), read_gids_result.end(), edge_gids.begin()));
}

TEST_F(StorageV2ChunkIteratorTest, EdgeTypeIndexChunkingBigDataset) {
  // Create edges with edge types
  std::vector<Gid> edge_gids;

  {
    auto acc = storage_->Access();
    // Create 1000 edges with edge_type_id1_
    for (int i = 0; i < 100000; ++i) {
      auto vertex_from = acc->CreateVertex();
      auto vertex_to = acc->CreateVertex();
      if (i % 2 == 0) {
        auto edge = acc->CreateEdge(&vertex_from, &vertex_to, edge_type_id1_);
        edge_gids.push_back(edge->Gid());
      } else {
        ASSERT_TRUE(acc->CreateEdge(&vertex_from, &vertex_to, edge_type_id2_).has_value());
      }
    }

    ASSERT_TRUE(acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()).has_value());
  }

  auto acc = storage_->Access();
  // Test chunking for edges with edge type
  auto edges = acc->ChunkedEdges(edge_type_id1_, View::OLD, 8);

  ASSERT_GT(edges.size(), 0);

  // Count total edges across all chunks using ProcessChunksInParallel
  auto read_gids_result = ProcessChunksInParallel(edges);
  ASSERT_TRUE(std::equal(read_gids_result.begin(), read_gids_result.end(), edge_gids.begin()));
}

TEST_F(StorageV2ChunkIteratorTest, EdgeTypeIndexChunkingConcurrentOperations) {
  // Create initial edges
  std::vector<Gid> edge_gids;
  {
    auto acc = storage_->Access();
    // Create 500 edges with edge_type_id1_
    for (int i = 0; i < 500; ++i) {
      auto vertex_from = acc->CreateVertex();
      auto vertex_to = acc->CreateVertex();
      auto edge = acc->CreateEdge(&vertex_from, &vertex_to, edge_type_id1_);
      edge_gids.push_back(edge->Gid());
    }

    ASSERT_TRUE(acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()).has_value());
  }

  auto acc = storage_->Access();
  // Test chunking for edges with edge type
  auto edges = acc->ChunkedEdges(edge_type_id1_, View::OLD, 6);

  ASSERT_GT(edges.size(), 0);

  // Start concurrent insertion threads
  std::atomic<bool> stop_insertions{false};
  std::atomic<int> inserted_count{0};

  std::vector<std::jthread> insertion_threads;
  insertion_threads.reserve(4);
  for (int t = 0; t < 4; ++t) {
    insertion_threads.emplace_back([this, &stop_insertions, &inserted_count]() {
      auto thread_acc = storage_->Access();
      while (!stop_insertions.load(std::memory_order_acquire)) {
        auto vertex_from = thread_acc->CreateVertex();
        auto vertex_to = thread_acc->CreateVertex();
        auto edge = thread_acc->CreateEdge(&vertex_from, &vertex_to, edge_type_id1_);
        if (edge.has_value()) {
          ++inserted_count;
        }
        std::this_thread::sleep_for(std::chrono::microseconds(1));
      }
    });
  }

  // Let the processing run for a bit
  std::this_thread::sleep_for(std::chrono::milliseconds(100));

  // Process chunks in parallel using ProcessChunksInParallel
  std::atomic<int> total_processed{0};

  ProcessChunksInParallel(
      edges,
      [&total_processed](const auto &edge) {
        static std::atomic<int> local_count{0};
        local_count.fetch_add(1, std::memory_order_relaxed);
        if (local_count.load(std::memory_order_relaxed) % 50 == 0) {
          // Make sure the iterator and insertions are interleaved
          std::this_thread::sleep_for(std::chrono::microseconds(10));
        }
        total_processed.fetch_add(1, std::memory_order_relaxed);
      },
      [](int local_count) {
        EXPECT_GT(local_count, 0);
        EXPECT_LT(local_count, 500 / 6 * 3);
      });

  // Stop insertions
  stop_insertions.store(true, std::memory_order_release);
  insertion_threads.clear();

  // Verify we processed exactly the initial elements
  ASSERT_EQ(total_processed.load(), 500);
}

TEST_F(StorageV2ChunkIteratorTest, EdgeTypeIndexChunkingMultipleEdgeTypes) {
  // Create edges with different edge types
  std::vector<Gid> edge_gids;
  {
    auto acc = storage_->Access();

    // Create 100 edges with edge_type_id1_
    for (int i = 0; i < 100; ++i) {
      auto vertex_from = acc->CreateVertex();
      auto vertex_to = acc->CreateVertex();
      auto edge = acc->CreateEdge(&vertex_from, &vertex_to, edge_type_id1_);
      edge_gids.push_back(edge->Gid());
    }

    // Create 50 edges with edge_type_id2_
    for (int i = 0; i < 50; ++i) {
      auto vertex_from = acc->CreateVertex();
      auto vertex_to = acc->CreateVertex();
      auto edge = acc->CreateEdge(&vertex_from, &vertex_to, edge_type_id2_);
      edge_gids.push_back(edge->Gid());
    }

    ASSERT_TRUE(acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()).has_value());
  }

  auto acc = storage_->Access();

  // Test chunking for edges with edge_type_id1_
  auto edges1 = acc->ChunkedEdges(edge_type_id1_, View::OLD, 4);

  ASSERT_GT(edges1.size(), 0);

  // Count total edges across all chunks for edge_type_id1_
  auto read_gids_result1 = ProcessChunksInParallel(
      edges1, [](const auto &) {},
      [](int local_count) {
        ASSERT_GT(local_count, 0);
        ASSERT_LT(local_count, 100 / 4 * 2);
      });

  // Test chunking for edges with edge_type_id2_
  auto edges2 = acc->ChunkedEdges(edge_type_id2_, View::OLD, 4);

  ASSERT_GT(edges2.size(), 0);

  // Count total edges across all chunks for edge_type_id2_
  auto read_gids_result2 = ProcessChunksInParallel(
      edges2, [](const auto &) {},
      [](int local_count) {
        ASSERT_GT(local_count, 0);
        ASSERT_LT(local_count, 50 / 4 * 2);
      });

  // Verify we got the correct counts for each edge type
  ASSERT_EQ(read_gids_result1.size(), 100);
  ASSERT_EQ(read_gids_result2.size(), 50);
}

TEST_F(StorageV2ChunkIteratorTest, EdgeTypeIndexChunkingEdgeCases) {
  // Create edges with edge types
  std::vector<Gid> edge_gids;
  {
    auto acc = storage_->Access();

    // Create 50 edges with edge_type_id1_
    for (int i = 0; i < 50; ++i) {
      auto vertex_from = acc->CreateVertex();
      auto vertex_to = acc->CreateVertex();
      auto edge = acc->CreateEdge(&vertex_from, &vertex_to, edge_type_id1_);
      edge_gids.push_back(edge->Gid());
    }

    ASSERT_TRUE(acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()).has_value());
  }

  auto acc = storage_->Access();
  // Test 1: Empty index (non-existent edge type)
  auto empty_edges = acc->ChunkedEdges(edge_type_id2_, View::OLD, 4);
  ASSERT_GT(empty_edges.size(), 0);

  int empty_count = 0;
  for (size_t i = 0; i < empty_edges.size(); ++i) {
    auto chunk = empty_edges.get_chunk(i);
    for (auto it = chunk.begin(); it != chunk.end(); ++it) {
      empty_count++;
    }
  }
  ASSERT_EQ(empty_count, 0);

  // Test 2: Single element
  auto single_edges = acc->ChunkedEdges(edge_type_id1_, View::OLD, 4);
  ASSERT_GT(single_edges.size(), 0);

  int single_count = 0;
  for (size_t i = 0; i < single_edges.size(); ++i) {
    auto chunk = single_edges.get_chunk(i);
    for (auto it = chunk.begin(); it != chunk.end(); ++it) {
      single_count++;
    }
  }
  ASSERT_EQ(single_count, 50);
}

TEST_F(StorageV2ChunkIteratorTest, EdgeTypeIndexChunkingDynamic) {
  // Create edges with edge types
  std::vector<Gid> edge_gids;
  {
    auto acc = storage_->Access();

    // Create 200 edges with edge_type_id1_
    for (int i = 0; i < 200; ++i) {
      auto vertex_from = acc->CreateVertex();
      auto vertex_to = acc->CreateVertex();
      auto edge = acc->CreateEdge(&vertex_from, &vertex_to, edge_type_id1_);
      edge_gids.push_back(edge->Gid());
    }

    ASSERT_TRUE(acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()).has_value());
  }

  auto acc = storage_->Access();
  // Test chunking for edges with edge type
  auto acc2 = storage_->Access();  // Get access before starting the modify thread

  std::atomic_bool run = true;
  auto modify_thread = std::jthread([&run, this, &edge_gids]() {
    while (run) {
      int i = 0;
      auto acc = storage_->Access();
      for (const auto gid : edge_gids) {
        auto edge = acc->FindEdge(gid, View::OLD);
        if (!edge) continue;
        ++i;
        if (i % 5 == 0) {
          ASSERT_TRUE(acc->DeleteEdge(&*edge).has_value());
        }
      }
      ASSERT_TRUE(acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()).has_value());
      storage_->FreeMemory();  // Run storage and skiplist gc
      std::this_thread::sleep_for(std::chrono::microseconds(100));
    }
  });

  std::this_thread::sleep_for(std::chrono::milliseconds(1));  // Wait for the modify thread to start
  auto edges = acc2->ChunkedEdges(edge_type_id1_, View::OLD, 4);

  ASSERT_GT(edges.size(), 0);

  // Count total edges across all chunks using ProcessChunksInParallel
  auto read_gids_result = ProcessChunksInParallel(
      edges, [](const auto &) {},
      [](int local_count) {
        ASSERT_GT(local_count, 0);
        ASSERT_LT(local_count, 200 / 4 * 2);
      });

  run.store(false);
  modify_thread.join();

  ASSERT_GT(read_gids_result.size(), 0);
  ASSERT_LE(read_gids_result.size(), 200);
}

TEST_F(StorageV2ChunkIteratorTest, EdgeTypeIndexChunkingComprehensiveEdgeCases) {
  // Create edges with edge types
  std::vector<Gid> edge_gids;

  // Empty index
  {
    // Test chunking for edges with edge type
    auto acc = storage_->Access();
    auto edges = acc->ChunkedEdges(edge_type_id1_, View::OLD, 4);
    ASSERT_EQ(edges.size(), 1);
    auto chunk = edges.get_chunk(0);
    ASSERT_TRUE(chunk.begin() == chunk.end());
  }

  // Fewer edges than chunks
  {
    auto acc = storage_->Access();
    for (int i = 0; i < 2; ++i) {
      auto vertex_from = acc->CreateVertex();
      auto vertex_to = acc->CreateVertex();
      auto edge = acc->CreateEdge(&vertex_from, &vertex_to, edge_type_id1_);
      edge_gids.push_back(edge->Gid());
    }
    ASSERT_TRUE(acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()).has_value());
    auto edges = acc->ChunkedEdges(edge_type_id1_, View::OLD, 4);
    ASSERT_EQ(edges.size(), 2);
  }

  // Exact number of edges and chunks
  {
    auto acc = storage_->Access();
    for (int i = 0; i < 4; ++i) {
      auto vertex_from = acc->CreateVertex();
      auto vertex_to = acc->CreateVertex();
      if (i % 2 == 0) {
        auto edge = acc->CreateEdge(&vertex_from, &vertex_to, edge_type_id1_);
        edge_gids.push_back(edge->Gid());
      } else {
        auto edge = acc->CreateEdge(&vertex_from, &vertex_to, edge_type_id2_);
      }
    }
    ASSERT_TRUE(acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()).has_value());
  }
  {
    auto acc = storage_->Access();
    auto edges = acc->ChunkedEdges(edge_type_id1_, View::OLD, 4);
    ASSERT_EQ(edges.size(), 4);
    auto read_gids = ProcessChunksInParallel(edges);
    ASSERT_EQ(read_gids.size(), 4);
    ASSERT_TRUE(std::equal(read_gids.begin(), read_gids.end(), edge_gids.begin()));
  }
}

// ============================================================================
// EDGE TYPE PROPERTY INDEX TESTS
// ============================================================================

TEST_F(StorageV2ChunkIteratorTest, BasicEdgeTypePropertyIndexChunking) {
  // Create edges with different property values
  std::vector<Gid> edge_gids;
  {
    auto acc = storage_->Access();
    // Create 100 edges with property values from 0 to 99
    for (int i = 0; i < 100; ++i) {
      auto vertex_from = acc->CreateVertex();
      auto vertex_to = acc->CreateVertex();
      auto edge = acc->CreateEdge(&vertex_from, &vertex_to, edge_type_id1_);
      ASSERT_TRUE(edge->SetProperty(property_id_, PropertyValue(i)).has_value());
      edge_gids.push_back(edge->Gid());
    }

    ASSERT_TRUE(acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()).has_value());
  }

  auto acc = storage_->Access();
  // Test chunking for edges with edge type and property
  auto edges = acc->ChunkedEdges(edge_type_id1_, property_id_, View::OLD, 4);

  ASSERT_GT(edges.size(), 0);

  // Count total edges across all chunks using ProcessChunksInParallel
  auto read_gids_result = ProcessChunksInParallel(
      edges, [](const auto &) {},
      [](int local_count) {
        ASSERT_GT(local_count, 0);
        ASSERT_LT(local_count, 100 / 4 * 2);
      });

  ASSERT_TRUE(std::equal(read_gids_result.begin(), read_gids_result.end(), edge_gids.begin()));
}

TEST_F(StorageV2ChunkIteratorTest, EdgeTypePropertyIndexChunkingWithRange) {
  // Create edges with property values in a specific range
  std::vector<Gid> edge_gids;

  {
    auto acc = storage_->Access();
    // Create 200 edges with property values from 0 to 199
    for (int i = 0; i < 200; ++i) {
      auto vertex_from = acc->CreateVertex();
      auto vertex_to = acc->CreateVertex();
      auto edge = acc->CreateEdge(&vertex_from, &vertex_to, edge_type_id1_);
      ASSERT_TRUE(edge->SetProperty(property_id_, PropertyValue(i)).has_value());
      edge_gids.push_back(edge->Gid());
    }

    ASSERT_TRUE(acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()).has_value());
  }

  auto acc = storage_->Access();
  // Test chunking for edges with edge type and property in range [50, 150]
  auto edges = acc->ChunkedEdges(edge_type_id1_, property_id_, memgraph::utils::MakeBoundInclusive(PropertyValue(50)),
                                 memgraph::utils::MakeBoundInclusive(PropertyValue(150)), View::OLD, 4);

  ASSERT_GT(edges.size(), 0);

  // Count total edges across all chunks using ProcessChunksInParallel
  auto read_gids_result = ProcessChunksInParallel(
      edges,
      [this](const auto &edge) {
        // Verify property value is in range
        auto property_value = edge.GetProperty(this->property_id_, View::OLD);
        ASSERT_TRUE(property_value.has_value());
        ASSERT_GE(property_value->ValueInt(), 50);
        ASSERT_LE(property_value->ValueInt(), 150);
      },
      [](int local_count) {
        ASSERT_GT(local_count, 0);
        ASSERT_LT(local_count, 101 / 4 * 2);
      });

  ASSERT_EQ(read_gids_result.size(), 101);  // 150 - 50 + 1 = 101
}

TEST_F(StorageV2ChunkIteratorTest, EdgeTypePropertyIndexChunkingWithLowerBound) {
  // Create edges with property values
  std::vector<Gid> edge_gids;

  {
    auto acc = storage_->Access();
    // Create 100 edges with property values from 0 to 99
    for (int i = 0; i < 100; ++i) {
      auto vertex_from = acc->CreateVertex();
      auto vertex_to = acc->CreateVertex();
      auto edge = acc->CreateEdge(&vertex_from, &vertex_to, edge_type_id1_);
      ASSERT_TRUE(edge->SetProperty(property_id_, PropertyValue(i)).has_value());
      edge_gids.push_back(edge->Gid());
    }

    ASSERT_TRUE(acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()).has_value());
  }

  auto acc = storage_->Access();
  // Test chunking for edges with edge type and property >= 30
  auto edges = acc->ChunkedEdges(edge_type_id1_, property_id_, memgraph::utils::MakeBoundInclusive(PropertyValue(30)),
                                 std::nullopt, View::OLD, 4);

  ASSERT_GT(edges.size(), 0);

  // Count total edges across all chunks using ProcessChunksInParallel
  auto read_gids_result = ProcessChunksInParallel(
      edges,
      [this](const auto &edge) {
        // Verify property value is >= 30
        auto property_value = edge.GetProperty(this->property_id_, View::OLD);
        ASSERT_TRUE(property_value.has_value());
        ASSERT_GE(property_value->ValueInt(), 30);
      },
      [](int local_count) {
        ASSERT_GT(local_count, 0);
        ASSERT_LT(local_count, 70 / 4 * 2);
      });

  ASSERT_EQ(read_gids_result.size(), 70);  // 99 - 30 + 1 = 70
}

TEST_F(StorageV2ChunkIteratorTest, EdgeTypePropertyIndexChunkingWithUpperBound) {
  // Create edges with property values
  std::vector<Gid> edge_gids;

  // Create 100 edges with property values from 0 to 99
  {
    auto acc = storage_->Access();
    for (int i = 0; i < 100; ++i) {
      auto vertex_from = acc->CreateVertex();
      auto vertex_to = acc->CreateVertex();
      auto edge = acc->CreateEdge(&vertex_from, &vertex_to, edge_type_id1_);
      ASSERT_TRUE(edge->SetProperty(property_id_, PropertyValue(i)).has_value());
      edge_gids.push_back(edge->Gid());
    }

    ASSERT_TRUE(acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()).has_value());
  }

  auto acc = storage_->Access();
  // Test chunking for edges with edge type and property <= 70
  auto edges = acc->ChunkedEdges(edge_type_id1_, property_id_, std::nullopt,
                                 memgraph::utils::MakeBoundInclusive(PropertyValue(70)), View::OLD, 4);

  ASSERT_GT(edges.size(), 0);

  // Count total edges across all chunks using ProcessChunksInParallel
  auto read_gids_result = ProcessChunksInParallel(
      edges,
      [this](const auto &edge) {
        // Verify property value is <= 70
        auto property_value = edge.GetProperty(this->property_id_, View::OLD);
        ASSERT_TRUE(property_value.has_value());
        ASSERT_LE(property_value->ValueInt(), 70);
      },
      [](int local_count) {
        ASSERT_GT(local_count, 0);
        ASSERT_LT(local_count, 71 / 4 * 2);
      });

  ASSERT_EQ(read_gids_result.size(), 71);  // 70 - 0 + 1 = 71
}

TEST_F(StorageV2ChunkIteratorTest, EdgeTypePropertyIndexChunkingEdgeCases) {
  // Create edges with property values
  std::vector<Gid> edge_gids;
  {
    auto acc = storage_->Access();
    // Create 50 edges with property values from 0 to 49
    for (int i = 0; i < 50; ++i) {
      auto vertex_from = acc->CreateVertex();
      auto vertex_to = acc->CreateVertex();
      auto edge = acc->CreateEdge(&vertex_from, &vertex_to, edge_type_id1_);
      ASSERT_TRUE(edge->SetProperty(property_id_, PropertyValue(i)).has_value());
      edge_gids.push_back(edge->Gid());
    }
    ASSERT_TRUE(acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()).has_value());
  }

  auto acc = storage_->Access();
  // Test 1: Empty range (lower > upper)
  auto empty_edges =
      acc->ChunkedEdges(edge_type_id1_, property_id_, memgraph::utils::MakeBoundInclusive(PropertyValue(100)),
                        memgraph::utils::MakeBoundInclusive(PropertyValue(50)), View::OLD, 4);
  ASSERT_EQ(empty_edges.size(), 0);

  int empty_count = 0;
  for (size_t i = 0; i < empty_edges.size(); ++i) {
    auto chunk = empty_edges.get_chunk(i);
    for (auto it = chunk.begin(); it != chunk.end(); ++it) {
      empty_count++;
    }
  }
  ASSERT_EQ(empty_count, 0);

  // Test 2: Single element range
  auto single_edges =
      acc->ChunkedEdges(edge_type_id1_, property_id_, memgraph::utils::MakeBoundInclusive(PropertyValue(25)),
                        memgraph::utils::MakeBoundInclusive(PropertyValue(25)), View::OLD, 4);
  ASSERT_GT(single_edges.size(), 0);

  int single_count = 0;
  for (size_t i = 0; i < single_edges.size(); ++i) {
    auto chunk = single_edges.get_chunk(i);
    for (auto it = chunk.begin(); it != chunk.end(); ++it) {
      single_count++;
      auto property_value = (*it).GetProperty(this->property_id_, View::OLD);
      ASSERT_TRUE(property_value.has_value());
      ASSERT_EQ(property_value->ValueInt(), 25);
    }
  }
  ASSERT_EQ(single_count, 1);

  // Test 3: Range with no elements (outside existing range)
  auto no_elements_edges =
      acc->ChunkedEdges(edge_type_id1_, property_id_, memgraph::utils::MakeBoundInclusive(PropertyValue(100)),
                        memgraph::utils::MakeBoundInclusive(PropertyValue(200)), View::OLD, 4);
  ASSERT_GT(no_elements_edges.size(), 0);

  int no_elements_count = 0;
  for (size_t i = 0; i < no_elements_edges.size(); ++i) {
    auto chunk = no_elements_edges.get_chunk(i);
    for (auto it = chunk.begin(); it != chunk.end(); ++it) {
      no_elements_count++;
    }
  }
  ASSERT_EQ(no_elements_count, 0);
}

TEST_F(StorageV2ChunkIteratorTest, EdgeTypePropertyIndexChunkingBigDataset) {
  // Create edges with property values
  std::vector<Gid> edge_gids;

  {
    auto acc = storage_->Access();
    // Create 1000 edges with property values from 0 to 999
    for (int i = 0; i < 100000; ++i) {
      auto vertex_from = acc->CreateVertex();
      auto vertex_to = acc->CreateVertex();
      auto edge = acc->CreateEdge(&vertex_from, &vertex_to, edge_type_id1_);
      ASSERT_TRUE(edge->SetProperty(property_id_, PropertyValue(i)).has_value());
      edge_gids.push_back(edge->Gid());
    }

    ASSERT_TRUE(acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()).has_value());
  }

  auto acc = storage_->Access();
  // Test chunking for edges with edge type and property in range [200, 800]
  auto edges =
      acc->ChunkedEdges(edge_type_id1_, property_id_, memgraph::utils::MakeBoundInclusive(PropertyValue(20'000)),
                        memgraph::utils::MakeBoundInclusive(PropertyValue(80'000)), View::OLD, 8);

  ASSERT_GT(edges.size(), 0);

  // Count total edges across all chunks
  std::atomic<int> total_count{0};
  auto read_gids = ProcessChunksInParallel(
      edges,
      [&total_count, this](const auto &edge) {
        total_count.fetch_add(1, std::memory_order_relaxed);
        // Verify property value is in range
        auto property_value = edge.GetProperty(this->property_id_, View::OLD);
        ASSERT_TRUE(property_value.has_value());
        ASSERT_GE(property_value->ValueInt(), 20'000);
        ASSERT_LE(property_value->ValueInt(), 80'000);
      },
      [](int local_count) {
        ASSERT_GT(local_count, 0);
        ASSERT_LT(local_count, 60'001 / 8 * 2);
      });

  std::sort(read_gids.begin(), read_gids.end());
  ASSERT_EQ(total_count.load(), 60'001);  // 80'000 - 20'000 + 1 = 60'001
  ASSERT_EQ(read_gids.size(), 60'001);
}

TEST_F(StorageV2ChunkIteratorTest, EdgeTypePropertyIndexChunkingConcurrentOperations) {
  // Create initial edges with property values
  std::vector<Gid> edge_gids;

  {
    auto acc = storage_->Access();
    // Create 500 edges with property values from 0 to 499
    for (int i = 0; i < 500; ++i) {
      auto vertex_from = acc->CreateVertex();
      auto vertex_to = acc->CreateVertex();
      auto edge = acc->CreateEdge(&vertex_from, &vertex_to, edge_type_id1_);
      ASSERT_TRUE(edge->SetProperty(property_id_, PropertyValue(i)).has_value());
      edge_gids.push_back(edge->Gid());
    }

    ASSERT_TRUE(acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()).has_value());
  }

  auto acc = storage_->Access();
  // Test chunking for edges with edge type and property in range [100, 400]
  auto edges = acc->ChunkedEdges(edge_type_id1_, property_id_, memgraph::utils::MakeBoundInclusive(PropertyValue(100)),
                                 memgraph::utils::MakeBoundInclusive(PropertyValue(400)), View::OLD, 6);

  ASSERT_GT(edges.size(), 0);

  // Start concurrent insertion threads
  std::atomic<bool> stop_insertions{false};
  std::atomic<int> inserted_count{0};

  std::vector<std::jthread> insertion_threads;
  for (int t = 0; t < 4; ++t) {
    insertion_threads.emplace_back([this, &stop_insertions, &inserted_count, t]() {
      auto thread_acc = storage_->Access();
      int new_value = 1000 + t * 100;
      while (!stop_insertions.load(std::memory_order_acquire)) {
        auto vertex_from = thread_acc->CreateVertex();
        auto vertex_to = thread_acc->CreateVertex();
        auto edge = thread_acc->CreateEdge(&vertex_from, &vertex_to, edge_type_id1_);
        if (edge->SetProperty(property_id_, PropertyValue(new_value++)).has_value()) {
          ++inserted_count;
        }
        std::this_thread::sleep_for(std::chrono::microseconds(1));
      }
    });
  }

  // Let the processing run for a bit
  std::this_thread::sleep_for(std::chrono::milliseconds(100));

  // Process chunks in parallel using ProcessChunksInParallel
  std::atomic<int> total_processed{0};

  ProcessChunksInParallel(
      edges,
      [&total_processed, this](const auto &edge) {
        static std::atomic<int> local_count{0};
        local_count.fetch_add(1, std::memory_order_relaxed);
        // Verify property value is within the specified range
        auto property_value = edge.GetProperty(this->property_id_, View::OLD);
        ASSERT_TRUE(property_value.has_value());
        ASSERT_GE(property_value->ValueInt(), 100);
        ASSERT_LE(property_value->ValueInt(), 400);
        if (local_count % 50 == 0) {
          // Make sure the iterator and insertions are interleaved
          std::this_thread::sleep_for(std::chrono::microseconds(10));
        }
        total_processed.fetch_add(1, std::memory_order_relaxed);
      },
      [](int local_count) {
        EXPECT_GT(local_count, 0);
        EXPECT_LT(local_count, 300 / 6 * 3);
      });

  // Stop insertions
  stop_insertions.store(true, std::memory_order_release);
  for (auto &thread : insertion_threads) {
    thread.join();
  }

  // Verify we processed exactly the elements in the range
  ASSERT_EQ(total_processed.load(), 301);  // 400 - 100 + 1 = 301
}

TEST_F(StorageV2ChunkIteratorTest, EdgeTypePropertyIndexChunkingMultipleEdgeTypes) {
  // Create edges with different edge types and property values
  std::vector<Gid> edge_gids;

  {
    auto acc = storage_->Access();
    // Create 100 edges with edge_type_id1_ and property values from 0 to 99
    for (int i = 0; i < 100; ++i) {
      auto vertex_from = acc->CreateVertex();
      auto vertex_to = acc->CreateVertex();
      auto edge = acc->CreateEdge(&vertex_from, &vertex_to, edge_type_id1_);
      ASSERT_TRUE(edge->SetProperty(property_id_, PropertyValue(i)).has_value());
      edge_gids.push_back(edge->Gid());
    }

    // Create 50 edges with edge_type_id2_ and property values from 0 to 49
    for (int i = 0; i < 50; ++i) {
      auto vertex_from = acc->CreateVertex();
      auto vertex_to = acc->CreateVertex();
      auto edge = acc->CreateEdge(&vertex_from, &vertex_to, edge_type_id2_);
      ASSERT_TRUE(edge->SetProperty(property_id_, PropertyValue(i)).has_value());
      edge_gids.push_back(edge->Gid());
    }

    ASSERT_TRUE(acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()).has_value());
  }

  auto acc = storage_->Access();
  // Test chunking for edges with edge_type_id1_ and property in range [20, 80]
  auto edges1 = acc->ChunkedEdges(edge_type_id1_, property_id_, memgraph::utils::MakeBoundInclusive(PropertyValue(20)),
                                  memgraph::utils::MakeBoundInclusive(PropertyValue(80)), View::OLD, 4);

  ASSERT_GT(edges1.size(), 0);

  // Count total edges across all chunks for edge_type_id1_
  std::atomic<int> total_count1{0};
  ProcessChunksInParallel(
      edges1,
      [&total_count1, this](const auto &edge) {
        total_count1.fetch_add(1, std::memory_order_relaxed);
        // Verify property value is in range
        auto property_value = edge.GetProperty(this->property_id_, View::OLD);
        ASSERT_TRUE(property_value.has_value());
        ASSERT_GE(property_value->ValueInt(), 20);
        ASSERT_LE(property_value->ValueInt(), 80);
      },
      [](int local_count) {
        ASSERT_GT(local_count, 0);
        ASSERT_LT(local_count, 61 / 4 * 2);
      });

  // Test chunking for edges with edge_type_id2_ and property in range [10, 40]
  auto edges2 = acc->ChunkedEdges(edge_type_id2_, property_id_, memgraph::utils::MakeBoundInclusive(PropertyValue(10)),
                                  memgraph::utils::MakeBoundInclusive(PropertyValue(40)), View::OLD, 4);

  ASSERT_GT(edges2.size(), 0);

  // Count total edges across all chunks for edge_type_id2_
  std::atomic<int> total_count2{0};
  ProcessChunksInParallel(
      edges2,
      [&total_count2, this](const auto &edge) {
        total_count2.fetch_add(1, std::memory_order_relaxed);
        // Verify property value is in range
        auto property_value = edge.GetProperty(this->property_id_, View::OLD);
        ASSERT_TRUE(property_value.has_value());
        ASSERT_GE(property_value->ValueInt(), 10);
        ASSERT_LE(property_value->ValueInt(), 40);
      },
      [](int local_count) {
        ASSERT_GT(local_count, 0);
        ASSERT_LT(local_count, 31 / 4 * 2);
      });

  // Verify we got the correct counts for each edge type
  ASSERT_EQ(total_count1.load(), 61);  // 80 - 20 + 1 = 61
  ASSERT_EQ(total_count2.load(), 31);  // 40 - 10 + 1 = 31
}

TEST_F(StorageV2ChunkIteratorTest, EdgeTypePropertyIndexChunkingMultipleEntries) {
  // Create edges with specific property values
  std::vector<Gid> edge_gids;
  std::vector<Gid> property_edge_gids;

  // Create 200 edges with property values
  {
    auto acc = storage_->Access();
    for (int i = 0; i < 200; ++i) {
      auto vertex_from = acc->CreateVertex();
      auto vertex_to = acc->CreateVertex();
      auto edge = acc->CreateEdge(&vertex_from, &vertex_to, edge_type_id1_);
      if (i % 3 == 0) {
        ASSERT_TRUE(edge->SetProperty(property_id_, PropertyValue(i)).has_value());
        property_edge_gids.push_back(edge->Gid());
      }
      edge_gids.push_back(edge->Gid());
    }

    ASSERT_TRUE(acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()).has_value());
  }

  // Modify edges to have multiple entries by updating properties
  {
    auto acc = storage_->Access();
    for (const auto gid : property_edge_gids) {
      auto edge = acc->FindEdge(gid, View::OLD);
      ASSERT_TRUE(edge);
      ASSERT_TRUE(edge->SetProperty(property_id_, PropertyValue{}).has_value());
    }
    ASSERT_TRUE(acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()).has_value());
  }
  {
    auto acc = storage_->Access();
    for (const auto gid : property_edge_gids) {
      auto edge = acc->FindEdge(gid, View::OLD);
      ASSERT_TRUE(edge);
      ASSERT_TRUE(edge->SetProperty(property_id_, PropertyValue{edge->Gid().AsInt()}).has_value());
    }
    ASSERT_TRUE(acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()).has_value());
  }

  // Test chunking for edges with edge type and property
  auto acc = storage_->Access();
  auto edges = acc->ChunkedEdges(edge_type_id1_, property_id_, View::OLD, 4);

  ASSERT_GT(edges.size(), 0);

  // Count total edges across all chunks
  std::atomic<int> total_count{0};
  auto read_gids = ProcessChunksInParallel(
      edges, [&total_count](const auto &) { total_count.fetch_add(1, std::memory_order_relaxed); },
      [](int local_count) {
        ASSERT_GT(local_count, 0);
        ASSERT_LT(local_count, 67 / 4 * 2);
      });

  {
    std::sort(property_edge_gids.begin(), property_edge_gids.end());
    ASSERT_EQ(read_gids.size(), property_edge_gids.size());
    std::sort(read_gids.begin(), read_gids.end());
    for (size_t i = 0; i < read_gids.size(); ++i) {
      ASSERT_EQ(read_gids[i], property_edge_gids[i]);
    }
  }
}

TEST_F(StorageV2ChunkIteratorTest, EdgeTypePropertyIndexChunkingDynamic) {
  // Create edges with specific property values
  std::vector<Gid> edge_gids;
  std::vector<Gid> property_edge_gids;

  // Create 200 edges with property values
  {
    auto acc = storage_->Access();
    for (int i = 0; i < 200; ++i) {
      auto vertex_from = acc->CreateVertex();
      auto vertex_to = acc->CreateVertex();
      auto edge = acc->CreateEdge(&vertex_from, &vertex_to, edge_type_id1_);
      if (i % 3 == 0) {
        ASSERT_TRUE(edge->SetProperty(property_id_, PropertyValue(i)).has_value());
        property_edge_gids.push_back(edge->Gid());
      }
      edge_gids.push_back(edge->Gid());
    }

    ASSERT_TRUE(acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()).has_value());
  }

  // Test chunking for edges with edge type and property
  auto acc = storage_->Access();  // Get access before starting the modify thread

  std::atomic_bool run = true;
  auto modify_thread = std::jthread([&run, this, &edge_gids]() {
    while (run) {
      int i = 0;
      auto acc = storage_->Access();
      for (const auto gid : edge_gids) {
        auto edge = acc->FindEdge(gid, View::OLD);
        if (!edge) continue;
        ++i;
        if (i % 2 == 0) {
          ASSERT_TRUE(edge->SetProperty(property_id_, PropertyValue{}).has_value());
        } else if (i % 3 == 0) {
          ASSERT_TRUE(edge->SetProperty(property_id_, PropertyValue{i}).has_value());
        } else if (i % 5 == 0) {
          ASSERT_TRUE(acc->DeleteEdge(&*edge).has_value());
        }
      }
      ASSERT_TRUE(acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()).has_value());
      storage_->FreeMemory();  // Run storage and skiplist gc
      std::this_thread::sleep_for(std::chrono::microseconds(100));
    }
  });

  std::this_thread::sleep_for(std::chrono::milliseconds(1));  // Wait for the modify thread to start
  auto edges = acc->ChunkedEdges(edge_type_id1_, property_id_, View::OLD, 4);

  ASSERT_GT(edges.size(), 0);

  // Count total edges across all chunks
  auto read_gids = ProcessChunksInParallel(
      edges,
      [](const auto &) {
        // No per-item processing needed
      },
      [](int local_count) {
        ASSERT_GT(local_count, 0);
        ASSERT_LT(local_count, 67 / 4 * 2);
      });
  run.store(false);
  modify_thread.join();

  {
    std::sort(property_edge_gids.begin(), property_edge_gids.end());
    ASSERT_EQ(read_gids.size(), property_edge_gids.size());
    std::sort(read_gids.begin(), read_gids.end());
    for (size_t i = 0; i < read_gids.size(); ++i) {
      ASSERT_EQ(read_gids[i], property_edge_gids[i]);
    }
  }
}

TEST_F(StorageV2ChunkIteratorTest, EdgeTypePropertyIndexChunkingManyEntries) {
  // Create edges with specific property values
  std::vector<Gid> edge_gids;
  std::vector<Gid> property_edge_gids;

  // Create 200 edges with property values
  {
    auto acc = storage_->Access();
    for (int i = 0; i < 200; ++i) {
      auto vertex_from = acc->CreateVertex();
      auto vertex_to = acc->CreateVertex();
      auto edge = acc->CreateEdge(&vertex_from, &vertex_to, edge_type_id1_);
      if (i % 3 == 0) {
        ASSERT_TRUE(edge->SetProperty(property_id_, PropertyValue(i)).has_value());
        property_edge_gids.push_back(edge->Gid());
      }
      edge_gids.push_back(edge->Gid());
    }

    ASSERT_TRUE(acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()).has_value());
  }

  // Modify one edge to have many entries
  auto modified_gid = property_edge_gids[50];
  for (int i = 0; i < 100; ++i) {
    {
      auto acc = storage_->Access();
      auto edge = acc->FindEdge(modified_gid, View::OLD);
      ASSERT_TRUE(edge);
      ASSERT_TRUE(edge->SetProperty(property_id_, PropertyValue{}).has_value());
      ASSERT_TRUE(acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()).has_value());
    }
    {
      auto acc = storage_->Access();
      auto edge = acc->FindEdge(modified_gid, View::OLD);
      ASSERT_TRUE(edge);
      ASSERT_TRUE(edge->SetProperty(property_id_, PropertyValue{i}).has_value());
      ASSERT_TRUE(acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()).has_value());
    }
  }

  // Test chunking for edges with edge type and property
  auto acc = storage_->Access();
  auto edges = acc->ChunkedEdges(edge_type_id1_, property_id_, View::OLD, 4);

  ASSERT_GT(edges.size(), 0);

  // Count total edges across all chunks
  std::atomic<int> total_count{0};
  auto read_gids = ProcessChunksInParallel(
      edges, [&total_count](const auto &) { total_count.fetch_add(1, std::memory_order_relaxed); },
      [](int local_count) {
        // NOTE: We don't assert on the count here because we don't know how many same edges are in the chunk.
        // ASSERT_GT(local_count, 0);
        // ASSERT_LT(local_count, 67 / 4 * 2);
      });

  {
    std::sort(property_edge_gids.begin(), property_edge_gids.end());
    ASSERT_EQ(read_gids.size(), property_edge_gids.size());
    std::sort(read_gids.begin(), read_gids.end());
    for (size_t i = 0; i < read_gids.size(); ++i) {
      ASSERT_EQ(read_gids[i], property_edge_gids[i]);
    }
  }
}

TEST_F(StorageV2ChunkIteratorTest, EdgeTypePropertyIndexChunkingComprehensiveEdgeCases) {
  // Create edges with specific property values
  std::vector<Gid> edge_gids;
  std::vector<Gid> property_edge_gids;

  // Empty index
  {
    // Test chunking for edges with edge type and property
    auto acc = storage_->Access();
    auto edges = acc->ChunkedEdges(edge_type_id1_, property_id_, View::OLD, 4);
    ASSERT_EQ(edges.size(), 1);
    auto chunk = edges.get_chunk(0);
    ASSERT_TRUE(chunk.begin() == chunk.end());
  }

  // Fewer edges than chunks
  {
    auto acc = storage_->Access();
    for (int i = 0; i < 2; ++i) {
      auto vertex_from = acc->CreateVertex();
      auto vertex_to = acc->CreateVertex();
      auto edge = acc->CreateEdge(&vertex_from, &vertex_to, edge_type_id1_);
      ASSERT_TRUE(edge->SetProperty(property_id_, PropertyValue(i)).has_value());
      property_edge_gids.push_back(edge->Gid());
    }
    ASSERT_TRUE(acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()).has_value());
    auto edges = acc->ChunkedEdges(edge_type_id1_, property_id_, View::OLD, 4);
    ASSERT_EQ(edges.size(), 2);
  }

  // Exact number of edges and chunks
  {
    auto acc = storage_->Access();
    for (int i = 0; i < 2 * 3; ++i) {
      auto vertex_from = acc->CreateVertex();
      auto vertex_to = acc->CreateVertex();
      auto edge = acc->CreateEdge(&vertex_from, &vertex_to, edge_type_id1_);
      if (i % 3 == 0) {
        ASSERT_TRUE(edge->SetProperty(property_id_, PropertyValue(i)).has_value());
        property_edge_gids.push_back(edge->Gid());
      }
      edge_gids.push_back(edge->Gid());
    }
    ASSERT_EQ(property_edge_gids.size(), 4);
    ASSERT_TRUE(acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()).has_value());
  }
  {
    auto acc = storage_->Access();
    auto edges = acc->ChunkedEdges(edge_type_id1_, property_id_, View::OLD, 4);
    ASSERT_EQ(edges.size(), 4);
    auto read_gids = ProcessChunksInParallel(edges);
    ASSERT_EQ(read_gids.size(), property_edge_gids.size());
    std::sort(read_gids.begin(), read_gids.end());
    for (size_t i = 0; i < read_gids.size(); ++i) {
      ASSERT_EQ(read_gids[i], property_edge_gids[i]);
    }
  }

  // Same 100 edge at the beginning
  {
    const auto first_gid = property_edge_gids[0];
    for (int i = 0; i < 100; ++i) {
      auto acc = storage_->Access();
      auto edge = acc->FindEdge(first_gid, View::OLD);
      ASSERT_TRUE(edge);
      ASSERT_TRUE(edge->SetProperty(property_id_, PropertyValue{}).has_value());
      ASSERT_TRUE(edge->SetProperty(property_id_, PropertyValue{i}).has_value());
      ASSERT_TRUE(acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()).has_value());
    }
    auto acc = storage_->Access();
    auto edges = acc->ChunkedEdges(edge_type_id1_, property_id_, View::OLD, 4);
    // NOTE This is an edge case where we need to skip whole chunks because of duplicates in the index.
    // ASSERT_EQ(edges.size(), 4);
    auto read_gids = ProcessChunksInParallel(edges);
    ASSERT_EQ(read_gids.size(), property_edge_gids.size());
    std::sort(read_gids.begin(), read_gids.end());
    for (size_t i = 0; i < read_gids.size(); ++i) {
      ASSERT_EQ(read_gids[i], property_edge_gids[i]);
    }
  }

  // Same 200 edge at the end
  {
    const auto last_gid = property_edge_gids.back();
    for (int i = 0; i < 200; ++i) {
      auto acc = storage_->Access();
      auto edge = acc->FindEdge(last_gid, View::OLD);
      ASSERT_TRUE(edge);
      ASSERT_TRUE(edge->SetProperty(property_id_, PropertyValue{}).has_value());
      ASSERT_TRUE(edge->SetProperty(property_id_, PropertyValue{i}).has_value());
      ASSERT_TRUE(acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()).has_value());
    }
    auto acc = storage_->Access();
    auto edges = acc->ChunkedEdges(edge_type_id1_, property_id_, View::OLD, 4);
    // NOTE This is an edge case where we need to skip whole chunks because of duplicates in the index.
    // ASSERT_EQ(edges.size(), 4);
    auto read_gids = ProcessChunksInParallel(edges);
    ASSERT_EQ(read_gids.size(), property_edge_gids.size());
    std::sort(read_gids.begin(), read_gids.end());
    for (size_t i = 0; i < read_gids.size(); ++i) {
      ASSERT_EQ(read_gids[i], property_edge_gids[i]);
    }
  }
}

TEST_F(StorageV2ChunkIteratorTest, EdgeTypePropertyIndexChunkingBasicRange) {
  // Create edges with specific property values
  std::vector<Gid> edge_gids;
  std::vector<Gid> property_edge_gids;

  // Create 200 edges with property values
  {
    auto acc = storage_->Access();
    for (int i = 0; i < 200; ++i) {
      auto vertex_from = acc->CreateVertex();
      auto vertex_to = acc->CreateVertex();
      auto edge = acc->CreateEdge(&vertex_from, &vertex_to, edge_type_id1_);
      if (i % 3 == 0) {
        ASSERT_TRUE(edge->SetProperty(property_id_, PropertyValue(i)).has_value());
        property_edge_gids.push_back(edge->Gid());
      }
      edge_gids.push_back(edge->Gid());
    }

    ASSERT_TRUE(acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()).has_value());
  }

  // Checking lower bound
  {
    auto acc = storage_->Access();
    auto edges = acc->ChunkedEdges(edge_type_id1_, property_id_,
                                   memgraph::utils::MakeBoundExclusive(PropertyValue(131)), std::nullopt, View::OLD, 4);
    ASSERT_GT(edges.size(), 0);
    auto first_chunk = edges.get_chunk(0);
    ASSERT_EQ((*first_chunk.begin()).GetProperty(property_id_, View::OLD).value().ValueInt(), 132);
    auto last_chunk = edges.get_chunk(edges.size() - 1);
    PropertyValue last_pv;
    for (auto it = last_chunk.begin(); it != last_chunk.end(); ++it) {
      last_pv = (*it).GetProperty(property_id_, View::OLD).value();
    }
    ASSERT_EQ(last_pv.ValueInt(), 198);
  }
  {
    auto acc = storage_->Access();
    auto edges = acc->ChunkedEdges(edge_type_id1_, property_id_,
                                   memgraph::utils::MakeBoundExclusive(PropertyValue(132)), std::nullopt, View::OLD, 4);
    ASSERT_GT(edges.size(), 0);
    auto first_chunk = edges.get_chunk(0);
    ASSERT_EQ((*first_chunk.begin()).GetProperty(property_id_, View::OLD).value().ValueInt(), 135);
    auto last_chunk = edges.get_chunk(edges.size() - 1);
    PropertyValue last_pv;
    for (auto it = last_chunk.begin(); it != last_chunk.end(); ++it) {
      last_pv = (*it).GetProperty(property_id_, View::OLD).value();
    }
    ASSERT_EQ(last_pv.ValueInt(), 198);
  }
  {
    auto acc = storage_->Access();
    auto edges = acc->ChunkedEdges(edge_type_id1_, property_id_,
                                   memgraph::utils::MakeBoundInclusive(PropertyValue(132)), std::nullopt, View::OLD, 4);
    ASSERT_GT(edges.size(), 0);
    auto first_chunk = edges.get_chunk(0);
    ASSERT_EQ((*first_chunk.begin()).GetProperty(property_id_, View::OLD).value().ValueInt(), 132);
    auto last_chunk = edges.get_chunk(edges.size() - 1);
    PropertyValue last_pv;
    for (auto it = last_chunk.begin(); it != last_chunk.end(); ++it) {
      last_pv = (*it).GetProperty(property_id_, View::OLD).value();
    }
    ASSERT_EQ(last_pv.ValueInt(), 198);
  }
  {
    auto acc = storage_->Access();
    auto edges = acc->ChunkedEdges(edge_type_id1_, property_id_, memgraph::utils::MakeBoundInclusive(PropertyValue(-1)),
                                   std::nullopt, View::OLD, 4);
    ASSERT_EQ(edges.size(), 4);
    auto first_chunk = edges.get_chunk(0);
    ASSERT_EQ((*first_chunk.begin()).GetProperty(property_id_, View::OLD).value().ValueInt(), 0);
    auto last_chunk = edges.get_chunk(edges.size() - 1);
    PropertyValue last_pv;
    for (auto it = last_chunk.begin(); it != last_chunk.end(); ++it) {
      last_pv = (*it).GetProperty(property_id_, View::OLD).value();
    }
    ASSERT_EQ(last_pv.ValueInt(), 198);
  }
  {
    auto acc = storage_->Access();
    auto edges = acc->ChunkedEdges(edge_type_id1_, property_id_,
                                   memgraph::utils::MakeBoundInclusive(PropertyValue(210)), std::nullopt, View::OLD, 4);
    ASSERT_EQ(edges.size(), 1);
    auto chunk = edges.get_chunk(0);
    ASSERT_TRUE(chunk.begin() == chunk.end());
  }

  // Checking upper bound
  {
    auto acc = storage_->Access();
    auto edges = acc->ChunkedEdges(edge_type_id1_, property_id_, std::nullopt,
                                   memgraph::utils::MakeBoundExclusive(PropertyValue(143)), View::OLD, 4);
    ASSERT_EQ(edges.size(), 4);
    auto first_chunk = edges.get_chunk(0);
    ASSERT_EQ((*first_chunk.begin()).GetProperty(property_id_, View::OLD).value().ValueInt(), 0);
    auto last_chunk = edges.get_chunk(edges.size() - 1);
    PropertyValue last_pv;
    for (auto it = last_chunk.begin(); it != last_chunk.end(); ++it) {
      last_pv = (*it).GetProperty(property_id_, View::OLD).value();
    }
    ASSERT_EQ(last_pv.ValueInt(), 141);
  }
  {
    auto acc = storage_->Access();
    auto edges = acc->ChunkedEdges(edge_type_id1_, property_id_, std::nullopt,
                                   memgraph::utils::MakeBoundExclusive(PropertyValue(141)), View::OLD, 4);
    ASSERT_GT(edges.size(), 0);
    auto last_chunk = edges.get_chunk(edges.size() - 1);
    PropertyValue last_pv;
    for (auto it = last_chunk.begin(); it != last_chunk.end(); ++it) {
      last_pv = (*it).GetProperty(property_id_, View::OLD).value();
    }
    ASSERT_EQ(last_pv.ValueInt(), 138);
  }
  {
    auto acc = storage_->Access();
    auto edges = acc->ChunkedEdges(edge_type_id1_, property_id_, std::nullopt,
                                   memgraph::utils::MakeBoundInclusive(PropertyValue(141)), View::OLD, 4);
    ASSERT_GT(edges.size(), 0);
    auto last_chunk = edges.get_chunk(edges.size() - 1);
    PropertyValue last_pv;
    for (auto it = last_chunk.begin(); it != last_chunk.end(); ++it) {
      last_pv = (*it).GetProperty(property_id_, View::OLD).value();
    }
    ASSERT_EQ(last_pv.ValueInt(), 141);
  }
  {
    auto acc = storage_->Access();
    auto edges = acc->ChunkedEdges(edge_type_id1_, property_id_, std::nullopt,
                                   memgraph::utils::MakeBoundInclusive(PropertyValue(210)), View::OLD, 4);
    ASSERT_EQ(edges.size(), 4);
    auto first_chunk = edges.get_chunk(0);
    ASSERT_EQ((*first_chunk.begin()).GetProperty(property_id_, View::OLD).value().ValueInt(), 0);
    auto last_chunk = edges.get_chunk(edges.size() - 1);
    PropertyValue last_pv;
    for (auto it = last_chunk.begin(); it != last_chunk.end(); ++it) {
      last_pv = (*it).GetProperty(property_id_, View::OLD).value();
    }
    ASSERT_EQ(last_pv.ValueInt(), 198);
  }
  {
    auto acc = storage_->Access();
    auto edges = acc->ChunkedEdges(edge_type_id1_, property_id_, std::nullopt,
                                   memgraph::utils::MakeBoundInclusive(PropertyValue(-10)), View::OLD, 4);
    ASSERT_EQ(edges.size(), 1);
    auto chunk = edges.get_chunk(0);
    ASSERT_TRUE(chunk.begin() == chunk.end());
  }

  // Test chunking for edges with edge type and property range
  auto acc = storage_->Access();
  auto edges = acc->ChunkedEdges(edge_type_id1_, property_id_, memgraph::utils::MakeBoundExclusive(PropertyValue(10)),
                                 memgraph::utils::MakeBoundExclusive(PropertyValue(160)), View::OLD, 4);

  ASSERT_GT(edges.size(), 0);

  // Count total edges across all chunks
  auto read_props = ProcessChunksInParallelForPropertyValues(
      edges,
      [this](const auto &edge, std::vector<PropertyValue> &props) {
        const auto prop = edge.GetProperty(property_id_, View::OLD);
        ASSERT_TRUE(prop.has_value());
        ASSERT_TRUE(prop->IsInt());
        props.push_back(prop.value());
      },
      [](int local_count) {
        ASSERT_GT(local_count, 0);
        ASSERT_LT(local_count, 50 / 4 * 2);
      });

  {
    ASSERT_EQ(read_props.size(), 50);  // Every 3rd has the property
    for (size_t i = 0; i < read_props.size(); ++i) {
      ASSERT_EQ(read_props[i], PropertyValue{(static_cast<int>(i) * 3) +
                                             12});  // i*3 + 12 just because of the way we create the dataset
    }
  }

  // Edges cases where both bounds are set
  {
    // Non existing value
    auto acc = storage_->Access();
    auto edges =
        acc->ChunkedEdges(edge_type_id1_, property_id_, memgraph::utils::MakeBoundInclusive(PropertyValue(130)),
                          memgraph::utils::MakeBoundInclusive(PropertyValue(130)), View::OLD, 4);
    ASSERT_EQ(edges.size(), 1);
    auto chunk = edges.get_chunk(0);
    ASSERT_TRUE(chunk.begin() == chunk.end());
  }
  {
    // Exact match
    auto acc = storage_->Access();
    auto edges =
        acc->ChunkedEdges(edge_type_id1_, property_id_, memgraph::utils::MakeBoundInclusive(PropertyValue(132)),
                          memgraph::utils::MakeBoundInclusive(PropertyValue(132)), View::OLD, 4);
    ASSERT_EQ(edges.size(), 1);
    auto chunk = edges.get_chunk(0);
    auto it = chunk.begin();
    ASSERT_TRUE(it != chunk.end());
    ASSERT_EQ((*it).GetProperty(property_id_, View::OLD).value().ValueInt(), 132);
    ++it;
    ASSERT_TRUE(it == chunk.end());
  }
  {
    // Missed match
    auto acc = storage_->Access();
    auto edges =
        acc->ChunkedEdges(edge_type_id1_, property_id_, memgraph::utils::MakeBoundInclusive(PropertyValue(132)),
                          memgraph::utils::MakeBoundExclusive(PropertyValue(132)), View::OLD, 4);
    ASSERT_EQ(edges.size(), 1);
    auto chunk = edges.get_chunk(0);
    ASSERT_TRUE(chunk.begin() == chunk.end());
  }
  {
    // One match
    auto acc = storage_->Access();
    auto edges =
        acc->ChunkedEdges(edge_type_id1_, property_id_, memgraph::utils::MakeBoundInclusive(PropertyValue(131)),
                          memgraph::utils::MakeBoundInclusive(PropertyValue(133)), View::OLD, 4);
    ASSERT_EQ(edges.size(), 1);
    auto chunk = edges.get_chunk(0);
    auto it = chunk.begin();
    ASSERT_TRUE(it != chunk.end());
    ASSERT_EQ((*it).GetProperty(property_id_, View::OLD).value().ValueInt(), 132);
    ++it;
    ASSERT_TRUE(it == chunk.end());
  }
  {
    // Upper bound lower than lower bound
    auto acc = storage_->Access();
    auto edges =
        acc->ChunkedEdges(edge_type_id1_, property_id_, memgraph::utils::MakeBoundInclusive(PropertyValue(130)),
                          memgraph::utils::MakeBoundInclusive(PropertyValue(120)), View::OLD, 4);
    ASSERT_EQ(edges.size(), 0);
  }
}
