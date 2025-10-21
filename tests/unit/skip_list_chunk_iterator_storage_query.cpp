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

#include <atomic>
#include <chrono>
#include <iterator>
#include <thread>
#include <vector>

#include <fmt/format.h>
#include <gtest/gtest.h>

#include "query/db_accessor.hpp"
#include "storage/v2/inmemory/storage.hpp"
#include "storage/v2/storage.hpp"
#include "storage/v2/vertex_accessor.hpp"
#include "tests/test_commit_args_helper.hpp"

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

// ============================================================================
// STORAGE LEVEL TESTS
// ============================================================================

class SkipListChunkIteratorStorageTest : public ::testing::Test {
 protected:
  void SetUp() override {
    storage_ = std::make_unique<memgraph::storage::InMemoryStorage>(config_);
    {
      auto unique_acc = storage_->UniqueAccess();
      label_id_ = unique_acc->NameToLabel("label");
      property_id_ = unique_acc->NameToProperty("property");
      ASSERT_FALSE(unique_acc->CreateIndex(label_id_).HasError());
      ASSERT_FALSE(unique_acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()).HasError());
    }
    {
      auto unique_acc = storage_->UniqueAccess();
      ASSERT_FALSE(unique_acc->CreateIndex(label_id_, {property_id_}).HasError());
      ASSERT_FALSE(unique_acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()).HasError());
    }
  }

  void TearDown() override { storage_.reset(); }

  std::unique_ptr<Storage> storage_;
  LabelId label_id_;
  PropertyId property_id_;
  Config config_{.salient = {.items = {.properties_on_edges = true}}};
};

TEST_F(SkipListChunkIteratorStorageTest, AllVerticesChunkIteratorBasic) {
  // Create vertices with different labels
  auto acc = storage_->Access();
  std::vector<Gid> vertex_gids;

  // Create 100 vertices with the test label
  for (int i = 0; i < 100; ++i) {
    auto vertex = acc->CreateVertex();
    ASSERT_TRUE(vertex.AddLabel(label_id_).HasValue());
    vertex_gids.push_back(vertex.Gid());
  }

  // Create 50 vertices without the label
  for (int i = 0; i < 50; ++i) {
    auto vertex = acc->CreateVertex();
    vertex_gids.push_back(vertex.Gid());
  }

  ASSERT_FALSE(acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()).HasError());

  // Test chunking for all vertices
  auto vertices = acc->ChunkedVertices(View::OLD, 4);

  ASSERT_GT(vertices.size(), 0);

  // Count total vertices across all chunks
  std::atomic<int> total_count{0};
  std::vector<std::jthread> threads;

  memgraph::utils::Synchronized<std::vector<Gid>> read_gids;
  for (size_t i = 0; i < vertices.size(); ++i) {
    threads.emplace_back([&vertices, i, &total_count, &read_gids]() {
      std::vector<Gid> gids;
      int local_count = 0;
      auto it = vertices.begin(i);
      while (it != vertices.end(i)) {
        local_count++;
        gids.push_back((*it).Gid());
        ++it;
      }
      ASSERT_GT(local_count, 0);
      ASSERT_LT(local_count, 150 / 4 * 2);
      total_count.fetch_add(local_count, std::memory_order_relaxed);
      auto locked_gids = read_gids.Lock();
      locked_gids->insert(locked_gids->end(), std::make_move_iterator(gids.begin()),
                          std::make_move_iterator(gids.end()));
    });
  }

  threads.clear();

  {
    std::sort(vertex_gids.begin(), vertex_gids.end());
    auto locked_gids = read_gids.Lock();
    ASSERT_EQ(locked_gids->size(), vertex_gids.size());
    std::sort(locked_gids->begin(), locked_gids->end());
    for (size_t i = 0; i < locked_gids->size(); ++i) {
      ASSERT_EQ((*locked_gids)[i], vertex_gids[i]);
    }
  }
}

TEST_F(SkipListChunkIteratorStorageTest, LabelIndexChunkIteratorBasic) {
  // Create vertices with specific label
  std::vector<Gid> vertex_gids;
  std::vector<Gid> labeled_vertex_gids;

  // Create 200 vertices with the test label
  {
    auto acc = storage_->Access();
    for (int i = 0; i < 200; ++i) {
      auto vertex = acc->CreateVertex();
      if (i % 3 == 0) {
        ASSERT_TRUE(vertex.AddLabel(label_id_).HasValue());
        labeled_vertex_gids.push_back(vertex.Gid());
      }
      vertex_gids.push_back(vertex.Gid());
    }

    ASSERT_FALSE(acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()).HasError());
  }

  // Test chunking for labeled vertices
  auto acc = storage_->Access();
  auto vertices = acc->ChunkedVertices(label_id_, View::OLD, 4);

  ASSERT_GT(vertices.size(), 0);

  // Count total vertices across all chunks
  std::atomic<int> total_count{0};
  std::vector<std::jthread> threads;

  memgraph::utils::Synchronized<std::vector<Gid>> read_gids;

  for (size_t i = 0; i < vertices.size(); ++i) {
    threads.emplace_back([&vertices, i, &total_count, &read_gids]() {
      std::vector<Gid> gids;
      int local_count = 0;
      auto it = vertices.begin(i);
      while (it != vertices.end(i)) {
        local_count++;
        gids.push_back((*it).Gid());
        ++it;
      }
      ASSERT_GT(local_count, 0);
      ASSERT_LT(local_count, 67 / 4 * 2);
      total_count.fetch_add(local_count, std::memory_order_relaxed);
      auto locked_gids = read_gids.Lock();
      locked_gids->insert(locked_gids->end(), std::make_move_iterator(gids.begin()),
                          std::make_move_iterator(gids.end()));
    });
  }

  threads.clear();

  {
    std::sort(labeled_vertex_gids.begin(), labeled_vertex_gids.end());
    auto locked_gids = read_gids.Lock();
    ASSERT_EQ(locked_gids->size(), labeled_vertex_gids.size());
    std::sort(locked_gids->begin(), locked_gids->end());
    for (size_t i = 0; i < locked_gids->size(); ++i) {
      ASSERT_EQ((*locked_gids)[i], labeled_vertex_gids[i]);
    }
  }
}

TEST_F(SkipListChunkIteratorStorageTest, LabelIndexChunkIteratorMultipleEntries) {
  // Create vertices with specific label
  std::vector<Gid> vertex_gids;
  std::vector<Gid> labeled_vertex_gids;

  // Create 200 vertices with the test label
  {
    auto acc = storage_->Access();
    for (int i = 0; i < 200; ++i) {
      auto vertex = acc->CreateVertex();
      if (i % 3 == 0) {
        ASSERT_TRUE(vertex.AddLabel(label_id_).HasValue());
        labeled_vertex_gids.push_back(vertex.Gid());
      }
      vertex_gids.push_back(vertex.Gid());
    }

    ASSERT_FALSE(acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()).HasError());
  }

  // Modify labeled vertices to have multiple entries
  {
    auto acc = storage_->Access();
    for (const auto gid : labeled_vertex_gids) {
      auto vertex = acc->FindVertex(gid, View::OLD);
      ASSERT_TRUE(vertex);
      ASSERT_TRUE(vertex->RemoveLabel(label_id_).HasValue());
    }
    ASSERT_FALSE(acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()).HasError());
  }
  {
    auto acc = storage_->Access();
    for (const auto gid : labeled_vertex_gids) {
      auto vertex = acc->FindVertex(gid, View::OLD);
      ASSERT_TRUE(vertex);
      ASSERT_TRUE(vertex->AddLabel(label_id_).HasValue());
    }
    ASSERT_FALSE(acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()).HasError());
  }

  // Test chunking for labeled vertices
  auto acc = storage_->Access();
  auto vertices = acc->ChunkedVertices(label_id_, View::OLD, 4);

  ASSERT_GT(vertices.size(), 0);

  // Count total vertices across all chunks
  std::atomic<int> total_count{0};
  std::vector<std::jthread> threads;

  memgraph::utils::Synchronized<std::vector<Gid>> read_gids;

  for (size_t i = 0; i < vertices.size(); ++i) {
    threads.emplace_back([&vertices, i, &total_count, &read_gids]() {
      std::vector<Gid> gids;
      int local_count = 0;
      auto it = vertices.begin(i);
      while (it != vertices.end(i)) {
        local_count++;
        gids.push_back((*it).Gid());
        ++it;
      }
      ASSERT_GT(local_count, 0);
      ASSERT_LT(local_count, 67 / 4 * 2);
      total_count.fetch_add(local_count, std::memory_order_relaxed);
      auto locked_gids = read_gids.Lock();
      locked_gids->insert(locked_gids->end(), std::make_move_iterator(gids.begin()),
                          std::make_move_iterator(gids.end()));
    });
  }

  threads.clear();

  {
    std::sort(labeled_vertex_gids.begin(), labeled_vertex_gids.end());
    auto locked_gids = read_gids.Lock();
    ASSERT_EQ(locked_gids->size(), labeled_vertex_gids.size());
    std::sort(locked_gids->begin(), locked_gids->end());
    for (size_t i = 0; i < locked_gids->size(); ++i) {
      ASSERT_EQ((*locked_gids)[i], labeled_vertex_gids[i]);
    }
  }
}

TEST_F(SkipListChunkIteratorStorageTest, LabelIndexChunkIteratorDynamic) {
  // Create vertices with specific label
  std::vector<Gid> vertex_gids;
  std::vector<Gid> labeled_vertex_gids;

  // Create 200 vertices with the test label
  {
    auto acc = storage_->Access();
    for (int i = 0; i < 200; ++i) {
      auto vertex = acc->CreateVertex();
      if (i % 3 == 0) {
        ASSERT_TRUE(vertex.AddLabel(label_id_).HasValue());
        labeled_vertex_gids.push_back(vertex.Gid());
      }
      vertex_gids.push_back(vertex.Gid());
    }

    ASSERT_FALSE(acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()).HasError());
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
          ASSERT_TRUE(vertex->RemoveLabel(label_id_).HasValue());
        } else if (i % 3 == 0) {
          ASSERT_TRUE(vertex->AddLabel(label_id_).HasValue());
        } else if (i % 5 == 0) {
          ASSERT_TRUE(acc->DeleteVertex(&*vertex).HasValue());
        }
      }
      ASSERT_FALSE(acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()).HasError());
      storage_->FreeMemory();  // Run storage and skiplist gc
      std::this_thread::sleep_for(std::chrono::microseconds(100));
    }
  });

  std::this_thread::sleep_for(std::chrono::milliseconds(1));  // Wait for the modify thread to start
  auto vertices = acc->ChunkedVertices(label_id_, View::OLD, 4);

  ASSERT_GT(vertices.size(), 0);

  // Count total vertices across all chunks
  std::atomic<int> total_count{0};
  std::vector<std::jthread> threads;

  memgraph::utils::Synchronized<std::vector<Gid>> read_gids;

  for (size_t i = 0; i < vertices.size(); ++i) {
    threads.emplace_back([&vertices, i, &total_count, &read_gids]() {
      std::vector<Gid> gids;
      int local_count = 0;
      auto it = vertices.begin(i);
      while (it != vertices.end(i)) {
        local_count++;
        gids.push_back((*it).Gid());
        ++it;
      }
      ASSERT_GT(local_count, 0);
      ASSERT_LT(local_count, 67 / 4 * 2);
      total_count.fetch_add(local_count, std::memory_order_relaxed);
      auto locked_gids = read_gids.Lock();
      locked_gids->insert(locked_gids->end(), std::make_move_iterator(gids.begin()),
                          std::make_move_iterator(gids.end()));
    });
  }

  threads.clear();
  run.store(false);
  modify_thread.join();

  {
    std::sort(labeled_vertex_gids.begin(), labeled_vertex_gids.end());
    auto locked_gids = read_gids.Lock();
    ASSERT_EQ(locked_gids->size(), labeled_vertex_gids.size());
    std::sort(locked_gids->begin(), locked_gids->end());
    for (size_t i = 0; i < locked_gids->size(); ++i) {
      ASSERT_EQ((*locked_gids)[i], labeled_vertex_gids[i]);
    }
  }
}

TEST_F(SkipListChunkIteratorStorageTest, LabelIndexChunkIteratorManyEntries) {
  // Create vertices with specific label
  std::vector<Gid> vertex_gids;
  std::vector<Gid> labeled_vertex_gids;

  // Create 200 vertices with the test label
  {
    auto acc = storage_->Access();
    for (int i = 0; i < 200; ++i) {
      auto vertex = acc->CreateVertex();
      if (i % 3 == 0) {
        ASSERT_TRUE(vertex.AddLabel(label_id_).HasValue());
        labeled_vertex_gids.push_back(vertex.Gid());
      }
      vertex_gids.push_back(vertex.Gid());
    }

    ASSERT_FALSE(acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()).HasError());
  }

  // Modify labeled vertices to have multiple entries
  auto modified_gid = labeled_vertex_gids[50];
  for (int i = 0; i < 100; ++i) {
    {
      auto acc = storage_->Access();
      auto vertex = acc->FindVertex(modified_gid, View::OLD);
      ASSERT_TRUE(vertex);
      ASSERT_TRUE(vertex->RemoveLabel(label_id_).HasValue());
      ASSERT_FALSE(acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()).HasError());
    }
    {
      auto acc = storage_->Access();
      auto vertex = acc->FindVertex(modified_gid, View::OLD);
      ASSERT_TRUE(vertex);
      ASSERT_TRUE(vertex->AddLabel(label_id_).HasValue());
      ASSERT_FALSE(acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()).HasError());
    }
  }
  // Test chunking for labeled vertices
  auto acc = storage_->Access();
  auto vertices = acc->ChunkedVertices(label_id_, View::OLD, 4);

  ASSERT_GT(vertices.size(), 0);

  // Count total vertices across all chunks
  std::atomic<int> total_count{0};
  std::vector<std::jthread> threads;

  memgraph::utils::Synchronized<std::vector<Gid>> read_gids;

  for (size_t i = 0; i < vertices.size(); ++i) {
    threads.emplace_back([&vertices, i, &total_count, &read_gids]() {
      std::vector<Gid> gids;
      int local_count = 0;
      auto it = vertices.begin(i);
      while (it != vertices.end(i)) {
        local_count++;
        gids.push_back((*it).Gid());
        ++it;
      }
      // NOTE: We don't assert on the count here because we don't know how many same vertices are in the chunk.
      // ASSERT_GT(local_count, 0);
      // ASSERT_LT(local_count, 67 / 4 * 2);
      total_count.fetch_add(local_count, std::memory_order_relaxed);
      auto locked_gids = read_gids.Lock();
      locked_gids->insert(locked_gids->end(), std::make_move_iterator(gids.begin()),
                          std::make_move_iterator(gids.end()));
    });
  }

  threads.clear();

  {
    std::sort(labeled_vertex_gids.begin(), labeled_vertex_gids.end());
    auto locked_gids = read_gids.Lock();
    ASSERT_EQ(locked_gids->size(), labeled_vertex_gids.size());
    std::sort(locked_gids->begin(), locked_gids->end());
    for (size_t i = 0; i < locked_gids->size(); ++i) {
      ASSERT_EQ((*locked_gids)[i], labeled_vertex_gids[i]);
    }
  }
}

TEST_F(SkipListChunkIteratorStorageTest, LabelIndexChunkIteratorEdgeCases) {
  // Create vertices with specific label
  std::vector<Gid> vertex_gids;
  std::vector<Gid> labeled_vertex_gids;

  // Empty index
  {
    // Test chunking for labeled vertices
    auto acc = storage_->Access();
    auto vertices = acc->ChunkedVertices(label_id_, View::OLD, 4);
    ASSERT_EQ(vertices.size(), 1);
    ASSERT_EQ(vertices.begin(0), vertices.end(0));
  }

  // Non existing label (Cannot be tested because we just assert at the index level)

  // Fewer vertices than chunks
  {
    auto acc = storage_->Access();
    for (int i = 0; i < 2; ++i) {
      auto vertex = acc->CreateVertex();
      ASSERT_TRUE(vertex.AddLabel(label_id_).HasValue());
      labeled_vertex_gids.push_back(vertex.Gid());
    }
    ASSERT_FALSE(acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()).HasError());
    auto vertices = acc->ChunkedVertices(label_id_, View::OLD, 4);
    ASSERT_EQ(vertices.size(), 2);
  }

  // Exact number of vertices and chunks
  {
    auto acc = storage_->Access();
    for (int i = 0; i < 2 * 3; ++i) {
      auto vertex = acc->CreateVertex();
      if (i % 3 == 0) {
        ASSERT_TRUE(vertex.AddLabel(label_id_).HasValue());
        labeled_vertex_gids.push_back(vertex.Gid());
      }
      vertex_gids.push_back(vertex.Gid());
    }
    ASSERT_EQ(labeled_vertex_gids.size(), 4);
    ASSERT_FALSE(acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()).HasError());
  }
  {
    memgraph::utils::Synchronized<std::vector<Gid>> read_gids;
    auto acc = storage_->Access();
    auto vertices = acc->ChunkedVertices(label_id_, View::OLD, 4);
    ASSERT_EQ(vertices.size(), 4);
    std::vector<std::jthread> threads;
    for (size_t i = 0; i < vertices.size(); ++i) {
      threads.emplace_back([&vertices, i, &read_gids]() {
        std::vector<Gid> gids;
        auto it = vertices.begin(i);
        while (it != vertices.end(i)) {
          gids.push_back((*it).Gid());
          ++it;
        }
        auto locked_gids = read_gids.Lock();
        locked_gids->insert(locked_gids->end(), std::make_move_iterator(gids.begin()),
                            std::make_move_iterator(gids.end()));
      });
    }
    threads.clear();
    auto locked_gids = read_gids.Lock();
    ASSERT_EQ(locked_gids->size(), labeled_vertex_gids.size());
    std::sort(locked_gids->begin(), locked_gids->end());
    for (size_t i = 0; i < locked_gids->size(); ++i) {
      ASSERT_EQ((*locked_gids)[i], labeled_vertex_gids[i]);
    }
  }

  // Same 100 vertex at the beginning
  {
    const auto first_gid = labeled_vertex_gids[0];
    for (int i = 0; i < 100; ++i) {
      auto acc = storage_->Access();
      auto vertex = acc->FindVertex(first_gid, View::OLD);
      ASSERT_TRUE(vertex);
      ASSERT_TRUE(vertex->RemoveLabel(label_id_).HasValue());
      ASSERT_TRUE(vertex->AddLabel(label_id_).HasValue());
      ASSERT_FALSE(acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()).HasError());
    }
    memgraph::utils::Synchronized<std::vector<Gid>> read_gids;
    auto acc = storage_->Access();
    auto vertices = acc->ChunkedVertices(label_id_, View::OLD, 4);
    // NOTE This is an edge case where we need to skip whole chunks because of duplicates in the index.
    // ASSERT_EQ(vertices.size(), 4);
    std::vector<std::jthread> threads;
    for (size_t i = 0; i < vertices.size(); ++i) {
      threads.emplace_back([&vertices, i, &read_gids]() {
        std::vector<Gid> gids;
        auto it = vertices.begin(i);
        while (it != vertices.end(i)) {
          gids.push_back((*it).Gid());
          ++it;
        }
        auto locked_gids = read_gids.Lock();
        locked_gids->insert(locked_gids->end(), std::make_move_iterator(gids.begin()),
                            std::make_move_iterator(gids.end()));
      });
    }
    threads.clear();
    auto locked_gids = read_gids.Lock();
    ASSERT_EQ(locked_gids->size(), labeled_vertex_gids.size());
    std::sort(locked_gids->begin(), locked_gids->end());
    for (size_t i = 0; i < locked_gids->size(); ++i) {
      ASSERT_EQ((*locked_gids)[i], labeled_vertex_gids[i]);
    }
  }

  // Same 200 vertex at the end
  {
    const auto last_gid = labeled_vertex_gids.back();
    for (int i = 0; i < 200; ++i) {
      auto acc = storage_->Access();
      auto vertex = acc->FindVertex(last_gid, View::OLD);
      ASSERT_TRUE(vertex);
      ASSERT_TRUE(vertex->RemoveLabel(label_id_).HasValue());
      ASSERT_TRUE(vertex->AddLabel(label_id_).HasValue());
      ASSERT_FALSE(acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()).HasError());
    }
    memgraph::utils::Synchronized<std::vector<Gid>> read_gids;
    auto acc = storage_->Access();
    auto vertices = acc->ChunkedVertices(label_id_, View::OLD, 4);
    // NOTE This is an edge case where we need to skip whole chunks because of duplicates in the index.
    // ASSERT_EQ(vertices.size(), 4);
    std::vector<std::jthread> threads;
    for (size_t i = 0; i < vertices.size(); ++i) {
      threads.emplace_back([&vertices, i, &read_gids]() {
        std::vector<Gid> gids;
        auto it = vertices.begin(i);
        while (it != vertices.end(i)) {
          gids.push_back((*it).Gid());
          ++it;
        }
        auto locked_gids = read_gids.Lock();
        locked_gids->insert(locked_gids->end(), std::make_move_iterator(gids.begin()),
                            std::make_move_iterator(gids.end()));
      });
    }
    threads.clear();
    auto locked_gids = read_gids.Lock();
    ASSERT_EQ(locked_gids->size(), labeled_vertex_gids.size());
    std::sort(locked_gids->begin(), locked_gids->end());
    for (size_t i = 0; i < locked_gids->size(); ++i) {
      ASSERT_EQ((*locked_gids)[i], labeled_vertex_gids[i]);
    }
  }
}

// ============================================================================
// LABEL PROPERTY INDEX TESTS
// ============================================================================

TEST_F(SkipListChunkIteratorStorageTest, LabelPropertyIndexChunkIteratorBasic) {
  // Create vertices with specific label and property
  std::vector<Gid> vertex_gids;
  std::vector<Gid> labeled_property_vertex_gids;

  // Create 200 vertices with the test label and property
  {
    auto acc = storage_->Access();
    for (int i = 0; i < 200; ++i) {
      auto vertex = acc->CreateVertex();
      if (i % 3 == 0) {
        ASSERT_TRUE(vertex.AddLabel(label_id_).HasValue());
        ASSERT_TRUE(vertex.SetProperty(property_id_, PropertyValue(i)).HasValue());
        labeled_property_vertex_gids.push_back(vertex.Gid());
      }
      vertex_gids.push_back(vertex.Gid());
    }

    ASSERT_FALSE(acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()).HasError());
  }

  // Test chunking for labeled vertices with property
  auto acc = storage_->Access();
  std::vector<PropertyPath> properties = {{property_id_}};
  std::vector<PropertyValueRange> property_ranges = {};
  auto vertices = acc->ChunkedVertices(label_id_, properties, property_ranges, View::OLD, 4);

  ASSERT_GT(vertices.size(), 0);

  // Count total vertices across all chunks
  std::atomic<int> total_count{0};
  std::vector<std::jthread> threads;

  memgraph::utils::Synchronized<std::vector<Gid>> read_gids;

  for (size_t i = 0; i < vertices.size(); ++i) {
    threads.emplace_back([&vertices, i, &total_count, &read_gids]() {
      std::vector<Gid> gids;
      int local_count = 0;
      auto it = vertices.begin(i);
      while (it != vertices.end(i)) {
        local_count++;
        gids.push_back((*it).Gid());
        ++it;
      }
      ASSERT_GT(local_count, 0);
      ASSERT_LT(local_count, 67 / 4 * 2);
      total_count.fetch_add(local_count, std::memory_order_relaxed);
      auto locked_gids = read_gids.Lock();
      locked_gids->insert(locked_gids->end(), std::make_move_iterator(gids.begin()),
                          std::make_move_iterator(gids.end()));
    });
  }

  threads.clear();

  {
    std::sort(labeled_property_vertex_gids.begin(), labeled_property_vertex_gids.end());
    auto locked_gids = read_gids.Lock();
    ASSERT_EQ(locked_gids->size(), labeled_property_vertex_gids.size());
    std::sort(locked_gids->begin(), locked_gids->end());
    for (size_t i = 0; i < locked_gids->size(); ++i) {
      ASSERT_EQ((*locked_gids)[i], labeled_property_vertex_gids[i]);
    }
  }
}

TEST_F(SkipListChunkIteratorStorageTest, LabelPropertyIndexChunkIteratorMultipleEntries) {
  // Create vertices with specific label and property
  std::vector<Gid> vertex_gids;
  std::vector<Gid> labeled_property_vertex_gids;

  // Create 200 vertices with the test label and property
  {
    auto acc = storage_->Access();
    for (int i = 0; i < 200; ++i) {
      auto vertex = acc->CreateVertex();
      if (i % 3 == 0) {
        ASSERT_TRUE(vertex.AddLabel(label_id_).HasValue());
        ASSERT_TRUE(vertex.SetProperty(property_id_, PropertyValue(i)).HasValue());
        labeled_property_vertex_gids.push_back(vertex.Gid());
      }
      vertex_gids.push_back(vertex.Gid());
    }

    ASSERT_FALSE(acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()).HasError());
  }

  // Modify labeled vertices to have multiple entries
  {
    auto acc = storage_->Access();
    for (const auto gid : labeled_property_vertex_gids) {
      auto vertex = acc->FindVertex(gid, View::OLD);
      ASSERT_TRUE(vertex);
      ASSERT_TRUE(vertex->RemoveLabel(label_id_).HasValue());
    }
    ASSERT_FALSE(acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()).HasError());
  }
  {
    auto acc = storage_->Access();
    for (const auto gid : labeled_property_vertex_gids) {
      auto vertex = acc->FindVertex(gid, View::OLD);
      ASSERT_TRUE(vertex);
      ASSERT_TRUE(vertex->AddLabel(label_id_).HasValue());
    }
    ASSERT_FALSE(acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()).HasError());
  }

  // Test chunking for labeled vertices with property
  auto acc = storage_->Access();
  std::vector<PropertyPath> properties = {{property_id_}};
  std::vector<PropertyValueRange> property_ranges = {};
  auto vertices = acc->ChunkedVertices(label_id_, properties, property_ranges, View::OLD, 4);

  ASSERT_GT(vertices.size(), 0);

  // Count total vertices across all chunks
  std::atomic<int> total_count{0};
  std::vector<std::jthread> threads;

  memgraph::utils::Synchronized<std::vector<Gid>> read_gids;

  for (size_t i = 0; i < vertices.size(); ++i) {
    threads.emplace_back([&vertices, i, &total_count, &read_gids]() {
      std::vector<Gid> gids;
      int local_count = 0;
      auto it = vertices.begin(i);
      while (it != vertices.end(i)) {
        local_count++;
        gids.push_back((*it).Gid());
        ++it;
      }
      ASSERT_GT(local_count, 0);
      ASSERT_LT(local_count, 67 / 4 * 2);
      total_count.fetch_add(local_count, std::memory_order_relaxed);
      auto locked_gids = read_gids.Lock();
      locked_gids->insert(locked_gids->end(), std::make_move_iterator(gids.begin()),
                          std::make_move_iterator(gids.end()));
    });
  }

  threads.clear();

  {
    std::sort(labeled_property_vertex_gids.begin(), labeled_property_vertex_gids.end());
    auto locked_gids = read_gids.Lock();
    ASSERT_EQ(locked_gids->size(), labeled_property_vertex_gids.size());
    std::sort(locked_gids->begin(), locked_gids->end());
    for (size_t i = 0; i < locked_gids->size(); ++i) {
      ASSERT_EQ((*locked_gids)[i], labeled_property_vertex_gids[i]);
    }
  }
}

TEST_F(SkipListChunkIteratorStorageTest, LabelPropertyIndexChunkIteratorDynamic) {
  // Create vertices with specific label and property
  std::vector<Gid> vertex_gids;
  std::vector<Gid> labeled_property_vertex_gids;

  // Create 200 vertices with the test label and property
  {
    auto acc = storage_->Access();
    for (int i = 0; i < 200; ++i) {
      auto vertex = acc->CreateVertex();
      if (i % 3 == 0) {
        ASSERT_TRUE(vertex.AddLabel(label_id_).HasValue());
        ASSERT_TRUE(vertex.SetProperty(property_id_, PropertyValue(i)).HasValue());
        labeled_property_vertex_gids.push_back(vertex.Gid());
      }
      vertex_gids.push_back(vertex.Gid());
    }

    ASSERT_FALSE(acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()).HasError());
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
          ASSERT_TRUE(vertex->RemoveLabel(label_id_).HasValue());
        } else if (i % 3 == 0) {
          ASSERT_TRUE(vertex->AddLabel(label_id_).HasValue());
          ASSERT_TRUE(vertex->SetProperty(property_id_, PropertyValue(i)).HasValue());
        } else if (i % 5 == 0) {
          ASSERT_TRUE(acc->DeleteVertex(&*vertex).HasValue());
        }
      }
      ASSERT_FALSE(acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()).HasError());
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
  std::vector<std::jthread> threads;

  memgraph::utils::Synchronized<std::vector<Gid>> read_gids;

  for (size_t i = 0; i < vertices.size(); ++i) {
    threads.emplace_back([&vertices, i, &total_count, &read_gids]() {
      std::vector<Gid> gids;
      int local_count = 0;
      auto it = vertices.begin(i);
      while (it != vertices.end(i)) {
        local_count++;
        gids.push_back((*it).Gid());
        ++it;
      }
      ASSERT_GT(local_count, 0);
      ASSERT_LT(local_count, 67 / 4 * 2);
      total_count.fetch_add(local_count, std::memory_order_relaxed);
      auto locked_gids = read_gids.Lock();
      locked_gids->insert(locked_gids->end(), std::make_move_iterator(gids.begin()),
                          std::make_move_iterator(gids.end()));
    });
  }

  threads.clear();
  run.store(false);
  modify_thread.join();

  {
    std::sort(labeled_property_vertex_gids.begin(), labeled_property_vertex_gids.end());
    auto locked_gids = read_gids.Lock();
    ASSERT_EQ(locked_gids->size(), labeled_property_vertex_gids.size());
    std::sort(locked_gids->begin(), locked_gids->end());
    for (size_t i = 0; i < locked_gids->size(); ++i) {
      ASSERT_EQ((*locked_gids)[i], labeled_property_vertex_gids[i]);
    }
  }
}

TEST_F(SkipListChunkIteratorStorageTest, LabelPropertyIndexChunkIteratorManyEntries) {
  // Create vertices with specific label and property
  std::vector<Gid> vertex_gids;
  std::vector<Gid> labeled_property_vertex_gids;

  // Create 200 vertices with the test label and property
  {
    auto acc = storage_->Access();
    for (int i = 0; i < 200; ++i) {
      auto vertex = acc->CreateVertex();
      if (i % 3 == 0) {
        ASSERT_TRUE(vertex.AddLabel(label_id_).HasValue());
        ASSERT_TRUE(vertex.SetProperty(property_id_, PropertyValue(i)).HasValue());
        labeled_property_vertex_gids.push_back(vertex.Gid());
      }
      vertex_gids.push_back(vertex.Gid());
    }

    ASSERT_FALSE(acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()).HasError());
  }

  // Modify labeled vertices to have multiple entries
  auto modified_gid = labeled_property_vertex_gids[50];
  for (int i = 0; i < 100; ++i) {
    {
      auto acc = storage_->Access();
      auto vertex = acc->FindVertex(modified_gid, View::OLD);
      ASSERT_TRUE(vertex);
      ASSERT_TRUE(vertex->SetProperty(property_id_, PropertyValue{}).HasValue());
      ASSERT_FALSE(acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()).HasError());
    }
    {
      auto acc = storage_->Access();
      auto vertex = acc->FindVertex(modified_gid, View::OLD);
      ASSERT_TRUE(vertex);
      ASSERT_TRUE(vertex->SetProperty(property_id_, PropertyValue{i}).HasValue());
      ASSERT_FALSE(acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()).HasError());
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
  std::vector<std::jthread> threads;

  memgraph::utils::Synchronized<std::vector<Gid>> read_gids;

  for (size_t i = 0; i < vertices.size(); ++i) {
    threads.emplace_back([&vertices, i, &total_count, &read_gids]() {
      std::vector<Gid> gids;
      int local_count = 0;
      auto it = vertices.begin(i);
      while (it != vertices.end(i)) {
        local_count++;
        gids.push_back((*it).Gid());
        ++it;
      }
      // NOTE: We don't assert on the count here because we don't know how many same vertices are in the chunk.
      // ASSERT_GT(local_count, 0);
      // ASSERT_LT(local_count, 67 / 4 * 2);
      total_count.fetch_add(local_count, std::memory_order_relaxed);
      auto locked_gids = read_gids.Lock();
      locked_gids->insert(locked_gids->end(), std::make_move_iterator(gids.begin()),
                          std::make_move_iterator(gids.end()));
    });
  }

  threads.clear();

  {
    std::sort(labeled_property_vertex_gids.begin(), labeled_property_vertex_gids.end());
    auto locked_gids = read_gids.Lock();
    ASSERT_EQ(locked_gids->size(), labeled_property_vertex_gids.size());
    std::sort(locked_gids->begin(), locked_gids->end());
    for (size_t i = 0; i < locked_gids->size(); ++i) {
      ASSERT_EQ((*locked_gids)[i], labeled_property_vertex_gids[i]);
    }
  }
}

TEST_F(SkipListChunkIteratorStorageTest, LabelPropertyIndexChunkIteratorEdgeCases) {
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
    ASSERT_EQ(vertices.begin(0), vertices.end(0));
  }

  // Non existing label (Cannot be tested because we just assert at the index level)

  // Fewer vertices than chunks
  {
    auto acc = storage_->Access();
    for (int i = 0; i < 2; ++i) {
      auto vertex = acc->CreateVertex();
      ASSERT_TRUE(vertex.AddLabel(label_id_).HasValue());
      ASSERT_TRUE(vertex.SetProperty(property_id_, PropertyValue(i)).HasValue());
      labeled_property_vertex_gids.push_back(vertex.Gid());
    }
    ASSERT_FALSE(acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()).HasError());
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
        ASSERT_TRUE(vertex.AddLabel(label_id_).HasValue());
        ASSERT_TRUE(vertex.SetProperty(property_id_, PropertyValue(i)).HasValue());
        labeled_property_vertex_gids.push_back(vertex.Gid());
      }
      vertex_gids.push_back(vertex.Gid());
    }
    ASSERT_EQ(labeled_property_vertex_gids.size(), 4);
    ASSERT_FALSE(acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()).HasError());
  }
  {
    memgraph::utils::Synchronized<std::vector<Gid>> read_gids;
    auto acc = storage_->Access();
    std::vector<PropertyPath> properties = {{property_id_}};
    std::vector<PropertyValueRange> property_ranges = {};
    auto vertices = acc->ChunkedVertices(label_id_, properties, property_ranges, View::OLD, 4);
    ASSERT_EQ(vertices.size(), 4);
    std::vector<std::jthread> threads;
    for (size_t i = 0; i < vertices.size(); ++i) {
      threads.emplace_back([&vertices, i, &read_gids]() {
        std::vector<Gid> gids;
        auto it = vertices.begin(i);
        while (it != vertices.end(i)) {
          gids.push_back((*it).Gid());
          ++it;
        }
        auto locked_gids = read_gids.Lock();
        locked_gids->insert(locked_gids->end(), std::make_move_iterator(gids.begin()),
                            std::make_move_iterator(gids.end()));
      });
    }
    threads.clear();
    auto locked_gids = read_gids.Lock();
    ASSERT_EQ(locked_gids->size(), labeled_property_vertex_gids.size());
    std::sort(locked_gids->begin(), locked_gids->end());
    for (size_t i = 0; i < locked_gids->size(); ++i) {
      ASSERT_EQ((*locked_gids)[i], labeled_property_vertex_gids[i]);
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
        ASSERT_TRUE(vertex->RemoveLabel(label_id_).HasValue());
        ASSERT_TRUE(vertex->AddLabel(label_id_).HasValue());
        ASSERT_FALSE(acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()).HasError());
      }
      {
        auto acc = storage_->Access();
        auto vertex = acc->FindVertex(first_gid, View::OLD);
        ASSERT_TRUE(vertex);
        ASSERT_TRUE(vertex->SetProperty(property_id_, PropertyValue{}).HasValue());
        ASSERT_TRUE(vertex->SetProperty(property_id_, PropertyValue{i}).HasValue());
        ASSERT_FALSE(acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()).HasError());
      }
    }
    memgraph::utils::Synchronized<std::vector<Gid>> read_gids;
    auto acc = storage_->Access();
    std::vector<PropertyPath> properties = {{property_id_}};
    std::vector<PropertyValueRange> property_ranges = {};
    auto vertices = acc->ChunkedVertices(label_id_, properties, property_ranges, View::OLD, 4);
    // NOTE This is an edge case where we need to skip whole chunks because of duplicates in the index.
    // ASSERT_EQ(vertices.size(), 4);
    std::vector<std::jthread> threads;
    for (size_t i = 0; i < vertices.size(); ++i) {
      threads.emplace_back([&vertices, i, &read_gids]() {
        std::vector<Gid> gids;
        auto it = vertices.begin(i);
        while (it != vertices.end(i)) {
          gids.push_back((*it).Gid());
          ++it;
        }
        auto locked_gids = read_gids.Lock();
        locked_gids->insert(locked_gids->end(), std::make_move_iterator(gids.begin()),
                            std::make_move_iterator(gids.end()));
      });
    }
    threads.clear();
    auto locked_gids = read_gids.Lock();
    ASSERT_EQ(locked_gids->size(), labeled_property_vertex_gids.size());
    std::sort(locked_gids->begin(), locked_gids->end());
    for (size_t i = 0; i < locked_gids->size(); ++i) {
      ASSERT_EQ((*locked_gids)[i], labeled_property_vertex_gids[i]);
    }
  }

  // Same 200 vertex at the end
  {
    const auto last_gid = labeled_property_vertex_gids.back();
    for (int i = 0; i < 200; ++i) {
      {
        auto acc = storage_->Access();
        auto vertex = acc->FindVertex(last_gid, View::OLD);
        ASSERT_TRUE(vertex);
        ASSERT_TRUE(vertex->RemoveLabel(label_id_).HasValue());
        ASSERT_TRUE(vertex->AddLabel(label_id_).HasValue());
        ASSERT_FALSE(acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()).HasError());
      }
      {
        auto acc = storage_->Access();
        auto vertex = acc->FindVertex(last_gid, View::OLD);
        ASSERT_TRUE(vertex);
        ASSERT_TRUE(vertex->SetProperty(property_id_, PropertyValue{}).HasValue());
        ASSERT_TRUE(vertex->SetProperty(property_id_, PropertyValue{i}).HasValue());
        ASSERT_FALSE(acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()).HasError());
      }
    }
    memgraph::utils::Synchronized<std::vector<Gid>> read_gids;
    auto acc = storage_->Access();
    std::vector<PropertyPath> properties = {{property_id_}};
    std::vector<PropertyValueRange> property_ranges = {};
    auto vertices = acc->ChunkedVertices(label_id_, properties, property_ranges, View::OLD, 4);
    // NOTE This is an edge case where we need to skip whole chunks because of duplicates in the index.
    // ASSERT_EQ(vertices.size(), 4);
    std::vector<std::jthread> threads;
    for (size_t i = 0; i < vertices.size(); ++i) {
      threads.emplace_back([&vertices, i, &read_gids]() {
        std::vector<Gid> gids;
        auto it = vertices.begin(i);
        while (it != vertices.end(i)) {
          gids.push_back((*it).Gid());
          ++it;
        }
        auto locked_gids = read_gids.Lock();
        locked_gids->insert(locked_gids->end(), std::make_move_iterator(gids.begin()),
                            std::make_move_iterator(gids.end()));
      });
    }
    threads.clear();
    auto locked_gids = read_gids.Lock();
    ASSERT_EQ(locked_gids->size(), labeled_property_vertex_gids.size());
    std::sort(locked_gids->begin(), locked_gids->end());
    for (size_t i = 0; i < locked_gids->size(); ++i) {
      ASSERT_EQ((*locked_gids)[i], labeled_property_vertex_gids[i]);
    }
  }
}
