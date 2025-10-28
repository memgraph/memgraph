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
#include <optional>
#include <thread>
#include <vector>

#include <fmt/format.h>
#include <gtest/gtest.h>

#include "storage/v2/inmemory/storage.hpp"
#include "storage/v2/property_value.hpp"
#include "storage/v2/storage.hpp"
#include "tests/test_commit_args_helper.hpp"
#include "utils/bound.hpp"

using memgraph::storage::Config;
using memgraph::storage::Gid;
using memgraph::storage::PropertyId;
using memgraph::storage::PropertyValue;
using memgraph::storage::Storage;
using memgraph::storage::View;

// ============================================================================
// EDGE TYPE PROPERTY INDEX CHUNKING TESTS
// ============================================================================

class EdgeTypePropertyIndexChunkingTest : public ::testing::Test {
 protected:
  void SetUp() override {
    storage_ = std::make_unique<memgraph::storage::InMemoryStorage>(config_);
    {
      auto unique_acc = storage_->UniqueAccess();
      edge_type_id1_ = unique_acc->NameToEdgeType("EdgeType1");
      edge_type_id2_ = unique_acc->NameToEdgeType("EdgeType2");
      property_id_ = unique_acc->NameToProperty("property");
      ASSERT_FALSE(unique_acc->CreateIndex(edge_type_id1_, property_id_).HasError());
      ASSERT_FALSE(unique_acc->CreateIndex(edge_type_id2_, property_id_).HasError());
      ASSERT_FALSE(unique_acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()).HasError());
    }
  }

  void TearDown() override { storage_.reset(); }

  std::unique_ptr<Storage> storage_;
  memgraph::storage::EdgeTypeId edge_type_id1_;
  memgraph::storage::EdgeTypeId edge_type_id2_;
  PropertyId property_id_;
  Config config_{.salient = {.items = {.properties_on_edges = true}}};
};

TEST_F(EdgeTypePropertyIndexChunkingTest, BasicEdgeTypePropertyIndexChunking) {
  // Create edges with different property values
  auto acc = storage_->Access();
  std::vector<Gid> edge_gids;

  // Create 100 edges with property values from 0 to 99
  for (int i = 0; i < 100; ++i) {
    auto vertex_from = acc->CreateVertex();
    auto vertex_to = acc->CreateVertex();
    auto edge = acc->CreateEdge(&vertex_from, &vertex_to, edge_type_id1_);
    ASSERT_TRUE(edge->SetProperty(property_id_, PropertyValue(i)).HasValue());
    edge_gids.push_back(edge->Gid());
  }

  ASSERT_FALSE(acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()).HasError());

  // Test chunking for edges with edge type and property
  auto edges = acc->ChunkedEdges(edge_type_id1_, property_id_, View::OLD, 4);

  ASSERT_GT(edges.size(), 0);

  // Count total edges across all chunks
  std::atomic<int> total_count{0};
  std::vector<std::jthread> threads;

  memgraph::utils::Synchronized<std::vector<Gid>> read_gids;
  for (size_t i = 0; i < edges.size(); ++i) {
    threads.emplace_back([&edges, i, &total_count, &read_gids]() {
      std::vector<Gid> gids;
      int local_count = 0;
      auto chunk = edges.get_chunk(i);
      for (auto it = chunk.begin(); it != chunk.end(); ++it) {
        local_count++;
        gids.push_back((*it).Gid());
      }
      ASSERT_GT(local_count, 0);
      ASSERT_LT(local_count, 100 / 4 * 2);
      total_count.fetch_add(local_count, std::memory_order_relaxed);
      auto locked_gids = read_gids.Lock();
      locked_gids->insert(locked_gids->end(), std::make_move_iterator(gids.begin()),
                          std::make_move_iterator(gids.end()));
    });
  }

  threads.clear();

  {
    std::sort(read_gids.Lock()->begin(), read_gids.Lock()->end());
    ASSERT_EQ(total_count.load(), 100);
    ASSERT_EQ(read_gids.Lock()->size(), 100);
  }
}

TEST_F(EdgeTypePropertyIndexChunkingTest, EdgeTypePropertyIndexChunkingWithRange) {
  // Create edges with property values in a specific range
  auto acc = storage_->Access();
  std::vector<Gid> edge_gids;

  // Create 200 edges with property values from 0 to 199
  for (int i = 0; i < 200; ++i) {
    auto vertex_from = acc->CreateVertex();
    auto vertex_to = acc->CreateVertex();
    auto edge = acc->CreateEdge(&vertex_from, &vertex_to, edge_type_id1_);
    ASSERT_TRUE(edge->SetProperty(property_id_, PropertyValue(i)).HasValue());
    edge_gids.push_back(edge->Gid());
  }

  ASSERT_FALSE(acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()).HasError());

  // Test chunking for edges with edge type and property in range [50, 150]
  auto edges = acc->ChunkedEdges(edge_type_id1_, property_id_, memgraph::utils::MakeBoundInclusive(PropertyValue(50)),
                                 memgraph::utils::MakeBoundInclusive(PropertyValue(150)), View::OLD, 4);

  ASSERT_GT(edges.size(), 0);

  // Count total edges across all chunks
  std::atomic<int> total_count{0};
  std::vector<std::jthread> threads;

  memgraph::utils::Synchronized<std::vector<Gid>> read_gids;
  for (size_t i = 0; i < edges.size(); ++i) {
    threads.emplace_back([&edges, i, &total_count, &read_gids, this]() {
      std::vector<Gid> gids;
      int local_count = 0;
      auto chunk = edges.get_chunk(i);
      for (auto it = chunk.begin(); it != chunk.end(); ++it) {
        local_count++;
        gids.push_back((*it).Gid());
        // Verify property value is in range
        auto property_value = (*it).GetProperty(this->property_id_, View::OLD);
        ASSERT_TRUE(property_value.HasValue());
        ASSERT_GE(property_value->ValueInt(), 50);
        ASSERT_LE(property_value->ValueInt(), 150);
      }
      ASSERT_GT(local_count, 0);
      ASSERT_LT(local_count, 101 / 4 * 2);
      total_count.fetch_add(local_count, std::memory_order_relaxed);
      auto locked_gids = read_gids.Lock();
      locked_gids->insert(locked_gids->end(), std::make_move_iterator(gids.begin()),
                          std::make_move_iterator(gids.end()));
    });
  }

  threads.clear();

  {
    std::sort(read_gids.Lock()->begin(), read_gids.Lock()->end());
    ASSERT_EQ(total_count.load(), 101);  // 150 - 50 + 1 = 101
    ASSERT_EQ(read_gids.Lock()->size(), 101);
  }
}

TEST_F(EdgeTypePropertyIndexChunkingTest, EdgeTypePropertyIndexChunkingWithLowerBound) {
  // Create edges with property values
  auto acc = storage_->Access();
  std::vector<Gid> edge_gids;

  // Create 100 edges with property values from 0 to 99
  for (int i = 0; i < 100; ++i) {
    auto vertex_from = acc->CreateVertex();
    auto vertex_to = acc->CreateVertex();
    auto edge = acc->CreateEdge(&vertex_from, &vertex_to, edge_type_id1_);
    ASSERT_TRUE(edge->SetProperty(property_id_, PropertyValue(i)).HasValue());
    edge_gids.push_back(edge->Gid());
  }

  ASSERT_FALSE(acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()).HasError());

  // Test chunking for edges with edge type and property >= 30
  auto edges = acc->ChunkedEdges(edge_type_id1_, property_id_, memgraph::utils::MakeBoundInclusive(PropertyValue(30)),
                                 std::nullopt, View::OLD, 4);

  ASSERT_GT(edges.size(), 0);

  // Count total edges across all chunks
  std::atomic<int> total_count{0};
  std::vector<std::jthread> threads;

  memgraph::utils::Synchronized<std::vector<Gid>> read_gids;
  for (size_t i = 0; i < edges.size(); ++i) {
    threads.emplace_back([&edges, i, &total_count, &read_gids, this]() {
      std::vector<Gid> gids;
      int local_count = 0;
      auto chunk = edges.get_chunk(i);
      for (auto it = chunk.begin(); it != chunk.end(); ++it) {
        local_count++;
        gids.push_back((*it).Gid());
        // Verify property value is >= 30
        auto property_value = (*it).GetProperty(this->property_id_, View::OLD);
        ASSERT_TRUE(property_value.HasValue());
        ASSERT_GE(property_value->ValueInt(), 30);
      }
      ASSERT_GT(local_count, 0);
      ASSERT_LT(local_count, 70 / 4 * 2);
      total_count.fetch_add(local_count, std::memory_order_relaxed);
      auto locked_gids = read_gids.Lock();
      locked_gids->insert(locked_gids->end(), std::make_move_iterator(gids.begin()),
                          std::make_move_iterator(gids.end()));
    });
  }

  threads.clear();

  {
    std::sort(read_gids.Lock()->begin(), read_gids.Lock()->end());
    ASSERT_EQ(total_count.load(), 70);  // 99 - 30 + 1 = 70
    ASSERT_EQ(read_gids.Lock()->size(), 70);
  }
}

TEST_F(EdgeTypePropertyIndexChunkingTest, EdgeTypePropertyIndexChunkingWithUpperBound) {
  // Create edges with property values
  auto acc = storage_->Access();
  std::vector<Gid> edge_gids;

  // Create 100 edges with property values from 0 to 99
  for (int i = 0; i < 100; ++i) {
    auto vertex_from = acc->CreateVertex();
    auto vertex_to = acc->CreateVertex();
    auto edge = acc->CreateEdge(&vertex_from, &vertex_to, edge_type_id1_);
    ASSERT_TRUE(edge->SetProperty(property_id_, PropertyValue(i)).HasValue());
    edge_gids.push_back(edge->Gid());
  }

  ASSERT_FALSE(acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()).HasError());

  // Test chunking for edges with edge type and property <= 70
  auto edges = acc->ChunkedEdges(edge_type_id1_, property_id_, std::nullopt,
                                 memgraph::utils::MakeBoundInclusive(PropertyValue(70)), View::OLD, 4);

  ASSERT_GT(edges.size(), 0);

  // Count total edges across all chunks
  std::atomic<int> total_count{0};
  std::vector<std::jthread> threads;

  memgraph::utils::Synchronized<std::vector<Gid>> read_gids;
  for (size_t i = 0; i < edges.size(); ++i) {
    threads.emplace_back([&edges, i, &total_count, &read_gids, this]() {
      std::vector<Gid> gids;
      int local_count = 0;
      auto chunk = edges.get_chunk(i);
      for (auto it = chunk.begin(); it != chunk.end(); ++it) {
        local_count++;
        gids.push_back((*it).Gid());
        // Verify property value is <= 70
        auto property_value = (*it).GetProperty(this->property_id_, View::OLD);
        ASSERT_TRUE(property_value.HasValue());
        ASSERT_LE(property_value->ValueInt(), 70);
      }
      ASSERT_GT(local_count, 0);
      ASSERT_LT(local_count, 71 / 4 * 2);
      total_count.fetch_add(local_count, std::memory_order_relaxed);
      auto locked_gids = read_gids.Lock();
      locked_gids->insert(locked_gids->end(), std::make_move_iterator(gids.begin()),
                          std::make_move_iterator(gids.end()));
    });
  }

  threads.clear();

  {
    std::sort(read_gids.Lock()->begin(), read_gids.Lock()->end());
    ASSERT_EQ(total_count.load(), 71);  // 70 - 0 + 1 = 71
    ASSERT_EQ(read_gids.Lock()->size(), 71);
  }
}

TEST_F(EdgeTypePropertyIndexChunkingTest, EdgeTypePropertyIndexChunkingEdgeCases) {
  // Create edges with property values
  auto acc = storage_->Access();
  std::vector<Gid> edge_gids;

  // Create 50 edges with property values from 0 to 49
  for (int i = 0; i < 50; ++i) {
    auto vertex_from = acc->CreateVertex();
    auto vertex_to = acc->CreateVertex();
    auto edge = acc->CreateEdge(&vertex_from, &vertex_to, edge_type_id1_);
    ASSERT_TRUE(edge->SetProperty(property_id_, PropertyValue(i)).HasValue());
    edge_gids.push_back(edge->Gid());
  }

  ASSERT_FALSE(acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()).HasError());

  // Test 1: Empty range (lower > upper)
  auto empty_edges =
      acc->ChunkedEdges(edge_type_id1_, property_id_, memgraph::utils::MakeBoundInclusive(PropertyValue(100)),
                        memgraph::utils::MakeBoundInclusive(PropertyValue(50)), View::OLD, 4);
  ASSERT_GT(empty_edges.size(), 0);

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
      ASSERT_TRUE(property_value.HasValue());
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

TEST_F(EdgeTypePropertyIndexChunkingTest, EdgeTypePropertyIndexChunkingBigDataset) {
  // Create edges with property values
  auto acc = storage_->Access();
  std::vector<Gid> edge_gids;

  // Create 1000 edges with property values from 0 to 999
  for (int i = 0; i < 1000; ++i) {
    auto vertex_from = acc->CreateVertex();
    auto vertex_to = acc->CreateVertex();
    auto edge = acc->CreateEdge(&vertex_from, &vertex_to, edge_type_id1_);
    ASSERT_TRUE(edge->SetProperty(property_id_, PropertyValue(i)).HasValue());
    edge_gids.push_back(edge->Gid());
  }

  ASSERT_FALSE(acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()).HasError());

  // Test chunking for edges with edge type and property in range [200, 800]
  auto edges = acc->ChunkedEdges(edge_type_id1_, property_id_, memgraph::utils::MakeBoundInclusive(PropertyValue(200)),
                                 memgraph::utils::MakeBoundInclusive(PropertyValue(800)), View::OLD, 8);

  ASSERT_GT(edges.size(), 0);

  // Count total edges across all chunks
  std::atomic<int> total_count{0};
  std::vector<std::jthread> threads;

  memgraph::utils::Synchronized<std::vector<Gid>> read_gids;
  for (size_t i = 0; i < edges.size(); ++i) {
    threads.emplace_back([&edges, i, &total_count, &read_gids, this]() {
      std::vector<Gid> gids;
      int local_count = 0;
      auto chunk = edges.get_chunk(i);
      for (auto it = chunk.begin(); it != chunk.end(); ++it) {
        local_count++;
        gids.push_back((*it).Gid());
        // Verify property value is in range
        auto property_value = (*it).GetProperty(this->property_id_, View::OLD);
        ASSERT_TRUE(property_value.HasValue());
        ASSERT_GE(property_value->ValueInt(), 200);
        ASSERT_LE(property_value->ValueInt(), 800);
      }
      ASSERT_GT(local_count, 0);
      ASSERT_LT(local_count, 601 / 8 * 2);
      total_count.fetch_add(local_count, std::memory_order_relaxed);
      auto locked_gids = read_gids.Lock();
      locked_gids->insert(locked_gids->end(), std::make_move_iterator(gids.begin()),
                          std::make_move_iterator(gids.end()));
    });
  }

  threads.clear();

  {
    std::sort(read_gids.Lock()->begin(), read_gids.Lock()->end());
    ASSERT_EQ(total_count.load(), 601);  // 800 - 200 + 1 = 601
    ASSERT_EQ(read_gids.Lock()->size(), 601);
  }
}

TEST_F(EdgeTypePropertyIndexChunkingTest, EdgeTypePropertyIndexChunkingConcurrentOperations) {
  // Create initial edges with property values
  auto acc = storage_->Access();
  std::vector<Gid> edge_gids;

  // Create 500 edges with property values from 0 to 499
  for (int i = 0; i < 500; ++i) {
    auto vertex_from = acc->CreateVertex();
    auto vertex_to = acc->CreateVertex();
    auto edge = acc->CreateEdge(&vertex_from, &vertex_to, edge_type_id1_);
    ASSERT_TRUE(edge->SetProperty(property_id_, PropertyValue(i)).HasValue());
    edge_gids.push_back(edge->Gid());
  }

  ASSERT_FALSE(acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()).HasError());

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
        if (edge->SetProperty(property_id_, PropertyValue(new_value++)).HasValue()) {
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
        ASSERT_TRUE(property_value.HasValue());
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

TEST_F(EdgeTypePropertyIndexChunkingTest, EdgeTypePropertyIndexChunkingMultipleEdgeTypes) {
  // Create edges with different edge types and property values
  auto acc = storage_->Access();
  std::vector<Gid> edge_gids;

  // Create 100 edges with edge_type_id1_ and property values from 0 to 99
  for (int i = 0; i < 100; ++i) {
    auto vertex_from = acc->CreateVertex();
    auto vertex_to = acc->CreateVertex();
    auto edge = acc->CreateEdge(&vertex_from, &vertex_to, edge_type_id1_);
    ASSERT_TRUE(edge->SetProperty(property_id_, PropertyValue(i)).HasValue());
    edge_gids.push_back(edge->Gid());
  }

  // Create 50 edges with edge_type_id2_ and property values from 0 to 49
  for (int i = 0; i < 50; ++i) {
    auto vertex_from = acc->CreateVertex();
    auto vertex_to = acc->CreateVertex();
    auto edge = acc->CreateEdge(&vertex_from, &vertex_to, edge_type_id2_);
    ASSERT_TRUE(edge->SetProperty(property_id_, PropertyValue(i)).HasValue());
    edge_gids.push_back(edge->Gid());
  }

  ASSERT_FALSE(acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()).HasError());

  // Test chunking for edges with edge_type_id1_ and property in range [20, 80]
  auto edges1 = acc->ChunkedEdges(edge_type_id1_, property_id_, memgraph::utils::MakeBoundInclusive(PropertyValue(20)),
                                  memgraph::utils::MakeBoundInclusive(PropertyValue(80)), View::OLD, 4);

  ASSERT_GT(edges1.size(), 0);

  // Count total edges across all chunks for edge_type_id1_
  std::atomic<int> total_count1{0};
  std::vector<std::jthread> threads1;

  for (size_t i = 0; i < edges1.size(); ++i) {
    threads1.emplace_back([&edges1, i, &total_count1, this]() {
      int local_count = 0;
      auto chunk = edges1.get_chunk(i);
      for (auto it = chunk.begin(); it != chunk.end(); ++it) {
        local_count++;
        // Verify property value is in range
        auto property_value = (*it).GetProperty(this->property_id_, View::OLD);
        ASSERT_TRUE(property_value.HasValue());
        ASSERT_GE(property_value->ValueInt(), 20);
        ASSERT_LE(property_value->ValueInt(), 80);
      }
      ASSERT_GT(local_count, 0);
      ASSERT_LT(local_count, 61 / 4 * 2);
      total_count1.fetch_add(local_count, std::memory_order_relaxed);
    });
  }

  threads1.clear();

  // Test chunking for edges with edge_type_id2_ and property in range [10, 40]
  auto edges2 = acc->ChunkedEdges(edge_type_id2_, property_id_, memgraph::utils::MakeBoundInclusive(PropertyValue(10)),
                                  memgraph::utils::MakeBoundInclusive(PropertyValue(40)), View::OLD, 4);

  ASSERT_GT(edges2.size(), 0);

  // Count total edges across all chunks for edge_type_id2_
  std::atomic<int> total_count2{0};
  std::vector<std::jthread> threads2;

  for (size_t i = 0; i < edges2.size(); ++i) {
    threads2.emplace_back([&edges2, i, &total_count2, this]() {
      int local_count = 0;
      auto chunk = edges2.get_chunk(i);
      for (auto it = chunk.begin(); it != chunk.end(); ++it) {
        local_count++;
        // Verify property value is in range
        auto property_value = (*it).GetProperty(this->property_id_, View::OLD);
        ASSERT_TRUE(property_value.HasValue());
        ASSERT_GE(property_value->ValueInt(), 10);
        ASSERT_LE(property_value->ValueInt(), 40);
      }
      ASSERT_GT(local_count, 0);
      ASSERT_LT(local_count, 31 / 4 * 2);
      total_count2.fetch_add(local_count, std::memory_order_relaxed);
    });
  }

  threads2.clear();

  // Verify we got the correct counts for each edge type
  ASSERT_EQ(total_count1.load(), 61);  // 80 - 20 + 1 = 61
  ASSERT_EQ(total_count2.load(), 31);  // 40 - 10 + 1 = 31
}
