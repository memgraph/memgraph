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
#include "storage/v2/storage.hpp"
#include "tests/test_commit_args_helper.hpp"

using memgraph::storage::Config;
using memgraph::storage::Gid;
using memgraph::storage::Storage;
using memgraph::storage::View;

// ============================================================================
// EDGE TYPE INDEX CHUNKING TESTS
// ============================================================================

class EdgeTypeIndexChunkingTest : public ::testing::Test {
 protected:
  void SetUp() override {
    storage_ = std::make_unique<memgraph::storage::InMemoryStorage>(config_);
    {
      auto unique_acc = storage_->UniqueAccess();
      edge_type_id1_ = unique_acc->NameToEdgeType("EdgeType1");
      edge_type_id2_ = unique_acc->NameToEdgeType("EdgeType2");
      ASSERT_FALSE(unique_acc->CreateIndex(edge_type_id1_).HasError());
      ASSERT_FALSE(unique_acc->CreateIndex(edge_type_id2_).HasError());
      ASSERT_FALSE(unique_acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()).HasError());
    }
  }

  void TearDown() override { storage_.reset(); }

  std::unique_ptr<Storage> storage_;
  memgraph::storage::EdgeTypeId edge_type_id1_;
  memgraph::storage::EdgeTypeId edge_type_id2_;
  Config config_{.salient = {.items = {.properties_on_edges = true}}};
};

TEST_F(EdgeTypeIndexChunkingTest, BasicEdgeTypeIndexChunking) {
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

    ASSERT_FALSE(acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()).HasError());
  }

  auto acc = storage_->Access();
  // Test chunking for edges with edge type
  auto edges = acc->ChunkedEdges(edge_type_id1_, View::OLD, 4);

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
    auto locked_gids = read_gids.Lock();
    std::sort(locked_gids->begin(), locked_gids->end());
    ASSERT_EQ(total_count.load(), 100);
    ASSERT_EQ(locked_gids->size(), 100);
  }
}

TEST_F(EdgeTypeIndexChunkingTest, EdgeTypeIndexChunkingBigDataset) {
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
        auto edge = acc->CreateEdge(&vertex_from, &vertex_to, edge_type_id2_);
        edge_gids.push_back(edge->Gid());
      }
    }

    ASSERT_FALSE(acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()).HasError());
  }

  auto acc = storage_->Access();
  // Test chunking for edges with edge type
  auto edges = acc->ChunkedEdges(edge_type_id1_, View::OLD, 8);

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
      ASSERT_LT(local_count, 50000 / 8 * 2);
      total_count.fetch_add(local_count, std::memory_order_relaxed);
      auto locked_gids = read_gids.Lock();
      locked_gids->insert(locked_gids->end(), std::make_move_iterator(gids.begin()),
                          std::make_move_iterator(gids.end()));
    });
  }

  threads.clear();

  {
    auto locked_gids = read_gids.Lock();
    std::sort(locked_gids->begin(), locked_gids->end());
    ASSERT_EQ(total_count.load(), 50000);
    ASSERT_EQ(locked_gids->size(), 50000);
  }
}

TEST_F(EdgeTypeIndexChunkingTest, EdgeTypeIndexChunkingConcurrentOperations) {
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

    ASSERT_FALSE(acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()).HasError());
  }

  auto acc = storage_->Access();
  // Test chunking for edges with edge type
  auto edges = acc->ChunkedEdges(edge_type_id1_, View::OLD, 6);

  ASSERT_GT(edges.size(), 0);

  // Start concurrent insertion threads
  std::atomic<bool> stop_insertions{false};
  std::atomic<int> inserted_count{0};

  std::vector<std::jthread> insertion_threads;
  for (int t = 0; t < 4; ++t) {
    insertion_threads.emplace_back([this, &stop_insertions, &inserted_count]() {
      auto thread_acc = storage_->Access();
      while (!stop_insertions.load(std::memory_order_acquire)) {
        auto vertex_from = thread_acc->CreateVertex();
        auto vertex_to = thread_acc->CreateVertex();
        auto edge = thread_acc->CreateEdge(&vertex_from, &vertex_to, edge_type_id1_);
        if (edge.HasValue()) {
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
    processing_threads.emplace_back([&edges, i, &total_processed]() {
      int local_count = 0;
      auto chunk = edges.get_chunk(i);
      for (auto it = chunk.begin(); it != chunk.end(); ++it) {
        local_count++;
        if (local_count % 50 == 0) {
          // Make sure the iterator and insertions are interleaved
          std::this_thread::sleep_for(std::chrono::microseconds(10));
        }
      }
      EXPECT_GT(local_count, 0);
      EXPECT_LT(local_count, 500 / 6 * 3);
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

  // Verify we processed exactly the initial elements
  ASSERT_EQ(total_processed.load(), 500);
}

TEST_F(EdgeTypeIndexChunkingTest, EdgeTypeIndexChunkingMultipleEdgeTypes) {
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

    ASSERT_FALSE(acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()).HasError());
  }

  auto acc = storage_->Access();

  // Test chunking for edges with edge_type_id1_
  auto edges1 = acc->ChunkedEdges(edge_type_id1_, View::OLD, 4);

  ASSERT_GT(edges1.size(), 0);

  // Count total edges across all chunks for edge_type_id1_
  std::atomic<int> total_count1{0};
  std::vector<std::jthread> threads1;

  for (size_t i = 0; i < edges1.size(); ++i) {
    threads1.emplace_back([&edges1, i, &total_count1]() {
      int local_count = 0;
      auto chunk = edges1.get_chunk(i);
      for (auto it = chunk.begin(); it != chunk.end(); ++it) {
        local_count++;
      }
      ASSERT_GT(local_count, 0);
      ASSERT_LT(local_count, 100 / 4 * 2);
      total_count1.fetch_add(local_count, std::memory_order_relaxed);
    });
  }

  threads1.clear();

  // Test chunking for edges with edge_type_id2_
  auto edges2 = acc->ChunkedEdges(edge_type_id2_, View::OLD, 4);

  ASSERT_GT(edges2.size(), 0);

  // Count total edges across all chunks for edge_type_id2_
  std::atomic<int> total_count2{0};
  std::vector<std::jthread> threads2;

  for (size_t i = 0; i < edges2.size(); ++i) {
    threads2.emplace_back([&edges2, i, &total_count2]() {
      int local_count = 0;
      auto chunk = edges2.get_chunk(i);
      for (auto it = chunk.begin(); it != chunk.end(); ++it) {
        local_count++;
      }
      ASSERT_GT(local_count, 0);
      ASSERT_LT(local_count, 50 / 4 * 2);
      total_count2.fetch_add(local_count, std::memory_order_relaxed);
    });
  }

  threads2.clear();

  // Verify we got the correct counts for each edge type
  ASSERT_EQ(total_count1.load(), 100);
  ASSERT_EQ(total_count2.load(), 50);
}

TEST_F(EdgeTypeIndexChunkingTest, EdgeTypeIndexChunkingEdgeCases) {
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

    ASSERT_FALSE(acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()).HasError());
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

TEST_F(EdgeTypeIndexChunkingTest, EdgeTypeIndexChunkingDynamic) {
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

    ASSERT_FALSE(acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()).HasError());
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
          ASSERT_TRUE(acc->DeleteEdge(&*edge).HasValue());
        }
      }
      ASSERT_FALSE(acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()).HasError());
      storage_->FreeMemory();  // Run storage and skiplist gc
      std::this_thread::sleep_for(std::chrono::microseconds(100));
    }
  });

  std::this_thread::sleep_for(std::chrono::milliseconds(1));  // Wait for the modify thread to start
  auto edges = acc2->ChunkedEdges(edge_type_id1_, View::OLD, 4);

  ASSERT_GT(edges.size(), 0);

  // Count total edges across all chunks
  std::vector<std::jthread> threads;

  memgraph::utils::Synchronized<std::vector<Gid>> read_gids;

  for (size_t i = 0; i < edges.size(); ++i) {
    threads.emplace_back([&edges, i, &read_gids]() {
      std::vector<Gid> gids;
      int local_count = 0;
      auto chunk = edges.get_chunk(i);
      for (auto it = chunk.begin(); it != chunk.end(); ++it) {
        local_count++;
        gids.push_back((*it).Gid());
      }
      ASSERT_GT(local_count, 0);
      ASSERT_LT(local_count, 200 / 4 * 2);
      auto locked_gids = read_gids.Lock();
      locked_gids->insert(locked_gids->end(), std::make_move_iterator(gids.begin()),
                          std::make_move_iterator(gids.end()));
    });
  }

  threads.clear();
  run.store(false);
  modify_thread.join();

  {
    auto locked_gids = read_gids.Lock();
    ASSERT_GT(locked_gids->size(), 0);
    ASSERT_LE(locked_gids->size(), 200);
  }
}

TEST_F(EdgeTypeIndexChunkingTest, EdgeTypeIndexChunkingComprehensiveEdgeCases) {
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
    ASSERT_FALSE(acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()).HasError());
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
    ASSERT_FALSE(acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()).HasError());
  }
  {
    memgraph::utils::Synchronized<std::vector<Gid>> read_gids;
    auto acc = storage_->Access();
    auto edges = acc->ChunkedEdges(edge_type_id1_, View::OLD, 4);
    ASSERT_EQ(edges.size(), 4);
    std::vector<std::jthread> threads;
    for (size_t i = 0; i < edges.size(); ++i) {
      threads.emplace_back([&edges, i, &read_gids]() {
        std::vector<Gid> gids;
        auto chunk = edges.get_chunk(i);
        for (auto it = chunk.begin(); it != chunk.end(); ++it) {
          gids.push_back((*it).Gid());
        }
        auto locked_gids = read_gids.Lock();
        locked_gids->insert(locked_gids->end(), std::make_move_iterator(gids.begin()),
                            std::make_move_iterator(gids.end()));
      });
    }
    threads.clear();
    auto locked_gids = read_gids.Lock();
    ASSERT_EQ(locked_gids->size(), 4);
    std::sort(locked_gids->begin(), locked_gids->end());
    for (size_t i = 0; i < locked_gids->size(); ++i) {
      ASSERT_EQ((*locked_gids)[i], edge_gids[i]);
    }
  }
}
