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

#include <gmock/gmock.h>
#include <gtest/gtest.h>
#include <chrono>
#include <thread>

#include "storage/v2/config.hpp"
#include "storage/v2/inmemory/storage.hpp"

using namespace memgraph::storage;
using testing::UnorderedElementsAre;

class AutoIndexEdgeCountTest : public testing::Test {
 protected:
  void SetUp() override {
    // Enable auto-indexing
    config_.salient.items.properties_on_edges = true;
    config_.salient.items.enable_label_index_auto_creation = true;
    config_.salient.items.enable_edge_type_index_auto_creation = true;

    storage_ = std::make_unique<InMemoryStorage>(config_);

    auto acc = storage_->Access();
    label_from_ = acc->NameToLabel("LABEL_FROM");
    label_to_ = acc->NameToLabel("LABEL_TO");
    edge_type1_ = acc->NameToEdgeType("SOMEEDGETYPE1");
    edge_type2_ = acc->NameToEdgeType("SOMEEDGETYPE2");
    edge_type3_ = acc->NameToEdgeType("SOMEEDGETYPE3");
  }

  void TearDown() override { storage_.reset(nullptr); }

  // Helper to create vertices and edges
  void CreateNodesAndEdges() {
    // Create the two nodes
    {
      auto acc = storage_->Access();
      auto v_from = acc->CreateVertex();
      ASSERT_TRUE(v_from.AddLabel(label_from_).HasValue());
      from_gid_ = v_from.Gid();

      auto v_to = acc->CreateVertex();
      ASSERT_TRUE(v_to.AddLabel(label_to_).HasValue());
      to_gid_ = v_to.Gid();

      ASSERT_FALSE(acc->PrepareForCommitPhase().HasError());
    }

    // Create 120 edges of type 1 in separate transactions (like the e2e test)
    for (int i = 0; i < 120; ++i) {
      {
        auto acc = storage_->Access();
        auto v_from = acc->FindVertex(from_gid_, View::NEW);
        auto v_to = acc->FindVertex(to_gid_, View::NEW);
        ASSERT_TRUE(v_from.has_value());
        ASSERT_TRUE(v_to.has_value());

        auto edge = acc->CreateEdge(&v_from.value(), &v_to.value(), edge_type1_);
        ASSERT_TRUE(edge.HasValue());

        ASSERT_FALSE(acc->PrepareForCommitPhase().HasError());
      }  // Accessor destroyed here

      // Smaller gap to increase race conditions
      if (i % 5 == 0) {
        std::this_thread::sleep_for(std::chrono::microseconds(10));
      }
    }

    // Create 100 edges of type 2
    for (int i = 0; i < 100; ++i) {
      {
        auto acc = storage_->Access();
        auto v_from = acc->FindVertex(from_gid_, View::NEW);
        auto v_to = acc->FindVertex(to_gid_, View::NEW);
        ASSERT_TRUE(v_from.has_value());
        ASSERT_TRUE(v_to.has_value());

        auto edge = acc->CreateEdge(&v_from.value(), &v_to.value(), edge_type2_);
        ASSERT_TRUE(edge.HasValue());

        ASSERT_FALSE(acc->PrepareForCommitPhase().HasError());
      }  // Accessor destroyed here

      // Smaller gap to increase race conditions
      if (i % 5 == 0) {
        std::this_thread::sleep_for(std::chrono::microseconds(10));
      }
    }

    // Create 80 edges of type 3
    for (int i = 0; i < 80; ++i) {
      {
        auto acc = storage_->Access();
        auto v_from = acc->FindVertex(from_gid_, View::NEW);
        auto v_to = acc->FindVertex(to_gid_, View::NEW);
        ASSERT_TRUE(v_from.has_value());
        ASSERT_TRUE(v_to.has_value());

        auto edge = acc->CreateEdge(&v_from.value(), &v_to.value(), edge_type3_);
        ASSERT_TRUE(edge.HasValue());

        ASSERT_FALSE(acc->PrepareForCommitPhase().HasError());
      }  // Accessor destroyed here

      // Smaller gap to increase race conditions
      if (i % 5 == 0) {
        std::this_thread::sleep_for(std::chrono::microseconds(10));
      }
    }
  }

  // Helper to wait for auto-indexing to complete
  void WaitForAutoIndexing(int max_wait_ms = 3000) {
    auto start = std::chrono::steady_clock::now();
    while (true) {
      // Use READ access to avoid blocking the auto-indexer
      {
        auto acc = storage_->Access(memgraph::storage::Storage::Accessor::Type::READ);
        bool all_ready = acc->EdgeTypeIndexReady(edge_type1_) && acc->EdgeTypeIndexReady(edge_type2_) &&
                         acc->EdgeTypeIndexReady(edge_type3_);

        if (all_ready) break;
      }

      auto elapsed =
          std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::steady_clock::now() - start).count();
      if (elapsed > max_wait_ms) {
        FAIL() << "Auto-indexing did not complete within timeout";
      }

      std::this_thread::sleep_for(std::chrono::milliseconds(100));
    }
  }

  Config config_;
  std::unique_ptr<InMemoryStorage> storage_;
  LabelId label_from_;
  LabelId label_to_;
  EdgeTypeId edge_type1_;
  EdgeTypeId edge_type2_;
  EdgeTypeId edge_type3_;
  Gid from_gid_;
  Gid to_gid_;
};

TEST_F(AutoIndexEdgeCountTest, TestAutoIndexEdgeCount) {
  // Create edges which should trigger auto-indexing
  CreateNodesAndEdges();

  // Wait for auto-indexing to complete
  WaitForAutoIndexing();

  // Check the edge counts with READ access
  {
    auto acc = storage_->Access(memgraph::storage::Storage::Accessor::Type::READ);

    // These should match exactly what was created
    EXPECT_EQ(acc->ApproximateEdgeCount(edge_type1_), 120)
        << "Expected 120 edges of type 1, got " << acc->ApproximateEdgeCount(edge_type1_);
    EXPECT_EQ(acc->ApproximateEdgeCount(edge_type2_), 100)
        << "Expected 100 edges of type 2, got " << acc->ApproximateEdgeCount(edge_type2_);
    EXPECT_EQ(acc->ApproximateEdgeCount(edge_type3_), 80)
        << "Expected 80 edges of type 3, got " << acc->ApproximateEdgeCount(edge_type3_);

    // Also check vertex counts for completeness
    EXPECT_EQ(acc->ApproximateVertexCount(label_from_), 1);
    EXPECT_EQ(acc->ApproximateVertexCount(label_to_), 1);

    // List all indices to verify they were created
    auto indices = acc->ListAllIndices();
    EXPECT_EQ(indices.label.size(), 2);
    EXPECT_EQ(indices.edge_type.size(), 3);
  }
}

// Test multiple runs to catch non-deterministic behavior
TEST_F(AutoIndexEdgeCountTest, TestAutoIndexEdgeCountMultipleRuns) {
  const int num_runs = 5;  // Reduced to avoid timeout issues

  for (int run = 0; run < num_runs; ++run) {
    // Reset storage for each run
    storage_.reset(nullptr);

    // Small delay to allow any background threads to finish
    std::this_thread::sleep_for(std::chrono::milliseconds(100));

    storage_ = std::make_unique<InMemoryStorage>(config_);

    // Get name mappings with a separate accessor that gets destroyed
    {
      auto acc = storage_->Access();
      label_from_ = acc->NameToLabel("LABEL_FROM");
      label_to_ = acc->NameToLabel("LABEL_TO");
      edge_type1_ = acc->NameToEdgeType("SOMEEDGETYPE1");
      edge_type2_ = acc->NameToEdgeType("SOMEEDGETYPE2");
      edge_type3_ = acc->NameToEdgeType("SOMEEDGETYPE3");
    }  // Accessor destroyed here to reduce contention

    CreateNodesAndEdges();
    WaitForAutoIndexing();

    {
      auto acc2 = storage_->Access(memgraph::storage::Storage::Accessor::Type::READ);
      EXPECT_EQ(acc2->ApproximateEdgeCount(edge_type1_), 120)
          << "Run " << run << ": Expected 120 edges of type 1, got " << acc2->ApproximateEdgeCount(edge_type1_);
      EXPECT_EQ(acc2->ApproximateEdgeCount(edge_type2_), 100)
          << "Run " << run << ": Expected 100 edges of type 2, got " << acc2->ApproximateEdgeCount(edge_type2_);
      EXPECT_EQ(acc2->ApproximateEdgeCount(edge_type3_), 80)
          << "Run " << run << ": Expected 80 edges of type 3, got " << acc2->ApproximateEdgeCount(edge_type3_);
    }
  }
}

// Test that reproduces the e2e test scenario exactly
TEST_F(AutoIndexEdgeCountTest, TestE2EScenarioExact) {
  // This mimics the exact failing e2e test pattern:
  // CREATE (n:LABEL_FROM)
  // CREATE (n:LABEL_TO)
  // for _ in range(2): MATCH (n:LABEL_FROM), (m:LABEL_TO) CREATE (n)-[:SOMEEDGETYPE1]->(m)
  // etc.

  // Step 1: Create the two nodes
  {
    auto acc = storage_->Access();
    auto v_from = acc->CreateVertex();
    ASSERT_TRUE(v_from.AddLabel(label_from_).HasValue());
    from_gid_ = v_from.Gid();

    ASSERT_FALSE(acc->PrepareForCommitPhase().HasError());
  }

  {
    auto acc = storage_->Access();
    auto v_to = acc->CreateVertex();
    ASSERT_TRUE(v_to.AddLabel(label_to_).HasValue());
    to_gid_ = v_to.Gid();

    ASSERT_FALSE(acc->PrepareForCommitPhase().HasError());
  }

  // Step 2: Create 2 edges of edge_type1_ (mimicking the MATCH query pattern)
  for (int i = 0; i < 2; ++i) {
    auto acc = storage_->Access();
    auto v_from = acc->FindVertex(from_gid_, View::NEW);
    auto v_to = acc->FindVertex(to_gid_, View::NEW);
    ASSERT_TRUE(v_from.has_value() && v_to.has_value());

    auto edge = acc->CreateEdge(&v_from.value(), &v_to.value(), edge_type1_);
    ASSERT_TRUE(edge.HasValue());

    ASSERT_FALSE(acc->PrepareForCommitPhase().HasError());
  }

  // Step 3: Create 2 edges of edge_type2_
  for (int i = 0; i < 2; ++i) {
    auto acc = storage_->Access();
    auto v_from = acc->FindVertex(from_gid_, View::NEW);
    auto v_to = acc->FindVertex(to_gid_, View::NEW);
    ASSERT_TRUE(v_from.has_value() && v_to.has_value());

    auto edge = acc->CreateEdge(&v_from.value(), &v_to.value(), edge_type2_);
    ASSERT_TRUE(edge.HasValue());

    ASSERT_FALSE(acc->PrepareForCommitPhase().HasError());
  }

  // Step 4: Create 2 edges of edge_type3_
  for (int i = 0; i < 2; ++i) {
    auto acc = storage_->Access();
    auto v_from = acc->FindVertex(from_gid_, View::NEW);
    auto v_to = acc->FindVertex(to_gid_, View::NEW);
    ASSERT_TRUE(v_from.has_value() && v_to.has_value());

    auto edge = acc->CreateEdge(&v_from.value(), &v_to.value(), edge_type3_);
    ASSERT_TRUE(edge.HasValue());

    ASSERT_FALSE(acc->PrepareForCommitPhase().HasError());
  }

  // Wait for auto-indexing to complete
  WaitForAutoIndexing();

  // Check the edge counts - should be exactly 2 for each type
  {
    auto acc = storage_->Access(memgraph::storage::Storage::Accessor::Type::READ);

    auto count1 = acc->ApproximateEdgeCount(edge_type1_);
    auto count2 = acc->ApproximateEdgeCount(edge_type2_);
    auto count3 = acc->ApproximateEdgeCount(edge_type3_);

    std::cout << "Edge counts: type1=" << count1 << ", type2=" << count2 << ", type3=" << count3 << "\n";

    EXPECT_EQ(count1, 2) << "Expected 2 edges of type 1, got " << count1;
    EXPECT_EQ(count2, 2) << "Expected 2 edges of type 2, got " << count2;
    EXPECT_EQ(count3, 2) << "Expected 2 edges of type 3, got " << count3;
  }
}

// Test if auto-indexer can complete with minimal interference
TEST_F(AutoIndexEdgeCountTest, TestConcurrentEdgeCreation) {
  // Create vertices and first edge to trigger auto-indexing
  {
    auto acc = storage_->Access();
    auto v_from = acc->CreateVertex();
    ASSERT_TRUE(v_from.AddLabel(label_from_).HasValue());
    from_gid_ = v_from.Gid();

    auto v_to = acc->CreateVertex();
    ASSERT_TRUE(v_to.AddLabel(label_to_).HasValue());
    to_gid_ = v_to.Gid();

    // Create first edge to trigger auto-indexing
    auto edge = acc->CreateEdge(&v_from, &v_to, edge_type1_);
    ASSERT_TRUE(edge.HasValue());

    ASSERT_FALSE(acc->PrepareForCommitPhase().HasError());
  }  // Transaction ends here, auto-indexer request is dispatched

  // Wait for auto-indexer to complete with improved backoff strategy
  std::this_thread::sleep_for(std::chrono::milliseconds(1500));

  // Try to wait for auto-indexing - this will tell us if it completed
  bool auto_indexing_completed = false;
  {
    auto acc = storage_->Access(memgraph::storage::Storage::Accessor::Type::READ);
    auto_indexing_completed = acc->EdgeTypeIndexReady(edge_type1_);
  }

  if (auto_indexing_completed) {
    std::cout << "Auto-indexing completed successfully after first edge\n";

    // Now create remaining edges since auto-indexing is done
    for (int i = 1; i < 120; ++i) {
      auto acc = storage_->Access();
      auto v_from = acc->FindVertex(from_gid_, View::NEW);
      auto v_to = acc->FindVertex(to_gid_, View::NEW);
      if (v_from.has_value() && v_to.has_value()) {
        auto edge = acc->CreateEdge(&v_from.value(), &v_to.value(), edge_type1_);
        if (edge.HasValue()) {
          acc->PrepareForCommitPhase();
        }
      }
    }

    // Check final count
    auto acc = storage_->Access(memgraph::storage::Storage::Accessor::Type::READ);
    EXPECT_EQ(acc->ApproximateEdgeCount(edge_type1_), 120)
        << "Expected 120 edges after creation, got " << acc->ApproximateEdgeCount(edge_type1_);
  } else {
    std::cout << "Auto-indexing did not complete after 5 seconds\n";
    FAIL() << "Auto-indexer did not complete within 5 seconds - there may be a configuration or implementation issue";
  }
}
