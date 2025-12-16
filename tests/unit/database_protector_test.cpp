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

#include <gtest/gtest.h>
#include <atomic>
#include <chrono>
#include <condition_variable>
#include <mutex>
#include <thread>

#include "storage/v2/config.hpp"
#include "storage/v2/database_protector.hpp"
#include "storage/v2/inmemory/storage.hpp"
#include "tests/test_commit_args_helper.hpp"

namespace {

// Common test protector implementation
struct TestProtector : memgraph::storage::DatabaseProtector {
  auto clone() const -> memgraph::storage::DatabaseProtectorPtr override { return std::make_unique<TestProtector>(); }
};

// Synchronization helper for async indexer testing
struct AsyncIndexerNotifier {
  std::mutex mutex;
  std::condition_variable cv;
  std::atomic<int> call_count{0};
  std::atomic<bool> simulate_database_drop{false};
  std::atomic<bool> first_call_received{false};

  // Wait for the async indexer to make at least one call to the factory
  bool WaitForAsyncActivity(std::chrono::milliseconds timeout = std::chrono::milliseconds(2000)) {
    std::unique_lock<std::mutex> lock(mutex);
    return cv.wait_for(lock, timeout, [this] { return first_call_received.load(); });
  }

  // Create a factory that notifies when called
  auto CreateNotifyingFactory() {
    return [this]() -> std::unique_ptr<memgraph::storage::DatabaseProtector> {
      call_count.fetch_add(1);

      // Notify on first call
      if (!first_call_received.exchange(true)) {
        { std::lock_guard<std::mutex> lock(mutex); }
        cv.notify_all();
      }

      if (simulate_database_drop.load()) {
        return nullptr;  // Simulate database dropped
      }
      return std::make_unique<TestProtector>();
    };
  }
};

// Helper function to check if a specific index is ready by checking indices info
bool IsIndexReady(memgraph::storage::InMemoryStorage *storage, memgraph::storage::LabelId label) {
  try {
    auto acc = storage->Access();
    auto indices_info = acc->ListAllIndices();

    // Check if the label exists in the ready indices
    for (const auto &index_label : indices_info.label) {
      if (index_label == label) {
        auto commit_result = acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs());
        return commit_result.has_value();
      }
    }

    auto commit_result = acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs());
    return false;  // Index not found in ready list
  } catch (...) {
    return false;  // Error accessing storage or indices
  }
}

// Wait for an index to be ready with timeout
bool WaitForIndexReady(memgraph::storage::InMemoryStorage *storage, memgraph::storage::LabelId label,
                       std::chrono::milliseconds timeout = std::chrono::milliseconds(3000)) {
  auto start = std::chrono::steady_clock::now();

  while ((std::chrono::steady_clock::now() - start) < timeout) {
    if (IsIndexReady(storage, label)) {
      return true;
    }
    std::this_thread::sleep_for(std::chrono::milliseconds(5));
  }

  return false;  // Timeout
}

// Wait for async indexer to become idle (no pending work and not processing)
bool WaitForAsyncIndexerIdle(memgraph::storage::InMemoryStorage *storage,
                             std::chrono::milliseconds timeout = std::chrono::milliseconds(2000)) {
  auto start = std::chrono::steady_clock::now();

  while ((std::chrono::steady_clock::now() - start) < timeout) {
    if (storage->IsAsyncIndexerIdle()) {
      return true;
    }
    std::this_thread::sleep_for(std::chrono::milliseconds(2));
  }

  return false;  // Timeout
}

// Wait for async indexer thread to stop (due to null protector or shutdown)
bool WaitForAsyncIndexerStopped(memgraph::storage::InMemoryStorage *storage,
                                std::chrono::milliseconds timeout = std::chrono::milliseconds(2000)) {
  auto start = std::chrono::steady_clock::now();

  while ((std::chrono::steady_clock::now() - start) < timeout) {
    if (storage->HasAsyncIndexerStopped()) {
      return true;
    }
    std::this_thread::sleep_for(std::chrono::milliseconds(2));
  }

  return false;  // Timeout
}

// Helper function to create vertices with a label to trigger auto index creation
void CreateVerticesWithLabel(memgraph::storage::InMemoryStorage *storage, memgraph::storage::LabelId label,
                             int vertex_count) {
  auto acc = storage->Access();
  for (int i = 0; i < vertex_count; ++i) {
    auto vertex = acc->CreateVertex();
    auto add_label = vertex.AddLabel(label);
    if (!add_label) {
      throw std::runtime_error("Failed to add label to vertex");
    }
  }
  auto commit_result = acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs());
  if (!commit_result) {
    throw std::runtime_error("Failed to commit vertex creation transaction");
  }
}

// Helper function to verify vertex count for a given label
size_t CountVerticesWithLabel(memgraph::storage::InMemoryStorage *storage, memgraph::storage::LabelId label) {
  auto acc = storage->Access();
  size_t count = 0;
  for (auto vertex : acc->Vertices(label, memgraph::storage::View::NEW)) {
    (void)vertex;
    ++count;
  }
  auto commit_result = acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs());
  if (!commit_result) {
    return 0;  // Error in transaction
  }
  return count;
}

// Helper function to create a factory that can optionally return nullptr
// This simulates database drop scenarios for testing
auto CreateTestFactory(bool &should_drop, std::atomic<int> *call_counter = nullptr) {
  return [&should_drop, call_counter]() -> std::unique_ptr<memgraph::storage::DatabaseProtector> {
    if (call_counter) {
      call_counter->fetch_add(1);
    }
    if (should_drop) {
      return nullptr;  // Simulate database dropped
    }
    return std::make_unique<TestProtector>();
  };
}

/// Test that verifies the database access protector factory system works correctly
class DatabaseProtectorTest : public ::testing::Test {
 protected:
  void SetUp() override {
    config_.durability.storage_directory = std::filesystem::temp_directory_path() / "db_access_test";
    std::filesystem::remove_all(config_.durability.storage_directory);

    // Enable auto index creation for async indexer tests
    config_.salient.items.enable_label_index_auto_creation = true;
  }

  void TearDown() override { std::filesystem::remove_all(config_.durability.storage_directory); }

  memgraph::storage::Config config_;
};

TEST_F(DatabaseProtectorTest, DefaultSafeFactory) {
  // Default configuration should use safe factory
  auto storage = std::make_unique<memgraph::storage::InMemoryStorage>(config_);

  // Test that make_database_protector always returns a valid protector
  auto protector = storage->make_database_protector();
  EXPECT_NE(protector, nullptr) << "Default safe factory should never return nullptr";
}

TEST_F(DatabaseProtectorTest, FactoryHandlesNullProtector) {
  // Test that basic storage operations continue working even when factory returns nullptr
  // This verifies storage resilience during database drop scenarios (without async complexity)

  bool database_dropped = false;
  auto storage = std::make_unique<memgraph::storage::InMemoryStorage>(
      config_, std::nullopt, std::make_unique<memgraph::storage::PlanInvalidatorDefault>(),
      CreateTestFactory(database_dropped));

  // Simulate database being dropped
  database_dropped = true;
  EXPECT_EQ(storage->make_database_protector(), nullptr) << "Factory should return nullptr when database is dropped";

  // Test that basic storage operations still work even when factory returns nullptr
  {
    auto acc = storage->Access();
    auto vertex = acc->CreateVertex();
    ASSERT_TRUE(vertex.IsVisible(memgraph::storage::View::NEW)) << "Created vertex should be visible";
    auto commit_result = acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs());
    EXPECT_FALSE(!commit_result.has_value()) << "Basic operations should still work";
  }
}

TEST_F(DatabaseProtectorTest, AsyncIndexerStopsWhenProtectorReturnsNull) {
  // Test that the async indexer background thread stops when make_database_protector returns nullptr
  // This simulates proper cleanup when a database is dropped

  AsyncIndexerNotifier notifier;

  // Create storage with auto index creation enabled
  auto storage = std::make_unique<memgraph::storage::InMemoryStorage>(
      config_, std::nullopt, std::make_unique<memgraph::storage::PlanInvalidatorDefault>(),
      notifier.CreateNotifyingFactory());

  // Create vertices with labels to trigger auto index creation
  auto label = storage->NameToLabel("TestLabel");
  CreateVerticesWithLabel(storage.get(), label, 1);

  // Wait for async indexer to start processing (call our factory)
  EXPECT_TRUE(notifier.WaitForAsyncActivity()) << "Async indexer should have started within 2 seconds";

  // Verify factory was called by async indexer
  int calls_before_drop = notifier.call_count.load();
  EXPECT_GT(calls_before_drop, 0) << "Factory should have been called by async indexer";

  // Now simulate database being dropped - this should cause async indexer thread to stop
  notifier.simulate_database_drop = true;

  // Create another vertex with a different label to trigger more async indexing work
  // This will cause the async indexer to encounter the null protector and stop
  auto label2 = storage->NameToLabel("TestLabel2");
  CreateVerticesWithLabel(storage.get(), label2, 1);

  // Wait for the async indexer thread to detect the database drop and stop
  EXPECT_TRUE(WaitForAsyncIndexerStopped(storage.get()))
      << "Async indexer should stop within 2 seconds after database drop";

  // Destroy the storage - this should cleanly shut down the async indexer thread
  // If the thread doesn't stop properly, this could hang
  auto start_time = std::chrono::steady_clock::now();
  storage.reset();
  auto end_time = std::chrono::steady_clock::now();

  auto shutdown_time = std::chrono::duration_cast<std::chrono::milliseconds>(end_time - start_time).count();

  // If async indexer thread stopped properly, shutdown should be quick (< 1 second)
  EXPECT_LT(shutdown_time, 1000) << "Storage shutdown took too long, async indexer may not have stopped";

  // Test passes if we reach here without hanging - async indexer thread stopped correctly
}

TEST_F(DatabaseProtectorTest, AsyncIndexerCompletesBeforeShutdown) {
  // Test that verifies the async indexer completes its work and then stops cleanly

  AsyncIndexerNotifier notifier;

  auto storage = std::make_unique<memgraph::storage::InMemoryStorage>(
      config_, std::nullopt, std::make_unique<memgraph::storage::PlanInvalidatorDefault>(),
      notifier.CreateNotifyingFactory());

  // Create vertices with a label to trigger auto index creation
  auto label = storage->NameToLabel("TestLabel");
  CreateVerticesWithLabel(storage.get(), label, 5);

  // Wait for async indexer to start activity
  EXPECT_TRUE(notifier.WaitForAsyncActivity()) << "Async indexer should have started within 2 seconds";

  // Wait for async indexer to complete its work using proper index readiness checking
  EXPECT_TRUE(WaitForIndexReady(storage.get(), label))
      << "Async indexer should have completed index creation within 3 seconds";

  // Verify the index works correctly by querying indexed vertices using helper
  EXPECT_EQ(CountVerticesWithLabel(storage.get(), label), 5) << "Index should contain all 5 vertices";

  // Record successful factory calls
  int calls_before_drop = notifier.call_count.load();
  EXPECT_GT(calls_before_drop, 0) << "Factory should have been called by async indexer";

  // Now simulate database drop
  notifier.simulate_database_drop = true;

  // Even if we enqueue more work, the async indexer should stop after the database drop
  auto label2 = storage->NameToLabel("TestLabel2");
  CreateVerticesWithLabel(storage.get(), label2, 5);

  // Wait for the async indexer thread to detect the database drop and become idle
  EXPECT_TRUE(WaitForAsyncIndexerIdle(storage.get()))
      << "Async indexer should become idle within 2 seconds after database drop";

  // Storage destruction should be clean and fast since async work is done
  auto start_time = std::chrono::steady_clock::now();
  storage.reset();
  auto end_time = std::chrono::steady_clock::now();

  auto shutdown_time = std::chrono::duration_cast<std::chrono::milliseconds>(end_time - start_time).count();
  EXPECT_LT(shutdown_time, 500) << "Storage shutdown should be fast when async indexer completed";
}

}  // namespace
