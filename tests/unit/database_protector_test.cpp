// Copyright 2026 Memgraph Ltd.
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

#include "metrics/prometheus_metrics.hpp"
#include "storage/v2/config.hpp"
#include "storage/v2/database_protector.hpp"
#include "storage/v2/inmemory/storage.hpp"
#include "tests/test_commit_args_helper.hpp"
#include "utils/gatekeeper.hpp"

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
        {
          std::lock_guard<std::mutex> lock(mutex);
        }
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
    auto acc = storage->Access(memgraph::storage::WRITE);
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
  auto acc = storage->Access(memgraph::storage::WRITE);
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
  auto acc = storage->Access(memgraph::storage::WRITE);
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
      config_,
      std::nullopt,
      std::make_unique<memgraph::storage::PlanInvalidatorDefault>(),
      memgraph::metrics::DatabaseMetricHandles{},
      CreateTestFactory(database_dropped));

  // Simulate database being dropped
  database_dropped = true;
  EXPECT_EQ(storage->make_database_protector(), nullptr) << "Factory should return nullptr when database is dropped";

  // Test that basic storage operations still work even when factory returns nullptr
  {
    auto acc = storage->Access(memgraph::storage::WRITE);
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
      config_,
      std::nullopt,
      std::make_unique<memgraph::storage::PlanInvalidatorDefault>(),
      memgraph::metrics::DatabaseMetricHandles{},
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
      config_,
      std::nullopt,
      std::make_unique<memgraph::storage::PlanInvalidatorDefault>(),
      memgraph::metrics::DatabaseMetricHandles{},
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

// F24 regression test: reproduces try_delete()'s destroy-under-mutex deadlock (gatekeeper.hpp:290-297)
// using the real AsyncIndexer, via Gatekeeper<InMemoryStorage> instead of MG_ENTERPRISE's dbms::Database.

// Mirrors dbms::DatabaseProtector (database_protector.hpp:26-32): owns a live Accessor so
// try_delete()'s count_ == 1 gate is exercised faithfully, not stubbed out.
struct StorageGatekeeperProtector : memgraph::storage::DatabaseProtector {
  using Access = memgraph::utils::Gatekeeper<memgraph::storage::InMemoryStorage>::Accessor;

  explicit StorageGatekeeperProtector(Access access) : access_(std::move(access)) {}

  auto clone() const -> memgraph::storage::DatabaseProtectorPtr override {
    return std::make_unique<StorageGatekeeperProtector>(access_);
  }

 private:
  Access access_;
};

// Deliberately heap-allocated and, on the deadlock path, never freed: the indexer worker and the
// try_delete() caller both keep referencing this after the test function returns, so it must outlive it.
struct TryDeleteTrap {
  std::mutex mutex;
  std::condition_variable cv;
  // Set once the async indexer worker is parked inside AsyncIndexer::mutex_ with no protector minted,
  // i.e. provably about to call GK::access_via(internals).
  std::atomic<bool> reached{false};
  // Set by try_delete()'s predicate -- which gatekeeper.hpp:312 invokes while still holding
  // GKInternals::mutex_ -- releasing the trapped worker at the exact instant that mutex is held.
  std::atomic<bool> go{false};

  // Completion signal for `deleter` below; lives here for the same reason as `reached`/`go` above --
  // on the timeout path the detached thread still holds a reference to it after the test returns.
  std::mutex done_mutex;
  std::condition_variable done_cv;
  std::atomic<bool> try_delete_done{false};
  std::atomic<bool> try_delete_result{false};
};

TEST_F(DatabaseProtectorTest, TryDeleteDeadlocksUnderRealAsyncIndexer) {
  using Storage = memgraph::storage::InMemoryStorage;
  using GK = memgraph::utils::Gatekeeper<Storage>;
  using Internals = memgraph::utils::GKInternals<Storage>;

  // Leaked deliberately on the failure path (see below); harmless to leak unconditionally.
  auto *trap = new TryDeleteTrap();

  // Mirrors the route DatabaseHandler::MakeDatabaseProtectorFactory builds (database_handler.hpp:110-121).
  auto cell = std::make_shared<std::atomic<Internals *>>(nullptr);

  auto factory = [cell, trap]() -> memgraph::storage::DatabaseProtectorPtr {
    // Trap: announce arrival, then park until the dropper's predicate releases us. At this point the
    // worker holds AsyncIndexer::mutex_ (async_indexer.cpp:66) and has minted no protector yet.
    {
      std::lock_guard<std::mutex> lock(trap->mutex);
      trap->reached = true;
    }
    trap->cv.notify_all();
    {
      std::unique_lock<std::mutex> lock(trap->mutex);
      trap->cv.wait(lock, [trap] { return trap->go.load(); });
    }
    auto *internals = cell->load(std::memory_order_acquire);
    if (internals == nullptr) return nullptr;
    if (auto acc = GK::access_via(internals)) {
      return std::make_unique<StorageGatekeeperProtector>(std::move(*acc));
    }
    return nullptr;
  };

  auto *gk = new GK(config_,
                    std::nullopt,
                    std::make_unique<memgraph::storage::PlanInvalidatorDefault>(),
                    memgraph::metrics::DatabaseMetricHandles{},
                    factory);
  // Publish only after construction: the async indexer's request queue is empty until
  // CreateVerticesWithLabel below enqueues work, so the factory cannot be called before this store.
  cell->store(gk->internals(), std::memory_order_release);

  // Moved off the stack: on the timeout path this test returns without destroying it, and ~Accessor's
  // reset() (gatekeeper.hpp:333-342) would itself block on the same mutex the wedged deleter holds.
  auto minted = gk->access();
  ASSERT_TRUE(minted.has_value()) << "Fresh HOT gatekeeper must mint its first accessor";
  auto *acc = new GK::Accessor(std::move(*minted));

  auto *storage_ptr = acc->get();
  auto label = storage_ptr->NameToLabel("TestLabel");
  CreateVerticesWithLabel(storage_ptr, label, 1);

  // Bounded wait for the worker to reach the trap -- deterministic once reached (the trap, not
  // timing, decides what happens next), but getting there at all still needs a bound.
  {
    std::unique_lock<std::mutex> lock(trap->mutex);
    ASSERT_TRUE(trap->cv.wait_for(lock, std::chrono::seconds(5), [trap] { return trap->reached.load(); }))
        << "Async indexer worker never reached the factory trap within 5s";
  }

  std::thread deleter([acc, trap] {
    auto predicate = [trap](Storage & /*storage*/) {
      {
        std::lock_guard<std::mutex> lock(trap->mutex);
        trap->go = true;
      }
      trap->cv.notify_all();
      return true;
    };
    // Deadlocks on a pre-fix try_delete() that destroyed InMemoryStorage while still holding
    // GKInternals::mutex_ (gatekeeper.hpp:290-297) -- see the banner above for the full AB-BA chain.
    bool const result = acc->try_delete(std::chrono::seconds(3), predicate);
    trap->try_delete_result = result;
    {
      std::lock_guard<std::mutex> lock(trap->done_mutex);
      trap->try_delete_done = true;
    }
    trap->done_cv.notify_all();
  });

  bool finished_in_time = false;
  {
    std::unique_lock<std::mutex> lock(trap->done_mutex);
    finished_in_time =
        trap->done_cv.wait_for(lock, std::chrono::seconds(10), [trap] { return trap->try_delete_done.load(); });
  }

  if (!finished_in_time) {
    ADD_FAILURE() << "F24 AB-BA deadlock reproduced: try_delete() did not return within 10s. It is blocked "
                     "destroying InMemoryStorage while holding GKInternals::mutex_ (gatekeeper.hpp:291-307), "
                     "waiting on AsyncIndexer::mutex_ (via ~InMemoryStorage -> StopAllBackgroundTasks() -> "
                     "AsyncIndexer::Shutdown(), async_indexer.cpp:147-152) that the async indexer worker "
                     "holds while it blocks on the same GKInternals::mutex_ inside access_via().";
    // gk and acc would deadlock too: their destructors want the same GKInternals::mutex_ the wedged
    // threads hold. Detach the wedged thread and leak the rest so exactly one test fails, not the binary.
    deleter.detach();
    return;  // gk, acc, cell (captured by the leaked factory closure), and trap are intentionally leaked.
  }

  deleter.join();
  EXPECT_TRUE(trap->try_delete_result) << "try_delete() should report success once the predicate accepted the drop";

  acc->reset();
  delete acc;
  delete gk;
  delete trap;
}

}  // namespace
