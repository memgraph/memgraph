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
#include <algorithm>
#include <atomic>
#include <chrono>
#include <condition_variable>
#include <filesystem>
#include <mutex>
#include <thread>
#include <vector>

#include "metrics/prometheus_metrics.hpp"
#include "storage/v2/config.hpp"
#include "storage/v2/database_protector.hpp"
#include "storage/v2/durability/paths.hpp"
#include "storage/v2/durability/serialization.hpp"
#include "storage/v2/durability/version.hpp"
#include "storage/v2/durability/wal.hpp"
#include "storage/v2/inmemory/storage.hpp"
#include "storage/v2/property_value.hpp"
#include "storage/v2/ttl.hpp"
#include "tests/test_commit_args_helper.hpp"
#include "utils/gatekeeper.hpp"

namespace {

// Common test protector implementation
struct TestProtector : memgraph::storage::DatabaseProtector {
  auto clone() const -> memgraph::storage::DatabaseProtectorPtr override { return std::make_unique<TestProtector>(); }

  bool sealed() const override { return false; }
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

// Owns a live Gatekeeper Accessor so try_delete()'s count_ == 1 gate is exercised faithfully (mirrors
// dbms::DatabaseProtector). Used by the AB-BA deadlock repro below.
struct StorageGatekeeperProtector : memgraph::storage::DatabaseProtector {
  using Access = memgraph::utils::Gatekeeper<memgraph::storage::InMemoryStorage>::Accessor;

  explicit StorageGatekeeperProtector(Access access) : access_(std::move(access)) {}

  auto clone() const -> memgraph::storage::DatabaseProtectorPtr override {
    return std::make_unique<StorageGatekeeperProtector>(access_);
  }

  bool sealed() const override { return access_.is_marked_for_deletion(); }

 private:
  Access access_;
};

// Heap-allocated and, on the deadlock path, intentionally leaked: the indexer worker and the try_delete
// caller both keep referencing it after the test returns.
struct TryDeleteTrap {
  std::mutex mutex;
  std::condition_variable cv;
  std::atomic<bool> reached{false};  // worker parked inside AsyncIndexer::mutex_, no protector minted yet
  std::atomic<bool> go{false};       // set by try_delete's predicate (invoked under GKInternals::mutex_)
  std::mutex done_mutex;
  std::condition_variable done_cv;
  std::atomic<bool> try_delete_done{false};
  std::atomic<bool> try_delete_result{false};
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

// Regression for the gatekeeper try_delete() destroy-under-mutex AB-BA deadlock, via the real
// Gatekeeper::access() route (PR #4576 used access_via(), absent here). The async-indexer worker is
// parked inside the protector factory while holding AsyncIndexer::mutex_ and having minted no protector.
// A second thread then drops the tenant via Accessor::try_delete():
//   pre-fix (destroy value_ under GKInternals::mutex_): ~InMemoryStorage -> StopAllBackgroundTasks() ->
//     AsyncIndexer::Shutdown() blocks acquiring AsyncIndexer::mutex_ held by the parked worker, while
//     that worker (once released) blocks acquiring GKInternals::mutex_ held here -> deadlock.
//   fixed (#4572, move value_ out, release the mutex, destroy unlocked): try_delete releases
//     GKInternals::mutex_ before ~InMemoryStorage, so the released worker's access() returns nullptr,
//     the factory returns null, the worker exits and releases AsyncIndexer::mutex_, and Shutdown joins.
TEST_F(DatabaseProtectorTest, TryDeleteDeadlocksUnderRealAsyncIndexerViaAccess) {
  using Storage = memgraph::storage::InMemoryStorage;
  using GK = memgraph::utils::Gatekeeper<Storage>;

  auto *trap = new TryDeleteTrap();
  // GK* cell, populated only after construction: the factory must re-mint via the same gatekeeper that
  // try_delete operates on, and access() needs the Gatekeeper object (there is no access_via here).
  auto gk_cell = std::make_shared<std::atomic<GK *>>(nullptr);

  auto factory = [gk_cell, trap]() -> memgraph::storage::DatabaseProtectorPtr {
    // Park inside the factory: the async-indexer worker holds AsyncIndexer::mutex_ here and has minted
    // no protector yet. Announce arrival, then wait for the dropper's predicate to release us.
    {
      std::lock_guard<std::mutex> lock(trap->mutex);
      trap->reached = true;
    }
    trap->cv.notify_all();
    {
      std::unique_lock<std::mutex> lock(trap->mutex);
      trap->cv.wait(lock, [trap] { return trap->go.load(); });
    }
    auto *gk = gk_cell->load(std::memory_order_acquire);
    if (gk == nullptr) return nullptr;
    if (auto acc = gk->access()) {  // real route: Gatekeeper::access() takes GKInternals::mutex_
      return std::make_unique<StorageGatekeeperProtector>(std::move(*acc));
    }
    return nullptr;
  };

  auto *gk = new GK(config_,
                    std::nullopt,
                    std::make_unique<memgraph::storage::PlanInvalidatorDefault>(),
                    memgraph::metrics::DatabaseMetricHandles{},
                    factory);
  gk_cell->store(gk, std::memory_order_release);

  auto minted = gk->access();
  ASSERT_TRUE(minted.has_value()) << "Fresh HOT gatekeeper must mint its first accessor";
  auto *acc = new GK::Accessor(std::move(*minted));

  auto *storage_ptr = acc->get();
  auto label = storage_ptr->NameToLabel("TestLabel");
  CreateVerticesWithLabel(storage_ptr, label, 1);

  {
    std::unique_lock<std::mutex> lock(trap->mutex);
    ASSERT_TRUE(trap->cv.wait_for(lock, std::chrono::seconds(5), [trap] { return trap->reached.load(); }))
        << "Async indexer worker never reached the factory trap within 5s";
  }

  std::thread deleter([acc, trap] {
    auto predicate = [trap](Storage & /*storage*/) {
      // Invoked under GKInternals::mutex_; release the parked worker at that instant.
      {
        std::lock_guard<std::mutex> lock(trap->mutex);
        trap->go = true;
      }
      trap->cv.notify_all();
      return true;
    };
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
    ADD_FAILURE() << "AB-BA deadlock reproduced: try_delete() did not return within 10s — it is destroying "
                     "InMemoryStorage while holding GKInternals::mutex_, blocking on AsyncIndexer::mutex_ held by "
                     "the worker parked in the factory, which itself blocks acquiring GKInternals::mutex_ in access().";
    deleter.detach();  // leak gk/acc/trap on the deadlock path so exactly one test fails, not the binary
    return;
  }

  deleter.join();
  EXPECT_TRUE(trap->try_delete_result) << "try_delete() should report success once the predicate accepted the drop";
  delete acc;
  delete gk;
  delete trap;
}

#ifdef MG_ENTERPRISE
namespace {

using memgraph::storage::InMemoryStorage;
using memgraph::storage::PropertyPath;
using memgraph::storage::PropertyValue;

// The worker may spend one bounded acquisition slice (100 ms after the fix) plus one in-flight batch
// retiring; several seconds is a generous ceiling that still fails a worker parked forever.
constexpr auto kRetireBound = std::chrono::seconds(5);

// Create the :TTL(ttl) label-property index synchronously, so the TTL worker never has to create it
// in its first batch and the test's timing is about the batch Access(WRITE), not index creation.
void CreateTtlIndex(InMemoryStorage *storage) {
  auto label = storage->NameToLabel("TTL");
  std::vector<PropertyPath> path{storage->NameToProperty("ttl")};
  auto acc = storage->UniqueAccess();
  ASSERT_TRUE(acc) << "Failed to acquire UNIQUE access for TTL index creation";
  ASSERT_TRUE(acc->CreateIndex(label, path).has_value()) << "Failed to create the TTL label-property index";
  ASSERT_TRUE(acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()).has_value())
      << "Failed to commit the TTL index";
}

// An expired :TTL vertex is one whose integer `ttl` property is below `now` in microseconds, i.e. 0.
void CreateExpiredTtlVertices(InMemoryStorage *storage, size_t count) {
  auto label = storage->NameToLabel("TTL");
  auto prop = storage->NameToProperty("ttl");
  auto acc = storage->Access(memgraph::storage::WRITE);
  ASSERT_TRUE(acc) << "Failed to acquire WRITE access for TTL vertices";
  for (size_t i = 0; i < count; ++i) {
    auto vertex = acc->CreateVertex();
    ASSERT_TRUE(vertex.AddLabel(label).has_value()) << "Failed to add the TTL label";
    ASSERT_TRUE(vertex.SetProperty(prop, PropertyValue(0)).has_value()) << "Failed to set the expired ttl property";
  }
  ASSERT_TRUE(acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()).has_value())
      << "Failed to commit the TTL vertices";
}

// TTL ref lives inside the storage, outliving the accessor used to reach it.
memgraph::storage::ttl::TTL *TtlOf(InMemoryStorage *storage) {
  auto acc = storage->Access(memgraph::storage::WRITE);
  return &acc->ttl();
}

size_t CountAllVertices(InMemoryStorage *storage) {
  auto acc = storage->Access(memgraph::storage::WRITE);
  size_t count = 0;
  for (auto vertex : acc->Vertices(memgraph::storage::View::NEW)) {
    (void)vertex;
    ++count;
  }
  (void)acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs());
  return count;
}

// Decode the last committed transaction that carries a TTL operation from the storage's WAL files,
// returning its deltas in write order (transaction markers excluded). The storage must be destroyed
// first so its WAL is finalized.
std::vector<memgraph::storage::durability::WalDeltaData> ReadLastTtlWalTransaction(
    const std::filesystem::path &storage_directory) {
  using namespace memgraph::storage::durability;

  std::vector<std::filesystem::path> wal_files;
  std::error_code ec;
  for (const auto &entry : std::filesystem::directory_iterator(storage_directory / std::string{kWalDirectory}, ec)) {
    if (entry.is_regular_file()) wal_files.push_back(entry.path());
  }
  std::sort(wal_files.begin(), wal_files.end());

  std::vector<WalDeltaData> last_ttl_txn;
  std::vector<WalDeltaData> current;
  for (const auto &path : wal_files) {
    auto info = ReadWalInfo(path);
    Decoder wal;
    wal.Initialize(path, kWalMagic);
    wal.SetPosition(info.offset_deltas);
    for (uint64_t i = 0; i < info.num_deltas; ++i) {
      (void)ReadWalDeltaHeader(&wal);
      auto delta = ReadWalDeltaData(&wal);
      if (std::holds_alternative<WalTransactionStart>(delta.data_)) {
        current.clear();
      } else if (std::holds_alternative<WalTransactionEnd>(delta.data_)) {
        if (std::any_of(current.begin(), current.end(), [](const WalDeltaData &d) {
              return std::holds_alternative<WalTtlOperation>(d.data_);
            })) {
          last_ttl_txn = current;
        }
        current.clear();
      } else {
        current.push_back(std::move(delta));
      }
    }
  }
  return last_ttl_txn;
}

// Parks the TTL worker inside make_database_protector(), so a Disable() that has already requested a
// stop is deterministically in flight (joined-but-not-returned) until the test releases the factory.
struct BlockingProtectorFactory {
  std::mutex mutex;
  std::condition_variable cv;
  std::atomic<bool> reached{false};
  std::atomic<bool> release{false};

  std::unique_ptr<memgraph::storage::DatabaseProtector> operator()() {
    if (!reached.exchange(true)) {
      {
        std::lock_guard lock{mutex};
      }
      cv.notify_all();
    }
    std::unique_lock lock{mutex};
    cv.wait(lock, [this] { return release.load(); });
    return std::make_unique<TestProtector>();
  }

  bool WaitReached(std::chrono::milliseconds timeout) {
    std::unique_lock lock{mutex};
    return cv.wait_for(lock, timeout, [this] { return reached.load(); });
  }

  void Release() {
    {
      std::lock_guard lock{mutex};
      release.store(true);
    }
    cv.notify_all();
  }
};

}  // namespace

// A TTL stop/join must complete while the caller keeps a storage UNIQUE accessor live: first the raw
// TTL::Disable(), then DisableTtl() from the accessor. Pre-fix the worker parks in the unbounded
// Storage::Access(WRITE) at ttl.cpp:205; the join then blocks waiting for a worker that needs the
// UNIQUE the caller holds. Post-fix the batch acquisition is bounded and the top-of-batch stop check
// makes the worker retire at the batch boundary without ever touching the UNIQUE lock again.
TEST_F(DatabaseProtectorTest, TTLWorkerRetiresWhileUniqueHeld) {
  // A WAL is enabled so the DISABLE deltas' append order can be read back after the join (A2). The
  // storage is a bare InMemoryStorage, as the rest of this fixture's tests use.
  auto durability_config = config_;
  durability_config.durability.snapshot_wal_mode =
      memgraph::storage::Config::Durability::SnapshotWalMode::PERIODIC_SNAPSHOT_WITH_WAL;
  auto *storage = new InMemoryStorage(durability_config);
  auto *ttl = TtlOf(storage);

  CreateTtlIndex(storage);
  CreateExpiredTtlVertices(storage, 30'000);

  ttl->Enable();
  ttl->Configure(false);
  ttl->SetInterval(std::chrono::milliseconds(100));

  // Hold the UNIQUE across the whole stop/join, exactly as DisableTtl (inmemory/storage.hpp:781)
  // and a live query transaction do.
  auto unique_acc = storage->UniqueAccess();
  ttl->Resume();
  // Give the worker time to enter its first batch and park in Access(WRITE) pre-fix.
  std::this_thread::sleep_for(std::chrono::milliseconds(300));

  std::atomic<bool> retired{false};
  std::thread stopper([ttl, &retired] {
    ttl->Disable();
    retired.store(true, std::memory_order_release);
  });

  const auto deadline = std::chrono::steady_clock::now() + kRetireBound;
  while (!retired.load(std::memory_order_acquire) && std::chrono::steady_clock::now() < deadline) {
    std::this_thread::sleep_for(std::chrono::milliseconds(10));
  }

  if (!retired.load(std::memory_order_acquire)) {
    ADD_FAILURE() << "TTL worker did not retire within 5s while the test held a UNIQUE accessor: Disable() -> "
                     "Scheduler::Stop() is joining a worker parked in the unbounded Access(WRITE) (ttl.cpp:205), "
                     "which needs the UNIQUE the caller holds.";
    // Release the UNIQUE so the parked worker can drain. The worker's run is not cancellable pre-fix, so
    // it drains all remaining batches before Stop() can join; give it room, then leak only if it really hangs.
    unique_acc.reset();
    const auto drain_deadline = std::chrono::steady_clock::now() + std::chrono::seconds(20);
    while (!retired.load(std::memory_order_acquire) && std::chrono::steady_clock::now() < drain_deadline) {
      std::this_thread::sleep_for(std::chrono::milliseconds(10));
    }
    if (retired.load(std::memory_order_acquire)) {
      stopper.join();
      delete storage;
    } else {
      stopper.detach();  // leak storage; the detached thread still references it
    }
    return;
  }

  stopper.join();
  EXPECT_FALSE(ttl->Enabled()) << "Disable() must leave TTL disabled";
  unique_acc.reset();

  // DisableTtl() runs its join inside a live UNIQUE accessor (inmemory/storage.hpp:765-785): it drops
  // the indices, joins the worker, then appends its md_delta. Both the main and the replica
  // (is_main=false) form must return without the caller releasing the UNIQUE.
  CreateExpiredTtlVertices(storage, 30'000);
  ttl->Enable();
  ttl->Configure(false);
  ttl->SetInterval(std::chrono::milliseconds(100));

  std::atomic<bool> held{false};
  std::atomic<bool> go{false};
  std::atomic<bool> done{false};
  std::atomic<bool> committed{false};

  // The same thread both holds the UNIQUE and calls DisableTtl, so the lock is acquired and released on
  // one thread; the worker is parked in its batch Access(WRITE) by the time `go` is set.
  std::thread disabler([storage, &held, &go, &done, &committed] {
    auto acc = storage->UniqueAccess();
    held.store(true, std::memory_order_release);
    while (!go.load(std::memory_order_acquire)) {
      std::this_thread::sleep_for(std::chrono::milliseconds(2));
    }
    acc->DisableTtl();
    acc->DisableTtl(memgraph::storage::TTLReplicationArgs{.is_main = false});
    // Commit in the same transaction so the deltas' append order is the order the WAL records.
    committed.store(acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()).has_value(),
                    std::memory_order_release);
    done.store(true, std::memory_order_release);
  });

  while (!held.load(std::memory_order_acquire)) {
    std::this_thread::sleep_for(std::chrono::milliseconds(2));
  }
  ttl->Resume();
  std::this_thread::sleep_for(std::chrono::milliseconds(300));
  go.store(true, std::memory_order_release);

  const auto disable_deadline = std::chrono::steady_clock::now() + kRetireBound;
  while (!done.load(std::memory_order_acquire) && std::chrono::steady_clock::now() < disable_deadline) {
    std::this_thread::sleep_for(std::chrono::milliseconds(10));
  }

  if (!done.load(std::memory_order_acquire)) {
    ADD_FAILURE() << "DisableTtl() did not return within 5s while the caller held a UNIQUE accessor: the worker is "
                     "parked in the unbounded batch Access(WRITE) (ttl.cpp:205) and the join cannot get the UNIQUE "
                     "the caller holds.";
    disabler.detach();  // leak storage; the blocked thread still references it
    return;
  }

  disabler.join();
  EXPECT_FALSE(ttl->Enabled()) << "DisableTtl() must leave TTL disabled";
  EXPECT_TRUE(committed.load(std::memory_order_acquire))
      << "the replica DISABLE transaction must commit, otherwise its delta never reaches the WAL";
  delete storage;  // finalizes the WAL

  // A2: both DisableTtl forms append their md_delta after their join, and the WAL records the same
  // order. The main form drops the TTL index first, so the transaction must be
  // [label-properties-index drop, main DISABLE, replica DISABLE], replica last.
  const auto ttl_txn = ReadLastTtlWalTransaction(durability_config.durability.storage_directory);
  ASSERT_EQ(ttl_txn.size(), 3u)
      << "expected the committed DisableTtl transaction to carry exactly [index drop, main DISABLE, "
         "replica DISABLE]";
  EXPECT_NE(std::get_if<memgraph::storage::durability::WalLabelPropertyIndexDrop>(&ttl_txn[0].data_), nullptr)
      << "the index drop happens before ttl.Disable(), so it must be the first WAL delta of the transaction";
  const auto is_disable = [](const memgraph::storage::durability::WalDeltaData &delta) {
    const auto *op = std::get_if<memgraph::storage::durability::WalTtlOperation>(&delta.data_);
    return op != nullptr && op->operation_type == memgraph::storage::durability::TtlOperationType::DISABLE;
  };
  EXPECT_TRUE(is_disable(ttl_txn[1]))
      << "the main DISABLE delta must be appended after its join and keep its place in WAL order";
  EXPECT_TRUE(is_disable(ttl_txn[2]))
      << "the replica (is_main=false) DISABLE delta must stay appended after its join, last in WAL order";
}

// Guard: the TTL lifecycle API must stay live under concurrent stop/start and still reap afterwards.
// This pins that the serialized interface does not deadlock the worker or lose the final generation.
// Configure()/Run() is excluded: recreating the scheduler thread concurrently with Stop()'s join
// mutates the same std::jthread.
TEST_F(DatabaseProtectorTest, TtlConcurrentStopStartLifecycle) {
  auto *storage = new InMemoryStorage(config_);
  auto *ttl = TtlOf(storage);

  CreateTtlIndex(storage);

  ttl->Enable();
  ttl->Configure(false);
  ttl->SetInterval(std::chrono::milliseconds(50));
  ttl->Resume();

  std::atomic<bool> stop{false};
  std::thread stopper([ttl, &stop] {
    while (!stop.load(std::memory_order_acquire)) {
      try {
        ttl->Disable();
      } catch (...) {
      }
      try {
        ttl->Shutdown();
      } catch (...) {
      }
      std::this_thread::sleep_for(std::chrono::milliseconds(2));
    }
  });
  std::thread starter([ttl, &stop] {
    while (!stop.load(std::memory_order_acquire)) {
      try {
        ttl->Enable();
      } catch (...) {
      }
      try {
        ttl->SetInterval(std::chrono::milliseconds(50));
      } catch (...) {
      }
      try {
        ttl->Resume();
      } catch (...) {
      }
      try {
        ttl->Pause();
      } catch (...) {
      }
      std::this_thread::sleep_for(std::chrono::milliseconds(2));
    }
  });

  std::this_thread::sleep_for(std::chrono::milliseconds(300));
  stop.store(true, std::memory_order_release);
  stopper.join();
  starter.join();

  // Settle to a known stopped state, then create one expired vertex and enable a fresh generation.
  ttl->Disable();
  CreateExpiredTtlVertices(storage, 1);

  ttl->Enable();
  ttl->Configure(false);
  ttl->SetInterval(std::chrono::milliseconds(50));
  ttl->Resume();

  const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(10);
  while (CountAllVertices(storage) != 0 && std::chrono::steady_clock::now() < deadline) {
    std::this_thread::sleep_for(std::chrono::milliseconds(50));
  }
  EXPECT_EQ(CountAllVertices(storage), 0u)
      << "A re-enabled TTL generation must still reap an expired vertex after concurrent stop/start";

  ttl->Disable();
  delete storage;

  // A3 (red): a stop/join that is in flight must exclude every other lifecycle mutation. The worker is
  // parked inside make_database_protector(), so Disable() has requested the stop and is joining but
  // cannot return. Pre-fix Enable() is a bare bool write and completes during that window; post-fix it
  // waits on the serialization lock until the join is done.
  BlockingProtectorFactory factory;
  // Auto index creation off so the only make_database_protector() caller is the TTL worker itself;
  // otherwise the fixture's async indexer parks in the factory and masks whether the join is in flight.
  auto blocking_config = config_;
  blocking_config.salient.items.enable_label_index_auto_creation = false;
  auto *storage2 = new InMemoryStorage(blocking_config,
                                       std::nullopt,
                                       std::make_unique<memgraph::storage::PlanInvalidatorDefault>(),
                                       memgraph::metrics::DatabaseMetricHandles{},
                                       [&factory] { return factory(); });
  auto *ttl2 = TtlOf(storage2);
  CreateTtlIndex(storage2);
  CreateExpiredTtlVertices(storage2, 1);
  ttl2->Enable();
  ttl2->Configure(false);
  ttl2->SetInterval(std::chrono::milliseconds(50));
  ttl2->Resume();
  ASSERT_TRUE(factory.WaitReached(std::chrono::seconds(5)))
      << "TTL worker never reached make_database_protector(); cannot hold a stop in flight";

  std::atomic<bool> disable_returned{false};
  std::thread inflight_stopper([ttl2, &disable_returned] {
    ttl2->Disable();
    disable_returned.store(true, std::memory_order_release);
  });
  // Let Disable() get past taking the lifecycle lock and into Scheduler::Stop().
  std::this_thread::sleep_for(std::chrono::milliseconds(300));

  std::atomic<bool> enable_returned{false};
  std::thread concurrent_enabler([ttl2, &enable_returned] {
    ttl2->Enable();
    enable_returned.store(true, std::memory_order_release);
  });
  std::this_thread::sleep_for(std::chrono::milliseconds(300));

  EXPECT_FALSE(disable_returned.load(std::memory_order_acquire))
      << "setup check: the parked worker should keep the stop/join in flight";
  EXPECT_FALSE(enable_returned.load(std::memory_order_acquire))
      << "TTL::Enable() completed while a stop/join was in flight: the lifecycle is not serialized, so a "
         "new generation can start over a worker that is still being joined";

  factory.Release();
  const auto release_deadline = std::chrono::steady_clock::now() + std::chrono::seconds(5);
  while ((!disable_returned.load(std::memory_order_acquire) || !enable_returned.load(std::memory_order_acquire)) &&
         std::chrono::steady_clock::now() < release_deadline) {
    std::this_thread::sleep_for(std::chrono::milliseconds(10));
  }
  if (disable_returned.load(std::memory_order_acquire) && enable_returned.load(std::memory_order_acquire)) {
    inflight_stopper.join();
    concurrent_enabler.join();
    ttl2->Disable();
    delete storage2;
  } else {
    inflight_stopper.detach();
    concurrent_enabler.detach();
    ADD_FAILURE() << "stop/join and the queued lifecycle mutation did not complete after the protector was released";
  }
}
#endif  // MG_ENTERPRISE

}  // namespace
