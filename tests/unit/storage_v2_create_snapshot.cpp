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

#include <filesystem>
#include <string>

#include "dbms/database.hpp"
#include "replication/state.hpp"
#include "storage/v2/config.hpp"
#include "storage/v2/durability/paths.hpp"
#include "storage/v2/inmemory/storage.hpp"
#include "storage_test_utils.hpp"

using memgraph::replication_coordination_glue::ReplicationRole;

class CreateSnapshotTest : public testing::Test {
 protected:
  void SetUp() override {
    // Create a temporary directory for testing
    storage_directory = std::filesystem::temp_directory_path() / "create_snapshot_test";
    std::filesystem::create_directories(storage_directory);
  }

  void TearDown() override {
    // Clean up the test directory
    std::filesystem::remove_all(storage_directory);
  }

  memgraph::storage::Config CreateConfig() {
    return memgraph::storage::Config{
        .durability = {.storage_directory = storage_directory,
                       .recover_on_startup = false,
                       .snapshot_on_exit = false,
                       .items_per_batch = 13,
                       .allow_parallel_schema_creation = true},
        .salient = {.items = {.properties_on_edges = false, .enable_schema_info = true}},
    };
  }

  std::filesystem::path storage_directory;
};

TEST_F(CreateSnapshotTest, CreateSnapshotReturnsPathOnSuccess) {
  auto config = CreateConfig();
  memgraph::utils::Synchronized<memgraph::replication::ReplicationState, memgraph::utils::RWSpinLock> repl_state{
      memgraph::storage::ReplicationStateRootPath(config)};
  memgraph::dbms::Database db{config, repl_state};

  auto *mem_storage = static_cast<memgraph::storage::InMemoryStorage *>(db.storage());

  // Create some data to ensure snapshot has content
  {
    auto acc = mem_storage->Access();
    auto vertex = acc->CreateVertex();
    ASSERT_FALSE(acc->PrepareForCommitPhase().HasError());
  }

  // Test CreateSnapshot returns path on success
  auto result = mem_storage->CreateSnapshot(ReplicationRole::MAIN);

  ASSERT_FALSE(result.HasError()) << "CreateSnapshot should succeed with some data";

  auto snapshot_path = result.GetValue();
  ASSERT_TRUE(std::filesystem::exists(snapshot_path)) << "Snapshot file should exist at returned path";
  ASSERT_TRUE(std::filesystem::is_regular_file(snapshot_path)) << "Snapshot should be a regular file";

  // Verify the path is in the expected directory
  auto expected_dir = config.durability.storage_directory / memgraph::storage::durability::kSnapshotDirectory;
  ASSERT_EQ(snapshot_path.parent_path(), expected_dir) << "Snapshot should be in the snapshots directory";

  // Verify the filename format (should contain timestamp)
  auto filename = snapshot_path.filename().string();
  ASSERT_TRUE(filename.find("timestamp_") != std::string::npos) << "Snapshot filename should contain timestamp";
}

TEST_F(CreateSnapshotTest, CreateSnapshotReturnsErrorForReplica) {
  auto config = CreateConfig();
  memgraph::utils::Synchronized<memgraph::replication::ReplicationState, memgraph::utils::RWSpinLock> repl_state{
      memgraph::storage::ReplicationStateRootPath(config)};
  memgraph::dbms::Database db{config, repl_state};

  auto *mem_storage = static_cast<memgraph::storage::InMemoryStorage *>(db.storage());

  // Test CreateSnapshot returns error for replica role
  auto result = mem_storage->CreateSnapshot(ReplicationRole::REPLICA);

  ASSERT_TRUE(result.HasError()) << "CreateSnapshot should fail for replica role";
  ASSERT_EQ(result.GetError(), memgraph::storage::InMemoryStorage::CreateSnapshotError::DisabledForReplica)
      << "Should return DisabledForReplica error";
}

TEST_F(CreateSnapshotTest, CreateSnapshotReturnsErrorWhenNothingNewToWrite) {
  auto config = CreateConfig();
  memgraph::utils::Synchronized<memgraph::replication::ReplicationState, memgraph::utils::RWSpinLock> repl_state{
      memgraph::storage::ReplicationStateRootPath(config)};
  memgraph::dbms::Database db{config, repl_state};

  auto *mem_storage = static_cast<memgraph::storage::InMemoryStorage *>(db.storage());

  // Create some data and take a snapshot
  {
    auto acc = mem_storage->Access();
    auto vertex = acc->CreateVertex();
    ASSERT_FALSE(acc->PrepareForCommitPhase().HasError());
  }

  auto result1 = mem_storage->CreateSnapshot(ReplicationRole::MAIN);
  ASSERT_FALSE(result1.HasError()) << "First CreateSnapshot should succeed";

  // Try to create another snapshot immediately - should fail with NothingNewToWrite
  auto result2 = mem_storage->CreateSnapshot(ReplicationRole::MAIN);
  ASSERT_TRUE(result2.HasError()) << "Second CreateSnapshot should fail";
  ASSERT_EQ(result2.GetError(), memgraph::storage::InMemoryStorage::CreateSnapshotError::NothingNewToWrite)
      << "Should return NothingNewToWrite error";
}

TEST_F(CreateSnapshotTest, CreateSnapshotReturnsErrorWhenAlreadyRunning) {
  auto config = CreateConfig();
  memgraph::utils::Synchronized<memgraph::replication::ReplicationState, memgraph::utils::RWSpinLock> repl_state{
      memgraph::storage::ReplicationStateRootPath(config)};
  memgraph::dbms::Database db{config, repl_state};

  auto *mem_storage = static_cast<memgraph::storage::InMemoryStorage *>(db.storage());

  // Create some data
  {
    auto acc = mem_storage->Access();
    auto vertex = acc->CreateVertex();
    ASSERT_FALSE(acc->PrepareForCommitPhase().HasError());
  }

  // Start a snapshot creation in a separate thread
  std::thread snapshot_thread([mem_storage]() {
    auto result = mem_storage->CreateSnapshot(ReplicationRole::MAIN);
    // This should succeed, but we don't check the result here
  });

  // Try to create another snapshot while the first one is running
  auto result = mem_storage->CreateSnapshot(ReplicationRole::MAIN);

  // Wait for the first snapshot to complete
  snapshot_thread.join();

  // The second call should fail with AlreadyRunning
  ASSERT_TRUE(result.HasError()) << "Concurrent CreateSnapshot should fail";
  ASSERT_EQ(result.GetError(), memgraph::storage::InMemoryStorage::CreateSnapshotError::AlreadyRunning)
      << "Should return AlreadyRunning error";
}

TEST_F(CreateSnapshotTest, CreateSnapshotPathFormat) {
  auto config = CreateConfig();
  memgraph::utils::Synchronized<memgraph::replication::ReplicationState, memgraph::utils::RWSpinLock> repl_state{
      memgraph::storage::ReplicationStateRootPath(config)};
  memgraph::dbms::Database db{config, repl_state};

  auto *mem_storage = static_cast<memgraph::storage::InMemoryStorage *>(db.storage());

  // Create some data
  {
    auto acc = mem_storage->Access();
    auto vertex = acc->CreateVertex();
    ASSERT_FALSE(acc->PrepareForCommitPhase().HasError());
  }

  // Create snapshot and verify path format
  auto result = mem_storage->CreateSnapshot(ReplicationRole::MAIN);
  ASSERT_FALSE(result.HasError()) << "CreateSnapshot should succeed";

  auto snapshot_path = result.GetValue();
  auto filename = snapshot_path.filename().string();

  // Verify the filename follows the expected format: YYYYmmddHHMMSSffffff_timestamp_<timestamp>
  // The format should contain exactly one underscore before "timestamp_"
  auto timestamp_pos = filename.find("_timestamp_");
  ASSERT_NE(timestamp_pos, std::string::npos) << "Filename should contain '_timestamp_'";

  // Verify there's only one underscore before "timestamp_"
  auto first_underscore = filename.find('_');
  ASSERT_EQ(first_underscore, timestamp_pos) << "Should have only one underscore before 'timestamp_'";

  // Verify the timestamp part is numeric
  auto timestamp_str = filename.substr(timestamp_pos + 11);  // Skip "_timestamp_"
  ASSERT_FALSE(timestamp_str.empty()) << "Timestamp should not be empty";
  ASSERT_TRUE(std::all_of(timestamp_str.begin(), timestamp_str.end(), ::isdigit)) << "Timestamp should be numeric";
}

TEST_F(CreateSnapshotTest, BackwardCompatibilityWithErrorHandling) {
  auto config = CreateConfig();
  memgraph::utils::Synchronized<memgraph::replication::ReplicationState, memgraph::utils::RWSpinLock> repl_state{
      memgraph::storage::ReplicationStateRootPath(config)};
  memgraph::dbms::Database db{config, repl_state};

  auto *mem_storage = static_cast<memgraph::storage::InMemoryStorage *>(db.storage());

  // Test that existing error handling code still works
  auto result = mem_storage->CreateSnapshot(ReplicationRole::REPLICA);

  // This should work exactly as before - checking for errors
  if (result.HasError()) {
    auto error = result.GetError();
    ASSERT_EQ(error, memgraph::storage::InMemoryStorage::CreateSnapshotError::DisabledForReplica);
  } else {
    FAIL() << "Should have returned an error for replica role";
  }
}

TEST_F(CreateSnapshotTest, SuccessCaseWithPathRetrieval) {
  auto config = CreateConfig();
  memgraph::utils::Synchronized<memgraph::replication::ReplicationState, memgraph::utils::RWSpinLock> repl_state{
      memgraph::storage::ReplicationStateRootPath(config)};
  memgraph::dbms::Database db{config, repl_state};

  auto *mem_storage = static_cast<memgraph::storage::InMemoryStorage *>(db.storage());

  // Create some data
  {
    auto acc = mem_storage->Access();
    auto vertex = acc->CreateVertex();
    ASSERT_FALSE(acc->PrepareForCommitPhase().HasError());
  }

  // Test the new functionality - getting the path on success
  auto result = mem_storage->CreateSnapshot(ReplicationRole::MAIN);

  if (result.HasError()) {
    FAIL() << "CreateSnapshot should succeed with data present";
  } else {
    // New functionality: get the path
    auto snapshot_path = result.GetValue();
    ASSERT_TRUE(std::filesystem::exists(snapshot_path));

    // Verify the path is reasonable
    ASSERT_EQ(snapshot_path.extension(), "");
    ASSERT_TRUE(snapshot_path.filename().string().find("timestamp_") != std::string::npos);
  }
}
