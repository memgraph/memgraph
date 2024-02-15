// Copyright 2024 Memgraph Ltd.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt; by using this file, you agree to be bound by the terms of the Business Source
// License, and you may not use this file except in compliance with the Business Source License.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

#include "query/auth_query_handler.hpp"

#include <gmock/gmock.h>
#include <gtest/gtest.h>
#include <filesystem>
#include <system_error>

#include "dbms/constants.hpp"
#include "dbms/dbms_handler.hpp"
#include "dbms/global.hpp"
#include "glue/auth_checker.hpp"
#include "glue/auth_handler.hpp"
#include "query/config.hpp"
#include "query/interpreter.hpp"
#include "storage/v2/config.hpp"

// Global
std::filesystem::path storage_directory{std::filesystem::temp_directory_path() / "MG_test_unit_dbms_handler_community"};
static memgraph::storage::Config storage_conf;
std::unique_ptr<memgraph::auth::SynchedAuth> auth;
std::unique_ptr<memgraph::system::System> system_state;
std::unique_ptr<memgraph::replication::ReplicationState> repl_state;

// Let this be global so we can test it different states throughout

class TestEnvironment : public ::testing::Environment {
 public:
  static memgraph::dbms::DbmsHandler *get() { return ptr_.get(); }

  void SetUp() override {
    // Setup config
    memgraph::storage::UpdatePaths(storage_conf, storage_directory);
    storage_conf.durability.snapshot_wal_mode =
        memgraph::storage::Config::Durability::SnapshotWalMode::PERIODIC_SNAPSHOT_WITH_WAL;
    // Clean storage directory (running multiple parallel test, run only if the first process)
    if (std::filesystem::exists(storage_directory)) {
      memgraph::utils::OutputFile lock_file_handle_;
      lock_file_handle_.Open(storage_directory / ".lock", memgraph::utils::OutputFile::Mode::OVERWRITE_EXISTING);
      if (lock_file_handle_.AcquireLock()) {
        std::filesystem::remove_all(storage_directory);
      }
    }
    auth = std::make_unique<memgraph::auth::SynchedAuth>(storage_directory / "auth",
                                                         memgraph::auth::Auth::Config{/* default */});
    system_state = std::make_unique<memgraph::system::System>();
    repl_state = std::make_unique<memgraph::replication::ReplicationState>(ReplicationStateRootPath(storage_conf));
    ptr_ = std::make_unique<memgraph::dbms::DbmsHandler>(storage_conf, *repl_state.get());
  }

  void TearDown() override {
    ptr_.reset();
    repl_state.reset();
    system_state.reset();
    auth.reset();
    std::filesystem::remove_all(storage_directory);
  }

  static std::unique_ptr<memgraph::dbms::DbmsHandler> ptr_;
};

std::unique_ptr<memgraph::dbms::DbmsHandler> TestEnvironment::ptr_ = nullptr;

class DBMS_Handler : public testing::Test {};
using DBMS_HandlerDeath = DBMS_Handler;

TEST(DBMS_Handler, Init) {
  // Check that the default db has been created successfully
  std::vector<std::string> dirs = {"snapshots", "streams", "triggers", "wal"};
  for (const auto &dir : dirs)
    ASSERT_TRUE(std::filesystem::exists(storage_directory / dir)) << (storage_directory / dir);
  auto &dbms = *TestEnvironment::get();
  {
    const auto all = dbms.All();
    ASSERT_EQ(all.size(), 1);
    ASSERT_EQ(all[0], memgraph::dbms::kDefaultDB);
  }
}

TEST(DBMS_Handler, Get) {
  auto &dbms = *TestEnvironment::get();
  auto default_db = dbms.Get();
  ASSERT_TRUE(default_db);
  ASSERT_TRUE(default_db->storage() != nullptr);
  ASSERT_TRUE(default_db->streams() != nullptr);
  ASSERT_TRUE(default_db->trigger_store() != nullptr);
  ASSERT_TRUE(default_db->thread_pool() != nullptr);
  ASSERT_EQ(default_db->storage()->name(), memgraph::dbms::kDefaultDB);
  auto conf = storage_conf;
  conf.salient.name = memgraph::dbms::kDefaultDB;
  ASSERT_EQ(default_db->storage()->config_, conf);
}

int main(int argc, char *argv[]) {
  ::testing::InitGoogleTest(&argc, argv);
  // gtest takes ownership of the TestEnvironment ptr - we don't delete it.
  ::testing::AddGlobalTestEnvironment(new TestEnvironment);
  return RUN_ALL_TESTS();
}
