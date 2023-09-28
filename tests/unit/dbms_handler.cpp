// Copyright 2023 Memgraph Ltd.
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
#ifdef MG_ENTERPRISE
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

// Global
std::filesystem::path storage_directory{std::filesystem::temp_directory_path() / "MG_test_unit_dbms_handler"};
static memgraph::storage::Config storage_conf;
std::unique_ptr<memgraph::utils::Synchronized<memgraph::auth::Auth, memgraph::utils::WritePrioritizedRWLock>> auth;

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
    auth =
        std::make_unique<memgraph::utils::Synchronized<memgraph::auth::Auth, memgraph::utils::WritePrioritizedRWLock>>(
            storage_directory / "auth");
    ptr_ = std::make_unique<memgraph::dbms::DbmsHandler>(storage_conf, auth.get(), false, true);
  }

  void TearDown() override {
    ptr_.reset();
    auth.reset();
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
  const auto db_path = storage_directory / "databases" / memgraph::dbms::kDefaultDB;
  ASSERT_TRUE(std::filesystem::exists(db_path));
  for (const auto &dir : dirs) {
    std::error_code ec;
    const auto test_link = std::filesystem::read_symlink(db_path / dir, ec);
    ASSERT_TRUE(!ec) << ec.message();
    ASSERT_EQ(test_link, "../../" + dir);
  }
}

TEST(DBMS_Handler, New) {
  auto &dbms = *TestEnvironment::get();
  {
    const auto all = dbms.All();
    ASSERT_EQ(all.size(), 1);
    ASSERT_EQ(all[0], memgraph::dbms::kDefaultDB);
  }
  {
    auto db1 = dbms.New("db1");
    ASSERT_TRUE(db1.HasValue());
    ASSERT_TRUE(db1.GetValue());
    ASSERT_TRUE(std::filesystem::exists(storage_directory / "databases" / "db1"));
    ASSERT_TRUE(db1.GetValue()->storage() != nullptr);
    ASSERT_TRUE(db1.GetValue()->streams() != nullptr);
    ASSERT_TRUE(db1.GetValue()->trigger_store() != nullptr);
    ASSERT_TRUE(db1.GetValue()->thread_pool() != nullptr);
    const auto all = dbms.All();
    ASSERT_EQ(all.size(), 2);
    ASSERT_TRUE(std::find(all.begin(), all.end(), memgraph::dbms::kDefaultDB) != all.end());
    ASSERT_TRUE(std::find(all.begin(), all.end(), "db1") != all.end());
  }
  {
    // Fail if name exists
    auto db2 = dbms.New("db1");
    ASSERT_TRUE(db2.HasError() && db2.GetError() == memgraph::dbms::NewError::EXISTS);
  }
  {
    auto db3 = dbms.New("db3");
    ASSERT_TRUE(db3.HasValue());
    ASSERT_TRUE(std::filesystem::exists(storage_directory / "databases" / "db3"));
    ASSERT_TRUE(db3.GetValue()->storage() != nullptr);
    ASSERT_TRUE(db3.GetValue()->streams() != nullptr);
    ASSERT_TRUE(db3.GetValue()->trigger_store() != nullptr);
    ASSERT_TRUE(db3.GetValue()->thread_pool() != nullptr);
    const auto all = dbms.All();
    ASSERT_EQ(all.size(), 3);
    ASSERT_TRUE(std::find(all.begin(), all.end(), "db3") != all.end());
  }
}

TEST(DBMS_Handler, Get) {
  auto &dbms = *TestEnvironment::get();
  auto default_db = dbms.Get(memgraph::dbms::kDefaultDB);
  ASSERT_TRUE(default_db);
  ASSERT_TRUE(default_db->storage() != nullptr);
  ASSERT_TRUE(default_db->streams() != nullptr);
  ASSERT_TRUE(default_db->trigger_store() != nullptr);
  ASSERT_TRUE(default_db->thread_pool() != nullptr);

  ASSERT_ANY_THROW(dbms.Get("non-existent"));

  auto db1 = dbms.Get("db1");
  ASSERT_TRUE(db1);
  ASSERT_TRUE(db1->storage() != nullptr);
  ASSERT_TRUE(db1->streams() != nullptr);
  ASSERT_TRUE(db1->trigger_store() != nullptr);
  ASSERT_TRUE(db1->thread_pool() != nullptr);

  auto db3 = dbms.Get("db3");
  ASSERT_TRUE(db3);
  ASSERT_TRUE(db3->storage() != nullptr);
  ASSERT_TRUE(db3->streams() != nullptr);
  ASSERT_TRUE(db3->trigger_store() != nullptr);
  ASSERT_TRUE(db3->thread_pool() != nullptr);
}

TEST(DBMS_Handler, Delete) {
  auto &dbms = *TestEnvironment::get();

  auto db1_acc = dbms.Get("db1");  // Holds access to database

  {
    auto del = dbms.Delete(memgraph::dbms::kDefaultDB);
    ASSERT_TRUE(del.HasError() && del.GetError() == memgraph::dbms::DeleteError::DEFAULT_DB);
  }
  {
    auto del = dbms.Delete("non-existent");
    ASSERT_TRUE(del.HasError() && del.GetError() == memgraph::dbms::DeleteError::NON_EXISTENT);
  }
  {
    // db1_acc is using db1
    auto del = dbms.Delete("db1");
    ASSERT_TRUE(del.HasError());
    ASSERT_TRUE(del.GetError() == memgraph::dbms::DeleteError::USING);
  }
  {
    // Reset db1_acc (releases access) so delete will succeed
    db1_acc.reset();
    ASSERT_FALSE(db1_acc);
    auto del = dbms.Delete("db1");
    ASSERT_FALSE(del.HasError()) << (int)del.GetError();
    auto del2 = dbms.Delete("db1");
    ASSERT_TRUE(del2.HasError() && del2.GetError() == memgraph::dbms::DeleteError::NON_EXISTENT);
  }
  {
    auto del = dbms.Delete("db3");
    ASSERT_FALSE(del.HasError());
    ASSERT_FALSE(std::filesystem::exists(storage_directory / "databases" / "db3"));
  }
}

int main(int argc, char *argv[]) {
  ::testing::InitGoogleTest(&argc, argv);
  // gtest takes ownership of the TestEnvironment ptr - we don't delete it.
  ::testing::AddGlobalTestEnvironment(new TestEnvironment);
  return RUN_ALL_TESTS();
}

#endif
