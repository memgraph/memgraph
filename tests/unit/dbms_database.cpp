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

#include <gmock/gmock.h>
#include <gtest/gtest.h>
#include <filesystem>

#include "dbms/database_handler.hpp"
#include "dbms/global.hpp"

#include "license/license.hpp"
#include "query_plan_common.hpp"
#include "storage/v2/view.hpp"

std::filesystem::path storage_directory{std::filesystem::temp_directory_path() / "MG_test_unit_dbms_database"};

memgraph::storage::Config default_conf(std::string name = "") {
  return {.durability = {.storage_directory = storage_directory / name,
                         .snapshot_wal_mode =
                             memgraph::storage::Config::Durability::SnapshotWalMode::PERIODIC_SNAPSHOT_WITH_WAL},
          .disk = {.main_storage_directory = storage_directory / name / "disk"}};
}

class DBMS_Database : public ::testing::Test {
 protected:
  void SetUp() override { Clear(); }

  void TearDown() override { Clear(); }

 private:
  void Clear() {
    if (std::filesystem::exists(storage_directory)) {
      std::filesystem::remove_all(storage_directory);
    }
  }
};

#ifdef MG_ENTERPRISE
TEST_F(DBMS_Database, New) {
  memgraph::dbms::DatabaseHandler db_handler;
  { ASSERT_FALSE(db_handler.GetConfig("db1")); }
  {  // With custom config
    memgraph::storage::Config db_config{
        .durability = {.storage_directory = storage_directory / "db2",
                       .snapshot_wal_mode =
                           memgraph::storage::Config::Durability::SnapshotWalMode::PERIODIC_SNAPSHOT_WITH_WAL},
        .disk = {.main_storage_directory = storage_directory / "disk"}};
    auto db2 = db_handler.New("db2", db_config);
    ASSERT_TRUE(db2.HasValue() && db2.GetValue());
    ASSERT_TRUE(std::filesystem::exists(storage_directory / "db2"));
  }
  {
    // With default config
    auto db3 = db_handler.New("db3", default_conf("db3"));
    ASSERT_TRUE(db3.HasValue() && db3.GetValue());
    ASSERT_TRUE(std::filesystem::exists(storage_directory / "db3"));
    auto db4 = db_handler.New("db4", default_conf("four"));
    ASSERT_TRUE(db4.HasValue() && db4.GetValue());
    ASSERT_TRUE(std::filesystem::exists(storage_directory / "four"));
    auto db5 = db_handler.New("db5", default_conf("db3"));
    ASSERT_TRUE(db5.HasError() && db5.GetError() == memgraph::dbms::NewError::EXISTS);
  }

  auto all = db_handler.All();
  std::sort(all.begin(), all.end());
  ASSERT_EQ(all.size(), 3);
  ASSERT_EQ(all[0], "db2");
  ASSERT_EQ(all[1], "db3");
  ASSERT_EQ(all[2], "db4");
}

TEST_F(DBMS_Database, Get) {
  memgraph::dbms::DatabaseHandler db_handler;

  auto db1 = db_handler.New("db1", default_conf("db1"));
  auto db2 = db_handler.New("db2", default_conf("db2"));
  auto db3 = db_handler.New("db3", default_conf("db3"));

  ASSERT_TRUE(db1.HasValue());
  ASSERT_TRUE(db2.HasValue());
  ASSERT_TRUE(db3.HasValue());

  auto get_db1 = db_handler.Get("db1");
  auto get_db2 = db_handler.Get("db2");
  auto get_db3 = db_handler.Get("db3");

  ASSERT_TRUE(get_db1 && *get_db1 == db1.GetValue());
  ASSERT_TRUE(get_db2 && *get_db2 == db2.GetValue());
  ASSERT_TRUE(get_db3 && *get_db3 == db3.GetValue());

  ASSERT_FALSE(db_handler.Get("db123"));
  ASSERT_FALSE(db_handler.Get("db2 "));
  ASSERT_FALSE(db_handler.Get(" db3"));
}

TEST_F(DBMS_Database, Delete) {
  memgraph::dbms::DatabaseHandler db_handler;

  auto db1 = db_handler.New("db1", default_conf("db1"));
  auto db2 = db_handler.New("db2", default_conf("db2"));
  auto db3 = db_handler.New("db3", default_conf("db3"));

  ASSERT_TRUE(db1.HasValue());
  ASSERT_TRUE(db2.HasValue());
  ASSERT_TRUE(db3.HasValue());

  {
    // Release accessor to storage
    db1.GetValue().reset();
    // Delete from handler
    ASSERT_TRUE(db_handler.Delete("db1"));
    ASSERT_FALSE(db_handler.Get("db1"));
    auto all = db_handler.All();
    std::sort(all.begin(), all.end());
    ASSERT_EQ(all.size(), 2);
    ASSERT_EQ(all[0], "db2");
    ASSERT_EQ(all[1], "db3");
  }

  {
    ASSERT_THROW(db_handler.Delete("db0"), memgraph::utils::BasicException);
    ASSERT_THROW(db_handler.Delete("db1"), memgraph::utils::BasicException);
    auto all = db_handler.All();
    std::sort(all.begin(), all.end());
    ASSERT_EQ(all.size(), 2);
    ASSERT_EQ(all[0], "db2");
    ASSERT_EQ(all[1], "db3");
  }
}

TEST_F(DBMS_Database, DeleteAndRecover) {
  memgraph::license::global_license_checker.EnableTesting();
  memgraph::dbms::DatabaseHandler db_handler;

  {
    auto db1 = db_handler.New("db1", default_conf("db1"));
    auto db2 = db_handler.New("db2", default_conf("db2"));

    memgraph::storage::Config conf_w_snap{
        .durability = {.storage_directory = storage_directory / "db3",
                       .snapshot_wal_mode =
                           memgraph::storage::Config::Durability::SnapshotWalMode::PERIODIC_SNAPSHOT_WITH_WAL,
                       .snapshot_on_exit = true},
        .disk = {.main_storage_directory = storage_directory / "db3" / "disk"}};

    auto db3 = db_handler.New("db3", conf_w_snap);

    ASSERT_TRUE(db1.HasValue());
    ASSERT_TRUE(db2.HasValue());
    ASSERT_TRUE(db3.HasValue());

    // Add data to graphs
    {
      auto storage_dba = db1.GetValue()->Access();
      memgraph::query::DbAccessor dba{storage_dba.get()};
      memgraph::query::VertexAccessor v1{dba.InsertVertex()};
      memgraph::query::VertexAccessor v2{dba.InsertVertex()};
      ASSERT_TRUE(v1.AddLabel(dba.NameToLabel("l11")).HasValue());
      ASSERT_TRUE(v2.AddLabel(dba.NameToLabel("l12")).HasValue());
      ASSERT_FALSE(dba.Commit().HasError());
    }
    {
      auto storage_dba = db3.GetValue()->Access();
      memgraph::query::DbAccessor dba{storage_dba.get()};
      memgraph::query::VertexAccessor v1{dba.InsertVertex()};
      memgraph::query::VertexAccessor v2{dba.InsertVertex()};
      memgraph::query::VertexAccessor v3{dba.InsertVertex()};
      ASSERT_TRUE(v1.AddLabel(dba.NameToLabel("l31")).HasValue());
      ASSERT_TRUE(v2.AddLabel(dba.NameToLabel("l32")).HasValue());
      ASSERT_TRUE(v3.AddLabel(dba.NameToLabel("l33")).HasValue());
      ASSERT_FALSE(dba.Commit().HasError());
    }
  }

  // Delete from handler
  ASSERT_TRUE(db_handler.Delete("db1"));
  ASSERT_TRUE(db_handler.Delete("db2"));
  ASSERT_TRUE(db_handler.Delete("db3"));

  {
    // Recover graphs (only db3)
    auto db1 = db_handler.New("db1", default_conf("db1"));
    auto db2 = db_handler.New("db2", default_conf("db2"));

    memgraph::storage::Config conf_w_rec{
        .durability = {.storage_directory = storage_directory / "db3",
                       .recover_on_startup = true,
                       .snapshot_wal_mode =
                           memgraph::storage::Config::Durability::SnapshotWalMode::PERIODIC_SNAPSHOT_WITH_WAL},
        .disk = {.main_storage_directory = storage_directory / "db3" / "disk"}};

    auto db3 = db_handler.New("db3", conf_w_rec);

    // Check content
    {
      // Empty
      auto storage_dba = db1.GetValue()->Access();
      memgraph::query::DbAccessor dba{storage_dba.get()};
      ASSERT_EQ(dba.VerticesCount(), 0);
    }
    {
      // Empty
      auto storage_dba = db2.GetValue()->Access();
      memgraph::query::DbAccessor dba{storage_dba.get()};
      ASSERT_EQ(dba.VerticesCount(), 0);
    }
    {
      // Full
      auto storage_dba = db3.GetValue()->Access();
      memgraph::query::DbAccessor dba{storage_dba.get()};
      ASSERT_EQ(dba.VerticesCount(), 3);
    }
  }
}

#endif
