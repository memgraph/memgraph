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

#include "dbms/global.hpp"
#include "dbms/storage_handler.hpp"

#include "license/license.hpp"
#include "query_plan_common.hpp"
#include "storage/v2/view.hpp"

std::filesystem::path storage_directory{std::filesystem::temp_directory_path() / "MG_test_unit_dbms_storage"};

memgraph::storage::Config default_conf(std::string name = "") {
  return {.durability = {
              .storage_directory = storage_directory / name,
              .snapshot_wal_mode = memgraph::storage::Config::Durability::SnapshotWalMode::PERIODIC_SNAPSHOT_WITH_WAL}};
}

#ifdef MG_ENTERPRISE
TEST(DBMS_Storage, New) {
  memgraph::dbms::StorageHandler sh;
  { ASSERT_FALSE(sh.GetConfig("db1")); }
  {
    // With custom config
    memgraph::storage::Config db_config{
        .durability = {
            .storage_directory = storage_directory / "db2",
            .snapshot_wal_mode = memgraph::storage::Config::Durability::SnapshotWalMode::PERIODIC_SNAPSHOT_WITH_WAL}};
    auto db2 = sh.New("db2", db_config);
    ASSERT_TRUE(db2.HasValue() && db2.GetValue() != nullptr);
    ASSERT_TRUE(std::filesystem::exists(storage_directory / "db2"));
  }
  {
    // With default config
    auto db3 = sh.New("db3", default_conf("db3"));
    ASSERT_TRUE(db3.HasValue() && db3.GetValue() != nullptr);
    ASSERT_TRUE(std::filesystem::exists(storage_directory / "db3"));
    auto db4 = sh.New("db4", default_conf("four"));
    ASSERT_TRUE(db4.HasValue() && db4.GetValue() != nullptr);
    ASSERT_TRUE(std::filesystem::exists(storage_directory / "four"));
    auto db5 = sh.New("db5", default_conf("db3"));
    ASSERT_TRUE(db5.HasError() && db5.GetError() == memgraph::dbms::NewError::EXISTS);
  }

  auto all = sh.All();
  std::sort(all.begin(), all.end());
  ASSERT_EQ(all.size(), 3);
  ASSERT_EQ(all[0], "db2");
  ASSERT_EQ(all[1], "db3");
  ASSERT_EQ(all[2], "db4");
}

TEST(DBMS_Storage, Get) {
  memgraph::dbms::StorageHandler sh;

  auto db1 = sh.New("db1", default_conf("db1"));
  auto db2 = sh.New("db2", default_conf("db2"));
  auto db3 = sh.New("db3", default_conf("db3"));

  ASSERT_TRUE(db1.HasValue());
  ASSERT_TRUE(db2.HasValue());
  ASSERT_TRUE(db3.HasValue());

  auto get_db1 = sh.Get("db1");
  auto get_db2 = sh.Get("db2");
  auto get_db3 = sh.Get("db3");

  ASSERT_TRUE(get_db1 && *get_db1 == db1.GetValue());
  ASSERT_TRUE(get_db2 && *get_db2 == db2.GetValue());
  ASSERT_TRUE(get_db3 && *get_db3 == db3.GetValue());

  ASSERT_FALSE(sh.Get("db123"));
  ASSERT_FALSE(sh.Get("db2 "));
  ASSERT_FALSE(sh.Get(" db3"));
}

TEST(DBMS_Storage, Delete) {
  memgraph::dbms::StorageHandler sh;

  auto db1 = sh.New("db1", default_conf("db1"));
  auto db2 = sh.New("db2", default_conf("db2"));
  auto db3 = sh.New("db3", default_conf("db3"));

  ASSERT_TRUE(db1.HasValue());
  ASSERT_TRUE(db2.HasValue());
  ASSERT_TRUE(db3.HasValue());

  {
    // Release pointer to storage
    db1.GetValue().reset();
    // Delete from handler
    ASSERT_TRUE(sh.Delete("db1"));
    ASSERT_FALSE(sh.Get("db1"));
    auto all = sh.All();
    std::sort(all.begin(), all.end());
    ASSERT_EQ(all.size(), 2);
    ASSERT_EQ(all[0], "db2");
    ASSERT_EQ(all[1], "db3");
  }

  {
    ASSERT_FALSE(sh.Delete("db0"));
    ASSERT_FALSE(sh.Delete("db1"));
    auto all = sh.All();
    std::sort(all.begin(), all.end());
    ASSERT_EQ(all.size(), 2);
    ASSERT_EQ(all[0], "db2");
    ASSERT_EQ(all[1], "db3");
  }
}

TEST(DBMS_Storage, DeleteAndRecover) {
  memgraph::license::global_license_checker.EnableTesting();
  memgraph::dbms::StorageHandler sh;

  {
    auto db1 = sh.New("db1", default_conf("db1"));
    auto db2 = sh.New("db2", default_conf("db2"));

    memgraph::storage::Config conf_w_snap{
        .durability = {
            .storage_directory = storage_directory / "db3",
            .snapshot_wal_mode = memgraph::storage::Config::Durability::SnapshotWalMode::PERIODIC_SNAPSHOT_WITH_WAL,
            .snapshot_on_exit = true}};

    auto db3 = sh.New("db3", conf_w_snap);

    ASSERT_TRUE(db1.HasValue());
    ASSERT_TRUE(db2.HasValue());
    ASSERT_TRUE(db3.HasValue());

    // Add data to graphs
    {
      memgraph::storage::Storage::Accessor storage_dba{db1.GetValue()->Access()};
      memgraph::query::DbAccessor dba{&storage_dba};
      memgraph::query::VertexAccessor v1{dba.InsertVertex()};
      memgraph::query::VertexAccessor v2{dba.InsertVertex()};
      ASSERT_TRUE(v1.AddLabel(dba.NameToLabel("l11")).HasValue());
      ASSERT_TRUE(v2.AddLabel(dba.NameToLabel("l12")).HasValue());
      ASSERT_FALSE(dba.Commit().HasError());
    }
    {
      memgraph::storage::Storage::Accessor storage_dba{db3.GetValue()->Access()};
      memgraph::query::DbAccessor dba{&storage_dba};
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
  ASSERT_TRUE(sh.Delete("db1"));
  ASSERT_TRUE(sh.Delete("db2"));
  ASSERT_TRUE(sh.Delete("db3"));

  {
    // Recover graphs (only db3)
    auto db1 = sh.New("db1", default_conf("db1"));
    auto db2 = sh.New("db2", default_conf("db2"));

    memgraph::storage::Config conf_w_rec{
        .durability = {
            .storage_directory = storage_directory / "db3",
            .recover_on_startup = true,
            .snapshot_wal_mode = memgraph::storage::Config::Durability::SnapshotWalMode::PERIODIC_SNAPSHOT_WITH_WAL}};

    auto db3 = sh.New("db3", conf_w_rec);

    // Check content
    {
      // Empty
      memgraph::storage::Storage::Accessor storage_dba{db1.GetValue()->Access()};
      memgraph::query::DbAccessor dba{&storage_dba};
      ASSERT_EQ(dba.VerticesCount(), 0);
    }
    {
      // Empty
      memgraph::storage::Storage::Accessor storage_dba{db2.GetValue()->Access()};
      memgraph::query::DbAccessor dba{&storage_dba};
      ASSERT_EQ(dba.VerticesCount(), 0);
    }
    {
      // Full
      memgraph::storage::Storage::Accessor storage_dba{db3.GetValue()->Access()};
      memgraph::query::DbAccessor dba{&storage_dba};
      ASSERT_EQ(dba.VerticesCount(), 3);
    }
  }
}

#endif
