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

#ifdef MG_ENTERPRISE

#include <gmock/gmock.h>
#include <gtest/gtest.h>
#include <filesystem>

#include "dbms/global.hpp"
#include "dbms/interp_handler.hpp"

#include "query/auth_checker.hpp"
#include "query/frontend/ast/ast.hpp"
#include "query/interpreter.hpp"

class TestAuthHandler : public memgraph::query::AuthQueryHandler {
 public:
  TestAuthHandler() = default;

  bool CreateUser(const std::string & /*username*/, const std::optional<std::string> & /*password*/) override {
    return true;
  }
  bool DropUser(const std::string & /*username*/) override { return true; }
  void SetPassword(const std::string & /*username*/, const std::optional<std::string> & /*password*/) override {}
  bool RevokeDatabaseFromUser(const std::string & /*db*/, const std::string & /*username*/) override { return true; }
  bool GrantDatabaseToUser(const std::string & /*db*/, const std::string & /*username*/) override { return true; }
  bool SetMainDatabase(const std::string & /*db*/, const std::string & /*username*/) override { return true; }
  std::vector<std::vector<memgraph::query::TypedValue>> GetDatabasePrivileges(const std::string & /*user*/) override {
    return {};
  }
  bool CreateRole(const std::string & /*rolename*/) override { return true; }
  bool DropRole(const std::string & /*rolename*/) override { return true; }
  std::vector<memgraph::query::TypedValue> GetUsernames() override { return {}; }
  std::vector<memgraph::query::TypedValue> GetRolenames() override { return {}; }
  std::optional<std::string> GetRolenameForUser(const std::string & /*username*/) override { return {}; }
  std::vector<memgraph::query::TypedValue> GetUsernamesForRole(const std::string & /*rolename*/) override { return {}; }
  void SetRole(const std::string &username, const std::string & /*rolename*/) override {}
  void ClearRole(const std::string &username) override {}
  std::vector<std::vector<memgraph::query::TypedValue>> GetPrivileges(const std::string & /*user_or_role*/) override {
    return {};
  }
  void GrantPrivilege(
      const std::string & /*user_or_role*/, const std::vector<memgraph::query::AuthQuery::Privilege> & /*privileges*/,
      const std::vector<std::unordered_map<memgraph::query::AuthQuery::FineGrainedPrivilege, std::vector<std::string>>>
          & /*label_privileges*/,
      const std::vector<std::unordered_map<memgraph::query::AuthQuery::FineGrainedPrivilege, std::vector<std::string>>>
          & /*edge_type_privileges*/) override {}
  void DenyPrivilege(const std::string & /*user_or_role*/,
                     const std::vector<memgraph::query::AuthQuery::Privilege> & /*privileges*/) override {}
  void RevokePrivilege(
      const std::string & /*user_or_role*/, const std::vector<memgraph::query::AuthQuery::Privilege> & /*privileges*/,
      const std::vector<std::unordered_map<memgraph::query::AuthQuery::FineGrainedPrivilege, std::vector<std::string>>>
          & /*label_privileges*/,
      const std::vector<std::unordered_map<memgraph::query::AuthQuery::FineGrainedPrivilege, std::vector<std::string>>>
          & /*edge_type_privileges*/) override {}
};

class TestAuthChecker : public memgraph::query::AuthChecker {
 public:
  bool IsUserAuthorized(const std::optional<std::string> & /*username*/,
                        const std::vector<memgraph::query::AuthQuery::Privilege> & /*privileges*/,
                        const std::string & /*db*/) const override {
    return true;
  }

  std::unique_ptr<memgraph::query::FineGrainedAuthChecker> GetFineGrainedAuthChecker(
      const std::string & /*username*/, const memgraph::query::DbAccessor * /*db_accessor*/) const override {
    return {};
  }
};

std::filesystem::path storage_directory{std::filesystem::temp_directory_path() / "MG_test_unit_dbms_interp"};

memgraph::query::InterpreterConfig default_conf{};

memgraph::storage::Config default_storage_conf(std::string name = "") {
  return {.durability = {.storage_directory = storage_directory / name,
                         .snapshot_wal_mode =
                             memgraph::storage::Config::Durability::SnapshotWalMode::PERIODIC_SNAPSHOT_WITH_WAL},
          .disk = {.main_storage_directory = storage_directory / name / "disk"}};
}

class TestHandler {
} test_handler;

class DBMS_Interp : public ::testing::Test {
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

TEST_F(DBMS_Interp, New) {
  memgraph::dbms::InterpContextHandler<TestHandler> ih;
  memgraph::storage::Config db_conf{
      .durability = {.storage_directory = storage_directory,
                     .snapshot_wal_mode =
                         memgraph::storage::Config::Durability::SnapshotWalMode::PERIODIC_SNAPSHOT_WITH_WAL},
      .disk = {.main_storage_directory = storage_directory / "disk"}};
  TestAuthHandler ah;
  TestAuthChecker ac;

  {
    // Clean initialization
    auto ic1 = ih.New("ic1", test_handler, db_conf, default_conf, ah, ac);
    ASSERT_TRUE(ic1.HasValue() && ic1.GetValue() != nullptr);
    ASSERT_TRUE(std::filesystem::exists(storage_directory / "triggers"));
    ASSERT_TRUE(std::filesystem::exists(storage_directory / "streams"));
    ASSERT_NE(ic1.GetValue()->db, nullptr);
    ASSERT_EQ(&ic1.GetValue()->sc_handler_, &test_handler);
    ASSERT_EQ(ih.GetConfig("ic1")->storage_config.durability.storage_directory, storage_directory);
  }
  {
    memgraph::storage::Config db_conf2{
        .durability = {.storage_directory = storage_directory,
                       .snapshot_wal_mode =
                           memgraph::storage::Config::Durability::SnapshotWalMode::PERIODIC_SNAPSHOT_WITH_WAL},
        .disk = {.main_storage_directory = storage_directory / "disk"}};
    // Try to override data directory
    auto ic2 = ih.New("ic2", test_handler, db_conf2, default_conf, ah, ac);
    ASSERT_TRUE(ic2.HasError() && ic2.GetError() == memgraph::dbms::NewError::EXISTS);
  }
  {
    memgraph::storage::Config db_conf3{
        .durability = {.storage_directory = storage_directory / "ic3",
                       .snapshot_wal_mode =
                           memgraph::storage::Config::Durability::SnapshotWalMode::PERIODIC_SNAPSHOT_WITH_WAL},
        .disk = {.main_storage_directory = storage_directory / "disk"}};
    // Try to override the name "ic1"
    auto ic3 = ih.New("ic1", test_handler, db_conf3, default_conf, ah, ac);
    ASSERT_TRUE(ic3.HasError() && ic3.GetError() == memgraph::dbms::NewError::EXISTS);
  }
  {
    // Another clean initialization
    memgraph::storage::Config db_conf4{
        .durability = {.storage_directory = storage_directory / "ic4",
                       .snapshot_wal_mode =
                           memgraph::storage::Config::Durability::SnapshotWalMode::PERIODIC_SNAPSHOT_WITH_WAL},
        .disk = {.main_storage_directory = storage_directory / "disk"}};
    auto ic4 = ih.New("ic4", test_handler, db_conf4, default_conf, ah, ac);
    ASSERT_TRUE(ic4.HasValue() && ic4.GetValue() != nullptr);
    ASSERT_TRUE(std::filesystem::exists(storage_directory / "ic4" / "triggers"));
    ASSERT_TRUE(std::filesystem::exists(storage_directory / "ic4" / "streams"));
    ASSERT_EQ(&ic4.GetValue()->sc_handler_, &test_handler);
    ASSERT_EQ(ih.GetConfig("ic4")->storage_config.durability.storage_directory, storage_directory / "ic4");
  }
}

TEST_F(DBMS_Interp, Get) {
  memgraph::dbms::InterpContextHandler<TestHandler> ih;
  TestAuthHandler ah;
  TestAuthChecker ac;

  memgraph::storage::Config db_conf{
      .durability = {.storage_directory = storage_directory / "ic1",
                     .snapshot_wal_mode =
                         memgraph::storage::Config::Durability::SnapshotWalMode::PERIODIC_SNAPSHOT_WITH_WAL},
      .disk = {.main_storage_directory = storage_directory / "disk"}};
  auto ic1 = ih.New("ic1", test_handler, db_conf, default_conf, ah, ac);
  ASSERT_TRUE(ic1.HasValue() && ic1.GetValue() != nullptr);

  auto ic1_get = ih.Get("ic1");
  ASSERT_TRUE(ic1_get && *ic1_get == ic1.GetValue());

  memgraph::storage::Config db_conf2{
      .durability = {.storage_directory = storage_directory / "ic2",
                     .snapshot_wal_mode =
                         memgraph::storage::Config::Durability::SnapshotWalMode::PERIODIC_SNAPSHOT_WITH_WAL},
      .disk = {.main_storage_directory = storage_directory / "disk"}};
  auto ic2 = ih.New("ic2", test_handler, db_conf2, default_conf, ah, ac);
  ASSERT_TRUE(ic2.HasValue() && ic2.GetValue() != nullptr);

  auto ic2_get = ih.Get("ic2");
  ASSERT_TRUE(ic2_get && *ic2_get == ic2.GetValue());

  ASSERT_FALSE(ih.Get("aa"));
  ASSERT_FALSE(ih.Get("ic1 "));
  ASSERT_FALSE(ih.Get("ic21"));
  ASSERT_FALSE(ih.Get(" ic2"));
}

TEST_F(DBMS_Interp, Delete) {
  memgraph::dbms::InterpContextHandler<TestHandler> ih;
  TestAuthHandler ah;
  TestAuthChecker ac;

  memgraph::storage::Config db_conf{
      .durability = {.storage_directory = storage_directory / "ic1",
                     .snapshot_wal_mode =
                         memgraph::storage::Config::Durability::SnapshotWalMode::PERIODIC_SNAPSHOT_WITH_WAL},
      .disk = {.main_storage_directory = storage_directory / "disk"}};
  {
    auto ic1 = ih.New("ic1", test_handler, db_conf, default_conf, ah, ac);
    ASSERT_TRUE(ic1.HasValue() && ic1.GetValue() != nullptr);
  }

  memgraph::storage::Config db_conf2{
      .durability = {.storage_directory = storage_directory / "ic2",
                     .snapshot_wal_mode =
                         memgraph::storage::Config::Durability::SnapshotWalMode::PERIODIC_SNAPSHOT_WITH_WAL},
      .disk = {.main_storage_directory = storage_directory / "disk"}};
  {
    auto ic2 = ih.New("ic2", test_handler, db_conf2, default_conf, ah, ac);
    ASSERT_TRUE(ic2.HasValue() && ic2.GetValue() != nullptr);
  }

  ASSERT_TRUE(ih.Delete("ic1"));
  ASSERT_FALSE(ih.Get("ic1"));
  ASSERT_FALSE(ih.Delete("ic1"));
  ASSERT_FALSE(ih.Delete("ic3"));
}

/**
 *
 *
 *
 *
 *
 * Test storage (previous StorageHandler, now handled via InterpretContext)
 *
 *
 *
 *
 *
 */
TEST_F(DBMS_Interp, StorageNew) {
  memgraph::dbms::InterpContextHandler<TestHandler> ih;
  TestAuthHandler ah;
  TestAuthChecker ac;

  { ASSERT_FALSE(ih.GetConfig("db1")); }
  {
    // With custom config
    memgraph::storage::Config db_config{
        .durability = {.storage_directory = storage_directory / "db2",
                       .snapshot_wal_mode =
                           memgraph::storage::Config::Durability::SnapshotWalMode::PERIODIC_SNAPSHOT_WITH_WAL},
        .disk = {.main_storage_directory = storage_directory / "disk"}};
    auto db2 = ih.New("db2", test_handler, db_config, default_conf, ah, ac);
    ASSERT_TRUE(db2.HasValue() && db2.GetValue() != nullptr);
    ASSERT_TRUE(std::filesystem::exists(storage_directory / "db2"));
  }
  {
    // With default config
    auto db3 = ih.New("db3", test_handler, default_storage_conf("db3"), default_conf, ah, ac);
    ASSERT_TRUE(db3.HasValue() && db3.GetValue() != nullptr);
    ASSERT_TRUE(std::filesystem::exists(storage_directory / "db3"));
    auto db4 = ih.New("db4", test_handler, default_storage_conf("four"), default_conf, ah, ac);
    ASSERT_TRUE(db4.HasValue() && db4.GetValue() != nullptr);
    ASSERT_TRUE(std::filesystem::exists(storage_directory / "four"));
    auto db5 = ih.New("db5", test_handler, default_storage_conf("db3"), default_conf, ah, ac);
    ASSERT_TRUE(db5.HasError() && db5.GetError() == memgraph::dbms::NewError::EXISTS);
  }

  auto all = ih.All();
  std::sort(all.begin(), all.end());
  ASSERT_EQ(all.size(), 3);
  ASSERT_EQ(all[0], "db2");
  ASSERT_EQ(all[1], "db3");
  ASSERT_EQ(all[2], "db4");
}

TEST_F(DBMS_Interp, StorageGet) {
  memgraph::dbms::InterpContextHandler<TestHandler> ih;
  TestAuthHandler ah;
  TestAuthChecker ac;

  auto db1 = ih.New("db1", test_handler, default_storage_conf("db1"), default_conf, ah, ac);
  auto db2 = ih.New("db2", test_handler, default_storage_conf("db2"), default_conf, ah, ac);
  auto db3 = ih.New("db3", test_handler, default_storage_conf("db3"), default_conf, ah, ac);

  ASSERT_TRUE(db1.HasValue());
  ASSERT_TRUE(db2.HasValue());
  ASSERT_TRUE(db3.HasValue());

  auto get_db1 = ih.Get("db1");
  auto get_db2 = ih.Get("db2");
  auto get_db3 = ih.Get("db3");

  ASSERT_TRUE(get_db1 && *get_db1 == db1.GetValue());
  ASSERT_TRUE(get_db2 && *get_db2 == db2.GetValue());
  ASSERT_TRUE(get_db3 && *get_db3 == db3.GetValue());

  ASSERT_FALSE(ih.Get("db123"));
  ASSERT_FALSE(ih.Get("db2 "));
  ASSERT_FALSE(ih.Get(" db3"));
}

TEST_F(DBMS_Interp, StorageDelete) {
  memgraph::dbms::InterpContextHandler<TestHandler> ih;
  TestAuthHandler ah;
  TestAuthChecker ac;

  auto db1 = ih.New("db1", test_handler, default_storage_conf("db1"), default_conf, ah, ac);
  auto db2 = ih.New("db2", test_handler, default_storage_conf("db2"), default_conf, ah, ac);
  auto db3 = ih.New("db3", test_handler, default_storage_conf("db3"), default_conf, ah, ac);

  ASSERT_TRUE(db1.HasValue());
  ASSERT_TRUE(db2.HasValue());
  ASSERT_TRUE(db3.HasValue());

  {
    // Release pointer to storage
    db1.GetValue().reset();
    // Delete from handler
    ASSERT_TRUE(ih.Delete("db1"));
    ASSERT_FALSE(ih.Get("db1"));
    auto all = ih.All();
    std::sort(all.begin(), all.end());
    ASSERT_EQ(all.size(), 2);
    ASSERT_EQ(all[0], "db2");
    ASSERT_EQ(all[1], "db3");
  }

  {
    ASSERT_FALSE(ih.Delete("db0"));
    ASSERT_FALSE(ih.Delete("db1"));
    auto all = ih.All();
    std::sort(all.begin(), all.end());
    ASSERT_EQ(all.size(), 2);
    ASSERT_EQ(all[0], "db2");
    ASSERT_EQ(all[1], "db3");
  }
}

TEST_F(DBMS_Interp, StorageDeleteAndRecover) {
  // memgraph::license::global_license_checker.EnableTesting();
  memgraph::dbms::InterpContextHandler<TestHandler> ih;
  TestAuthHandler ah;
  TestAuthChecker ac;

  {
    auto db1 = ih.New("db1", test_handler, default_storage_conf("db1"), default_conf, ah, ac);
    auto db2 = ih.New("db2", test_handler, default_storage_conf("db2"), default_conf, ah, ac);

    memgraph::storage::Config conf_w_snap{
        .durability = {.storage_directory = storage_directory / "db3",
                       .snapshot_wal_mode =
                           memgraph::storage::Config::Durability::SnapshotWalMode::PERIODIC_SNAPSHOT_WITH_WAL,
                       .snapshot_on_exit = true},
        .disk = {.main_storage_directory = storage_directory / "db3" / "disk"}};

    auto db3 = ih.New("db3", test_handler, conf_w_snap, default_conf, ah, ac);

    ASSERT_TRUE(db1.HasValue());
    ASSERT_TRUE(db2.HasValue());
    ASSERT_TRUE(db3.HasValue());

    // Add data to graphs
    {
      auto storage_dba = db1.GetValue()->db->Access();
      memgraph::query::DbAccessor dba{storage_dba.get()};
      memgraph::query::VertexAccessor v1{dba.InsertVertex()};
      memgraph::query::VertexAccessor v2{dba.InsertVertex()};
      ASSERT_TRUE(v1.AddLabel(dba.NameToLabel("l11")).HasValue());
      ASSERT_TRUE(v2.AddLabel(dba.NameToLabel("l12")).HasValue());
      ASSERT_FALSE(dba.Commit().HasError());
    }
    {
      auto storage_dba = db3.GetValue()->db->Access();
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
  ASSERT_TRUE(ih.Delete("db1"));
  ASSERT_TRUE(ih.Delete("db2"));
  ASSERT_TRUE(ih.Delete("db3"));

  {
    // Recover graphs (only db3)
    auto db1 = ih.New("db1", test_handler, default_storage_conf("db1"), default_conf, ah, ac);
    auto db2 = ih.New("db2", test_handler, default_storage_conf("db2"), default_conf, ah, ac);

    memgraph::storage::Config conf_w_rec{
        .durability = {.storage_directory = storage_directory / "db3",
                       .recover_on_startup = true,
                       .snapshot_wal_mode =
                           memgraph::storage::Config::Durability::SnapshotWalMode::PERIODIC_SNAPSHOT_WITH_WAL},
        .disk = {.main_storage_directory = storage_directory / "db3" / "disk"}};

    auto db3 = ih.New("db3", test_handler, conf_w_rec, default_conf, ah, ac);

    // Check content
    {
      // Empty
      auto storage_dba = db1.GetValue()->db->Access();
      memgraph::query::DbAccessor dba{storage_dba.get()};
      ASSERT_EQ(dba.VerticesCount(), 0);
    }
    {
      // Empty
      auto storage_dba = db2.GetValue()->db->Access();
      memgraph::query::DbAccessor dba{storage_dba.get()};
      ASSERT_EQ(dba.VerticesCount(), 0);
    }
    {
      // Full
      auto storage_dba = db3.GetValue()->db->Access();
      memgraph::query::DbAccessor dba{storage_dba.get()};
      ASSERT_EQ(dba.VerticesCount(), 3);
    }
  }
}

#endif
