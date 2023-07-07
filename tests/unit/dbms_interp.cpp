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
  memgraph::storage::Storage db;
  TestAuthHandler ah;
  TestAuthChecker ac;

  {
    // Clean initialization
    auto ic1 = ih.New("ic1", test_handler, db, default_conf, storage_directory, ah, ac);
    ASSERT_TRUE(ic1.HasValue() && ic1.GetValue() != nullptr);
    ASSERT_TRUE(std::filesystem::exists(storage_directory / "triggers"));
    ASSERT_TRUE(std::filesystem::exists(storage_directory / "streams"));
    ASSERT_TRUE(ic1.GetValue()->db == &db);
    ASSERT_EQ(&ic1.GetValue()->sc_handler_, &test_handler);
    ASSERT_EQ(ih.GetConfig("ic1")->storage_dir, storage_directory);
  }
  {
    memgraph::storage::Storage db2;
    // Try to override data directory
    auto ic2 = ih.New("ic2", test_handler, db2, default_conf, storage_directory, ah, ac);
    ASSERT_TRUE(ic2.HasError() && ic2.GetError() == memgraph::dbms::NewError::EXISTS);
  }
  {
    // Try to reuse tha same Storage
    auto ic3 = ih.New("ic3", test_handler, db, default_conf, storage_directory / "ic3", ah, ac);
    ASSERT_TRUE(ic3.HasError() && ic3.GetError() == memgraph::dbms::NewError::EXISTS);
  }
  {
    memgraph::storage::Storage db4;
    // Try to override the name "ic1"
    auto ic4 = ih.New("ic1", test_handler, db4, default_conf, storage_directory / "ic4", ah, ac);
    ASSERT_TRUE(ic4.HasError() && ic4.GetError() == memgraph::dbms::NewError::EXISTS);
  }
  {
    // Another clean initialization
    memgraph::storage::Storage db5;
    auto ic5 = ih.New("ic5", test_handler, db5, default_conf, storage_directory / "ic5", ah, ac);
    ASSERT_TRUE(ic5.HasValue() && ic5.GetValue() != nullptr);
    ASSERT_TRUE(std::filesystem::exists(storage_directory / "ic5" / "triggers"));
    ASSERT_TRUE(std::filesystem::exists(storage_directory / "ic5" / "streams"));
    ASSERT_EQ(&ic5.GetValue()->sc_handler_, &test_handler);
    ASSERT_EQ(ih.GetConfig("ic5")->storage_dir, storage_directory / "ic5");
  }
}

TEST_F(DBMS_Interp, Get) {
  memgraph::dbms::InterpContextHandler<TestHandler> ih;
  TestAuthHandler ah;
  TestAuthChecker ac;

  memgraph::storage::Storage db1;
  auto ic1 = ih.New("ic1", test_handler, db1, default_conf, storage_directory / "ic1", ah, ac);
  ASSERT_TRUE(ic1.HasValue() && ic1.GetValue() != nullptr);

  auto ic1_get = ih.Get("ic1");
  ASSERT_TRUE(ic1_get && *ic1_get == ic1.GetValue());

  memgraph::storage::Storage db2;
  auto ic2 = ih.New("ic2", test_handler, db2, default_conf, storage_directory / "ic2", ah, ac);
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

  memgraph::storage::Storage db1;
  {
    auto ic1 = ih.New("ic1", test_handler, db1, default_conf, storage_directory / "ic1", ah, ac);
    ASSERT_TRUE(ic1.HasValue() && ic1.GetValue() != nullptr);
  }

  memgraph::storage::Storage db2;
  {
    auto ic2 = ih.New("ic2", test_handler, db2, default_conf, storage_directory / "ic2", ah, ac);
    ASSERT_TRUE(ic2.HasValue() && ic2.GetValue() != nullptr);
  }

  ASSERT_TRUE(ih.Delete("ic1"));
  ASSERT_FALSE(ih.Get("ic1"));
  ASSERT_FALSE(ih.Delete("ic1"));
  ASSERT_FALSE(ih.Delete("ic3"));
}

#endif
