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

#include <system_error>
#include "global.hpp"
#ifdef MG_ENTERPRISE

#include <gmock/gmock.h>
#include <gtest/gtest.h>
#include <filesystem>

#include "dbms/constants.hpp"
#include "dbms/session_context_handler.hpp"

#include "query/config.hpp"

std::filesystem::path storage_directory{std::filesystem::temp_directory_path() / "MG_test_unit_dbms_handler"};

memgraph::storage::Config storage_conf{
    .durability = {
        .storage_directory = storage_directory,
        .snapshot_wal_mode = memgraph::storage::Config::Durability::SnapshotWalMode::PERIODIC_SNAPSHOT_WITH_WAL}};

memgraph::query::InterpreterConfig interp_conf{};

// Global
memgraph::audit::Log audit_log{storage_directory / "audit", 100, 1000};

class TestInterface : public memgraph::dbms::SessionInterface {
 public:
  TestInterface(std::string name, auto on_change, auto on_delete) : id_(id++), db_(name) {
    on_change_ = on_change;
    on_delete_ = on_delete;
  }
  std::string UUID() const override { return std::to_string(id_); }
  std::string GetDB() const override { return db_; }
  memgraph::dbms::SetForResult OnChange(const std::string &name) override { return on_change_(name); }
  bool OnDelete(const std::string &name) override { return on_delete_(name); }

  static int id;
  int id_;
  std::string db_;
  std::function<memgraph::dbms::SetForResult(const std::string &)> on_change_;
  std::function<bool(const std::string &)> on_delete_;
};

int TestInterface::id{0};

// Let this be global so we can test it different states throughout

class TestEnvironment : public ::testing::Environment {
 public:
  static memgraph::dbms::SessionContextHandler *get() { return ptr_.get(); }

  // Initialise the timestamp.
  void SetUp() override {
    // Clean storage directory
    // if (std::filesystem::exists(storage_directory))
    //   for (const auto &entry : std::filesystem::directory_iterator(storage_directory))
    //     std::filesystem::remove_all(entry.path());
    ptr_ = std::make_unique<memgraph::dbms::SessionContextHandler>(
        audit_log, memgraph::dbms::SessionContextHandler::Config{storage_conf, interp_conf, ""}, false);
  }

  void TearDown() override { ptr_.reset(); }

  static std::unique_ptr<memgraph::dbms::SessionContextHandler> ptr_;
};

std::unique_ptr<memgraph::dbms::SessionContextHandler> TestEnvironment::ptr_ = nullptr;

class DBMS_Handler : public testing::Test {};
using DBMS_HandlerDeath = DBMS_Handler;

TEST(DBMS_Handler, Init) {
  // Check that the default db has been created successfully
  std::vector<std::string> dirs = {"auth", "snapshots", "streams", "triggers", "wal"};
  for (const auto &dir : dirs) ASSERT_TRUE(std::filesystem::exists(storage_directory / dir));
  const auto db_path = storage_directory / "databases" / memgraph::dbms::kDefaultDB;
  ASSERT_TRUE(std::filesystem::exists(db_path));
  for (const auto &dir : dirs) {
    std::error_code ec;
    const auto test_link = std::filesystem::read_symlink(db_path / dir, ec);
    ASSERT_TRUE(!ec && test_link == "../../" + dir);
  }
}

TEST(DBMS_HandlerDeath, InitSameDir) {
  // This will be executed in a clean process (so the singleton will NOT be initalized)
  (void)(::testing::GTEST_FLAG(death_test_style) = "threadsafe");
  // NOTE: Init test has ran in another process (so holds the lock)
  ASSERT_DEATH(
      {
        memgraph::dbms::SessionContextHandler sch(audit_log, {storage_conf, interp_conf, ""}, false);
      },
      R"(\b.*\b)");
}

TEST(DBMS_Handler, New) {
  auto &sch = *TestEnvironment::get();
  {
    const auto all = sch.All();
    ASSERT_EQ(all.size(), 1);
    ASSERT_EQ(all[0], memgraph::dbms::kDefaultDB);
  }
  {
    auto sc1 = sch.New("sc1");
    ASSERT_TRUE(sc1.HasValue());
    ASSERT_TRUE(std::filesystem::exists(storage_directory / "databases" / "sc1"));
    ASSERT_TRUE(sc1.GetValue().db != nullptr);
    ASSERT_TRUE(sc1.GetValue().interpreter_context != nullptr);
    ASSERT_TRUE(sc1.GetValue().audit_log != nullptr);
    ASSERT_TRUE(sc1.GetValue().auth_context != nullptr);
    const auto all = sch.All();
    ASSERT_EQ(all.size(), 2);
    ASSERT_TRUE(std::find(all.begin(), all.end(), memgraph::dbms::kDefaultDB) != all.end());
    ASSERT_TRUE(std::find(all.begin(), all.end(), "sc1") != all.end());
  }
  {
    // Fail if name exists
    auto sc2 = sch.New("sc1");
    ASSERT_TRUE(sc2.HasError() && sc2.GetError() == memgraph::dbms::NewError::EXISTS);
  }
  {
    auto sc3 = sch.New("sc3");
    ASSERT_TRUE(sc3.HasValue());
    ASSERT_TRUE(std::filesystem::exists(storage_directory / "databases" / "sc3"));
    ASSERT_TRUE(sc3.GetValue().db != nullptr);
    ASSERT_TRUE(sc3.GetValue().interpreter_context != nullptr);
    ASSERT_TRUE(sc3.GetValue().audit_log != nullptr);
    ASSERT_TRUE(sc3.GetValue().auth_context != nullptr);
    const auto all = sch.All();
    ASSERT_EQ(all.size(), 3);
    ASSERT_TRUE(std::find(all.begin(), all.end(), "sc3") != all.end());
  }
}

TEST(DBMS_Handler, Get) {
  auto &sch = *TestEnvironment::get();
  auto default_sc = sch.Get(memgraph::dbms::kDefaultDB);
  ASSERT_TRUE(default_sc.db != nullptr);
  ASSERT_TRUE(default_sc.interpreter_context != nullptr);
  ASSERT_TRUE(default_sc.audit_log != nullptr);
  ASSERT_TRUE(default_sc.auth_context != nullptr);

  ASSERT_ANY_THROW(sch.Get("non-existent"));

  auto sc1 = sch.Get("sc1");
  ASSERT_TRUE(sc1.db != nullptr);
  ASSERT_TRUE(sc1.interpreter_context != nullptr);
  ASSERT_TRUE(sc1.audit_log != nullptr);
  ASSERT_TRUE(sc1.auth_context != nullptr);

  auto sc3 = sch.Get("sc3");
  ASSERT_TRUE(sc3.db != nullptr);
  ASSERT_TRUE(sc3.interpreter_context != nullptr);
  ASSERT_TRUE(sc3.audit_log != nullptr);
  ASSERT_TRUE(sc3.auth_context != nullptr);
}

TEST(DBMS_Handler, SetFor) {
  auto &sch = *TestEnvironment::get();

  ASSERT_TRUE(sch.New("db1").HasValue());

  bool ti0_on_change_ = false;
  bool ti0_on_delete_ = false;
  TestInterface ti0(
      "memgraph",
      [&ti0, &ti0_on_change_](const std::string &name) -> memgraph::dbms::SetForResult {
        ti0_on_change_ = true;
        if (name != ti0.db_) {
          ti0.db_ = name;
          return memgraph::dbms::SetForResult::SUCCESS;
        }
        return memgraph::dbms::SetForResult::ALREADY_SET;
      },
      [&](const std::string &name) -> bool {
        ti0_on_delete_ = true;
        return true;
      });

  bool ti1_on_change_ = false;
  bool ti1_on_delete_ = false;
  TestInterface ti1(
      "db1",
      [&](const std::string &name) -> memgraph::dbms::SetForResult {
        ti1_on_change_ = true;
        return memgraph::dbms::SetForResult::SUCCESS;
      },
      [&](const std::string &name) -> bool {
        ti1_on_delete_ = true;
        return true;
      });

  ASSERT_TRUE(sch.Register(ti0));
  ASSERT_FALSE(sch.Register(ti0));

  {
    ASSERT_EQ(sch.SetFor("0", "db1"), memgraph::dbms::SetForResult::SUCCESS);
    ASSERT_TRUE(ti0_on_change_);
    ti0_on_change_ = false;
    ASSERT_EQ(sch.SetFor("0", "db1"), memgraph::dbms::SetForResult::ALREADY_SET);
    ASSERT_TRUE(ti0_on_change_);
    ti0_on_change_ = false;
    ASSERT_ANY_THROW(sch.SetFor(std::to_string(TestInterface::id), "db1"));  // Session does not exist
    ASSERT_ANY_THROW(sch.SetFor("1", "db1"));                                // Session not registered
    ASSERT_ANY_THROW(sch.SetFor("0", "db2"));                                // No db2
    ASSERT_EQ(sch.SetFor("0", "memgraph"), memgraph::dbms::SetForResult::SUCCESS);
    ASSERT_TRUE(ti0_on_change_);
  }

  ASSERT_TRUE(sch.Delete(ti0));
  ASSERT_FALSE(sch.Delete(ti1));
}

TEST(DBMS_Handler, Delete) {
  auto &sch = *TestEnvironment::get();

  bool ti0_on_change_ = false;
  bool ti0_on_delete_ = false;
  TestInterface ti0(
      "memgraph",
      [&ti0_on_change_](const std::string &name) -> memgraph::dbms::SetForResult {
        ti0_on_change_ = true;
        std::cout << name << " != sc1 " << (name != "sc3") << std::endl;
        if (name != "sc3") return memgraph::dbms::SetForResult::SUCCESS;
        return memgraph::dbms::SetForResult::FAIL;
      },
      [&](const std::string &name) -> bool {
        ti0_on_delete_ = true;
        return (name != "sc3");
      });

  bool ti1_on_change_ = false;
  bool ti1_on_delete_ = false;
  TestInterface ti1(
      "sc1",
      [&ti1_on_change_, &ti1](const std::string &name) -> memgraph::dbms::SetForResult {
        ti1_on_change_ = true;
        ti1.db_ = name;
        return memgraph::dbms::SetForResult::SUCCESS;
      },
      [&](const std::string &name) -> bool {
        ti1_on_delete_ = true;
        return true;
      });

  ASSERT_TRUE(sch.Register(ti0));
  ASSERT_TRUE(sch.Register(ti1));

  {
    auto del = sch.Delete(memgraph::dbms::kDefaultDB);
    ASSERT_TRUE(del.HasError() && del.GetError() == memgraph::dbms::DeleteError::DEFAULT_DB);
  }
  {
    auto del = sch.Delete("non-existent");
    ASSERT_TRUE(del.HasError() && del.GetError() == memgraph::dbms::DeleteError::NON_EXISTENT);
  }
  {
    // ti1 is using sc1
    auto del = sch.Delete("sc1");
    ASSERT_TRUE(del.HasError() && del.GetError() == memgraph::dbms::DeleteError::USING);
  }
  {
    ASSERT_EQ(sch.SetFor(ti1.UUID(), "memgraph"), memgraph::dbms::SetForResult::SUCCESS);
    auto del = sch.Delete("sc1");
    ASSERT_FALSE(del.HasError()) << (int)del.GetError();
  }
  {
    auto del = sch.Delete("sc3");
    ASSERT_TRUE(del.HasError()) << (int)del.GetError();
    ASSERT_EQ(del.GetError(), memgraph::dbms::DeleteError::FAIL);
  }
  {
    // delete ti0 so it doesn't fail
    ASSERT_TRUE(sch.Delete(ti0));
    auto del = sch.Delete("sc3");
    ASSERT_FALSE(del.HasError());
    ASSERT_FALSE(std::filesystem::exists(storage_directory / "databases" / "sc3"));
  }

  ASSERT_TRUE(sch.Delete(ti1));
}

TEST(DBMS_Handler, DeleteAndRecover) {}

int main(int argc, char *argv[]) {
  ::testing::InitGoogleTest(&argc, argv);
  // gtest takes ownership of the TestEnvironment ptr - we don't delete it.
  ::testing::AddGlobalTestEnvironment(new TestEnvironment);
  return RUN_ALL_TESTS();
}

#endif
