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
#include "query/interpreter.hpp"
#ifdef MG_ENTERPRISE

#include <gmock/gmock.h>
#include <gtest/gtest.h>
#include <filesystem>

#include "dbms/constants.hpp"
#include "dbms/global.hpp"
#include "dbms/session_context_handler.hpp"
#include "glue/auth_checker.hpp"
#include "glue/auth_handler.hpp"
#include "query/config.hpp"

std::filesystem::path storage_directory{std::filesystem::temp_directory_path() / "MG_test_unit_dbms_sc_handler"};

static memgraph::storage::Config storage_conf;

memgraph::query::InterpreterConfig interp_conf;

// Global
memgraph::audit::Log audit_log{storage_directory / "audit", 100, 1000};

class TestInterface : public memgraph::dbms::SessionInterface {
 public:
  TestInterface(std::string name, auto on_change, auto on_delete) : id_(id++), db_(name) {
    on_change_ = on_change;
    on_delete_ = on_delete;
  }
  std::string UUID() const override { return std::to_string(id_); }
  std::string GetID() const override { return db_; }
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
    ptr_ = std::make_unique<memgraph::dbms::SessionContextHandler>(
        audit_log,
        memgraph::dbms::SessionContextHandler::Config{
            storage_conf, interp_conf,
            [](memgraph::utils::Synchronized<memgraph::auth::Auth, memgraph::utils::WritePrioritizedRWLock> *auth,
               std::unique_ptr<memgraph::query::AuthQueryHandler> &ah,
               std::unique_ptr<memgraph::query::AuthChecker> &ac) {
              // Glue high level auth implementations to the query side
              ah = std::make_unique<memgraph::glue::AuthQueryHandler>(auth, "");
              ac = std::make_unique<memgraph::glue::AuthChecker>(auth);
            }},
        false, true);
  }

  void TearDown() override { ptr_.reset(); }

  static std::unique_ptr<memgraph::dbms::SessionContextHandler> ptr_;
};

std::unique_ptr<memgraph::dbms::SessionContextHandler> TestEnvironment::ptr_ = nullptr;

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

TEST(DBMS_HandlerDeath, InitSameDir) {
  // This will be executed in a clean process (so the singleton will NOT be initalized)
  (void)(::testing::GTEST_FLAG(death_test_style) = "threadsafe");
  // NOTE: Init test has ran in another process (so holds the lock)
  ASSERT_DEATH(
      {
        memgraph::dbms::SessionContextHandler sch(
            audit_log,
            {storage_conf, interp_conf,
             [](memgraph::utils::Synchronized<memgraph::auth::Auth, memgraph::utils::WritePrioritizedRWLock> *auth,
                std::unique_ptr<memgraph::query::AuthQueryHandler> &ah,
                std::unique_ptr<memgraph::query::AuthChecker> &ac) {
               // Glue high level auth implementations to the query side
               ah = std::make_unique<memgraph::glue::AuthQueryHandler>(auth, "");
               ac = std::make_unique<memgraph::glue::AuthChecker>(auth);
             }},
            false, true);
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
    ASSERT_TRUE(sc1.GetValue().interpreter_context->db != nullptr);
    ASSERT_TRUE(sc1.GetValue().interpreter_context != nullptr);
    ASSERT_TRUE(sc1.GetValue().audit_log != nullptr);
    ASSERT_TRUE(sc1.GetValue().auth != nullptr);
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
    ASSERT_TRUE(sc3.GetValue().interpreter_context->db != nullptr);
    ASSERT_TRUE(sc3.GetValue().interpreter_context != nullptr);
    ASSERT_TRUE(sc3.GetValue().audit_log != nullptr);
    ASSERT_TRUE(sc3.GetValue().auth != nullptr);
    const auto all = sch.All();
    ASSERT_EQ(all.size(), 3);
    ASSERT_TRUE(std::find(all.begin(), all.end(), "sc3") != all.end());
  }
}

TEST(DBMS_Handler, Get) {
  auto &sch = *TestEnvironment::get();
  auto default_sc = sch.Get(memgraph::dbms::kDefaultDB);
  ASSERT_TRUE(default_sc.interpreter_context->db != nullptr);
  ASSERT_TRUE(default_sc.interpreter_context != nullptr);
  ASSERT_TRUE(default_sc.audit_log != nullptr);
  ASSERT_TRUE(default_sc.auth != nullptr);

  ASSERT_ANY_THROW(sch.Get("non-existent"));

  auto sc1 = sch.Get("sc1");
  ASSERT_TRUE(sc1.interpreter_context->db != nullptr);
  ASSERT_TRUE(sc1.interpreter_context != nullptr);
  ASSERT_TRUE(sc1.audit_log != nullptr);
  ASSERT_TRUE(sc1.auth != nullptr);

  auto sc3 = sch.Get("sc3");
  ASSERT_TRUE(sc3.interpreter_context->db != nullptr);
  ASSERT_TRUE(sc3.interpreter_context != nullptr);
  ASSERT_TRUE(sc3.audit_log != nullptr);
  ASSERT_TRUE(sc3.auth != nullptr);
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
      [&](const std::string &name) -> memgraph::dbms::SetForResult {
        ti0_on_change_ = true;
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
      [&](const std::string &name) -> memgraph::dbms::SetForResult {
        ti1_on_change_ = true;
        ti1.db_ = name;
        return memgraph::dbms::SetForResult::SUCCESS;
      },
      [&](const std::string &name) -> bool {
        ti1_on_delete_ = true;
        return ti1.db_ != name;
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
    ASSERT_TRUE(del.HasError());
    ASSERT_TRUE(del.GetError() == memgraph::dbms::DeleteError::FAIL);
  }
  {
    // Delete ti1 so delete will succeed
    ASSERT_EQ(sch.SetFor(ti1.UUID(), "memgraph"), memgraph::dbms::SetForResult::SUCCESS);
    auto del = sch.Delete("sc1");
    ASSERT_FALSE(del.HasError()) << (int)del.GetError();
    auto del2 = sch.Delete("sc1");
    ASSERT_TRUE(del2.HasError() && del2.GetError() == memgraph::dbms::DeleteError::NON_EXISTENT);
  }
  {
    // Using based on the active interpreters
    auto new_sc = sch.New("sc1");
    ASSERT_TRUE(new_sc.HasValue()) << (int)new_sc.GetError();
    auto sc = sch.Get("sc1");
    memgraph::query::Interpreter interpreter(sc.interpreter_context.get());
    sc.interpreter_context->interpreters.WithLock([&](auto &interpreters) { interpreters.insert(&interpreter); });
    auto del = sch.Delete("sc1");
    ASSERT_TRUE(del.HasError());
    ASSERT_EQ(del.GetError(), memgraph::dbms::DeleteError::USING);
    sc.interpreter_context->interpreters.WithLock([&](auto &interpreters) { interpreters.erase(&interpreter); });
  }
  {
    // Interpreter deactivated, so we should be able to delete
    auto del = sch.Delete("sc1");
    ASSERT_FALSE(del.HasError()) << (int)del.GetError();
  }
  {
    ASSERT_TRUE(sch.Delete(ti0));
    auto del = sch.Delete("sc3");
    ASSERT_FALSE(del.HasError());
    ASSERT_FALSE(std::filesystem::exists(storage_directory / "databases" / "sc3"));
  }

  ASSERT_TRUE(sch.Delete(ti1));
}

int main(int argc, char *argv[]) {
  ::testing::InitGoogleTest(&argc, argv);
  // gtest takes ownership of the TestEnvironment ptr - we don't delete it.
  ::testing::AddGlobalTestEnvironment(new TestEnvironment);
  return RUN_ALL_TESTS();
}

#endif
