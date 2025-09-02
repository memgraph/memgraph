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

#include <algorithm>
#include <cstdlib>
#include <filesystem>
#include <thread>

#include "auth/auth.hpp"
#include "communication/bolt/v1/value.hpp"
#include "communication/result_stream_faker.hpp"
#include "csv/parsing.hpp"
#include "dbms/dbms_handler.hpp"
#include "disk_test_utils.hpp"
#include "flags/run_time_configurable.hpp"
#include "glue/communication.hpp"
#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "interpreter_faker.hpp"
#include "license/license.hpp"
#include "query/auth_checker.hpp"
#include "query/config.hpp"
#include "query/exceptions.hpp"
#include "query/interpreter.hpp"
#include "query/interpreter_context.hpp"
#include "query/metadata.hpp"
#include "query/stream.hpp"
#include "query/typed_value.hpp"
#include "query_common.hpp"
#include "replication/state.hpp"
#include "storage/v2/inmemory/storage.hpp"
#include "storage/v2/isolation_level.hpp"
#include "storage/v2/property_value.hpp"
#include "storage/v2/storage_mode.hpp"
#include "utils/logging.hpp"
#include "utils/lru_cache.hpp"
#include "utils/synchronized.hpp"

namespace {
std::set<std::string> GetDirs(auto path) {
  std::set<std::string> dirs;
  // Clean the unused directories
  for (const auto &entry : std::filesystem::directory_iterator(path)) {
    const auto &name = entry.path().filename().string();
    if (entry.is_directory() && !name.empty() && name.front() != '.') {
      dirs.emplace(name);
    }
  }
  return dirs;
}

auto RunMtQuery(auto &interpreter, const std::string &query, std::string_view res) {
  auto [stream, qid] = interpreter.Prepare(query);
  ASSERT_EQ(stream.GetHeader().size(), 1U);
  EXPECT_EQ(stream.GetHeader()[0], "STATUS");
  interpreter.Pull(&stream, 1);
  ASSERT_EQ(stream.GetSummary().count("has_more"), 1);
  ASSERT_FALSE(stream.GetSummary().at("has_more").ValueBool());
  ASSERT_EQ(stream.GetResults()[0].size(), 1U);
  ASSERT_EQ(stream.GetResults()[0][0].ValueString(), res);
}

auto RunQuery(auto &interpreter, const std::string &query) {
  auto [stream, qid] = interpreter.Prepare(query);
  interpreter.Pull(&stream, 1);
  return stream.GetResults();
}

void UseDatabase(auto &interpreter, const std::string &name, std::string_view res) {
  RunMtQuery(interpreter, "USE DATABASE " + name, res);
}

void DropDatabase(auto &interpreter, const std::string &name, std::string_view res) {
  RunMtQuery(interpreter, "DROP DATABASE " + name, res);
}

void ForceDropDatabase(auto &interpreter, const std::string &name, std::string_view res) {
  RunMtQuery(interpreter, "DROP DATABASE " + name + " FORCE", res);
}
}  // namespace

class MultiTenantTest : public ::testing::Test {
 public:
  std::filesystem::path data_directory = std::filesystem::temp_directory_path() / "MG_tests_unit_multi_tenancy";

  MultiTenantTest() = default;

  memgraph::storage::Config config{
      [&]() {
        memgraph::storage::Config config{};
        UpdatePaths(config, data_directory);
        return config;
      }()  // iile
  };

  struct MinMemgraph {
    explicit MinMemgraph(const memgraph::storage::Config &conf)
        : auth{conf.durability.storage_directory / "auth", memgraph::auth::Auth::Config{/* default */}},
          repl_state{ReplicationStateRootPath(conf)},
          dbms{conf, repl_state, auth, true},
          interpreter_context{{},
                              &dbms,
                              repl_state,
                              system
#ifdef MG_ENTERPRISE
                              ,
                              std::nullopt,
                              nullptr
#endif
          } {
      memgraph::utils::global_settings.Initialize(conf.durability.storage_directory / "settings");
      memgraph::license::RegisterLicenseSettings(memgraph::license::global_license_checker,
                                                 memgraph::utils::global_settings);
      memgraph::flags::run_time::Initialize();
      memgraph::license::global_license_checker.CheckEnvLicense();
    }

    ~MinMemgraph() { memgraph::utils::global_settings.Finalize(); }

    auto NewInterpreter() { return InterpreterFaker{&interpreter_context, dbms.Get()}; }

    memgraph::auth::SynchedAuth auth;
    memgraph::system::System system;
    memgraph::utils::Synchronized<memgraph::replication::ReplicationState, memgraph::utils::RWSpinLock> repl_state;
    memgraph::dbms::DbmsHandler dbms;
    memgraph::query::InterpreterContext interpreter_context;
  };

  void SetUp() override {
    TearDown();
    min_mg.emplace(config);
  }

  void TearDown() override {
    min_mg.reset();
    if (std::filesystem::exists(data_directory)) std::filesystem::remove_all(data_directory);
  }

  auto NewInterpreter() { return min_mg->NewInterpreter(); }

  auto &DBMS() { return min_mg->dbms; }

  std::optional<MinMemgraph> min_mg;
};

TEST_F(MultiTenantTest, SimpleCreateDrop) {
  // 1) Create multiple interpreters with the default db
  // 2) Create multiple databases using both
  // 3) Drop databases while the other is using

  // 1
  auto interpreter1 = this->NewInterpreter();
  auto interpreter2 = this->NewInterpreter();

  // 2
  auto create = [&](auto &interpreter, const std::string &name, bool success) {
    RunMtQuery(interpreter, "CREATE DATABASE " + name,
               success ? ("Successfully created database " + name) : (name + " already exists."));
  };

  create(interpreter1, "db1", true);
  create(interpreter1, "db1", false);
  create(interpreter2, "db1", false);
  create(interpreter2, "db2", true);
  create(interpreter1, "db2", false);
  create(interpreter2, "db3", true);
  create(interpreter2, "db4", true);

  // 3
  UseDatabase(interpreter1, "db2", "Using db2");
  UseDatabase(interpreter1, "db2", "Already using db2");
  UseDatabase(interpreter2, "db2", "Using db2");
  UseDatabase(interpreter1, "db4", "Using db4");

  ASSERT_THROW(DropDatabase(interpreter1, memgraph::dbms::kDefaultDB.data(), ""),
               memgraph::query::QueryRuntimeException);  // default db

  DropDatabase(interpreter1, "db1", "Successfully deleted db1");
  ASSERT_THROW(DropDatabase(interpreter2, "db1", ""), memgraph::query::QueryRuntimeException);  // No db1
  ASSERT_THROW(DropDatabase(interpreter1, "db1", ""), memgraph::query::QueryRuntimeException);  // No db1

  ASSERT_THROW(DropDatabase(interpreter1, "db2", ""), memgraph::query::QueryRuntimeException);  // i2 using db2
  ASSERT_THROW(DropDatabase(interpreter1, "db4", ""), memgraph::query::QueryRuntimeException);  // i1 using db4
}

TEST_F(MultiTenantTest, DbmsNewTryDelete) {
  // 1) Create multiple interpreters with the default db
  // 2) Create multiple databases using dbms
  // 3) Try delete databases while the interpreters are using them

  // 1
  auto interpreter1 = this->NewInterpreter();
  auto interpreter2 = this->NewInterpreter();

  // 2
  auto &dbms = DBMS();
  ASSERT_FALSE(dbms.New("db1").HasError());
  ASSERT_FALSE(dbms.New("db2").HasError());
  ASSERT_FALSE(dbms.New("db3").HasError());
  ASSERT_FALSE(dbms.New("db4").HasError());

  // 3
  UseDatabase(interpreter2, "db2", "Using db2");
  UseDatabase(interpreter1, "db4", "Using db4");

  ASSERT_FALSE(dbms.TryDelete("db1").HasError());
  ASSERT_TRUE(dbms.TryDelete("db2").HasError());
  ASSERT_FALSE(dbms.TryDelete("db3").HasError());
  ASSERT_TRUE(dbms.TryDelete("db4").HasError());
}

TEST_F(MultiTenantTest, DbmsUpdate) {
  // 1) Create multiple interpreters with the default db
  // 2) Create multiple databases using dbms
  // 3) Try to update databases

  auto &dbms = DBMS();
  auto interpreter1 = this->NewInterpreter();

  // Update clean default db
  auto default_db = dbms.Get();
  const auto old_uuid = default_db->config().salient.uuid;
  const memgraph::utils::UUID new_uuid{/* random */};
  const memgraph::storage::SalientConfig &config{.name = "memgraph", .uuid = new_uuid};
  auto new_default = dbms.Update(config);
  ASSERT_TRUE(new_default.HasValue());
  ASSERT_NE(new_uuid, old_uuid);
  ASSERT_EQ(default_db->storage(), new_default.GetValue()->storage());

  // Add node to default
  RunQuery(interpreter1, "CREATE (:Node)");

  // Fail to update dirty default db
  const memgraph::storage::SalientConfig &failing_config{.name = "memgraph", .uuid = {}};
  auto failed_update = dbms.Update(failing_config);
  ASSERT_TRUE(failed_update.HasError());

  // Succeed when updating with the same config
  auto same_update = dbms.Update(config);
  ASSERT_TRUE(same_update.HasValue());
  ASSERT_EQ(new_default.GetValue()->storage(), same_update.GetValue()->storage());

  // Create new db
  auto db1 = dbms.New("db1");
  ASSERT_FALSE(db1.HasError());
  RunMtQuery(interpreter1, "USE DATABASE db1", "Using db1");
  RunQuery(interpreter1, "CREATE (:NewNode)");
  RunQuery(interpreter1, "CREATE (:NewNode)");
  const auto db1_config_old = db1.GetValue()->config();

  // Begin a transaction on db1
  auto interpreter2 = this->NewInterpreter();
  RunMtQuery(interpreter2, "USE DATABASE db1", "Using db1");
  ASSERT_EQ(RunQuery(interpreter2, "SHOW DATABASE")[0][0].ValueString(), "db1");
  RunQuery(interpreter2, "BEGIN");

  // Update and check the new db in clean
  auto interpreter3 = this->NewInterpreter();
  const memgraph::storage::SalientConfig &db1_config_new{.name = "db1", .uuid = {}};
  auto new_db1 = dbms.Update(db1_config_new);
  ASSERT_TRUE(new_db1.HasValue());
  ASSERT_NE(db1_config_new.uuid, db1_config_old.salient.uuid);
  RunMtQuery(interpreter3, "USE DATABASE db1", "Using db1");
  ASSERT_EQ(RunQuery(interpreter3, "MATCH(n) RETURN count(*)")[0][0].ValueInt(), 0);

  // Check that the interpreter1 is still valid, but lacking a db
  ASSERT_THROW(RunQuery(interpreter1, "CREATE (:Node)"), memgraph::query::DatabaseContextRequiredException);

  // Check that the interpreter2 is still valid and pointing to the old db1 (until commit)
  RunQuery(interpreter2, "CREATE (:NewNode)");
  ASSERT_EQ(RunQuery(interpreter2, "MATCH(n) RETURN count(*)")[0][0].ValueInt(), 3);
  RunQuery(interpreter2, "COMMIT");
  ASSERT_THROW(RunQuery(interpreter2, "MATCH(n) RETURN n"), memgraph::query::DatabaseContextRequiredException);
}

TEST_F(MultiTenantTest, DbmsNewDelete) {
  // 1) Create multiple interpreters with the default db
  // 2) Create multiple databases using dbms
  // 3) Defer delete databases while the interpreters are using them
  // 4) Database should be a zombie until the using interpreter retries to query it
  // 5) Check it is deleted from disk

  // 1
  auto interpreter1 = this->NewInterpreter();
  auto interpreter2 = this->NewInterpreter();

  // 2
  auto &dbms = DBMS();
  ASSERT_FALSE(dbms.New("db1").HasError());
  ASSERT_FALSE(dbms.New("db2").HasError());
  ASSERT_FALSE(dbms.New("db3").HasError());
  ASSERT_FALSE(dbms.New("db4").HasError());

  // 3
  UseDatabase(interpreter2, "db2", "Using db2");
  UseDatabase(interpreter1, "db4", "Using db4");

  RunQuery(interpreter1, "CREATE (:Node{on:\"db4\"})");
  RunQuery(interpreter1, "CREATE (:Node{on:\"db4\"})");
  RunQuery(interpreter1, "CREATE (:Node{on:\"db4\"})");
  RunQuery(interpreter1, "CREATE (:Node{on:\"db4\"})");
  RunQuery(interpreter2, "CREATE (:Node{on:\"db2\"})");
  RunQuery(interpreter2, "CREATE (:Node{on:\"db2\"})");

  ASSERT_FALSE(dbms.Delete("db1").HasError());
  ASSERT_FALSE(dbms.Delete("db2").HasError());
  ASSERT_FALSE(dbms.Delete("db3").HasError());
  ASSERT_FALSE(dbms.Delete("db4").HasError());

  // 4
  ASSERT_EQ(dbms.All().size(), 1);
  ASSERT_EQ(GetDirs(data_directory / "databases").size(), 3);  // All used databases remain on disk, but unusable
  ASSERT_THROW(RunQuery(interpreter1, "MATCH(:Node{on:db4}) RETURN count(*)"),
               memgraph::query::DatabaseContextRequiredException);
  ASSERT_THROW(RunQuery(interpreter2, "MATCH(:Node{on:db2}) RETURN count(*)"),
               memgraph::query::DatabaseContextRequiredException);

  // 5
  int tries = 0;
  constexpr int max_tries = 50;
  for (; tries < max_tries; tries++) {
    if (GetDirs(data_directory / "databases").size() == 1) break;
    std::this_thread::sleep_for(std::chrono::milliseconds(100));  // Wait for the filesystem to be updated
  }
  ASSERT_LT(tries, max_tries) << "Failed to delete databases. Remaining databases "
                              << GetDirs(data_directory / "databases").size();
  ASSERT_THROW(RunQuery(interpreter1, "MATCH(n) RETURN n"), memgraph::query::DatabaseContextRequiredException);
  ASSERT_THROW(RunQuery(interpreter2, "MATCH(n) RETURN n"), memgraph::query::DatabaseContextRequiredException);
}

TEST_F(MultiTenantTest, DbmsNewDeleteWTx) {
  // 1) Create multiple interpreters with the default db
  // 2) Create multiple databases using dbms
  // 3) Defer delete databases while the interpreters are using them
  // 4) Interpreters that had an open transaction before should still be working
  // 5) New transactions on deleted databases should throw
  // 6) Switching databases should still be possible

  // 1
  auto interpreter1 = this->NewInterpreter();
  auto interpreter2 = this->NewInterpreter();

  // 2
  auto &dbms = DBMS();
  ASSERT_FALSE(dbms.New("db1").HasError());
  ASSERT_FALSE(dbms.New("db2").HasError());
  ASSERT_FALSE(dbms.New("db3").HasError());
  ASSERT_FALSE(dbms.New("db4").HasError());

  // 3
  UseDatabase(interpreter2, "db2", "Using db2");
  UseDatabase(interpreter1, "db4", "Using db4");

  RunQuery(interpreter1, "CREATE (:Node{on:\"db4\"})");
  RunQuery(interpreter1, "CREATE (:Node{on:\"db4\"})");
  RunQuery(interpreter1, "CREATE (:Node{on:\"db4\"})");
  RunQuery(interpreter1, "CREATE (:Node{on:\"db4\"})");
  RunQuery(interpreter2, "CREATE (:Node{on:\"db2\"})");
  RunQuery(interpreter2, "CREATE (:Node{on:\"db2\"})");

  RunQuery(interpreter1, "BEGIN");
  RunQuery(interpreter2, "BEGIN");

  ASSERT_FALSE(dbms.Delete("db1").HasError());
  ASSERT_FALSE(dbms.Delete("db2").HasError());
  ASSERT_FALSE(dbms.Delete("db3").HasError());
  ASSERT_FALSE(dbms.Delete("db4").HasError());

  // 4
  ASSERT_EQ(dbms.All().size(), 1);
  ASSERT_EQ(GetDirs(data_directory / "databases").size(), 3);  // All used databases remain on disk, and usable
  ASSERT_EQ(RunQuery(interpreter1, "MATCH(:Node{on:\"db4\"}) RETURN count(*)")[0][0].ValueInt(), 4);
  ASSERT_EQ(RunQuery(interpreter2, "MATCH(:Node{on:\"db2\"}) RETURN count(*)")[0][0].ValueInt(), 2);
  RunQuery(interpreter1, "MATCH(n:Node{on:\"db4\"}) DELETE n");
  RunQuery(interpreter2, "CREATE(:Node{on:\"db2\"})");
  ASSERT_EQ(RunQuery(interpreter1, "MATCH(:Node{on:\"db4\"}) RETURN count(*)")[0][0].ValueInt(), 0);
  ASSERT_EQ(RunQuery(interpreter2, "MATCH(:Node{on:\"db2\"}) RETURN count(*)")[0][0].ValueInt(), 3);
  RunQuery(interpreter1, "COMMIT");
  RunQuery(interpreter2, "COMMIT");

  // 5
  int tries = 0;
  constexpr int max_tries = 50;
  for (; tries < max_tries; tries++) {
    if (GetDirs(data_directory / "databases").size() == 1) break;
    std::this_thread::sleep_for(std::chrono::milliseconds(100));  // Wait for the filesystem to be updated
  }
  ASSERT_LT(tries, max_tries) << "Failed to delete databases. Remaining databases "
                              << GetDirs(data_directory / "databases").size();
  ASSERT_EQ(GetDirs(data_directory / "databases").size(), 1);  // Only the active databases remain
  ASSERT_THROW(RunQuery(interpreter1, "MATCH(n) RETURN n"), memgraph::query::DatabaseContextRequiredException);
  ASSERT_THROW(RunQuery(interpreter2, "MATCH(n) RETURN n"), memgraph::query::DatabaseContextRequiredException);

  // 6
  UseDatabase(interpreter2, memgraph::dbms::kDefaultDB.data(), "Using memgraph");
  UseDatabase(interpreter1, memgraph::dbms::kDefaultDB.data(), "Using memgraph");
}

TEST_F(MultiTenantTest, ForceDropDatabase) {
  // 1) Create multiple interpreters with the default db
  // 2) Create multiple databases using both
  // 3) Test force drop database while databases are in use
  // 4) Verify that force drop bypasses normal usage checks
  // 5) Check that active transactions are properly handled

  // 1
  auto interpreter1 = this->NewInterpreter();
  auto interpreter2 = this->NewInterpreter();

  // 2
  auto create = [&](auto &interpreter, const std::string &name, bool success) {
    RunMtQuery(interpreter, "CREATE DATABASE " + name,
               success ? ("Successfully created database " + name) : (name + " already exists."));
  };

  create(interpreter1, "db1", true);
  create(interpreter2, "db2", true);

  // 3 - Use databases and create data
  UseDatabase(interpreter1, "db1", "Using db1");
  UseDatabase(interpreter2, "db1", "Using db1");

  // Create some data in the databases
  RunQuery(interpreter1, "CREATE (:Node{on:\"db1\"})");
  RunQuery(interpreter1, "CREATE (:Node{on:\"db1\"})");
  RunQuery(interpreter2, "CREATE (:Node{on:\"db1\"})");
  RunQuery(interpreter2, "CREATE (:Node{on:\"db1\"})");
  RunQuery(interpreter2, "CREATE (:Node{on:\"db1\"})");

  // 4 - Test force drop while databases are in use
  // Normal drop should fail when database is in use
  ASSERT_THROW(DropDatabase(interpreter1, "db1", ""), memgraph::query::QueryRuntimeException);
  ASSERT_THROW(DropDatabase(interpreter2, "db1", ""), memgraph::query::QueryRuntimeException);

  // Force drop should succeed even when database is in use
  ForceDropDatabase(interpreter1, "db1", "Successfully deleted db1");
  ForceDropDatabase(interpreter2, "db2", "Successfully deleted db2");

  // Verify databases are marked for deletion
  ASSERT_EQ(DBMS().All().size(), 1) << "Expected memgraph only; Got: " << memgraph::utils::Join(DBMS().All(), ", ");
  ASSERT_EQ(DBMS().All()[0], memgraph::dbms::kDefaultDB.data());

  // 5 - Check that active transactions are properly handled
  // Force drop should have terminated the transactions, so new queries should fail
  ASSERT_THROW(RunQuery(interpreter1, "CREATE (:Node{on:\"db1\"})"), memgraph::query::DatabaseContextRequiredException);
  ASSERT_THROW(RunQuery(interpreter2, "CREATE (:Node{on:\"db1\"})"), memgraph::query::DatabaseContextRequiredException);

  // Verify that the databases are no longer accessible
  ASSERT_THROW(UseDatabase(interpreter1, "db1", ""), memgraph::query::QueryRuntimeException);
  ASSERT_THROW(UseDatabase(interpreter2, "db2", ""), memgraph::query::QueryRuntimeException);

  // But switching to default database should still work
  UseDatabase(interpreter1, memgraph::dbms::kDefaultDB.data(), "Using memgraph");
  UseDatabase(interpreter2, memgraph::dbms::kDefaultDB.data(), "Using memgraph");

  // Verify we can still create databases and use them
  create(interpreter1, "newdb", true);
  UseDatabase(interpreter1, "newdb", "Using newdb");
  RunQuery(interpreter1, "CREATE (:Node{on:\"newdb\"})");
  ASSERT_EQ(RunQuery(interpreter1, "MATCH(:Node{on:\"newdb\"}) RETURN count(*)")[0][0].ValueInt(), 1);
}

TEST_F(MultiTenantTest, ForceDropDatabaseWithActiveTransactions) {
  // 1) Create multiple interpreters with the default db
  // 2) Create databases and start transactions
  // 3) Test force drop while transactions are active
  // 4) Verify transaction termination behavior
  // 5) Check cleanup and recovery

  // 1
  auto interpreter1 = this->NewInterpreter();
  auto interpreter2 = this->NewInterpreter();

  // 2
  auto &dbms = DBMS();
  ASSERT_FALSE(dbms.New("db1").HasError());
  ASSERT_FALSE(dbms.New("db2").HasError());

  UseDatabase(interpreter1, "db1", "Using db1");
  UseDatabase(interpreter2, "db2", "Using db2");

  // Create data and start transactions
  RunQuery(interpreter1, "CREATE (:Node{on:\"db1\"})");
  RunQuery(interpreter1, "CREATE (:Node{on:\"db1\"})");
  RunQuery(interpreter2, "CREATE (:Node{on:\"db2\"})");
  RunQuery(interpreter2, "CREATE (:Node{on:\"db2\"})");

  // Start transactions
  RunQuery(interpreter1, "BEGIN");

  // Verify transactions are active
  ASSERT_EQ(RunQuery(interpreter1, "MATCH(:Node{on:\"db1\"}) RETURN count(*)")[0][0].ValueInt(), 2);

  // 3 - Force drop while transactions are active
  ForceDropDatabase(interpreter2, "db1", "Successfully deleted db1");

  // 4 - Verify transaction termination behavior
  // Active transactions should still work even after force drop
  auto res = RunQuery(interpreter1, "MATCH(:Node{on:\"db1\"}) RETURN count(*)");
  ASSERT_EQ(res.size(), 1);
  ASSERT_EQ(res[0].size(), 1);
  ASSERT_EQ(res[0][0].ValueInt(), 2);

  // But attempting to commit should fail
  try {
    RunQuery(interpreter1, "COMMIT");
  } catch (const memgraph::utils::BasicException &e) {
    ASSERT_TRUE(std::string_view(e.what()).starts_with("Aborting"));
    interpreter1.Abort();  // Client will do this automatically
  }

  // 5 - Check cleanup and recovery
  // Verify databases are no longer accessible
  ASSERT_THROW(UseDatabase(interpreter1, "db1", ""), memgraph::query::QueryRuntimeException);

  // Switch back to default database
  UseDatabase(interpreter1, memgraph::dbms::kDefaultDB.data(), "Using memgraph");
  UseDatabase(interpreter2, memgraph::dbms::kDefaultDB.data(), "Using memgraph");

  // Verify we can still work with the default database
  RunQuery(interpreter1, "CREATE (:Node{on:\"default\"})");
  ASSERT_EQ(RunQuery(interpreter1, "MATCH(:Node{on:\"default\"}) RETURN count(*)")[0][0].ValueInt(), 1);

  // Verify database count is correct
  ASSERT_EQ(dbms.All().size(), 2);
}
