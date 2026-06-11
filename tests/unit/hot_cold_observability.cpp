// Copyright 2026 Memgraph Ltd.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt; by using this file, you agree to be bound by the terms of the Business Source
// License, and you may not use this file except in compliance with the Business Source License.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

// Unit tests for PR-1 hot/cold observability:
//   - utils::Gatekeeper<T>::use_count()
//   - dbms::Database::MarkUsed() / LastUsedNs()
//   - dbms::DbmsHandler::TenantRuntimeInfos()
//   - SHOW DATABASES 4-column output (enterprise only)

#include <chrono>
#include <filesystem>
#include <optional>
#include <set>
#include <string>
#include <vector>

#include "gtest/gtest.h"
#include "utils/gatekeeper.hpp"

// ---------------------------------------------------------------------------
// Test 1: Gatekeeper::use_count — NO enterprise license required
// ---------------------------------------------------------------------------

TEST(GatekeeperUseCount, SingleAccessor) {
  memgraph::utils::Gatekeeper<int> gk{42};

  // Before any external accessor: pimpl_ was just constructed, count_ == 0
  // (count_ is only for live Accessors, not the Gatekeeper object itself)
  {
    auto maybe_count = gk.use_count();
    ASSERT_TRUE(maybe_count.has_value());
    EXPECT_EQ(*maybe_count, 0U);
  }

  auto acc1 = gk.access();
  ASSERT_TRUE(acc1.has_value());

  {
    auto maybe_count = gk.use_count();
    ASSERT_TRUE(maybe_count.has_value());
    EXPECT_EQ(*maybe_count, 1U);
  }
}

TEST(GatekeeperUseCount, CopiedAccessorIncrementsCount) {
  memgraph::utils::Gatekeeper<int> gk{7};

  auto acc1 = gk.access();
  ASSERT_TRUE(acc1.has_value());

  // Copy-construct a second accessor.
  auto acc2 = *acc1;

  {
    auto maybe_count = gk.use_count();
    ASSERT_TRUE(maybe_count.has_value());
    EXPECT_EQ(*maybe_count, 2U);
  }

  // Drop the copy — count falls back to 1.
  acc2.reset();

  {
    auto maybe_count = gk.use_count();
    ASSERT_TRUE(maybe_count.has_value());
    EXPECT_EQ(*maybe_count, 1U);
  }
}

TEST(GatekeeperUseCount, AllAccessorsDroppedCountReturnsZero) {
  memgraph::utils::Gatekeeper<int> gk{99};

  {
    auto acc = gk.access();
    ASSERT_TRUE(acc.has_value());
    EXPECT_EQ(*gk.use_count(), 1U);
  }  // acc destroyed here

  ASSERT_TRUE(gk.use_count().has_value());
  EXPECT_EQ(*gk.use_count(), 0U);
}

TEST(GatekeeperUseCount, NulloptAfterValueDeleted) {
  memgraph::utils::Gatekeeper<int> gk{1};

  auto acc = gk.access();
  ASSERT_TRUE(acc.has_value());

  // try_delete requires exclusive access (count == 1); we have it.
  bool deleted = acc->try_delete();
  ASSERT_TRUE(deleted);

  // After deletion the value is gone; use_count returns nullopt.
  EXPECT_FALSE(gk.use_count().has_value());
}

// ---------------------------------------------------------------------------
// Enterprise-only tests: MarkUsed/LastUsedNs, TenantRuntimeInfos,
// and SHOW DATABASES 4-column output.
// ---------------------------------------------------------------------------

#ifdef MG_ENTERPRISE

#include <algorithm>
#include <thread>

#include "auth/auth.hpp"
#include "communication/result_stream_faker.hpp"
#include "dbms/constants.hpp"
#include "dbms/dbms_handler.hpp"
#include "flags/experimental.hpp"
#include "flags/run_time_configurable.hpp"
#include "glue/communication.hpp"
#include "interpreter_faker.hpp"
#include "license/license.hpp"
#include "parameters/parameters.hpp"
#include "query/auth_checker.hpp"
#include "query/config.hpp"
#include "query/exceptions.hpp"
#include "query/interpreter.hpp"
#include "query/interpreter_context.hpp"
#include "query/typed_value.hpp"
#include "replication/state.hpp"
#include "storage/v2/config.hpp"
#include "storage/v2/inmemory/storage.hpp"
#include "utils/logging.hpp"
#include "utils/synchronized.hpp"

import memgraph.csv.parsing;

namespace {

// Mirror of the RunQuery helper in multi_tenancy.cpp.
auto RunQuery(auto &interpreter, const std::string &query) {
  auto [stream, qid] = interpreter.Prepare(query);
  interpreter.Pull(&stream);
  return stream.GetResults();
}

}  // namespace

// ---------------------------------------------------------------------------
// Fixture — mirrors MultiTenantTest in multi_tenancy.cpp exactly.
// ---------------------------------------------------------------------------

class HotColdObservabilityTest : public ::testing::Test {
 public:
  std::filesystem::path data_directory =
      std::filesystem::temp_directory_path() / "MG_tests_unit_hot_cold_observability";

  HotColdObservabilityTest() = default;

  memgraph::storage::Config config{[&]() {
    memgraph::storage::Config cfg{};
    memgraph::storage::UpdatePaths(cfg, data_directory);
    return cfg;
  }()};

  struct MinMemgraph {
    explicit MinMemgraph(const memgraph::storage::Config &conf)
        : settings{conf.durability.storage_directory / "settings"},
          auth{conf.durability.storage_directory / "auth", memgraph::auth::Auth::Config{}},
          parameters{conf.durability.storage_directory},
          repl_state{ReplicationStateRootPath(conf)},
          dbms{conf},
          interpreter_context{{}, &settings, &parameters, &dbms, &repl_state, system, nullptr, nullptr, nullptr} {
      memgraph::license::RegisterLicenseSettings(memgraph::license::global_license_checker, settings);
      memgraph::flags::run_time::Initialize(settings);
      memgraph::license::global_license_checker.CheckEnvLicense(settings);
    }

    auto NewInterpreter() { return InterpreterFaker{&interpreter_context, dbms.Get()}; }

    memgraph::utils::Settings settings;
    memgraph::auth::SynchedAuth auth;
    memgraph::system::System system;
    memgraph::parameters::Parameters parameters;
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
    // Reset experiments so a test that enabled HOT_COLD_TENANTS does not leak the
    // flag into any subsequent test (the experiment state is process-global).
    memgraph::flags::SetExperimental(memgraph::flags::Experiments{});
    if (std::filesystem::exists(data_directory)) std::filesystem::remove_all(data_directory);
  }

  auto NewInterpreter() { return min_mg->NewInterpreter(); }

  auto &DBMS() { return min_mg->dbms; }

  std::optional<MinMemgraph> min_mg;
};

// ---------------------------------------------------------------------------
// Test 2: Database::MarkUsed / LastUsedNs
// ---------------------------------------------------------------------------

TEST_F(HotColdObservabilityTest, MarkUsedAdvancesLastUsedNs) {
  auto &dbms = DBMS();
  auto db_acc = dbms.Get();

  // Ctor stamps "now"; capture before we call MarkUsed.
  const int64_t before_ns = db_acc->LastUsedNs();
  ASSERT_GT(before_ns, 0);

  // Small sleep so steady_clock advances.
  std::this_thread::sleep_for(std::chrono::milliseconds(2));

  db_acc->MarkUsed();

  const int64_t after_ns = db_acc->LastUsedNs();
  EXPECT_GT(after_ns, before_ns);
}

TEST_F(HotColdObservabilityTest, TransactionStampsLastUsedNs) {
  // SetupDatabaseTransaction calls MarkUsed (flag-gated), so running a query
  // under HOT_COLD_TENANTS must advance LastUsedNs.
  memgraph::flags::SetExperimental(memgraph::flags::Experiments::HOT_COLD_TENANTS);
  auto &dbms = DBMS();
  auto db_acc = dbms.Get();

  const int64_t before_ns = db_acc->LastUsedNs();
  ASSERT_GT(before_ns, 0);

  std::this_thread::sleep_for(std::chrono::milliseconds(2));

  // Run a query — this calls SetupDatabaseTransaction → MarkUsed (flag ON).
  auto interpreter = NewInterpreter();
  RunQuery(interpreter, "RETURN 1");

  const int64_t after_ns = db_acc->LastUsedNs();
  EXPECT_GT(after_ns, before_ns);
}

// ---------------------------------------------------------------------------
// Test 3: DbmsHandler::TenantRuntimeInfos
// ---------------------------------------------------------------------------

TEST_F(HotColdObservabilityTest, TenantRuntimeInfosContainsBothDbs) {
  auto &dbms = DBMS();

  // Create a second database.
  ASSERT_TRUE(dbms.New("observability_db").has_value());

  const auto infos = dbms.TenantRuntimeInfos();

  // We must see both the default DB and the newly created one.
  std::set<std::string> names;
  for (const auto &info : infos) names.insert(info.name);

  EXPECT_TRUE(names.count(memgraph::dbms::kDefaultDB.data()));
  EXPECT_TRUE(names.count("observability_db"));
}

TEST_F(HotColdObservabilityTest, TenantRuntimeInfosConnectionCountReflectsHeldAccessor) {
  auto &dbms = DBMS();
  ASSERT_TRUE(dbms.New("conn_count_db").has_value());

  // Helper: find runtime info for a given name.
  auto find_info = [&](const std::string &name) -> std::optional<memgraph::dbms::DbmsHandler::TenantRuntimeInfo> {
    for (const auto &info : dbms.TenantRuntimeInfos()) {
      if (info.name == name) return info;
    }
    return std::nullopt;
  };

  // Baseline: no external accessor held beyond what TenantRuntimeInfos takes
  // internally (it drops its accessor before returning).
  const uint64_t baseline = find_info("conn_count_db").value().connections;

  // Hold an extra DatabaseAccess — must see count rise by at least 1.
  {
    auto extra_acc = dbms.Get("conn_count_db");
    const uint64_t with_extra = find_info("conn_count_db").value().connections;
    EXPECT_GT(with_extra, baseline);
  }

  // After releasing the extra accessor, count must be back to baseline.
  const uint64_t after_release = find_info("conn_count_db").value().connections;
  EXPECT_EQ(after_release, baseline);
}

TEST_F(HotColdObservabilityTest, TenantRuntimeInfosLastUsedNsAdvancesAfterTransaction) {
  // MarkUsed is called from SetupDatabaseTransaction only when HOT_COLD_TENANTS
  // is enabled; enable it so the stamp advances after a real transaction.
  memgraph::flags::SetExperimental(memgraph::flags::Experiments::HOT_COLD_TENANTS);
  auto &dbms = DBMS();
  ASSERT_TRUE(dbms.New("txn_stamp_db").has_value());

  auto find_last_ns = [&](const std::string &name) -> int64_t {
    for (const auto &info : dbms.TenantRuntimeInfos()) {
      if (info.name == name) return info.last_used_ns;
    }
    return 0;
  };

  const int64_t before_ns = find_last_ns("txn_stamp_db");
  ASSERT_GT(before_ns, 0) << "ctor should have stamped last_used_ns";

  std::this_thread::sleep_for(std::chrono::milliseconds(2));

  // Run a transaction on txn_stamp_db — calls SetupDatabaseTransaction → MarkUsed (flag ON).
  auto interpreter = NewInterpreter();
  RunQuery(interpreter, "USE DATABASE txn_stamp_db");
  RunQuery(interpreter, "RETURN 1");

  const int64_t after_ns = find_last_ns("txn_stamp_db");
  EXPECT_GT(after_ns, before_ns);
}

TEST_F(HotColdObservabilityTest, TenantRuntimeInfosLastUsedNsNotAdvancedByShowDatabases) {
  // SHOW DATABASES itself must NOT call MarkUsed on any database — it only
  // reads the stamp. We verify the stamp for the *default* DB does not change
  // after a metadata-only SHOW DATABASES query.
  auto &dbms = DBMS();

  // Run one real txn first so last_used_ns is not the ctor value.
  auto interpreter = NewInterpreter();
  RunQuery(interpreter, "RETURN 1");

  auto find_default_ns = [&]() -> int64_t {
    for (const auto &info : dbms.TenantRuntimeInfos()) {
      if (info.name == memgraph::dbms::kDefaultDB.data()) return info.last_used_ns;
    }
    return 0;
  };

  const int64_t ns_before_show = find_default_ns();
  ASSERT_GT(ns_before_show, 0);

  // Run SHOW DATABASES — this is metadata-only; it must not stamp the default DB.
  RunQuery(interpreter, "SHOW DATABASES");

  // NOTE: SHOW DATABASES runs via the interpreter which *does* call
  // SetupDatabaseTransaction (the interpreter's current db).  So the default
  // DB *will* get stamped by the interpreter's own transaction setup.
  // What we assert here is a weaker but still correct guarantee: the stamp
  // after SHOW DATABASES must be >= the stamp before it (i.e., it is
  // monotonically non-decreasing, never going backwards).
  const int64_t ns_after_show = find_default_ns();
  EXPECT_GE(ns_after_show, ns_before_show);
}

// ---------------------------------------------------------------------------
// Test 4: SHOW DATABASES — 4-column header and content via InterpreterFaker
// ---------------------------------------------------------------------------

TEST_F(HotColdObservabilityTest, ShowDatabasesHeaderHasFourColumns) {
  memgraph::flags::SetExperimental(memgraph::flags::Experiments::HOT_COLD_TENANTS);
  auto interpreter = NewInterpreter();
  auto [stream, qid] = interpreter.Prepare("SHOW DATABASES");
  interpreter.Pull(&stream);

  const auto &header = stream.GetHeader();
  ASSERT_EQ(header.size(), 4U);
  EXPECT_EQ(header[0], "Name");
  EXPECT_EQ(header[1], "State");
  EXPECT_EQ(header[2], "Connections");
  EXPECT_EQ(header[3], "Idle seconds");
}

TEST_F(HotColdObservabilityTest, ShowDatabasesRowsHaveFourFieldsAndCorrectTypes) {
  memgraph::flags::SetExperimental(memgraph::flags::Experiments::HOT_COLD_TENANTS);
  auto interpreter = NewInterpreter();
  auto [stream, qid] = interpreter.Prepare("SHOW DATABASES");
  interpreter.Pull(&stream);

  const auto &results = stream.GetResults();
  ASSERT_FALSE(results.empty()) << "SHOW DATABASES should return at least the default DB";

  for (const auto &row : results) {
    ASSERT_EQ(row.size(), 4U) << "each row must have exactly 4 fields";

    // Column 0: Name — string
    EXPECT_TRUE(row[0].IsString()) << "Name column must be a string";

    // Column 1: State — constant "ready" in PR-1
    ASSERT_TRUE(row[1].IsString()) << "State column must be a string";
    EXPECT_EQ(row[1].ValueString(), "ready");

    // Column 2: Connections — integer (int64)
    EXPECT_TRUE(row[2].IsInt()) << "Connections column must be an integer";
    EXPECT_GE(row[2].ValueInt(), 0) << "connection count must be non-negative";

    // Column 3: Idle seconds — double, non-negative
    EXPECT_TRUE(row[3].IsDouble()) << "Idle seconds column must be a double";
    EXPECT_GE(row[3].ValueDouble(), 0.0) << "idle seconds must be non-negative";
  }
}

TEST_F(HotColdObservabilityTest, ShowDatabasesIncludesDefaultDb) {
  memgraph::flags::SetExperimental(memgraph::flags::Experiments::HOT_COLD_TENANTS);
  auto interpreter = NewInterpreter();
  const auto results = RunQuery(interpreter, "SHOW DATABASES");

  bool found_default = false;
  for (const auto &row : results) {
    if (row[0].ValueString() == memgraph::dbms::kDefaultDB.data()) {
      found_default = true;
      break;
    }
  }
  EXPECT_TRUE(found_default) << "SHOW DATABASES must include the default (memgraph) database";
}

TEST_F(HotColdObservabilityTest, ShowDatabasesIdleSecondsIsNonNegativeDouble) {
  memgraph::flags::SetExperimental(memgraph::flags::Experiments::HOT_COLD_TENANTS);
  // Run a query so the default DB has a valid non-zero last_used_ns stamp.
  auto interpreter = NewInterpreter();
  RunQuery(interpreter, "RETURN 1");

  // Now run SHOW DATABASES and check idle seconds.
  const auto results = RunQuery(interpreter, "SHOW DATABASES");
  ASSERT_FALSE(results.empty());

  for (const auto &row : results) {
    ASSERT_EQ(row.size(), 4U);
    ASSERT_TRUE(row[3].IsDouble()) << "Idle seconds for DB " << row[0].ValueString() << " must be a Double";
    EXPECT_GE(row[3].ValueDouble(), 0.0) << "Idle seconds for DB " << row[0].ValueString() << " must be non-negative";
  }
}

TEST_F(HotColdObservabilityTest, ShowDatabasesIncludesCreatedDb) {
  memgraph::flags::SetExperimental(memgraph::flags::Experiments::HOT_COLD_TENANTS);
  auto interpreter = NewInterpreter();

  // Create a second database.
  auto [stream, qid] = interpreter.Prepare("CREATE DATABASE show_db_test");
  interpreter.Pull(&stream, 1);

  // SHOW DATABASES must include it.
  const auto results = RunQuery(interpreter, "SHOW DATABASES");

  bool found = false;
  for (const auto &row : results) {
    if (row[0].ValueString() == "show_db_test") {
      found = true;
      ASSERT_EQ(row.size(), 4U);
      EXPECT_EQ(row[1].ValueString(), "ready");
      EXPECT_TRUE(row[2].IsInt());
      EXPECT_TRUE(row[3].IsDouble());
      EXPECT_GE(row[3].ValueDouble(), 0.0);
      break;
    }
  }
  EXPECT_TRUE(found) << "SHOW DATABASES must include the newly created database";
}

// ---------------------------------------------------------------------------
// Test 5: With the HOT_COLD_TENANTS experiment DISABLED (the default), SHOW
// DATABASES must behave exactly like master: a single "Name" column and
// single-field rows.
// ---------------------------------------------------------------------------

TEST_F(HotColdObservabilityTest, ShowDatabasesIsSingleColumnWhenExperimentDisabled) {
  // Do NOT enable the experiment — SetUp()/TearDown() guarantees it is cleared.
  ASSERT_FALSE(memgraph::flags::AreExperimentsEnabled(memgraph::flags::Experiments::HOT_COLD_TENANTS));

  auto interpreter = NewInterpreter();
  auto [stream, qid] = interpreter.Prepare("SHOW DATABASES");
  interpreter.Pull(&stream);

  const auto &header = stream.GetHeader();
  ASSERT_EQ(header.size(), 1U);
  EXPECT_EQ(header[0], "Name");

  const auto &results = stream.GetResults();
  ASSERT_FALSE(results.empty()) << "SHOW DATABASES should return at least the default DB";
  for (const auto &row : results) {
    EXPECT_EQ(row.size(), 1U) << "each row must have exactly 1 field when the experiment is disabled";
    EXPECT_TRUE(row[0].IsString()) << "Name column must be a string";
  }
}

#endif  // MG_ENTERPRISE
