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

// Unit tests for the hot/cold SUSPEND engine (DbmsHandler::Suspend).
// Test-only at runtime in this commit: Suspend() is exercised directly here.

#include "gtest/gtest.h"

#ifdef MG_ENTERPRISE

#include <algorithm>
#include <chrono>
#include <filesystem>
#include <optional>
#include <set>
#include <string>
#include <thread>
#include <vector>

#include "auth/auth.hpp"
#include "dbms/constants.hpp"
#include "dbms/dbms_handler.hpp"
#include "flags/general.hpp"
#include "flags/run_time_configurable.hpp"
#include "license/license.hpp"
#include "query/interpreter_context.hpp"
#include "replication/state.hpp"
#include "storage/v2/config.hpp"
#include "storage/v2/view.hpp"
#include "tests/test_commit_args_helper.hpp"
#include "utils/gatekeeper.hpp"
#include "utils/logging.hpp"
#include "utils/synchronized.hpp"

namespace {

memgraph::storage::Config MakeConfig(
    const std::filesystem::path &dir, bool recover_on_startup,
    memgraph::storage::Config::Durability::SnapshotWalMode mode =
        memgraph::storage::Config::Durability::SnapshotWalMode::PERIODIC_SNAPSHOT_WITH_WAL) {
  memgraph::storage::Config cfg{};
  memgraph::storage::UpdatePaths(cfg, dir);
  cfg.durability.snapshot_wal_mode = mode;
  cfg.durability.recover_on_startup = recover_on_startup;
  // Disable periodic background snapshots; we only need the {snapshot+WAL} durability *mode* to be
  // declared, the suspend teardown writes the durable artifacts.
  cfg.durability.snapshot_on_exit = false;
  return cfg;
}

// Minimal DbmsHandler harness (mirrors the MinMemgraph idiom from hot_cold_observability.cpp /
// multi_tenancy.cpp). Owns everything DbmsHandler depends on, in a self-contained scope so it can
// be torn down and re-created on the SAME data_directory for the restart-equivalence test.
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

  memgraph::utils::Settings settings;
  memgraph::auth::SynchedAuth auth;
  memgraph::system::System system;
  memgraph::parameters::Parameters parameters;
  memgraph::utils::Synchronized<memgraph::replication::ReplicationState, memgraph::utils::RWSpinLock> repl_state;
  memgraph::dbms::DbmsHandler dbms;
  memgraph::query::InterpreterContext interpreter_context;
};

}  // namespace

class HotColdSuspendTest : public ::testing::Test {
 public:
  std::filesystem::path data_directory = std::filesystem::temp_directory_path() / "MG_tests_unit_hot_cold_suspend";

  void SetUp() override {
    TearDown();
    // Debounce off by default; the dedicated min-residency test overrides this.
    FLAGS_storage_hot_cold_min_hot_residency_sec = 0;
    min_mg.emplace(MakeConfig(data_directory, /*recover_on_startup=*/true));
  }

  void TearDown() override {
    min_mg.reset();
    if (std::filesystem::exists(data_directory)) std::filesystem::remove_all(data_directory);
  }

  auto &DBMS() { return min_mg->dbms; }

  // Create a tenant, write N nodes through a real storage accessor (so the WAL/snapshot has content).
  void CreateAndPopulate(const std::string &name, int n) {
    ASSERT_TRUE(DBMS().New(name).has_value());
    auto db_acc = DBMS().Get(name);
    auto storage_acc = db_acc->Access(memgraph::storage::WRITE);
    for (int i = 0; i < n; ++i) {
      storage_acc->CreateVertex();
    }
    ASSERT_TRUE(storage_acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()).has_value());
  }

  static int64_t CountNodes(memgraph::dbms::DatabaseAccess &db_acc) {
    auto storage_acc = db_acc->Access(memgraph::storage::READ);
    int64_t count = 0;
    for ([[maybe_unused]] auto _ : storage_acc->Vertices(memgraph::storage::View::OLD)) ++count;
    return count;
  }

  std::optional<MinMemgraph> min_mg;
};

// ---------------------------------------------------------------------------
// Rejection cases
// ---------------------------------------------------------------------------

TEST_F(HotColdSuspendTest, SuspendDefaultDbRejected) {
  auto res = DBMS().Suspend(memgraph::dbms::kDefaultDB);
  ASSERT_FALSE(res.has_value());
  EXPECT_EQ(res.error(), memgraph::dbms::DbmsHandler::SuspendError::DEFAULT_DB);
}

TEST_F(HotColdSuspendTest, SuspendNonExistentRejected) {
  auto res = DBMS().Suspend("does_not_exist");
  ASSERT_FALSE(res.has_value());
  EXPECT_EQ(res.error(), memgraph::dbms::DbmsHandler::SuspendError::NON_EXISTENT);
}

TEST_F(HotColdSuspendTest, SuspendWithActiveAccessorRejected) {
  CreateAndPopulate("active_db", 3);

  {
    // Hold a second accessor — Suspend must fail because count cannot drain to 1.
    auto extra = DBMS().Get("active_db");
    auto res = DBMS().Suspend("active_db");
    ASSERT_FALSE(res.has_value());
    EXPECT_EQ(res.error(), memgraph::dbms::DbmsHandler::SuspendError::ACTIVE_CONNECTIONS);
  }

  // After releasing the extra accessor, suspend succeeds.
  auto res = DBMS().Suspend("active_db");
  EXPECT_TRUE(res.has_value()) << "expected success after the extra accessor was released";
}

TEST_F(HotColdSuspendTest, SuspendDurabilityIncompleteRejected) {
  // A dedicated handler whose default durability is PERIODIC_SNAPSHOT only (no WAL).
  const auto dir = data_directory / "incomplete";
  MinMemgraph mg{MakeConfig(
      dir, /*recover_on_startup=*/false, memgraph::storage::Config::Durability::SnapshotWalMode::PERIODIC_SNAPSHOT)};
  ASSERT_TRUE(mg.dbms.New("incomplete_db").has_value());

  auto res = mg.dbms.Suspend("incomplete_db");
  ASSERT_FALSE(res.has_value());
  EXPECT_EQ(res.error(), memgraph::dbms::DbmsHandler::SuspendError::DURABILITY_INCOMPLETE);
}

TEST_F(HotColdSuspendTest, SuspendOnReplicaRoleRejected) {
  CreateAndPopulate("replica_db", 1);
  DBMS().SetReplicaRoleCheck([] { return true; });

  auto res = DBMS().Suspend("replica_db");
  ASSERT_FALSE(res.has_value());
  EXPECT_EQ(res.error(), memgraph::dbms::DbmsHandler::SuspendError::REPLICA_ROLE);

  // Clear the injected predicate so the rest of the fixture behaves normally.
  DBMS().SetReplicaRoleCheck({});
}

TEST_F(HotColdSuspendTest, SuspendWithinMinResidencyRejectsThenSucceeds) {
  // Large debounce window so a freshly-touched tenant is not yet eligible.
  FLAGS_storage_hot_cold_min_hot_residency_sec = 3600;

  CreateAndPopulate("residency_db", 1);
  // Make sure the last-used stamp is recent.
  DBMS().Get("residency_db")->MarkUsed();

  auto res = DBMS().Suspend("residency_db");
  ASSERT_FALSE(res.has_value());
  EXPECT_EQ(res.error(), memgraph::dbms::DbmsHandler::SuspendError::MIN_RESIDENCY);

  // Drop the debounce — now eligible.
  FLAGS_storage_hot_cold_min_hot_residency_sec = 0;
  auto res2 = DBMS().Suspend("residency_db");
  EXPECT_TRUE(res2.has_value()) << "expected success once the debounce window is zero";
}

// ---------------------------------------------------------------------------
// Success / state cases
// ---------------------------------------------------------------------------

TEST_F(HotColdSuspendTest, SuspendSuccessLeavesColdShellAndRecordsMetadata) {
  CreateAndPopulate("cold_db", 5);

  // Capture the durability dir before suspend.
  const auto durability_dir = DBMS().Get("cold_db")->config().durability.storage_directory;
  ASSERT_TRUE(std::filesystem::exists(durability_dir));

  auto res = DBMS().Suspend("cold_db");
  ASSERT_TRUE(res.has_value()) << "suspend should succeed";

  // The gatekeeper remains in the map as a COLD shell: no live accessor can be minted, so the
  // public Get() (which mints an accessor) must throw UnknownDatabaseException. This proves the
  // accessor is unavailable (db_handler_.Get(name) == nullopt internally).
  EXPECT_THROW((void)DBMS().Get("cold_db"), memgraph::dbms::UnknownDatabaseException);

  // The suspended tenant is reported COLD via TenantRuntimeInfos (proves suspended_ has the entry
  // AND that the COLD shell remains in db_handler_).
  bool found_cold = false;
  for (const auto &info : DBMS().TenantRuntimeInfos()) {
    if (info.name == "cold_db") {
      found_cold = true;
      EXPECT_EQ(info.state, memgraph::dbms::DbmsHandler::TenantState::COLD);
      EXPECT_EQ(info.connections, 0U);
    }
  }
  EXPECT_TRUE(found_cold);

  // On-disk durability must NOT be deleted by suspend.
  EXPECT_TRUE(std::filesystem::exists(durability_dir));
}

TEST_F(HotColdSuspendTest, SuspendedTenantAppearsAsColdInRuntimeInfos) {
  CreateAndPopulate("info_db", 2);
  ASSERT_TRUE(DBMS().Suspend("info_db").has_value());

  std::optional<memgraph::dbms::DbmsHandler::TenantRuntimeInfo> info;
  for (const auto &i : DBMS().TenantRuntimeInfos()) {
    if (i.name == "info_db") info = i;
  }
  ASSERT_TRUE(info.has_value());
  EXPECT_EQ(info->state, memgraph::dbms::DbmsHandler::TenantState::COLD);
  EXPECT_EQ(info->connections, 0U);
}

TEST_F(HotColdSuspendTest, RestartEquivalenceRecoversSuspendedTenantData) {
  constexpr int kNodes = 17;
  CreateAndPopulate("durable_db", kNodes);
  ASSERT_TRUE(DBMS().Suspend("durable_db").has_value());

  // Build a FRESH DbmsHandler on the SAME data_directory with recover_on_startup=true.
  // The tenant must recover with exactly kNodes nodes — proving suspend == durable.
  min_mg.reset();  // tear down the first handler (releases the data dir)
  min_mg.emplace(MakeConfig(data_directory, /*recover_on_startup=*/true));

  auto db_acc = DBMS().Get("durable_db");
  EXPECT_EQ(CountNodes(db_acc), kNodes);
}

#endif  // MG_ENTERPRISE
