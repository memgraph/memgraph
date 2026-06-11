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

// Unit tests for the hot/cold EVICTION engine (DbmsHandler::SuspendColdestIdleTenants).
// Exercises the public SuspendColdestIdleTenants() API directly; deterministic coldness ordering
// is forced via explicit MarkUsed() calls with small sleeps to ensure strictly increasing
// last_used_ns timestamps.

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
#include "storage/v2/inmemory/storage.hpp"
#include "storage/v2/storage_mode.hpp"
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

// Minimal DbmsHandler harness (mirrors hot_cold_suspend.cpp). Owns everything DbmsHandler depends
// on, in a self-contained scope so it can be torn down and re-created on the SAME data_directory.
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

class HotColdEvictionTest : public ::testing::Test {
 public:
  std::filesystem::path data_directory = std::filesystem::temp_directory_path() / "MG_tests_unit_hot_cold_eviction";

  void SetUp() override {
    TearDown();
    // Debounce off: suspends must not be blocked by the min-hot-residency anti-thrash gate.
    FLAGS_storage_hot_cold_min_hot_residency_sec = 0;
    min_mg.emplace(MakeConfig(data_directory, /*recover_on_startup=*/true));
  }

  void TearDown() override {
    min_mg.reset();
    if (std::filesystem::exists(data_directory)) std::filesystem::remove_all(data_directory);
  }

  auto &DBMS() { return min_mg->dbms; }

  // Create a tenant and write N nodes so the durability artefacts exist.
  void CreateAndPopulate(const std::string &name, int n) {
    ASSERT_TRUE(DBMS().New(name).has_value());
    auto db_acc = DBMS().Get(name);
    auto storage_acc = db_acc->Access(memgraph::storage::WRITE);
    for (int i = 0; i < n; ++i) {
      storage_acc->CreateVertex();
    }
    ASSERT_TRUE(storage_acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()).has_value());
  }

  // Returns true iff name is currently COLD (present in TenantRuntimeInfos as COLD).
  bool IsCold(const std::string &name) {
    for (const auto &info : DBMS().TenantRuntimeInfos()) {
      if (info.name == name) {
        return info.state == memgraph::dbms::DbmsHandler::TenantState::COLD;
      }
    }
    return false;
  }

  // Returns true iff name is currently HOT (present in TenantRuntimeInfos as READY/HOT).
  bool IsHot(const std::string &name) {
    for (const auto &info : DBMS().TenantRuntimeInfos()) {
      if (info.name == name) {
        return info.state == memgraph::dbms::DbmsHandler::TenantState::READY;
      }
    }
    return false;
  }

  std::optional<MinMemgraph> min_mg;
};

// ---------------------------------------------------------------------------
// T1: EvictsColdestFirst
// Create A, B, C. Force a deterministic coldness order: touch C last, then B, then A (most recent).
// With a small sleep between touches to guarantee strictly increasing last_used_ns.
// SuspendColdestIdleTenants(INT64_MAX, 1) must suspend exactly C (coldest) and leave A + B HOT.
// ---------------------------------------------------------------------------

TEST_F(HotColdEvictionTest, EvictsColdestFirst) {
  CreateAndPopulate("A", 2);
  CreateAndPopulate("B", 2);
  CreateAndPopulate("C", 2);

  // Force coldness order: C is coldest (touched first), A is warmest (touched last).
  DBMS().Get("C")->MarkUsed();
  std::this_thread::sleep_for(std::chrono::milliseconds(5));
  DBMS().Get("B")->MarkUsed();
  std::this_thread::sleep_for(std::chrono::milliseconds(5));
  DBMS().Get("A")->MarkUsed();

  const int64_t freed = DBMS().SuspendColdestIdleTenants(INT64_MAX, /*max_evictions=*/1);

  EXPECT_GT(freed, 0) << "at least one tenant was suspended; freed bytes must be positive";

  // C is the coldest — it must now be COLD.
  EXPECT_TRUE(IsCold("C")) << "C (coldest) must be suspended";
  // A and B must still be HOT.
  EXPECT_TRUE(IsHot("A")) << "A (warmest) must remain HOT";
  EXPECT_TRUE(IsHot("B")) << "B (middle) must remain HOT";
}

// ---------------------------------------------------------------------------
// T2: RespectsMaxEvictions
// 3 cold-rankable tenants; max_evictions=2 must suspend exactly 2 (the 2 coldest).
// ---------------------------------------------------------------------------

TEST_F(HotColdEvictionTest, RespectsMaxEvictions) {
  CreateAndPopulate("X", 2);
  CreateAndPopulate("Y", 2);
  CreateAndPopulate("Z", 2);

  // Coldness order: X coldest, Y middle, Z warmest.
  DBMS().Get("X")->MarkUsed();
  std::this_thread::sleep_for(std::chrono::milliseconds(5));
  DBMS().Get("Y")->MarkUsed();
  std::this_thread::sleep_for(std::chrono::milliseconds(5));
  DBMS().Get("Z")->MarkUsed();

  const int64_t freed = DBMS().SuspendColdestIdleTenants(INT64_MAX, /*max_evictions=*/2);

  EXPECT_GT(freed, 0);

  // X and Y (the 2 coldest) must be COLD.
  EXPECT_TRUE(IsCold("X")) << "X (coldest) must be suspended";
  EXPECT_TRUE(IsCold("Y")) << "Y (second-coldest) must be suspended";
  // Z (warmest) must remain HOT.
  EXPECT_TRUE(IsHot("Z")) << "Z (warmest) must remain HOT — max_evictions cap hit";
}

// ---------------------------------------------------------------------------
// T3: StopsAtBytesToFree
// Pass bytes_to_free = 1 (effectively "free at least 1 byte") with max_evictions=10.
// Since any real tenant uses more than 1 byte, exactly 1 eviction satisfies the target.
// Assert that freed >= 1 and that the return value satisfies freed >= bytes_to_free after 1 eviction.
//
// Implementation note: the loop condition is `if (freed >= bytes_to_free) break` AFTER the eviction,
// so it stops after the first successful suspend once the target is met.
// ---------------------------------------------------------------------------

TEST_F(HotColdEvictionTest, StopsAtBytesToFree) {
  CreateAndPopulate("P", 5);
  CreateAndPopulate("Q", 5);
  CreateAndPopulate("R", 5);

  // Coldness order: P coldest, R warmest.
  DBMS().Get("P")->MarkUsed();
  std::this_thread::sleep_for(std::chrono::milliseconds(5));
  DBMS().Get("Q")->MarkUsed();
  std::this_thread::sleep_for(std::chrono::milliseconds(5));
  DBMS().Get("R")->MarkUsed();

  // bytes_to_free = 1 is trivially satisfied by the first eviction (any in-memory tenant > 1 byte).
  constexpr int64_t kBytesToFree = 1;
  const int64_t freed = DBMS().SuspendColdestIdleTenants(kBytesToFree, /*max_evictions=*/10);

  EXPECT_GE(freed, kBytesToFree) << "freed bytes must satisfy the request after the first eviction";

  // Exactly P (coldest) is suspended; Q and R are still HOT.
  EXPECT_TRUE(IsCold("P")) << "P (coldest) must be suspended";
  EXPECT_TRUE(IsHot("Q")) << "Q must remain HOT — bytes_to_free satisfied after first eviction";
  EXPECT_TRUE(IsHot("R")) << "R must remain HOT — bytes_to_free satisfied after first eviction";
}

// ---------------------------------------------------------------------------
// T4: SkipsDefaultDb
// With only the default DB present (plus one normal tenant), assert the default DB is never
// suspended: it must stay HOT and queryable, regardless of max_evictions.
// ---------------------------------------------------------------------------

TEST_F(HotColdEvictionTest, SkipsDefaultDb) {
  // One extra normal tenant so the call has at least one candidate to consider.
  CreateAndPopulate("normal_db", 2);

  // Touch default DB most recently and normal_db less recently (make default DB warmest to confirm
  // it is skipped even when it would be the only remaining HOT tenant after normal_db is evicted).
  DBMS().Get("normal_db")->MarkUsed();
  std::this_thread::sleep_for(std::chrono::milliseconds(5));
  // Touch kDefaultDB to make it the warmest (but it must still be skipped).
  DBMS().Get(memgraph::dbms::kDefaultDB)->MarkUsed();

  // Evict everything possible.
  DBMS().SuspendColdestIdleTenants(INT64_MAX, /*max_evictions=*/100);

  // Default DB must still be HOT.
  EXPECT_TRUE(IsHot(std::string{memgraph::dbms::kDefaultDB}))
      << "the default DB must never be evicted by SuspendColdestIdleTenants";
}

// ---------------------------------------------------------------------------
// T5: SkipsTenantWithActiveAccessor
// Hold a live DatabaseAccess on the coldest tenant; SuspendColdestIdleTenants must skip it
// (Suspend_ returns ACTIVE_CONNECTIONS) and suspend the next-coldest instead.
// ---------------------------------------------------------------------------

TEST_F(HotColdEvictionTest, SkipsTenantWithActiveAccessor) {
  CreateAndPopulate("cold_active", 2);
  CreateAndPopulate("warm_free", 2);

  // Make cold_active the coldest.
  DBMS().Get("cold_active")->MarkUsed();
  std::this_thread::sleep_for(std::chrono::milliseconds(5));
  DBMS().Get("warm_free")->MarkUsed();

  // Hold a live accessor on cold_active — this pins the use-count above 1.
  auto pinned = DBMS().Get("cold_active");

  const int64_t freed = DBMS().SuspendColdestIdleTenants(INT64_MAX, /*max_evictions=*/1);

  EXPECT_GT(freed, 0) << "warm_free must have been evicted instead";

  // cold_active must still be HOT (accessor blocked the suspend).
  EXPECT_TRUE(IsHot("cold_active")) << "cold_active has a live accessor — must remain HOT";
  // warm_free (next-coldest) must now be COLD.
  EXPECT_TRUE(IsCold("warm_free")) << "warm_free (next-coldest, no active accessor) must be COLD";

  // Release the pinned accessor explicitly.
  pinned.reset();
}

// ---------------------------------------------------------------------------
// E1 regression (audit 2026-06-12): a tenant switched to IN_MEMORY_ANALYTICAL at runtime must NOT
// be suspendable. Analytical mode suppresses WAL and pauses the snapshot runner, but
// IsDurabilityCompleteForSuspend() only inspects the creation-time config (never updated by
// SetStorageMode), so without an explicit runtime-mode gate the analytical tenant would pass the
// durability check and lose all post-switch commits on suspend. Suspend_ must reject it (the gate
// requires runtime IN_MEMORY_TRANSACTIONAL).
// ---------------------------------------------------------------------------
TEST_F(HotColdEvictionTest, DoesNotSuspendAnalyticalModeTenant) {
  CreateAndPopulate("ana", 2);

  // Switch the tenant to analytical mode at runtime, then release the accessor so the only thing
  // that could block suspend is the storage-mode gate (not ACTIVE_CONNECTIONS).
  {
    auto db_acc = DBMS().Get("ana");
    auto *mem = static_cast<memgraph::storage::InMemoryStorage *>(db_acc->storage());
    mem->SetStorageMode(memgraph::storage::StorageMode::IN_MEMORY_ANALYTICAL);
    ASSERT_EQ(db_acc->storage()->GetStorageMode(), memgraph::storage::StorageMode::IN_MEMORY_ANALYTICAL);
  }

  // Direct suspend must be rejected with NOT_IN_MEMORY (in-memory analytical is not suspendable).
  auto res = DBMS().Suspend("ana");
  ASSERT_FALSE(res.has_value()) << "analytical-mode tenant must not be suspendable";
  EXPECT_EQ(res.error(), memgraph::dbms::DbmsHandler::SuspendError::NOT_IN_MEMORY);

  // Auto-eviction must also leave it HOT (no silent data loss via the eviction path).
  DBMS().SuspendColdestIdleTenants(INT64_MAX, /*max_evictions=*/4);
  EXPECT_TRUE(IsHot("ana")) << "analytical tenant must remain HOT after an eviction sweep";
}

#endif  // MG_ENTERPRISE
