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

// Unit tests for the hot/cold suspend engine (DbmsHandler::Suspend_).
//
// C4 is the NODE-LOCAL suspend engine only. Suspension is verified through the
// primitives this commit owns: the Suspend() result code, Get() accessibility
// (a COLD tenant is no longer HOT, so Get() throws), and All() membership (the
// HOT set excludes the COLD shell). Durable round-trip is covered in C9 and the
// SHOW-facing observability views in C11.
//
// Coverage:
//   SuspendDefaultDbRejected            — kDefaultDB is never suspendable
//   SuspendNonExistentRejected          — absent tenant returns NON_EXISTENT
//   SuspendWithActiveAccessorRejected   — live accessor -> ACTIVE_CONNECTIONS, rolls back to HOT
//   SuspendDurabilityIncompleteRejected — DISABLED durability -> DURABILITY_INCOMPLETE
//   SuspendSuccessMakesColdShellInaccessible — happy path: Get() throws, name leaves All()
//   SuspendMultipleTenants              — two independent suspends both succeed

#ifdef MG_ENTERPRISE

#include <algorithm>
#include <filesystem>
#include <optional>
#include <string>

#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include "dbms/constants.hpp"
#include "dbms/dbms_handler.hpp"
#include "storage/v2/config.hpp"

namespace fs = std::filesystem;
using memgraph::dbms::DbmsHandler;

// Per-test-binary storage root.
static fs::path g_storage_root{fs::temp_directory_path() / "MG_test_unit_hot_cold_suspend"};

class HotColdSuspend : public ::testing::Test {
 protected:
  void SetUp() override {
    test_dir_ = g_storage_root / ::testing::UnitTest::GetInstance()->current_test_info()->name();
    fs::remove_all(test_dir_);
    fs::create_directories(test_dir_);

    memgraph::storage::Config conf;
    memgraph::storage::UpdatePaths(conf, test_dir_);
    conf.durability.snapshot_wal_mode =
        memgraph::storage::Config::Durability::SnapshotWalMode::PERIODIC_SNAPSHOT_WITH_WAL;
    handler_ = std::make_unique<DbmsHandler>(conf);
  }

  void TearDown() override {
    handler_.reset();
    fs::remove_all(test_dir_);
  }

  /// Create a secondary (non-default) tenant with full snapshot+WAL durability.
  std::string CreateTenant(std::string name) {
    auto result = handler_->New(name);
    EXPECT_TRUE(result.has_value()) << "Failed to create tenant: " << name;
    return name;
  }

  /// True iff `name` is present in the HOT set returned by All().
  bool InAll(std::string_view name) {
    auto all = handler_->All();
    return std::find(all.begin(), all.end(), name) != all.end();
  }

  fs::path test_dir_;
  std::unique_ptr<DbmsHandler> handler_;
};

// The default database ("memgraph") is never suspendable.
TEST_F(HotColdSuspend, SuspendDefaultDbRejected) {
  auto result = handler_->Suspend(memgraph::dbms::kDefaultDB);
  ASSERT_FALSE(result.has_value());
  EXPECT_EQ(result.error(), DbmsHandler::SuspendError::DEFAULT_DB);
}

// Suspending a name that was never created returns NON_EXISTENT.
TEST_F(HotColdSuspend, SuspendNonExistentRejected) {
  auto result = handler_->Suspend("ghost_db");
  ASSERT_FALSE(result.has_value());
  EXPECT_EQ(result.error(), DbmsHandler::SuspendError::NON_EXISTENT);
}

// A live DatabaseAccess keeps the gatekeeper count > 1, so try_begin_suspend
// times out and Suspend_ returns ACTIVE_CONNECTIONS. The rollback guard must
// leave the tenant HOT (still accessible via Get()).
TEST_F(HotColdSuspend, SuspendWithActiveAccessorRejected) {
  auto name = CreateTenant("busy_db");

  auto acc = handler_->Get(name);  // bumps the gatekeeper count to 2
  ASSERT_TRUE(acc);

  auto result = handler_->Suspend(name);
  ASSERT_FALSE(result.has_value());
  EXPECT_EQ(result.error(), DbmsHandler::SuspendError::ACTIVE_CONNECTIONS);

  // Rolled back to HOT: the tenant is still in the HOT set and still accessible.
  EXPECT_TRUE(InAll(name)) << "Tenant must remain HOT after a failed (rolled-back) suspend";
  auto acc2 = handler_->Get(name);
  EXPECT_TRUE(acc2) << "Gatekeeper must be back in HOT after abort_suspend";
}

// A tenant whose durability config is DISABLED (no periodic snapshot+WAL) must
// be rejected with DURABILITY_INCOMPLETE.
TEST(HotColdSuspendNoDurability, SuspendDurabilityIncompleteRejected) {
  fs::path dir{g_storage_root / "no_durability_test"};
  fs::remove_all(dir);
  fs::create_directories(dir);

  memgraph::storage::Config conf;
  memgraph::storage::UpdatePaths(conf, dir);
  conf.durability.snapshot_wal_mode = memgraph::storage::Config::Durability::SnapshotWalMode::DISABLED;

  auto handler = std::make_unique<DbmsHandler>(conf);
  {
    // The accessor returned by New() must be released BEFORE handler.reset(),
    // otherwise ~Gatekeeper() waits forever for count_ == 0.
    auto result = handler->New("no_wal_db");
    ASSERT_TRUE(result.has_value()) << "Failed to create no-durability tenant";

    auto suspend = handler->Suspend("no_wal_db");
    ASSERT_FALSE(suspend.has_value());
    EXPECT_EQ(suspend.error(), DbmsHandler::SuspendError::DURABILITY_INCOMPLETE);
  }
  handler.reset();
  fs::remove_all(dir);
}

// HOLE-1 (C15): a USER suspend of a durability-incomplete tenant is rejected (DURABILITY_INCOMPLETE),
// but SuspendForRecovery() BYPASSES that gate so a replica converging to MAIN's authoritative cold set
// is not stuck in a BEHIND retry loop. The consolidating snapshot is written unconditionally, so the
// forced cold shell stays recoverable.
TEST(HotColdSuspendNoDurability, SuspendForRecoveryBypassesDurabilityGate) {
  fs::path dir{g_storage_root / "no_durability_recovery_test"};
  fs::remove_all(dir);
  fs::create_directories(dir);

  memgraph::storage::Config conf;
  memgraph::storage::UpdatePaths(conf, dir);
  conf.durability.snapshot_wal_mode = memgraph::storage::Config::Durability::SnapshotWalMode::DISABLED;

  auto handler = std::make_unique<DbmsHandler>(conf);
  std::string uuid_str;
  {
    // Release the New() accessor before suspending: a held accessor pins the gatekeeper and the
    // suspend freeze would never reach sole-accessor (count == 1).
    auto result = handler->New("no_wal_db");
    ASSERT_TRUE(result.has_value()) << "Failed to create no-durability tenant";
    uuid_str = static_cast<std::string>(result.value()->storage()->uuid());
  }

  // A user-initiated suspend is rejected — the gate protects against an unrecoverable cold tenant.
  {
    auto suspend = handler->Suspend("no_wal_db");
    ASSERT_FALSE(suspend.has_value());
    EXPECT_EQ(suspend.error(), DbmsHandler::SuspendError::DURABILITY_INCOMPLETE);
  }

  // Recovery bypasses the gate and succeeds.
  {
    auto recovery = handler->SuspendForRecovery("no_wal_db");
    ASSERT_TRUE(recovery.has_value()) << "SuspendForRecovery must bypass the durability-complete gate";
  }
  EXPECT_TRUE(handler->IsSuspended("no_wal_db")) << "the recovery-forced tenant must be COLD";
  EXPECT_THROW(handler->Get("no_wal_db"), std::exception) << "a COLD tenant's Get() must fail";

  // The bypass's whole safety argument is that the consolidating snapshot is written UNCONDITIONALLY
  // (even with WAL disabled), so the forced cold shell stays recoverable. Lock that invariant in: the
  // tenant's snapshot directory must hold a snapshot after SuspendForRecovery — otherwise a future
  // regression that skipped the snapshot on the no-WAL path would silently make the shell unrecoverable.
  const auto snap_dir = dir / std::string(memgraph::dbms::kMultiTenantDir) / uuid_str / "snapshots";
  EXPECT_TRUE(fs::exists(snap_dir) && !fs::is_empty(snap_dir))
      << "the consolidating snapshot must be written so the recovery-forced cold shell is recoverable: " << snap_dir;

  handler.reset();
  fs::remove_all(dir);
}

// Happy path: a successful Suspend() leaves a COLD shell that is no longer HOT,
// so the tenant drops out of the HOT set (All()) and Get() throws.
TEST_F(HotColdSuspend, SuspendSuccessMakesColdShellInaccessible) {
  auto name = CreateTenant("freeze_db");
  ASSERT_TRUE(InAll(name));

  auto result = handler_->Suspend(name);
  ASSERT_TRUE(result.has_value()) << "Suspend unexpectedly failed";

  // COLD: not in the HOT set, and a Get() no longer yields an accessor.
  EXPECT_FALSE(InAll(name)) << "A COLD tenant must not appear in the HOT set";
  EXPECT_THROW(handler_->Get(name), std::exception) << "Get() on a COLD tenant must fail";
}

// Two independent tenants can be suspended; each goes COLD without affecting the
// other or the default DB.
TEST_F(HotColdSuspend, SuspendMultipleTenants) {
  auto a = CreateTenant("cold_a");
  auto b = CreateTenant("cold_b");

  ASSERT_TRUE(handler_->Suspend(a).has_value());
  EXPECT_FALSE(InAll(a));
  EXPECT_TRUE(InAll(b)) << "Suspending one tenant must not affect another";
  EXPECT_TRUE(InAll(memgraph::dbms::kDefaultDB));

  ASSERT_TRUE(handler_->Suspend(b).has_value());
  EXPECT_FALSE(InAll(b));
  EXPECT_THROW(handler_->Get(a), std::exception);
  EXPECT_THROW(handler_->Get(b), std::exception);
}

#else

// Community build: the entire suspend feature is enterprise-only.
#include <gtest/gtest.h>

TEST(HotColdSuspend, NotApplicableInCommunity) { GTEST_SKIP() << "hot/cold suspend is an enterprise-only feature"; }

#endif  // MG_ENTERPRISE
