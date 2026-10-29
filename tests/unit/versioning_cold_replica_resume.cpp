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

// A tenant resumed on a REPLICA must not keep Graph Versioning branches (the demotion sweep skips COLD tenants);
// on MAIN, resume keeps them.

#include <chrono>
#include <filesystem>
#include <functional>
#include <future>
#include <memory>
#include <optional>
#include <stdexcept>
#include <string>
#include <thread>

#include <gtest/gtest.h>

#include "auth/auth.hpp"
#include "dbms/dbms_handler.hpp"
#include "flags/general.hpp"
#include "parameters/parameters.hpp"
#include "replication/config.hpp"
#include "replication/state.hpp"
#include "replication_handler/replication_handler.hpp"
#include "storage/v2/config.hpp"
#include "system/system.hpp"
#include "utils/rw_spin_lock.hpp"
#include "utils/synchronized.hpp"
#include "versioning/version_store.hpp"

namespace fs = std::filesystem;
using memgraph::replication::ReplicationHandler;
using memgraph::replication::ReplicationServerConfig;

class ColdReplicaResume : public ::testing::Test {
 protected:
  void SetUp() override {
    test_dir_ = fs::temp_directory_path() / "MG_test_unit_versioning_cold_replica_resume";
    fs::remove_all(test_dir_);
    fs::create_directories(test_dir_);
    memgraph::storage::UpdatePaths(conf_, test_dir_);
    conf_.durability.snapshot_wal_mode =
        memgraph::storage::Config::Durability::SnapshotWalMode::PERIODIC_SNAPSHOT_WITH_WAL;
    conf_.durability.recover_on_startup = true;
    conf_.experimental_versioning = true;

    auth_.emplace(test_dir_ / "auth", memgraph::auth::Auth::Config{});
    parameters_.emplace(test_dir_);
    repl_state_.emplace(memgraph::storage::ReplicationStateRootPath(conf_));
    dbms_.emplace(conf_);
    repl_handler_.emplace(*repl_state_, *dbms_, system_, *auth_, *parameters_);
    dbms_->SetOnResume([this](memgraph::dbms::DatabaseAccess db_acc) {
      if (!repl_handler_->DropBranchesIfReplica(db_acc)) throw std::runtime_error("branch drop failed");
      if (after_drop_) after_drop_();
    });
    dbms_->SetOnPublished([this](memgraph::dbms::DatabaseAccess db_acc) {
      repl_handler_->ReconcileBranchesAfterPublish(std::move(db_acc));
    });
  }

  void TearDown() override {
    {
      auto locked = repl_state_->Lock();
      if (locked->IsReplica()) {
        std::get<memgraph::replication::RoleReplicaData>(locked->ReplicationData()).server.reset();
      }
    }
    repl_handler_.reset();
    dbms_.reset();
    repl_state_.reset();
    fs::remove_all(test_dir_);
  }

  void CreateTenantWithBranch(const std::string &name) {
    ASSERT_TRUE(dbms_->New(name).has_value());
    auto db_acc = dbms_->Get(name);
    ASSERT_TRUE(db_acc->version_store()->CreateBranch("b", "main").has_value());
  }

  static constexpr auto kWait = std::chrono::seconds(30);

  // Resume "cold_tenant" on a thread; the on_resume hook parks after the branch drop until released.
  // Runs `during` on the calling thread while the resume is parked.
  std::optional<memgraph::dbms::DatabaseAccess> ResumeParked(const std::function<void()> &during) {
    std::promise<void> parked, release;
    auto release_f = release.get_future();
    after_drop_ = [&] {
      parked.set_value();
      release_f.wait_for(kWait);
    };
    std::optional<memgraph::dbms::DatabaseAccess> out;
    std::jthread t([&] {
      auto r = dbms_->Resume("cold_tenant");
      if (r.has_value()) out = std::move(r.value().db);
    });
    const bool ok = parked.get_future().wait_for(kWait) == std::future_status::ready;
    if (ok) during();
    release.set_value();
    t.join();
    after_drop_ = nullptr;
    if (!ok) ADD_FAILURE() << "resume never reached the hook";
    return out;
  }

  std::function<void()> after_drop_;
  fs::path test_dir_;
  memgraph::storage::Config conf_;
  memgraph::system::System system_;
  std::optional<memgraph::auth::SynchedAuth> auth_;
  std::optional<memgraph::parameters::Parameters> parameters_;
  std::optional<memgraph::utils::Synchronized<memgraph::replication::ReplicationState, memgraph::utils::RWSpinLock>>
      repl_state_;
  std::optional<memgraph::dbms::DbmsHandler> dbms_;
  std::optional<ReplicationHandler> repl_handler_;
};

TEST_F(ColdReplicaResume, ResumedTenantAfterDemotionHasNoBranches) {
  CreateTenantWithBranch("hot_tenant");
  CreateTenantWithBranch("cold_tenant");
  ASSERT_TRUE(dbms_->Suspend("cold_tenant").has_value());

  ASSERT_TRUE(repl_handler_->SetReplicationRoleReplica(
      ReplicationServerConfig{.repl_server = memgraph::io::network::Endpoint("127.0.0.1", 10'731)}, std::nullopt));

  {  // Control: the HOT tenant was swept by the demotion.
    auto hot = dbms_->Get("hot_tenant");
    EXPECT_TRUE(hot->version_store()->List().empty());
  }

  auto resumed = dbms_->Resume("cold_tenant");
  ASSERT_TRUE(resumed.has_value());
  auto *store = resumed.value().db->version_store();
  ASSERT_NE(store, nullptr);
  EXPECT_TRUE(store->List().empty()) << "a replica's resumed tenant must not hold branches";
}

TEST_F(ColdReplicaResume, ResumedTenantOnMainKeepsBranches) {
  CreateTenantWithBranch("cold_tenant");
  ASSERT_TRUE(dbms_->Suspend("cold_tenant").has_value());

  auto resumed = dbms_->Resume("cold_tenant");
  ASSERT_TRUE(resumed.has_value());
  auto *store = resumed.value().db->version_store();
  ASSERT_NE(store, nullptr);
  EXPECT_EQ(store->List().size(), 1U);
}

TEST_F(ColdReplicaResume, DemotionDuringResume) {
  CreateTenantWithBranch("cold_tenant");
  ASSERT_TRUE(dbms_->Suspend("cold_tenant").has_value());

  std::future<bool> demoted;
  auto resumed = ResumeParked([&] {
    demoted = std::async(std::launch::async, [&] {
      return repl_handler_->SetReplicationRoleReplica(
          ReplicationServerConfig{.repl_server = memgraph::io::network::Endpoint("127.0.0.1", 10'732)}, std::nullopt);
    });
    // Demotion must finish while the resume is parked; if it blocks on the RESUMING tenant, fail instead of hang.
    EXPECT_EQ(demoted.wait_for(kWait), std::future_status::ready) << "demotion blocked on the parked resume";
  });
  if (demoted.valid()) ASSERT_TRUE(demoted.get());
  ASSERT_TRUE(resumed.has_value());
  ASSERT_NE((*resumed)->version_store(), nullptr);
  EXPECT_TRUE((*resumed)->version_store()->List().empty());
}

TEST_F(ColdReplicaResume, PromotionDuringResume) {
  CreateTenantWithBranch("cold_tenant");
  ASSERT_TRUE(dbms_->Suspend("cold_tenant").has_value());
  ASSERT_TRUE(repl_handler_->SetReplicationRoleReplica(
      ReplicationServerConfig{.repl_server = memgraph::io::network::Endpoint("127.0.0.1", 10'733)}, std::nullopt));

  std::future<bool> promoted;
  auto resumed = ResumeParked([&] {
    promoted = std::async(std::launch::async, [&] { return repl_handler_->SetReplicationRoleMain(); });
    EXPECT_EQ(promoted.wait_for(kWait), std::future_status::ready) << "promotion blocked on the parked resume";
  });
  if (promoted.valid()) ASSERT_TRUE(promoted.get());
  ASSERT_TRUE(resumed.has_value());
  auto *store = (*resumed)->version_store();
  ASSERT_NE(store, nullptr);
  EXPECT_TRUE(store->CreateBranch("x", "main").has_value()) << "resumed store left frozen after promotion";
}
