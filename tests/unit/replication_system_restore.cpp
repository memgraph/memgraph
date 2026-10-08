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

#include <filesystem>
#include <functional>
#include <memory>

#include <gtest/gtest.h>

// Must precede any rpc/utils.hpp inclusion so its SLK Load/Save overloads are visible there.
#include "replication_handler/system_rpc.hpp"

#include "auth/auth.hpp"
#include "communication/context.hpp"
#include "communication/init.hpp"
#include "dbms/dbms_handler.hpp"
#include "kvstore/kvstore.hpp"
#include "license/license.hpp"
#include "parameters/parameters.hpp"
#include "replication/config.hpp"
#include "replication/replication_client.hpp"
#include "replication_handler/replication_handler.hpp"
#include "rpc/file_replication_handler.hpp"
#include "rpc/server.hpp"
#include "rpc/utils.hpp"
#include "storage/v2/config.hpp"
#include "system/system.hpp"

namespace {
using memgraph::replication::ReplicationClient;
using memgraph::replication::SystemRecoveryReq;
using memgraph::replication::SystemRecoveryRes;
using memgraph::replication::SystemRecoveryRpc;
using State = ReplicationClient::State;

memgraph::communication::SSLInit ssl_init;

constexpr uint16_t kPort = 10'451;
}  // namespace

class SystemRestoreTest : public ::testing::Test {
 protected:
  void SetUp() override {
    std::filesystem::remove_all(dir_);
    std::filesystem::create_directories(dir_);
  }

  void TearDown() override {
    if (client_) client_->Shutdown();
    if (server_) {
      server_->Shutdown();
      server_->AwaitShutdown();
    }
    memgraph::license::global_license_checker.DisableTesting();
    std::filesystem::remove_all(dir_);
  }

  // corrupt_auth: stores a user whose "uuid" is a bad JSON array, so Auth::AllUsers() throws a nlohmann type_error
  // (not AuthException).
  void Start(bool corrupt_auth = false) {
    auto const auth_dir = (dir_ / "auth").string();
    if (corrupt_auth) {
      {
        memgraph::auth::Auth const create_store{auth_dir, memgraph::auth::Auth::Config{}};
      }
      memgraph::kvstore::KVStore store{auth_dir};
      auto user = memgraph::auth::User{"bob"}.Serialize();
      user["uuid"] = nlohmann::json::array({"not", "a", "uuid"});
      ASSERT_TRUE(store.Put("user:bob", user.dump()));
    }
    auth_ = std::make_unique<memgraph::auth::SynchedAuth>(auth_dir, memgraph::auth::Auth::Config{});
    parameters_ = std::make_unique<memgraph::parameters::Parameters>(dir_);
    memgraph::storage::Config conf;
    conf.durability.root_data_directory = dir_;
    memgraph::storage::UpdatePaths(conf, dir_);
    dbms_ = std::make_unique<memgraph::dbms::DbmsHandler>(conf);

    endpoint_ = memgraph::io::network::Endpoint{"127.0.0.1", kPort};
    server_ = std::make_unique<memgraph::rpc::Server>(endpoint_, &server_context_, /* workers */ 1);
    server_->Register<SystemRecoveryRpc>([this](std::optional<memgraph::rpc::FileReplicationHandler> const &,
                                                uint64_t const request_version,
                                                auto *req_reader,
                                                auto *res_builder) {
      SystemRecoveryReq req;
      memgraph::rpc::LoadWithUpgrade(req, request_version, req_reader);
      if (during_rpc_) during_rpc_();
      SystemRecoveryRes res{recovery_result_};
      memgraph::rpc::SendFinalResponse(res, request_version, res_builder);
    });
    ASSERT_TRUE(server_->Start());

    client_ = std::make_unique<ReplicationClient>(memgraph::replication::ReplicationClientConfig{
        .name = "replica",
        .mode = memgraph::replication_coordination_glue::ReplicationMode::SYNC,
        .repl_server_endpoint = endpoint_,
        .replica_check_frequency = std::chrono::seconds{0}});
  }

  void Restore() {
    client_->state_.WithLock([](auto &s) { s = State::BEHIND; });
    memgraph::replication::SystemRestore<true>(*client_,
                                               system_,
                                               *dbms_,
                                               main_uuid_
#ifdef MG_ENTERPRISE
                                               ,
                                               *auth_
#endif
                                               ,
                                               *parameters_);
  }

  State CurrentState() const {
    return client_->state_.WithLock([](auto &s) { return s; });
  }

  std::filesystem::path dir_{std::filesystem::temp_directory_path() / "MG_test_unit_replication_system_restore"};
  memgraph::utils::UUID main_uuid_;
  memgraph::system::System system_;
  std::function<void()> during_rpc_;
  SystemRecoveryRes::Result recovery_result_{SystemRecoveryRes::Result::SUCCESS};
  memgraph::io::network::Endpoint endpoint_;
  std::unique_ptr<memgraph::auth::SynchedAuth> auth_;
  std::unique_ptr<memgraph::parameters::Parameters> parameters_;
  std::unique_ptr<memgraph::dbms::DbmsHandler> dbms_;
  memgraph::communication::ServerContext server_context_;
  std::unique_ptr<memgraph::rpc::Server> server_;
  std::unique_ptr<ReplicationClient> client_;
};

TEST_F(SystemRestoreTest, QuietRecoveryEndsReady) {
  Start();
  Restore();
  EXPECT_EQ(CurrentState(), State::READY);
}

TEST_F(SystemRestoreTest, ConcurrentBehindIsNotOverwritten) {
  Start();
  during_rpc_ = [this] { client_->state_.WithLock([](auto &s) { s = State::BEHIND; }); };
  Restore();
  EXPECT_EQ(CurrentState(), State::BEHIND);
}

TEST_F(SystemRestoreTest, RecoveryFailureReplyLeavesReplicaBehind) {
  Start();
  recovery_result_ = SystemRecoveryRes::Result::FAILURE;
  Restore();
  EXPECT_EQ(CurrentState(), State::BEHIND);
}

#ifdef MG_ENTERPRISE
TEST_F(SystemRestoreTest, SnapshotFailureDoesNotThrowAndLeavesReplicaBehind) {
  memgraph::license::global_license_checker.EnableTesting();
  Start(/* corrupt_auth */ true);
  EXPECT_NO_THROW(Restore());
  EXPECT_EQ(CurrentState(), State::BEHIND);
}
#endif
