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

#ifdef MG_ENTERPRISE

#include <filesystem>
#include <optional>
#include <vector>

#include <gtest/gtest.h>

#include "auth/auth.hpp"
#include "auth/rpc.hpp"
#include "dbms/dbms_handler.hpp"
#include "license/license.hpp"
#include "parameters/parameters.hpp"
#include "replication_handler/auth_replication_handlers.hpp"
#include "replication_handler/system_replication.hpp"
#include "replication_handler/system_rpc.hpp"
#include "rpc/utils.hpp"
#include "rpc/version.hpp"
#include "slk/streams.hpp"
#include "storage/v2/config.hpp"
#include "system/rpc.hpp"
#include "system/system.hpp"

namespace {

using memgraph::replication::FinalizeSystemTxReq;
using memgraph::replication::FinalizeSystemTxRes;
using memgraph::replication::SystemRecoveryReq;
using memgraph::replication::SystemRecoveryRes;
using memgraph::replication::UpdateAuthDataReq;
using memgraph::replication::UpdateAuthDataRes;
using memgraph::utils::UUID;

template <typename Req, typename Res, typename Handler>
Res RoundTrip(Req const &req, Handler &&handler) {
  std::vector<uint8_t> req_bytes;
  memgraph::slk::Builder req_builder([&](const uint8_t *data, size_t size, bool /*have_more*/) {
    req_bytes.insert(req_bytes.end(), data, data + size);
  });
  memgraph::slk::Save(req, &req_builder);
  req_builder.Finalize();
  memgraph::slk::Reader req_reader(req_bytes.data(), req_bytes.size());

  std::vector<uint8_t> res_bytes;
  memgraph::slk::Builder res_builder([&](const uint8_t *data, size_t size, bool /*have_more*/) {
    res_bytes.insert(res_bytes.end(), data, data + size);
  });
  handler(Req::kVersion, &req_reader, &res_builder);

  memgraph::slk::Reader res_reader(res_bytes.data(), res_bytes.size());
  (void)memgraph::rpc::LoadMessageHeader(&res_reader);
  Res res;
  memgraph::slk::Load(&res, &res_reader);
  return res;
}

class SystemReplicationHandlersTest : public ::testing::Test {
 protected:
  void SetUp() override {
    memgraph::license::global_license_checker.EnableTesting();
    std::filesystem::remove_all(dir_);
    memgraph::storage::Config config{.durability = {.root_data_directory = dir_}};
    memgraph::storage::UpdatePaths(config, dir_);
    auth_.emplace(dir_ / "auth", memgraph::auth::Auth::Config{});
    parameters_.emplace(dir_);
    dbms_.emplace(config);
    access_.emplace(system_.CreateSystemStateAccess());
  }

  void TearDown() override {
    access_.reset();
    dbms_.reset();
    parameters_.reset();
    auth_.reset();
    std::filesystem::remove_all(dir_);
  }

  bool UpdateAuth(UUID const &main, uint64_t expected, uint64_t next, std::string const &username) {
    return RoundTrip<UpdateAuthDataReq, UpdateAuthDataRes>(
               UpdateAuthDataReq{main, expected, next, memgraph::auth::User{username}},
               [&](uint64_t version, auto *reader, auto *builder) {
                 memgraph::auth::UpdateAuthDataHandler(*access_, current_main_, *auth_, version, reader, builder);
               })
        .success;
  }

  bool Finalize(UUID const &main, uint64_t expected, uint64_t next) {
    return RoundTrip<FinalizeSystemTxReq, FinalizeSystemTxRes>(FinalizeSystemTxReq{main, expected, next},
                                                               [&](uint64_t version, auto *reader, auto *builder) {
                                                                 memgraph::replication::FinalizeSystemTxHandler(
                                                                     *access_, current_main_, version, reader, builder);
                                                               })
        .success;
  }

  SystemRecoveryRes::Result Recover(UUID const &main, uint64_t forced_ts) {
    SystemRecoveryReq req{
        main, forced_ts, {dbms_->Get()->config().salient}, memgraph::auth::Auth::Config{}, {}, {}, {}, {}, {}};
    return RoundTrip<SystemRecoveryReq, SystemRecoveryRes>(
               req,
               [&](uint64_t version, auto *reader, auto *builder) {
                 memgraph::replication::SystemRecoveryHandler(
                     *access_, current_main_, *dbms_, *auth_, *parameters_, version, reader, builder);
               })
        .result;
  }

  bool HasUser(std::string const &username) { return auth_->Lock()->GetUser(username).has_value(); }

  uint64_t Lcts() const { return access_->LastCommitedTS(); }

  std::filesystem::path dir_{std::filesystem::temp_directory_path() / "MG_test_unit_system_replication_handlers"};
  UUID main_uuid_;
  std::optional<UUID> current_main_{main_uuid_};
  memgraph::system::System system_;
  std::optional<memgraph::system::ReplicaHandlerAccessToState> access_;
  std::optional<memgraph::auth::SynchedAuth> auth_;
  std::optional<memgraph::parameters::Parameters> parameters_;
  std::optional<memgraph::dbms::DbmsHandler> dbms_;
};

TEST_F(SystemReplicationHandlersTest, StaleRecoveryAfterDeltaAndFinalizeIsRefused) {
  ASSERT_TRUE(UpdateAuth(main_uuid_, 0, 1, "alice"));
  ASSERT_TRUE(Finalize(main_uuid_, 0, 1));
  ASSERT_EQ(Lcts(), 1);

  EXPECT_EQ(Recover(main_uuid_, 0), SystemRecoveryRes::Result::FAILURE);
  EXPECT_EQ(Lcts(), 1);
  EXPECT_TRUE(HasUser("alice"));
}

TEST_F(SystemReplicationHandlersTest, StaleRecoveryBetweenDeltaAndFinalizeIsRefused) {
  ASSERT_TRUE(UpdateAuth(main_uuid_, 0, 1, "alice"));

  EXPECT_EQ(Recover(main_uuid_, 0), SystemRecoveryRes::Result::FAILURE);
  EXPECT_TRUE(HasUser("alice"));

  EXPECT_TRUE(Finalize(main_uuid_, 0, 1));
  EXPECT_EQ(Lcts(), 1);
  EXPECT_TRUE(HasUser("alice"));
}

TEST_F(SystemReplicationHandlersTest, RecoveryFromNewMainWithLowerTsIsApplied) {
  ASSERT_TRUE(Finalize(main_uuid_, 0, 5));
  ASSERT_EQ(Lcts(), 5);

  UUID const new_main;
  current_main_ = new_main;
  EXPECT_EQ(Recover(new_main, 2), SystemRecoveryRes::Result::SUCCESS);
  EXPECT_EQ(Lcts(), 2);
}

TEST_F(SystemReplicationHandlersTest, StaleRecoveryIsRefusedOnlyOnce) {
  ASSERT_TRUE(UpdateAuth(main_uuid_, 0, 1, "alice"));
  ASSERT_TRUE(Finalize(main_uuid_, 0, 1));

  EXPECT_EQ(Recover(main_uuid_, 0), SystemRecoveryRes::Result::FAILURE);
  EXPECT_EQ(Lcts(), 1);

  EXPECT_EQ(Recover(main_uuid_, 0), SystemRecoveryRes::Result::SUCCESS);
  EXPECT_EQ(Lcts(), 0);
}

TEST_F(SystemReplicationHandlersTest, RecoveryNotOlderThanDeltasIsApplied) {
  ASSERT_TRUE(Finalize(main_uuid_, 0, 1));

  EXPECT_EQ(Recover(main_uuid_, 1), SystemRecoveryRes::Result::SUCCESS);
  EXPECT_EQ(Lcts(), 1);
}

}  // namespace

#endif  // MG_ENTERPRISE
