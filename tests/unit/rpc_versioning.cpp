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

#include "gtest/gtest.h"

#include "coordination/coordinator_rpc.hpp"
#include "coordination/instance_state.hpp"
#include "replication_coordination_glue/common.hpp"

#include "rpc_messages.hpp"

#include "replication_handler/system_rpc.hpp"
#include "rpc/client.hpp"
#include "rpc/file_replication_handler.hpp"
#include "rpc/server.hpp"
#include "rpc/utils.hpp"  // Needs to be included last so that SLK definitions are seen
#include "slk/streams.hpp"

using memgraph::communication::ClientContext;
using memgraph::communication::ServerContext;

using memgraph::io::network::Endpoint;
using memgraph::rpc::Client;
using memgraph::rpc::Server;
using memgraph::storage::replication::HeartbeatRpc;

using namespace std::string_view_literals;
using namespace std::literals::chrono_literals;

namespace {
constexpr int port{8182};
}  // namespace

// RPC client is setup with timeout but shouldn't be triggered.
TEST(RpcVersioning, SumUpgrade) {
  Endpoint const endpoint{"localhost", port};

  ServerContext server_context;
  Server rpc_server{endpoint, &server_context, /* workers */ 1};
  auto const on_exit = memgraph::utils::OnScopeExit{[&rpc_server] {
    rpc_server.Shutdown();
    rpc_server.AwaitShutdown();
  }};

  rpc_server.Register<Sum>([](std::optional<memgraph::rpc::FileReplicationHandler> const & /*file_replication_handler*/,
                              uint64_t const request_version,
                              auto *req_reader,
                              auto *res_builder) {
    SumReq req;
    memgraph::rpc::LoadWithUpgrade(req, request_version, req_reader);

    auto const sum = std::accumulate(req.nums_.begin(), req.nums_.end(), 0);

    SumRes const res({sum});
    memgraph::rpc::SendFinalResponse(res, request_version, res_builder);
  });

  ASSERT_TRUE(rpc_server.Start());
  std::this_thread::sleep_for(100ms);

  auto const rpc_timeouts = std::unordered_map{std::make_pair("EchoMessage"sv, 2000)};
  ClientContext client_context;
  Client client{endpoint, &client_context, rpc_timeouts};
  {
    // Send new version request
    auto stream = client.Stream<Sum>(std::initializer_list<int>{35, 30});
    auto reply = stream.value().SendAndWait();
    EXPECT_EQ(reply.value().sum, std::vector<int>{65});
  }
  {
    // Send old versioned request
    auto stream = client.Stream<SumV1>(10, 12);
    auto reply = stream.value().SendAndWait();
    EXPECT_EQ(reply.value().sum, 22);
  }
}

#ifdef MG_ENTERPRISE

namespace memgraph::coordination {
using GetDatabaseHistoriesRpcV1 = rpc::RequestResponse<GetDatabaseHistoriesReqV1, GetDatabaseHistoriesResV1>;
}  // namespace memgraph::coordination

TEST(RpcVersioning, GetDBHistories) {
  Endpoint const endpoint{"localhost", port};

  ServerContext server_context;
  Server rpc_server{endpoint, &server_context, /* workers */ 1};
  auto const on_exit = memgraph::utils::OnScopeExit{[&rpc_server] {
    rpc_server.Shutdown();
    rpc_server.AwaitShutdown();
  }};

  rpc_server.Register<memgraph::coordination::GetDatabaseHistoriesRpc>(
      [](std::optional<memgraph::rpc::FileReplicationHandler> const & /*file_replication_handler*/,
         uint64_t const request_version,
         auto * /*req_reader*/,
         auto *res_builder) {
        // The request is empty hence I don't need to call LoadWithUpgrade

        if (request_version == memgraph::coordination::GetDatabaseHistoriesReqV1::kVersion) {
          memgraph::coordination::GetDatabaseHistoriesResV1 res;
          res.arg_.last_committed_system_timestamp = 81;
          res.arg_.dbs_info = std::vector{memgraph::replication_coordination_glue::InstanceDBInfoV1{
                                              .db_uuid = "123", .latest_durable_timestamp = 4},
                                          memgraph::replication_coordination_glue::InstanceDBInfoV1{
                                              .db_uuid = "1234", .latest_durable_timestamp = 13}};

          memgraph::rpc::SendFinalResponse(res, request_version, res_builder);
        } else {
          memgraph::coordination::GetDatabaseHistoriesRes res;
          res.arg_.last_committed_system_timestamp = 81;
          res.arg_.dbs_info = std::vector{
              memgraph::replication_coordination_glue::InstanceDBInfo{.db_uuid = "123", .num_committed_txns = 2},
              memgraph::replication_coordination_glue::InstanceDBInfo{.db_uuid = "1234", .num_committed_txns = 22}};

          memgraph::rpc::SendFinalResponse(res, request_version, res_builder);
        }
      });

  ASSERT_TRUE(rpc_server.Start());
  std::this_thread::sleep_for(100ms);

  ClientContext client_context;
  Client client{endpoint, &client_context};
  {
    // Send new version request
    auto stream = client.Stream<memgraph::coordination::GetDatabaseHistoriesRpc>();

    auto reply = stream.value().SendAndWait();

    EXPECT_EQ(reply.value().arg_.last_committed_system_timestamp, 81);
    auto const dbs_info_res = std::vector{
        {memgraph::replication_coordination_glue::InstanceDBInfo{.db_uuid = "123", .num_committed_txns = 2},
         memgraph::replication_coordination_glue::InstanceDBInfo{.db_uuid = "1234", .num_committed_txns = 22}}};
    EXPECT_EQ(reply.value().arg_.dbs_info, dbs_info_res);
  }

  {
    // Send old version request
    auto stream = client.Stream<memgraph::coordination::GetDatabaseHistoriesRpcV1>();
    auto reply = stream.value().SendAndWait();
    EXPECT_EQ(reply.value().arg_.last_committed_system_timestamp, 81);
    auto const dbs_info_res = std::vector{
        {memgraph::replication_coordination_glue::InstanceDBInfoV1{.db_uuid = "123", .latest_durable_timestamp = 4},
         memgraph::replication_coordination_glue::InstanceDBInfoV1{
             .db_uuid = "1234",
             .latest_durable_timestamp = 13,
         }}};
    EXPECT_EQ(reply.value().arg_.dbs_info, dbs_info_res);
  }
}

namespace memgraph::coordination {
using StateCheckRpcV1 = rpc::RequestResponse<StateCheckReqV1, StateCheckResV1>;
}  // namespace memgraph::coordination

TEST(RpcVersioning, StateCheckRpc) {
  Endpoint const endpoint{"localhost", port};

  ServerContext server_context;
  Server rpc_server{endpoint, &server_context, /* workers */ 1};
  auto const on_exit = memgraph::utils::OnScopeExit{[&rpc_server] {
    rpc_server.Shutdown();
    rpc_server.AwaitShutdown();
  }};

  std::map<std::string, uint64_t> const main_num_txns{{"a", 1}, {"b", 5}, {"c", 9}};
  std::map<std::string, std::map<std::string, int64_t>> const replicas_num_txns{
      {"instance_1", std::map<std::string, int64_t>{{"a", 1}, {"b", 4}, {"c", 6}}},
      {"instance_2", std::map<std::string, int64_t>{{"a", 1}, {"b", 6}, {"c", 9}}}

  };

  rpc_server.Register<memgraph::coordination::StateCheckRpc>(
      [&main_num_txns, &replicas_num_txns](
          std::optional<memgraph::rpc::FileReplicationHandler> const & /*file_replication_handler*/,
          uint64_t const request_version,
          auto * /*req_reader*/,
          auto *res_builder) {
        memgraph::coordination::InstanceStateV2 inner_state{.is_replica = false,
                                                            .uuid = memgraph::utils::UUID{},
                                                            .is_writing_enabled = true,
                                                            .main_num_txns = main_num_txns,
                                                            .replicas_num_txns = replicas_num_txns};
        memgraph::coordination::InstanceState const instance_state{.inner_state = std::move(inner_state),
                                                                   .deltas_batch_progress_size = 12000};
        memgraph::coordination::StateCheckRes const res{instance_state};
        memgraph::rpc::SendFinalResponse(res, request_version, res_builder);
      });

  ASSERT_TRUE(rpc_server.Start());
  std::this_thread::sleep_for(100ms);

  ClientContext client_context;
  Client client{endpoint, &client_context};
  {
    // Send new version request
    auto stream = client.Stream<memgraph::coordination::StateCheckRpc>();
    auto reply = stream.value().SendAndWait();

    EXPECT_FALSE(reply.value().arg_.inner_state.is_replica);
    EXPECT_TRUE(reply.value().arg_.inner_state.is_writing_enabled);
    EXPECT_EQ(*reply.value().arg_.inner_state.main_num_txns, main_num_txns);
    EXPECT_EQ(*reply.value().arg_.inner_state.replicas_num_txns, replicas_num_txns);
  }

  {
    // Send old version request
    auto stream = client.Stream<memgraph::coordination::StateCheckRpcV1>();
    auto reply = stream.value().SendAndWait();
    EXPECT_FALSE(reply.value().arg_.is_replica);
    EXPECT_TRUE(reply.value().arg_.is_writing_enabled);
  }
}
#endif

// Test: when request has 2 versions but response has only one version (no Downgrade),
// SendFinalResponse(res, request_version=1, ...) throws because SaveWithDowngrade cannot produce v1.
TEST(RpcVersioning, RequestTwoVersionsSingleVersionResponse_ThrowsWhenSendingV1) {
  std::vector<uint8_t> sink;
  memgraph::slk::Builder builder(
      [&sink](const uint8_t *data, size_t size, bool) { sink.insert(sink.end(), data, data + size); });
  TestResSingleVersion res;
  EXPECT_THROW(memgraph::rpc::SaveWithDowngrade(res, 1, &builder), std::runtime_error);
}

// SystemRecoveryRpc: with ResV1 and Downgrade, both v1 and v2 requests get correct response.
TEST(RpcVersioning, SystemRecoveryRpc_V1AndV2Request_BothSucceed) {
  Endpoint const endpoint{"localhost", port};

  ServerContext server_context;
  Server rpc_server{endpoint, &server_context, /* workers */ 1};
  auto const on_exit = memgraph::utils::OnScopeExit{[&rpc_server] {
    rpc_server.Shutdown();
    rpc_server.AwaitShutdown();
  }};

  rpc_server.Register<memgraph::replication::SystemRecoveryRpc>(
      [](std::optional<memgraph::rpc::FileReplicationHandler> const & /*file_replication_handler*/,
         uint64_t const request_version,
         auto *req_reader,
         auto *res_builder) {
        memgraph::replication::SystemRecoveryReq req;
        memgraph::rpc::LoadWithUpgrade(req, request_version, req_reader);
        memgraph::replication::SystemRecoveryRes const res(memgraph::replication::SystemRecoveryRes::Result::SUCCESS);
        memgraph::rpc::SendFinalResponse(res, request_version, res_builder);
      });

  ASSERT_TRUE(rpc_server.Start());
  std::this_thread::sleep_for(100ms);

  ClientContext client_context;
  Client client{endpoint, &client_context};
  {
    auto stream =
        client.Stream<memgraph::replication::SystemRecoveryRpc>(memgraph::utils::UUID{},
                                                                0,
                                                                std::vector<memgraph::storage::SalientConfig>{},
                                                                memgraph::auth::Auth::Config{},
                                                                std::vector<memgraph::auth::User>{},
                                                                std::vector<memgraph::auth::Role>{},
                                                                std::vector<memgraph::auth::UserProfiles::Profile>{});
    auto reply = stream.SendAndWait();
    EXPECT_EQ(reply.result, memgraph::replication::SystemRecoveryRes::Result::SUCCESS);
  }
  {
    auto stream =
        client.Stream<memgraph::replication::SystemRecoveryRpc>(memgraph::utils::UUID{},
                                                                0,
                                                                std::vector<memgraph::storage::SalientConfig>{},
                                                                memgraph::auth::Auth::Config{},
                                                                std::vector<memgraph::auth::User>{},
                                                                std::vector<memgraph::auth::Role>{},
                                                                std::vector<memgraph::auth::UserProfiles::Profile>{},
                                                                std::vector<memgraph::parameters::ParameterInfo>{});
    auto reply = stream.SendAndWait();
    EXPECT_EQ(reply.result, memgraph::replication::SystemRecoveryRes::Result::SUCCESS);
  }
}
