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
    ASSERT_TRUE(rpc_server.Shutdown());
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
    auto reply = stream.SendAndWait();
    EXPECT_EQ(reply.sum, std::vector<int>{65});
  }
  {
    // Send old versioned request
    auto stream = client.Stream<SumV1>(10, 12);
    auto reply = stream.SendAndWait();
    EXPECT_EQ(reply.sum, 22);
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
    ASSERT_TRUE(rpc_server.Shutdown());
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

    auto reply = stream.SendAndWait();

    EXPECT_EQ(reply.arg_.last_committed_system_timestamp, 81);
    auto const dbs_info_res = std::vector{
        {memgraph::replication_coordination_glue::InstanceDBInfo{.db_uuid = "123", .num_committed_txns = 2},
         memgraph::replication_coordination_glue::InstanceDBInfo{.db_uuid = "1234", .num_committed_txns = 22}}};
    EXPECT_EQ(reply.arg_.dbs_info, dbs_info_res);
  }

  {
    // Send old version request
    auto stream = client.Stream<memgraph::coordination::GetDatabaseHistoriesRpcV1>();
    auto reply = stream.SendAndWait();
    EXPECT_EQ(reply.arg_.last_committed_system_timestamp, 81);
    auto const dbs_info_res = std::vector{
        {memgraph::replication_coordination_glue::InstanceDBInfoV1{.db_uuid = "123", .latest_durable_timestamp = 4},
         memgraph::replication_coordination_glue::InstanceDBInfoV1{
             .db_uuid = "1234",
             .latest_durable_timestamp = 13,
         }}};
    EXPECT_EQ(reply.arg_.dbs_info, dbs_info_res);
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
    ASSERT_TRUE(rpc_server.Shutdown());
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
    auto reply = stream.SendAndWait();

    EXPECT_FALSE(reply.arg_.inner_state.is_replica);
    EXPECT_TRUE(reply.arg_.inner_state.is_writing_enabled);
    EXPECT_EQ(*reply.arg_.inner_state.main_num_txns, main_num_txns);
    EXPECT_EQ(*reply.arg_.inner_state.replicas_num_txns, replicas_num_txns);
  }

  {
    // Send old version request
    auto stream = client.Stream<memgraph::coordination::StateCheckRpcV1>();
    auto reply = stream.SendAndWait();
    EXPECT_FALSE(reply.arg_.is_replica);
    EXPECT_TRUE(reply.arg_.is_writing_enabled);
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

namespace memgraph::replication {
// Old-version typed RPCs so the test can send a genuine V1/V2 SystemRecoveryReq on the wire (the
// SystemRecoveryRpc alias always serializes at the latest version, V3). Same pattern as
// GetDatabaseHistoriesRpcV1 above.
using SystemRecoveryRpcV1 = rpc::RequestResponse<SystemRecoveryReqV1, SystemRecoveryResV1>;
using SystemRecoveryRpcV2 = rpc::RequestResponse<SystemRecoveryReqV2, SystemRecoveryResV2>;
}  // namespace memgraph::replication

TEST(RpcVersioning, SystemRecoveryRpc_V1AndV2Request_UpgradeOnServer) {
  Endpoint const endpoint{"localhost", port};

  ServerContext server_context;
  Server rpc_server{endpoint, &server_context, /* workers */ 1};
  auto const on_exit = memgraph::utils::OnScopeExit{[&rpc_server] {
    ASSERT_TRUE(rpc_server.Shutdown());
    rpc_server.AwaitShutdown();
  }};

  // Written in the (single) server worker, read after SendAndWait returns — the response read
  // establishes the happens-before, matching the SystemRecoveryRpc_V3Request_CarriesColdSet test below.
  uint64_t seen_version = 0;
  bool cold_defaulted_empty = false;
  bool params_defaulted_empty = false;
  rpc_server.Register<memgraph::replication::SystemRecoveryRpc>(
      [&](std::optional<memgraph::rpc::FileReplicationHandler> const & /*file_replication_handler*/,
          uint64_t const request_version,
          auto *req_reader,
          auto *res_builder) {
        memgraph::replication::SystemRecoveryReq req;
        memgraph::rpc::LoadWithUpgrade(req, request_version, req_reader);
        seen_version = request_version;
        cold_defaulted_empty = req.cold_databases.empty();  // V3-only field, absent on the old wire
        params_defaulted_empty = req.parameters.empty();    // V2+ field
        memgraph::replication::SystemRecoveryRes const res(memgraph::replication::SystemRecoveryRes::Result::SUCCESS);
        memgraph::rpc::SendFinalResponse(res, request_version, res_builder);
      });

  ASSERT_TRUE(rpc_server.Start());
  std::this_thread::sleep_for(100ms);

  ClientContext client_context;
  Client client{endpoint, &client_context};
  {
    // V1 request (pre-parameters, pre-hot/cold): 7-arg ctor -> wire version 1 -> upgraded V1->V2->V3.
    auto stream =
        client.Stream<memgraph::replication::SystemRecoveryRpcV1>(memgraph::utils::UUID{},
                                                                  0,
                                                                  std::vector<memgraph::storage::SalientConfig>{},
                                                                  memgraph::auth::Auth::Config{},
                                                                  std::vector<memgraph::auth::User>{},
                                                                  std::vector<memgraph::auth::Role>{},
                                                                  std::vector<memgraph::auth::UserProfiles::Profile>{});
    auto reply = stream.SendAndWait();
    EXPECT_EQ(reply.result, memgraph::replication::SystemRecoveryResV1::Result::SUCCESS);
    EXPECT_EQ(seen_version, 1U) << "the server must observe the V1 wire version and run the upgrade chain";
    EXPECT_TRUE(cold_defaulted_empty) << "V3 cold_databases must default empty when upgrading a V1 request";
    EXPECT_TRUE(params_defaulted_empty) << "V2 parameters must default empty when upgrading a V1 request";
  }
  {
    // V2 request (has parameters, pre-hot/cold): 8-arg ctor -> wire version 2 -> upgraded V2->V3.
    auto stream =
        client.Stream<memgraph::replication::SystemRecoveryRpcV2>(memgraph::utils::UUID{},
                                                                  0,
                                                                  std::vector<memgraph::storage::SalientConfig>{},
                                                                  memgraph::auth::Auth::Config{},
                                                                  std::vector<memgraph::auth::User>{},
                                                                  std::vector<memgraph::auth::Role>{},
                                                                  std::vector<memgraph::auth::UserProfiles::Profile>{},
                                                                  std::vector<memgraph::parameters::ParameterInfo>{});
    auto reply = stream.SendAndWait();
    EXPECT_EQ(reply.result, memgraph::replication::SystemRecoveryResV2::Result::SUCCESS);
    EXPECT_EQ(seen_version, 2U) << "the server must observe the V2 wire version and run the upgrade chain";
    EXPECT_TRUE(cold_defaulted_empty) << "V3 cold_databases must default empty when upgrading a V2 request";
  }
  {
    auto stream = client.Stream<memgraph::replication::SystemRecoveryRpc>(
        memgraph::utils::UUID{},
        0,
        std::vector<memgraph::storage::SalientConfig>{},
        memgraph::auth::Auth::Config{},
        std::vector<memgraph::auth::User>{},
        std::vector<memgraph::auth::Role>{},
        std::vector<memgraph::auth::UserProfiles::Profile>{},
        std::vector<memgraph::parameters::ParameterInfo>{},
        std::vector<memgraph::storage::ColdTenantRecovery>{},
        std::vector<memgraph::utils::UUID>{memgraph::utils::UUID{}, memgraph::utils::UUID{}});
    auto reply = stream.SendAndWait();
    EXPECT_EQ(reply.result, memgraph::replication::SystemRecoveryRes::Result::SUCCESS);
  }
}

// SystemRecoveryRpc V3 (hot/cold): the COLD set (a vector of ColdTenantRecovery: salient + StorageInfo)
// survives the SLK round-trip, including the StorageInfo serializer.
TEST(RpcVersioning, SystemRecoveryRpc_V3Request_CarriesColdSet) {
  Endpoint const endpoint{"localhost", port};

  ServerContext server_context;
  Server rpc_server{endpoint, &server_context, /* workers */ 1};
  auto const on_exit = memgraph::utils::OnScopeExit{[&rpc_server] {
    ASSERT_TRUE(rpc_server.Shutdown());
    rpc_server.AwaitShutdown();
  }};

  std::vector<memgraph::storage::ColdTenantRecovery> seen_cold;
  rpc_server.Register<memgraph::replication::SystemRecoveryRpc>(
      [&](std::optional<memgraph::rpc::FileReplicationHandler> const & /*file_replication_handler*/,
          uint64_t const request_version,
          auto *req_reader,
          auto *res_builder) {
        memgraph::replication::SystemRecoveryReq req;
        memgraph::rpc::LoadWithUpgrade(req, request_version, req_reader);
        seen_cold = req.cold_databases;
        memgraph::replication::SystemRecoveryRes const res(memgraph::replication::SystemRecoveryRes::Result::SUCCESS);
        memgraph::rpc::SendFinalResponse(res, request_version, res_builder);
      });

  ASSERT_TRUE(rpc_server.Start());
  std::this_thread::sleep_for(100ms);

  ClientContext client_context;
  Client client{endpoint, &client_context};

  memgraph::storage::ColdTenantRecovery cold{};
  cold.salient.name = "cold_tenant";
  cold.salient.uuid = memgraph::utils::UUID{};
  cold.salient.storage_mode = memgraph::storage::StorageMode::IN_MEMORY_TRANSACTIONAL;
  cold.stats.vertex_count = 123;
  cold.stats.edge_count = 456;
  cold.stats.average_degree = 2.5;
  cold.stats.memory_res = 7890;
  cold.stats.storage_mode = memgraph::storage::StorageMode::IN_MEMORY_TRANSACTIONAL;
  cold.stats.isolation_level = memgraph::storage::IsolationLevel::SNAPSHOT_ISOLATION;
  cold.stats.durability_wal_enabled = true;
  cold.stats.schema_vertex_count = 9;

  auto stream =
      client.Stream<memgraph::replication::SystemRecoveryRpc>(memgraph::utils::UUID{},
                                                              0,
                                                              std::vector<memgraph::storage::SalientConfig>{},
                                                              memgraph::auth::Auth::Config{},
                                                              std::vector<memgraph::auth::User>{},
                                                              std::vector<memgraph::auth::Role>{},
                                                              std::vector<memgraph::auth::UserProfiles::Profile>{},
                                                              std::vector<memgraph::parameters::ParameterInfo>{},
                                                              std::vector{cold});
  auto reply = stream.SendAndWait();
  EXPECT_EQ(reply.result, memgraph::replication::SystemRecoveryRes::Result::SUCCESS);

  ASSERT_EQ(seen_cold.size(), 1U);
  EXPECT_EQ(*seen_cold[0].salient.name.str_view(), "cold_tenant");
  EXPECT_EQ(seen_cold[0].salient.storage_mode, memgraph::storage::StorageMode::IN_MEMORY_TRANSACTIONAL);
  EXPECT_EQ(seen_cold[0].stats.vertex_count, 123U);
  EXPECT_EQ(seen_cold[0].stats.edge_count, 456U);
  EXPECT_DOUBLE_EQ(seen_cold[0].stats.average_degree, 2.5);
  EXPECT_EQ(seen_cold[0].stats.memory_res, 7890U);
  EXPECT_EQ(seen_cold[0].stats.storage_mode, memgraph::storage::StorageMode::IN_MEMORY_TRANSACTIONAL);
  EXPECT_EQ(seen_cold[0].stats.isolation_level, memgraph::storage::IsolationLevel::SNAPSHOT_ISOLATION);
  EXPECT_TRUE(seen_cold[0].stats.durability_wal_enabled);
  EXPECT_EQ(seen_cold[0].stats.schema_vertex_count, 9U);
}
