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

#include "gmock/gmock.h"
#include "gtest/gtest.h"

#include <atomic>

#include "rpc_messages.hpp"

#include "replication_handler/system_rpc.hpp"
#include "rpc/client.hpp"
#include "rpc/file_replication_handler.hpp"
#include "rpc/server.hpp"
#include "rpc/utils.hpp"

import memgraph.coordination.coordinator_rpc;
// Needs to be included last so that SLK definitions are seen

using memgraph::communication::ClientContext;
using memgraph::communication::ServerContext;
using memgraph::coordination::DemoteMainToReplicaRpc;
using memgraph::coordination::EnableWritingOnMainRpc;
using memgraph::coordination::GetDatabaseHistoriesRpc;
using memgraph::coordination::PromoteToMainRpc;
using memgraph::coordination::RegisterReplicaOnMainRpc;
using memgraph::coordination::ShowInstancesRpc;
using memgraph::coordination::StateCheckRpc;
using memgraph::coordination::UnregisterReplicaRpc;
using memgraph::io::network::Endpoint;
using memgraph::replication::SystemRecoveryRpc;
using memgraph::replication_coordination_glue::FrequentHeartbeatRpc;
using memgraph::replication_coordination_glue::SwapMainUUIDRpc;
using memgraph::rpc::Client;
using memgraph::rpc::GenericRpcFailedException;
using memgraph::rpc::Server;
using memgraph::slk::Load;
using memgraph::storage::replication::HeartbeatRpc;

using namespace std::string_view_literals;
using namespace std::literals::chrono_literals;

namespace {
constexpr int port{8181};
std::atomic_bool rpc_akn{false};
}  // namespace

// RPC client is setup with timeout but shouldn't be triggered.
TEST(RpcTimeout, TimeoutNoFailure) {
  Endpoint endpoint{"localhost", port};

  ServerContext server_context;
  Server rpc_server{endpoint, &server_context, /* workers */ 1};
  auto const on_exit = memgraph::utils::OnScopeExit{[&rpc_server] {
    rpc_server.Shutdown();
    rpc_server.AwaitShutdown();
  }};

  rpc_server.Register<Echo>(
      [](std::optional<memgraph::rpc::FileReplicationHandler> const & /*file_replication_handler*/,
         uint64_t const request_version, auto *req_reader, auto *res_builder) {
        EchoMessage req;
        memgraph::rpc::LoadWithUpgrade(req, request_version, req_reader);

        EchoMessage res{"Sending reply"};
        memgraph::rpc::SendFinalResponse(res, request_version, res_builder);
      });

  ASSERT_TRUE(rpc_server.Start());
  std::this_thread::sleep_for(100ms);

  auto const rpc_timeouts = std::unordered_map{std::make_pair("EchoMessage"sv, 2000)};
  ClientContext client_context;
  Client client{endpoint, &client_context, rpc_timeouts};

  auto stream = client.Stream<Echo>("Sending request");
  auto reply = stream.SendAndWait();
  EXPECT_EQ(reply.data, "Sending reply");
}

// Simulate something long executing on server.
TEST(RpcTimeout, TimeoutExecutionBlocks) {
  Endpoint endpoint{"localhost", port};

  ServerContext server_context;
  Server rpc_server{endpoint, &server_context, /* workers */ 1};
  auto const on_exit = memgraph::utils::OnScopeExit{[&rpc_server] {
    rpc_server.Shutdown();
    rpc_server.AwaitShutdown();
  }};

  rpc_server.Register<Echo>(
      [](std::optional<memgraph::rpc::FileReplicationHandler> const & /*file_replication_handler*/,
         uint64_t const request_version, auto *req_reader, auto *res_builder) {
        EchoMessage req;
        memgraph::rpc::LoadWithUpgrade(req, request_version, req_reader);

        std::this_thread::sleep_for(1100ms);
        EchoMessage res{"Sending reply"};
        memgraph::rpc::SendFinalResponse(res, request_version, res_builder);
      });

  ASSERT_TRUE(rpc_server.Start());
  std::this_thread::sleep_for(100ms);

  auto const rpc_timeouts = std::unordered_map{std::make_pair("EchoMessage"sv, 1000)};
  ClientContext client_context;
  Client client{endpoint, &client_context, rpc_timeouts};

  auto stream = client.Stream<Echo>("Sending request");
  EXPECT_THROW(stream.SendAndWait(), GenericRpcFailedException);
}

// Simulate server with one thread being busy processing other RPC message.
TEST(RpcTimeout, TimeoutServerBusy) {
  Endpoint endpoint{"localhost", port};

  ServerContext server_context;
  Server rpc_server{endpoint, &server_context, /* workers */ 1};
  auto const on_exit = memgraph::utils::OnScopeExit{[&rpc_server] {
    rpc_server.Shutdown();
    rpc_server.AwaitShutdown();
  }};

  rpc_server.Register<Sum>([](std::optional<memgraph::rpc::FileReplicationHandler> const & /*file_replication_handler*/,
                              uint64_t const request_version, auto *req_reader, auto *res_builder) {
    spdlog::trace("Received sum request.");
    SumReq req;
    memgraph::rpc::LoadWithUpgrade(req, request_version, req_reader);
    std::this_thread::sleep_for(2500ms);
    auto const sum = std::accumulate(req.nums_.begin(), req.nums_.end(), 0);
    SumRes res({sum});
    memgraph::rpc::SendFinalResponse(res, request_version, res_builder);
  });

  rpc_server.Register<Echo>(
      [](std::optional<memgraph::rpc::FileReplicationHandler> const & /*file_replication_handler*/,
         uint64_t const request_version, auto *req_reader, auto *res_builder) {
        spdlog::trace("Received echo request");
        EchoMessage req;
        memgraph::rpc::LoadWithUpgrade(req, request_version, req_reader);

        EchoMessage res{"Sending reply"};
        memgraph::rpc::SendFinalResponse(res, request_version, res_builder);
      });

  ASSERT_TRUE(rpc_server.Start());
  std::this_thread::sleep_for(100ms);

  auto const rpc_timeouts = std::unordered_map{std::make_pair("EchoMessage"sv, 1000)};
  ClientContext sum_client_context;
  Client sum_client{endpoint, &sum_client_context};

  ClientContext echo_client_context;
  Client echo_client{endpoint, &echo_client_context, rpc_timeouts};

  // Sum request won't timeout but Echo should timeout because server has only one
  // processing thread.
  auto sum_stream = sum_client.Stream<SumV1>(10, 10);
  auto echo_stream = echo_client.Stream<Echo>("Sending request");
  // Don't block main test thread so echo_stream could timeout
  auto sum_thread_ = std::jthread([&sum_stream]() { sum_stream.SendAndWait(); });
  // Wait so that server receives first SumReq and then EchoMessage
  std::this_thread::sleep_for(100ms);
  EXPECT_THROW(echo_stream.SendAndWait(), GenericRpcFailedException);
}

TEST(RpcTimeout, SendingToWrongSocket) {
  Endpoint endpoint{"localhost", port};

  ServerContext server_context;
  Server rpc_server{endpoint, &server_context, /* workers */ 1};
  auto const on_exit = memgraph::utils::OnScopeExit{[&rpc_server] {
    rpc_server.Shutdown();
    rpc_server.AwaitShutdown();
  }};

  rpc_server.Register<Echo>(
      [](std::optional<memgraph::rpc::FileReplicationHandler> const & /*file_replication_handler*/,
         uint64_t const request_version, auto *req_reader, auto *res_builder) {
        EchoMessage req;
        memgraph::rpc::LoadWithUpgrade(req, request_version, req_reader);

        std::this_thread::sleep_for(1100ms);
        EchoMessage res{"Sending reply"};
        memgraph::rpc::SendFinalResponse(res, request_version, res_builder);
      });

  ASSERT_TRUE(rpc_server.Start());
  std::this_thread::sleep_for(100ms);

  auto const rpc_timeouts = std::unordered_map{std::make_pair("EchoMessage"sv, 1000)};
  ClientContext client_context;
  Client client{endpoint, &client_context, rpc_timeouts};

  auto stream = client.Stream<Echo>("Sending request");
  EXPECT_THROW(stream.SendAndWait(), GenericRpcFailedException);
}

template <memgraph::rpc::IsRpc T>
void RegisterRpcCallback(Server &rpc_server) {
  rpc_server.Register<T>([](std::optional<memgraph::rpc::FileReplicationHandler> const & /*file_replication_handler*/,
                            uint64_t const request_version, auto *req_reader, auto /* *res_builder */) {
    typename T::Request req;
    if constexpr (!std::is_same_v<T, EnableWritingOnMainRpc> && !std::is_same_v<T, GetDatabaseHistoriesRpc>) {
      memgraph::rpc::LoadWithUpgrade(req, request_version, req_reader);
    }
    rpc_akn.wait(true);    // Wait for the timeout
    rpc_akn.store(false);  // Reset to signal handler is finished
  });
}

template <memgraph::rpc::IsRpc T>
void SendAndAssert(Client &client) {
  rpc_akn.store(false);
  auto stream = client.Stream<T>();
  EXPECT_THROW(stream.SendAndWait(), GenericRpcFailedException);
  rpc_akn.store(true);  // Signal the timeout occurred
  rpc_akn.wait(false);  // Wait for the reset
}

TEST(RpcTimeout, Timeouts) {
  Endpoint endpoint{"localhost", port};
  ServerContext server_context;
  Server rpc_server{endpoint, &server_context, /* workers */ 2};
  auto const on_exit = memgraph::utils::OnScopeExit{[&rpc_server] {
    rpc_server.Shutdown();
    rpc_server.AwaitShutdown();
  }};

  RegisterRpcCallback<ShowInstancesRpc>(rpc_server);
  RegisterRpcCallback<DemoteMainToReplicaRpc>(rpc_server);
  RegisterRpcCallback<PromoteToMainRpc>(rpc_server);
  RegisterRpcCallback<RegisterReplicaOnMainRpc>(rpc_server);
  RegisterRpcCallback<UnregisterReplicaRpc>(rpc_server);
  RegisterRpcCallback<EnableWritingOnMainRpc>(rpc_server);
  RegisterRpcCallback<GetDatabaseHistoriesRpc>(rpc_server);
  RegisterRpcCallback<StateCheckRpc>(rpc_server);
  RegisterRpcCallback<SwapMainUUIDRpc>(rpc_server);
  RegisterRpcCallback<FrequentHeartbeatRpc>(rpc_server);
  RegisterRpcCallback<HeartbeatRpc>(rpc_server);
  RegisterRpcCallback<SystemRecoveryRpc>(rpc_server);

  ASSERT_TRUE(rpc_server.Start());

  auto const rpc_timeouts = std::unordered_map{
      std::make_pair("ShowInstancesReq"sv, 50),
      std::make_pair("DemoteMainToReplicaReq"sv, 50),
      std::make_pair("PromoteToMainReq"sv, 50),
      std::make_pair("RegisterReplicaOnMainReq"sv, 50),
      std::make_pair("UnregisterReplicaReq"sv, 50),
      std::make_pair("EnableWritingOnMainReq"sv, 50),
      std::make_pair("GetDatabaseHistoriesReq"sv, 50),
      std::make_pair("StateCheckReq"sv, 50),
      std::make_pair("SwapMainUUIDReq"sv, 50),
      std::make_pair("FrequentHeartbeatReq"sv, 50),
      std::make_pair("HeartbeatReq"sv, 50),
      std::make_pair("SystemRecoveryReq"sv, 50),

  };

  ClientContext client_context;
  Client client{endpoint, &client_context, rpc_timeouts};

  SendAndAssert<ShowInstancesRpc>(client);
  SendAndAssert<DemoteMainToReplicaRpc>(client);
  SendAndAssert<PromoteToMainRpc>(client);
  SendAndAssert<UnregisterReplicaRpc>(client);
  SendAndAssert<EnableWritingOnMainRpc>(client);
  SendAndAssert<GetDatabaseHistoriesRpc>(client);
  SendAndAssert<StateCheckRpc>(client);
  SendAndAssert<SwapMainUUIDRpc>(client);
  SendAndAssert<FrequentHeartbeatRpc>(client);
  SendAndAssert<HeartbeatRpc>(client);
  SendAndAssert<SystemRecoveryRpc>(client);
}
