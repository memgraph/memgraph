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

#include "boost/dll/runtime_symbol_info.hpp"
#include "gtest/gtest.h"

#include <atomic>
#include <filesystem>

#include "rpc_messages.hpp"

#include "communication/context.hpp"
#include "communication/init.hpp"
#include "coordination/coordinator_rpc.hpp"
#include "replication_handler/system_rpc.hpp"
#include "rpc/client.hpp"
#include "rpc/file_replication_handler.hpp"
#include "rpc/server.hpp"
#include "rpc/utils.hpp"  // Needs to be included last so that SLK definitions are seen
#include "utils/tls.hpp"

using memgraph::communication::ClientContext;
using memgraph::communication::CreateClientContext;
using memgraph::communication::CreateServerContext;
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
using memgraph::rpc::RpcTimeoutException;
using memgraph::rpc::Server;
using memgraph::slk::Load;
using memgraph::storage::replication::HeartbeatRpc;
using memgraph::utils::TlsConfig;

using namespace std::string_view_literals;
using namespace std::literals::chrono_literals;
namespace fs = std::filesystem;

namespace {

std::atomic_bool rpc_akn{false};

// Initialize OpenSSL once for the whole test binary and ignore SIGPIPE. Without this, the TLS
// timeout tests crash: once the client closes its socket on timeout, the server's SSL_write
// hits a broken pipe and SIGPIPE terminates the process (the BIO layer calls plain write()
// without MSG_NOSIGNAL, so the process-wide handler is the only place to neutralize it).
memgraph::communication::SSLInit ssl_init;

fs::path CertsDir() { return fs::path{boost::dll::program_location().parent_path().string()} / "tls_certs"; }

TlsConfig MakeTlsConfig(std::string const &instance) {
  auto const dir = CertsDir();
  return TlsConfig{.key_file = (dir / (instance + ".key")).string(),
                   .cert_file = (dir / (instance + ".crt")).string(),
                   .ca_file = (dir / "ca.crt").string()};
}

struct PlainMode {
  [[maybe_unused]] static constexpr int kPort = 8189;

  static ServerContext MakeServerCtx() { return CreateServerContext(std::nullopt); }

  static ClientContext MakeClientCtx() { return CreateClientContext(std::nullopt); }
};

struct TlsMode {
  [[maybe_unused]] static constexpr int kPort = 8190;

  static ServerContext MakeServerCtx() { return CreateServerContext(MakeTlsConfig("instance2")); }

  static ClientContext MakeClientCtx() { return CreateClientContext(MakeTlsConfig("instance1")); }
};

}  // namespace

template <typename T>
class RpcTimeoutTest : public ::testing::Test {};

using Modes = ::testing::Types<PlainMode, TlsMode>;
TYPED_TEST_SUITE(RpcTimeoutTest, Modes);

// RPC client is setup with timeout but shouldn't be triggered.
TYPED_TEST(RpcTimeoutTest, TimeoutNoFailure) {
  Endpoint endpoint{"localhost", TypeParam::kPort};

  auto server_context = TypeParam::MakeServerCtx();
  Server rpc_server{endpoint, &server_context, /* workers */ 1};
  auto const on_exit = memgraph::utils::OnScopeExit{[&rpc_server] {
    ASSERT_TRUE(rpc_server.Shutdown());
    rpc_server.AwaitShutdown();
  }};

  rpc_server.template Register<Echo>(
      [](std::optional<memgraph::rpc::FileReplicationHandler> const & /*file_replication_handler*/,
         uint64_t const request_version,
         auto *req_reader,
         auto *res_builder) {
        EchoMessage req;
        memgraph::rpc::LoadWithUpgrade(req, request_version, req_reader);

        EchoMessage res{"Sending reply"};
        memgraph::rpc::SendFinalResponse(res, request_version, res_builder);
      });

  ASSERT_TRUE(rpc_server.Start());
  std::this_thread::sleep_for(100ms);

  auto const rpc_timeouts = std::unordered_map{std::make_pair("EchoMessage"sv, 2000)};
  auto client_context = TypeParam::MakeClientCtx();
  Client client{endpoint, &client_context, rpc_timeouts};

  auto stream = client.template Stream<Echo>("Sending request");
  auto reply = stream.SendAndWait();
  EXPECT_EQ(reply.data, "Sending reply");
}

// Simulate something long executing on server.
TYPED_TEST(RpcTimeoutTest, TimeoutExecutionBlocks) {
  Endpoint endpoint{"localhost", TypeParam::kPort};

  auto server_context = TypeParam::MakeServerCtx();
  Server rpc_server{endpoint, &server_context, /* workers */ 1};
  auto const on_exit = memgraph::utils::OnScopeExit{[&rpc_server] {
    ASSERT_TRUE(rpc_server.Shutdown());
    rpc_server.AwaitShutdown();
  }};

  rpc_server.template Register<Echo>(
      [](std::optional<memgraph::rpc::FileReplicationHandler> const & /*file_replication_handler*/,
         uint64_t const request_version,
         auto *req_reader,
         auto *res_builder) {
        EchoMessage req;
        memgraph::rpc::LoadWithUpgrade(req, request_version, req_reader);

        std::this_thread::sleep_for(1100ms);
        EchoMessage res{"Sending reply"};
        memgraph::rpc::SendFinalResponse(res, request_version, res_builder);
      });

  ASSERT_TRUE(rpc_server.Start());
  std::this_thread::sleep_for(100ms);

  auto const rpc_timeouts = std::unordered_map{std::make_pair("EchoMessage"sv, 200)};
  auto client_context = TypeParam::MakeClientCtx();
  Client client{endpoint, &client_context, rpc_timeouts};

  auto stream = client.template Stream<Echo>("Sending request");
  EXPECT_THROW(stream.SendAndWait(), RpcTimeoutException);
}

// Simulate server with one thread being busy processing other RPC message.
TYPED_TEST(RpcTimeoutTest, TimeoutServerBusy) {
  Endpoint endpoint{"localhost", TypeParam::kPort};

  auto server_context = TypeParam::MakeServerCtx();
  Server rpc_server{endpoint, &server_context, /* workers */ 1};
  auto const on_exit = memgraph::utils::OnScopeExit{[&rpc_server] {
    ASSERT_TRUE(rpc_server.Shutdown());
    rpc_server.AwaitShutdown();
  }};

  rpc_server.template Register<Sum>(
      [](std::optional<memgraph::rpc::FileReplicationHandler> const & /*file_replication_handler*/,
         uint64_t const request_version,
         auto *req_reader,
         auto *res_builder) {
        spdlog::trace("Received sum request.");
        SumReq req;
        memgraph::rpc::LoadWithUpgrade(req, request_version, req_reader);
        std::this_thread::sleep_for(2500ms);
        auto const sum = std::accumulate(req.nums_.begin(), req.nums_.end(), 0);
        SumRes res({sum});
        memgraph::rpc::SendFinalResponse(res, request_version, res_builder);
      });

  rpc_server.template Register<Echo>(
      [](std::optional<memgraph::rpc::FileReplicationHandler> const & /*file_replication_handler*/,
         uint64_t const request_version,
         auto *req_reader,
         auto *res_builder) {
        spdlog::trace("Received echo request");
        EchoMessage req;
        memgraph::rpc::LoadWithUpgrade(req, request_version, req_reader);

        EchoMessage res{"Sending reply"};
        memgraph::rpc::SendFinalResponse(res, request_version, res_builder);
      });

  ASSERT_TRUE(rpc_server.Start());
  std::this_thread::sleep_for(100ms);

  auto const rpc_timeouts = std::unordered_map{std::make_pair("EchoMessage"sv, 2000)};
  auto sum_client_context = TypeParam::MakeClientCtx();
  Client sum_client{endpoint, &sum_client_context};

  auto echo_client_context = TypeParam::MakeClientCtx();
  Client echo_client{endpoint, &echo_client_context, rpc_timeouts};

  // Sum request won't timeout but Echo should timeout because server has only one
  // processing thread.
  auto sum_stream = sum_client.template Stream<SumV1>(10, 10);
  auto echo_stream = echo_client.template Stream<Echo>("Sending request");
  // Don't block main test thread so echo_stream could timeout
  auto sum_thread_ = std::jthread([&sum_stream]() { sum_stream.SendAndWait(); });
  // Wait so the server picks up SumReq before EchoMessage. Margin must cover both
  // TLS handshakes (one per client) when running the TlsMode instantiation.
  std::this_thread::sleep_for(300ms);
  EXPECT_THROW(echo_stream.SendAndWait(), RpcTimeoutException);
}

TYPED_TEST(RpcTimeoutTest, SendingToWrongSocket) {
  Endpoint endpoint{"localhost", TypeParam::kPort};

  auto server_context = TypeParam::MakeServerCtx();
  Server rpc_server{endpoint, &server_context, /* workers */ 1};
  auto const on_exit = memgraph::utils::OnScopeExit{[&rpc_server] {
    ASSERT_TRUE(rpc_server.Shutdown());
    rpc_server.AwaitShutdown();
  }};

  rpc_server.template Register<Echo>(
      [](std::optional<memgraph::rpc::FileReplicationHandler> const & /*file_replication_handler*/,
         uint64_t const request_version,
         auto *req_reader,
         auto *res_builder) {
        EchoMessage req;
        memgraph::rpc::LoadWithUpgrade(req, request_version, req_reader);

        std::this_thread::sleep_for(1100ms);
        EchoMessage res{"Sending reply"};
        memgraph::rpc::SendFinalResponse(res, request_version, res_builder);
      });

  ASSERT_TRUE(rpc_server.Start());
  std::this_thread::sleep_for(100ms);

  auto const rpc_timeouts = std::unordered_map{std::make_pair("EchoMessage"sv, 200)};
  auto client_context = TypeParam::MakeClientCtx();
  Client client{endpoint, &client_context, rpc_timeouts};

  auto stream = client.template Stream<Echo>("Sending request");
  EXPECT_THROW(stream.SendAndWait(), RpcTimeoutException);
}

template <memgraph::rpc::IsRpc T>
void RegisterRpcCallback(Server &rpc_server) {
  rpc_server.Register<T>([](std::optional<memgraph::rpc::FileReplicationHandler> const & /*file_replication_handler*/,
                            uint64_t const request_version,
                            auto *req_reader,
                            auto /* *res_builder */) {
    typename T::Request req;
    if constexpr (!std::is_same_v<T, EnableWritingOnMainRpc> && !std::is_same_v<T, GetDatabaseHistoriesRpc>) {
      memgraph::rpc::LoadWithUpgrade(req, request_version, req_reader);
    }
    rpc_akn.wait(true);    // Wait for the timeout
    rpc_akn.store(false);  // Reset to signal handler is finished
  });
}

namespace {
template <memgraph::rpc::IsRpc T>
void SendAndAssert(Client &client) {
  rpc_akn.store(false);
  auto stream = client.Stream<T>();
  EXPECT_THROW(stream.SendAndWait(), RpcTimeoutException);
  rpc_akn.store(true);  // Signal the timeout occurred
  rpc_akn.wait(false);  // Wait for the reset
}
}  // namespace

TYPED_TEST(RpcTimeoutTest, Timeouts) {
  Endpoint endpoint{"localhost", TypeParam::kPort};
  auto server_context = TypeParam::MakeServerCtx();
  Server rpc_server{endpoint, &server_context, /* workers */ 2};
  auto const on_exit = memgraph::utils::OnScopeExit{[&rpc_server] {
    ASSERT_TRUE(rpc_server.Shutdown());
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
      std::make_pair("ShowInstancesReq"sv, 200),
      std::make_pair("DemoteMainToReplicaReq"sv, 200),
      std::make_pair("PromoteToMainReq"sv, 200),
      std::make_pair("RegisterReplicaOnMainReq"sv, 200),
      std::make_pair("UnregisterReplicaReq"sv, 200),
      std::make_pair("EnableWritingOnMainReq"sv, 200),
      std::make_pair("GetDatabaseHistoriesReq"sv, 200),
      std::make_pair("StateCheckReq"sv, 200),
      std::make_pair("SwapMainUUIDReq"sv, 200),
      std::make_pair("FrequentHeartbeatReq"sv, 200),
      std::make_pair("HeartbeatReq"sv, 200),
      std::make_pair("SystemRecoveryReq"sv, 200),

  };

  auto client_context = TypeParam::MakeClientCtx();
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
