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
#include "gmock/gmock.h"
#include "gtest/gtest.h"

#ifdef MG_ENTERPRISE

#include "coordination/coordinator_instance_management_server.hpp"
#include "coordination/data_instance_management_server.hpp"
#include "replication_coordination_glue/messages.hpp"
#include "rpc/client.hpp"
#include "rpc/file_replication_handler.hpp"
#include "rpc/utils.hpp"
#include "utils/tls.hpp"

#include <chrono>
#include <unordered_map>

namespace fs = std::filesystem;

using memgraph::communication::ClientContext;
using memgraph::coordination::CoordinatorInstanceManagementServer;
using memgraph::coordination::DataInstanceManagementServer;
using memgraph::coordination::ManagementServerConfig;
using memgraph::io::network::Endpoint;
using memgraph::replication_coordination_glue::SwapMainUUIDReq;
using memgraph::replication_coordination_glue::SwapMainUUIDRes;
using memgraph::replication_coordination_glue::SwapMainUUIDRpc;
using memgraph::rpc::Client;
using memgraph::rpc::RpcFailedException;
using memgraph::rpc::RpcFailedToConnectException;
using memgraph::utils::TlsConfig;
using memgraph::utils::UUID;

using namespace std::string_view_literals;

namespace {

// Per-server-type port (avoids EADDRINUSE between consecutive cases under TIME_WAIT).
template <typename T>
struct ServerPort;

template <>
struct ServerPort<DataInstanceManagementServer> {
  static constexpr int value = 8187;
};

template <>
struct ServerPort<CoordinatorInstanceManagementServer> {
  static constexpr int value = 8188;
};

auto MakeSwapHandler() {
  return [](std::optional<memgraph::rpc::FileReplicationHandler> const & /*file_replication_handler*/,
            uint64_t const request_version,
            auto *req_reader,
            auto *res_builder) {
    SwapMainUUIDReq req;
    memgraph::slk::Load(&req, req_reader);
    SwapMainUUIDRes res{true};
    memgraph::rpc::SendFinalResponse(res, request_version, res_builder);
  };
}

fs::path CertsDir() { return fs::path{boost::dll::program_location().parent_path().string()} / "tls_certs"; }

TlsConfig MakeTlsConfig(std::string const &instance) {
  auto const dir = CertsDir();
  return TlsConfig{.key_file = (dir / (instance + ".key")).string(),
                   .cert_file = (dir / (instance + ".crt")).string(),
                   .ca_file = (dir / "ca.crt").string()};
}

}  // namespace

template <typename T>
class ManagementServerTest : public ::testing::Test {
 protected:
  static constexpr int port = ServerPort<T>::value;
};

using ServerTypes = ::testing::Types<DataInstanceManagementServer, CoordinatorInstanceManagementServer>;
TYPED_TEST_SUITE(ManagementServerTest, ServerTypes);

TYPED_TEST(ManagementServerTest, NoTlsConnection) {
  ManagementServerConfig cfg{Endpoint("0.0.0.0", this->port)};
  TypeParam server(cfg, std::nullopt);
  server.template Register<SwapMainUUIDRpc>(MakeSwapHandler());
  ASSERT_TRUE(server.Start());

  ClientContext client_context;
  Client client(Endpoint("0.0.0.0", this->port), &client_context);
  auto stream = client.template Stream<SwapMainUUIDRpc>(UUID{});
  EXPECT_NO_THROW(stream.SendAndWait());
}

TYPED_TEST(ManagementServerTest, TlsClientToNoTlsServer) {
  // Server uses instance2, client uses instance1 — both signed by the shared CA.
  ManagementServerConfig cfg{Endpoint("0.0.0.0", this->port)};
  TypeParam server(cfg, std::nullopt);
  server.template Register<SwapMainUUIDRpc>(MakeSwapHandler());
  ASSERT_TRUE(server.Start());

  auto const client_tls = MakeTlsConfig("instance1");
  ClientContext client_context(client_tls.key_file, client_tls.cert_file, client_tls.ca_file);
  auto const rpc_timeouts = std::unordered_map{std::make_pair("SwapMainUUIDReq"sv, 500)};
  Client client(Endpoint("0.0.0.0", this->port), &client_context, rpc_timeouts, std::chrono::milliseconds{500});
  EXPECT_THROW(client.template Stream<SwapMainUUIDRpc>(UUID{}), RpcFailedToConnectException);
}

TYPED_TEST(ManagementServerTest, TlsClientTlsServer) {
  // Server uses instance2, client uses instance1 — both signed by the shared CA.
  ManagementServerConfig cfg{Endpoint("0.0.0.0", this->port)};
  TypeParam server(cfg, MakeTlsConfig("instance2"));
  server.template Register<SwapMainUUIDRpc>(MakeSwapHandler());
  ASSERT_TRUE(server.Start());

  auto const client_tls = MakeTlsConfig("instance1");
  ClientContext client_context(client_tls.key_file, client_tls.cert_file, client_tls.ca_file);
  auto const rpc_timeouts = std::unordered_map{std::make_pair("SwapMainUUIDReq"sv, 500)};
  Client client(Endpoint("0.0.0.0", this->port), &client_context, rpc_timeouts);
  auto stream = client.template Stream<SwapMainUUIDRpc>(UUID{});
  EXPECT_NO_THROW(stream.SendAndWait());
}

TYPED_TEST(ManagementServerTest, TlsServerNoTlsClient) {
  ManagementServerConfig cfg{Endpoint("0.0.0.0", this->port)};
  TypeParam server(cfg, MakeTlsConfig("instance2"));
  server.template Register<SwapMainUUIDRpc>(MakeSwapHandler());
  ASSERT_TRUE(server.Start());

  // Plain client — TLS server should reject the raw SLK bytes.
  ClientContext client_context;
  Client client(Endpoint("0.0.0.0", this->port), &client_context);
  auto stream = client.template Stream<SwapMainUUIDRpc>(UUID{});
  EXPECT_THROW(stream.SendAndWait(), RpcFailedException);
}

#endif  // MG_ENTERPRISE
