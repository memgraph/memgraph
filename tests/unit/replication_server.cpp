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

#include "replication/config.hpp"
#include "replication/replication_server.hpp"
#include "replication_coordination_glue/messages.hpp"
#include "rpc/client.hpp"
#include "utils/tls.hpp"

#include <unordered_map>

namespace fs = std::filesystem;

using memgraph::communication::ClientContext;
using memgraph::io::network::Endpoint;
using memgraph::replication::ReplicationServer;
using memgraph::replication::ReplicationServerConfig;
using memgraph::replication_coordination_glue::FrequentHeartbeatRpc;
using memgraph::rpc::Client;
using memgraph::rpc::RpcFailedException;
using memgraph::utils::TlsConfig;

using namespace std::string_view_literals;

constexpr int port{8186};

TEST(ReplicationServer, NoTlsConnection) {
  ReplicationServerConfig cfg{.repl_server = Endpoint("0.0.0.0", port)};
  ReplicationServer server(cfg);
  ASSERT_TRUE(server.Start());

  // Configured without TLS
  ClientContext client_conntext;
  Client client(Endpoint("0.0.0.0", port), &client_conntext);
  auto stream = client.Stream<FrequentHeartbeatRpc>();
  EXPECT_NO_THROW(stream.SendAndWait());
}

// TODO: (andi) Need to handle SSL_connect timeout
/*
TEST(ReplicationServer, TlsClientToNoTlsServer) {
  ReplicationServerConfig cfg{.repl_server = Endpoint("0.0.0.0", port)};
  ReplicationServer server(cfg);
  ASSERT_TRUE(server.Start());

  // Configure TLS client
  auto const certs_dir = fs::path{boost::dll::program_location().parent_path().string()} / "tls_certs";
  auto const key_file = (certs_dir / "instance1.key").string();
  auto const cert_file = (certs_dir / "instance1.crt").string();
  auto const ca_file = (certs_dir / "ca.crt").string();
  ClientContext client_conntext(key_file, cert_file, ca_file);
  auto const rpc_timeouts = std::unordered_map{std::make_pair("FrequentHeartbeatReq"sv, 500)};
  Client client(Endpoint("0.0.0.0", port), &client_conntext, rpc_timeouts);
  auto stream = client.Stream<FrequentHeartbeatRpc>();
  EXPECT_THROW(stream.SendAndWait(), RpcFailedException);
}
*/

TEST(ReplicationServer, TlsClientTlsServer) {
  // Test that instance1 can connect to instance2
  auto const certs_dir = fs::path{boost::dll::program_location().parent_path().string()} / "tls_certs";
  auto const key2_file = (certs_dir / "instance2.key").string();
  auto const cert2_file = (certs_dir / "instance2.crt").string();
  auto const ca_file = (certs_dir / "ca.crt").string();
  TlsConfig tls_config{.key_file = key2_file, .cert_file = cert2_file, .ca_file = ca_file};
  ReplicationServerConfig cfg{.repl_server = Endpoint("0.0.0.0", port), .tls_config = std::move(tls_config)};
  ReplicationServer server(cfg);
  ASSERT_TRUE(server.Start());

  // Configure TLS client
  auto const key1_file = (certs_dir / "instance1.key").string();
  auto const cert1_file = (certs_dir / "instance1.crt").string();
  ClientContext client_conntext(key1_file, cert1_file, ca_file);
  auto const rpc_timeouts = std::unordered_map{std::make_pair("FrequentHeartbeatReq"sv, 500)};
  Client client(Endpoint("0.0.0.0", port), &client_conntext, rpc_timeouts);
  auto stream = client.Stream<FrequentHeartbeatRpc>();
  EXPECT_NO_THROW(stream.SendAndWait());
}

TEST(ReplicationServer, TlsServerNoTlsClient) {
  // Test that instance1 can connect to instance2
  auto const certs_dir = fs::path{boost::dll::program_location().parent_path().string()} / "tls_certs";
  auto const key2_file = (certs_dir / "instance2.key").string();
  auto const cert2_file = (certs_dir / "instance2.crt").string();
  auto const ca_file = (certs_dir / "ca.crt").string();
  TlsConfig tls_config{.key_file = key2_file, .cert_file = cert2_file, .ca_file = ca_file};
  ReplicationServerConfig cfg{.repl_server = Endpoint("0.0.0.0", port), .tls_config = std::move(tls_config)};
  ReplicationServer server(cfg);
  ASSERT_TRUE(server.Start());

  // Configure TLS client
  // Configured without TLS
  ClientContext client_conntext;
  Client client(Endpoint("0.0.0.0", port), &client_conntext);
  auto stream = client.Stream<FrequentHeartbeatRpc>();
  EXPECT_THROW(stream.SendAndWait(), RpcFailedException);
}
