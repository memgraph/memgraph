// Copyright 2023 Memgraph Ltd.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt; by using this file, you agree to be bound by the terms of the Business Source
// License, and you may not use this file except in compliance with the Business Source License.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

#include "replication_server.hpp"
#include "io/network/endpoint.hpp"
#include "replication/config.hpp"
#include "rpc.hpp"

namespace memgraph::storage {
namespace {

auto CreateServerContext(const memgraph::replication::ReplicationServerConfig &config) -> communication::ServerContext {
  return (config.ssl) ? communication::ServerContext{config.ssl->key_file, config.ssl->cert_file, config.ssl->ca_file,
                                                     config.ssl->verify_peer}
                      : communication::ServerContext{};
}

// NOTE: The replication server must have a single thread for processing
// because there is no need for more processing threads - each replica can
// have only a single main server. Also, the single-threaded guarantee
// simplifies the rest of the implementation.
constexpr auto kReplictionServerThreads = 1;
}  // namespace

ReplicationServer::ReplicationServer(const memgraph::replication::ReplicationServerConfig &config)
    : rpc_server_context_{CreateServerContext(config)},
      rpc_server_{io::network::Endpoint{config.ip_address, config.port}, &rpc_server_context_,
                  kReplictionServerThreads} {
  rpc_server_.Register<replication::FrequentHeartbeatRpc>([](auto *req_reader, auto *res_builder) {
    spdlog::debug("Received FrequentHeartbeatRpc");
    FrequentHeartbeatHandler(req_reader, res_builder);
  });
}

ReplicationServer::~ReplicationServer() {
  if (rpc_server_.IsRunning()) {
    auto const &endpoint = rpc_server_.endpoint();
    spdlog::trace("Closing replication server on {}:{}", endpoint.address, endpoint.port);
    rpc_server_.Shutdown();
  }
  rpc_server_.AwaitShutdown();
}

bool ReplicationServer::Start() { return rpc_server_.Start(); }

void ReplicationServer::FrequentHeartbeatHandler(slk::Reader *req_reader, slk::Builder *res_builder) {
  replication::FrequentHeartbeatReq req;
  slk::Load(&req, req_reader);
  replication::FrequentHeartbeatRes res{true};
  slk::Save(res, res_builder);
}

}  // namespace memgraph::storage
