// Copyright 2024 Memgraph Ltd.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt; by using this file, you agree to be bound by the terms of the Business Source
// License, and you may not use this file except in compliance with the Business Source License.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

#include "coordination/coordinator_instance_management_server.hpp"
#include "replication_coordination_glue/handler.hpp"

#include <spdlog/spdlog.h>

namespace memgraph::coordination {
#ifdef MG_ENTERPRISE
namespace {

auto CreateServerContext(const memgraph::coordination::CoordinatorInstanceManagementServerConfig &config)
    -> communication::ServerContext {
  return (config.ssl) ? communication::ServerContext{config.ssl->key_file, config.ssl->cert_file, config.ssl->ca_file,
                                                     config.ssl->verify_peer}
                      : communication::ServerContext{};
}

// NOTE: The coordinator server doesn't more than 1 processing thread - each replica can
// have only a single coordinator server. Also, the single-threaded guarantee
// simplifies the rest of the implementation.
constexpr auto kCoordInstanceManagementServerThreads = 1;

}  // namespace

CoordinatorInstanceManagementServer::CoordinatorInstanceManagementServer(
    const CoordinatorInstanceManagementServerConfig &config)
    : rpc_server_context_{CreateServerContext(config)},
      rpc_server_{config.endpoint, &rpc_server_context_, kCoordInstanceManagementServerThreads} {
  rpc_server_.Register<replication_coordination_glue::FrequentHeartbeatRpc>([](auto *req_reader, auto *res_builder) {
    spdlog::debug("Received FrequentHeartbeatRpc on coordinator instance management server");
    replication_coordination_glue::FrequentHeartbeatHandler(req_reader, res_builder);
  });
}

CoordinatorInstanceManagementServer::~CoordinatorInstanceManagementServer() {
  if (rpc_server_.IsRunning()) {
    auto const &endpoint = rpc_server_.endpoint();
    spdlog::trace("Closing coordinator instance management server on {}", endpoint.SocketAddress());
    rpc_server_.Shutdown();
  }
  rpc_server_.AwaitShutdown();
}

bool CoordinatorInstanceManagementServer::Start() { return rpc_server_.Start(); }

#endif
}  // namespace memgraph::coordination