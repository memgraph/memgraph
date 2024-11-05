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
#ifdef MG_ENTERPRISE
namespace memgraph::coordination {

namespace {

auto CreateServerContext(const memgraph::coordination::ManagementServerConfig &config) -> communication::ServerContext {
  return (config.ssl) ? communication::ServerContext{config.ssl->key_file, config.ssl->cert_file, config.ssl->ca_file,
                                                     config.ssl->verify_peer}
                      : communication::ServerContext{};
}

// NOTE: The coordinator server doesn't need more than 1 processing thread - it's not a bottleneck
constexpr auto kCoordInstanceManagementServerThreads = 1;

}  // namespace

CoordinatorInstanceManagementServer::CoordinatorInstanceManagementServer(const ManagementServerConfig &config)
    : rpc_server_context_{CreateServerContext(config)},
      rpc_server_{config.endpoint, &rpc_server_context_, kCoordInstanceManagementServerThreads} {}

CoordinatorInstanceManagementServer::~CoordinatorInstanceManagementServer() {
  if (rpc_server_.IsRunning()) {
    rpc_server_.Shutdown();
  }
  rpc_server_.AwaitShutdown();
}

bool CoordinatorInstanceManagementServer::Start() { return rpc_server_.Start(); }

}  // namespace memgraph::coordination
#endif
