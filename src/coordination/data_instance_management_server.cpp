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

#ifdef MG_ENTERPRISE

#include "coordination/data_instance_management_server.hpp"
#include "coordination/coordinator_rpc.hpp"
#include "replication_coordination_glue/handler.hpp"

#include <spdlog/spdlog.h>

namespace memgraph::coordination {

namespace {

auto CreateServerContext(const memgraph::coordination::ManagementServerConfig &config) -> communication::ServerContext {
  return (config.ssl) ? communication::ServerContext{config.ssl->key_file, config.ssl->cert_file, config.ssl->ca_file,
                                                     config.ssl->verify_peer}
                      : communication::ServerContext{};
}

// NOTE: The coordinator server doesn't more than 1 processing thread - each replica can
// have only a single coordinator server. Also, the single-threaded guarantee
// simplifies the rest of the implementation.
constexpr auto kDataInstanceManagementServerThreads = 1;

}  // namespace

DataInstanceManagementServer::DataInstanceManagementServer(const ManagementServerConfig &config)
    : rpc_server_context_{CreateServerContext(config)},
      rpc_server_{config.endpoint, &rpc_server_context_, kDataInstanceManagementServerThreads} {}

DataInstanceManagementServer::~DataInstanceManagementServer() {
  if (rpc_server_.IsRunning()) {
    rpc_server_.Shutdown();
  }
  rpc_server_.AwaitShutdown();
}

bool DataInstanceManagementServer::Start() { return rpc_server_.Start(); }

}  // namespace memgraph::coordination
#endif
