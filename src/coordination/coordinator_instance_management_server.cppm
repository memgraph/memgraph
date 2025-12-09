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

module;

#include "communication/context.hpp"
#include "rpc/server.hpp"

export module memgraph.coordination.coordinator_instance_management_server;

import memgraph.coordination.coordinator_communication_config;

#ifdef MG_ENTERPRISE
export namespace memgraph::coordination {

class CoordinatorInstanceManagementServer {
 public:
  explicit CoordinatorInstanceManagementServer(const ManagementServerConfig &config);
  CoordinatorInstanceManagementServer(const CoordinatorInstanceManagementServer &) = delete;
  CoordinatorInstanceManagementServer(CoordinatorInstanceManagementServer &&) = delete;
  CoordinatorInstanceManagementServer &operator=(const CoordinatorInstanceManagementServer &) = delete;
  CoordinatorInstanceManagementServer &operator=(CoordinatorInstanceManagementServer &&) = delete;

  ~CoordinatorInstanceManagementServer();

  bool Start();

  template <typename TRequestResponse, typename F>
  void Register(F &&callback) {
    rpc_server_.Register<TRequestResponse>(std::forward<F>(callback));
  }

 private:
  communication::ServerContext rpc_server_context_;
  rpc::Server rpc_server_;
};
}  // namespace memgraph::coordination

module : private;
namespace memgraph::coordination {

namespace {

// NOTE: The coordinator server doesn't need more than 1 processing thread - it's not a bottleneck
constexpr auto kCoordInstanceManagementServerThreads = 1;

}  // namespace

CoordinatorInstanceManagementServer::CoordinatorInstanceManagementServer(const ManagementServerConfig &config)
    : rpc_server_context_{communication::ServerContext{}},
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
