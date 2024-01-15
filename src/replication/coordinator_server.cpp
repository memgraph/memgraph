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

#include "replication/coordinator_server.hpp"

#include "replication/messages.hpp"
#include "replication/replication_utils.hpp"

#ifdef MG_ENTERPRISE
namespace memgraph::replication {

namespace {

// TODO: (andi) Do I need this to be 1
// NOTE: The replication server must have a single thread for processing
// because there is no need for more processing threads - each replica can
// have only a single main server. Also, the single-threaded guarantee
// simplifies the rest of the implementation.
constexpr auto kReplicationServerThreads = 1;
}  // namespace

CoordinatorServer::CoordinatorServer(const memgraph::replication::ReplicationServerConfig &config)
    : rpc_server_context_{CreateServerContext(config)},
      rpc_server_{io::network::Endpoint{config.ip_address, config.port}, &rpc_server_context_,
                  kReplicationServerThreads} {
  rpc_server_.Register<FrequentHeartbeatRpc>([](auto *req_reader, auto *res_builder) {
    spdlog::debug("Received FrequentHeartbeatRpc on coordinator server");
    FrequentHeartbeatHandler(req_reader, res_builder);
  });
}

CoordinatorServer::~CoordinatorServer() {
  if (rpc_server_.IsRunning()) {
    auto const &endpoint = rpc_server_.endpoint();
    spdlog::trace("Closing coordinator server on {}:{}", endpoint.address, endpoint.port);
    rpc_server_.Shutdown();
  }
  rpc_server_.AwaitShutdown();
}

// TODO: (andi) Do I need this method
bool CoordinatorServer::Start() { return rpc_server_.Start(); }

}  // namespace memgraph::replication
#endif
