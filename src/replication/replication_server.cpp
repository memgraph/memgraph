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

#include "replication/replication_server.hpp"
#include "replication_coordination_glue/handler.hpp"

#include <spdlog/spdlog.h>

namespace memgraph::rpc {
class FileReplicationHandler;
}  // namespace memgraph::rpc

namespace memgraph::replication {
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
constexpr auto kReplicationServerThreads = 1;
}  // namespace

ReplicationServer::ReplicationServer(const memgraph::replication::ReplicationServerConfig &config)
    : rpc_server_context_{CreateServerContext(config)},
      rpc_server_{config.repl_server, &rpc_server_context_, kReplicationServerThreads} {
  rpc_server_.Register<replication_coordination_glue::FrequentHeartbeatRpc>(
      [](std::optional<rpc::FileReplicationHandler> const & /*file_replication_handler*/,
         uint64_t const request_version, auto *req_reader, auto *res_builder) {
        replication_coordination_glue::FrequentHeartbeatHandler(request_version, req_reader, res_builder);
      });
}

ReplicationServer::~ReplicationServer() { Shutdown(); }

bool ReplicationServer::Start() { return rpc_server_.Start(); }

void ReplicationServer::Shutdown() {
  if (rpc_server_.IsRunning()) {
    try {
      // trace can throw
      auto const &endpoint = rpc_server_.endpoint();
      spdlog::trace("Closing replication server on {}", endpoint.SocketAddress());
      rpc_server_.Shutdown();
      // NOLINTNEXTLINE(bugprone-empty-catch)
    } catch (std::exception const &) {
    }
  }
  rpc_server_.AwaitShutdown();
}

}  // namespace memgraph::replication
