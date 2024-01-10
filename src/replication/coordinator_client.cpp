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

#include "replication/coordinator_client.hpp"
#include "replication/coordinator_config.hpp"

namespace memgraph::replication {

#ifdef MG_ENTERPRISE
static auto CreateClientContext(const memgraph::replication::CoordinatorClientConfig &config)
    -> communication::ClientContext {
  return (config.ssl) ? communication::ClientContext{config.ssl->key_file, config.ssl->cert_file}
                      : communication::ClientContext{};
}

CoordinatorClient::CoordinatorClient(const memgraph::replication::CoordinatorClientConfig &config)
    : name_{config.name},
      rpc_context_{CreateClientContext(config)},
      rpc_client_{io::network::Endpoint(io::network::Endpoint::needs_resolving, config.ip_address, config.port),
                  &rpc_context_},
      replica_check_frequency_{config.replica_check_frequency} {}

CoordinatorClient::~CoordinatorClient() {
  auto endpoint = rpc_client_.Endpoint();
  try {
    spdlog::trace("Closing replication client on {}:{}", endpoint.address, endpoint.port);
  } catch (...) {
    // Logging can throw. Not a big deal, just ignore.
  }
  thread_pool_.Shutdown();
}

void CoordinatorClient::StartFrequentCheck() {
  MG_ASSERT(replica_check_frequency_ > std::chrono::seconds(0), "Replica check frequency must be greater than 0");
  replica_checker_.Run("Coord checker", replica_check_frequency_, [this] {
    try {
      {
        auto stream{rpc_client_.Stream<memgraph::replication::FrequentHeartbeatRpc>()};
        stream.AwaitResponse();
      }
    } catch (const rpc::RpcFailedException &) {
      // Nothing to do...wait for a reconnect
    }
  });
}

bool CoordinatorClient::DoHealthCheck() const {
  try {
    {
      // this should be different RPC message?
      // doesn't have to be because there is lock taken in RPC request but we can do it
      // TODO:(andi) What is the timeout of this message? New message would be better...
      // Since the end goal is to create automatic failover, how to realize when the
      // instance is down? Because when we figure out this then we won't need to send
      // explicit frequent heartbeat message.
      auto stream{rpc_client_.Stream<memgraph::replication::FrequentHeartbeatRpc>()};
      stream.AwaitResponse();
      return true;
    }
  } catch (const rpc::RpcFailedException &) {
    // Nothing to do...wait for a reconnect
  }
  return false;
}
#endif
}  // namespace memgraph::replication
