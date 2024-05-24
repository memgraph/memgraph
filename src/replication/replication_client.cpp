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

#include "replication/replication_client.hpp"
#include "io/network/endpoint.hpp"
#include "io/network/fmt.hpp"

namespace memgraph::replication {

static auto CreateClientContext(const memgraph::replication::ReplicationClientConfig &config)
    -> communication::ClientContext {
  return (config.ssl) ? communication::ClientContext{config.ssl->key_file, config.ssl->cert_file}
                      : communication::ClientContext{};
}

ReplicationClient::ReplicationClient(const memgraph::replication::ReplicationClientConfig &config)
    : name_{config.name},
      rpc_context_{CreateClientContext(config)},
      rpc_client_{config.repl_server_endpoint, &rpc_context_},
      replica_check_frequency_{config.replica_check_frequency},
      mode_{config.mode} {}

ReplicationClient::~ReplicationClient() {
  try {
    auto const &endpoint = rpc_client_.Endpoint();
    spdlog::trace("Closing replication client on {}", endpoint);
  } catch (...) {
    // Logging can throw. Not a big deal, just ignore.
  }
  thread_pool_.ShutDown();
}

}  // namespace memgraph::replication
