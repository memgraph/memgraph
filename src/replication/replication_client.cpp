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

#include "replication/replication_client.hpp"

namespace memgraph::replication {

static auto CreateClientContext(const memgraph::replication::ReplicationClientConfig &config)
    -> communication::ClientContext {
  return (config.ssl) ? communication::ClientContext{config.ssl->key_file, config.ssl->cert_file}
                      : communication::ClientContext{};
}

ReplicationClient::ReplicationClient(const memgraph::replication::ReplicationClientConfig &config)
    : name_{config.name},
      rpc_context_{CreateClientContext(config)},
      rpc_client_{io::network::Endpoint(io::network::Endpoint::needs_resolving, config.ip_address, config.port),
                  &rpc_context_},
      replica_check_frequency_{config.replica_check_frequency},
      mode_{config.mode} {}

ReplicationClient::~ReplicationClient() {
  auto endpoint = rpc_client_.Endpoint();
  try {
    spdlog::trace("Closing replication client on {}:{}", endpoint.address, endpoint.port);
  } catch (...) {
    // Logging can throw. Not a big deal, just ignore.
  }
  thread_pool_.Shutdown();
}

}  // namespace memgraph::replication
