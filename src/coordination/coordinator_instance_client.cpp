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

#include "coordination/coordinator_instance_client.hpp"

#include "coordination/coordinator_communication_config.hpp"

namespace {
auto CreateClientContext(memgraph::coordination::ManagementServerConfig const &config)
    -> memgraph::communication::ClientContext {
  return (config.ssl) ? memgraph::communication::ClientContext{config.ssl->key_file, config.ssl->cert_file}
                      : memgraph::communication::ClientContext{};
}
}  // namespace

namespace memgraph::coordination {

CoordinatorInstanceClient::CoordinatorInstanceClient(ManagementServerConfig const &config)
    : rpc_context_{CreateClientContext(config)},
      rpc_client_{config.endpoint, &rpc_context_, config.rpc_connection_timeout_sec} {}

auto CoordinatorInstanceClient::RpcClient() -> rpc::Client & { return rpc_client_; }
}  // namespace memgraph::coordination
#endif
