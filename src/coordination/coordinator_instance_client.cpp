// Copyright 2026 Memgraph Ltd.
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

namespace {
auto CreateClientContext(std::optional<memgraph::utils::TlsConfig> const &tls_config)
    -> memgraph::communication::ClientContext {
  return tls_config.has_value() ? memgraph::communication::ClientContext{tls_config->key_file, tls_config->cert_file}
                                : memgraph::communication::ClientContext{};
}

}  // namespace

namespace memgraph::coordination {

CoordinatorInstanceClient::CoordinatorInstanceClient(ManagementServerConfig const &config,
                                                     std::optional<utils::TlsConfig> const &tls_config)
    : rpc_context_(CreateClientContext(tls_config)), rpc_client_{config.endpoint, &rpc_context_} {}

auto CoordinatorInstanceClient::RpcClient() const -> rpc::Client & { return rpc_client_; }
}  // namespace memgraph::coordination
#endif
