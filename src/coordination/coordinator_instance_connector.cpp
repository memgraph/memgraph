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

#ifdef MG_ENTERPRISE
#include "coordination/coordinator_instance_connector.hpp"

namespace memgraph::coordination {

auto CoordinatorInstanceConnector::SendShowInstances() const -> std::optional<std::vector<InstanceStatus>> {
  try {
    spdlog::trace("Sending ShowInstancesRPC to endpoint {}", client_.RpcClient().Endpoint().SocketAddress());
    auto stream{client_.RpcClient().Stream<ShowInstancesRpc>()};
    spdlog::trace("Waiting response to ShowInstancesRpc.");
    auto res = stream.SendAndWait();
    spdlog::trace("Received ShowInstancesRPC response");
    return res.instances_status_;
  } catch (std::exception const &e) {
    spdlog::error("Failed to send ShowInstancesRPC: {}", e.what());
    return std::nullopt;
  }
}

}  // namespace memgraph::coordination
#endif
