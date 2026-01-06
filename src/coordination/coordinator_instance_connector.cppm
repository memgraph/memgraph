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

#include <optional>
#include <string_view>
#include <vector>

#include "coordination/coordinator_rpc.hpp"

#include "spdlog/spdlog.h"

export module memgraph.coordination.coordinator_instance_connector;

#ifdef MG_ENTERPRISE

import memgraph.coordination.coordinator_communication_config;
import memgraph.coordination.coordinator_instance_client;
import memgraph.coordination.instance_status;
import memgraph.coordination.utils;

export namespace memgraph::coordination {

class CoordinatorInstanceConnector {
 public:
  explicit CoordinatorInstanceConnector(ManagementServerConfig const &config) : client_{config} {}

  auto SendShowInstances() const -> std::optional<std::vector<InstanceStatus>>;

  auto SendGetRoutingTable(std::string_view db_name) const -> std::optional<RoutingTable>;

 private:
  mutable CoordinatorInstanceClient client_;
};
}  // namespace memgraph::coordination

module : private;

namespace memgraph::coordination {

auto CoordinatorInstanceConnector::SendShowInstances() const -> std::optional<std::vector<InstanceStatus>> {
  try {
    spdlog::trace("Sending ShowInstancesRPC to endpoint {}", client_.RpcClient().Endpoint().SocketAddress());
    auto stream{client_.RpcClient().Stream<ShowInstancesRpc>()};
    auto res = stream.SendAndWait();
    return res.instances_status_;
  } catch (std::exception const &e) {
    spdlog::error("Failed to send ShowInstancesRPC: {}", e.what());
    return std::nullopt;
  }
}

auto CoordinatorInstanceConnector::SendGetRoutingTable(std::string_view const db_name) const
    -> std::optional<RoutingTable> {
  try {
    auto stream{client_.RpcClient().Stream<GetRoutingTableRpc>(std::string{db_name})};
    auto res = stream.SendAndWait();
    return res.routing_table_;
  } catch (std::exception const &e) {
    spdlog::error("Failed to receive response to GetRoutingTableRpc: {}", e.what());
    return std::nullopt;
  }
}

}  // namespace memgraph::coordination

#endif
