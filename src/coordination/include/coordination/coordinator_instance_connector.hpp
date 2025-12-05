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

#pragma once

#include <optional>
#include <string_view>
#include <vector>

import memgraph.coordination.coordinator_communication_config;
import memgraph.coordination.coordinator_instance_client;
import memgraph.coordination.instance_status;
import memgraph.coordination.utils;

namespace memgraph::coordination {

// TODO: (andi) Good candidate for module private fragment
class CoordinatorInstanceConnector {
 public:
  explicit CoordinatorInstanceConnector(ManagementServerConfig const &config) : client_{config} {}

  auto SendShowInstances() const -> std::optional<std::vector<InstanceStatus>>;

  auto SendGetRoutingTable(std::string_view db_name) const -> std::optional<RoutingTable>;

 private:
  mutable CoordinatorInstanceClient client_;
};

}  // namespace memgraph::coordination
#endif
