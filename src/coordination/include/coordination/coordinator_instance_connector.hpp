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

#include "coordination/coordinator_instance_client.hpp"
#include "coordination/coordinator_rpc.hpp"
#include "coordination/instance_status.hpp"
#include "coordination/utils.hpp"

namespace memgraph::coordination {

class CoordinatorInstanceConnector {
 public:
  explicit CoordinatorInstanceConnector(ManagementServerConfig const &config) : client_{config} {}

  auto SendShowInstances() const -> std::optional<std::vector<InstanceStatus>>;

  auto SendGetRoutingTable() const -> RoutingTable;

 private:
  mutable CoordinatorInstanceClient client_;
};

}  // namespace memgraph::coordination
#endif
