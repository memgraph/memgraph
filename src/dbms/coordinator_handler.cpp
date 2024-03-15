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

#include "dbms/coordinator_handler.hpp"

#include "coordination/register_main_replica_coordinator_status.hpp"

namespace memgraph::dbms {

CoordinatorHandler::CoordinatorHandler(coordination::CoordinatorState &coordinator_state)
    : coordinator_state_(coordinator_state) {}

auto CoordinatorHandler::RegisterReplicationInstance(coordination::CoordinatorClientConfig const &config)
    -> coordination::RegisterInstanceCoordinatorStatus {
  return coordinator_state_.RegisterReplicationInstance(config);
}

auto CoordinatorHandler::UnregisterReplicationInstance(std::string_view instance_name)
    -> coordination::UnregisterInstanceCoordinatorStatus {
  return coordinator_state_.UnregisterReplicationInstance(instance_name);
}

auto CoordinatorHandler::SetReplicationInstanceToMain(std::string_view instance_name)
    -> coordination::SetInstanceToMainCoordinatorStatus {
  return coordinator_state_.SetReplicationInstanceToMain(instance_name);
}

auto CoordinatorHandler::ShowInstances() const -> std::vector<coordination::InstanceStatus> {
  return coordinator_state_.ShowInstances();
}

auto CoordinatorHandler::AddCoordinatorInstance(uint32_t raft_server_id, uint32_t raft_port,
                                                std::string_view raft_address) -> void {
  coordinator_state_.AddCoordinatorInstance(raft_server_id, raft_port, raft_address);
}

}  // namespace memgraph::dbms

#endif
