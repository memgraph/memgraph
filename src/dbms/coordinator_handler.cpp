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

#include <optional>

#include "coordination/coordinator_communication_config.hpp"
#include "coordination/register_main_replica_coordinator_status.hpp"
#include "dbms/coordinator_handler.hpp"

namespace memgraph::dbms {

CoordinatorHandler::CoordinatorHandler(coordination::CoordinatorState &coordinator_state)
    : coordinator_state_(coordinator_state) {}

auto CoordinatorHandler::RegisterReplicationInstance(coordination::CoordinatorToReplicaConfig const &config)
    -> coordination::RegisterInstanceCoordinatorStatus {
  return coordinator_state_.RegisterReplicationInstance(config);
}

auto CoordinatorHandler::UnregisterReplicationInstance(std::string_view instance_name)
    -> coordination::UnregisterInstanceCoordinatorStatus {
  return coordinator_state_.UnregisterReplicationInstance(instance_name);
}

auto CoordinatorHandler::DemoteInstanceToReplica(std::string_view instance_name)
    -> coordination::DemoteInstanceCoordinatorStatus {
  return coordinator_state_.DemoteInstanceToReplica(instance_name);
}

auto CoordinatorHandler::SetReplicationInstanceToMain(std::string_view instance_name)
    -> coordination::SetInstanceToMainCoordinatorStatus {
  return coordinator_state_.SetReplicationInstanceToMain(instance_name);
}

auto CoordinatorHandler::ForceResetClusterState() -> coordination::ForceResetClusterStateStatus {
  return coordinator_state_.ForceResetClusterState();
}

auto CoordinatorHandler::ShowInstances() const -> std::vector<coordination::InstanceStatus> {
  return coordinator_state_.ShowInstances();
}

auto CoordinatorHandler::AddCoordinatorInstance(coordination::CoordinatorToCoordinatorConfig const &config)
    -> coordination::AddCoordinatorInstanceStatus {
  return coordinator_state_.AddCoordinatorInstance(config);
}

auto CoordinatorHandler::GetLeaderCoordinatorData() const
    -> std::optional<coordination::CoordinatorToCoordinatorConfig> {
  return coordinator_state_.GetLeaderCoordinatorData();
}

}  // namespace memgraph::dbms

#endif
