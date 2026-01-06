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

#include <cstdint>
#include <map>
#include <optional>
#include <string_view>

#include "dbms/coordinator_handler.hpp"

import memgraph.coordination.coordinator_communication_config;
import memgraph.coordination.coordinator_ops_status;

namespace memgraph::dbms {

CoordinatorHandler::CoordinatorHandler(coordination::CoordinatorState &coordinator_state)
    : coordinator_state_(coordinator_state) {}

auto CoordinatorHandler::RegisterReplicationInstance(coordination::DataInstanceConfig const &config)
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

auto CoordinatorHandler::ForceResetClusterState() -> coordination::ReconcileClusterStateStatus {
  // Query is called ForceResetClusterState but internally we have function which verifies and corrects
  // cluster state
  return coordinator_state_.ReconcileClusterState();
}

auto CoordinatorHandler::YieldLeadership() const -> coordination::YieldLeadershipStatus {
  return coordinator_state_.YieldLeadership();
}

auto CoordinatorHandler::SetCoordinatorSetting(std::string_view const setting_name,
                                               std::string_view const setting_value) const
    -> coordination::SetCoordinatorSettingStatus {
  return coordinator_state_.SetCoordinatorSetting(setting_name, setting_value);
}

auto CoordinatorHandler::ShowCoordinatorSettings() const -> std::vector<std::pair<std::string, std::string>> {
  return coordinator_state_.ShowCoordinatorSettings();
}

auto CoordinatorHandler::ShowReplicationLag() const
    -> std::map<std::string, std::map<std::string, coordination::ReplicaDBLagData>> {
  return coordinator_state_.ShowReplicationLag();
}

auto CoordinatorHandler::ShowInstance() const -> coordination::InstanceStatus {
  return coordinator_state_.ShowInstance();
}

auto CoordinatorHandler::ShowInstances() const -> std::vector<coordination::InstanceStatus> {
  return coordinator_state_.ShowInstances();
}

auto CoordinatorHandler::AddCoordinatorInstance(coordination::CoordinatorInstanceConfig const &config)
    -> coordination::AddCoordinatorInstanceStatus {
  return coordinator_state_.AddCoordinatorInstance(config);
}

auto CoordinatorHandler::RemoveCoordinatorInstance(int32_t coordinator_id)
    -> coordination::RemoveCoordinatorInstanceStatus {
  return coordinator_state_.RemoveCoordinatorInstance(coordinator_id);
}

auto CoordinatorHandler::GetLeaderCoordinatorData() const -> std::optional<coordination::LeaderCoordinatorData> {
  return coordinator_state_.GetLeaderCoordinatorData();
}

}  // namespace memgraph::dbms

#endif
