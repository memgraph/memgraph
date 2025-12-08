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

#include "io/network/endpoint.hpp"
#include "spdlog/spdlog.h"
#include "utils/logging.hpp"
#include "utils/variant_helpers.hpp"

#include <nlohmann/json.hpp>
#include <optional>
#include <string_view>
#include <variant>

module memgraph.coordination.coordinator_state;

#ifdef MG_ENTERPRISE

import memgraph.coordination.coordinator_communication_config;
import memgraph.coordination.coordinator_instance;
import memgraph.coordination.coordinator_ops_status;
import memgraph.coordination.data_instance_management_server;
import memgraph.coordination.replication_lag_info;
import memgraph.coordination.utils;

namespace memgraph::coordination {

CoordinatorState::CoordinatorState(CoordinatorInstanceInitConfig const &config) {
  data_.emplace<CoordinatorInstance>(config);
}

CoordinatorState::CoordinatorState(ReplicationInstanceInitConfig const &config) {
  ManagementServerConfig const mgmt_config{
      io::network::Endpoint{kDefaultManagementServerIp, static_cast<uint16_t>(config.management_port)},
  };
  data_ = CoordinatorMainReplicaData{.data_instance_management_server_ =
                                         std::make_unique<DataInstanceManagementServer>(mgmt_config)};
  spdlog::trace("Created data instance management server on address {}:{}.", mgmt_config.endpoint.GetAddress(),
                mgmt_config.endpoint.GetPort());
}

auto CoordinatorState::RegisterReplicationInstance(DataInstanceConfig const &config)
    -> RegisterInstanceCoordinatorStatus {
  MG_ASSERT(std::holds_alternative<CoordinatorInstance>(data_),
            "Coordinator cannot register replica since variant holds wrong alternative");

  return std::visit(
      memgraph::utils::Overloaded{[](const CoordinatorMainReplicaData & /*coordinator_main_replica_data*/) {
                                    return RegisterInstanceCoordinatorStatus::NOT_COORDINATOR;
                                  },
                                  [config](CoordinatorInstance &coordinator_instance) {
                                    return coordinator_instance.RegisterReplicationInstance(config);
                                  }},
      data_);
}

auto CoordinatorState::UnregisterReplicationInstance(std::string_view instance_name)
    -> UnregisterInstanceCoordinatorStatus {
  MG_ASSERT(std::holds_alternative<CoordinatorInstance>(data_),
            "Coordinator cannot unregister instance since variant holds wrong alternative");

  return std::visit(
      memgraph::utils::Overloaded{[](const CoordinatorMainReplicaData & /*coordinator_main_replica_data*/) {
                                    return UnregisterInstanceCoordinatorStatus::NOT_COORDINATOR;
                                  },
                                  [&instance_name](CoordinatorInstance &coordinator_instance) {
                                    return coordinator_instance.UnregisterReplicationInstance(instance_name);
                                  }},
      data_);
}

auto CoordinatorState::DemoteInstanceToReplica(std::string_view instance_name) -> DemoteInstanceCoordinatorStatus {
  MG_ASSERT(std::holds_alternative<CoordinatorInstance>(data_),
            "Coordinator cannot demote instance to replica since variant holds wrong alternative");

  return std::visit(
      memgraph::utils::Overloaded{[](const CoordinatorMainReplicaData & /*coordinator_main_replica_data*/) {
                                    return DemoteInstanceCoordinatorStatus::NOT_COORDINATOR;
                                  },
                                  [&instance_name](CoordinatorInstance &coordinator_instance) {
                                    return coordinator_instance.DemoteInstanceToReplica(instance_name);
                                  }},
      data_);
}

auto CoordinatorState::ReconcileClusterState() -> ReconcileClusterStateStatus {
  MG_ASSERT(std::holds_alternative<CoordinatorInstance>(data_),
            "Coordinator cannot force reset cluster state since variant holds wrong alternative.");

  return std::visit(
      memgraph::utils::Overloaded{[](const CoordinatorMainReplicaData & /*coordinator_main_replica_data*/) {
                                    spdlog::error(
                                        "Coordinator cannot force reset cluster state since it is not a "
                                        "coordinator instance.");
                                    return ReconcileClusterStateStatus::FAIL;
                                  },
                                  [](CoordinatorInstance &coordinator_instance) {
                                    return coordinator_instance.TryVerifyOrCorrectClusterState();
                                  }},
      data_);
}

auto CoordinatorState::SetReplicationInstanceToMain(std::string_view instance_name)
    -> SetInstanceToMainCoordinatorStatus {
  MG_ASSERT(std::holds_alternative<CoordinatorInstance>(data_),
            "Coordinator cannot register replica since variant holds wrong alternative");

  return std::visit(
      memgraph::utils::Overloaded{[](const CoordinatorMainReplicaData & /*coordinator_main_replica_data*/) {
                                    return SetInstanceToMainCoordinatorStatus::NOT_COORDINATOR;
                                  },
                                  [&instance_name](CoordinatorInstance &coordinator_instance) {
                                    return coordinator_instance.SetReplicationInstanceToMain(instance_name);
                                  }},
      data_);
}

auto CoordinatorState::ShowInstance() const -> InstanceStatus {
  MG_ASSERT(std::holds_alternative<CoordinatorInstance>(data_),
            "Can't call show instance on data_, as variant holds wrong alternative");
  return std::get<CoordinatorInstance>(data_).ShowInstance();
}

auto CoordinatorState::ShowInstances() const -> std::vector<InstanceStatus> {
  MG_ASSERT(std::holds_alternative<CoordinatorInstance>(data_),
            "Can't call show instances on data_, as variant holds wrong alternative");
  return std::get<CoordinatorInstance>(data_).ShowInstances();
}

auto CoordinatorState::GetDataInstanceManagementServer() const -> DataInstanceManagementServer & {
  MG_ASSERT(std::holds_alternative<CoordinatorMainReplicaData>(data_),
            "Cannot get coordinator server since variant holds wrong alternative");
  return *std::get<CoordinatorMainReplicaData>(data_).data_instance_management_server_;
}

auto CoordinatorState::AddCoordinatorInstance(CoordinatorInstanceConfig const &config) const
    -> AddCoordinatorInstanceStatus {
  MG_ASSERT(std::holds_alternative<CoordinatorInstance>(data_),
            "Coordinator cannot be added since variant holds wrong alternative");
  return std::get<CoordinatorInstance>(data_).AddCoordinatorInstance(config);
}

auto CoordinatorState::RemoveCoordinatorInstance(int32_t coordinator_id) const -> RemoveCoordinatorInstanceStatus {
  MG_ASSERT(std::holds_alternative<CoordinatorInstance>(data_),
            "Coordinator cannot be unregistered since variant holds wrong alternative");
  return std::get<CoordinatorInstance>(data_).RemoveCoordinatorInstance(coordinator_id);
}

auto CoordinatorState::SetCoordinatorSetting(std::string_view const setting_name,
                                             std::string_view const setting_value) const
    -> SetCoordinatorSettingStatus {
  MG_ASSERT(std::holds_alternative<CoordinatorInstance>(data_),
            "Coordinator's settings cannot be updated since variant holds wrong alternative");
  return std::get<CoordinatorInstance>(data_).SetCoordinatorSetting(setting_name, setting_value);
}

auto CoordinatorState::ShowCoordinatorSettings() const -> std::vector<std::pair<std::string, std::string>> {
  MG_ASSERT(std::holds_alternative<CoordinatorInstance>(data_),
            "Coordinator settings cannot be retrieved since variant holds wrong alternative");
  return std::get<CoordinatorInstance>(data_).ShowCoordinatorSettings();
}

auto CoordinatorState::ShowReplicationLag() const -> std::map<std::string, std::map<std::string, ReplicaDBLagData>> {
  return std::get<CoordinatorInstance>(data_).ShowReplicationLag();
}

auto CoordinatorState::GetRoutingTable(std::string_view db_name) const -> RoutingTable {
  MG_ASSERT(std::holds_alternative<CoordinatorInstance>(data_),
            "Coordinator cannot get routing table since variant holds wrong alternative");
  return std::get<CoordinatorInstance>(data_).GetRoutingTable(db_name);
}

auto CoordinatorState::GetLeaderCoordinatorData() const -> std::optional<coordination::LeaderCoordinatorData> {
  MG_ASSERT(std::holds_alternative<CoordinatorInstance>(data_),
            "Coordinator cannot get leader coordinator data since variant holds wrong alternative");
  return std::get<CoordinatorInstance>(data_).GetLeaderCoordinatorData();
}

auto CoordinatorState::YieldLeadership() const -> YieldLeadershipStatus {
  MG_ASSERT(std::holds_alternative<CoordinatorInstance>(data_),
            "Coordinator cannot yield leadership since variant holds wrong alternative");
  return std::get<CoordinatorInstance>(data_).YieldLeadership();
}

auto CoordinatorState::GetTelemetryJson() const -> nlohmann::json {
  MG_ASSERT(std::holds_alternative<CoordinatorInstance>(data_),
            "Coordinator cannot return telemetry json data since variant holds wrong alternative");
  return std::get<CoordinatorInstance>(data_).GetTelemetryJson();
}

auto CoordinatorState::IsCoordinator() const -> bool { return std::holds_alternative<CoordinatorInstance>(data_); }

auto CoordinatorState::IsDataInstance() const -> bool {
  return std::holds_alternative<CoordinatorMainReplicaData>(data_);
}

void CoordinatorState::ShutDownCoordinator() {
  MG_ASSERT(std::holds_alternative<CoordinatorInstance>(data_),
            "Coordinator cannot get leader coordinator data since variant holds wrong alternative");
  std::get<CoordinatorInstance>(data_).ShuttingDown();
}

}  // namespace memgraph::coordination
#endif
