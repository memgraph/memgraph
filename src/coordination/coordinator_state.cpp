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

#include "coordination/coordinator_state.hpp"

#include "coordination/coordinator_communication_config.hpp"
#include "coordination/coordinator_instance.hpp"
#include "coordination/register_main_replica_coordinator_status.hpp"
#include "spdlog/spdlog.h"
#include "utils/logging.hpp"
#include "utils/variant_helpers.hpp"

#include <optional>
#include <string_view>
#include <variant>

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

auto CoordinatorState::RegisterReplicationInstance(CoordinatorToReplicaConfig const &config)
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

auto CoordinatorState::AddCoordinatorInstance(coordination::CoordinatorToCoordinatorConfig const &config)
    -> AddCoordinatorInstanceStatus {
  MG_ASSERT(std::holds_alternative<CoordinatorInstance>(data_),
            "Coordinator cannot register replica since variant holds wrong alternative");
  return std::get<CoordinatorInstance>(data_).AddCoordinatorInstance(config);
}

auto CoordinatorState::GetRoutingTable() -> RoutingTable {
  MG_ASSERT(std::holds_alternative<CoordinatorInstance>(data_),
            "Coordinator cannot get routing table since variant holds wrong alternative");
  return std::get<CoordinatorInstance>(data_).GetRoutingTable();
}

auto CoordinatorState::GetLeaderCoordinatorData() const -> std::optional<coordination::CoordinatorToCoordinatorConfig> {
  MG_ASSERT(std::holds_alternative<CoordinatorInstance>(data_),
            "Coordinator cannot get leader coordinator data since variant holds wrong alternative");
  return std::get<CoordinatorInstance>(data_).GetLeaderCoordinatorData();
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
