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

#pragma once

#ifdef MG_ENTERPRISE

#include <optional>
#include <string_view>

#include "coordination/coordinator_communication_config.hpp"
#include "coordination/coordinator_instance.hpp"
#include "coordination/data_instance_management_server.hpp"
#include "coordination/instance_status.hpp"
#include "coordination/register_main_replica_coordinator_status.hpp"

#include <variant>

namespace memgraph::coordination {

class CoordinatorState {
 public:
  explicit CoordinatorState(CoordinatorInstanceInitConfig const &config);
  explicit CoordinatorState(ReplicationInstanceInitConfig const &config);
  ~CoordinatorState() = default;

  CoordinatorState(CoordinatorState const &) = delete;
  CoordinatorState &operator=(CoordinatorState const &) = delete;

  CoordinatorState(CoordinatorState &&) noexcept = delete;
  CoordinatorState &operator=(CoordinatorState &&) noexcept = delete;

  [[nodiscard]] auto RegisterReplicationInstance(CoordinatorToReplicaConfig const &config)
      -> RegisterInstanceCoordinatorStatus;
  [[nodiscard]] auto UnregisterReplicationInstance(std::string_view instance_name)
      -> UnregisterInstanceCoordinatorStatus;

  [[nodiscard]] auto DemoteInstanceToReplica(std::string_view instance_name) -> DemoteInstanceCoordinatorStatus;

  [[nodiscard]] auto ReconcileClusterState() -> ReconcileClusterStateStatus;

  [[nodiscard]] auto SetReplicationInstanceToMain(std::string_view instance_name) -> SetInstanceToMainCoordinatorStatus;

  [[nodiscard]] auto ShowInstances() const -> std::vector<InstanceStatus>;

  auto AddCoordinatorInstance(coordination::CoordinatorToCoordinatorConfig const &config)
      -> AddCoordinatorInstanceStatus;

  [[nodiscard]] auto GetLeaderCoordinatorData() const -> std::optional<coordination::CoordinatorToCoordinatorConfig>;

  // NOTE: The client code must check that the server exists before calling this method.
  auto GetDataInstanceManagementServer() const -> DataInstanceManagementServer &;

  auto GetRoutingTable() -> RoutingTable;

  [[nodiscard]] auto IsCoordinator() const -> bool;
  [[nodiscard]] auto IsDataInstance() const -> bool;

  void ShutDownCoordinator();

 private:
  struct CoordinatorMainReplicaData {
    std::unique_ptr<DataInstanceManagementServer> data_instance_management_server_;
  };

  std::variant<CoordinatorMainReplicaData, CoordinatorInstance> data_;
};

}  // namespace memgraph::coordination
#endif
