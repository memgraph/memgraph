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

#pragma once

#ifdef MG_ENTERPRISE

#include <optional>
#include <string_view>
#include <variant>

#include "coordination/data_instance_management_server.hpp"

#include "nlohmann/json_fwd.hpp"

import memgraph.coordination.coordinator_communication_config;
import memgraph.coordination.coordinator_ops_status;
import memgraph.coordination.coordinator_instance;
import memgraph.coordination.instance_status;
import memgraph.coordination.replication_lag_info;
import memgraph.coordination.utils;

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

  [[nodiscard]] auto RegisterReplicationInstance(DataInstanceConfig const &config) -> RegisterInstanceCoordinatorStatus;
  [[nodiscard]] auto UnregisterReplicationInstance(std::string_view instance_name)
      -> UnregisterInstanceCoordinatorStatus;

  [[nodiscard]] auto DemoteInstanceToReplica(std::string_view instance_name) -> DemoteInstanceCoordinatorStatus;

  [[nodiscard]] auto ReconcileClusterState() -> ReconcileClusterStateStatus;

  [[nodiscard]] auto SetReplicationInstanceToMain(std::string_view instance_name) -> SetInstanceToMainCoordinatorStatus;

  [[nodiscard]] auto ShowInstance() const -> InstanceStatus;

  [[nodiscard]] auto ShowInstances() const -> std::vector<InstanceStatus>;

  auto AddCoordinatorInstance(CoordinatorInstanceConfig const &config) const -> AddCoordinatorInstanceStatus;

  auto RemoveCoordinatorInstance(int32_t coordinator_id) const -> RemoveCoordinatorInstanceStatus;

  auto SetCoordinatorSetting(std::string_view setting_name, std::string_view setting_value) const
      -> SetCoordinatorSettingStatus;

  auto ShowCoordinatorSettings() const -> std::vector<std::pair<std::string, std::string>>;

  auto ShowReplicationLag() const -> std::map<std::string, std::map<std::string, ReplicaDBLagData>>;

  [[nodiscard]] auto GetLeaderCoordinatorData() const -> std::optional<LeaderCoordinatorData>;

  auto YieldLeadership() const -> YieldLeadershipStatus;

  // NOTE: The client code must check that the server exists before calling this method.
  auto GetDataInstanceManagementServer() const -> DataInstanceManagementServer &;

  auto GetRoutingTable(std::string_view db_name) const -> RoutingTable;

  auto GetTelemetryJson() const -> nlohmann::json;

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
