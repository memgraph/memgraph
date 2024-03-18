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

#include "coordination/coordinator_instance.hpp"
#include "coordination/coordinator_server.hpp"
#include "coordination/instance_status.hpp"
#include "coordination/register_main_replica_coordinator_status.hpp"

#include <variant>

namespace memgraph::coordination {

struct ReplicationInstanceInitConfig {
  uint32_t management_port{0};
};

struct CoordinatorInstanceInitConfig {
  uint32_t raft_server_id{0};
  uint32_t coordinator_port{0};
};

struct InstanceInitConfig {
  std::variant<ReplicationInstanceInitConfig, CoordinatorInstanceInitConfig> data_;
};

class CoordinatorState {
 public:
  explicit CoordinatorState(InstanceInitConfig const &config);
  ~CoordinatorState() = default;

  CoordinatorState(CoordinatorState const &) = delete;
  CoordinatorState &operator=(CoordinatorState const &) = delete;

  CoordinatorState(CoordinatorState &&) noexcept = delete;
  CoordinatorState &operator=(CoordinatorState &&) noexcept = delete;

  [[nodiscard]] auto RegisterReplicationInstance(CoordinatorToReplicaConfig const &config)
      -> RegisterInstanceCoordinatorStatus;
  [[nodiscard]] auto UnregisterReplicationInstance(std::string_view instance_name)
      -> UnregisterInstanceCoordinatorStatus;

  [[nodiscard]] auto SetReplicationInstanceToMain(std::string_view instance_name) -> SetInstanceToMainCoordinatorStatus;

  auto ShowInstances() const -> std::vector<InstanceStatus>;

  auto AddCoordinatorInstance(coordination::CoordinatorToCoordinatorConfig const &config) -> void;

  // NOTE: The client code must check that the server exists before calling this method.
  auto GetCoordinatorServer() const -> CoordinatorServer &;

  auto GetRoutingTable(std::map<std::string, std::string> const &routing) -> RoutingTable;

 private:
  struct CoordinatorMainReplicaData {
    std::unique_ptr<CoordinatorServer> coordinator_server_;
  };

  std::variant<CoordinatorInstance, CoordinatorMainReplicaData> data_;
};

}  // namespace memgraph::coordination
#endif
