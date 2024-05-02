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
#include "coordination/coordinator_state.hpp"
#include "coordination/instance_status.hpp"
#include "coordination/register_main_replica_coordinator_status.hpp"

#include <vector>

namespace memgraph::dbms {

class DbmsHandler;

class CoordinatorHandler {
 public:
  explicit CoordinatorHandler(coordination::CoordinatorState &coordinator_state);

  // TODO: (andi) When moving coordinator state on same instances, rename from RegisterReplicationInstance to
  // RegisterInstance
  auto RegisterReplicationInstance(coordination::CoordinatorToReplicaConfig const &config)
      -> coordination::RegisterInstanceCoordinatorStatus;

  auto UnregisterReplicationInstance(std::string_view instance_name)
      -> coordination::UnregisterInstanceCoordinatorStatus;

  auto SetReplicationInstanceToMain(std::string_view instance_name) -> coordination::SetInstanceToMainCoordinatorStatus;

  auto DemoteInstanceToReplica(std::string_view instance_name) -> coordination::DemoteInstanceCoordinatorStatus;

  auto ForceResetClusterState() -> coordination::ForceResetClusterStateStatus;

  auto ShowInstances() const -> std::vector<coordination::InstanceStatus>;

  auto AddCoordinatorInstance(coordination::CoordinatorToCoordinatorConfig const &config)
      -> coordination::AddCoordinatorInstanceStatus;

  auto GetLeaderCoordinatorData() const -> std::optional<coordination::CoordinatorToCoordinatorConfig>;

 private:
  coordination::CoordinatorState &coordinator_state_;
};

}  // namespace memgraph::dbms
#endif
