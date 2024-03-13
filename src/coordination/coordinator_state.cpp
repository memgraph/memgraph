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

#include "coordination/coordinator_config.hpp"
#include "coordination/register_main_replica_coordinator_status.hpp"
#include "flags/replication.hpp"
#include "spdlog/spdlog.h"
#include "utils/logging.hpp"
#include "utils/variant_helpers.hpp"

#include <algorithm>

namespace memgraph::coordination {

CoordinatorState::CoordinatorState() {
  MG_ASSERT(!(FLAGS_raft_server_id && FLAGS_coordinator_server_port),
            "Instance cannot be a coordinator and have registered coordinator server.");

  spdlog::info("Executing coordinator constructor");
  if (FLAGS_coordinator_server_port) {
    spdlog::info("Coordinator server port set");
    auto const config = CoordinatorServerConfig{
        .ip_address = kDefaultReplicationServerIp,
        .port = static_cast<uint16_t>(FLAGS_coordinator_server_port),
    };
    spdlog::info("Executing coordinator constructor main replica");

    data_ = CoordinatorMainReplicaData{.coordinator_server_ = std::make_unique<CoordinatorServer>(config)};
  }
}

auto CoordinatorState::RegisterReplicationInstance(CoordinatorClientConfig const &config)
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

auto CoordinatorState::GetCoordinatorServer() const -> CoordinatorServer & {
  MG_ASSERT(std::holds_alternative<CoordinatorMainReplicaData>(data_),
            "Cannot get coordinator server since variant holds wrong alternative");
  return *std::get<CoordinatorMainReplicaData>(data_).coordinator_server_;
}

auto CoordinatorState::AddCoordinatorInstance(uint32_t raft_server_id, io::network::Endpoint const &coordinator_server)
    -> void {
  MG_ASSERT(std::holds_alternative<CoordinatorInstance>(data_),
            "Coordinator cannot register replica since variant holds wrong alternative");
  return std::get<CoordinatorInstance>(data_).AddCoordinatorInstance(raft_server_id, coordinator_server);
}

auto CoordinatorState::GetRoutingTable() -> RoutingTable {
  MG_ASSERT(std::holds_alternative<CoordinatorInstance>(data_),
            "Coordinator cannot get routing table since variant holds wrong alternative");
  return std::get<CoordinatorInstance>(data_).GetRoutingTable();
}

}  // namespace memgraph::coordination
#endif
