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

#include "coordination/coordinator_state.hpp"

#ifdef MG_ENTERPRISE

#include "coordination/coordinator_config.hpp"
#include "flags/replication.hpp"
#include "spdlog/spdlog.h"
#include "utils/logging.hpp"
#include "utils/variant_helpers.hpp"

namespace memgraph::coordination {

CoordinatorState::CoordinatorState() {
  MG_ASSERT(!(FLAGS_coordinator && FLAGS_coordinator_server_port),
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

auto CoordinatorState::RegisterReplica(CoordinatorClientConfig config) -> RegisterMainReplicaCoordinatorStatus {
  MG_ASSERT(std::holds_alternative<CoordinatorData>(data_),
            "Coordinator cannot register replica since variant holds wrong alternative");

  return std::visit(
      memgraph::utils::Overloaded{
          [](const CoordinatorMainReplicaData & /*coordinator_main_replica_data*/) {
            return RegisterMainReplicaCoordinatorStatus::NOT_COORDINATOR;
          },
          [&config](CoordinatorData &coordinator_data) { return coordinator_data.RegisterReplica(std::move(config)); }},
      data_);
}

auto CoordinatorState::RegisterMain(CoordinatorClientConfig config) -> RegisterMainReplicaCoordinatorStatus {
  MG_ASSERT(std::holds_alternative<CoordinatorData>(data_),
            "Coordinator cannot register main since variant holds wrong alternative");

  return std::visit(
      memgraph::utils::Overloaded{
          [](const CoordinatorMainReplicaData & /*coordinator_main_replica_data*/) {
            return RegisterMainReplicaCoordinatorStatus::NOT_COORDINATOR;
          },
          [&config](CoordinatorData &coordinator_data) { return coordinator_data.RegisterMain(std::move(config)); }},
      data_);
}

auto CoordinatorState::ShowReplicas() const -> std::vector<CoordinatorInstanceStatus> {
  MG_ASSERT(std::holds_alternative<CoordinatorData>(data_),
            "Can't call show replicas on data_, as variant holds wrong alternative");
  return std::get<CoordinatorData>(data_).ShowReplicas();
}

auto CoordinatorState::ShowMain() const -> std::optional<CoordinatorInstanceStatus> {
  MG_ASSERT(std::holds_alternative<CoordinatorData>(data_),
            "Can't call show main on data_, as variant holds wrong alternative");
  return std::get<CoordinatorData>(data_).ShowMain();
};

[[nodiscard]] auto CoordinatorState::DoFailover() -> DoFailoverStatus {
  MG_ASSERT(std::holds_alternative<CoordinatorData>(data_), "Cannot do failover since variant holds wrong alternative");
  auto &coord_state = std::get<CoordinatorData>(data_);
  return coord_state.DoFailover();
}

auto CoordinatorState::GetCoordinatorServer() const -> CoordinatorServer & {
  MG_ASSERT(std::holds_alternative<CoordinatorMainReplicaData>(data_),
            "Cannot get coordinator server since variant holds wrong alternative");
  return *std::get<CoordinatorMainReplicaData>(data_).coordinator_server_;
}
}  // namespace memgraph::coordination
#endif
