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

#include "replication/coordinator.hpp"
#include <variant>

#include "flags/replication.hpp"
#include "replication/coordinator_config.hpp"
#include "replication/register_replica_error.hpp"
#include "utils/variant_helpers.hpp"

namespace memgraph::replication {

#ifdef MG_ENTERPRISE

CoordinatorState::CoordinatorState() {
  MG_ASSERT(!(FLAGS_coordinator && FLAGS_coordinator_server_port),
            "Instance cannot be a coordinator and have registered coordinator server.");

  // MAIN or REPLICA instance
  if (FLAGS_coordinator_server_port) {
    auto const config = memgraph::replication::ReplicationServerConfig{
        .ip_address = memgraph::replication::kDefaultReplicationServerIp,
        .port = static_cast<uint16_t>(FLAGS_coordinator_server_port),
    };
    data_ = CoordinatorMainReplicaData{.coordinator_server_ = std::make_unique<CoordinatorServer>(config)};
    // TODO: (andi) Register coordinator handlers
    // InMemoryReplicationHandlers::Register(&dbms_handler_, *data.server);
    if (!std::get<CoordinatorMainReplicaData>(data_).coordinator_server_->Start()) {
      MG_ASSERT(false, "Failed to start coordinator server!");
    }
  }
}

utils::BasicResult<RegisterMainReplicaCoordinatorStatus, CoordinatorClient *> CoordinatorState::RegisterReplica(
    const CoordinatorClientConfig &config) {
  // TODO: (andi) Solve DRY by extracting
  auto name_check = [&config](auto const &replicas) {
    auto name_matches = [&name = config.name](auto const &replica) { return replica.name_ == name; };
    return std::any_of(replicas.begin(), replicas.end(), name_matches);
  };

  // endpoint check
  auto endpoint_check = [&](auto const &replicas) {
    auto endpoint_matches = [&config](auto const &replica) {
      const auto &ep = replica.rpc_client_.Endpoint();
      return ep.address == config.ip_address && ep.port == config.port;
    };
    return std::any_of(replicas.begin(), replicas.end(), endpoint_matches);
  };

  const auto name_endpoint_status =
      std::visit(memgraph::utils::Overloaded{[](const CoordinatorMainReplicaData & /*coordinator_main_replica_data*/) {
                                               return RegisterMainReplicaCoordinatorStatus::NOT_COORDINATOR;
                                             },
                                             [&name_check, &endpoint_check](const CoordinatorData &coordinator_data) {
                                               if (name_check(coordinator_data.registered_replicas_)) {
                                                 return RegisterMainReplicaCoordinatorStatus::NAME_EXISTS;
                                               }
                                               if (endpoint_check(coordinator_data.registered_replicas_)) {
                                                 return RegisterMainReplicaCoordinatorStatus::END_POINT_EXISTS;
                                               }
                                               return RegisterMainReplicaCoordinatorStatus::SUCCESS;
                                             }},
                 data_);

  if (name_endpoint_status != RegisterMainReplicaCoordinatorStatus::SUCCESS) {
    return name_endpoint_status;
  }

  // Maybe no need to return client if you can start replica client here
  return &std::get<CoordinatorData>(data_).registered_replicas_.emplace_back(config);
}

utils::BasicResult<RegisterMainReplicaCoordinatorStatus, CoordinatorClient *> CoordinatorState::RegisterMain(
    const CoordinatorClientConfig &config) {
  // endpoint check
  auto endpoint_check = [&](auto const &replicas) {
    auto endpoint_matches = [&config](auto const &replica) {
      const auto &ep = replica.rpc_client_.Endpoint();
      return ep.address == config.ip_address && ep.port == config.port;
    };
    return std::any_of(replicas.begin(), replicas.end(), endpoint_matches);
  };

  const auto endpoint_status =
      std::visit(memgraph::utils::Overloaded{[](const CoordinatorMainReplicaData & /*coordinator_main_replica_data*/) {
                                               return RegisterMainReplicaCoordinatorStatus::NOT_COORDINATOR;
                                             },
                                             [&endpoint_check](const CoordinatorData &coordinator_data) {
                                               if (endpoint_check(coordinator_data.registered_replicas_)) {
                                                 return RegisterMainReplicaCoordinatorStatus::END_POINT_EXISTS;
                                               }
                                               return RegisterMainReplicaCoordinatorStatus::SUCCESS;
                                             }},
                 data_);

  if (endpoint_status != RegisterMainReplicaCoordinatorStatus::SUCCESS) {
    return endpoint_status;
  }

  auto &registered_main = std::get<CoordinatorData>(data_).registered_main_;
  registered_main = std::make_unique<CoordinatorClient>(config);
  return registered_main.get();
}

std::vector<CoordinatorEntityInfo> CoordinatorState::ShowReplicas() const {
  if (!std::holds_alternative<CoordinatorData>(data_)) {
    MG_ASSERT(false, "Can't call show replicas on data_, as variant holds wrong alternative");
  }
  std::vector<CoordinatorEntityInfo> result;
  const auto &registered_replicas = std::get<CoordinatorData>(data_).registered_replicas_;
  result.reserve(registered_replicas.size());
  std::transform(registered_replicas.begin(), registered_replicas.end(), std::back_inserter(result),
                 [](const auto &replica) {
                   return CoordinatorEntityInfo{replica.name_, replica.rpc_client_.Endpoint()};
                 });
  return result;
}

std::optional<CoordinatorEntityInfo> CoordinatorState::ShowMain() const {
  if (!std::holds_alternative<CoordinatorData>(data_)) {
    MG_ASSERT(false, "Can't call show main on data_, as variant holds wrong alternative");
  }
  const auto &registered_main = std::get<CoordinatorData>(data_).registered_main_;
  if (registered_main) {
    return CoordinatorEntityInfo{registered_main->name_, registered_main->rpc_client_.Endpoint()};
  }
  return std::nullopt;
}

#endif

}  // namespace memgraph::replication
