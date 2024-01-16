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
#include <span>
#include "coordination/coordinator_client.hpp"

#ifdef MG_ENTERPRISE

#include "coordination/coordinator_config.hpp"
#include "coordination/coordinator_entity_info.hpp"
#include "flags/replication.hpp"
#include "spdlog/spdlog.h"
#include "utils/logging.hpp"
#include "utils/variant_helpers.hpp"

#include <atomic>
#include <exception>
#include <optional>

namespace memgraph::coordination {

namespace {

bool CheckEndpointExists(const std::list<CoordinatorClient> &replicas, const CoordinatorClientConfig &config) {
  auto endpoint_matches = [&config](auto const &replica) {
    const auto &ep = replica.Endpoint();
    return ep.address == config.ip_address && ep.port == config.port;
  };
  return std::any_of(replicas.begin(), replicas.end(), endpoint_matches);
};

bool CheckName(const std::list<CoordinatorClient> &replicas, const CoordinatorClientConfig &config) {
  auto name_matches = [&instance_name = config.instance_name](auto const &replica) {
    return replica.InstanceName() == instance_name;
  };
  return std::any_of(replicas.begin(), replicas.end(), name_matches);
};

}  // namespace

CoordinatorState::CoordinatorState() {
  MG_ASSERT(!(FLAGS_coordinator && FLAGS_coordinator_server_port),
            "Instance cannot be a coordinator and have registered coordinator server.");

  if (FLAGS_coordinator_server_port) {
    auto const config = CoordinatorServerConfig{
        .ip_address = kDefaultReplicationServerIp,
        .port = static_cast<uint16_t>(FLAGS_coordinator_server_port),
    };

    data_ = CoordinatorMainReplicaData{.coordinator_server_ = std::make_unique<CoordinatorServer>(config)};
  }
}

auto CoordinatorState::RegisterReplica(const CoordinatorClientConfig &config)
    -> utils::BasicResult<RegisterMainReplicaCoordinatorStatus, CoordinatorClient *> {
  const auto name_endpoint_status =
      std::visit(memgraph::utils::Overloaded{
                     [](const CoordinatorMainReplicaData & /*coordinator_main_replica_data*/) {
                       return RegisterMainReplicaCoordinatorStatus::NOT_COORDINATOR;
                     },
                     [&config](const CoordinatorData &coordinator_data) {
                       if (memgraph::coordination::CheckName(coordinator_data.registered_replicas_, config)) {
                         return RegisterMainReplicaCoordinatorStatus::NAME_EXISTS;
                       }
                       if (memgraph::coordination::CheckEndpointExists(coordinator_data.registered_replicas_, config)) {
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

auto CoordinatorState::RegisterMain(const CoordinatorClientConfig &config)
    -> utils::BasicResult<RegisterMainReplicaCoordinatorStatus, CoordinatorClient *> {
  const auto endpoint_status =
      std::visit(memgraph::utils::Overloaded{[](const CoordinatorMainReplicaData & /*coordinator_main_replica_data*/) {
                                               return RegisterMainReplicaCoordinatorStatus::NOT_COORDINATOR;
                                             },
                                             [&config](const CoordinatorData &coordinator_data) {
                                               if (memgraph::coordination::CheckEndpointExists(
                                                       coordinator_data.registered_replicas_, config)) {
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

auto CoordinatorState::ShowReplicas() const -> std::vector<CoordinatorEntityInfo> {
  if (!std::holds_alternative<CoordinatorData>(data_)) {
    MG_ASSERT(false, "Can't call show replicas on data_, as variant holds wrong alternative");
  }
  std::vector<CoordinatorEntityInfo> result;
  const auto &registered_replicas = std::get<CoordinatorData>(data_).registered_replicas_;
  result.reserve(registered_replicas.size());
  std::ranges::transform(registered_replicas, std::back_inserter(result), [](const auto &replica) {
    return CoordinatorEntityInfo{replica.InstanceName(), replica.Endpoint()};
  });
  return result;
}

auto CoordinatorState::ShowMain() const -> std::optional<CoordinatorEntityInfo> {
  if (!std::holds_alternative<CoordinatorData>(data_)) {
    MG_ASSERT(false, "Can't call show main on data_, as variant holds wrong alternative");
  }
  const auto &registered_main = std::get<CoordinatorData>(data_).registered_main_;
  if (registered_main) {
    return CoordinatorEntityInfo{registered_main->InstanceName(), registered_main->Endpoint()};
  }
  return std::nullopt;
}

auto CoordinatorState::PingReplicas() const -> std::unordered_map<std::string_view, bool> {
  if (!std::holds_alternative<CoordinatorData>(data_)) {
    MG_ASSERT(false, "Can't call ping replicas on data_, as variant holds wrong alternative");
  }
  std::unordered_map<std::string_view, bool> result;
  const auto &registered_replicas = std::get<CoordinatorData>(data_).registered_replicas_;
  result.reserve(registered_replicas.size());
  for (const CoordinatorClient &replica_client : registered_replicas) {
    result.emplace(replica_client.InstanceName(), replica_client.DoHealthCheck());
  }

  return result;
}

auto CoordinatorState::PingMain() const -> std::optional<CoordinatorEntityHealthInfo> {
  if (!std::holds_alternative<CoordinatorData>(data_)) {
    MG_ASSERT(false, "Can't call show main on data_, as variant holds wrong alternative");
  }
  const auto &registered_main = std::get<CoordinatorData>(data_).registered_main_;
  if (registered_main) {
    return CoordinatorEntityHealthInfo{registered_main->InstanceName(), registered_main->DoHealthCheck()};
  }
  return std::nullopt;
}

auto CoordinatorState::DoFailover() -> DoFailoverStatus {
  // 1. MAIN is already down, stop sending frequent checks
  // 2. find new replica (coordinator)
  // 3. make copy replica's client as potential new main client (coordinator)
  // 4. send failover RPC to new main (coordinator and new main)
  // 5. exchange old main to new main (coordinator)
  // 6. remove replica which was promoted to main from all replicas -> this will shut down RPC frequent check client
  // (coordinator)
  // 7. for new main start frequent checks (coordinator)

  MG_ASSERT(std::holds_alternative<CoordinatorData>(data_), "Cannot do failover since variant holds wrong alternative");
  using ReplicationClientInfo = CoordinatorClientConfig::ReplicationClientInfo;

  // 1.
  auto &current_main = std::get<CoordinatorData>(data_).registered_main_;
  if (current_main->DoHealthCheck()) {
    return DoFailoverStatus::MAIN_ALIVE;
  }
  current_main->StopFrequentCheck();

  // 2.
  // Get all replicas and find new main
  auto &registered_replicas = std::get<CoordinatorData>(data_).registered_replicas_;

  const auto chosen_replica = std::ranges::find_if(
      registered_replicas, [](const CoordinatorClient &replica) { return replica.DoHealthCheck(); });
  if (chosen_replica == registered_replicas.end()) {
    return DoFailoverStatus::ALL_REPLICAS_DOWN;
  }

  std::vector<ReplicationClientInfo> repl_clients_info;
  repl_clients_info.reserve(registered_replicas.size() - 1);
  std::ranges::for_each(registered_replicas, [&chosen_replica, &repl_clients_info](const CoordinatorClient &replica) {
    if (replica == *chosen_replica) return;
    repl_clients_info.emplace_back(replica.ReplicationClientInfo());
  });

  // 3.
  // Set on coordinator data of new main
  // allocate resources for new main, clear replication info on this replica as main
  // set last response time
  auto potential_new_main = std::make_unique<CoordinatorClient>(chosen_replica->Config());
  potential_new_main->ReplicationClientInfo().reset();
  potential_new_main->UpdateTimeCheck(chosen_replica->GetLastTimeResponse());

  // 4.
  if (!chosen_replica->SendFailoverRpc(std::move(repl_clients_info))) {
    // TODO: rollback all changes that were done...
    spdlog::error("Sent RPC message, but exception was caught, aborting Failover");
    // TODO: new status
    MG_ASSERT(false, "RPC message failed");
    // return DoFailoverStatus::FAIL;
  }

  // 5.
  current_main = std::move(potential_new_main);

  // 6. remove old replica
  // TODO: Stop pinging chosen_replica before failover.
  // Check that it doesn't fail when you call StopFrequentCheck if it is already stopped
  registered_replicas.erase(chosen_replica);

  // 7.
  current_main->StartFrequentCheck();

  return DoFailoverStatus::SUCCESS;
}

auto CoordinatorState::GetCoordinatorServer() const -> CoordinatorServer & {
  MG_ASSERT(std::holds_alternative<CoordinatorMainReplicaData>(data_),
            "Cannot get coordinator server since variant holds wrong alternative");
  return *std::get<CoordinatorMainReplicaData>(data_).coordinator_server_;
}
}  // namespace memgraph::coordination
#endif
