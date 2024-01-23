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
#include <algorithm>
#include "coordination/coordinator_client_info.hpp"
#include "coordination/register_main_replica_coordinator_status.hpp"

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

auto CoordinatorState::RegisterInstanceOnCoordinator(CoordinatorClientConfig config)
    -> RegisterInstanceCoordinatorStatus {
  MG_ASSERT(std::holds_alternative<CoordinatorData>(data_),
            "Coordinator cannot register replica since variant holds wrong alternative");

  const auto name_endpoint_status =
      std::visit(memgraph::utils::Overloaded{[](const CoordinatorMainReplicaData & /*coordinator_main_replica_data*/) {
                                               return RegisterInstanceCoordinatorStatus::NOT_COORDINATOR;
                                             },
                                             [&config](const CoordinatorData &coordinator_data) {
                                               if (memgraph::coordination::CheckName(
                                                       coordinator_data.registered_replicas_, config)) {
                                                 return RegisterInstanceCoordinatorStatus::NAME_EXISTS;
                                               }
                                               return RegisterInstanceCoordinatorStatus::SUCCESS;
                                             }},
                 data_);

  if (name_endpoint_status != RegisterInstanceCoordinatorStatus::SUCCESS) {
    return name_endpoint_status;
  }

  auto find_client_info = [](CoordinatorState *coord_state, std::string_view instance_name) -> CoordinatorClientInfo & {
    MG_ASSERT(std::holds_alternative<CoordinatorData>(coord_state->data_),
              "Can't execute CoordinatorClient's callback since variant holds wrong alternative");
    auto &coord_data = std::get<CoordinatorData>(coord_state->data_);
    std::shared_lock<utils::RWLock> lock{coord_data.coord_data_lock_};

    auto replica_client_info = std::ranges::find_if(
        coord_data.registered_replicas_info_,
        [instance_name](const CoordinatorClientInfo &replica) { return replica.InstanceName() == instance_name; });

    if (replica_client_info != coord_data.registered_replicas_info_.end()) {
      return *replica_client_info;
    }

    MG_ASSERT(coord_data.registered_main_info_->InstanceName() == instance_name,
              "Instance is neither a replica nor main...");
    return *coord_data.registered_main_info_;
  };

  // TODO MERGE WITH ANDI's WORK
  auto repl_succ_cb = [find_client_info](CoordinatorState *coord_state, std::string_view instance_name) -> void {
    auto &client_info = find_client_info(coord_state, instance_name);
    client_info.UpdateLastResponseTime();
  };

  auto repl_fail_cb = [find_client_info](CoordinatorState *coord_state, std::string_view instance_name) -> void {
    auto &client_info = find_client_info(coord_state, instance_name);
    client_info.UpdateInstanceStatus();
  };
  CoordinatorClientConfig::ReplicationClientInfo replication_client_info = *config.replication_client_info;
  auto *coord_client = &std::get<CoordinatorData>(data_).registered_replicas_.emplace_back(
      this, std::move(config), std::move(repl_succ_cb), std::move(repl_fail_cb));

  coord_client->SendSetToReplicaRpc(replication_client_info);

  std::get<CoordinatorData>(data_).registered_replicas_info_.emplace_back(coord_client->InstanceName(),
                                                                          coord_client->SocketAddress());
  coord_client->StartFrequentCheck();

  return RegisterInstanceCoordinatorStatus::SUCCESS;
}

auto CoordinatorState::SetInstanceToMain(std::string instance_name) -> SetInstanceToMainCoordinatorStatus {
  MG_ASSERT(std::holds_alternative<CoordinatorData>(data_),
            "Coordinator cannot register replica since variant holds wrong alternative");

  // TODO: (andi) How does the situation change when restoration of main is implemented regarding callbacks?
  // We should probably at that point search for main also in instances as for replicas...
  auto get_client_info = [](CoordinatorState *coord_state, std::string_view instance_name) -> CoordinatorClientInfo & {
    MG_ASSERT(std::holds_alternative<CoordinatorData>(coord_state->data_),
              "Can't execute CoordinatorClient's callback since variant holds wrong alternative");
    MG_ASSERT(std::get<CoordinatorData>(coord_state->data_).registered_main_info_.has_value(),
              "Main info is not set, but callback is called");
    auto &coord_data = std::get<CoordinatorData>(coord_state->data_);
    std::shared_lock<utils::RWLock> lock{coord_data.coord_data_lock_};

    // TODO When we will support restoration of main, we have to assert that the instance is main or replica, not at
    // this point....
    auto &registered_main_info = coord_data.registered_main_info_;
    MG_ASSERT(registered_main_info->InstanceName() == instance_name,
              "Callback called for wrong instance name: {}, expected: {}", instance_name,
              registered_main_info->InstanceName());
    return *registered_main_info;
  };

  auto succ_cb = [get_client_info](CoordinatorState *coord_state, std::string_view instance_name) -> void {
    auto &registered_main_info = get_client_info(coord_state, instance_name);
    registered_main_info.UpdateLastResponseTime();
  };

  auto fail_cb = [this, get_client_info](CoordinatorState *coord_state, std::string_view instance_name) -> void {
    auto &registered_main_info = get_client_info(coord_state, instance_name);
    if (bool main_alive = registered_main_info.UpdateInstanceStatus(); !main_alive) {
      spdlog::warn("Main is not alive, starting failover");
      switch (auto failover_status = DoFailover(); failover_status) {
        using enum DoFailoverStatus;
        case ALL_REPLICAS_DOWN:
          spdlog::warn("Failover aborted since all replicas are down!");
        case MAIN_ALIVE:
          spdlog::warn("Failover aborted since main is alive!");
        case CLUSTER_UNINITIALIZED:
          spdlog::warn("Failover aborted since cluster is uninitialized!");
        case SUCCESS:
          break;
      }
    }
  };

  auto &registered_replicas = std::get<CoordinatorData>(data_).registered_replicas_;
  // Find replica we already registered
  auto registered_replica =
      std::find_if(registered_replicas.begin(), registered_replicas.end(), [instance_name](const auto &replica_client) {
        std::cout << "replica name: " << replica_client.InstanceName() << ", instance name: " << instance_name
                  << std::endl;
        return replica_client.InstanceName() == instance_name;
      });

  std::for_each(registered_replicas.begin(), registered_replicas.end(),
                [](const auto &client) { std::cout << "replica names: " << client.InstanceName() << std::endl; });
  // if replica not found...
  if (registered_replica == registered_replicas.end()) {
    spdlog::error("You didn't register instance with given name {}", instance_name);
    return SetInstanceToMainCoordinatorStatus::NO_INSTANCE_WITH_NAME;
  }
  registered_replica->StopFrequentCheck();
  // Set instance as MAIN
  // THIS WILL SHUT DOWN CLIENT
  auto &registered_main = std::get<CoordinatorData>(data_).registered_main_;
  registered_main =
      std::make_unique<CoordinatorClient>(this, registered_replica->Config(), std::move(succ_cb), std::move(fail_cb));

  std::get<CoordinatorData>(data_).registered_main_info_.emplace(registered_main->InstanceName(),
                                                                 registered_main->SocketAddress());
  std::vector<CoordinatorClientConfig::ReplicationClientInfo> repl_clients_info;
  repl_clients_info.reserve(registered_replicas.size() - 1);
  std::ranges::for_each(registered_replicas,
                        [registered_replica, &repl_clients_info](const CoordinatorClient &replica) {
                          if (replica != *registered_replica) {
                            repl_clients_info.emplace_back(replica.ReplicationClientInfo());
                          }
                        });

  // PROMOTE REPLICA TO MAIN
  // THIS SHOULD FAIL HERE IF IT IS DOWN
  if (auto result = registered_main->SendPromoteReplicaToMainRpc(std::move(repl_clients_info)); !result) {
    registered_replica->StartFrequentCheck();
    registered_main.reset();
    return SetInstanceToMainCoordinatorStatus::COULD_NOT_PROMOTE_TO_MAIN;
  }

  registered_main->StartFrequentCheck();
  registered_replicas.erase(registered_replica);
  return SetInstanceToMainCoordinatorStatus::SUCCESS;
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
