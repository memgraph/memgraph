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

#include "coordination/coordinator_client.hpp"
#include "coordination/coordinator_cluster_config.hpp"
#include "coordination/coordinator_config.hpp"
#include "flags/replication.hpp"
#include "spdlog/spdlog.h"
#include "utils/logging.hpp"
#include "utils/variant_helpers.hpp"

#include <atomic>
#include <exception>
#include <optional>

namespace memgraph::coordination {

namespace {

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

auto CoordinatorState::RegisterReplica(CoordinatorClientConfig config) -> RegisterMainReplicaCoordinatorStatus {
  MG_ASSERT(std::holds_alternative<CoordinatorData>(data_),
            "Coordinator cannot register replica since variant holds wrong alternative");

  const auto name_endpoint_status =
      std::visit(memgraph::utils::Overloaded{[](const CoordinatorMainReplicaData & /*coordinator_main_replica_data*/) {
                                               return RegisterMainReplicaCoordinatorStatus::NOT_COORDINATOR;
                                             },
                                             [&config](const CoordinatorData &coordinator_data) {
                                               if (memgraph::coordination::CheckName(
                                                       coordinator_data.registered_replicas_, config)) {
                                                 return RegisterMainReplicaCoordinatorStatus::NAME_EXISTS;
                                               }
                                               return RegisterMainReplicaCoordinatorStatus::SUCCESS;
                                             }},
                 data_);

  if (name_endpoint_status != RegisterMainReplicaCoordinatorStatus::SUCCESS) {
    return name_endpoint_status;
  }

  auto freq_check_cb = [&](std::string_view instance_name) -> void {
    MG_ASSERT(std::holds_alternative<CoordinatorData>(data_),
              "Can't execute CoordinatorClient's callback since variant holds wrong alternative");
    auto &registered_replicas_info = std::get<CoordinatorData>(data_).registered_replicas_info_;

    auto replica_client_info = std::ranges::find_if(
        registered_replicas_info,
        [instance_name](const CoordinatorClientInfo &replica) { return replica.instance_name_ == instance_name; });

    // TODO: Refactor so that we just know about instances, nothing more. Here after failover it can happen that replica
    // has become main MG_ASSERT(replica_client_info != registered_replicas_info.end(), "Replica {} not found in
    // registered replicas info",
    //          instance_name);

    if (replica_client_info == registered_replicas_info.end()) {
      auto &registered_main_info = std::get<CoordinatorData>(data_).registered_main_info_;
      MG_ASSERT(registered_main_info->instance_name_ == instance_name, "Instance is neither a replica nor main...");
      registered_main_info->last_response_time_.store(std::chrono::system_clock::now(), std::memory_order_release);
    } else {
      replica_client_info->last_response_time_.store(std::chrono::system_clock::now(), std::memory_order_release);
    }
  };

  auto *coord_client =
      &std::get<CoordinatorData>(data_).registered_replicas_.emplace_back(std::move(config), std::move(freq_check_cb));

  std::get<CoordinatorData>(data_).registered_replicas_info_.emplace_back(coord_client->InstanceName(),
                                                                          coord_client->Endpoint());

  return RegisterMainReplicaCoordinatorStatus::SUCCESS;
}

auto CoordinatorState::RegisterMain(CoordinatorClientConfig config) -> RegisterMainReplicaCoordinatorStatus {
  MG_ASSERT(std::holds_alternative<CoordinatorData>(data_),
            "Coordinator cannot register main since variant holds wrong alternative");

  const auto endpoint_status = std::visit(
      memgraph::utils::Overloaded{
          [](const CoordinatorMainReplicaData & /*coordinator_main_replica_data*/) {
            return RegisterMainReplicaCoordinatorStatus::NOT_COORDINATOR;
          },
          [](const CoordinatorData & /*coordinator_data*/) { return RegisterMainReplicaCoordinatorStatus::SUCCESS; }},
      data_);

  if (endpoint_status != RegisterMainReplicaCoordinatorStatus::SUCCESS) {
    return endpoint_status;
  }

  auto freq_check_cb = [&](std::string_view instance_name) -> void {
    MG_ASSERT(std::holds_alternative<CoordinatorData>(data_),
              "Can't execute CoordinatorClient's callback since variant holds wrong alternative");
    MG_ASSERT(std::get<CoordinatorData>(data_).registered_main_info_.has_value(),
              "Main info is not set, but callback is called");

    // TODO When we will support restoration of main, we have to assert that the instance is main or replica, not at
    // this point....
    auto &registered_main_info = std::get<CoordinatorData>(data_).registered_main_info_;
    MG_ASSERT(registered_main_info->instance_name_ == instance_name,
              "Callback called for wrong instance name: {}, expected: {}", instance_name,
              registered_main_info->instance_name_);

    registered_main_info->last_response_time_.store(std::chrono::system_clock::now(), std::memory_order_release);

    // if (!registered_main_info->is_alive_) {
    // spdlog::warn("Main is not alive, starting failover");
    // switch (auto failover_status = DoFailover(); failover_status) {
    //   using enum DoFailoverStatus;
    //   case ALL_REPLICAS_DOWN:
    //     spdlog::warn("Failover aborted since all replicas are down!");
    //   case MAIN_ALIVE:
    //     spdlog::warn("Failover aborted since main is alive!");
    //   case CLUSTER_UNINITIALIZED:
    //     spdlog::warn("Failover aborted since cluster is uninitialized!");
    //   case SUCCESS:
    //     break;
    // }
    // }
  };

  auto &registered_main = std::get<CoordinatorData>(data_).registered_main_;
  registered_main = std::make_unique<CoordinatorClient>(std::move(config), std::move(freq_check_cb));

  auto &registered_main_info = std::get<CoordinatorData>(data_).registered_main_info_;
  registered_main_info.emplace(registered_main->InstanceName(), registered_main->Endpoint());

  return RegisterMainReplicaCoordinatorStatus::SUCCESS;
}

auto CoordinatorState::ShowReplicas() const -> std::vector<CoordinatorInstanceStatus> {
  MG_ASSERT(std::holds_alternative<CoordinatorData>(data_),
            "Can't call show replicas on data_, as variant holds wrong alternative");
  const auto &registered_replicas_info = std::get<CoordinatorData>(data_).registered_replicas_info_;
  std::vector<CoordinatorInstanceStatus> instances_status;
  instances_status.reserve(registered_replicas_info.size());

  std::ranges::transform(
      registered_replicas_info, std::back_inserter(instances_status),
      [](const CoordinatorClientInfo &coord_client_info) {
        const auto sec_since_last_response = std::chrono::duration_cast<std::chrono::seconds>(
                                                 std::chrono::system_clock::now() -
                                                 coord_client_info.last_response_time_.load(std::memory_order_acquire))
                                                 .count();

        return CoordinatorInstanceStatus{
            .instance_name = coord_client_info.instance_name_,
            .socket_address = coord_client_info.endpoint->SocketAddress(),
            .is_alive = sec_since_last_response <= CoordinatorClusterConfig::alive_response_time_difference_sec_};
      });
  return instances_status;
}

auto CoordinatorState::ShowMain() const -> std::optional<CoordinatorInstanceStatus> {
  MG_ASSERT(std::holds_alternative<CoordinatorData>(data_),
            "Can't call show main on data_, as variant holds wrong alternative");
  const auto &main = std::get<CoordinatorData>(data_).registered_main_info_;
  if (!main.has_value()) {
    return std::nullopt;
  }
  const auto sec_since_last_response =
      std::chrono::duration_cast<std::chrono::seconds>(std::chrono::system_clock::now() -
                                                       main->last_response_time_.load(std::memory_order_acquire))
          .count();

  return CoordinatorInstanceStatus{
      .instance_name = main->instance_name_,
      .socket_address = main->endpoint->SocketAddress(),
      .is_alive = sec_since_last_response <= CoordinatorClusterConfig::alive_response_time_difference_sec_};
};

[[nodiscard]] auto CoordinatorState::DoFailover() -> DoFailoverStatus {
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
  auto &current_main_info = std::get<CoordinatorData>(data_).registered_main_info_;

  if (!current_main_info.has_value()) {
    return DoFailoverStatus::CLUSTER_UNINITIALIZED;
  }

  auto sec_since_last_response_main =
      std::chrono::duration_cast<std::chrono::seconds>(std::chrono::system_clock::now() -
                                                       current_main_info->last_response_time_.load())
          .count();

  if (sec_since_last_response_main <= CoordinatorClusterConfig::alive_response_time_difference_sec_) {
    return DoFailoverStatus::MAIN_ALIVE;
  }

  auto &current_main = std::get<CoordinatorData>(data_).registered_main_;
  // TODO: stop pinging as soon as you figure out that failover is needed
  current_main->StopFrequentCheck();

  // 2.
  // Get all replicas and find new main
  auto &registered_replicas_info = std::get<CoordinatorData>(data_).registered_replicas_info_;

  const auto chosen_replica_info =
      std::ranges::find_if(registered_replicas_info, [](const CoordinatorClientInfo &client_info) {
        auto sec_since_last_response = std::chrono::duration_cast<std::chrono::seconds>(
                                           std::chrono::system_clock::now() - client_info.last_response_time_.load())
                                           .count();
        return sec_since_last_response <= CoordinatorClusterConfig::alive_response_time_difference_sec_;
      });
  if (chosen_replica_info == registered_replicas_info.end()) {
    return DoFailoverStatus::ALL_REPLICAS_DOWN;
  }

  auto &registered_replicas = std::get<CoordinatorData>(data_).registered_replicas_;
  const auto chosen_replica =
      std::ranges::find_if(registered_replicas, [&chosen_replica_info](const CoordinatorClient &replica) {
        return replica.InstanceName() == chosen_replica_info->instance_name_;
      });
  MG_ASSERT(chosen_replica != registered_replicas.end(), "Chosen replica {} not found in registered replicas",
            chosen_replica_info->instance_name_);

  std::vector<ReplicationClientInfo> repl_clients_info;
  repl_clients_info.reserve(registered_replicas.size() - 1);
  std::ranges::for_each(registered_replicas, [&chosen_replica, &repl_clients_info](const CoordinatorClient &replica) {
    if (replica != *chosen_replica) {
      repl_clients_info.emplace_back(replica.ReplicationClientInfo());
    }
  });

  // 3.
  // Set on coordinator data of new main
  // allocate resources for new main, clear replication info on this replica as main
  // set last response time
  auto potential_new_main = std::make_unique<CoordinatorClient>(chosen_replica->Config(), chosen_replica->Callback());
  potential_new_main->ReplicationClientInfo().reset();
  auto potential_new_main_info = *chosen_replica_info;

  // 4.
  if (!chosen_replica->SendPromoteReplicaToMainRpc(std::move(repl_clients_info))) {
    spdlog::error("Sent RPC message, but exception was caught, aborting Failover");
    // TODO: new status and rollback all changes that were done...
    MG_ASSERT(false, "RPC message failed");
  }

  // 5.
  current_main = std::move(potential_new_main);
  current_main_info.emplace(potential_new_main_info);

  // 6. remove old replica
  // TODO: Stop pinging chosen_replica before failover.
  // Check that it doesn't fail when you call StopFrequentCheck if it is already stopped
  registered_replicas.erase(chosen_replica);
  registered_replicas_info.erase(chosen_replica_info);

  return DoFailoverStatus::SUCCESS;
}

auto CoordinatorState::GetCoordinatorServer() const -> CoordinatorServer & {
  MG_ASSERT(std::holds_alternative<CoordinatorMainReplicaData>(data_),
            "Cannot get coordinator server since variant holds wrong alternative");
  return *std::get<CoordinatorMainReplicaData>(data_).coordinator_server_;
}
}  // namespace memgraph::coordination
#endif
