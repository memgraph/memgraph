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

  // TODO: Refactor so that we just know about instances, nothing more. Design struct Instance which will know about
  // role... From CoordinatorState perspective there should just be instances, no specific handling of main vs. replica.
  // Here after failover it can happen that replica
  // has become main MG_ASSERT(replica_client_info != registered_replicas_info.end(), "Replica {} not found in
  // registered replicas info",
  //          instance_name);
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

  auto repl_succ_cb = [find_client_info](CoordinatorState *coord_state, std::string_view instance_name) -> void {
    auto &client_info = find_client_info(coord_state, instance_name);
    client_info.UpdateLastResponseTime();
  };

  auto repl_fail_cb = [find_client_info](CoordinatorState *coord_state, std::string_view instance_name) -> void {
    auto &client_info = find_client_info(coord_state, instance_name);
    client_info.UpdateInstanceStatus();
  };

  auto *coord_client = &std::get<CoordinatorData>(data_).registered_replicas_.emplace_back(
      this, std::move(config), std::move(repl_succ_cb), std::move(repl_fail_cb));

  std::get<CoordinatorData>(data_).registered_replicas_info_.emplace_back(coord_client->InstanceName(),
                                                                          coord_client->SocketAddress());
  coord_client->StartFrequentCheck();

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

  auto &registered_main = std::get<CoordinatorData>(data_).registered_main_;
  registered_main =
      std::make_unique<CoordinatorClient>(this, std::move(config), std::move(succ_cb), std::move(fail_cb));

  std::get<CoordinatorData>(data_).registered_main_info_.emplace(registered_main->InstanceName(),
                                                                 registered_main->SocketAddress());
  registered_main->StartFrequentCheck();

  return RegisterMainReplicaCoordinatorStatus::SUCCESS;
}

auto CoordinatorState::ShowReplicas() const -> std::vector<CoordinatorInstanceStatus> {
  MG_ASSERT(std::holds_alternative<CoordinatorData>(data_),
            "Can't call show replicas on data_, as variant holds wrong alternative");
  const auto &registered_replicas_info = std::get<CoordinatorData>(data_).registered_replicas_info_;
  std::vector<CoordinatorInstanceStatus> instances_status;
  instances_status.reserve(registered_replicas_info.size());

  std::ranges::transform(registered_replicas_info, std::back_inserter(instances_status),
                         [](const CoordinatorClientInfo &coord_client_info) {
                           return CoordinatorInstanceStatus{.instance_name = coord_client_info.InstanceName(),
                                                            .socket_address = coord_client_info.SocketAddress(),
                                                            .is_alive = coord_client_info.IsAlive()};
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

  return CoordinatorInstanceStatus{
      .instance_name = main->InstanceName(), .socket_address = main->SocketAddress(), .is_alive = main->IsAlive()};
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
  using ReplicationClientInfo = CoordinatorClientConfig::ReplicationClientInfo;
  MG_ASSERT(std::holds_alternative<CoordinatorData>(data_), "Cannot do failover since variant holds wrong alternative");
  auto &coord_state = std::get<CoordinatorData>(data_);

  // std::lock_guard<utils::RWLock> lock{coord_state.coord_data_lock_};

  // 1.
  auto &current_main_info = coord_state.registered_main_info_;

  if (!current_main_info.has_value()) {
    return DoFailoverStatus::CLUSTER_UNINITIALIZED;
  }

  if (current_main_info->IsAlive()) {
    return DoFailoverStatus::MAIN_ALIVE;
  }

  auto &current_main = coord_state.registered_main_;
  // TODO: stop pinging as soon as you figure out that failover is needed
  current_main->PauseFrequentCheck();

  // 2.
  // Get all replicas and find new main
  auto &registered_replicas_info = coord_state.registered_replicas_info_;

  const auto chosen_replica_info = std::ranges::find_if(
      registered_replicas_info, [](const CoordinatorClientInfo &client_info) { return client_info.IsAlive(); });
  if (chosen_replica_info == registered_replicas_info.end()) {
    return DoFailoverStatus::ALL_REPLICAS_DOWN;
  }

  auto &registered_replicas = coord_state.registered_replicas_;
  auto chosen_replica =
      std::ranges::find_if(registered_replicas, [&chosen_replica_info](const CoordinatorClient &replica) {
        return replica.InstanceName() == chosen_replica_info->InstanceName();
      });
  MG_ASSERT(chosen_replica != registered_replicas.end(), "Chosen replica {} not found in registered replicas",
            chosen_replica_info->InstanceName());

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
  auto potential_new_main = std::make_unique<CoordinatorClient>(
      this, chosen_replica->Config(), chosen_replica->SuccCallback(), chosen_replica->FailCallback());
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

  current_main->ResumeFrequentCheck();

  return DoFailoverStatus::SUCCESS;
}

auto CoordinatorState::GetCoordinatorServer() const -> CoordinatorServer & {
  MG_ASSERT(std::holds_alternative<CoordinatorMainReplicaData>(data_),
            "Cannot get coordinator server since variant holds wrong alternative");
  return *std::get<CoordinatorMainReplicaData>(data_).coordinator_server_;
}
}  // namespace memgraph::coordination
#endif
