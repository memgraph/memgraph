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

#include "coordination/coordinator_data.hpp"

#include <range/v3/view.hpp>

namespace memgraph::coordination {

CoordinatorData::CoordinatorData() {
  auto find_instance = [](CoordinatorData *coord_data, std::string_view instance_name) -> CoordinatorInstance & {
    std::shared_lock<utils::RWLock> lock{coord_data->coord_data_lock_};

    auto instance =
        std::ranges::find_if(coord_data->registered_instances_, [instance_name](const CoordinatorInstance &instance) {
          return instance.client_info_.InstanceName() == instance_name;
        });

    MG_ASSERT(instance != coord_data->registered_instances_.end(), "Instance {} not found during callback!",
              instance_name);
    return *instance;
  };

  replica_succ_cb_ = [find_instance](CoordinatorData *coord_data, std::string_view instance_name) -> void {
    auto &instance = find_instance(coord_data, instance_name);
    MG_ASSERT(instance.IsReplica(), "Instance {} is not a replica!", instance_name);
    instance.client_info_.UpdateLastResponseTime();
  };

  replica_fail_cb_ = [find_instance](CoordinatorData *coord_data, std::string_view instance_name) -> void {
    auto &instance = find_instance(coord_data, instance_name);
    MG_ASSERT(instance.IsReplica(), "Instance {} is not a replica!", instance_name);
    instance.client_info_.UpdateInstanceStatus();
  };

  main_succ_cb_ = [find_instance](CoordinatorData *coord_data, std::string_view instance_name) -> void {
    auto &instance = find_instance(coord_data, instance_name);
    MG_ASSERT(instance.IsMain(), "Instance {} is not a main!", instance_name);
    instance.client_info_.UpdateLastResponseTime();
  };

  main_fail_cb_ = [this, find_instance](CoordinatorData *coord_data, std::string_view instance_name) -> void {
    auto &instance = find_instance(coord_data, instance_name);
    if (bool main_alive = instance.client_info_.UpdateInstanceStatus(); !main_alive) {
      //  spdlog::warn("Main is not alive, starting failover");
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
    }
  };
}

auto CoordinatorData::DoFailover() -> DoFailoverStatus {
  using ReplicationClientInfo = CoordinatorClientConfig::ReplicationClientInfo;
  // std::lock_guard<utils::RWLock> lock{coord_data_lock_};

  // TODO: (andi) Make const what is possible to make const

  auto main_instance = std::ranges::find_if(registered_instances_, &CoordinatorInstance::IsMain);

  if (main_instance == registered_instances_.end()) {
    return DoFailoverStatus::CLUSTER_UNINITIALIZED;
  }

  if (main_instance->client_info_.IsAlive()) {
    return DoFailoverStatus::MAIN_ALIVE;
  }

  main_instance->client_.StopFrequentCheck();

  auto replica_instances = registered_instances_ | ranges::views::filter(&CoordinatorInstance::IsReplica);

  auto chosen_replica_instance = std::ranges::find_if(
      replica_instances, [](const CoordinatorInstance &instance) { return instance.client_info_.IsAlive(); });

  if (chosen_replica_instance == replica_instances.end()) {
    return DoFailoverStatus::ALL_REPLICAS_DOWN;
  }

  chosen_replica_instance->client_.PauseFrequentCheck();

  std::vector<ReplicationClientInfo> repl_clients_info;
  repl_clients_info.reserve(std::ranges::distance(replica_instances));

  auto not_chosen_replica_instance = [&chosen_replica_instance](const CoordinatorInstance &instance) {
    return instance != *chosen_replica_instance;
  };

  for (const auto &unchosen_replica_instance : replica_instances | ranges::views::filter(not_chosen_replica_instance)) {
    repl_clients_info.emplace_back(unchosen_replica_instance.client_.ReplicationClientInfo());
  }

  if (!chosen_replica_instance->client_.SendPromoteReplicaToMainRpc(std::move(repl_clients_info))) {
    spdlog::error("Sent RPC message, but exception was caught, aborting Failover");
    // TODO: new status and rollback all changes that were done...
    MG_ASSERT(false, "RPC message failed");
  }

  chosen_replica_instance->client_.SetSuccCallback(main_succ_cb_);
  chosen_replica_instance->client_.SetFailCallback(main_fail_cb_);
  chosen_replica_instance->replication_role_ = replication_coordination_glue::ReplicationRole::MAIN;
  // TODO: (andi) Is this correct
  chosen_replica_instance->client_.ReplicationClientInfo().reset();
  chosen_replica_instance->client_.ResumeFrequentCheck();

  registered_instances_.erase(main_instance);

  return DoFailoverStatus::SUCCESS;
}

auto CoordinatorData::ShowMain() const -> std::optional<CoordinatorInstanceStatus> {
  auto main_instance = std::ranges::find_if(registered_instances_, &CoordinatorInstance::IsMain);
  if (main_instance == registered_instances_.end()) {
    return std::nullopt;
  }

  return CoordinatorInstanceStatus{.instance_name = main_instance->client_info_.InstanceName(),
                                   .socket_address = main_instance->client_info_.SocketAddress(),
                                   .is_alive = main_instance->client_info_.IsAlive()};
};

auto CoordinatorData::ShowReplicas() const -> std::vector<CoordinatorInstanceStatus> {
  std::vector<CoordinatorInstanceStatus> instances_status;

  for (const auto &replica_instance : registered_instances_ | ranges::views::filter(&CoordinatorInstance::IsReplica)) {
    instances_status.emplace_back(
        CoordinatorInstanceStatus{.instance_name = replica_instance.client_info_.InstanceName(),
                                  .socket_address = replica_instance.client_info_.SocketAddress(),
                                  .is_alive = replica_instance.client_info_.IsAlive()});
  }

  return instances_status;
}

auto CoordinatorData::RegisterMain(CoordinatorClientConfig config) -> RegisterMainReplicaCoordinatorStatus {
  // TODO: (andi) test this

  if (std::ranges::any_of(registered_instances_, [&config](const CoordinatorInstance &instance) {
        return instance.client_info_.InstanceName() == config.instance_name;
      })) {
    return RegisterMainReplicaCoordinatorStatus::NAME_EXISTS;
  }

  if (std::ranges::any_of(registered_instances_, [&config](const CoordinatorInstance &instance) {
        return instance.client_info_.SocketAddress() == config.ip_address + ":" + std::to_string(config.port);
      })) {
    return RegisterMainReplicaCoordinatorStatus::ENDPOINT_EXISTS;
  }

  // TODO: (andi) Improve this
  auto *instance = &registered_instances_.emplace_back(this, std::move(config), main_succ_cb_, main_fail_cb_,
                                                       replication_coordination_glue::ReplicationRole::MAIN);
  instance->client_.StartFrequentCheck();

  return RegisterMainReplicaCoordinatorStatus::SUCCESS;
}

auto CoordinatorData::RegisterReplica(CoordinatorClientConfig config) -> RegisterMainReplicaCoordinatorStatus {
  // TODO: (andi) Test it
  if (std::ranges::any_of(registered_instances_, [&config](const CoordinatorInstance &instance) {
        return instance.client_info_.InstanceName() == config.instance_name;
      })) {
    return RegisterMainReplicaCoordinatorStatus::NAME_EXISTS;
  }

  if (std::ranges::any_of(registered_instances_, [&config](const CoordinatorInstance &instance) {
        return instance.client_info_.SocketAddress() == config.ip_address + ":" + std::to_string(config.port);
      })) {
    return RegisterMainReplicaCoordinatorStatus::ENDPOINT_EXISTS;
  }

  // TODO: (andi) Improve this
  auto *instance = &registered_instances_.emplace_back(this, std::move(config), replica_succ_cb_, replica_fail_cb_,
                                                       replication_coordination_glue::ReplicationRole::REPLICA);
  instance->client_.StartFrequentCheck();

  return RegisterMainReplicaCoordinatorStatus::SUCCESS;
}

}  // namespace memgraph::coordination
#endif
