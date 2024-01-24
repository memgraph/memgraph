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

#include "coordination/coordinator_instance.hpp"
#include "coordination/register_main_replica_coordinator_status.hpp"
#ifdef MG_ENTERPRISE

#include "coordination/coordinator_data.hpp"

#include <range/v3/view.hpp>
#include <shared_mutex>

namespace memgraph::coordination {

CoordinatorData::CoordinatorData() {
  auto find_instance = [](CoordinatorData *coord_data, std::string_view instance_name) -> CoordinatorInstance & {
    std::shared_lock<utils::RWLock> lock{coord_data->coord_data_lock_};

    auto instance = std::ranges::find_if(
        coord_data->registered_instances_,
        [instance_name](const CoordinatorInstance &instance) { return instance.InstanceName() == instance_name; });

    MG_ASSERT(instance != coord_data->registered_instances_.end(), "Instance {} not found during callback!",
              instance_name);
    return *instance;
  };

  replica_succ_cb_ = [find_instance](CoordinatorData *coord_data, std::string_view instance_name) -> void {
    spdlog::trace("Instance {} performing replica successful callback", instance_name);
    auto &instance = find_instance(coord_data, instance_name);
    MG_ASSERT(instance.IsReplica(), "Instance {} is not a replica!", instance_name);
    instance.UpdateLastResponseTime();
  };

  replica_fail_cb_ = [find_instance](CoordinatorData *coord_data, std::string_view instance_name) -> void {
    spdlog::trace("Instance {} performing replica failure callback", instance_name);
    auto &instance = find_instance(coord_data, instance_name);
    MG_ASSERT(instance.IsReplica(), "Instance {} is not a replica!", instance_name);
    instance.UpdateInstanceStatus();
  };

  main_succ_cb_ = [find_instance](CoordinatorData *coord_data, std::string_view instance_name) -> void {
    spdlog::trace("Instance {} performing main successful callback", instance_name);
    auto &instance = find_instance(coord_data, instance_name);
    MG_ASSERT(instance.IsMain(), "Instance {} is not a main!", instance_name);
    instance.UpdateLastResponseTime();
  };

  main_fail_cb_ = [this, find_instance](CoordinatorData *coord_data, std::string_view instance_name) -> void {
    spdlog::trace("Instance {} performing main failure callback", instance_name);
    auto &instance = find_instance(coord_data, instance_name);
    MG_ASSERT(instance.IsMain(), "Instance {} is not a main!", instance_name);
    if (bool main_alive = instance.UpdateInstanceStatus(); !main_alive) {
      spdlog::info("Main instance {} is not alive, starting automatic failover", instance_name);
      switch (auto failover_status = DoFailover(); failover_status) {
        using enum DoFailoverStatus;
        case ALL_REPLICAS_DOWN:
          spdlog::warn("Failover aborted since all replicas are down!");
          break;
        case MAIN_ALIVE:
          spdlog::warn("Failover aborted since main is alive!");
          break;
        case CLUSTER_UNINITIALIZED:
          spdlog::warn("Failover aborted since cluster is uninitialized!");
          break;
        case RPC_FAILED:
          spdlog::warn("Failover aborted since promoting replica to main failed!");
          break;
        case SUCCESS:
          break;
      }
    }
  };
}

auto CoordinatorData::DoFailover() -> DoFailoverStatus {
  using ReplicationClientInfo = CoordinatorClientConfig::ReplicationClientInfo;
  std::lock_guard<utils::RWLock> lock{coord_data_lock_};

  const auto main_instance = std::ranges::find_if(registered_instances_, &CoordinatorInstance::IsMain);

  if (main_instance == registered_instances_.end()) {
    return DoFailoverStatus::CLUSTER_UNINITIALIZED;
  }

  if (main_instance->IsAlive()) {
    return DoFailoverStatus::MAIN_ALIVE;
  }

  main_instance->client_.PauseFrequentCheck();

  auto replica_instances = registered_instances_ | ranges::views::filter(&CoordinatorInstance::IsReplica);

  auto chosen_replica_instance = std::ranges::find_if(replica_instances, &CoordinatorInstance::IsAlive);
  if (chosen_replica_instance == replica_instances.end()) {
    return DoFailoverStatus::ALL_REPLICAS_DOWN;
  }

  chosen_replica_instance->PrepareForFailover();

  std::vector<ReplicationClientInfo> repl_clients_info;
  repl_clients_info.reserve(std::ranges::distance(replica_instances));

  auto not_chosen_replica_instance = [&chosen_replica_instance](const CoordinatorInstance &instance) {
    return instance != *chosen_replica_instance;
  };
  auto not_main = [](const CoordinatorInstance &instance) { return !instance.IsMain(); };

  // Filter not current replicas and not MAIN instance
  // TODO (antoniofilipovic): Should we send also data on old MAIN???
  for (const auto &unchosen_replica_instance :
       replica_instances | ranges::views::filter(not_chosen_replica_instance) | ranges::views::filter(not_main)) {
    repl_clients_info.emplace_back(unchosen_replica_instance.client_.ReplicationClientInfo());
  }

  if (!chosen_replica_instance->client_.SendPromoteReplicaToMainRpc(std::move(repl_clients_info))) {
    chosen_replica_instance->RestoreAfterFailedFailover();
    return DoFailoverStatus::RPC_FAILED;
  }

  chosen_replica_instance->PostFailover(main_succ_cb_, main_fail_cb_);
  main_instance->replication_role_ = replication_coordination_glue::ReplicationRole::REPLICA;

  return DoFailoverStatus::SUCCESS;
}

auto CoordinatorData::ShowMain() const -> std::optional<CoordinatorInstanceStatus> {
  std::shared_lock<utils::RWLock> lock{coord_data_lock_};
  auto main_instance = std::ranges::find_if(registered_instances_, &CoordinatorInstance::IsMain);
  if (main_instance == registered_instances_.end()) {
    return std::nullopt;
  }

  return CoordinatorInstanceStatus{.instance_name = main_instance->InstanceName(),
                                   .socket_address = main_instance->SocketAddress(),
                                   .is_alive = main_instance->IsAlive()};
};

auto CoordinatorData::ShowReplicas() const -> std::vector<CoordinatorInstanceStatus> {
  std::shared_lock<utils::RWLock> lock{coord_data_lock_};
  std::vector<CoordinatorInstanceStatus> instances_status;

  for (const auto &replica_instance : registered_instances_ | ranges::views::filter(&CoordinatorInstance::IsReplica)) {
    instances_status.emplace_back(CoordinatorInstanceStatus{.instance_name = replica_instance.InstanceName(),
                                                            .socket_address = replica_instance.SocketAddress(),
                                                            .is_alive = replica_instance.IsAlive()});
  }

  return instances_status;
}

auto CoordinatorData::SetInstanceToMain(std::string instance_name) -> SetInstanceToMainCoordinatorStatus {
  // TODO: (andi) test this
  std::lock_guard<utils::RWLock> lock{coord_data_lock_};

  // Find replica we already registered
  auto registered_replica = std::find_if(
      registered_instances_.begin(), registered_instances_.end(),
      [instance_name](const CoordinatorInstance &instance) { return instance.InstanceName() == instance_name; });

  // if replica not found...
  if (registered_replica == registered_instances_.end()) {
    spdlog::error("You didn't register instance with given name {}", instance_name);
    return SetInstanceToMainCoordinatorStatus::NO_INSTANCE_WITH_NAME;
  }

  // Stop for now because we need to swap success and fail callbacks
  registered_replica->client_.StopFrequentCheck();

  std::vector<CoordinatorClientConfig::ReplicationClientInfo> repl_clients_info;
  repl_clients_info.reserve(registered_instances_.size() - 1);
  std::ranges::for_each(registered_instances_,
                        [registered_replica, &repl_clients_info](const CoordinatorInstance &replica) {
                          if (replica != *registered_replica) {
                            repl_clients_info.emplace_back(replica.client_.ReplicationClientInfo());
                          }
                        });

  // PROMOTE REPLICA TO MAIN
  // THIS SHOULD FAIL HERE IF IT IS DOWN
  if (auto result = registered_replica->client_.SendPromoteReplicaToMainRpc(std::move(repl_clients_info)); !result) {
    registered_replica->client_.StartFrequentCheck();
    return SetInstanceToMainCoordinatorStatus::COULD_NOT_PROMOTE_TO_MAIN;
  }

  registered_replica->replication_role_ = replication_coordination_glue::ReplicationRole::MAIN;
  registered_replica->client_.SetSuccCallback(main_succ_cb_);
  registered_replica->client_.SetFailCallback(main_fail_cb_);

  return SetInstanceToMainCoordinatorStatus::SUCCESS;
}

auto CoordinatorData::RegisterInstance(CoordinatorClientConfig config) -> RegisterInstanceCoordinatorStatus {
  std::lock_guard<utils::RWLock> lock{coord_data_lock_};
  if (std::ranges::any_of(registered_instances_, [&config](const CoordinatorInstance &instance) {
        return instance.InstanceName() == config.instance_name;
      })) {
    return RegisterInstanceCoordinatorStatus::NAME_EXISTS;
  }

  if (std::ranges::any_of(registered_instances_, [&config](const CoordinatorInstance &instance) {
        spdlog::trace("Comparing {} with {}", instance.SocketAddress(), config.SocketAddress());
        return instance.SocketAddress() == config.SocketAddress();
      })) {
    return RegisterInstanceCoordinatorStatus::END_POINT_EXISTS;
  }

  CoordinatorClientConfig::ReplicationClientInfo replication_client_info_copy = config.replication_client_info;

  // TODO (antoniofilipovic) create and then push back
  auto *instance = &registered_instances_.emplace_back(this, std::move(config), replica_succ_cb_, replica_fail_cb_,
                                                       replication_coordination_glue::ReplicationRole::REPLICA);
  if (auto res = instance->client_.SendSetToReplicaRpc(replication_client_info_copy); !res) {
    return RegisterInstanceCoordinatorStatus::RPC_FAILED;
  }

  instance->client_.StartFrequentCheck();

  return RegisterInstanceCoordinatorStatus::SUCCESS;
}

}  // namespace memgraph::coordination
#endif
