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
      spdlog::warn("Main is not alive, starting automatic failover");
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

  auto chosen_replica_instance =
      std::ranges::find_if(replica_instances, [](const CoordinatorInstance &instance) { return instance.IsAlive(); });

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
    if (auto repl_client_info = unchosen_replica_instance.client_.ReplicationClientInfo();
        repl_client_info.has_value()) {
      repl_clients_info.emplace_back(std::move(repl_client_info.value()));
    }
  }

  if (!chosen_replica_instance->client_.SendPromoteReplicaToMainRpc(std::move(repl_clients_info))) {
    // TODO: new status and rollback all changes that were done...
    MG_ASSERT(false, "Promoting replica to main failed!");
  }

  chosen_replica_instance->client_.SetSuccCallback(main_succ_cb_);
  chosen_replica_instance->client_.SetFailCallback(main_fail_cb_);
  chosen_replica_instance->client_.ResetReplicationClientInfo();
  chosen_replica_instance->client_.ResumeFrequentCheck();
  chosen_replica_instance->replication_role_ = replication_coordination_glue::ReplicationRole::MAIN;

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

auto CoordinatorData::RegisterMain(CoordinatorClientConfig config) -> RegisterMainReplicaCoordinatorStatus {
  // TODO: (andi) test this
  std::lock_guard<utils::RWLock> lock{coord_data_lock_};

  if (std::ranges::any_of(registered_instances_, [&config](const CoordinatorInstance &instance) {
        return instance.InstanceName() == config.instance_name;
      })) {
    return RegisterMainReplicaCoordinatorStatus::NAME_EXISTS;
  }

  if (std::ranges::any_of(registered_instances_, [&config](const CoordinatorInstance &instance) {
        return instance.SocketAddress() == config.SocketAddress();
      })) {
    return RegisterMainReplicaCoordinatorStatus::ENDPOINT_EXISTS;
  }

  auto *instance = &registered_instances_.emplace_back(this, std::move(config), main_succ_cb_, main_fail_cb_,
                                                       replication_coordination_glue::ReplicationRole::MAIN);
  instance->client_.StartFrequentCheck();

  return RegisterMainReplicaCoordinatorStatus::SUCCESS;
}

auto CoordinatorData::RegisterReplica(CoordinatorClientConfig config) -> RegisterMainReplicaCoordinatorStatus {
  std::lock_guard<utils::RWLock> lock{coord_data_lock_};
  if (std::ranges::any_of(registered_instances_, [&config](const CoordinatorInstance &instance) {
        return instance.InstanceName() == config.instance_name;
      })) {
    return RegisterMainReplicaCoordinatorStatus::NAME_EXISTS;
  }

  if (std::ranges::any_of(registered_instances_, [&config](const CoordinatorInstance &instance) {
        spdlog::trace("Comparing {} with {}", instance.SocketAddress(), config.SocketAddress());
        return instance.SocketAddress() == config.SocketAddress();
      })) {
    return RegisterMainReplicaCoordinatorStatus::ENDPOINT_EXISTS;
  }

  auto *instance = &registered_instances_.emplace_back(this, std::move(config), replica_succ_cb_, replica_fail_cb_,
                                                       replication_coordination_glue::ReplicationRole::REPLICA);
  instance->client_.StartFrequentCheck();

  return RegisterMainReplicaCoordinatorStatus::SUCCESS;
}

}  // namespace memgraph::coordination
#endif
