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
    auto instance = std::ranges::find_if(
        coord_data->registered_instances_,
        [instance_name](const CoordinatorInstance &instance) { return instance.InstanceName() == instance_name; });

    MG_ASSERT(instance != coord_data->registered_instances_.end(), "Instance {} not found during callback!",
              instance_name);
    return *instance;
  };

  replica_succ_cb_ = [find_instance](CoordinatorData *coord_data, std::string_view instance_name) -> void {
    auto lock = std::lock_guard{coord_data->coord_data_lock_};
    spdlog::trace("Instance {} performing replica successful callback", instance_name);
    auto &instance = find_instance(coord_data, instance_name);
    instance.OnSuccessPing();
  };

  replica_fail_cb_ = [find_instance](CoordinatorData *coord_data, std::string_view instance_name) -> void {
    auto lock = std::lock_guard{coord_data->coord_data_lock_};
    spdlog::trace("Instance {} performing replica failure callback", instance_name);
    auto &instance = find_instance(coord_data, instance_name);
    instance.OnFailPing();
  };

  main_succ_cb_ = [this, find_instance](CoordinatorData *coord_data, std::string_view instance_name) -> void {
    auto lock = std::lock_guard{coord_data->coord_data_lock_};
    spdlog::trace("Instance {} performing main successful callback", instance_name);

    auto &instance = find_instance(coord_data, instance_name);

    if (instance.IsAlive()) {
      instance.OnSuccessPing();
    } else {
      auto const new_role = coord_data->ClusterHasAliveMain() ? replication_coordination_glue::ReplicationRole::REPLICA
                                                              : replication_coordination_glue::ReplicationRole::MAIN;
      if (new_role == replication_coordination_glue::ReplicationRole::REPLICA) {
        thread_pool_.AddTask([&instance, coord_data, instance_name]() {
          auto lock = std::lock_guard{coord_data->coord_data_lock_};
          spdlog::info("Demoting instance {} to replica", instance_name);
          instance.PauseFrequentCheck();
          utils::OnScopeExit scope_exit{[&instance] { instance.ResumeFrequentCheck(); }};
          auto const status = instance.DemoteToReplica(coord_data->replica_succ_cb_, coord_data->replica_fail_cb_);
          if (!status) {
            spdlog::error("Instance {} failed to demote to replica", instance_name);
          } else {
            spdlog::info("Instance {} demoted to replica", instance_name);
            instance.OnSuccessPing();
          }
        });
      } else {
        instance.OnSuccessPing();
      }
    }
  };

  main_fail_cb_ = [this, find_instance](CoordinatorData *coord_data, std::string_view instance_name) -> void {
    auto lock = std::lock_guard{coord_data->coord_data_lock_};
    spdlog::trace("Instance {} performing main failure callback", instance_name);
    auto &instance = find_instance(coord_data, instance_name);
    instance.OnFailPing();

    if (!ClusterHasAliveMain()) {
      spdlog::info("Cluster without main instance, starting automatic failover");
      switch (auto failover_status = DoFailover(); failover_status) {
        using enum DoFailoverStatus;
        case ALL_REPLICAS_DOWN:
          spdlog::warn("Failover aborted since all replicas are down!");
          break;
        case MAIN_ALIVE:
          spdlog::warn("Failover aborted since main is alive!");
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

auto CoordinatorData::ClusterHasAliveMain() const -> bool {
  auto const alive_main = [](const CoordinatorInstance &instance) { return instance.IsMain() && instance.IsAlive(); };
  return std::ranges::any_of(registered_instances_, alive_main);
}

auto CoordinatorData::DoFailover() -> DoFailoverStatus {
  auto replica_instances = registered_instances_ | ranges::views::filter(&CoordinatorInstance::IsReplica);

  auto chosen_replica_instance = std::ranges::find_if(replica_instances, &CoordinatorInstance::IsAlive);
  if (chosen_replica_instance == replica_instances.end()) {
    return DoFailoverStatus::ALL_REPLICAS_DOWN;
  }

  chosen_replica_instance->PauseFrequentCheck();
  utils::OnScopeExit scope_exit{[&chosen_replica_instance] { chosen_replica_instance->ResumeFrequentCheck(); }};

  std::vector<ReplClientInfo> repl_clients_info;
  repl_clients_info.reserve(std::ranges::distance(replica_instances));

  auto const not_chosen_replica_instance = [&chosen_replica_instance](const CoordinatorInstance &instance) {
    return instance != *chosen_replica_instance;
  };

  std::ranges::transform(replica_instances | ranges::views::filter(not_chosen_replica_instance),
                         std::back_inserter(repl_clients_info),
                         [](const CoordinatorInstance &instance) { return instance.ReplicationClientInfo(); });

  if (!chosen_replica_instance->PromoteToMain(std::move(repl_clients_info), main_succ_cb_, main_fail_cb_)) {
    return DoFailoverStatus::RPC_FAILED;
  }
  return DoFailoverStatus::SUCCESS;
}

auto CoordinatorData::ShowInstances() const -> std::vector<CoordinatorInstanceStatus> {
  std::vector<CoordinatorInstanceStatus> instances_status;
  instances_status.reserve(registered_instances_.size());

  auto const stringify_repl_role = [](const CoordinatorInstance &instance) -> std::string {
    if (!instance.IsAlive()) return "unknown";
    if (instance.IsMain()) return "main";
    return "replica";
  };

  auto const instance_to_status =
      [&stringify_repl_role](const CoordinatorInstance &instance) -> CoordinatorInstanceStatus {
    return {.instance_name = instance.InstanceName(),
            .socket_address = instance.SocketAddress(),
            .replication_role = stringify_repl_role(instance),
            .is_alive = instance.IsAlive()};
  };

  {
    auto lock = std::shared_lock{coord_data_lock_};
    std::ranges::transform(registered_instances_, std::back_inserter(instances_status), instance_to_status);
  }

  return instances_status;
}

auto CoordinatorData::SetInstanceToMain(std::string instance_name) -> SetInstanceToMainCoordinatorStatus {
  auto lock = std::lock_guard{coord_data_lock_};

  auto const is_new_main = [&instance_name](const CoordinatorInstance &instance) {
    return instance.InstanceName() == instance_name;
  };
  auto new_main = std::ranges::find_if(registered_instances_, is_new_main);

  if (new_main == registered_instances_.end()) {
    spdlog::error("Instance {} not registered. Please register it using REGISTER INSTANCE {}", instance_name,
                  instance_name);
    return SetInstanceToMainCoordinatorStatus::NO_INSTANCE_WITH_NAME;
  }

  new_main->PauseFrequentCheck();
  utils::OnScopeExit scope_exit{[&new_main] { new_main->ResumeFrequentCheck(); }};

  std::vector<CoordinatorClientConfig::ReplicationClientInfo> repl_clients_info;
  repl_clients_info.reserve(registered_instances_.size() - 1);

  auto const is_not_new_main = [&instance_name](const CoordinatorInstance &instance) {
    return instance.InstanceName() != instance_name;
  };
  std::ranges::transform(registered_instances_ | ranges::views::filter(is_not_new_main),
                         std::back_inserter(repl_clients_info),
                         [](const CoordinatorInstance &instance) { return instance.ReplicationClientInfo(); });

  if (!new_main->PromoteToMain(std::move(repl_clients_info), main_succ_cb_, main_fail_cb_)) {
    return SetInstanceToMainCoordinatorStatus::COULD_NOT_PROMOTE_TO_MAIN;
  }

  spdlog::info("Instance {} promoted to main", instance_name);
  return SetInstanceToMainCoordinatorStatus::SUCCESS;
}

auto CoordinatorData::RegisterInstance(CoordinatorClientConfig config) -> RegisterInstanceCoordinatorStatus {
  auto lock = std::lock_guard{coord_data_lock_};
  if (std::ranges::any_of(registered_instances_, [&config](const CoordinatorInstance &instance) {
        return instance.InstanceName() == config.instance_name;
      })) {
    return RegisterInstanceCoordinatorStatus::NAME_EXISTS;
  }

  if (std::ranges::any_of(registered_instances_, [&config](const CoordinatorInstance &instance) {
        return instance.SocketAddress() == config.SocketAddress();
      })) {
    return RegisterInstanceCoordinatorStatus::END_POINT_EXISTS;
  }

  try {
    registered_instances_.emplace_back(this, std::move(config), replica_succ_cb_, replica_fail_cb_);
    return RegisterInstanceCoordinatorStatus::SUCCESS;

  } catch (CoordinatorRegisterInstanceException const &) {
    return RegisterInstanceCoordinatorStatus::RPC_FAILED;
  }
}

}  // namespace memgraph::coordination
#endif
