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
#include "utils/uuid.hpp"
#ifdef MG_ENTERPRISE

#include "coordination/coordinator_data.hpp"

#include <range/v3/view.hpp>
#include <shared_mutex>
#include "libnuraft/nuraft.hxx"

namespace memgraph::coordination {

CoordinatorData::CoordinatorData() {
  auto find_instance = [](CoordinatorData *coord_data, std::string_view instance_name) -> CoordinatorInstance & {
    auto instance = std::ranges::find_if(
        coord_data->registered_instances_,
        [instance_name](CoordinatorInstance const &instance) { return instance.InstanceName() == instance_name; });

    MG_ASSERT(instance != coord_data->registered_instances_.end(), "Instance {} not found during callback!",
              instance_name);
    return *instance;
  };

  replica_succ_cb_ = [find_instance](CoordinatorData *coord_data, std::string_view instance_name) -> void {
    auto lock = std::lock_guard{coord_data->coord_data_lock_};
    spdlog::trace("Instance {} performing replica successful callback", instance_name);
    find_instance(coord_data, instance_name).OnSuccessPing();
  };

  replica_fail_cb_ = [find_instance](CoordinatorData *coord_data, std::string_view instance_name) -> void {
    auto lock = std::lock_guard{coord_data->coord_data_lock_};
    spdlog::trace("Instance {} performing replica failure callback", instance_name);
    find_instance(coord_data, instance_name).OnFailPing();
  };

  main_succ_cb_ = [find_instance](CoordinatorData *coord_data, std::string_view instance_name) -> void {
    auto lock = std::lock_guard{coord_data->coord_data_lock_};
    spdlog::trace("Instance {} performing main successful callback", instance_name);

    auto &instance = find_instance(coord_data, instance_name);

    if (instance.IsAlive() || !coord_data->ClusterHasAliveMain_()) {
      instance.OnSuccessPing();
      return;
    }

    bool const demoted = instance.DemoteToReplica(coord_data->replica_succ_cb_, coord_data->replica_fail_cb_);
    if (demoted) {
      instance.OnSuccessPing();
      spdlog::info("Instance {} demoted to replica", instance_name);
    } else {
      spdlog::error("Instance {} failed to become replica", instance_name);
    }
  };

  main_fail_cb_ = [find_instance](CoordinatorData *coord_data, std::string_view instance_name) -> void {
    auto lock = std::lock_guard{coord_data->coord_data_lock_};
    spdlog::trace("Instance {} performing main failure callback", instance_name);
    find_instance(coord_data, instance_name).OnFailPing();

    if (!coord_data->ClusterHasAliveMain_()) {
      spdlog::info("Cluster without main instance, trying automatic failover");
      coord_data->TryFailover();
    }
  };
}

auto CoordinatorData::ClusterHasAliveMain_() const -> bool {
  auto const alive_main = [](CoordinatorInstance const &instance) { return instance.IsMain() && instance.IsAlive(); };
  return std::ranges::any_of(registered_instances_, alive_main);
}

auto CoordinatorData::TryFailover() -> void {
  auto replica_instances = registered_instances_ | ranges::views::filter(&CoordinatorInstance::IsReplica);

  auto chosen_replica_instance = std::ranges::find_if(replica_instances, &CoordinatorInstance::IsAlive);
  if (chosen_replica_instance == replica_instances.end()) {
    spdlog::warn("Failover failed since all replicas are down!");
    return;
  }

  chosen_replica_instance->PauseFrequentCheck();
  utils::OnScopeExit scope_exit{[&chosen_replica_instance] { chosen_replica_instance->ResumeFrequentCheck(); }};

  std::vector<ReplClientInfo> repl_clients_info;
  repl_clients_info.reserve(std::ranges::distance(replica_instances));

  auto const not_chosen_replica_instance = [&chosen_replica_instance](CoordinatorInstance const &instance) {
    return instance != *chosen_replica_instance;
  };

  std::ranges::transform(registered_instances_ | ranges::views::filter(not_chosen_replica_instance),
                         std::back_inserter(repl_clients_info),
                         [](const CoordinatorInstance &instance) { return instance.ReplicationClientInfo(); });

  if (!chosen_replica_instance->PromoteToMain(std::move(repl_clients_info), main_succ_cb_, main_fail_cb_)) {
    spdlog::warn("Failover failed since promoting replica to main failed!");
    return;
  }
<<<<<<< HEAD
  spdlog::info("Failover successful! Instance {} promoted to main.", chosen_replica_instance->InstanceName());
=======

  main_id = utils::UUID{};

  for (const auto &unchosen_replica_instance :
       replica_instances | ranges::views::filter(not_chosen_replica_instance) | ranges::views::filter(not_main)) {
    if (auto res = unchosen_replica_instance.client_.SendSwapMainUUIDRpc(main_id); !res) {
      // TODO AF: update message here
      spdlog::error("Failed to swap uuid for replica, aborting failover");
      return DoFailoverStatus::RPC_FAILED;
    }
  }
  spdlog::error(fmt::format("FICO: sending swap main uuid rpc passed {} ", std::string(main_id)));

  if (!chosen_replica_instance->client_.SendPromoteReplicaToMainRpc(main_id, std::move(repl_clients_info))) {
    chosen_replica_instance->RestoreAfterFailedFailover();
    return DoFailoverStatus::RPC_FAILED;
  }

  auto old_main = std::ranges::find_if(registered_instances_, &CoordinatorInstance::IsMain);
  // TODO: (andi) For performing restoration we will have to improve this
  old_main->client_.PauseFrequentCheck();

  chosen_replica_instance->PostFailover(main_succ_cb_, main_fail_cb_);

  return DoFailoverStatus::SUCCESS;
>>>>>>> df7655e8f (add swap uuid rpc, implement rpc in failover and set instance to main funcs)
}

auto CoordinatorData::ShowInstances() const -> std::vector<CoordinatorInstanceStatus> {
  std::vector<CoordinatorInstanceStatus> instances_status;
  instances_status.reserve(registered_instances_.size());

  auto const stringify_repl_role = [](CoordinatorInstance const &instance) -> std::string {
    if (!instance.IsAlive()) return "unknown";
    if (instance.IsMain()) return "main";
    return "replica";
  };

  auto const instance_to_status =
      [&stringify_repl_role](CoordinatorInstance const &instance) -> CoordinatorInstanceStatus {
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

  auto const is_new_main = [&instance_name](CoordinatorInstance const &instance) {
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

  ReplicationClientsInfo repl_clients_info;
  repl_clients_info.reserve(registered_instances_.size() - 1);


  auto const is_not_new_main = [&instance_name](CoordinatorInstance const &instance) {
    return instance.InstanceName() != instance_name;
  };
  

  main_id = utils::UUID{};
  spdlog::error(fmt::format("FICO: UUID for new main is {} ", std::string(main_id)));


  for (const auto &unchosen_replica_instance :
       registered_instances_ | ranges::views::filter(is_not_new_main)) {
    if (auto res = unchosen_replica_instance.client_.SendSwapMainUUIDRpc(main_id); !res) {
      // TODO AF: update message here
      spdlog::error("Failed to swap uuid for replica, aborting failover");
      return SetInstanceToMainCoordinatorStatus::COULD_NOT_PROMOTE_TO_MAIN;
    }
    spdlog::error(fmt::format("FICO: sent new main UUID {} to instance {} ", std::string(main_id),
                              unchosen_replica_instance.InstanceName()));
  }

  // PROMOTE REPLICA TO MAIN
  // THIS SHOULD FAIL HERE IF IT IS DOWN

  std::ranges::transform(registered_instances_ | ranges::views::filter(is_not_new_main),
                         std::back_inserter(repl_clients_info),
                         [](const CoordinatorInstance &instance) { return instance.ReplicationClientInfo(); });

  if (!new_main->PromoteToMain(main_id, std::move(repl_clients_info), main_succ_cb_, main_fail_cb_)) {
    return SetInstanceToMainCoordinatorStatus::COULD_NOT_PROMOTE_TO_MAIN;
  }

  spdlog::info("Instance {} promoted to main", instance_name);
  return SetInstanceToMainCoordinatorStatus::SUCCESS;
}

auto CoordinatorData::RegisterInstance(CoordinatorClientConfig config) -> RegisterInstanceCoordinatorStatus {
  auto lock = std::lock_guard{coord_data_lock_};
  if (std::ranges::any_of(registered_instances_, [&config](CoordinatorInstance const &instance) {
        return instance.InstanceName() == config.instance_name;
      })) {
    return RegisterInstanceCoordinatorStatus::NAME_EXISTS;
  }

  if (std::ranges::any_of(registered_instances_, [&config](CoordinatorInstance const &instance) {
        return instance.SocketAddress() == config.SocketAddress();
      })) {
    return RegisterInstanceCoordinatorStatus::ENDPOINT_EXISTS;
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
