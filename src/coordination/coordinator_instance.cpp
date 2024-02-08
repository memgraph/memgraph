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

#include "coordination/coordinator_instance.hpp"

#include "coordination/coordinator_exceptions.hpp"
#include "nuraft/coordinator_state_machine.hpp"
#include "nuraft/coordinator_state_manager.hpp"
#include "utils/counter.hpp"

#include <range/v3/view.hpp>
#include <shared_mutex>

namespace memgraph::coordination {

using nuraft::ptr;
using nuraft::srv_config;

CoordinatorInstance::CoordinatorInstance()
    : self_([this] { std::ranges::for_each(repl_instances_, &ReplicationInstance::StartFrequentCheck); },
            [this] { std::ranges::for_each(repl_instances_, &ReplicationInstance::StopFrequentCheck); }) {
  auto find_instance = [](CoordinatorInstance *coord_instance,
                          std::string_view instance_name) -> ReplicationInstance & {
    auto instance = std::ranges::find_if(
        coord_instance->repl_instances_,
        [instance_name](ReplicationInstance const &instance) { return instance.InstanceName() == instance_name; });

    MG_ASSERT(instance != coord_instance->repl_instances_.end(), "Instance {} not found during callback!",
              instance_name);
    return *instance;
  };

  replica_succ_cb_ = [find_instance](CoordinatorInstance *coord_instance, std::string_view instance_name) -> void {
    auto lock = std::lock_guard{coord_instance->coord_instance_lock_};
    spdlog::trace("Instance {} performing replica successful callback", instance_name);
    find_instance(coord_instance, instance_name).OnSuccessPing();
  };

  replica_fail_cb_ = [find_instance](CoordinatorInstance *coord_instance, std::string_view instance_name) -> void {
    auto lock = std::lock_guard{coord_instance->coord_instance_lock_};
    spdlog::trace("Instance {} performing replica failure callback", instance_name);
    find_instance(coord_instance, instance_name).OnFailPing();
  };

  main_succ_cb_ = [find_instance](CoordinatorInstance *coord_instance, std::string_view instance_name) -> void {
    auto lock = std::lock_guard{coord_instance->coord_instance_lock_};
    spdlog::trace("Instance {} performing main successful callback", instance_name);

    auto &instance = find_instance(coord_instance, instance_name);

    if (instance.IsAlive()) {
      instance.OnSuccessPing();
      return;
    }

    bool const is_latest_main = !coord_instance->ClusterHasAliveMain_();
    if (is_latest_main) {
      spdlog::info("Instance {} is the latest main", instance_name);
      instance.OnSuccessPing();
      return;
    }

    bool const demoted = instance.DemoteToReplica(coord_instance->replica_succ_cb_, coord_instance->replica_fail_cb_);
    if (demoted) {
      instance.OnSuccessPing();
      spdlog::info("Instance {} demoted to replica", instance_name);
    } else {
      spdlog::error("Instance {} failed to become replica", instance_name);
    }
  };

  main_fail_cb_ = [find_instance](CoordinatorInstance *coord_instance, std::string_view instance_name) -> void {
    auto lock = std::lock_guard{coord_instance->coord_instance_lock_};
    spdlog::trace("Instance {} performing main failure callback", instance_name);
    find_instance(coord_instance, instance_name).OnFailPing();

    if (!coord_instance->ClusterHasAliveMain_()) {
      spdlog::info("Cluster without main instance, trying automatic failover");
      coord_instance->TryFailover();
    }
  };
}

auto CoordinatorInstance::ClusterHasAliveMain_() const -> bool {
  auto const alive_main = [](ReplicationInstance const &instance) { return instance.IsMain() && instance.IsAlive(); };
  return std::ranges::any_of(repl_instances_, alive_main);
}

auto CoordinatorInstance::TryFailover() -> void {
  auto alive_replicas = repl_instances_ | ranges::views::filter(&ReplicationInstance::IsReplica) |
                        ranges::views::filter(&ReplicationInstance::IsAlive);

  if (ranges::empty(alive_replicas)) {
    spdlog::warn("Failover failed since all replicas are down!");
    return;
  }

  // TODO: Smarter choice
  auto chosen_replica_instance = ranges::begin(alive_replicas);

  chosen_replica_instance->PauseFrequentCheck();
  utils::OnScopeExit scope_exit{[&chosen_replica_instance] { chosen_replica_instance->ResumeFrequentCheck(); }};

  auto const potential_new_main_uuid = utils::UUID{};

  auto const is_not_chosen_replica_instance = [&chosen_replica_instance](ReplicationInstance &instance) {
    return instance != *chosen_replica_instance;
  };

  // If for some replicas swap fails, for others on successful ping we will revert back on next change
  // or we will do failover first again and then it will be consistent again
  for (auto &other_replica_instance : alive_replicas | ranges::views::filter(is_not_chosen_replica_instance)) {
    if (!other_replica_instance.SendSwapAndUpdateUUID(potential_new_main_uuid)) {
      spdlog::error(fmt::format("Failed to swap uuid for instance {} which is alive, aborting failover",
                                other_replica_instance.InstanceName()));
      return;
    }
  }

  std::vector<ReplClientInfo> repl_clients_info;
  repl_clients_info.reserve(repl_instances_.size() - 1);
  std::ranges::transform(repl_instances_ | ranges::views::filter(is_not_chosen_replica_instance),
                         std::back_inserter(repl_clients_info), &ReplicationInstance::ReplicationClientInfo);

  if (!chosen_replica_instance->PromoteToMain(potential_new_main_uuid, std::move(repl_clients_info), main_succ_cb_,
                                              main_fail_cb_)) {
    spdlog::warn("Failover failed since promoting replica to main failed!");
    return;
  }
  chosen_replica_instance->SetNewMainUUID(potential_new_main_uuid);
  main_uuid_ = potential_new_main_uuid;

  spdlog::info("Failover successful! Instance {} promoted to main.", chosen_replica_instance->InstanceName());
}

auto CoordinatorInstance::ShowInstances() const -> std::vector<InstanceStatus> {
  auto const coord_instances = self_.GetAllCoordinators();

  std::vector<InstanceStatus> instances_status;
  instances_status.reserve(repl_instances_.size() + coord_instances.size());

  auto const stringify_repl_role = [](ReplicationInstance const &instance) -> std::string {
    if (!instance.IsAlive()) return "unknown";
    if (instance.IsMain()) return "main";
    return "replica";
  };

  auto const repl_instance_to_status = [&stringify_repl_role](ReplicationInstance const &instance) -> InstanceStatus {
    return {.instance_name = instance.InstanceName(),
            .coord_socket_address = instance.SocketAddress(),
            .cluster_role = stringify_repl_role(instance),
            .is_alive = instance.IsAlive()};
  };

  auto const coord_instance_to_status = [](ptr<srv_config> const &instance) -> InstanceStatus {
    return {.instance_name = "coordinator_" + std::to_string(instance->get_id()),
            .raft_socket_address = instance->get_endpoint(),
            .cluster_role = "coordinator",
            .is_alive = true};  // TODO: (andi) Get this info from RAFT and test it or when we will move
                                // CoordinatorState to every instance, we can be smarter about this using our RPC.
  };

  std::ranges::transform(coord_instances, std::back_inserter(instances_status), coord_instance_to_status);

  {
    auto lock = std::shared_lock{coord_instance_lock_};
    std::ranges::transform(repl_instances_, std::back_inserter(instances_status), repl_instance_to_status);
  }

  return instances_status;
}

// TODO: (andi) Make sure you cannot put coordinator instance to the main
auto CoordinatorInstance::SetReplicationInstanceToMain(std::string instance_name)
    -> SetInstanceToMainCoordinatorStatus {
  auto lock = std::lock_guard{coord_instance_lock_};

  auto const is_new_main = [&instance_name](ReplicationInstance const &instance) {
    return instance.InstanceName() == instance_name;
  };
  auto new_main = std::ranges::find_if(repl_instances_, is_new_main);

  if (new_main == repl_instances_.end()) {
    spdlog::error("Instance {} not registered. Please register it using REGISTER INSTANCE {}", instance_name,
                  instance_name);
    return SetInstanceToMainCoordinatorStatus::NO_INSTANCE_WITH_NAME;
  }

  new_main->PauseFrequentCheck();
  utils::OnScopeExit scope_exit{[&new_main] { new_main->ResumeFrequentCheck(); }};

  ReplicationClientsInfo repl_clients_info;
  repl_clients_info.reserve(repl_instances_.size() - 1);

  auto const is_not_new_main = [&instance_name](ReplicationInstance const &instance) {
    return instance.InstanceName() != instance_name;
  };

  auto potential_new_main_uuid = utils::UUID{};
  spdlog::trace("Generated potential new main uuid");

  for (auto &other_instance : repl_instances_ | ranges::views::filter(is_not_new_main)) {
    if (!other_instance.SendSwapAndUpdateUUID(potential_new_main_uuid)) {
      spdlog::error(
          fmt::format("Failed to swap uuid for instance {}, aborting failover", other_instance.InstanceName()));
      return SetInstanceToMainCoordinatorStatus::SWAP_UUID_FAILED;
    }
  }

  std::ranges::transform(repl_instances_ | ranges::views::filter(is_not_new_main),
                         std::back_inserter(repl_clients_info),
                         [](const ReplicationInstance &instance) { return instance.ReplicationClientInfo(); });

  if (!new_main->PromoteToMain(potential_new_main_uuid, std::move(repl_clients_info), main_succ_cb_, main_fail_cb_)) {
    return SetInstanceToMainCoordinatorStatus::COULD_NOT_PROMOTE_TO_MAIN;
  }

  new_main->SetNewMainUUID(potential_new_main_uuid);
  main_uuid_ = potential_new_main_uuid;
  spdlog::info("Instance {} promoted to main", instance_name);
  return SetInstanceToMainCoordinatorStatus::SUCCESS;
}

auto CoordinatorInstance::RegisterReplicationInstance(CoordinatorClientConfig config)
    -> RegisterInstanceCoordinatorStatus {
  auto lock = std::lock_guard{coord_instance_lock_};

  auto const name_matches = [&config](ReplicationInstance const &instance) {
    return instance.InstanceName() == config.instance_name;
  };

  if (std::ranges::any_of(repl_instances_, name_matches)) {
    return RegisterInstanceCoordinatorStatus::NAME_EXISTS;
  }

  auto const socket_address_matches = [&config](ReplicationInstance const &instance) {
    return instance.SocketAddress() == config.SocketAddress();
  };

  if (std::ranges::any_of(repl_instances_, socket_address_matches)) {
    return RegisterInstanceCoordinatorStatus::ENDPOINT_EXISTS;
  }

  if (!self_.RequestLeadership()) {
    return RegisterInstanceCoordinatorStatus::NOT_LEADER;
  }

  auto const res = self_.AppendRegisterReplicationInstance(config.instance_name);
  if (!res->get_accepted()) {
    spdlog::error(
        "Failed to accept request for registering instance {}. Most likely the reason is that the instance is not the "
        "leader.",
        config.instance_name);
    return RegisterInstanceCoordinatorStatus::RAFT_COULD_NOT_ACCEPT;
  }

  spdlog::info("Request for registering instance {} accepted", config.instance_name);
  try {
    repl_instances_.emplace_back(this, std::move(config), replica_succ_cb_, replica_fail_cb_);
  } catch (CoordinatorRegisterInstanceException const &) {
    return RegisterInstanceCoordinatorStatus::RPC_FAILED;
  }

  if (res->get_result_code() != nuraft::cmd_result_code::OK) {
    spdlog::error("Failed to register instance {} with error code {}", config.instance_name, res->get_result_code());
    return RegisterInstanceCoordinatorStatus::RAFT_COULD_NOT_APPEND;
  }

  spdlog::info("Instance {} registered", config.instance_name);
  return RegisterInstanceCoordinatorStatus::SUCCESS;
}

auto CoordinatorInstance::AddCoordinatorInstance(uint32_t raft_server_id, uint32_t raft_port, std::string raft_address)
    -> void {
  self_.AddCoordinatorInstance(raft_server_id, raft_port, std::move(raft_address));
}

}  // namespace memgraph::coordination
#endif
