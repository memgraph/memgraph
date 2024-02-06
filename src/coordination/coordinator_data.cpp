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

#include "coordination/register_main_replica_coordinator_status.hpp"
#include "coordination/replication_instance.hpp"
#include "utils/uuid.hpp"

#include <range/v3/view.hpp>
#include <shared_mutex>

namespace memgraph::coordination {

using nuraft::ptr;
using nuraft::srv_config;

CoordinatorData::CoordinatorData() {
  auto find_instance = [](CoordinatorData *coord_data, std::string_view instance_name) -> ReplicationInstance & {
    auto instance = std::ranges::find_if(
        coord_data->repl_instances_,
        [instance_name](ReplicationInstance const &instance) { return instance.InstanceName() == instance_name; });

    MG_ASSERT(instance != coord_data->repl_instances_.end(), "Instance {} not found during callback!", instance_name);
    return *instance;
  };

  replica_succ_cb_ = [this, find_instance](CoordinatorData *coord_data, std::string_view instance_name) -> void {
    auto lock = std::lock_guard{coord_data->coord_data_lock_};
    spdlog::trace("Instance {} performing replica successful callback", instance_name);
    auto &instance = find_instance(coord_data, instance_name);

    if (!instance.GetMainUUID().has_value() || main_uuid_ != instance.GetMainUUID().value()) {
      if (!instance.SendSwapAndUpdateUUID(main_uuid_)) {
        spdlog::error(
            fmt::format("Failed to swap uuid for replica instance {} which is alive", instance.InstanceName()));
        return;
      }
    }

    instance.OnSuccessPing();
  };

  replica_fail_cb_ = [find_instance](CoordinatorData *coord_data, std::string_view instance_name) -> void {
    auto lock = std::lock_guard{coord_data->coord_data_lock_};
    spdlog::trace("Instance {} performing replica failure callback", instance_name);
    auto &instance = find_instance(coord_data, instance_name);
    instance.OnFailPing();
    // We need to restart main uuid from instance since it was "down" at least a second
    // There is slight delay, if we choose to use isAlive, instance can be down and back up in less than
    // our isAlive time difference, which would lead to instance setting UUID to nullopt and stopping accepting any
    // incoming RPCs from valid main
    // TODO(antoniofilipovic) this needs here more complex logic
    // We need to get id of main replica is listening to on successful ping
    // and swap it to correct uuid if it failed
    instance.SetNewMainUUID();
  };

  main_succ_cb_ = [this, find_instance](CoordinatorData *coord_data, std::string_view instance_name) -> void {
    auto lock = std::lock_guard{coord_data->coord_data_lock_};
    spdlog::trace("Instance {} performing main successful callback", instance_name);

    auto &instance = find_instance(coord_data, instance_name);

    if (instance.IsAlive()) {
      instance.OnSuccessPing();
      return;
    }

    const auto &instance_uuid = instance.GetMainUUID();
    MG_ASSERT(instance_uuid.has_value(), "Instance must have uuid set");
    if (main_uuid_ == instance_uuid.value()) {
      instance.OnSuccessPing();
      return;
    }

    // TODO(antoniof) make demoteToReplica idempotent since main can be demoted to replica but
    // swapUUID can fail
    bool const demoted = instance.DemoteToReplica(coord_data->replica_succ_cb_, coord_data->replica_fail_cb_);
    if (demoted) {
      instance.OnSuccessPing();
      spdlog::info("Instance {} demoted to replica", instance_name);
    } else {
      spdlog::error("Instance {} failed to become replica", instance_name);
      return;
    }

    if (!instance.SendSwapAndUpdateUUID(main_uuid_)) {
      spdlog::error(fmt::format("Failed to swap uuid for demoted main instance {}", instance.InstanceName()));
      return;
    }
  };

  main_fail_cb_ = [this, find_instance](CoordinatorData *coord_data, std::string_view instance_name) -> void {
    auto lock = std::lock_guard{coord_data->coord_data_lock_};
    spdlog::trace("Instance {} performing main failure callback", instance_name);
    auto &instance = find_instance(coord_data, instance_name);
    instance.OnFailPing();
    const auto &instance_uuid = instance.GetMainUUID();
    MG_ASSERT(instance_uuid.has_value(), "Instance must have uuid set");

    if (!instance.IsAlive() && main_uuid_ == instance_uuid.value()) {
      spdlog::info("Cluster without main instance, trying automatic failover");
      coord_data->TryFailover();
    }
  };
}

auto CoordinatorData::TryFailover() -> void {
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

auto CoordinatorData::ShowInstances() const -> std::vector<InstanceStatus> {
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
    auto lock = std::shared_lock{coord_data_lock_};
    std::ranges::transform(repl_instances_, std::back_inserter(instances_status), repl_instance_to_status);
  }

  return instances_status;
}

// TODO: (andi) Make sure you cannot put coordinator instance to the main
auto CoordinatorData::SetInstanceToMain(std::string instance_name) -> SetInstanceToMainCoordinatorStatus {
  auto lock = std::lock_guard{coord_data_lock_};

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

auto CoordinatorData::RegisterInstance(CoordinatorClientConfig config) -> RegisterInstanceCoordinatorStatus {
  auto lock = std::lock_guard{coord_data_lock_};

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

  try {
    auto *repl_instance = &repl_instances_.emplace_back(this, std::move(config), replica_succ_cb_, replica_fail_cb_);
    if (self_.IsLeader()) {
      repl_instance->StartFrequentCheck();
    }
    return RegisterInstanceCoordinatorStatus::SUCCESS;

  } catch (CoordinatorRegisterInstanceException const &) {
    return RegisterInstanceCoordinatorStatus::RPC_FAILED;
  }
}

auto CoordinatorData::AddCoordinatorInstance(uint32_t raft_server_id, uint32_t raft_port, std::string raft_address)
    -> void {
  self_.AddCoordinatorInstance(raft_server_id, raft_port, std::move(raft_address));
}

}  // namespace memgraph::coordination
#endif
