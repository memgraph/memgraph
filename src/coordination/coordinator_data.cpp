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
  std::vector<CoordinatorInstance *> alive_registered_replica_instances{};
  std::ranges::transform(registered_instances_ | ranges::views::filter(&CoordinatorInstance::IsReplica) |
                             ranges::views::filter(&CoordinatorInstance::IsAlive),
                         std::back_inserter(alive_registered_replica_instances),
                         [](CoordinatorInstance &instance) { return &instance; });

  // TODO(antoniof) more complex logic of choosing replica instance
  CoordinatorInstance *chosen_replica_instance =
      !alive_registered_replica_instances.empty() ? alive_registered_replica_instances[0] : nullptr;

  if (nullptr == chosen_replica_instance) {
    spdlog::warn("Failover failed since all replicas are down!");
    return;
  }

  chosen_replica_instance->PauseFrequentCheck();
  utils::OnScopeExit scope_exit{[&chosen_replica_instance] { chosen_replica_instance->ResumeFrequentCheck(); }};

  utils::UUID potential_new_main_uuid = utils::UUID{};
  spdlog::trace("Generated potential new main uuid");

  auto not_chosen_instance = [chosen_replica_instance](auto *instance) {
    return *instance != *chosen_replica_instance;
  };
  // If for some replicas swap fails, for others on successful ping we will revert back on next change
  // or we will do failover first again and then it will be consistent again
  for (auto *other_replica_instance : alive_registered_replica_instances | ranges::views::filter(not_chosen_instance)) {
    if (!other_replica_instance->SendSwapAndUpdateUUID(potential_new_main_uuid)) {
      spdlog::error(fmt::format("Failed to swap uuid for instance {} which is alive, aborting failover",
                                other_replica_instance->InstanceName()));
      return;
    }
  }

  std::vector<ReplClientInfo> repl_clients_info;
  repl_clients_info.reserve(registered_instances_.size() - 1);

  std::ranges::transform(registered_instances_ | ranges::views::filter([chosen_replica_instance](const auto &instance) {
                           return *chosen_replica_instance != instance;
                         }),
                         std::back_inserter(repl_clients_info),
                         [](const CoordinatorInstance &instance) { return instance.ReplicationClientInfo(); });

  if (!chosen_replica_instance->PromoteToMain(potential_new_main_uuid, std::move(repl_clients_info), main_succ_cb_,
                                              main_fail_cb_)) {
    spdlog::warn("Failover failed since promoting replica to main failed!");
    return;
  }
  chosen_replica_instance->SetNewMainUUID(potential_new_main_uuid);
  main_uuid_ = potential_new_main_uuid;

  spdlog::info("Failover successful! Instance {} promoted to main.", chosen_replica_instance->InstanceName());
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

  auto potential_new_main_uuid = utils::UUID{};
  spdlog::trace("Generated potential new main uuid");

  for (auto &other_instance : registered_instances_ | ranges::views::filter(is_not_new_main)) {
    if (!other_instance.SendSwapAndUpdateUUID(potential_new_main_uuid)) {
      spdlog::error(
          fmt::format("Failed to swap uuid for instance {}, aborting failover", other_instance.InstanceName()));
      return SetInstanceToMainCoordinatorStatus::SWAP_UUID_FAILED;
    }
  }

  std::ranges::transform(registered_instances_ | ranges::views::filter(is_not_new_main),
                         std::back_inserter(repl_clients_info),
                         [](const CoordinatorInstance &instance) { return instance.ReplicationClientInfo(); });

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
