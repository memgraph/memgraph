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
    : raft_state_(RaftState::MakeRaftState(
          [this] { std::ranges::for_each(repl_instances_, &ReplicationInstance::StartFrequentCheck); },
          [this] { std::ranges::for_each(repl_instances_, &ReplicationInstance::StopFrequentCheck); })) {
  auto find_repl_instance = [](CoordinatorInstance *self,
                               std::string_view repl_instance_name) -> ReplicationInstance & {
    auto repl_instance =
        std::ranges::find_if(self->repl_instances_, [repl_instance_name](ReplicationInstance const &instance) {
          return instance.InstanceName() == repl_instance_name;
        });

    MG_ASSERT(repl_instance != self->repl_instances_.end(), "Instance {} not found during callback!",
              repl_instance_name);
    return *repl_instance;
  };

  replica_succ_cb_ = [find_repl_instance](CoordinatorInstance *self, std::string_view repl_instance_name) -> void {
    auto lock = std::lock_guard{self->coord_instance_lock_};
    spdlog::trace("Instance {} performing replica successful callback", repl_instance_name);
    auto &repl_instance = find_repl_instance(self, repl_instance_name);

    if (!repl_instance.EnsureReplicaHasCorrectMainUUID(self->GetMainUUID())) {
      spdlog::error(
          fmt::format("Failed to swap uuid for replica instance {} which is alive", repl_instance.InstanceName()));
      return;
    }

    repl_instance.OnSuccessPing();
  };

  replica_fail_cb_ = [find_repl_instance](CoordinatorInstance *self, std::string_view repl_instance_name) -> void {
    auto lock = std::lock_guard{self->coord_instance_lock_};
    spdlog::trace("Instance {} performing replica failure callback", repl_instance_name);
    auto &repl_instance = find_repl_instance(self, repl_instance_name);
    repl_instance.OnFailPing();
    // We need to restart main uuid from instance since it was "down" at least a second
    // There is slight delay, if we choose to use isAlive, instance can be down and back up in less than
    // our isAlive time difference, which would lead to instance setting UUID to nullopt and stopping accepting any
    // incoming RPCs from valid main
    // TODO(antoniofilipovic) this needs here more complex logic
    // We need to get id of main replica is listening to on successful ping
    // and swap it to correct uuid if it failed
    repl_instance.ResetMainUUID();
  };

  main_succ_cb_ = [find_repl_instance](CoordinatorInstance *self, std::string_view repl_instance_name) -> void {
    auto lock = std::lock_guard{self->coord_instance_lock_};
    spdlog::trace("Instance {} performing main successful callback", repl_instance_name);

    auto &repl_instance = find_repl_instance(self, repl_instance_name);

    if (repl_instance.IsAlive()) {
      repl_instance.OnSuccessPing();
      return;
    }

    const auto &repl_instance_uuid = repl_instance.GetMainUUID();
    MG_ASSERT(repl_instance_uuid.has_value(), "Instance must have uuid set.");

    auto const curr_main_uuid = self->GetMainUUID();
    if (curr_main_uuid == repl_instance_uuid.value()) {
      repl_instance.OnSuccessPing();
      return;
    }

    // TODO(antoniof) make demoteToReplica idempotent since main can be demoted to replica but
    // swapUUID can fail
    if (repl_instance.DemoteToReplica(self->replica_succ_cb_, self->replica_fail_cb_)) {
      repl_instance.OnSuccessPing();
      spdlog::info("Instance {} demoted to replica", repl_instance_name);
    } else {
      spdlog::error("Instance {} failed to become replica", repl_instance_name);
      return;
    }

    if (!repl_instance.SendSwapAndUpdateUUID(curr_main_uuid)) {
      spdlog::error(fmt::format("Failed to swap uuid for demoted main instance {}", repl_instance.InstanceName()));
      return;
    }
  };

  main_fail_cb_ = [find_repl_instance](CoordinatorInstance *self, std::string_view repl_instance_name) -> void {
    auto lock = std::lock_guard{self->coord_instance_lock_};
    spdlog::trace("Instance {} performing main failure callback", repl_instance_name);
    auto &repl_instance = find_repl_instance(self, repl_instance_name);
    repl_instance.OnFailPing();
    const auto &repl_instance_uuid = repl_instance.GetMainUUID();
    MG_ASSERT(repl_instance_uuid.has_value(), "Instance must have uuid set");

    if (!repl_instance.IsAlive() && self->GetMainUUID() == repl_instance_uuid.value()) {
      spdlog::info("Cluster without main instance, trying automatic failover");
      self->TryFailover();  // TODO: (andi) Initiate failover
    }
  };
}

auto CoordinatorInstance::ClusterHasAliveMain_() const -> bool {
  auto const alive_main = [](ReplicationInstance const &instance) { return instance.IsMain() && instance.IsAlive(); };
  return std::ranges::any_of(repl_instances_, alive_main);
}

auto CoordinatorInstance::ShowInstances() const -> std::vector<InstanceStatus> {
  auto const coord_instances = raft_state_.GetAllCoordinators();

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

auto CoordinatorInstance::TryFailover() -> void {
  auto alive_replicas = repl_instances_ | ranges::views::filter(&ReplicationInstance::IsReplica) |
                        ranges::views::filter(&ReplicationInstance::IsAlive);

  if (ranges::empty(alive_replicas)) {
    spdlog::warn("Failover failed since all replicas are down!");
    return;
  }

  // TODO: Smarter choice
  auto new_main = ranges::begin(alive_replicas);

  new_main->PauseFrequentCheck();
  utils::OnScopeExit scope_exit{[&new_main] { new_main->ResumeFrequentCheck(); }};

  auto const is_not_new_main = [&new_main](ReplicationInstance &instance) {
    return instance.InstanceName() != new_main->InstanceName();
  };

  auto const new_main_uuid = utils::UUID{};
  // If for some replicas swap fails, for others on successful ping we will revert back on next change
  // or we will do failover first again and then it will be consistent again
  for (auto &other_replica_instance : alive_replicas | ranges::views::filter(is_not_new_main)) {
    if (!other_replica_instance.SendSwapAndUpdateUUID(new_main_uuid)) {
      spdlog::error(fmt::format("Failed to swap uuid for instance {} which is alive, aborting failover",
                                other_replica_instance.InstanceName()));
      return;
    }
  }

  ReplicationClientsInfo repl_clients_info;
  repl_clients_info.reserve(repl_instances_.size() - 1);
  std::ranges::transform(repl_instances_ | ranges::views::filter(is_not_new_main),
                         std::back_inserter(repl_clients_info), &ReplicationInstance::ReplicationClientInfo);

  if (!new_main->PromoteToMain(new_main_uuid, std::move(repl_clients_info), main_succ_cb_, main_fail_cb_)) {
    spdlog::warn("Failover failed since promoting replica to main failed!");
    return;
  }
  // TODO: (andi) This should be replicated across all coordinator instances with Raft log
  SetMainUUID(new_main_uuid);
  spdlog::info("Failover successful! Instance {} promoted to main.", new_main->InstanceName());
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

  auto const is_not_new_main = [&instance_name](ReplicationInstance const &instance) {
    return instance.InstanceName() != instance_name;
  };

  auto const new_main_uuid = utils::UUID{};

  for (auto &other_instance : repl_instances_ | ranges::views::filter(is_not_new_main)) {
    if (!other_instance.SendSwapAndUpdateUUID(new_main_uuid)) {
      spdlog::error(
          fmt::format("Failed to swap uuid for instance {}, aborting failover", other_instance.InstanceName()));
      return SetInstanceToMainCoordinatorStatus::SWAP_UUID_FAILED;
    }
  }

  ReplicationClientsInfo repl_clients_info;
  repl_clients_info.reserve(repl_instances_.size() - 1);
  std::ranges::transform(repl_instances_ | ranges::views::filter(is_not_new_main),
                         std::back_inserter(repl_clients_info), &ReplicationInstance::ReplicationClientInfo);

  if (!new_main->PromoteToMain(new_main_uuid, std::move(repl_clients_info), main_succ_cb_, main_fail_cb_)) {
    return SetInstanceToMainCoordinatorStatus::COULD_NOT_PROMOTE_TO_MAIN;
  }

  // TODO: (andi) This should be replicated across all coordinator instances with Raft log
  SetMainUUID(new_main_uuid);
  spdlog::info("Instance {} promoted to main", instance_name);
  return SetInstanceToMainCoordinatorStatus::SUCCESS;
}

auto CoordinatorInstance::RegisterReplicationInstance(CoordinatorClientConfig config)
    -> RegisterInstanceCoordinatorStatus {
  auto lock = std::lock_guard{coord_instance_lock_};

  auto instance_name = config.instance_name;

  auto const name_matches = [&instance_name](ReplicationInstance const &instance) {
    return instance.InstanceName() == instance_name;
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

  if (!raft_state_.RequestLeadership()) {
    return RegisterInstanceCoordinatorStatus::NOT_LEADER;
  }

  auto const res = raft_state_.AppendRegisterReplicationInstance(instance_name);
  if (!res->get_accepted()) {
    spdlog::error(
        "Failed to accept request for registering instance {}. Most likely the reason is that the instance is not "
        "the "
        "leader.",
        config.instance_name);
    return RegisterInstanceCoordinatorStatus::RAFT_COULD_NOT_ACCEPT;
  }

  spdlog::info("Request for registering instance {} accepted", instance_name);
  try {
    repl_instances_.emplace_back(this, std::move(config), replica_succ_cb_, replica_fail_cb_);
  } catch (CoordinatorRegisterInstanceException const &) {
    return RegisterInstanceCoordinatorStatus::RPC_FAILED;
  }

  if (res->get_result_code() != nuraft::cmd_result_code::OK) {
    spdlog::error("Failed to register instance {} with error code {}", instance_name, res->get_result_code());
    return RegisterInstanceCoordinatorStatus::RAFT_COULD_NOT_APPEND;
  }

  spdlog::info("Instance {} registered", instance_name);
  return RegisterInstanceCoordinatorStatus::SUCCESS;
}

auto CoordinatorInstance::AddCoordinatorInstance(uint32_t raft_server_id, uint32_t raft_port, std::string raft_address)
    -> void {
  raft_state_.AddCoordinatorInstance(raft_server_id, raft_port, std::move(raft_address));
}

auto CoordinatorInstance::GetMainUUID() const -> utils::UUID { return main_uuid_; }

// TODO: (andi) Add to the RAFT log.
auto CoordinatorInstance::SetMainUUID(utils::UUID new_uuid) -> void { main_uuid_ = new_uuid; }

}  // namespace memgraph::coordination
#endif
