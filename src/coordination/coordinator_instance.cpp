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
#include "coordination/fmt.hpp"
#include "dbms/constants.hpp"
#include "nuraft/coordinator_state_machine.hpp"
#include "nuraft/coordinator_state_manager.hpp"
#include "utils/counter.hpp"
#include "utils/functional.hpp"
#include "utils/resource_lock.hpp"

#include <range/v3/view.hpp>
#include <shared_mutex>

namespace memgraph::coordination {

using nuraft::ptr;
using nuraft::srv_config;

CoordinatorInstance::CoordinatorInstance()
    : raft_state_(RaftState::MakeRaftState(
          [this] { std::ranges::for_each(repl_instances_, &ReplicationInstance::StartFrequentCheck); },
          [this] { std::ranges::for_each(repl_instances_, &ReplicationInstance::StopFrequentCheck); })) {
  client_succ_cb_ = [](CoordinatorInstance *self, std::string_view repl_instance_name) -> void {
    auto lock = std::unique_lock{self->coord_instance_lock_};
    auto &repl_instance = self->FindReplicationInstance(repl_instance_name);
    std::invoke(repl_instance.GetSuccessCallback(), self, repl_instance_name);
  };

  client_fail_cb_ = [](CoordinatorInstance *self, std::string_view repl_instance_name) -> void {
    auto lock = std::unique_lock{self->coord_instance_lock_};
    auto &repl_instance = self->FindReplicationInstance(repl_instance_name);
    std::invoke(repl_instance.GetFailCallback(), self, repl_instance_name);
  };
}

auto CoordinatorInstance::FindReplicationInstance(std::string_view replication_instance_name) -> ReplicationInstance & {
  auto repl_instance =
      std::ranges::find_if(repl_instances_, [replication_instance_name](ReplicationInstance const &instance) {
        return instance.InstanceName() == replication_instance_name;
      });

  MG_ASSERT(repl_instance != repl_instances_.end(), "Instance {} not found during callback!",
            replication_instance_name);
  return *repl_instance;
}

auto CoordinatorInstance::ShowInstances() const -> std::vector<InstanceStatus> {
  auto const coord_instances = raft_state_.GetAllCoordinators();

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

  auto instances_status = utils::fmap(coord_instance_to_status, coord_instances);
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

  // for each DB in instance we get one DatabaseHistory
  using DatabaseHistories = replication_coordination_glue::DatabaseHistories;
  std::vector<std::pair<std::string, DatabaseHistories>> instance_database_histories;

  bool success{true};
  std::ranges::for_each(alive_replicas, [&success, &instance_database_histories](ReplicationInstance &replica) {
    if (!success) {
      return;
    }
    auto res = replica.GetClient().SendGetInstanceTimestampsRpc();
    if (res.HasError()) {
      spdlog::error("Could get per db history data for instance {}", replica.InstanceName());
      success = false;
      return;
    }
    instance_database_histories.emplace_back(replica.InstanceName(), std::move(res.GetValue()));
  });

  if (!success) {
    spdlog::error("Aborting failover as at least one instance didn't provide per database history.");
    return;
  }

  auto [most_up_to_date_instance, latest_epoch, latest_commit_timestamp] =
      ChooseMostUpToDateInstance(instance_database_histories);

  spdlog::trace("The most up to date instance is {} with epoch {} and {} latest commit timestamp",
                most_up_to_date_instance, latest_epoch, latest_commit_timestamp);

  auto *new_main = &FindReplicationInstance(most_up_to_date_instance);

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

  auto repl_clients_info = repl_instances_ | ranges::views::filter(is_not_new_main) |
                           ranges::views::transform(&ReplicationInstance::ReplicationClientInfo) |
                           ranges::to<ReplicationClientsInfo>();

  if (!new_main->PromoteToMain(new_main_uuid, std::move(repl_clients_info), &CoordinatorInstance::MainSuccessCallback,
                               &CoordinatorInstance::MainFailCallback)) {
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

  if (std::ranges::any_of(repl_instances_, &ReplicationInstance::IsMain)) {
    return SetInstanceToMainCoordinatorStatus::MAIN_ALREADY_EXISTS;
  }

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

  if (!new_main->PromoteToMain(new_main_uuid, std::move(repl_clients_info), &CoordinatorInstance::MainSuccessCallback,
                               &CoordinatorInstance::MainFailCallback)) {
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

  // TODO: (andi) I think this should be part of Raft state machine
  spdlog::info("Request for registering instance {} accepted", instance_name);
  try {
    repl_instances_.emplace_back(this, std::move(config), client_succ_cb_, client_fail_cb_,
                                 &CoordinatorInstance::ReplicaSuccessCallback,
                                 &CoordinatorInstance::ReplicaFailCallback);
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

void CoordinatorInstance::MainFailCallback(std::string_view repl_instance_name) {
  auto &repl_instance = FindReplicationInstance(repl_instance_name);
  repl_instance.OnFailPing();
  const auto &repl_instance_uuid = repl_instance.GetMainUUID();
  MG_ASSERT(repl_instance_uuid.has_value(), "Instance must have uuid set");

  if (!repl_instance.IsAlive() && GetMainUUID() == repl_instance_uuid.value()) {
    spdlog::info("Cluster without main instance, trying automatic failover");
    TryFailover();
  }
}

void CoordinatorInstance::MainSuccessCallback(std::string_view repl_instance_name) {
  auto &repl_instance = FindReplicationInstance(repl_instance_name);
  spdlog::trace("Instance {} performing main successful callback", repl_instance_name);

  if (repl_instance.IsAlive()) {
    repl_instance.OnSuccessPing();
    return;
  }

  const auto &repl_instance_uuid = repl_instance.GetMainUUID();
  MG_ASSERT(repl_instance_uuid.has_value(), "Instance must have uuid set.");

  auto const curr_main_uuid = GetMainUUID();
  if (curr_main_uuid == repl_instance_uuid.value()) {
    if (!repl_instance.EnableWritingOnMain()) {
      spdlog::error("Failed to enable writing on main instance {}", repl_instance_name);
      return;
    }

    repl_instance.OnSuccessPing();
    return;
  }

  if (repl_instance.DemoteToReplica(&CoordinatorInstance::ReplicaSuccessCallback,
                                    &CoordinatorInstance::ReplicaFailCallback)) {
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
}

void CoordinatorInstance::ReplicaSuccessCallback(std::string_view repl_instance_name) {
  auto &repl_instance = FindReplicationInstance(repl_instance_name);
  if (!repl_instance.IsReplica()) {
    spdlog::error("Aborting replica callback since instance {} is not replica anymore", repl_instance_name);
    return;
  }
  spdlog::trace("Instance {} performing replica successful callback", repl_instance_name);
  // We need to get replicas UUID from time to time to ensure replica is listening to correct main
  // and that it didn't go down for less time than we could notice
  // We need to get id of main replica is listening to
  // and swap if necessary
  if (!repl_instance.EnsureReplicaHasCorrectMainUUID(GetMainUUID())) {
    spdlog::error("Failed to swap uuid for replica instance {} which is alive", repl_instance.InstanceName());
    return;
  }

  repl_instance.OnSuccessPing();
}

void CoordinatorInstance::ReplicaFailCallback(std::string_view repl_instance_name) {
  auto &repl_instance = FindReplicationInstance(repl_instance_name);
  if (!repl_instance.IsReplica()) {
    spdlog::error("Aborting replica fail callback since instance {} is not replica anymore", repl_instance_name);
    return;
  }
  spdlog::trace("Instance {} performing replica failure callback", repl_instance_name);
  repl_instance.OnFailPing();
}

auto CoordinatorInstance::UnregisterReplicationInstance(std::string instance_name)
    -> UnregisterInstanceCoordinatorStatus {
  auto lock = std::lock_guard{coord_instance_lock_};

  auto const name_matches = [&instance_name](ReplicationInstance const &instance) {
    return instance.InstanceName() == instance_name;
  };

  auto inst_to_remove = std::ranges::find_if(repl_instances_, name_matches);
  if (inst_to_remove == repl_instances_.end()) {
    return UnregisterInstanceCoordinatorStatus::NO_INSTANCE_WITH_NAME;
  }

  if (inst_to_remove->IsMain() && inst_to_remove->IsAlive()) {
    return UnregisterInstanceCoordinatorStatus::IS_MAIN;
  }

  inst_to_remove->StopFrequentCheck();
  auto curr_main = std::ranges::find_if(repl_instances_, &ReplicationInstance::IsMain);
  MG_ASSERT(curr_main != repl_instances_.end(), "There must be a main instance when unregistering a replica");
  if (!curr_main->SendUnregisterReplicaRpc(instance_name)) {
    inst_to_remove->StartFrequentCheck();
    return UnregisterInstanceCoordinatorStatus::RPC_FAILED;
  }
  std::erase_if(repl_instances_, name_matches);

  return UnregisterInstanceCoordinatorStatus::SUCCESS;
}

auto CoordinatorInstance::AddCoordinatorInstance(uint32_t raft_server_id, uint32_t raft_port, std::string raft_address)
    -> void {
  raft_state_.AddCoordinatorInstance(raft_server_id, raft_port, std::move(raft_address));
}

auto CoordinatorInstance::GetMainUUID() const -> utils::UUID { return main_uuid_; }

// TODO: (andi) Add to the RAFT log.
auto CoordinatorInstance::SetMainUUID(utils::UUID new_uuid) -> void { main_uuid_ = new_uuid; }

auto CoordinatorInstance::ChooseMostUpToDateInstance(
    const std::vector<std::pair<std::string, replication_coordination_glue::DatabaseHistories>>
        &instance_database_histories) -> NewMainRes {
  std::optional<NewMainRes> new_main_res;
  std::for_each(
      instance_database_histories.begin(), instance_database_histories.end(),
      [&new_main_res](const InstanceNameDbHistories &instance_res_pair) {
        const auto &[instance_name, instance_db_histories] = instance_res_pair;

        // Find default db for instance and its history
        auto default_db_history_data = std::ranges::find_if(
            instance_db_histories, [default_db = memgraph::dbms::kDefaultDB](
                                       const replication_coordination_glue::DatabaseHistory &db_timestamps) {
              return db_timestamps.name == default_db;
            });

        std::ranges::for_each(
            instance_db_histories,
            [&instance_name = instance_name](const replication_coordination_glue::DatabaseHistory &db_history) {
              spdlog::debug("Instance {}: name {}, default db {}", instance_name, db_history.name,
                            memgraph::dbms::kDefaultDB);
            });

        MG_ASSERT(default_db_history_data != instance_db_histories.end(), "No history for instance");

        const auto &instance_default_db_history = default_db_history_data->history;

        std::ranges::for_each(instance_default_db_history | ranges::views::reverse,
                              [&instance_name = instance_name](const auto &epoch_history_it) {
                                spdlog::debug("Instance {}: epoch {}, last_commit_timestamp: {}", instance_name,
                                              std::get<0>(epoch_history_it), std::get<1>(epoch_history_it));
                              });

        // get latest epoch
        // get latest timestamp

        if (!new_main_res) {
          const auto &[epoch, timestamp] = *instance_default_db_history.crbegin();
          new_main_res = std::make_optional<NewMainRes>({instance_name, epoch, timestamp});
          spdlog::debug("Currently the most up to date instance is {} with epoch {} and {} latest commit timestamp",
                        instance_name, epoch, timestamp);
          return;
        }

        bool found_same_point{false};
        std::string last_most_up_to_date_epoch{new_main_res->latest_epoch};
        for (auto [epoch, timestamp] : ranges::reverse_view(instance_default_db_history)) {
          if (new_main_res->latest_commit_timestamp < timestamp) {
            new_main_res = std::make_optional<NewMainRes>({instance_name, epoch, timestamp});
            spdlog::trace("Found the new most up to date instance {} with epoch {} and {} latest commit timestamp",
                          instance_name, epoch, timestamp);
          }

          // we found point at which they were same
          if (epoch == last_most_up_to_date_epoch) {
            found_same_point = true;
            break;
          }
        }

        if (!found_same_point) {
          spdlog::error("Didn't find same history epoch {} for instance {} and instance {}", last_most_up_to_date_epoch,
                        new_main_res->most_up_to_date_instance, instance_name);
        }
      });

  return std::move(*new_main_res);
}
}  // namespace memgraph::coordination
#endif
