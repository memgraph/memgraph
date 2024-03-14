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
          [this]() {
            // TODO(antoniofilipovic) Do we miss lock here
            spdlog::info("Leader changed, starting all replication instances!");
            auto const instances = raft_state_.GetReplicationInstances();
            auto replicas = instances | ranges::views::filter([](auto const &instance) {
                              return instance.status == ReplicationRole::REPLICA;
                            });

            std::ranges::for_each(replicas, [this](auto &replica) {
              spdlog::info("Started pinging replication instance {}", replica.config.instance_name);
              repl_instances_.emplace_back(this, replica.config, client_succ_cb_, client_fail_cb_,
                                           &CoordinatorInstance::ReplicaSuccessCallback,
                                           &CoordinatorInstance::ReplicaFailCallback);
            });

            auto main = instances | ranges::views::filter(
                                        [](auto const &instance) { return instance.status == ReplicationRole::MAIN; });

            std::ranges::for_each(main, [this](auto &main_instance) {
              spdlog::info("Started pinging main instance {}", main_instance.config.instance_name);
              repl_instances_.emplace_back(this, main_instance.config, client_succ_cb_, client_fail_cb_,
                                           &CoordinatorInstance::MainSuccessCallback,
                                           &CoordinatorInstance::MainFailCallback);
            });

            std::ranges::for_each(repl_instances_, [this](auto &instance) {
              instance.SetNewMainUUID(raft_state_.GetUUID());
              instance.StartFrequentCheck();
            });
          },
          [this]() {
            thread_pool_.AddTask([this]() {
              auto lock = std::lock_guard{coord_instance_lock_};
              spdlog::info("Leader changed, stopping all replication instances!");
              is_shutting_down_ = true;
              repl_instances_.clear();
            });
          })) {
  client_succ_cb_ = [](CoordinatorInstance *self, std::string_view repl_instance_name) -> void {
    auto lock = std::lock_guard{self->coord_instance_lock_};
    auto &repl_instance = self->FindReplicationInstance(repl_instance_name);
    std::invoke(repl_instance.GetSuccessCallback(), self, repl_instance_name);
  };

  client_fail_cb_ = [](CoordinatorInstance *self, std::string_view repl_instance_name) -> void {
    auto lock = std::lock_guard{self->coord_instance_lock_};
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
  auto const coord_instance_to_status = [](ptr<srv_config> const &instance) -> InstanceStatus {
    return {.instance_name = "coordinator_" + std::to_string(instance->get_id()),
            .raft_socket_address = instance->get_endpoint(),
            .cluster_role = "coordinator",
            .health = "unknown"};  // TODO: (andi) Get this info from RAFT and test it or when we will move
  };
  auto instances_status = utils::fmap(raft_state_.GetAllCoordinators(), coord_instance_to_status);

  if (raft_state_.IsLeader()) {
    auto const stringify_repl_role = [this](ReplicationInstance const &instance) -> std::string {
      if (!instance.IsAlive()) return "unknown";
      if (raft_state_.IsMain(instance.InstanceName())) return "main";
      return "replica";
    };

    auto const stringify_repl_health = [](ReplicationInstance const &instance) -> std::string {
      return instance.IsAlive() ? "up" : "down";
    };

    auto process_repl_instance_as_leader =
        [&stringify_repl_role, &stringify_repl_health](ReplicationInstance const &instance) -> InstanceStatus {
      return {.instance_name = instance.InstanceName(),
              .coord_socket_address = instance.CoordinatorSocketAddress(),
              .cluster_role = stringify_repl_role(instance),
              .health = stringify_repl_health(instance)};
    };

    {
      auto lock = std::shared_lock{coord_instance_lock_};
      std::ranges::transform(repl_instances_, std::back_inserter(instances_status), process_repl_instance_as_leader);
    }
  } else {
    auto const stringify_inst_status = [](ReplicationRole status) -> std::string {
      return status == ReplicationRole::MAIN ? "main" : "replica";
    };

    // TODO: (andi) Add capability that followers can also return socket addresses
    auto process_repl_instance_as_follower = [&stringify_inst_status](auto const &instance) -> InstanceStatus {
      return {.instance_name = instance.config.instance_name,
              .cluster_role = stringify_inst_status(instance.status),
              .health = "unknown"};
    };

    std::ranges::transform(raft_state_.GetReplicationInstances(), std::back_inserter(instances_status),
                           process_repl_instance_as_follower);
  }

  return instances_status;
}

auto CoordinatorInstance::TryFailover() -> void {
  auto const is_replica = [this](ReplicationInstance const &instance) { return IsReplica(instance.InstanceName()); };

  auto alive_replicas =
      repl_instances_ | ranges::views::filter(is_replica) | ranges::views::filter(&ReplicationInstance::IsAlive);

  if (ranges::empty(alive_replicas)) {
    spdlog::warn("Failover failed since all replicas are down!");
    return;
  }

  // TODO(antoniofilipovic) We can't request leadership here, what if leader is current doing an action
  MG_ASSERT(raft_state_.IsLeader() || is_shutting_down_, "Coordinator in wrong state.");

  auto const get_ts = [](ReplicationInstance &replica) { return replica.GetClient().SendGetInstanceTimestampsRpc(); };

  auto maybe_instance_db_histories = alive_replicas | ranges::views::transform(get_ts) | ranges::to<std::vector>();

  auto const ts_has_error = [](auto const &res) -> bool { return res.HasError(); };

  if (std::ranges::any_of(maybe_instance_db_histories, ts_has_error)) {
    spdlog::error("Aborting failover as at least one instance didn't provide per database history.");
    return;
  }

  auto transform_to_pairs = ranges::views::transform([](auto const &zipped) {
    auto &[replica, res] = zipped;
    return std::make_pair(replica.InstanceName(), res.GetValue());
  });

  auto instance_db_histories =
      ranges::views::zip(alive_replicas, maybe_instance_db_histories) | transform_to_pairs | ranges::to<std::vector>();

  auto [most_up_to_date_instance, latest_epoch, latest_commit_timestamp] =
      ChooseMostUpToDateInstance(instance_db_histories);

  spdlog::trace("The most up to date instance is {} with epoch {} and {} latest commit timestamp",
                most_up_to_date_instance, latest_epoch, latest_commit_timestamp);  // NOLINT

  auto *new_main = &FindReplicationInstance(most_up_to_date_instance);

  if (!raft_state_.AppendDistributedLockFailover(most_up_to_date_instance)) {
    spdlog::error("Aborting failover as instance is not anymore leader.");
    return;
  }
  unhealthy_state_ = true;
  new_main->PauseFrequentCheck();
  utils::OnScopeExit scope_exit{[&new_main] { new_main->ResumeFrequentCheck(); }};

  auto const is_not_new_main = [&new_main](ReplicationInstance &instance) {
    return instance.InstanceName() != new_main->InstanceName();
  };

  auto const new_main_uuid = utils::UUID{};

  auto const failed_to_swap = [&new_main_uuid](ReplicationInstance &instance) {
    return !instance.SendSwapAndUpdateUUID(new_main_uuid);
  };

  // If for some replicas swap fails, for others on successful ping we will revert back on next change
  // or we will do failover first again and then it will be consistent again
  if (std::ranges::any_of(alive_replicas | ranges::views::filter(is_not_new_main), failed_to_swap)) {
    spdlog::error("Failed to swap uuid for all instances");
    return;
  }
  auto const failed_to_append_swap_to_raft_log = [this, &new_main_uuid](ReplicationInstance &instance) {
    return raft_state_.AppendUpdateInstanceUUID(instance, new_main_uuid);
  };

  if (std::ranges::any_of(alive_replicas | ranges::views::filter(is_not_new_main), failed_to_append_swap_to_raft_log)) {
    spdlog::error("Failed to append to raft log uuid for all instances");
    return;
  }

  auto repl_clients_info = repl_instances_ | ranges::views::filter(is_not_new_main) |
                           ranges::views::transform(&ReplicationInstance::ReplicationClientInfo) |
                           ranges::to<ReplicationClientsInfo>();

  if (!new_main->PromoteToMain(new_main_uuid, std::move(repl_clients_info), &CoordinatorInstance::MainSuccessCallback,
                               &CoordinatorInstance::MainFailCallback)) {
    spdlog::warn("Failover failed since promoting replica to main failed!");
    return;
  }

  if (!raft_state_.AppendUpdateUUIDLog(new_main_uuid)) {
    return;
  }

  auto const new_main_instance_name = new_main->InstanceName();

  if (!raft_state_.AppendSetInstanceAsMainLog(new_main_instance_name)) {
    return;
  }
  unhealthy_state_ = false;
  spdlog::info("Failover successful! Instance {} promoted to main.", new_main->InstanceName());
}

// TODO(antoniofilipovic) - done
auto CoordinatorInstance::SetReplicationInstanceToMain(std::string_view instance_name)
    -> SetInstanceToMainCoordinatorStatus {
  auto lock = std::lock_guard{coord_instance_lock_};
  if (unhealthy_state_) {
    // TODO(antoniofilipovic) Update error log
    return SetInstanceToMainCoordinatorStatus::NOT_LEADER;
  }

  if (raft_state_.MainExists()) {
    return SetInstanceToMainCoordinatorStatus::MAIN_ALREADY_EXISTS;
  }

  // TODO(antoniofilipovic) This can be a problem if other instance is just doing failover or something important
  if (!raft_state_.RequestLeadership()) {
    return SetInstanceToMainCoordinatorStatus::NOT_LEADER;
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

  if (!raft_state_.AppendDistributedLockSetInstanceToMain(instance_name)) {
    // TODO(antoniofilipovic) fix this part
    return SetInstanceToMainCoordinatorStatus::NOT_LEADER;
  }
  // TODO(antoniofilipovic) From here on if we fail anywhere we need to close the lock. If we succeed we consider lock
  // closed
  unhealthy_state_ = true;

  new_main->PauseFrequentCheck();
  utils::OnScopeExit scope_exit{[&new_main] { new_main->ResumeFrequentCheck(); }};

  auto const is_not_new_main = [&instance_name](ReplicationInstance const &instance) {
    return instance.InstanceName() != instance_name;
  };

  auto const new_main_uuid = utils::UUID{};

  auto const failed_to_swap = [&new_main_uuid](ReplicationInstance &instance) {
    return !instance.SendSwapAndUpdateUUID(new_main_uuid);
  };

  if (std::ranges::any_of(repl_instances_ | ranges::views::filter(is_not_new_main), failed_to_swap)) {
    spdlog::error("Failed to swap uuid for all instances");
    return SetInstanceToMainCoordinatorStatus::SWAP_UUID_FAILED;
  }

  auto repl_clients_info = repl_instances_ | ranges::views::filter(is_not_new_main) |
                           ranges::views::transform(&ReplicationInstance::ReplicationClientInfo) |
                           ranges::to<ReplicationClientsInfo>();

  if (!new_main->PromoteToMain(new_main_uuid, std::move(repl_clients_info), &CoordinatorInstance::MainSuccessCallback,
                               &CoordinatorInstance::MainFailCallback)) {
    return SetInstanceToMainCoordinatorStatus::COULD_NOT_PROMOTE_TO_MAIN;
  }

  if (!raft_state_.AppendUpdateUUIDLog(new_main_uuid)) {
    return SetInstanceToMainCoordinatorStatus::RAFT_LOG_ERROR;
  }

  if (!raft_state_.AppendSetInstanceAsMainLog(instance_name)) {
    return SetInstanceToMainCoordinatorStatus::RAFT_LOG_ERROR;
  }

  unhealthy_state_ = false;

  spdlog::info("Instance {} promoted to main on leader", instance_name);
  return SetInstanceToMainCoordinatorStatus::SUCCESS;
}

auto CoordinatorInstance::RegisterReplicationInstance(CoordinatorToReplicaConfig const &config)
    -> RegisterInstanceCoordinatorStatus {
  auto lock = std::lock_guard{coord_instance_lock_};
  if (raft_state_.IsHealthy()) {
    return RegisterInstanceCoordinatorStatus::UNHEALTHY_STATE;
  }

  if (std::ranges::any_of(repl_instances_, [instance_name = config.instance_name](ReplicationInstance const &instance) {
        return instance.InstanceName() == instance_name;
      })) {
    return RegisterInstanceCoordinatorStatus::NAME_EXISTS;
  }

  if (std::ranges::any_of(repl_instances_, [&config](ReplicationInstance const &instance) {
        return instance.CoordinatorSocketAddress() == config.CoordinatorSocketAddress();
      })) {
    return RegisterInstanceCoordinatorStatus::COORD_ENDPOINT_EXISTS;
  }

  if (std::ranges::any_of(repl_instances_, [&config](ReplicationInstance const &instance) {
        return instance.ReplicationSocketAddress() == config.ReplicationSocketAddress();
      })) {
    return RegisterInstanceCoordinatorStatus::REPL_ENDPOINT_EXISTS;
  }

  DMG_ASSERT(raft_state_.IsLeader(), "Coordinator server is not leader");

  // TODO(antoniofilipovic) Two things:
  //  leadership request is just request and there is still waiting to do
  //  also there is possibility that user registered to follower, and leader is doing currently failover
  //  Maybe we just need to check for last state
  if (!raft_state_.RequestLeadership()) {
    return RegisterInstanceCoordinatorStatus::NOT_LEADER;
  }

  if (!raft_state_.AppendOpenLockRegister(config)) {
    // TODO(antoniofilipovic) change
    return RegisterInstanceCoordinatorStatus::UNHEALTHY_STATE;
  }

  auto const undo_action_ = [this]() { repl_instances_.pop_back(); };

  auto *new_instance = &repl_instances_.emplace_back(this, config, client_succ_cb_, client_fail_cb_,
                                                     &CoordinatorInstance::ReplicaSuccessCallback,
                                                     &CoordinatorInstance::ReplicaFailCallback);

  if (!new_instance->SendDemoteToReplicaRpc()) {
    spdlog::error("Failed to send demote to replica rpc for instance {}", config.instance_name);
    undo_action_();
    return RegisterInstanceCoordinatorStatus::RPC_FAILED;
  }

  if (!raft_state_.AppendRegisterReplicationInstanceLog(config)) {
    // TODO(antoniofilipovic) Enter state with "RESET cluster"
    return RegisterInstanceCoordinatorStatus::RAFT_LOG_ERROR;
  }
  // Success above closes lock
  new_instance->StartFrequentCheck();

  spdlog::info("Instance {} registered", config.instance_name);
  return RegisterInstanceCoordinatorStatus::SUCCESS;
}

auto CoordinatorInstance::UnregisterReplicationInstance(std::string_view instance_name)
    -> UnregisterInstanceCoordinatorStatus {
  auto lock = std::lock_guard{coord_instance_lock_};

  // This doesn't make sense as it needs to be leader and we can't request it
  if (!raft_state_.RequestLeadership()) {
    return UnregisterInstanceCoordinatorStatus::NOT_LEADER;
  }

  auto const name_matches = [&instance_name](ReplicationInstance const &instance) {
    return instance.InstanceName() == instance_name;
  };

  auto inst_to_remove = std::ranges::find_if(repl_instances_, name_matches);
  if (inst_to_remove == repl_instances_.end()) {
    return UnregisterInstanceCoordinatorStatus::NO_INSTANCE_WITH_NAME;
  }

  auto const is_main = [this](ReplicationInstance const &instance) {
    return IsMain(instance.InstanceName()) && instance.GetMainUUID() == raft_state_.GetUUID() && instance.IsAlive();
  };

  if (is_main(*inst_to_remove)) {
    return UnregisterInstanceCoordinatorStatus::IS_MAIN;
  }

  inst_to_remove->StopFrequentCheck();

  auto curr_main = std::ranges::find_if(repl_instances_, is_main);

  if (curr_main != repl_instances_.end() && curr_main->IsAlive()) {
    if (!curr_main->SendUnregisterReplicaRpc(instance_name)) {
      inst_to_remove->StartFrequentCheck();
      return UnregisterInstanceCoordinatorStatus::RPC_FAILED;
    }
  }

  std::erase_if(repl_instances_, name_matches);

  if (!raft_state_.AppendUnregisterReplicationInstanceLog(instance_name)) {
    return UnregisterInstanceCoordinatorStatus::RAFT_LOG_ERROR;
  }

  return UnregisterInstanceCoordinatorStatus::SUCCESS;
}

auto CoordinatorInstance::AddCoordinatorInstance(coordination::CoordinatorToCoordinatorConfig const &config) -> void {
  raft_state_.AddCoordinatorInstance(config);
  // NOTE: We ignore error we added coordinator instance to networkign stuff but not in raft log.
  if (!raft_state_.AppendAddCoordinatorInstanceLog(config)) {
    spdlog::error("Failed to append add coordinator instance log");
  }
}

void CoordinatorInstance::MainFailCallback(std::string_view repl_instance_name) {
  spdlog::trace("Instance {} performing main fail callback", repl_instance_name);
  auto &repl_instance = FindReplicationInstance(repl_instance_name);
  repl_instance.OnFailPing();
  const auto &repl_instance_uuid = repl_instance.GetMainUUID();
  MG_ASSERT(repl_instance_uuid.has_value(), "Replication instance must have uuid set");

  // NOLINTNEXTLINE
  if (!repl_instance.IsAlive() && raft_state_.GetUUID() == repl_instance_uuid.value()) {
    spdlog::info("Cluster without main instance, trying automatic failover");
    TryFailover();
  }
}

void CoordinatorInstance::MainSuccessCallback(std::string_view repl_instance_name) {
  spdlog::trace("Instance {} performing main successful callback", repl_instance_name);
  auto &repl_instance = FindReplicationInstance(repl_instance_name);

  if (repl_instance.IsAlive()) {
    repl_instance.OnSuccessPing();
    return;
  }

  const auto &repl_instance_uuid = repl_instance.GetMainUUID();
  MG_ASSERT(repl_instance_uuid.has_value(), "Instance must have uuid set.");

  // NOLINTNEXTLINE
  if (raft_state_.GetUUID() == repl_instance_uuid.value()) {
    if (!repl_instance.EnableWritingOnMain()) {
      spdlog::error("Failed to enable writing on main instance {}", repl_instance_name);
      return;
    }

    repl_instance.OnSuccessPing();
    return;
  }

  if (!raft_state_.RequestLeadership()) {
    spdlog::error("Demoting main instance {} to replica failed since the instance is not the leader!",
                  repl_instance_name);
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

  if (!repl_instance.SendSwapAndUpdateUUID(raft_state_.GetUUID())) {
    spdlog::error("Failed to swap uuid for demoted main instance {}", repl_instance_name);
    return;
  }

  if (!raft_state_.AppendSetInstanceAsReplicaLog(repl_instance_name)) {
    return;
  }
}

void CoordinatorInstance::ReplicaSuccessCallback(std::string_view repl_instance_name) {
  spdlog::trace("Instance {} performing replica successful callback", repl_instance_name);
  auto &repl_instance = FindReplicationInstance(repl_instance_name);

  if (!IsReplica(repl_instance_name)) {
    spdlog::error("Aborting replica callback since instance {} is not replica anymore", repl_instance_name);
    return;
  }
  // We need to get replicas UUID from time to time to ensure replica is listening to correct main
  // and that it didn't go down for less time than we could notice
  // We need to get id of main replica is listening to
  // and swap if necessary
  if (!repl_instance.EnsureReplicaHasCorrectMainUUID(raft_state_.GetUUID())) {
    spdlog::error("Failed to swap uuid for replica instance {} which is alive", repl_instance.InstanceName());
    return;
  }

  repl_instance.OnSuccessPing();
}

void CoordinatorInstance::ReplicaFailCallback(std::string_view repl_instance_name) {
  spdlog::trace("Instance {} performing replica failure callback", repl_instance_name);
  auto &repl_instance = FindReplicationInstance(repl_instance_name);

  if (!IsReplica(repl_instance_name)) {
    spdlog::error("Aborting replica fail callback since instance {} is not replica anymore", repl_instance_name);
    return;
  }

  repl_instance.OnFailPing();
}

auto CoordinatorInstance::ChooseMostUpToDateInstance(std::span<InstanceNameDbHistories> instance_database_histories)
    -> NewMainRes {
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

auto CoordinatorInstance::IsMain(std::string_view instance_name) const -> bool {
  return raft_state_.IsMain(instance_name);
}

auto CoordinatorInstance::IsReplica(std::string_view instance_name) const -> bool {
  return raft_state_.IsReplica(instance_name);
}

auto CoordinatorInstance::GetRoutingTable(std::map<std::string, std::string> const &routing) -> RoutingTable {
  auto res = RoutingTable{};

  auto const repl_instance_to_bolt = [](ReplicationInstanceState const &instance) {
    return instance.config.BoltSocketAddress();
  };

  // TODO: (andi) This is wrong check, Fico will correct in #1819.
  auto const is_instance_main = [&](ReplicationInstanceState const &instance) {
    return instance.status == ReplicationRole::MAIN;
  };

  auto const is_instance_replica = [&](ReplicationInstanceState const &instance) {
    return instance.status == ReplicationRole::REPLICA;
  };

  auto const &raft_log_repl_instances = raft_state_.GetReplicationInstances();

  auto bolt_mains = raft_log_repl_instances | ranges::views::filter(is_instance_main) |
                    ranges::views::transform(repl_instance_to_bolt) | ranges::to<std::vector>();
  MG_ASSERT(bolt_mains.size() <= 1, "There can be at most one main instance active!");

  if (!std::ranges::empty(bolt_mains)) {
    res.emplace_back(std::move(bolt_mains), "WRITE");
  }

  auto bolt_replicas = raft_log_repl_instances | ranges::views::filter(is_instance_replica) |
                       ranges::views::transform(repl_instance_to_bolt) | ranges::to<std::vector>();
  if (!std::ranges::empty(bolt_replicas)) {
    res.emplace_back(std::move(bolt_replicas), "READ");
  }

  auto const coord_instance_to_bolt = [](CoordinatorInstanceState const &instance) {
    return instance.config.bolt_server.SocketAddress();
  };

  auto const &raft_log_coord_instances = raft_state_.GetCoordinatorInstances();
  auto bolt_coords =
      raft_log_coord_instances | ranges::views::transform(coord_instance_to_bolt) | ranges::to<std::vector>();

  auto const &local_bolt_coord = routing.find("address");
  if (local_bolt_coord == routing.end()) {
    throw InvalidRoutingTableException("No bolt address found in routing table for the current coordinator!");
  }

  bolt_coords.push_back(local_bolt_coord->second);
  res.emplace_back(std::move(bolt_coords), "ROUTE");

  return res;
}

}  // namespace memgraph::coordination
#endif
