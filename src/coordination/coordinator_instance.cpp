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
#include "utils/functional.hpp"
#include "utils/resource_lock.hpp"

#include <range/v3/view.hpp>
#include <shared_mutex>

namespace memgraph::coordination {

using nuraft::ptr;
using nuraft::srv_config;

CoordinatorInstance::CoordinatorInstance(CoordinatorInstanceInitConfig const &config)
    : thread_pool_{1},
      raft_state_(RaftState::MakeRaftState(
          config,
          [this]() {
            if (raft_state_.IsLockOpened()) {
              spdlog::error("Leader hasn't encountered healthy state, doing force reset of cluster.");
              thread_pool_.AddTask([this]() { this->ForceResetCluster(); });
              return;
            }
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

            auto main_instances = instances | ranges::views::filter([](auto const &instance) {
                                    return instance.status == ReplicationRole::MAIN;
                                  });

            std::ranges::for_each(main_instances, [this](auto &main_instance) {
              spdlog::info("Started pinging main instance {}", main_instance.config.instance_name);
              repl_instances_.emplace_back(this, main_instance.config, client_succ_cb_, client_fail_cb_,
                                           &CoordinatorInstance::MainSuccessCallback,
                                           &CoordinatorInstance::MainFailCallback);
            });

            // In case we got out of force reset but instances weren't still demoted
            // we need to apply functions to these instances to demote them
            auto instances_to_demote =
                instances | ranges::views::filter([](ReplicationInstanceState const &replication_instance_state) {
                  return replication_instance_state.needs_demote;
                });

            std::ranges::for_each(
                instances_to_demote, [this](ReplicationInstanceState const &replication_instance_state) {
                  spdlog::trace("Changing callback for instance {} to demote callback",
                                replication_instance_state.config.instance_name);
                  auto &instance = FindReplicationInstance(replication_instance_state.config.instance_name);
                  instance.SetCallbacks(&CoordinatorInstance::DemoteSuccessCallback,
                                        &CoordinatorInstance::DemoteFailCallback);
                });
            std::ranges::for_each(repl_instances_, [](auto &instance) { instance.StartFrequentCheck(); });
          },
          [this]() {
            thread_pool_.AddTask([this]() {
              spdlog::info("Leader changed, trying to stop all replication instances frequent checks!");
              // We need to stop checks before taking a lock because deadlock can happen if instances waits
              // to take a lock in frequent check, and this thread already has a lock and waits for instance to
              // be done with frequent check
              std::ranges::for_each(repl_instances_, [](auto &repl_instance) {
                spdlog::trace("Stopping frequent checks for instance {}", repl_instance.InstanceName());
                repl_instance.StopFrequentCheck();
                spdlog::trace("Stopped frequent checks for instance {}", repl_instance.InstanceName());
              });
              auto lock = std::unique_lock{coord_instance_lock_};
              repl_instances_.clear();
              spdlog::info("Stopped all replication instance frequent checks.");
            });
          })) {
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
  auto const coord_instance_to_status = [](CoordinatorInstanceState const &instance) -> InstanceStatus {
    return {.instance_name = fmt::format("coordinator_{}", instance.config.coordinator_id),
            .raft_socket_address = instance.config.coordinator_server.SocketAddress(),
            .cluster_role = "coordinator",
            .health = "unknown"};
  };
  auto instances_status = utils::fmap(raft_state_.GetCoordinatorInstances(), coord_instance_to_status);

  if (raft_state_.IsLeader()) {
    auto const stringify_repl_role = [this](ReplicationInstance const &instance) -> std::string {
      if (!instance.IsAlive()) return "unknown";
      if (raft_state_.IsCurrentMain(instance.InstanceName())) return "main";
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
    auto const stringify_inst_status = [raft_state_ptr = &raft_state_](
                                           utils::UUID const &main_uuid,
                                           ReplicationInstanceState const &instance) -> std::string {
      if (raft_state_ptr->IsCurrentMain(instance.config.instance_name)) {
        return "main";
      }
      if (raft_state_ptr->HasMainState(instance.config.instance_name)) {
        return "unknown";
      }
      return "replica";
    };

    // TODO: (andi) Add capability that followers can also return socket addresses
    auto process_repl_instance_as_follower =
        [this, &stringify_inst_status](ReplicationInstanceState const &instance) -> InstanceStatus {
      return {.instance_name = instance.config.instance_name,
              .cluster_role = stringify_inst_status(raft_state_.GetCurrentMainUUID(), instance),
              .health = "unknown"};
    };

    std::ranges::transform(raft_state_.GetReplicationInstances(), std::back_inserter(instances_status),
                           process_repl_instance_as_follower);
  }
  return instances_status;
}

void CoordinatorInstance::ForceResetCluster() {
  // Force reset tries to return cluster to state in which we have all the replicas we had before
  // and try to do failover to new MAIN. Only then is force reset successful

  // 0. Open lock
  // 1. Try to demote each instance to replica
  // 2. Instances which are demoted proceed in next step as part of selection process
  // 3. For selected instances try to send SWAP UUID and update log -> both must succeed
  // 4. Do failover
  // 5. For instances which were down set correct callback as before
  // 6. After instance get's back up, do steps needed to recover

  spdlog::trace("Force resetting cluster!");
  // Ordering is important here, we must stop frequent check before
  // taking lock to avoid deadlock between us stopping thread and thread wanting to take lock but can't because
  // we have it
  std::ranges::for_each(repl_instances_, [](auto &repl_instance) {
    spdlog::trace("Stopping frequent check for instance {}", repl_instance.InstanceName());
    repl_instance.StopFrequentCheck();
    spdlog::trace("Stopped frequent check for instance {}", repl_instance.InstanceName());
  });
  spdlog::trace("Stopped all replication instance frequent checks.");
  auto lock = std::unique_lock{coord_instance_lock_};
  repl_instances_.clear();

  if (!raft_state_.IsLeader()) {
    spdlog::trace("Exiting force reset as coordinator is not any more leader!");
    return;
  }

  if (!raft_state_.AppendOpenLock()) {
    spdlog::trace("Appending log force reset failed, aborting force reset");
    // Here we want to continue doing force reset or kill coordinator. If we can't append open lock,
    // then we must be follower, and some other coordinator will continue trying force reset. Otherwise
    // we are leader  but append lock failed, so we are in wrong state.
    MG_ASSERT(!raft_state_.IsLeader(), "Coordinator is leader but append open lock failed, encountered wrong state.");
    return;
  }

  utils::OnScopeExit maybe_do_another_reset{[this]() {
    if (raft_state_.IsLockOpened() && raft_state_.IsLeader()) {
      spdlog::trace("Adding task to try force reset cluster again as lock is opened still.");
      thread_pool_.AddTask([this]() { this->ForceResetCluster(); });
      return;
    }
    spdlog::trace("Lock is not opened anymore or coordinator is not leader, not doing force reset again.");
  }};

  auto const instances = raft_state_.GetReplicationInstances();

  // To each instance we send RPC
  // If RPC fails we consider instance dead
  // Otherwise we consider instance alive
  // If at any point later RPC fails for alive instance, we consider this failure

  std::ranges::for_each(instances, [this](auto &replica) {
    repl_instances_.emplace_back(this, replica.config, client_succ_cb_, client_fail_cb_,
                                 &CoordinatorInstance::ReplicaSuccessCallback,
                                 &CoordinatorInstance::ReplicaFailCallback);
  });

  auto instances_mapped_to_resp = repl_instances_ | ranges::views::transform([](auto &instance) {
                                    return std::pair{instance.InstanceName(), instance.SendFrequentHeartbeat()};
                                  }) |
                                  ranges::to<std::unordered_map<std::string, bool>>();

  auto alive_instances = repl_instances_ | ranges::views::filter([&instances_mapped_to_resp](auto const &instance) {
                           return instances_mapped_to_resp[instance.InstanceName()];
                         });

  auto demote_to_replica_failed = [this](auto &instance) {
    if (!instance.DemoteToReplica(&CoordinatorInstance::ReplicaSuccessCallback,
                                  &CoordinatorInstance::ReplicaFailCallback)) {
      return true;
    }
    return !raft_state_.AppendSetInstanceAsReplicaLog(instance.InstanceName());
  };
  if (std::ranges::any_of(alive_instances, demote_to_replica_failed)) {
    spdlog::error("Failed to send log instance demoted to replica.");
    return;
  }

  auto const new_uuid = utils::UUID{};

  auto update_uuid_failed = [&new_uuid, this](auto &repl_instance) {
    if (!repl_instance.SendSwapAndUpdateUUID(new_uuid)) {
      return true;
    }
    return !raft_state_.AppendUpdateUUIDForInstanceLog(repl_instance.InstanceName(), new_uuid);
  };
  if (std::ranges::any_of(alive_instances, update_uuid_failed)) {
    spdlog::error("Force reset failed since update log swap uuid failed, assuming coordinator is now follower.");
    return;
  }

  if (!raft_state_.AppendUpdateUUIDForNewMainLog(new_uuid)) {
    spdlog::error("Update log for new MAIN failed, assuming coordinator is now follower");
    return;
  }

  auto maybe_most_up_to_date_instance = GetMostUpToDateInstanceFromHistories(alive_instances);

  if (!maybe_most_up_to_date_instance.has_value()) {
    spdlog::error("Couldn't choose instance for failover, check logs for more details.");
    return;
  }

  auto &new_main = FindReplicationInstance(*maybe_most_up_to_date_instance);

  auto const is_not_new_main = [&new_main](ReplicationInstance const &repl_instance) {
    return repl_instance.InstanceName() != new_main.InstanceName();
  };
  auto repl_clients_info = repl_instances_ | ranges::views::filter(is_not_new_main) |
                           ranges::views::transform(&ReplicationInstance::ReplicationClientInfo) |
                           ranges::to<ReplicationClientsInfo>();

  if (!new_main.PromoteToMain(new_uuid, std::move(repl_clients_info), &CoordinatorInstance::MainSuccessCallback,
                              &CoordinatorInstance::MainFailCallback)) {
    spdlog::warn("Force reset failed since promoting replica to main failed.");
    return;
  }

  // This will set cluster in healthy state again
  if (!raft_state_.AppendSetInstanceAsMainLog(*maybe_most_up_to_date_instance, new_uuid)) {
    spdlog::error("Update log for new MAIN failed");
    return;
  }

  // We need to clear repl instances in the beginning as we don't know where exactly action failed and
  // we need to recreate state from raft log
  // If instance in raft log is MAIN, it can be REPLICA but raft append failed when we demoted it
  // If instance in raft log is REPLICA, it can be MAIN but raft log failed when we promoted it
  // CRUX of problem: We need demote callbacks which will demote instance to replica and only then change to
  // REPLICA callbacks

  auto needs_demote_setup_failed = [&instances_mapped_to_resp, this](ReplicationInstance &repl_instance) {
    if (instances_mapped_to_resp[repl_instance.InstanceName()]) {
      return false;
    }
    if (!raft_state_.AppendInstanceNeedsDemote(repl_instance.InstanceName())) {
      return true;
    }
    repl_instance.SetCallbacks(&CoordinatorInstance::DemoteSuccessCallback, &CoordinatorInstance::DemoteFailCallback);
    return false;
  };

  if (std::ranges::any_of(repl_instances_, needs_demote_setup_failed)) {
    spdlog::error("Raft log didn't accept that some instances are in unknown state.");
    return;
  }

  auto check_correct_callbacks_set = [this](auto &repl_instance) {
    if (raft_state_.HasReplicaState(repl_instance.InstanceName())) {
      MG_ASSERT(repl_instance.GetSuccessCallback() == &CoordinatorInstance::ReplicaSuccessCallback &&
                    repl_instance.GetFailCallback() == &CoordinatorInstance::ReplicaFailCallback,
                "Callbacks are wrong");
    } else {
      MG_ASSERT(repl_instance.GetSuccessCallback() == &CoordinatorInstance::MainSuccessCallback &&
                repl_instance.GetFailCallback() == &CoordinatorInstance::MainFailCallback);
    }
  };
  std::ranges::for_each(alive_instances, check_correct_callbacks_set);

  if (!raft_state_.AppendCloseLock()) {
    spdlog::error("Aborting force reset as we failed to close lock on action.");
    return;
  }

  std::ranges::for_each(repl_instances_, [](auto &instance) { instance.StartFrequentCheck(); });

  MG_ASSERT(!raft_state_.IsLockOpened(), "After force reset we need to be in healthy state.");
}

auto CoordinatorInstance::TryFailover() -> void {
  auto const is_replica = [this](ReplicationInstance const &instance) {
    return HasReplicaState(instance.InstanceName());
  };

  auto alive_replicas =
      repl_instances_ | ranges::views::filter(is_replica) | ranges::views::filter(&ReplicationInstance::IsAlive);

  if (ranges::empty(alive_replicas)) {
    spdlog::warn("Failover failed since all replicas are down!");
    return;
  }

  auto maybe_most_up_to_date_instance = GetMostUpToDateInstanceFromHistories(alive_replicas);

  if (!maybe_most_up_to_date_instance.has_value()) {
    spdlog::error("Couldn't choose instance for failover, check logs for more details.");
    return;
  }

  auto &new_main = FindReplicationInstance(*maybe_most_up_to_date_instance);

  if (!raft_state_.AppendOpenLock()) {
    spdlog::error("Aborting failover as instance is not anymore leader.");
    return;
  }

  utils::OnScopeExit do_reset{[this]() {
    if (raft_state_.IsLockOpened() && raft_state_.IsLeader()) {
      spdlog::trace("Adding task to try force reset cluster as lock is still opened after failover.");
      thread_pool_.AddTask([this]() { this->ForceResetCluster(); });
    }
    spdlog::trace("Failover done, lock is not opened anymore or coordinator is not leader.");
  }};

  // We don't need to stop frequent check as we have lock, and we will swap callback function during locked phase
  // In frequent check only when we take lock we then check which function (MAIN/REPLICA) success or fail callback
  // we need to call

  auto const is_not_new_main = [&new_main](ReplicationInstance &instance) {
    return instance.InstanceName() != new_main.InstanceName();
  };

  auto const new_main_uuid = utils::UUID{};

  auto const failed_to_swap = [this, &new_main_uuid](ReplicationInstance &instance) {
    return !instance.SendSwapAndUpdateUUID(new_main_uuid) ||
           !raft_state_.AppendUpdateUUIDForInstanceLog(instance.InstanceName(), new_main_uuid);
  };

  // If for some replicas swap fails, for others on successful ping we will revert back on next change
  // or we will do failover first again and then it will be consistent again
  if (std::ranges::any_of(alive_replicas | ranges::views::filter(is_not_new_main), failed_to_swap)) {
    spdlog::error("Aborting failover. Failed to swap uuid for all alive instances.");
    return;
  }

  auto repl_clients_info = repl_instances_ | ranges::views::filter(is_not_new_main) |
                           ranges::views::transform(&ReplicationInstance::ReplicationClientInfo) |
                           ranges::to<ReplicationClientsInfo>();

  if (!new_main.PromoteToMain(new_main_uuid, std::move(repl_clients_info), &CoordinatorInstance::MainSuccessCallback,
                              &CoordinatorInstance::MainFailCallback)) {
    spdlog::warn("Failover failed since promoting replica to main failed!");
    return;
  }

  if (!raft_state_.AppendUpdateUUIDForNewMainLog(new_main_uuid)) {
    return;
  }

  auto const new_main_instance_name = new_main.InstanceName();

  if (!raft_state_.AppendSetInstanceAsMainLog(new_main_instance_name, new_main_uuid)) {
    return;
  }

  if (!raft_state_.AppendCloseLock()) {
    spdlog::error("Aborting failover as we failed to close lock on action.");
    return;
  }

  MG_ASSERT(!raft_state_.IsLockOpened(), "After failover we need to be in healthy state.");

  if (!new_main.EnableWritingOnMain()) {
    spdlog::error("Failover successful but couldn't enable writing on instance.");
  }

  spdlog::info("Failover successful! Instance {} promoted to main.", new_main.InstanceName());
}

auto CoordinatorInstance::SetReplicationInstanceToMain(std::string_view instance_name)
    -> SetInstanceToMainCoordinatorStatus {
  auto lock = std::lock_guard{coord_instance_lock_};
  if (raft_state_.IsLockOpened()) {
    return SetInstanceToMainCoordinatorStatus::LOCK_OPENED;
  }

  if (raft_state_.MainExists()) {
    return SetInstanceToMainCoordinatorStatus::MAIN_ALREADY_EXISTS;
  }

  // TODO(antoniofilipovic) Check if request leadership can cause problems due to changing of leadership while other
  // doing failover
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

  if (!raft_state_.AppendOpenLock()) {
    return SetInstanceToMainCoordinatorStatus::FAILED_TO_OPEN_LOCK;
  }

  utils::OnScopeExit do_reset{[this]() {
    if (raft_state_.IsLockOpened() && raft_state_.IsLeader()) {
      spdlog::trace(
          "Adding task to try force reset cluster as lock didn't close successfully after setting instance to MAIN.");
      thread_pool_.AddTask([this]() { this->ForceResetCluster(); });
    }
    spdlog::trace("Lock is not opened anymore or coordinator is not leader, after setting instance to MAIN.");
  }};

  new_main->PauseFrequentCheck();
  utils::OnScopeExit scope_exit{[&new_main] { new_main->ResumeFrequentCheck(); }};

  auto const is_not_new_main = [&instance_name](ReplicationInstance const &instance) {
    return instance.InstanceName() != instance_name;
  };

  auto const new_main_uuid = utils::UUID{};

  auto const failed_to_swap = [this, &new_main_uuid](ReplicationInstance &instance) {
    return !instance.SendSwapAndUpdateUUID(new_main_uuid) ||
           !raft_state_.AppendUpdateUUIDForInstanceLog(instance.InstanceName(), new_main_uuid);
  };

  if (std::ranges::any_of(repl_instances_ | ranges::views::filter(is_not_new_main), failed_to_swap)) {
    spdlog::error("Failed to swap uuid for all currently alive instances.");
    return SetInstanceToMainCoordinatorStatus::SWAP_UUID_FAILED;
  }

  auto repl_clients_info = repl_instances_ | ranges::views::filter(is_not_new_main) |
                           ranges::views::transform(&ReplicationInstance::ReplicationClientInfo) |
                           ranges::to<ReplicationClientsInfo>();

  if (!new_main->PromoteToMain(new_main_uuid, std::move(repl_clients_info), &CoordinatorInstance::MainSuccessCallback,
                               &CoordinatorInstance::MainFailCallback)) {
    return SetInstanceToMainCoordinatorStatus::COULD_NOT_PROMOTE_TO_MAIN;
  }
  if (!raft_state_.AppendUpdateUUIDForNewMainLog(new_main_uuid)) {
    return SetInstanceToMainCoordinatorStatus::RAFT_LOG_ERROR;
  }

  if (!raft_state_.AppendSetInstanceAsMainLog(instance_name, new_main_uuid)) {
    return SetInstanceToMainCoordinatorStatus::RAFT_LOG_ERROR;
  }

  spdlog::trace("Instance {} promoted to main on leader", instance_name);

  if (!raft_state_.AppendCloseLock()) {
    spdlog::error("Aborting failover as we failed to close lock on action.");
    return SetInstanceToMainCoordinatorStatus::FAILED_TO_CLOSE_LOCK;
  }
  MG_ASSERT(!raft_state_.IsLockOpened(), "After setting replication instance we need to be in healthy state.");
  if (!new_main->EnableWritingOnMain()) {
    return SetInstanceToMainCoordinatorStatus::ENABLE_WRITING_FAILED;
  }
  return SetInstanceToMainCoordinatorStatus::SUCCESS;
}

auto CoordinatorInstance::RegisterReplicationInstance(CoordinatorToReplicaConfig const &config)
    -> RegisterInstanceCoordinatorStatus {
  auto lock = std::lock_guard{coord_instance_lock_};
  if (raft_state_.IsLockOpened()) {
    return RegisterInstanceCoordinatorStatus::LOCK_OPENED;
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

  // TODO(antoniofilipovic) Check if this is an issue
  if (!raft_state_.RequestLeadership()) {
    return RegisterInstanceCoordinatorStatus::NOT_LEADER;
  }

  if (!raft_state_.AppendOpenLock()) {
    return RegisterInstanceCoordinatorStatus::FAILED_TO_OPEN_LOCK;
  }

  utils::OnScopeExit do_reset{[this]() {
    if (raft_state_.IsLockOpened() && raft_state_.IsLeader()) {
      spdlog::trace(
          "Adding task to try force reset cluster as lock didn't close successfully after registration of instance.");
      thread_pool_.AddTask([this]() { this->ForceResetCluster(); });
    }
    spdlog::trace("Lock is not opened anymore or coordinator is not leader after instance registration.");
  }};

  auto *new_instance = &repl_instances_.emplace_back(this, config, client_succ_cb_, client_fail_cb_,
                                                     &CoordinatorInstance::ReplicaSuccessCallback,
                                                     &CoordinatorInstance::ReplicaFailCallback);

  if (!new_instance->DemoteToReplica(&CoordinatorInstance::ReplicaSuccessCallback,
                                     &CoordinatorInstance::ReplicaFailCallback)) {
    // TODO(antoniofilipovic) We don't need to do here force reset, only close lock later on
    spdlog::error("Failed to send demote to replica rpc for instance {}", config.instance_name);
    return RegisterInstanceCoordinatorStatus::RPC_FAILED;
  }

  if (!raft_state_.AppendRegisterReplicationInstanceLog(config)) {
    return RegisterInstanceCoordinatorStatus::RAFT_LOG_ERROR;
  }

  if (!raft_state_.AppendCloseLock()) {
    spdlog::error("Aborting register instance as we failed to close lock on action.");
    return RegisterInstanceCoordinatorStatus::FAILED_TO_CLOSE_LOCK;
  }

  MG_ASSERT(!raft_state_.IsLockOpened(), "After registration of replication instance we need to be in healthy state.");

  new_instance->StartFrequentCheck();

  spdlog::info("Instance {} registered", config.instance_name);
  return RegisterInstanceCoordinatorStatus::SUCCESS;
}

auto CoordinatorInstance::UnregisterReplicationInstance(std::string_view instance_name)
    -> UnregisterInstanceCoordinatorStatus {
  auto lock = std::lock_guard{coord_instance_lock_};

  if (raft_state_.IsLockOpened()) {
    return UnregisterInstanceCoordinatorStatus::LOCK_OPENED;
  }

  // TODO(antoniofilipovic) Check if this is an issue
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

  auto const is_current_main = [this](ReplicationInstance const &instance) {
    return raft_state_.IsCurrentMain(instance.InstanceName()) && instance.IsAlive();
  };

  if (is_current_main(*inst_to_remove)) {
    return UnregisterInstanceCoordinatorStatus::IS_MAIN;
  }

  if (!raft_state_.AppendOpenLock()) {
    return UnregisterInstanceCoordinatorStatus::FAILED_TO_OPEN_LOCK;
  }

  utils::OnScopeExit do_reset{[this]() {
    if (raft_state_.IsLockOpened() && raft_state_.IsLeader()) {
      spdlog::trace(
          "Adding task to try force reset cluster as lock didn't close successfully after unregistration of instance.");
      thread_pool_.AddTask([this]() { this->ForceResetCluster(); });
    }
    spdlog::trace("Unregistration done. Lock is not opened anymore or coordinator is not leader.");
  }};

  inst_to_remove->StopFrequentCheck();

  auto curr_main = std::ranges::find_if(repl_instances_, is_current_main);

  if (curr_main != repl_instances_.end()) {
    if (!curr_main->SendUnregisterReplicaRpc(instance_name)) {
      inst_to_remove->StartFrequentCheck();
      return UnregisterInstanceCoordinatorStatus::RPC_FAILED;
    }
  }

  std::erase_if(repl_instances_, name_matches);

  if (!raft_state_.AppendUnregisterReplicationInstanceLog(instance_name)) {
    return UnregisterInstanceCoordinatorStatus::RAFT_LOG_ERROR;
  }

  if (!raft_state_.AppendCloseLock()) {
    spdlog::error("Aborting register instance as we failed to close lock on action.");
    return UnregisterInstanceCoordinatorStatus::FAILED_TO_CLOSE_LOCK;
  }

  MG_ASSERT(!raft_state_.IsLockOpened(),
            "After unregistration of replication instance we need to be in healthy state.");

  return UnregisterInstanceCoordinatorStatus::SUCCESS;
}

auto CoordinatorInstance::AddCoordinatorInstance(coordination::CoordinatorToCoordinatorConfig const &config) -> void {
  raft_state_.AddCoordinatorInstance(config);
  // NOTE: We ignore error we added coordinator instance to networking stuff but not in raft log.
  if (!raft_state_.AppendAddCoordinatorInstanceLog(config)) {
    spdlog::error("Failed to append add coordinator instance log");
  }
}

void CoordinatorInstance::MainFailCallback(std::string_view repl_instance_name) {
  spdlog::trace("Instance {} performing main fail callback", repl_instance_name);
  if (raft_state_.IsLockOpened()) {
    spdlog::error("Returning from main fail callback as the last action didn't successfully finish");
  }

  auto &repl_instance = FindReplicationInstance(repl_instance_name);
  repl_instance.OnFailPing();

  // NOLINTNEXTLINE
  if (!repl_instance.IsAlive() && raft_state_.IsCurrentMain(repl_instance_name)) {
    spdlog::info("Cluster without main instance, trying automatic failover");
    TryFailover();
  }
}

void CoordinatorInstance::MainSuccessCallback(std::string_view repl_instance_name) {
  spdlog::trace("Instance {} performing main successful callback", repl_instance_name);

  if (raft_state_.IsLockOpened()) {
    spdlog::error("Stopping main successful callback as the last action didn't successfully finish");
    return;
  }

  auto &repl_instance = FindReplicationInstance(repl_instance_name);

  if (repl_instance.IsAlive()) {
    repl_instance.OnSuccessPing();
    return;
  }

  // NOLINTNEXTLINE
  if (raft_state_.IsCurrentMain(repl_instance.InstanceName())) {
    if (!repl_instance.EnableWritingOnMain()) {
      spdlog::error("Failed to enable writing on main instance {}", repl_instance_name);
      return;
    }

    repl_instance.OnSuccessPing();
    return;
  }

  // Demote to replica callback

  if (!raft_state_.AppendOpenLock()) {
    spdlog::error("Raft log didn't accept instance open lock for demoting instance {} to replica.", repl_instance_name);
    return;
  }
  utils::OnScopeExit do_force_reset{[this]() {
    if (raft_state_.IsLockOpened() && raft_state_.IsLeader()) {
      spdlog::trace(
          "Adding task to try force reset cluster again as lock is opened still after setting instance needs demote.");
      thread_pool_.AddTask([this]() { this->ForceResetCluster(); });
      return;
    }
    spdlog::trace("Lock is not opened anymore or coordinator is not leader.");
  }};

  if (!raft_state_.AppendInstanceNeedsDemote(repl_instance_name)) {
    spdlog::error("Raft log didn't accept instance needs demote.");
    return;
  }

  if (!raft_state_.AppendCloseLock()) {
    spdlog::error("Raft log didn't accept instance close lock for demoting instance {} to replica.",
                  repl_instance_name);
    return;
  }

  repl_instance.SetCallbacks(&CoordinatorInstance::DemoteSuccessCallback, &CoordinatorInstance::DemoteFailCallback);
}

void CoordinatorInstance::ReplicaSuccessCallback(std::string_view repl_instance_name) {
  spdlog::trace("Instance {} performing replica successful callback", repl_instance_name);

  if (raft_state_.IsLockOpened()) {
    spdlog::error("Stopping main successful callback as the last action didn't successfully finish");
    return;
  }

  auto &repl_instance = FindReplicationInstance(repl_instance_name);

  // We need to get replicas UUID from time to time to ensure replica is listening to correct main
  // and that it didn't go down for less time than we could notice
  // We need to get id of main replica is listening to
  // and swap if necessary
  if (!repl_instance.EnsureReplicaHasCorrectMainUUID(raft_state_.GetCurrentMainUUID())) {
    spdlog::error("Failed to swap uuid for replica instance {} which is alive", repl_instance.InstanceName());
    return;
  }

  repl_instance.OnSuccessPing();
}

void CoordinatorInstance::ReplicaFailCallback(std::string_view repl_instance_name) {
  spdlog::trace("Instance {} performing replica failure callback", repl_instance_name);

  if (raft_state_.IsLockOpened()) {
    spdlog::error("Stopping main successful callback as the last action didn't successfully finish.");
    return;
  }

  auto &repl_instance = FindReplicationInstance(repl_instance_name);

  repl_instance.OnFailPing();
}

void CoordinatorInstance::DemoteSuccessCallback(std::string_view repl_instance_name) {
  spdlog::trace("Instance {} performing demote to replica successful callback", repl_instance_name);

  auto &repl_instance = FindReplicationInstance(repl_instance_name);

  if (!raft_state_.AppendOpenLock()) {
    spdlog::error("Failed to open lock for demoting instance {} to REPLICA", repl_instance_name);
    return;
  }

  utils::OnScopeExit do_force_reset{[this]() {
    if (raft_state_.IsLockOpened() && raft_state_.IsLeader()) {
      spdlog::trace("Adding task to try force reset cluster again as lock is opened still after demoting instance.");
      thread_pool_.AddTask([this]() { this->ForceResetCluster(); });
      return;
    }
    spdlog::trace("Lock is not opened anymore or coordinator is not leader.");
  }};

  // Can't set callbacks still
  if (repl_instance.SendDemoteToReplicaRpc()) {
    spdlog::info("Instance {} demoted to replica", repl_instance_name);
  } else {
    spdlog::error("Instance {} failed to become replica", repl_instance_name);
    return;
  }

  if (!raft_state_.AppendSetInstanceAsReplicaLog(repl_instance_name)) {
    spdlog::error("Failed to append log that OLD MAIN was demoted to REPLICA {}", repl_instance_name);
    return;
  }

  if (!repl_instance.SendSwapAndUpdateUUID(raft_state_.GetCurrentMainUUID())) {
    spdlog::error("Failed to swap uuid for demoted main instance {}", repl_instance_name);
    return;
  }

  if (!raft_state_.AppendUpdateUUIDForInstanceLog(repl_instance_name, raft_state_.GetCurrentMainUUID())) {
    spdlog::error("Failed to update log of changing instance uuid {} to {}", repl_instance_name,
                  std::string{raft_state_.GetCurrentMainUUID()});
    return;
  }

  if (!raft_state_.AppendCloseLock()) {
    spdlog::error("Failed to close lock for demoting MAIN to REPLICA", repl_instance_name);
    return;
  }

  repl_instance.SetCallbacks(&CoordinatorInstance::ReplicaSuccessCallback, &CoordinatorInstance::ReplicaFailCallback);

  repl_instance.OnSuccessPing();
}

void CoordinatorInstance::DemoteFailCallback(std::string_view repl_instance_name) {
  spdlog::trace("Instance {} performing demote to replica failure callback", repl_instance_name);
  auto &repl_instance = FindReplicationInstance(repl_instance_name);
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

auto CoordinatorInstance::HasMainState(std::string_view instance_name) const -> bool {
  return raft_state_.HasMainState(instance_name);
}

auto CoordinatorInstance::HasReplicaState(std::string_view instance_name) const -> bool {
  return raft_state_.HasReplicaState(instance_name);
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
