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
#include "coordination/utils.hpp"
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
<<<<<<< HEAD
          [this] { std::ranges::for_each(repl_instances_, &ReplicationInstance::StopFrequentCheck); })) {
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
  client_succ_cb_ = [](CoordinatorInstance *self, std::string_view repl_instance_name) -> void {
    auto lock = std::unique_lock{self->coord_instance_lock_};
    auto &repl_instance = self->FindReplicationInstance(repl_instance_name);
    std::invoke(repl_instance.GetSuccessCallback(), self, repl_instance_name);
||||||| parent of b1af5ceeb (Move ReplRole to ClusterState)
  client_succ_cb_ = [](CoordinatorInstance *self, std::string_view repl_instance_name) -> void {
    auto lock = std::unique_lock{self->coord_instance_lock_};
    auto &repl_instance = self->FindReplicationInstance(repl_instance_name);

    std::invoke(repl_instance.GetSuccessCallback(), self, repl_instance_name, std::move(lock));
=======
  auto find_repl_instance = [](CoordinatorInstance *self,
                               std::string_view repl_instance_name) -> ReplicationInstance & {
    auto repl_instance =
        std::ranges::find_if(self->repl_instances_, [repl_instance_name](ReplicationInstance const &instance) {
          return instance.InstanceName() == repl_instance_name;
        });

    MG_ASSERT(repl_instance != self->repl_instances_.end(), "Instance {} not found during callback!",
              repl_instance_name);
    return *repl_instance;
>>>>>>> b1af5ceeb (Move ReplRole to ClusterState)
  };

<<<<<<< HEAD
  client_fail_cb_ = [](CoordinatorInstance *self, std::string_view repl_instance_name) -> void {
    auto lock = std::unique_lock{self->coord_instance_lock_};
    auto &repl_instance = self->FindReplicationInstance(repl_instance_name);
    std::invoke(repl_instance.GetFailCallback(), self, repl_instance_name);
||||||| parent of b1af5ceeb (Move ReplRole to ClusterState)
  client_fail_cb_ = [](CoordinatorInstance *self, std::string_view repl_instance_name) -> void {
    auto lock = std::unique_lock{self->coord_instance_lock_};
    auto &repl_instance = self->FindReplicationInstance(repl_instance_name);
    std::invoke(repl_instance.GetFailCallback(), self, repl_instance_name, std::move(lock));
=======
  replica_succ_cb_ = [find_repl_instance](CoordinatorInstance *self, std::string_view repl_instance_name) -> void {
    auto lock = std::lock_guard{self->coord_instance_lock_};
    spdlog::trace("Instance {} performing replica successful callback", repl_instance_name);
    auto &repl_instance = find_repl_instance(self, repl_instance_name);

    // We need to get replicas UUID from time to time to ensure replica is listening to correct main
    // and that it didn't go down for less time than we could notice
    // We need to get id of main replica is listening to
    // and swap if necessary
    if (!repl_instance.EnsureReplicaHasCorrectMainUUID(self->main_uuid_)) {
      spdlog::error("Failed to swap uuid for alive replica instance {}.", repl_instance.InstanceName());
      return;
    }

    repl_instance.OnSuccessPing();
>>>>>>> b1af5ceeb (Move ReplRole to ClusterState)
  };

  replica_fail_cb_ = [find_repl_instance](CoordinatorInstance *self, std::string_view repl_instance_name) -> void {
    auto lock = std::lock_guard{self->coord_instance_lock_};
    spdlog::trace("Instance {} performing replica failure callback", repl_instance_name);
    auto &repl_instance = find_repl_instance(self, repl_instance_name);
    repl_instance.OnFailPing();
    // We need to restart main uuid from instance since it was "down" at least a second
    // There is slight delay, if we choose to use isAlive, instance can be down and back up in less than
    // our isAlive time difference, which would lead to instance
    // https://github.com/memgraph/memgraph/pull/1720#discussion_r1493833414 setting UUID to nullopt and stopping
    // accepting any incoming RPCs from valid main
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

    if (self->main_uuid_ == repl_instance_uuid.value()) {
      if (!repl_instance.EnableWritingOnMain()) {
        spdlog::error("Failed to enable writing on main instance {}", repl_instance_name);
        return;
      }

      repl_instance.OnSuccessPing();
      return;
    }

    if (!self->raft_state_.RequestLeadership()) {
      spdlog::error("Failed to request leadership for demoting instance to replica {}.", repl_instance_name);
      return;
    }

    // TODO: (andi) Improve std::string, appending...
    auto const res = self->raft_state_.AppendSetInstanceAsReplica(std::string(repl_instance_name));
    if (!res->get_accepted()) {
      spdlog::error(
          "Failed to accept request for demoting instance {}. Most likely the reason is that the instance is not the "
          "leader.",
          repl_instance_name);
      return;
    }

    if (res->get_result_code() != nuraft::cmd_result_code::OK) {
      spdlog::error("Failed to demote instance {} with error code {}", repl_instance_name, res->get_result_code());
      return;
    }

    spdlog::info("Request for demoting instance {} accepted", repl_instance_name);

    // TODO(antoniof) make demoteToReplica idempotent since main can be demoted to replica but
    // swapUUID can fail
    if (!repl_instance.DemoteToReplica(self->replica_succ_cb_, self->replica_fail_cb_)) {
      spdlog::error("Instance {} failed to become replica", repl_instance_name);
      return;
    }

    if (!repl_instance.SendSwapAndUpdateUUID(self->main_uuid_)) {
      spdlog::error(fmt::format("Failed to swap uuid for demoted main instance {}", repl_instance.InstanceName()));
      return;
    }

    repl_instance.OnSuccessPing();
    spdlog::info("Instance {} demoted to replica", repl_instance_name);
  };

  main_fail_cb_ = [find_repl_instance](CoordinatorInstance *self, std::string_view repl_instance_name) -> void {
    auto lock = std::lock_guard{self->coord_instance_lock_};
    spdlog::trace("Instance {} performing main failure callback", repl_instance_name);
    auto &repl_instance = find_repl_instance(self, repl_instance_name);
    repl_instance.OnFailPing();
    const auto &repl_instance_uuid = repl_instance.GetMainUUID();
    MG_ASSERT(repl_instance_uuid.has_value(), "Instance must have uuid set");

    if (!repl_instance.IsAlive() && self->main_uuid_ == repl_instance_uuid.value()) {
      spdlog::info("Cluster without main instance, trying automatic failover");
      self->TryFailover();
    }
  };
||||||| parent of 99c53148c (registration backed-up by raft)
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

    // We need to get replicas UUID from time to time to ensure replica is listening to correct main
    // and that it didn't go down for less time than we could notice
    // We need to get id of main replica is listening to
    // and swap if necessary
    if (!repl_instance.EnsureReplicaHasCorrectMainUUID(self->main_uuid_)) {
      spdlog::error("Failed to swap uuid for alive replica instance {}.", repl_instance.InstanceName());
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
    // our isAlive time difference, which would lead to instance
    // https://github.com/memgraph/memgraph/pull/1720#discussion_r1493833414 setting UUID to nullopt and stopping
    // accepting any incoming RPCs from valid main
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

    if (self->main_uuid_ == repl_instance_uuid.value()) {
      if (!repl_instance.EnableWritingOnMain()) {
        spdlog::error("Failed to enable writing on main instance {}", repl_instance_name);
        return;
      }

      repl_instance.OnSuccessPing();
      return;
    }

    if (!self->raft_state_.RequestLeadership()) {
      spdlog::error("Failed to request leadership for demoting instance to replica {}.", repl_instance_name);
      return;
    }

    // TODO: (andi) Improve std::string, appending...
    auto const res = self->raft_state_.AppendSetInstanceAsReplica(std::string(repl_instance_name));
    if (!res->get_accepted()) {
      spdlog::error(
          "Failed to accept request for demoting instance {}. Most likely the reason is that the instance is not the "
          "leader.",
          repl_instance_name);
      return;
    }

    if (res->get_result_code() != nuraft::cmd_result_code::OK) {
      spdlog::error("Failed to demote instance {} with error code {}", repl_instance_name, res->get_result_code());
      return;
    }

    spdlog::info("Request for demoting instance {} accepted", repl_instance_name);

    // TODO(antoniof) make demoteToReplica idempotent since main can be demoted to replica but
    // swapUUID can fail
    if (!repl_instance.DemoteToReplica(self->replica_succ_cb_, self->replica_fail_cb_)) {
      spdlog::error("Instance {} failed to become replica", repl_instance_name);
      return;
    }

    if (!repl_instance.SendSwapAndUpdateUUID(self->main_uuid_)) {
      spdlog::error(fmt::format("Failed to swap uuid for demoted main instance {}", repl_instance.InstanceName()));
      return;
    }

    repl_instance.OnSuccessPing();
    spdlog::info("Instance {} demoted to replica", repl_instance_name);
  };

  main_fail_cb_ = [find_repl_instance](CoordinatorInstance *self, std::string_view repl_instance_name) -> void {
    auto lock = std::lock_guard{self->coord_instance_lock_};
    spdlog::trace("Instance {} performing main failure callback", repl_instance_name);
    auto &repl_instance = find_repl_instance(self, repl_instance_name);
    repl_instance.OnFailPing();
    const auto &repl_instance_uuid = repl_instance.GetMainUUID();
    MG_ASSERT(repl_instance_uuid.has_value(), "Instance must have uuid set");

    if (!repl_instance.IsAlive() && self->main_uuid_ == repl_instance_uuid.value()) {
      spdlog::info("Cluster without main instance, trying automatic failover");
      self->TryFailover();
    }
  };
=======
>>>>>>> 99c53148c (registration backed-up by raft)
||||||| parent of 9081c5c24 (Optional main on unregistering)
=======
||||||| parent of fab8d3d76 (Shared (Un)Registration networking part with raft)
          [this] { std::ranges::for_each(repl_instances_, &ReplicationInstance::StopFrequentCheck); })) {
=======
          [this] { std::ranges::for_each(repl_instances_, &ReplicationInstance::StopFrequentCheck); },
          [this](TRaftLog const &log_entry, RaftLogAction log_action) {
            OnRaftCommitCallback(log_entry, log_action);
          })) {
>>>>>>> fab8d3d76 (Shared (Un)Registration networking part with raft)
  client_succ_cb_ = [](CoordinatorInstance *self, std::string_view repl_instance_name) -> void {
    auto lock = std::unique_lock{self->coord_instance_lock_};
    auto &repl_instance = self->FindReplicationInstance(repl_instance_name);
    std::invoke(repl_instance.GetSuccessCallback(), self, repl_instance_name, std::move(lock));
  };

  client_fail_cb_ = [](CoordinatorInstance *self, std::string_view repl_instance_name) -> void {
    auto lock = std::unique_lock{self->coord_instance_lock_};
    auto &repl_instance = self->FindReplicationInstance(repl_instance_name);
    std::invoke(repl_instance.GetFailCallback(), self, repl_instance_name, std::move(lock));
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
>>>>>>> 9081c5c24 (Optional main on unregistering)
}

auto CoordinatorInstance::ShowInstances() const -> std::vector<InstanceStatus> {
  auto const coord_instances = raft_state_.GetAllCoordinators();

  auto const coord_instance_to_status = [](ptr<srv_config> const &instance) -> InstanceStatus {
    return {.instance_name = "coordinator_" + std::to_string(instance->get_id()),
            .raft_socket_address = instance->get_endpoint(),
            .cluster_role = "coordinator",
            .health = "unknown"};  // TODO: (andi) Improve
  };

  auto instances_status = utils::fmap(coord_instance_to_status, coord_instances);

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
              .coord_socket_address = instance.SocketAddress(),
              .cluster_role = stringify_repl_role(instance),
              .health = stringify_repl_health(instance)};
    };
    {
      auto lock = std::shared_lock{coord_instance_lock_};
      std::ranges::transform(repl_instances_, std::back_inserter(instances_status), process_repl_instance_as_leader);
    }
  } else {
    auto process_repl_instance_as_follower = [](auto const &instance) -> InstanceStatus {
      return {.instance_name = instance.first, .cluster_role = instance.second, .health = "unknown"};
    };
    {
      auto lock = std::shared_lock{coord_instance_lock_};
      std::ranges::transform(raft_state_.GetInstances(), std::back_inserter(instances_status),
                             process_repl_instance_as_follower);
    }
  }

  return instances_status;
}

auto CoordinatorInstance::TryFailover() -> void {
  auto const is_replica = [this](ReplicationInstance const &instance) {
    return raft_state_.IsReplica(instance.InstanceName());
  };

  auto alive_replicas =
      repl_instances_ | ranges::views::filter(is_replica) | ranges::views::filter(&ReplicationInstance::IsAlive);

  if (ranges::empty(alive_replicas)) {
    spdlog::warn("Failover failed since all replicas are down!");
    return;
  }

<<<<<<< HEAD
<<<<<<< HEAD
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
||||||| parent of b1af5ceeb (Move ReplRole to ClusterState)
  std::string most_up_to_date_instance;
  std::optional<uint64_t> latest_commit_timestamp;
  std::optional<std::string> latest_epoch;
  {
    // for each DB in instance we get one DatabaseHistory
    using DatabaseHistories = replication_coordination_glue::DatabaseHistories;
    std::vector<std::pair<std::string, DatabaseHistories>> instance_database_histories;

    bool success{true};
    std::for_each(alive_replicas.begin(), alive_replicas.end(),
                  [&success, &instance_database_histories](ReplicationInstance &replica) {
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

    most_up_to_date_instance =
        coordination::ChooseMostUpToDateInstance(instance_database_histories, latest_epoch, latest_commit_timestamp);
  }

  spdlog::trace("The most up to date instance is {} with epoch {} and {} latest commit timestamp",
                most_up_to_date_instance, *latest_epoch, *latest_commit_timestamp);  // NOLINT

  auto &new_repl_instance = FindReplicationInstance(most_up_to_date_instance);
  auto *new_main = &new_repl_instance;
=======
  // TODO: Smarter choice
  auto new_main = ranges::begin(alive_replicas);
>>>>>>> b1af5ceeb (Move ReplRole to ClusterState)
||||||| parent of 9081c5c24 (Optional main on unregistering)
  // TODO: Smarter choice
  auto new_main = ranges::begin(alive_replicas);
=======
  std::string most_up_to_date_instance;
  std::optional<uint64_t> latest_commit_timestamp;
  std::optional<std::string> latest_epoch;
  {
    // for each DB in instance we get one DatabaseHistory
    using DatabaseHistories = replication_coordination_glue::DatabaseHistories;
    std::vector<std::pair<std::string, DatabaseHistories>> instance_database_histories;

    bool success{true};
    std::for_each(alive_replicas.begin(), alive_replicas.end(),
                  [&success, &instance_database_histories](ReplicationInstance &replica) {
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

    most_up_to_date_instance =
        coordination::ChooseMostUpToDateInstance(instance_database_histories, latest_epoch, latest_commit_timestamp);
  }
  spdlog::trace("The most up to date instance is {} with epoch {} and {} latest commit timestamp",
                most_up_to_date_instance, *latest_epoch, *latest_commit_timestamp);

  auto &new_repl_instance = FindReplicationInstance(most_up_to_date_instance);
  auto *new_main = &new_repl_instance;
>>>>>>> 9081c5c24 (Optional main on unregistering)

  if (!raft_state_.RequestLeadership()) {
    spdlog::error("Failed to request leadership for promoting instance to main {}.", new_main->InstanceName());
    return;
  }

  // TODO: (andi) Improve std::string, appending...
  auto const res = raft_state_.AppendSetInstanceAsMain(new_main->InstanceName());
  if (!res->get_accepted()) {
    spdlog::error(
        "Failed to accept request for promoting instance {}. Most likely the reason is that the instance is not the "
        "leader.",
        new_main->InstanceName());
    return;
  }

  if (res->get_result_code() != nuraft::cmd_result_code::OK) {
    spdlog::error("Failed to promote instance {} with error code {}", new_main->InstanceName(), res->get_result_code());
    return;
  }

  spdlog::info("Request for promoting instance {} accepted", new_main->InstanceName());
}

<<<<<<< HEAD
// TODO: (andi) Make sure you cannot put coordinator instance to the main
auto CoordinatorInstance::SetReplicationInstanceToMain(std::string instance_name)
    -> SetInstanceToMainCoordinatorStatus {
  auto lock = std::lock_guard{coord_instance_lock_};

  if (raft_state_.MainExists()) {
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

  if (!raft_state_.RequestLeadership()) {
    return SetInstanceToMainCoordinatorStatus::NOT_LEADER;
  }

  auto const res = raft_state_.AppendSetInstanceAsMain(instance_name);
  if (!res->get_accepted()) {
    spdlog::error(
        "Failed to accept request for promoting instance {}. Most likely the reason is that the instance is not "
        "the leader.",
        instance_name);
    return SetInstanceToMainCoordinatorStatus::RAFT_COULD_NOT_ACCEPT;
  }

  if (res->get_result_code() != nuraft::cmd_result_code::OK) {
    spdlog::error("Failed to promote instance {} with error code {}", instance_name, res->get_result_code());
    return SetInstanceToMainCoordinatorStatus::RAFT_COULD_NOT_APPEND;
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

  auto repl_clients_info = repl_instances_ | ranges::views::filter(is_not_new_main) |
                           ranges::views::transform(&ReplicationInstance::ReplicationClientInfo) |
                           ranges::to<ReplicationClientsInfo>();

  if (!new_main->PromoteToMain(new_main_uuid, std::move(repl_clients_info), &CoordinatorInstance::MainSuccessCallback,
                               &CoordinatorInstance::MainFailCallback)) {
    return SetInstanceToMainCoordinatorStatus::COULD_NOT_PROMOTE_TO_MAIN;
  }

  main_uuid_ = new_main_uuid;
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

<<<<<<< HEAD
<<<<<<< HEAD
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

||||||| parent of b1af5ceeb (Move ReplRole to ClusterState)
||||||| parent of fab8d3d76 (Shared (Un)Registration networking part with raft)
// TODO: (andi) Make sure you cannot put coordinator instance to the main
auto CoordinatorInstance::SetReplicationInstanceToMain(std::string instance_name)
    -> SetInstanceToMainCoordinatorStatus {
  auto lock = std::lock_guard{coord_instance_lock_};

  if (raft_state_.MainExists()) {
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

  if (!raft_state_.RequestLeadership()) {
    return SetInstanceToMainCoordinatorStatus::NOT_LEADER;
  }

  auto const res = raft_state_.AppendSetInstanceAsMain(instance_name);
  if (!res->get_accepted()) {
    spdlog::error(
        "Failed to accept request for promoting instance {}. Most likely the reason is that the instance is not "
        "the leader.",
        instance_name);
    return SetInstanceToMainCoordinatorStatus::RAFT_COULD_NOT_ACCEPT;
  }

  if (res->get_result_code() != nuraft::cmd_result_code::OK) {
    spdlog::error("Failed to promote instance {} with error code {}", instance_name, res->get_result_code());
    return SetInstanceToMainCoordinatorStatus::RAFT_COULD_NOT_APPEND;
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

  auto repl_clients_info = repl_instances_ | ranges::views::filter(is_not_new_main) |
                           ranges::views::transform(&ReplicationInstance::ReplicationClientInfo) |
                           ranges::to<ReplicationClientsInfo>();

  if (!new_main->PromoteToMain(new_main_uuid, std::move(repl_clients_info), &CoordinatorInstance::MainSuccessCallback,
                               &CoordinatorInstance::MainFailCallback)) {
    return SetInstanceToMainCoordinatorStatus::COULD_NOT_PROMOTE_TO_MAIN;
  }

  main_uuid_ = new_main_uuid;
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

=======
>>>>>>> fab8d3d76 (Shared (Un)Registration networking part with raft)
void CoordinatorInstance::MainFailCallback(std::string_view repl_instance_name,
                                           std::unique_lock<utils::ResourceLock> lock) {
  MG_ASSERT(lock.owns_lock(), "Callback doesn't own lock");
  spdlog::trace("Instance {} performing main failure callback", repl_instance_name);
  auto &repl_instance = FindReplicationInstance(repl_instance_name);
  repl_instance.OnFailPing();
  const auto &repl_instance_uuid = repl_instance.GetMainUUID();
  MG_ASSERT(repl_instance_uuid.has_value(), "Instance must have uuid set");

  // NOLINTNEXTLINE
  if (!repl_instance.IsAlive() && main_uuid_ == repl_instance_uuid.value()) {
    spdlog::info("Cluster without main instance, trying automatic failover");
    TryFailover();
  }
}

void CoordinatorInstance::MainSuccessCallback(std::string_view repl_instance_name,
                                              std::unique_lock<utils::ResourceLock> lock) {
  MG_ASSERT(lock.owns_lock(), "Callback doesn't own lock");
  spdlog::trace("Instance {} performing main successful callback", repl_instance_name);

  auto &repl_instance = FindReplicationInstance(repl_instance_name);

  if (repl_instance.IsAlive()) {
    repl_instance.OnSuccessPing();
    return;
  }

  const auto &repl_instance_uuid = repl_instance.GetMainUUID();
  MG_ASSERT(repl_instance_uuid.has_value(), "Instance must have uuid set.");

  // NOLINTNEXTLINE
  if (main_uuid_ == repl_instance_uuid.value()) {
    if (!repl_instance.EnableWritingOnMain()) {
      spdlog::error("Failed to enable writing on main instance {}", repl_instance_name);
      return;
    }

    repl_instance.OnSuccessPing();
    return;
  }

  if (!raft_state_.RequestLeadership()) {
    spdlog::error("Failed to request leadership for demoting instance to replica {}.", repl_instance_name);
    return;
  }

  // TODO: (andi) Improve std::string, appending...
  auto const res = raft_state_.AppendSetInstanceAsReplica(std::string(repl_instance_name));
  if (!res->get_accepted()) {
    spdlog::error(
        "Failed to accept request for demoting instance {}. Most likely the reason is that the instance is not the "
        "leader.",
        repl_instance_name);
    return;
  }

  if (res->get_result_code() != nuraft::cmd_result_code::OK) {
    spdlog::error("Failed to demote instance {} with error code {}", repl_instance_name, res->get_result_code());
    return;
  }

  spdlog::info("Request for demoting instance {} accepted", repl_instance_name);
}

void CoordinatorInstance::ReplicaSuccessCallback(std::string_view repl_instance_name,
                                                 std::unique_lock<utils::ResourceLock> lock) {
  MG_ASSERT(lock.owns_lock(), "Callback doesn't own lock");
  auto &repl_instance = FindReplicationInstance(repl_instance_name);

  auto const is_replica = [this](ReplicationInstance const &instance) {
    return raft_state_.IsReplica(instance.InstanceName());
  };

  if (!is_replica(repl_instance)) {
    spdlog::error("Aborting replica callback since instance {} is not replica anymore", repl_instance_name);
    return;
  }
  spdlog::trace("Instance {} performing replica successful callback", repl_instance_name);
  // We need to get replicas UUID from time to time to ensure replica is listening to correct main
  // and that it didn't go down for less time than we could notice
  // We need to get id of main replica is listening to
  // and swap if necessary
  if (!repl_instance.EnsureReplicaHasCorrectMainUUID(main_uuid_)) {
    spdlog::error("Failed to swap uuid for replica instance {} which is alive", repl_instance.InstanceName());
    return;
  }

  repl_instance.OnSuccessPing();
}

void CoordinatorInstance::ReplicaFailCallback(std::string_view repl_instance_name,
                                              std::unique_lock<utils::ResourceLock> lock) {
  MG_ASSERT(lock.owns_lock(), "Callback doesn't own lock");
  auto &repl_instance = FindReplicationInstance(repl_instance_name);
  auto const is_replica = [this](ReplicationInstance const &instance) {
    return raft_state_.IsReplica(instance.InstanceName());
  };

  if (!is_replica(repl_instance)) {
    spdlog::error("Aborting replica fail callback since instance {} is not replica anymore", repl_instance_name);
    return;
  }

  spdlog::trace("Instance {} performing replica failure callback", repl_instance_name);
  repl_instance.OnFailPing();
}

<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> b1af5ceeb (Move ReplRole to ClusterState)
||||||| parent of 9081c5c24 (Optional main on unregistering)
=======
void CoordinatorInstance::MainFailCallback(std::string_view repl_instance_name,
                                           std::unique_lock<utils::ResourceLock> lock) {
  MG_ASSERT(lock.owns_lock(), "Callback doesn't own lock");
  spdlog::trace("Instance {} performing main failure callback", repl_instance_name);
  auto &repl_instance = FindReplicationInstance(repl_instance_name);
  repl_instance.OnFailPing();
  const auto &repl_instance_uuid = repl_instance.GetMainUUID();
  MG_ASSERT(repl_instance_uuid.has_value(), "Instance must have uuid set");

  if (!repl_instance.IsAlive() && GetMainUUID() == repl_instance_uuid.value()) {
    spdlog::info("Cluster without main instance, trying automatic failover");
    TryFailover();
  }
}

void CoordinatorInstance::MainSuccessCallback(std::string_view repl_instance_name,
                                              std::unique_lock<utils::ResourceLock> lock) {
  MG_ASSERT(lock.owns_lock(), "Callback doesn't own lock");
  spdlog::trace("Instance {} performing main successful callback", repl_instance_name);

  auto &repl_instance = FindReplicationInstance(repl_instance_name);

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

void CoordinatorInstance::ReplicaSuccessCallback(std::string_view repl_instance_name,
                                                 std::unique_lock<utils::ResourceLock> lock) {
  MG_ASSERT(lock.owns_lock(), "Callback doesn't own lock");
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

void CoordinatorInstance::ReplicaFailCallback(std::string_view repl_instance_name,
                                              std::unique_lock<utils::ResourceLock> lock) {
  MG_ASSERT(lock.owns_lock(), "Callback doesn't own lock");
  auto &repl_instance = FindReplicationInstance(repl_instance_name);
  if (!repl_instance.IsReplica()) {
    spdlog::error("Aborting replica fail callback since instance {} is not replica anymore", repl_instance_name);
    return;
  }
  spdlog::trace("Instance {} performing replica failure callback", repl_instance_name);
  repl_instance.OnFailPing();
}

>>>>>>> 9081c5c24 (Optional main on unregistering)
||||||| parent of fab8d3d76 (Shared (Un)Registration networking part with raft)
=======
// TODO: (andi) Make sure you cannot put coordinator instance to the main
auto CoordinatorInstance::SetReplicationInstanceToMain(std::string instance_name)
    -> SetInstanceToMainCoordinatorStatus {
  auto lock = std::lock_guard{coord_instance_lock_};

  if (raft_state_.MainExists()) {
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

  if (!raft_state_.RequestLeadership()) {
    return SetInstanceToMainCoordinatorStatus::NOT_LEADER;
  }

  auto const res = raft_state_.AppendSetInstanceAsMain(instance_name);
  if (!res->get_accepted()) {
    spdlog::error(
        "Failed to accept request for promoting instance {}. Most likely the reason is that the instance is not "
        "the leader.",
        instance_name);
    return SetInstanceToMainCoordinatorStatus::RAFT_COULD_NOT_ACCEPT;
  }

  if (res->get_result_code() != nuraft::cmd_result_code::OK) {
    spdlog::error("Failed to promote instance {} with error code {}", instance_name, res->get_result_code());
    return SetInstanceToMainCoordinatorStatus::RAFT_COULD_NOT_APPEND;
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

  auto repl_clients_info = repl_instances_ | ranges::views::filter(is_not_new_main) |
                           ranges::views::transform(&ReplicationInstance::ReplicationClientInfo) |
                           ranges::to<ReplicationClientsInfo>();

  if (!new_main->PromoteToMain(new_main_uuid, std::move(repl_clients_info), &CoordinatorInstance::MainSuccessCallback,
                               &CoordinatorInstance::MainFailCallback)) {
    return SetInstanceToMainCoordinatorStatus::COULD_NOT_PROMOTE_TO_MAIN;
  }

  main_uuid_ = new_main_uuid;
  spdlog::info("Instance {} promoted to main", instance_name);
  return SetInstanceToMainCoordinatorStatus::SUCCESS;
}

||||||| parent of ecf7e6839 (Set instance to main shared with commit)
// TODO: (andi) Make sure you cannot put coordinator instance to the main
auto CoordinatorInstance::SetReplicationInstanceToMain(std::string instance_name)
    -> SetInstanceToMainCoordinatorStatus {
  auto lock = std::lock_guard{coord_instance_lock_};

  if (raft_state_.MainExists()) {
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

  if (!raft_state_.RequestLeadership()) {
    return SetInstanceToMainCoordinatorStatus::NOT_LEADER;
  }

  auto const res = raft_state_.AppendSetInstanceAsMain(instance_name);
  if (!res->get_accepted()) {
    spdlog::error(
        "Failed to accept request for promoting instance {}. Most likely the reason is that the instance is not "
        "the leader.",
        instance_name);
    return SetInstanceToMainCoordinatorStatus::RAFT_COULD_NOT_ACCEPT;
  }

  if (res->get_result_code() != nuraft::cmd_result_code::OK) {
    spdlog::error("Failed to promote instance {} with error code {}", instance_name, res->get_result_code());
    return SetInstanceToMainCoordinatorStatus::RAFT_COULD_NOT_APPEND;
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

  auto repl_clients_info = repl_instances_ | ranges::views::filter(is_not_new_main) |
                           ranges::views::transform(&ReplicationInstance::ReplicationClientInfo) |
                           ranges::to<ReplicationClientsInfo>();

  if (!new_main->PromoteToMain(new_main_uuid, std::move(repl_clients_info), &CoordinatorInstance::MainSuccessCallback,
                               &CoordinatorInstance::MainFailCallback)) {
    return SetInstanceToMainCoordinatorStatus::COULD_NOT_PROMOTE_TO_MAIN;
  }

  main_uuid_ = new_main_uuid;
  spdlog::info("Instance {} promoted to main", instance_name);
  return SetInstanceToMainCoordinatorStatus::SUCCESS;
}

=======
>>>>>>> ecf7e6839 (Set instance to main shared with commit)
// TODO: (andi) Status of registration, maybe not all needed.
// Incorporate checking of replication socket address.
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

  if (!raft_state_.RequestLeadership()) {
    return RegisterInstanceCoordinatorStatus::NOT_LEADER;
  }

  auto const res = raft_state_.AppendRegisterReplicationInstance(config);
  if (!res->get_accepted()) {
    spdlog::error(
        "Failed to accept request for registering instance {}. Most likely the reason is that the instance is not "
        "the "
        "leader.",
        config.instance_name);
    return RegisterInstanceCoordinatorStatus::RAFT_COULD_NOT_ACCEPT;
  }

  spdlog::info("Request for registering instance {} accepted", config.instance_name);

  if (res->get_result_code() != nuraft::cmd_result_code::OK) {
    spdlog::error("Failed to register instance {} with error code {}", config.instance_name, res->get_result_code());
    return RegisterInstanceCoordinatorStatus::RAFT_COULD_NOT_APPEND;
  }

  return RegisterInstanceCoordinatorStatus::SUCCESS;
}

>>>>>>> fab8d3d76 (Shared (Un)Registration networking part with raft)
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

  auto const is_main = [this](ReplicationInstance const &instance) {
    return raft_state_.IsMain(instance.InstanceName());
  };

  // TODO: (andi) Better thing would be to have role UNKNOWN and then no need to check if it is alive. Or optional role.
  if (std::invoke(is_main, *inst_to_remove) && inst_to_remove->IsAlive()) {
    return UnregisterInstanceCoordinatorStatus::IS_MAIN;
  }

  if (!raft_state_.RequestLeadership()) {
    return UnregisterInstanceCoordinatorStatus::NOT_LEADER;
  }

  auto const res = raft_state_.AppendUnregisterReplicationInstance(instance_name);
  if (!res->get_accepted()) {
    spdlog::error(
        "Failed to accept request for unregistering instance {}. Most likely the reason is that the instance is not "
        "the leader.",
        instance_name);
    return UnregisterInstanceCoordinatorStatus::RAFT_COULD_NOT_ACCEPT;
  }
  spdlog::info("Request for unregistering instance {} accepted", instance_name);

  if (res->get_result_code() != nuraft::cmd_result_code::OK) {
    spdlog::error("Failed to unregister instance {} with error code {}", instance_name, res->get_result_code());
    return UnregisterInstanceCoordinatorStatus::RAFT_COULD_NOT_APPEND;
  }

  return UnregisterInstanceCoordinatorStatus::SUCCESS;
}

auto CoordinatorInstance::AddCoordinatorInstance(uint32_t raft_server_id, uint32_t raft_port, std::string raft_address)
    -> void {
  raft_state_.AddCoordinatorInstance(raft_server_id, raft_port, std::move(raft_address));
}

<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
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
||||||| parent of b1af5ceeb (Move ReplRole to ClusterState)
auto CoordinatorInstance::GetMainUUID() const -> utils::UUID { return main_uuid_; }

// TODO: (andi) Add to the RAFT log.
auto CoordinatorInstance::SetMainUUID(utils::UUID new_uuid) -> void { main_uuid_ = new_uuid; }

=======
>>>>>>> b1af5ceeb (Move ReplRole to ClusterState)
||||||| parent of 9081c5c24 (Optional main on unregistering)
=======
auto CoordinatorInstance::GetMainUUID() const -> utils::UUID { return main_uuid_; }

// TODO: (andi) Add to the RAFT log.
auto CoordinatorInstance::SetMainUUID(utils::UUID new_uuid) -> void { main_uuid_ = new_uuid; }

>>>>>>> 9081c5c24 (Optional main on unregistering)
||||||| parent of 1b150ee92 (Address PR comments)
auto CoordinatorInstance::GetMainUUID() const -> utils::UUID { return main_uuid_; }

// TODO: (andi) Add to the RAFT log.
auto CoordinatorInstance::SetMainUUID(utils::UUID new_uuid) -> void { main_uuid_ = new_uuid; }

=======
>>>>>>> 1b150ee92 (Address PR comments)
||||||| parent of fab8d3d76 (Shared (Un)Registration networking part with raft)
=======
||||||| parent of ecf7e6839 (Set instance to main shared with commit)
=======
// TODO: (andi) Make sure you cannot put coordinator instance to the main
// change arg types
auto CoordinatorInstance::SetReplicationInstanceToMain(std::string instance_name)
    -> SetInstanceToMainCoordinatorStatus {
  auto lock = std::lock_guard{coord_instance_lock_};

  if (raft_state_.MainExists()) {
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

  if (!raft_state_.RequestLeadership()) {
    return SetInstanceToMainCoordinatorStatus::NOT_LEADER;
  }

  auto const res = raft_state_.AppendSetInstanceAsMain(instance_name);
  if (!res->get_accepted()) {
    spdlog::error(
        "Failed to accept request for promoting instance {}. Most likely the reason is that the instance is not "
        "the leader.",
        instance_name);
    return SetInstanceToMainCoordinatorStatus::RAFT_COULD_NOT_ACCEPT;
  }

  spdlog::info("Request for promoting instance {} accepted", instance_name);

  if (res->get_result_code() != nuraft::cmd_result_code::OK) {
    spdlog::error("Failed to promote instance {} with error code {}", instance_name, res->get_result_code());
    return SetInstanceToMainCoordinatorStatus::RAFT_COULD_NOT_APPEND;
  }

  return SetInstanceToMainCoordinatorStatus::SUCCESS;
}

>>>>>>> ecf7e6839 (Set instance to main shared with commit)
auto CoordinatorInstance::OnRaftCommitCallback(TRaftLog const &log_entry, RaftLogAction log_action) -> void {
  // TODO: (andi) Solve it locking scheme and write comment.
  switch (log_action) {
    case RaftLogAction::REGISTER_REPLICATION_INSTANCE: {
      auto config = std::get<CoordinatorClientConfig>(log_entry);
      auto *new_instance = &repl_instances_.emplace_back(this, std::move(config), client_succ_cb_, client_fail_cb_,
                                                         &CoordinatorInstance::ReplicaSuccessCallback,
                                                         &CoordinatorInstance::ReplicaFailCallback);

      if (raft_state_.IsLeader()) {
        if (!new_instance->SendDemoteToReplicaRpc()) {
          throw CoordinatorRegisterInstanceException("Failed to demote instance {} to replica on registration.",
                                                     new_instance->InstanceName());
        }

        new_instance->StartFrequentCheck();
        // TODO: (andi) Pinging for InstanceName() raft_state?
        spdlog::info("Leader instance {} started frequent check on ", raft_state_.InstanceName(),
                     new_instance->InstanceName());
      }

      spdlog::info("Instance {} registered", new_instance->InstanceName());
      break;
    }
    case RaftLogAction::UNREGISTER_REPLICATION_INSTANCE: {
      auto const instance_name = std::get<std::string>(log_entry);

      if (raft_state_.IsLeader()) {
        auto &inst_to_remove = FindReplicationInstance(instance_name);
        inst_to_remove.StopFrequentCheck();

        auto const is_main = [this](ReplicationInstance const &instance) {
          return raft_state_.IsMain(instance.InstanceName());
        };

        if (auto curr_main = std::ranges::find_if(repl_instances_, is_main); curr_main != repl_instances_.end()) {
          if (!curr_main->SendUnregisterReplicaRpc(instance_name)) {
            // TODO: (andi) Restore state in the RAFT log if needed.
            inst_to_remove.StartFrequentCheck();
          }
        }
      }

      auto const name_matches = [&instance_name](ReplicationInstance const &instance) {
        return instance.InstanceName() == instance_name;
      };

      std::erase_if(repl_instances_, name_matches);

      spdlog::info("Instance {} unregistered", instance_name);
      break;
    }
    case RaftLogAction::SET_INSTANCE_AS_MAIN: {
      auto const instance_name = std::get<std::string>(log_entry);
      auto &new_main = FindReplicationInstance(instance_name);

      new_main.PauseFrequentCheck();
      utils::OnScopeExit scope_exit{[&new_main] { new_main.ResumeFrequentCheck(); }};

      auto const new_main_uuid = utils::UUID{};

      if (raft_state_.IsLeader()) {
        auto const is_not_new_main = [&instance_name](ReplicationInstance const &instance) {
          return instance.InstanceName() != instance_name;
        };

        auto alive_instances = repl_instances_ | ranges::views::filter(&ReplicationInstance::IsAlive) |
                               ranges::views::filter(is_not_new_main);

        if (std::ranges::any_of(alive_instances, [&new_main_uuid](ReplicationInstance &instance) {
              return !instance.SendSwapAndUpdateUUID(new_main_uuid);
            })) {
          spdlog::error("Failed to swap uuid for instance {}.", new_main.InstanceName());
          // TODO: (andi) What to do on network failure when appended to raft log?
        }

        auto repl_clients_info = repl_instances_ | ranges::views::filter(is_not_new_main) |
                                 ranges::views::transform(&ReplicationInstance::ReplicationClientInfo) |
                                 ranges::to<ReplicationClientsInfo>();

        if (!new_main.PromoteToMainAsLeader(new_main_uuid, std::move(repl_clients_info),
                                            &CoordinatorInstance::MainSuccessCallback,
                                            &CoordinatorInstance::MainFailCallback)) {
          spdlog::error("Failed to promote instance {} to main", instance_name);
          // TODO: (andi) What to do on network failure when appended to raft log?
        }
      } else {
        new_main.PromoteToMainAsFollower(new_main_uuid, &CoordinatorInstance::MainSuccessCallback,
                                         &CoordinatorInstance::MainFailCallback);
      }

      main_uuid_ = new_main_uuid;

      spdlog::info("Instance {} promoted to main", instance_name);
      break;
    }
    case RaftLogAction::SET_INSTANCE_AS_REPLICA: {
      auto const repl_instance_name = std::get<std::string>(log_entry);
      auto &repl_instance = FindReplicationInstance(repl_instance_name);

      if (raft_state_.IsLeader()) {
        if (repl_instance.DemoteToReplicaAsLeader(&CoordinatorInstance::ReplicaSuccessCallback,
                                                  &CoordinatorInstance::ReplicaFailCallback)) {
          repl_instance.OnSuccessPing();
        } else {
          // TODO: (andi) What to do on failure?
          spdlog::error("Instance {} failed to become replica", repl_instance_name);
        }

        if (!repl_instance.SendSwapAndUpdateUUID(main_uuid_)) {
          spdlog::error(fmt::format("Failed to swap uuid for demoted main instance {}", repl_instance.InstanceName()));
          return;
        }

      } else {
        repl_instance.DemoteToReplicaAsFollower(&CoordinatorInstance::ReplicaSuccessCallback,
                                                &CoordinatorInstance::ReplicaFailCallback);
      }
      spdlog::info("Instance {} demoted to replica", repl_instance_name);
      break;
    }
  };
}

>>>>>>> fab8d3d76 (Shared (Un)Registration networking part with raft)
}  // namespace memgraph::coordination
#endif
