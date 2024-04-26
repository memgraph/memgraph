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

#pragma once

#ifdef MG_ENTERPRISE

#include <atomic>
#include <functional>
#include <memory>
#include <optional>

#include "coordination/coordinator_communication_config.hpp"
#include "coordination/coordinator_server.hpp"
#include "coordination/instance_status.hpp"
#include "coordination/raft_state.hpp"
#include "coordination/register_main_replica_coordinator_status.hpp"
#include "coordination/replication_instance_client.hpp"
#include "coordination/replication_instance_connector.hpp"
#include "utils/resource_lock.hpp"
#include "utils/rw_lock.hpp"
#include "utils/thread_pool.hpp"

#include <list>
#include <range/v3/range/primitives.hpp>

namespace memgraph::coordination {

struct NewMainRes {
  std::string most_up_to_date_instance;
  std::string latest_epoch;
  uint64_t latest_commit_timestamp;
};
using InstanceNameDbHistories = std::pair<std::string, replication_coordination_glue::DatabaseHistories>;

class CoordinatorInstance {
 public:
  explicit CoordinatorInstance(CoordinatorInstanceInitConfig const &config);
  CoordinatorInstance(CoordinatorInstance const &) = delete;
  CoordinatorInstance &operator=(CoordinatorInstance const &) = delete;
  CoordinatorInstance(CoordinatorInstance &&) noexcept = delete;
  CoordinatorInstance &operator=(CoordinatorInstance &&) noexcept = delete;

  ~CoordinatorInstance() = default;

  [[nodiscard]] auto RegisterReplicationInstance(CoordinatorToReplicaConfig const &config)
      -> RegisterInstanceCoordinatorStatus;
  [[nodiscard]] auto UnregisterReplicationInstance(std::string_view instance_name)
      -> UnregisterInstanceCoordinatorStatus;

  [[nodiscard]] auto SetReplicationInstanceToMain(std::string_view instance_name) -> SetInstanceToMainCoordinatorStatus;

  auto ShowInstances() const -> std::vector<InstanceStatus>;

  auto TryFailover() -> void;

  auto AddCoordinatorInstance(CoordinatorToCoordinatorConfig const &config) -> AddCoordinatorInstanceStatus;

  auto GetRoutingTable() const -> RoutingTable;

  static auto ChooseMostUpToDateInstance(std::span<InstanceNameDbHistories> histories) -> NewMainRes;

  auto HasMainState(std::string_view instance_name) const -> bool;

  auto HasReplicaState(std::string_view instance_name) const -> bool;

  auto GetLeaderCoordinatorData() const -> std::optional<CoordinatorToCoordinatorConfig>;

  auto DemoteInstanceToReplica(std::string_view instance_name) -> DemoteInstanceCoordinatorStatus;

  auto TryForceResetClusterState() -> ForceResetClusterStateStatus;

  auto ForceResetClusterState() -> ForceResetClusterStateStatus;

 private:
  template <ranges::forward_range R>
  auto GetMostUpToDateInstanceFromHistories(R &&alive_instances) -> std::optional<std::string> {
    if (ranges::empty(alive_instances)) {
      return std::nullopt;
    }
    auto const get_ts = [](ReplicationInstanceConnector &replica) {
      spdlog::trace("Sending get instance timestamps to {}", replica.InstanceName());
      return replica.GetClient().SendGetInstanceTimestampsRpc();
    };

    auto maybe_instance_db_histories = alive_instances | ranges::views::transform(get_ts) | ranges::to<std::vector>();

    auto const ts_has_error = [](auto const &res) -> bool { return res.HasError(); };

    if (std::ranges::any_of(maybe_instance_db_histories, ts_has_error)) {
      spdlog::error("At least one instance which was alive didn't provide per database history.");
      return std::nullopt;
    }

    auto const ts_has_value = [](auto const &zipped) -> bool {
      auto &[replica, res] = zipped;
      return res.HasValue();
    };

    auto transform_to_pairs = ranges::views::transform([](auto const &zipped) {
      auto &[replica, res] = zipped;
      return std::make_pair(replica.InstanceName(), res.GetValue());
    });

    auto instance_db_histories = ranges::views::zip(alive_instances, maybe_instance_db_histories) |
                                 ranges::views::filter(ts_has_value) | transform_to_pairs | ranges::to<std::vector>();

    auto [most_up_to_date_instance, latest_epoch, latest_commit_timestamp] =
        ChooseMostUpToDateInstance(instance_db_histories);

    spdlog::trace("The most up to date instance is {} with epoch {} and {} latest commit timestamp",
                  most_up_to_date_instance, latest_epoch, latest_commit_timestamp);  // NOLINT

    return most_up_to_date_instance;
  }

  auto FindReplicationInstance(std::string_view replication_instance_name) -> ReplicationInstanceConnector &;

  void MainFailCallback(std::string_view);

  void MainSuccessCallback(std::string_view);

  void ReplicaSuccessCallback(std::string_view);

  void ReplicaFailCallback(std::string_view);

  void DemoteSuccessCallback(std::string_view repl_instance_name);

  void DemoteFailCallback(std::string_view repl_instance_name);

  template <bool IsUserAction>
  auto ForceResetCluster() -> ForceResetClusterStateStatus {
    // Force reset tries to return cluster to state in which we have all the replicas we had before
    // and try to do failover to new MAIN. Only then is force reset successful

    // 0. Open lock
    // 1. Try to demote each instance to replica
    // 2. Instances which are demoted proceed in next step as part of selection process
    // 3. For selected instances try to send SWAP UUID and update log -> both must succeed
    // 4. Do failover
    // 5. For instances which were down set correct callback as before
    // 6. After instance gets back up, do steps needed to recover

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

    if (!raft_state_->IsLeader()) {
      spdlog::trace("Exiting force reset as coordinator is not any more leader!");
      return ForceResetClusterStateStatus::NOT_LEADER;
    }

    if (!raft_state_->AppendOpenLock()) {
      spdlog::trace("Appending log force reset failed, aborting force reset");
      // Here we want to continue doing force reset or kill coordinator. If we can't append open lock,
      // then we must be follower, and some other coordinator will continue trying force reset. Otherwise
      // we are leader  but append lock failed, so we are in wrong state.
      MG_ASSERT(!raft_state_->IsLeader(),
                "Coordinator is leader but append open lock failed, encountered wrong state.");
      return ForceResetClusterStateStatus::FAILED_TO_OPEN_LOCK;
    }

    utils::OnScopeExit const maybe_do_another_reset{[this]() {
      spdlog::trace("Lock opened {}, coordinator leader {}", raft_state_->IsLeader(), raft_state_->IsLockOpened());
      if (raft_state_->IsLeader() && !raft_state_->IsLockOpened()) {
        is_leader_ready_ = true;
        spdlog::trace("Lock is not opened anymore and coordinator is leader, not doing force reset again.");
      }
      if (!IsUserAction) {
        if (raft_state_->IsLockOpened() && raft_state_->IsLeader()) {
          spdlog::trace("Adding task to try force reset cluster again as lock is opened still.");
          thread_pool_.AddTask([this]() { this->ForceResetClusterState(); });
        }
      }
    }};

    auto const instances = raft_state_->GetReplicationInstances();

    // To each instance we send RPC
    // If RPC fails we consider instance dead
    // Otherwise we consider instance alive
    // If at any point later RPC fails for alive instance, we consider this failure

    std::ranges::for_each(instances, [this](auto &replica) {
      auto client = std::make_unique<ReplicationInstanceClient>(this, replica.config, client_succ_cb_, client_fail_cb_);

      repl_instances_.emplace_back(std::move(client), &CoordinatorInstance::ReplicaSuccessCallback,
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
      return !raft_state_->AppendSetInstanceAsReplicaLog(instance.InstanceName());
    };
    if (std::ranges::any_of(alive_instances, demote_to_replica_failed)) {
      spdlog::error("Failed to send raft log or rpc that instance is demoted to replica.");
      return ForceResetClusterStateStatus::RPC_FAILED;
    }

    auto const new_uuid = utils::UUID{};

    auto update_uuid_failed = [&new_uuid, this](auto &repl_instance) {
      if (!repl_instance.SendSwapAndUpdateUUID(new_uuid)) {
        return true;
      }
      return !raft_state_->AppendUpdateUUIDForInstanceLog(repl_instance.InstanceName(), new_uuid);
    };
    if (std::ranges::any_of(alive_instances, update_uuid_failed)) {
      spdlog::error("Force reset failed since update log swap uuid failed, assuming coordinator is now follower.");
      return ForceResetClusterStateStatus::ACTION_FAILED;
    }

    if (!raft_state_->AppendUpdateUUIDForNewMainLog(new_uuid)) {
      spdlog::error("Update log for new MAIN failed, assuming coordinator is now follower");
      return ForceResetClusterStateStatus::RAFT_LOG_ERROR;
    }

    auto maybe_most_up_to_date_instance = GetMostUpToDateInstanceFromHistories(alive_instances);
    spdlog::trace(" Maybe most up to date instance {}", maybe_most_up_to_date_instance.value_or("a"));
    if (!maybe_most_up_to_date_instance.has_value()) {
      spdlog::error("Couldn't choose instance for failover, check logs for more details.");
      return ForceResetClusterStateStatus::NO_NEW_MAIN;
    }

    auto &new_main = FindReplicationInstance(*maybe_most_up_to_date_instance);
    spdlog::trace("Found new main");
    auto const is_not_new_main = [&new_main](ReplicationInstanceConnector const &repl_instance) {
      return repl_instance.InstanceName() != new_main.InstanceName();
    };
    auto repl_clients_info = repl_instances_ | ranges::views::filter(is_not_new_main) |
                             ranges::views::transform(&ReplicationInstanceConnector::GetReplicationClientInfo) |
                             ranges::to<ReplicationClientsInfo>();

    if (!new_main.PromoteToMain(new_uuid, std::move(repl_clients_info), &CoordinatorInstance::MainSuccessCallback,
                                &CoordinatorInstance::MainFailCallback)) {
      spdlog::warn("Force reset failed since promoting replica to main failed.");
      return ForceResetClusterStateStatus::RPC_FAILED;
    }

    // This will set cluster in healthy state again
    if (!raft_state_->AppendSetInstanceAsMainLog(*maybe_most_up_to_date_instance, new_uuid)) {
      spdlog::error("Update log for new MAIN failed");
      return ForceResetClusterStateStatus::RAFT_LOG_ERROR;
    }

    // We need to clear repl instances in the beginning as we don't know where exactly action failed and
    // we need to recreate state from raft log
    // If instance in raft log is MAIN, it can be REPLICA but raft append failed when we demoted it
    // If instance in raft log is REPLICA, it can be MAIN but raft log failed when we promoted it
    // CRUX of problem: We need demote callbacks which will demote instance to replica and only then change to
    // REPLICA callbacks

    auto needs_demote_setup_failed = [&instances_mapped_to_resp, this](ReplicationInstanceConnector &repl_instance) {
      if (instances_mapped_to_resp[repl_instance.InstanceName()]) {
        return false;
      }
      if (!raft_state_->AppendInstanceNeedsDemote(repl_instance.InstanceName())) {
        return true;
      }
      repl_instance.SetCallbacks(&CoordinatorInstance::DemoteSuccessCallback, &CoordinatorInstance::DemoteFailCallback);
      return false;
    };

    if (std::ranges::any_of(repl_instances_, needs_demote_setup_failed)) {
      spdlog::error("Raft log didn't accept that some instances are in unknown state.");
      return ForceResetClusterStateStatus::ACTION_FAILED;
    }

    auto check_correct_callbacks_set = [this](auto &repl_instance) {
      if (raft_state_->HasReplicaState(repl_instance.InstanceName())) {
        MG_ASSERT(repl_instance.GetSuccessCallback() == &CoordinatorInstance::ReplicaSuccessCallback &&
                      repl_instance.GetFailCallback() == &CoordinatorInstance::ReplicaFailCallback,
                  "Callbacks are wrong");
      } else {
        MG_ASSERT(repl_instance.GetSuccessCallback() == &CoordinatorInstance::MainSuccessCallback &&
                  repl_instance.GetFailCallback() == &CoordinatorInstance::MainFailCallback);
      }
    };
    std::ranges::for_each(alive_instances, check_correct_callbacks_set);

    if (!raft_state_->AppendCloseLock()) {
      spdlog::error("Aborting force reset as we failed to close lock on action.");
      return ForceResetClusterStateStatus::FAILED_TO_CLOSE_LOCK;
    }

    std::ranges::for_each(repl_instances_, [](auto &instance) { instance.StartFrequentCheck(); });

    if (!IsUserAction) {
      MG_ASSERT(!raft_state_->IsLockOpened(), "After force reset we need to be in healthy state.");
    }

    return ForceResetClusterStateStatus::SUCCESS;
  }

  auto GetBecomeLeaderCallback() -> std::function<void()>;
  auto GetBecomeFollowerCallback() -> std::function<void()>;

  HealthCheckClientCallback client_succ_cb_, client_fail_cb_;
  std::atomic<bool> is_leader_ready_{false};
  // NOTE: Must be std::list because we rely on pointer stability.
  // TODO(antoniofilipovic) do we still rely on pointer stability
  std::list<ReplicationInstanceConnector> repl_instances_;
  mutable utils::ResourceLock coord_instance_lock_{};

  // Thread pool needs to be constructed before raft state as raft state can call thread pool
  utils::ThreadPool thread_pool_{1};

  // This needs to be constructed last because raft state may call
  // setting up leader instance
  std::unique_ptr<RaftState> raft_state_;
};

}  // namespace memgraph::coordination
#endif
