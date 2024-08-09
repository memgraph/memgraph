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

#include <algorithm>
#include <atomic>
#include <chrono>
#include <functional>
#include <iterator>
#include <memory>
#include <mutex>
#include <optional>
#include <shared_mutex>
#include <string>
#include <string_view>
#include <thread>
#include <utility>
#include <vector>

#include "coordination/coordination_observer.hpp"
#include "coordination/coordinator_communication_config.hpp"
#include "coordination/coordinator_exceptions.hpp"
#include "coordination/coordinator_instance.hpp"
#include "coordination/coordinator_instance_management_server.hpp"
#include "coordination/coordinator_instance_management_server_handlers.hpp"
#include "coordination/coordinator_rpc.hpp"
#include "coordination/instance_status.hpp"
#include "coordination/raft_state.hpp"
#include "coordination/register_main_replica_coordinator_status.hpp"
#include "coordination/replication_instance_client.hpp"
#include "coordination/replication_instance_connector.hpp"
#include "dbms/constants.hpp"
#include "nuraft/coordinator_cluster_state.hpp"
#include "replication_coordination_glue/role.hpp"
#include "utils/exponential_backoff.hpp"
#include "utils/functional.hpp"
#include "utils/logging.hpp"
#include "utils/on_scope_exit.hpp"

#include <spdlog/spdlog.h>
#include <range/v3/range/conversion.hpp>
#include <range/v3/view/filter.hpp>
#include <range/v3/view/transform.hpp>

namespace memgraph::coordination {

namespace {
constexpr int kDisconnectedCluster = 1;
}  // namespace

using nuraft::ptr;

CoordinatorInstance::CoordinatorInstance(CoordinatorInstanceInitConfig const &config)
    : coordinator_management_server_{ManagementServerConfig{
          io::network::Endpoint{kDefaultManagementServerIp, static_cast<uint16_t>(config.management_port)}}} {
  client_succ_cb_ = [](CoordinatorInstance *self, std::string_view repl_instance_name) -> void {
    spdlog::trace("Acquiring lock in thread {} for client success callback for instance {}", std::this_thread::get_id(),
                  repl_instance_name);
    auto lock = std::unique_lock{self->coord_instance_lock_};
    auto &repl_instance = self->FindReplicationInstance(repl_instance_name);
    spdlog::trace("Executing success callback in thread {} for instance {}", std::this_thread::get_id(),
                  repl_instance_name);
    std::invoke(repl_instance.GetSuccessCallback(), self, repl_instance_name);
  };

  client_fail_cb_ = [](CoordinatorInstance *self, std::string_view repl_instance_name) -> void {
    spdlog::trace("Acquiring lock in thread {} for client failure callback for instance {}", std::this_thread::get_id(),
                  repl_instance_name);
    auto lock = std::unique_lock{self->coord_instance_lock_};

    auto &repl_instance = self->FindReplicationInstance(repl_instance_name);
    spdlog::trace("Executing failure callback in thread {} for instance {}", std::this_thread::get_id(),
                  repl_instance_name);
    std::invoke(repl_instance.GetFailCallback(), self, repl_instance_name);
  };

  CoordinatorInstanceManagementServerHandlers::Register(coordinator_management_server_, *this);
  MG_ASSERT(coordinator_management_server_.Start());

  // Delay constructing of Raft state until everything is constructed in coordinator instance
  // since raft state will call become leader callback or become follower callback on construction.
  // If something is not yet constructed in coordinator instance, we get UB
  raft_state_ = std::make_unique<RaftState>(config, GetBecomeLeaderCallback(), GetBecomeFollowerCallback(),
                                            CoordinationClusterChangeObserver{this});
  AddOrUpdateClientConnectors(raft_state_->GetCoordinatorToCoordinatorConfigs());
  raft_state_->InitRaftServer();
}

CoordinatorInstance::~CoordinatorInstance() {
  // Coordinator instance can be stuck in infinite loop of doing reconciliation of cluster state. We need to notify
  // thread that we are shutting down, and we need to stop frequent checks and thread pool before anything is destructed
  // otherwise thread pool can get task become leader, when raft_state_ ptr is already destructed
  // Order is important:
  // 1. Thread pool might be running with reconcile cluster state, or become follower, so we should shut that down
  // 2. Await shutdown of coordinator thread pool so that coordinator can't become follower or do reconcile cluster
  // state
  // 3. Frequent checks running, we need to stop them before raft_state_ is destroyed
  ShuttingDown();
  thread_pool_.ShutDown();
  // We don't need to take lock as reconcile cluster state can't be running, coordinator can't become follower,
  // user queries can't be running as memgraph awaits server shutdown
  std::ranges::for_each(repl_instances_, [](auto &repl_instance) { repl_instance.StopFrequentCheck(); });
}

auto CoordinatorInstance::GetBecomeLeaderCallback() -> std::function<void()> {
  return [this]() {
    spdlog::trace("Executing become leader callback in thread {}", std::this_thread::get_id());
    if (is_shutting_down_.load(std::memory_order_seq_cst)) {
      return;
    }
    is_leader_ready_.store(false, std::memory_order_seq_cst);
    // Thread pool is needed because becoming leader is blocking action, and if we don't succeed to check state of
    // cluster we will try again and again in same thread, thus blocking progress of nuRAFT leader election
    thread_pool_.AddTask([this]() { this->ReconcileClusterState(); });
    return;
  };
}

auto CoordinatorInstance::GetBecomeFollowerCallback() -> std::function<void()> {
  return [this]() {
    thread_pool_.AddTask([this]() {
      spdlog::info("Leader changed, trying to stop all replication instances frequent checks in thread {}!",
                   std::this_thread::get_id());
      is_leader_ready_ = false;
      // We need to stop checks before taking a lock because deadlock can happen if instances waits
      // to take a lock in frequent check, and this thread already has a lock and waits for instance to
      // be done with frequent check
      std::ranges::for_each(repl_instances_, [](auto &repl_instance) {
        spdlog::trace("Stopping frequent checks for instance {}", repl_instance.InstanceName());
        repl_instance.StopFrequentCheck();
        spdlog::trace("Stopped frequent checks for instance {}", repl_instance.InstanceName());
      });
      spdlog::info("Acquiring lock in become follower callback in thread {}!", std::this_thread::get_id());
      auto lock = std::unique_lock{coord_instance_lock_};
      repl_instances_.clear();
      spdlog::info("Stopped all replication instance frequent checks.");
    });
  };
}

auto CoordinatorInstance::FindReplicationInstance(std::string_view replication_instance_name)
    -> ReplicationInstanceConnector & {
  auto repl_instance =
      std::ranges::find_if(repl_instances_, [replication_instance_name](ReplicationInstanceConnector const &instance) {
        return instance.InstanceName() == replication_instance_name;
      });

  MG_ASSERT(repl_instance != repl_instances_.end(), "Instance {} not found during callback!",
            replication_instance_name);
  return *repl_instance;
}

auto CoordinatorInstance::GetLeaderCoordinatorData() const -> std::optional<CoordinatorToCoordinatorConfig> {
  return raft_state_->GetLeaderCoordinatorData();
}

void CoordinatorInstance::AddOrUpdateClientConnectors(std::vector<CoordinatorToCoordinatorConfig> const &configs) {
  auto connectors = coordinator_connectors_.Lock();

  for (auto const &config : configs) {
    if (config.coordinator_id == raft_state_->GetCoordinatorId()) {
      continue;
    }
    auto const connector = std::ranges::find_if(
        *connectors, [&config](auto &&connector) { return connector.first == config.coordinator_id; });
    if (connector != connectors->end()) {
      continue;
    }
    spdlog::trace("Creating new connector to coordinator with id {}, on endpoint:{}.", config.coordinator_id,
                  config.management_server.SocketAddress());
    connectors->emplace(connectors->end(), config.coordinator_id, ManagementServerConfig{config.management_server});
  }
}

auto CoordinatorInstance::GetCoordinatorsInstanceStatus() const -> std::vector<InstanceStatus> {
  auto const stringify_coord_health = [this](CoordinatorToCoordinatorConfig const &instance) -> std::string {
    if (!is_leader_ready_) {
      return "unknown";
    }

    auto const last_succ_resp_ms = raft_state_->CoordLastSuccRespMs(instance.coordinator_id);
    return last_succ_resp_ms < instance.instance_down_timeout_sec ? "up" : "down";
  };

  auto const get_coord_role = [](auto const coordinator_id, auto const curr_leader) -> std::string {
    return coordinator_id == curr_leader ? "leader" : "follower";
  };

  auto const coord_instance_to_status = [this, &stringify_coord_health, &get_coord_role](
                                            CoordinatorToCoordinatorConfig const &instance) -> InstanceStatus {
    auto const curr_leader = raft_state_->GetLeaderId();
    return {
        .instance_name = fmt::format("coordinator_{}", instance.coordinator_id),
        .coordinator_server = instance.coordinator_server.SocketAddress(),  // show non-resolved IP
        .management_server = instance.management_server.SocketAddress(),    // show non-resolved IP
        .bolt_server = instance.bolt_server.SocketAddress(),                // show non-resolved IP
        .cluster_role = get_coord_role(instance.coordinator_id, curr_leader),
        .health = stringify_coord_health(instance),
        .last_succ_resp_ms = raft_state_->CoordLastSuccRespMs(instance.coordinator_id).count(),

    };
  };

  return utils::fmap(raft_state_->GetCoordinatorInstances(), coord_instance_to_status);
}

auto CoordinatorInstance::ShowInstancesStatusAsFollower() const -> std::vector<InstanceStatus> {
  spdlog::trace("Processing show instances as follower");
  auto instances_status = GetCoordinatorsInstanceStatus();
  auto const stringify_inst_status = [raft_state_ptr =
                                          raft_state_.get()](ReplicationInstanceState const &instance) -> std::string {
    // Instance is down and it was not yet demoted
    if (raft_state_ptr->IsCurrentMain(instance.config.instance_name)) {
      return "main";
    }
    if (raft_state_ptr->HasMainState(instance.config.instance_name)) {
      return "unknown";
    }
    return "replica";
  };

  auto process_repl_instance_as_follower =
      [&stringify_inst_status](ReplicationInstanceState const &instance) -> InstanceStatus {
    return {.instance_name = instance.config.instance_name,
            .management_server = instance.config.ManagementSocketAddress(),  // show non-resolved IP
            .bolt_server = instance.config.BoltSocketAddress(),              // show non-resolved IP
            .cluster_role = stringify_inst_status(instance),
            .health = "unknown"};
  };

  std::ranges::transform(raft_state_->GetReplicationInstances(), std::back_inserter(instances_status),
                         process_repl_instance_as_follower);
  return instances_status;
}

auto CoordinatorInstance::ShowInstancesAsLeader() const -> std::vector<InstanceStatus> {
  spdlog::trace("Processing show instances as leader");
  auto instances_status = GetCoordinatorsInstanceStatus();

  auto const stringify_repl_role = [this](ReplicationInstanceConnector const &instance) -> std::string {
    if (!instance.IsAlive()) return "unknown";
    if (raft_state_->IsCurrentMain(instance.InstanceName())) return "main";
    return "replica";
  };

  auto const stringify_repl_health = [](ReplicationInstanceConnector const &instance) -> std::string {
    return instance.IsAlive() ? "up" : "down";
  };

  auto process_repl_instance_as_leader =
      [&stringify_repl_role, &stringify_repl_health](ReplicationInstanceConnector const &instance) -> InstanceStatus {
    return {.instance_name = instance.InstanceName(),
            .management_server = instance.ManagementSocketAddress(),  // show non-resolved IP
            .bolt_server = instance.BoltSocketAddress(),              // show non-resolved IP
            .cluster_role = stringify_repl_role(instance),
            .health = stringify_repl_health(instance),
            .last_succ_resp_ms = instance.LastSuccRespMs().count()};
  };

  {
    auto lock = std::shared_lock{coord_instance_lock_};
    std::ranges::transform(repl_instances_, std::back_inserter(instances_status), process_repl_instance_as_leader);
  }

  return instances_status;
}

auto CoordinatorInstance::ShowInstances() const -> std::vector<InstanceStatus> {
  if (is_leader_ready_) {
    return ShowInstancesAsLeader();
  }

  auto const leader_id = raft_state_->GetLeaderId();
  if (leader_id == raft_state_->GetCoordinatorId()) {
    spdlog::trace("Coordinator itself not yet leader, returning report as follower.");
    return ShowInstancesStatusAsFollower();  // We don't want to ask ourselves for instances, as coordinator is
                                             // not ready still as leader
  }

  if (leader_id == -1) {
    spdlog::trace("No leader found, returning report as follower");
    return ShowInstancesStatusAsFollower();
  }
  CoordinatorInstanceConnector *leader{nullptr};
  {
    auto connectors = coordinator_connectors_.Lock();

    auto connector =
        std::ranges::find_if(*connectors, [&leader_id](auto &&connector) { return connector.first == leader_id; });
    if (connector != connectors->end()) {
      leader = &connector->second;
    }
  }
  if (leader == nullptr) {
    spdlog::trace("Connection to leader not found, returning SHOW INSTANCES output as follower.");
    return ShowInstancesStatusAsFollower();
  }

  spdlog::trace("Sending show instances RPC to leader with id {}", leader_id);
  auto maybe_res = leader->SendShowInstances();

  if (!maybe_res.has_value()) {
    spdlog::trace("Couldn't get instances from leader {}. Returning result as a follower.", leader_id);
    return ShowInstancesStatusAsFollower();
  }
  spdlog::trace("Got instances from leader {}.", leader_id);
  return std::move(maybe_res.value());
}

auto CoordinatorInstance::ReconcileClusterState() -> ReconcileClusterStateStatus {
  utils::ExponentialBackoff backoff{std::chrono::milliseconds(1000), std::chrono::seconds(60)};

  while (raft_state_->IsLeader()) {
    spdlog::trace("Ensuring healthy state in cluster still as coordinator is leader and lock is opened.");
    auto const result = ReconcileClusterState_();
    switch (result) {
      case (ReconcileClusterStateStatus::SUCCESS):
        is_leader_ready_.store(true, std::memory_order_seq_cst);
        spdlog::trace("Reconcile cluster state was success.");
        return result;
      case ReconcileClusterStateStatus::FAIL:
        break;
      case ReconcileClusterStateStatus::SHUTTING_DOWN:
        spdlog::trace("Stopping reconciliation as coordinator is shutting down.");
        return result;
    }
    // TODO add stop token
    backoff.wait();
  }

  return ReconcileClusterStateStatus::SUCCESS;  // Shouldn't execute
}

auto CoordinatorInstance::ReconcileClusterState_() -> ReconcileClusterStateStatus {
  // Only function which can run without opening lock as long as we have is_leader_ready_ set on false
  // and all other functions checking whether leader is ready to start executing functions

  // 0. Close lock
  // 1. Get all instances from raft state, check if we have one main and others are replicas
  // a) Set callbacks
  // b) Start frequent checks
  // c) Return true

  // 2. Try find instance and do failover
  // 3. For selected instances try to send SWAP UUID and update log -> both must succeed
  // 4. Do failover
  // 5. For instances which are down set demote instance to main callback

  spdlog::trace("Doing ReconcileClusterState.");

  if (is_shutting_down_.load()) {
    return ReconcileClusterStateStatus::SHUTTING_DOWN;
  }

  // Do we need to open lock?
  //  We don't need to open lock as long as we have check whether leader is ready on any action.
  //  Also, function which uses this function can call in loop this function either until we succeed or we are leader.
  //  If we have only three states (SUCCESS, FAIL, SHUTTING_DOWN) we can figure out whether we should continue executing
  //  or no.

  // Ordering is important here, we must stop frequent check before
  // taking lock to avoid deadlock between us stopping thread and thread wanting to take lock but can't because
  // we have it
  std::ranges::for_each(repl_instances_, [](auto &repl_instance) {
    spdlog::trace("Stopping frequent check for instance {}", repl_instance.InstanceName());
    repl_instance.StopFrequentCheck();
    spdlog::trace("Stopped frequent check for instance {}", repl_instance.InstanceName());
  });
  spdlog::trace("Stopped all frequent checks and acquiring lock in the thread {}", std::this_thread::get_id());
  auto lock = std::unique_lock{coord_instance_lock_};
  spdlog::trace("Acquired lock in the reconciliation cluster state reset thread {}", std::this_thread::get_id());
  repl_instances_.clear();

  utils::OnScopeExit const do_reconcile_cluster_state{[this]() {
    spdlog::trace("Reconcile cluster state function exit, lock opened {}, coordinator leader {}",
                  raft_state_->IsLockOpened(), raft_state_->IsLeader());
    if (raft_state_->IsLeader() && !raft_state_->IsLockOpened()) {
      is_leader_ready_ = true;
      spdlog::trace("Lock is not opened anymore and coordinator is leader, not reconciling cluster state again.");
    }
  }};

  auto const raft_state_replication_instances = raft_state_->GetReplicationInstances();

  // User can execute action on coordinator while it is connected to the cluster
  // or it is not connected yet.
  // If cluster is connected we need to append close lock and fix the issues
  // If cluster is non connected (only one coordinator)
  // 1. error can happen only if user tried something -> close lock and live with possibility that cluster may diverge
  // 2. if there is no lock opened, return

  if (raft_state_->GetCoordinatorInstances().size() == kDisconnectedCluster) {
    if (raft_state_->IsLockOpened()) {
      spdlog::error(
          "Don't execute actions while coordinator is not connected to other coordinators, it could lead to "
          "diverging of cluster log.");
    } else {
      return ReconcileClusterStateStatus::SUCCESS;
    }
  }
  if (!raft_state_->AppendCloseLock()) {
    spdlog::trace("Exiting ReconcileClusterState. Failed to append close lock.");
    return ReconcileClusterStateStatus::FAIL;
  }

  // There is nothing to do if we don't have any instances
  if (raft_state_replication_instances.empty()) {
    spdlog::trace("Exiting ReconcileClusterState. Didn't get any instances.");
    return ReconcileClusterStateStatus::SUCCESS;
  }

  //  We don't need to open lock here as we have mechanism to know whether action was success (state which we return).
  //  If action failed we try again, otherwise we are done and we set leader is ready.

  // To each instance we send RPC
  // If RPC fails we consider instance dead
  // Otherwise we consider instance alive
  // If at any point later RPC fails for alive instance, we consider this failure

  std::ranges::for_each(raft_state_replication_instances, [this](auto &&replica) {
    auto client = std::make_unique<ReplicationInstanceClient>(this, replica.config, client_succ_cb_, client_fail_cb_);

    repl_instances_.emplace_back(std::move(client));
  });

  auto instances_mapped_to_resp = repl_instances_ | ranges::views::transform([](auto &&instance) {
                                    return std::pair{instance.InstanceName(), instance.SendFrequentHeartbeat()};
                                  }) |
                                  ranges::to<std::unordered_map<std::string, bool>>();

  auto const maybe_main_instance =
      std::ranges::find_if(repl_instances_, [this, &instances_mapped_to_resp](auto &&instance) {
        return instances_mapped_to_resp[instance.InstanceName()] && raft_state_->IsCurrentMain(instance.InstanceName());
      });

  auto const have_alive_main_instance = maybe_main_instance != repl_instances_.end();
  if (have_alive_main_instance) {
    spdlog::trace("Alive MAIN instance found {}.", maybe_main_instance->InstanceName());
    // if we have alive MAIN instance alive we expect cluster was in correct state already, we can start frequent checks
    // and set callbacks

    auto alive_instances = repl_instances_ | ranges::views::filter([&instances_mapped_to_resp](auto &&instance) {
                             return instances_mapped_to_resp[instance.InstanceName()];
                           });

    auto alive_replica_instances = alive_instances | ranges::views::filter([this](auto &&instance) {
                                     return raft_state_->HasReplicaState(instance.InstanceName());
                                   });

    auto alive_non_replica_instances = alive_instances | ranges::views::filter([this](auto const &instance) {
                                         return !raft_state_->HasReplicaState(instance.InstanceName());
                                       });

    // our set of instances currently consists of
    // 1. Alive MAIN
    // 2. Alive REPLICAS
    // 3. Alive non-replicas (instances which need to be demoted)
    // 4. Dead REPLICA(s)
    // 5. Dead MAIN(s)

    // For instances 3, 4 and 5 we need to set demote callbacks

    std::ranges::for_each(alive_replica_instances, [](auto &&instance) {
      instance.SetCallbacks(&CoordinatorInstance::ReplicaSuccessCallback, &CoordinatorInstance::ReplicaFailCallback);
    });

    std::ranges::for_each(alive_non_replica_instances, [](auto &&instance) {
      instance.SetCallbacks(&CoordinatorInstance::DemoteSuccessCallback, &CoordinatorInstance::DemoteFailCallback);
    });

    auto dead_instances = repl_instances_ | ranges::views::filter([&instances_mapped_to_resp](auto &&instance) {
                            return !instances_mapped_to_resp[instance.InstanceName()];
                          });

    std::ranges::for_each(dead_instances, [](auto &&instance) {
      instance.SetCallbacks(&CoordinatorInstance::DemoteSuccessCallback, &CoordinatorInstance::DemoteFailCallback);
    });

    maybe_main_instance->SetCallbacks(&CoordinatorInstance::MainSuccessCallback,
                                      &CoordinatorInstance::MainFailCallback);

    std::ranges::for_each(repl_instances_, [](auto &&instance) {
      MG_ASSERT(instance.GetSuccessCallback() != nullptr && instance.GetFailCallback() != nullptr,
                "Callbacks are not properly set");
      instance.StartFrequentCheck();
    });
    spdlog::trace("Exiting ReconcileClusterState. Cluster is in healthy state.");
    return ReconcileClusterStateStatus::SUCCESS;
  }

  // Currently MAIN is considered dead
  // We want to demote all alive instances to replica immediately so MAIN instances can stop sending requests to
  // replicas

  // Actions to do
  // 1. Send to each alive instance which is REPLICA that we SWAP UUID of new MAIN
  // 2. Set new UUID for new MAIN
  // 3. Promote new MAIN

  // Cluster currently consists of:
  // 1. Dead or network partitioned MAIN
  // 2. Alive replica instances
  // 3. Alive non-replica instances (old mains)
  // 4. Dead MAINs
  // 5. Dead REPLICAs

  // Note: If we first send SET UUID of NEW MAIN and coordinator fails at that point, even if main gets back up when new
  // coordinator becomes LEADER, we will lose current MAIN

  auto alive_instances = repl_instances_ | ranges::views::filter([&instances_mapped_to_resp](auto &&instance) {
                           return instances_mapped_to_resp[instance.InstanceName()];
                         });
  auto demote_to_replica_failed = [this](auto &&instance) {
    if (!instance.DemoteToReplica(&CoordinatorInstance::ReplicaSuccessCallback,
                                  &CoordinatorInstance::ReplicaFailCallback)) {
      return true;
    }
    return !raft_state_->AppendSetInstanceAsReplicaLog(instance.InstanceName());
  };
  if (std::ranges::any_of(alive_instances, demote_to_replica_failed)) {
    spdlog::error("Exiting ReconcileClusterState. Failed to send raft log or rpc that instance is demoted to replica.");
    return ReconcileClusterStateStatus::FAIL;
  }

  auto const new_uuid = utils::UUID{};

  // We want to change cluster ID of current MAIN so no old instance will return to MAIN state

  if (!raft_state_->AppendUpdateUUIDForNewMainLog(new_uuid)) {
    spdlog::error("Update log for new MAIN failed, assuming coordinator is now follower");
    return ReconcileClusterStateStatus::FAIL;
  }

  auto update_uuid_failed = [&new_uuid, this](auto &&repl_instance) {
    spdlog::trace("Executing swap and update uuid {}", repl_instance.InstanceName());
    if (!repl_instance.SendSwapAndUpdateUUID(new_uuid)) {
      return true;
    }
    spdlog::trace("Executing append update uuid for instance {}", repl_instance.InstanceName());
    return !raft_state_->AppendUpdateUUIDForInstanceLog(repl_instance.InstanceName(), new_uuid);
  };

  // For replicas we need to update uuid
  if (std::ranges::any_of(alive_instances, update_uuid_failed)) {
    spdlog::error("Exiting ReconcileClusterState. Failed to append log swap uuid for alive replicas.");
    return ReconcileClusterStateStatus::FAIL;
  }

  spdlog::trace("Updated uuid for new main {}", std::string{new_uuid});

  auto maybe_most_up_to_date_instance = GetMostUpToDateInstanceFromHistories(alive_instances);
  if (!maybe_most_up_to_date_instance.has_value()) {
    spdlog::error("Exiting ReconcileClusterState. Couldn't choose instance for failover, check logs for more details.");
    return ReconcileClusterStateStatus::FAIL;
  }

  auto &new_main = FindReplicationInstance(*maybe_most_up_to_date_instance);
  spdlog::trace("Found the new main with instance name {}", new_main.InstanceName());
  auto const is_not_new_main = [&new_main](auto &&repl_instance) {
    return repl_instance.InstanceName() != new_main.InstanceName();
  };
  auto repl_clients_info = repl_instances_ | ranges::views::filter(is_not_new_main) |
                           ranges::views::transform(&ReplicationInstanceConnector::GetReplicationClientInfo) |
                           ranges::to<ReplicationClientsInfo>();

  if (!new_main.PromoteToMain(new_uuid, std::move(repl_clients_info), &CoordinatorInstance::MainSuccessCallback,
                              &CoordinatorInstance::MainFailCallback)) {
    spdlog::warn(
        "Exiting ReconcileClusterState. Reconcile cluster state failed since promoting replica to main failed.");
    return ReconcileClusterStateStatus::FAIL;
  }
  spdlog::trace("Promoted instance {} to main", new_main.InstanceName());

  // This will set cluster in healthy state again
  if (!raft_state_->AppendSetInstanceAsMainLog(*maybe_most_up_to_date_instance, new_uuid)) {
    spdlog::error("Exiting ReconcileClusterState. Update log for new MAIN failed");
    return ReconcileClusterStateStatus::FAIL;
  }

  auto check_correct_callbacks_set = [this](auto &&repl_instance) {
    if (raft_state_->HasReplicaState(repl_instance.InstanceName())) {
      MG_ASSERT(repl_instance.GetSuccessCallback() == &CoordinatorInstance::ReplicaSuccessCallback &&
                    repl_instance.GetFailCallback() == &CoordinatorInstance::ReplicaFailCallback,
                "Instance {} should have replica callbacks", repl_instance.InstanceName());
    } else if (raft_state_->IsCurrentMain(repl_instance.InstanceName())) {
      MG_ASSERT(repl_instance.GetSuccessCallback() == &CoordinatorInstance::MainSuccessCallback &&
                    repl_instance.GetFailCallback() == &CoordinatorInstance::MainFailCallback,
                "Instance {} should have main callbacks", repl_instance.InstanceName());
    } else {
      MG_ASSERT(
          false,
          "Instance {} should have replica or main callback but has success callback type: {}, fail callback type: {}",
          repl_instance.InstanceName(), GetSuccessCallbackTypeName(repl_instance),
          GetFailCallbackTypeName(repl_instance));
    }
  };

  // alive instances can either be ready to be demoted, promoted or replicas
  std::ranges::for_each(alive_instances, check_correct_callbacks_set);

  auto dead_instances = repl_instances_ | ranges::views::filter([&instances_mapped_to_resp](auto &&instance) {
                          return !instances_mapped_to_resp[instance.InstanceName()];
                        });

  std::ranges::for_each(dead_instances, [](auto &instance) {
    instance.SetCallbacks(&CoordinatorInstance::DemoteSuccessCallback, &CoordinatorInstance::DemoteFailCallback);
  });

  std::ranges::for_each(repl_instances_, [](auto &instance) {
    MG_ASSERT(instance.GetSuccessCallback() != nullptr && instance.GetFailCallback() != nullptr,
              "Callbacks are not properly set");
    instance.StartFrequentCheck();
  });
  spdlog::trace("Exiting ReconcileClusterState. Cluster is in healthy state after correct action.");
  return ReconcileClusterStateStatus::SUCCESS;
}

auto CoordinatorInstance::GetSuccessCallbackTypeName(ReplicationInstanceConnector const &instance) -> std::string_view {
  if (instance.GetSuccessCallback() == &CoordinatorInstance::ReplicaSuccessCallback) {
    return "ReplicaSuccessCallback";
  }
  if (instance.GetSuccessCallback() == &CoordinatorInstance::MainSuccessCallback) {
    return "MainSuccessCallback";
  }
  if (instance.GetSuccessCallback() == &CoordinatorInstance::DemoteSuccessCallback) {
    return "DemoteSuccessCallback";
  }
  return "Unknown";
}

auto CoordinatorInstance::GetFailCallbackTypeName(ReplicationInstanceConnector const &instance) -> std::string_view {
  if (instance.GetFailCallback() == &CoordinatorInstance::ReplicaFailCallback) {
    return "ReplicaFailCallback";
  }
  if (instance.GetFailCallback() == &CoordinatorInstance::MainFailCallback) {
    return "MainFailCallback";
  }
  if (instance.GetFailCallback() == &CoordinatorInstance::DemoteFailCallback) {
    return "DemoteFailCallback";
  }
  return "Unknown";
}

auto CoordinatorInstance::TryVerifyOrCorrectClusterState() -> ReconcileClusterStateStatus {
  // Follows nomenclature from replication handler where Try<> means doing action from
  // user query
  return ReconcileClusterState_();
}

void CoordinatorInstance::ShuttingDown() { is_shutting_down_.store(true, std::memory_order_release); }

auto CoordinatorInstance::TryFailover() -> void {
  auto const is_replica = [this](ReplicationInstanceConnector const &instance) {
    return HasReplicaState(instance.InstanceName());
  };

  auto alive_replicas = repl_instances_ | ranges::views::filter(is_replica) |
                        ranges::views::filter(&ReplicationInstanceConnector::IsAlive);
  spdlog::trace("Trying failover in thread {}", std::this_thread::get_id());
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
  spdlog::trace("Found new main {} to do failover", new_main.InstanceName());
  if (!raft_state_->AppendOpenLock()) {
    spdlog::error("Aborting failover as instance is not anymore leader.");
    return;
  }

  utils::OnScopeExit const do_reset{[this]() {
    if (raft_state_->IsLockOpened() && raft_state_->IsLeader()) {
      spdlog::trace("Adding task to try reconcile cluster state as lock is still opened after failover.");
      thread_pool_.AddTask([this]() { this->ReconcileClusterState(); });
      return;
    }
    spdlog::trace("Failover done, lock is not opened anymore or coordinator is not leader.");
  }};

  // We don't need to stop frequent check as we have lock, and we will swap callback function during locked phase
  // In frequent check only when we take lock we then check which function (MAIN/REPLICA) success or fail callback
  // we need to call

  auto const is_not_new_main = [&new_main](ReplicationInstanceConnector &instance) {
    return instance.InstanceName() != new_main.InstanceName();
  };

  auto const new_main_uuid = utils::UUID{};

  auto const failed_to_swap = [this, &new_main_uuid](ReplicationInstanceConnector &instance) {
    spdlog::trace("Sending swap uuid to instance {} and updating raft log", instance.InstanceName());
    return !instance.SendSwapAndUpdateUUID(new_main_uuid) ||
           !raft_state_->AppendUpdateUUIDForInstanceLog(instance.InstanceName(), new_main_uuid);
  };

  // If for some replicas swap fails, for others on successful ping we will revert back on next change
  // or we will do failover first again and then it will be consistent again
  if (std::ranges::any_of(alive_replicas | ranges::views::filter(is_not_new_main), failed_to_swap)) {
    spdlog::error("Aborting failover. Failed to swap uuid for all alive instances.");
    return;
  }

  auto repl_clients_info = repl_instances_ | ranges::views::filter(is_not_new_main) |
                           ranges::views::transform(&ReplicationInstanceConnector::GetReplicationClientInfo) |
                           ranges::to<ReplicationClientsInfo>();

  if (!new_main.PromoteToMain(new_main_uuid, std::move(repl_clients_info), &CoordinatorInstance::MainSuccessCallback,
                              &CoordinatorInstance::MainFailCallback)) {
    spdlog::warn("Failover failed since promoting replica to main failed!");
    return;
  }

  if (!raft_state_->AppendUpdateUUIDForNewMainLog(new_main_uuid)) {
    return;
  }

  auto const new_main_instance_name = new_main.InstanceName();

  if (!raft_state_->AppendSetInstanceAsMainLog(new_main_instance_name, new_main_uuid)) {
    return;
  }

  if (!raft_state_->AppendCloseLock()) {
    spdlog::error("Aborting failover as we failed to close lock on action.");
    return;
  }

  MG_ASSERT(!raft_state_->IsLockOpened(), "After failover we need to be in healthy state.");

  if (!new_main.EnableWritingOnMain()) {
    spdlog::error("Failover successful but couldn't enable writing on instance.");
  }

  spdlog::info("Failover successful! Instance {} promoted to main.", new_main.InstanceName());
}

auto CoordinatorInstance::SetReplicationInstanceToMain(std::string_view instance_name)
    -> SetInstanceToMainCoordinatorStatus {
  spdlog::trace("Acquiring lock to set replication instance to main in thread {}", std::this_thread::get_id());
  auto lock = std::lock_guard{coord_instance_lock_};
  spdlog::trace("Acquired lock to set replication instance to main");

  if (!is_leader_ready_) {
    return SetInstanceToMainCoordinatorStatus::NOT_LEADER;
  }

  if (raft_state_->IsLockOpened()) {
    return SetInstanceToMainCoordinatorStatus::LOCK_OPENED;
  }

  if (raft_state_->MainExists()) {
    return SetInstanceToMainCoordinatorStatus::MAIN_ALREADY_EXISTS;
  }

  auto const is_new_main = [&instance_name](ReplicationInstanceConnector const &instance) {
    return instance.InstanceName() == instance_name;
  };

  auto new_main = std::ranges::find_if(repl_instances_, is_new_main);

  if (new_main == repl_instances_.end()) {
    spdlog::error("Instance {} not registered. Please register it using REGISTER INSTANCE {}", instance_name,
                  instance_name);
    return SetInstanceToMainCoordinatorStatus::NO_INSTANCE_WITH_NAME;
  }

  if (!raft_state_->AppendOpenLock()) {
    return SetInstanceToMainCoordinatorStatus::FAILED_TO_OPEN_LOCK;
  }

  utils::OnScopeExit const do_reconcile_cluster_state{[this]() {
    if (raft_state_->IsLockOpened() && raft_state_->IsLeader()) {
      spdlog::trace(
          "Adding task to try reconcile cluster state as lock didn't close successfully after setting instance to "
          "MAIN.");
      thread_pool_.AddTask([this]() { this->ReconcileClusterState(); });
      return;
    }
    spdlog::trace("Lock is not opened anymore or coordinator is not leader, after setting instance to MAIN.");
  }};

  new_main->PauseFrequentCheck();
  utils::OnScopeExit scope_exit{[&new_main] { new_main->ResumeFrequentCheck(); }};

  auto const is_not_new_main = [&instance_name](ReplicationInstanceConnector const &instance) {
    return instance.InstanceName() != instance_name;
  };

  auto const new_main_uuid = utils::UUID{};

  auto const failed_to_swap = [this, &new_main_uuid](ReplicationInstanceConnector &instance) {
    return !instance.SendSwapAndUpdateUUID(new_main_uuid) ||
           !raft_state_->AppendUpdateUUIDForInstanceLog(instance.InstanceName(), new_main_uuid);
  };

  if (std::ranges::any_of(repl_instances_ | ranges::views::filter(is_not_new_main), failed_to_swap)) {
    spdlog::error("Failed to swap uuid for all currently alive instances.");
    return SetInstanceToMainCoordinatorStatus::SWAP_UUID_FAILED;
  }

  spdlog::trace("Swapped uuid for all replica instances");

  auto repl_clients_info = repl_instances_ | ranges::views::filter(is_not_new_main) |
                           ranges::views::transform(&ReplicationInstanceConnector::GetReplicationClientInfo) |
                           ranges::to<ReplicationClientsInfo>();

  spdlog::trace("Collected ReplicationClientsInfo, trying to promote instance {} to main", new_main->InstanceName());

  if (!new_main->PromoteToMain(new_main_uuid, std::move(repl_clients_info), &CoordinatorInstance::MainSuccessCallback,
                               &CoordinatorInstance::MainFailCallback)) {
    spdlog::trace("Promoting instance {} failed", new_main->InstanceName());
    return SetInstanceToMainCoordinatorStatus::COULD_NOT_PROMOTE_TO_MAIN;
  }

  spdlog::trace("Instance {} promoted to main.", new_main->InstanceName());

  if (!raft_state_->AppendUpdateUUIDForNewMainLog(new_main_uuid)) {
    return SetInstanceToMainCoordinatorStatus::RAFT_LOG_ERROR;
  }

  spdlog::trace("Updated UUID for new main");

  if (!raft_state_->AppendSetInstanceAsMainLog(instance_name, new_main_uuid)) {
    return SetInstanceToMainCoordinatorStatus::RAFT_LOG_ERROR;
  }

  spdlog::trace("Instance {} promoted to main on leader", instance_name);

  if (!raft_state_->AppendCloseLock()) {
    spdlog::error("Aborting failover as we failed to close lock on action.");
    return SetInstanceToMainCoordinatorStatus::FAILED_TO_CLOSE_LOCK;
  }
  MG_ASSERT(!raft_state_->IsLockOpened(), "After setting replication instance we need to be in healthy state.");
  if (!new_main->EnableWritingOnMain()) {
    return SetInstanceToMainCoordinatorStatus::ENABLE_WRITING_FAILED;
  }
  return SetInstanceToMainCoordinatorStatus::SUCCESS;
}

auto CoordinatorInstance::RegisterReplicationInstance(CoordinatorToReplicaConfig const &config)
    -> RegisterInstanceCoordinatorStatus {
  spdlog::trace("Acquiring lock to register replication instance in thread {}", std::this_thread::get_id());
  auto lock = std::lock_guard{coord_instance_lock_};
  spdlog::trace("Acquired lock to register replication instance", std::this_thread::get_id());
  if (!is_leader_ready_) {
    spdlog::trace(" Is leader ready is in state {}", is_leader_ready_);
    return RegisterInstanceCoordinatorStatus::NOT_LEADER;
  }

  if (raft_state_->IsLockOpened()) {
    return RegisterInstanceCoordinatorStatus::LOCK_OPENED;
  }

  // TODO: (andi) Change that this is being asked from raft state
  if (std::ranges::any_of(repl_instances_,
                          [instance_name = config.instance_name](ReplicationInstanceConnector const &instance) {
                            return instance.InstanceName() == instance_name;
                          })) {
    return RegisterInstanceCoordinatorStatus::NAME_EXISTS;
  }

  if (std::ranges::any_of(repl_instances_, [&config](ReplicationInstanceConnector const &instance) {
        return instance.ManagementSocketAddress() == config.ManagementSocketAddress();
      })) {
    return RegisterInstanceCoordinatorStatus::MGMT_ENDPOINT_EXISTS;
  }

  if (std::ranges::any_of(repl_instances_, [&config](ReplicationInstanceConnector const &instance) {
        return instance.ReplicationSocketAddress() == config.ReplicationSocketAddress();
      })) {
    return RegisterInstanceCoordinatorStatus::REPL_ENDPOINT_EXISTS;
  }

  if (!raft_state_->AppendOpenLock()) {
    return RegisterInstanceCoordinatorStatus::FAILED_TO_OPEN_LOCK;
  }

  utils::OnScopeExit const do_reset{[this]() {
    if (raft_state_->IsLockOpened() && raft_state_->IsLeader()) {
      spdlog::trace(
          "Adding task to try reconcile cluster state as lock didn't close successfully after registration of "
          "instance.");
      thread_pool_.AddTask([this]() { this->ReconcileClusterState(); });
      return;
    }
    spdlog::trace("Lock is not opened anymore or coordinator is not leader after instance registration.");
  }};

  auto client = std::make_unique<ReplicationInstanceClient>(this, config, client_succ_cb_, client_fail_cb_);
  auto *new_instance = &repl_instances_.emplace_back(std::move(client), &CoordinatorInstance::ReplicaSuccessCallback,
                                                     &CoordinatorInstance::ReplicaFailCallback);

  if (!new_instance->DemoteToReplica(&CoordinatorInstance::ReplicaSuccessCallback,
                                     &CoordinatorInstance::ReplicaFailCallback)) {
    spdlog::error("Failed to send demote to replica rpc for instance {}", config.instance_name);
    return RegisterInstanceCoordinatorStatus::RPC_FAILED;
  }

  auto const main_name = raft_state_->TryGetCurrentMainName();

  if (main_name.has_value()) {
    auto &current_main = FindReplicationInstance(*main_name);

    if (!current_main.RegisterReplica(raft_state_->GetCurrentMainUUID(), new_instance->GetReplicationClientInfo())) {
      spdlog::error("Failed to register replica instance.");
      return RegisterInstanceCoordinatorStatus::RPC_FAILED;
    }
  }

  if (!raft_state_->AppendRegisterReplicationInstanceLog(config)) {
    return RegisterInstanceCoordinatorStatus::RAFT_LOG_ERROR;
  }

  if (!raft_state_->AppendCloseLock()) {
    spdlog::error("Aborting register instance as we failed to close lock on action.");
    return RegisterInstanceCoordinatorStatus::FAILED_TO_CLOSE_LOCK;
  }

  MG_ASSERT(!raft_state_->IsLockOpened(), "After registration of replication instance we need to be in healthy state.");

  new_instance->StartFrequentCheck();

  spdlog::info("Instance {} registered", config.instance_name);
  return RegisterInstanceCoordinatorStatus::SUCCESS;
}

auto CoordinatorInstance::DemoteInstanceToReplica(std::string_view instance_name) -> DemoteInstanceCoordinatorStatus {
  spdlog::trace("Acquiring lock to demote instance to replica in thread {}", std::this_thread::get_id());
  auto lock = std::lock_guard{coord_instance_lock_};
  spdlog::trace("Acquired lock to demote instance to replica");

  if (!is_leader_ready_) {
    spdlog::trace("Is leader ready is in state {}", is_leader_ready_);
    return DemoteInstanceCoordinatorStatus::NOT_LEADER;
  }

  if (raft_state_->IsLockOpened()) {
    spdlog::error("Lock is still opened.");
    return DemoteInstanceCoordinatorStatus::LOCK_OPENED;
  }

  if (!raft_state_->AppendOpenLock()) {
    spdlog::error("Failed to open lock to demote instance to REPLICA");
    return DemoteInstanceCoordinatorStatus::FAILED_TO_OPEN_LOCK;
  }

  utils::OnScopeExit const do_reset{[this]() {
    if (raft_state_->IsLockOpened() && raft_state_->IsLeader()) {
      spdlog::trace(
          "Adding task to try reconcile cluster state as lock didn't close successfully after demote of instance.");
      thread_pool_.AddTask([this]() { this->ReconcileClusterState(); });
      return;
    }
    spdlog::trace("Lock is not opened anymore or coordinator is not leader after demoting instance.");
  }};

  auto const name_matches = [&instance_name](ReplicationInstanceConnector const &instance) {
    return instance.InstanceName() == instance_name;
  };

  auto instance = std::ranges::find_if(repl_instances_, name_matches);
  if (instance == repl_instances_.end()) {
    return DemoteInstanceCoordinatorStatus::NO_INSTANCE_WITH_NAME;
  }

  if (!instance->DemoteToReplica(&CoordinatorInstance::ReplicaSuccessCallback,
                                 &CoordinatorInstance::ReplicaFailCallback)) {
    spdlog::error("Failed to send demote to replica rpc for instance {}", instance_name);
    return DemoteInstanceCoordinatorStatus::RPC_FAILED;
  }

  if (!raft_state_->AppendSetInstanceAsReplicaLog(instance->InstanceName())) {
    spdlog::error("Failed to send demote to replica rpc for instance {}", instance_name);
    return DemoteInstanceCoordinatorStatus::RPC_FAILED;
  }

  if (!raft_state_->AppendCloseLock()) {
    return DemoteInstanceCoordinatorStatus::FAILED_TO_CLOSE_LOCK;
  }

  return DemoteInstanceCoordinatorStatus::SUCCESS;
}

auto CoordinatorInstance::UnregisterReplicationInstance(std::string_view instance_name)
    -> UnregisterInstanceCoordinatorStatus {
  spdlog::trace("Acquiring lock to unregister instance thread {}", std::this_thread::get_id());
  auto lock = std::lock_guard{coord_instance_lock_};
  spdlog::trace("Acquired lock to demote instance to replica");

  if (!is_leader_ready_) {
    return UnregisterInstanceCoordinatorStatus::NOT_LEADER;
  }

  if (raft_state_->IsLockOpened()) {
    return UnregisterInstanceCoordinatorStatus::LOCK_OPENED;
  }

  auto const name_matches = [&instance_name](ReplicationInstanceConnector const &instance) {
    return instance.InstanceName() == instance_name;
  };

  auto inst_to_remove = std::ranges::find_if(repl_instances_, name_matches);
  if (inst_to_remove == repl_instances_.end()) {
    return UnregisterInstanceCoordinatorStatus::NO_INSTANCE_WITH_NAME;
  }

  auto const is_current_main = [this](ReplicationInstanceConnector const &instance) {
    return raft_state_->IsCurrentMain(instance.InstanceName()) && instance.IsAlive();
  };

  if (is_current_main(*inst_to_remove)) {
    return UnregisterInstanceCoordinatorStatus::IS_MAIN;
  }

  if (!raft_state_->AppendOpenLock()) {
    return UnregisterInstanceCoordinatorStatus::FAILED_TO_OPEN_LOCK;
  }

  utils::OnScopeExit const do_reset{[this]() {
    if (raft_state_->IsLockOpened() && raft_state_->IsLeader()) {
      spdlog::trace(
          "Adding task to try reconcile cluster state as lock didn't close successfully after unregistration of "
          "instance.");
      thread_pool_.AddTask([this]() { this->ReconcileClusterState(); });
      return;
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

  if (!raft_state_->AppendUnregisterReplicationInstanceLog(instance_name)) {
    return UnregisterInstanceCoordinatorStatus::RAFT_LOG_ERROR;
  }

  if (!raft_state_->AppendCloseLock()) {
    spdlog::error("Aborting register instance as we failed to close lock on action.");
    return UnregisterInstanceCoordinatorStatus::FAILED_TO_CLOSE_LOCK;
  }

  MG_ASSERT(!raft_state_->IsLockOpened(),
            "After unregistration of replication instance we need to be in healthy state.");

  return UnregisterInstanceCoordinatorStatus::SUCCESS;
}

auto CoordinatorInstance::AddCoordinatorInstance(CoordinatorToCoordinatorConfig const &config)
    -> AddCoordinatorInstanceStatus {
  spdlog::trace("Adding coordinator instance {} start in CoordinatorInstance for {}", config.coordinator_id,
                raft_state_->InstanceName());

  auto const curr_instances = raft_state_->GetCoordinatorInstances();
  if (std::ranges::any_of(curr_instances, [&config](auto const &instance) {
        return instance.coordinator_id == config.coordinator_id;
      })) {
    return AddCoordinatorInstanceStatus::ID_ALREADY_EXISTS;
  }

  if (std::ranges::any_of(curr_instances, [&config](auto const &instance) {
        return instance.coordinator_server.SocketAddress() == config.coordinator_server.SocketAddress();
      })) {
    return AddCoordinatorInstanceStatus::COORDINATOR_ENDPOINT_ALREADY_EXISTS;
  }

  if (std::ranges::any_of(curr_instances, [&config](auto const &instance) {
        return instance.bolt_server.SocketAddress() == config.bolt_server.SocketAddress();
      })) {
    return AddCoordinatorInstanceStatus::BOLT_ENDPOINT_ALREADY_EXISTS;
  }

  raft_state_->AddCoordinatorInstance(config);
  return AddCoordinatorInstanceStatus::SUCCESS;
}

void CoordinatorInstance::MainFailCallback(std::string_view repl_instance_name) {
  spdlog::trace("Instance {} performing main fail callback in thread {}", repl_instance_name,
                std::this_thread::get_id());
  if (raft_state_->IsLockOpened()) {
    spdlog::error("Returning from main fail callback as the last action didn't successfully finish");
  }

  auto &repl_instance = FindReplicationInstance(repl_instance_name);
  repl_instance.OnFailPing();

  // NOLINTNEXTLINE
  if (!repl_instance.IsAlive() && raft_state_->IsCurrentMain(repl_instance_name)) {
    spdlog::info("Cluster without main instance, trying automatic failover");
    TryFailover();
  }
}

void CoordinatorInstance::MainSuccessCallback(std::string_view repl_instance_name) {
  spdlog::trace("Instance {} performing main successful callback in thread", repl_instance_name,
                std::this_thread::get_id());

  if (raft_state_->IsLockOpened()) {
    spdlog::error("Stopping main successful callback as the last action didn't successfully finish");
    return;
  }

  auto &repl_instance = FindReplicationInstance(repl_instance_name);

  if (repl_instance.IsAlive()) {
    repl_instance.OnSuccessPing();
    return;
  }

  // NOLINTNEXTLINE
  if (raft_state_->IsCurrentMain(repl_instance.InstanceName())) {
    if (!repl_instance.EnableWritingOnMain()) {
      spdlog::error("Failed to enable writing on main instance {}", repl_instance_name);
      return;
    }

    repl_instance.OnSuccessPing();
    return;
  }

  // Demote to replica callback

  if (!raft_state_->AppendOpenLock()) {
    spdlog::error("Raft log didn't accept instance open lock for demoting instance {} to replica.", repl_instance_name);
    return;
  }
  utils::OnScopeExit const reconcile_cluster_state{[this]() {
    if (raft_state_->IsLockOpened() && raft_state_->IsLeader()) {
      spdlog::trace(
          "Adding task to try reconcile cluster state lock is opened still after setting instance needs demote.");
      thread_pool_.AddTask([this]() { this->ReconcileClusterState(); });
      return;
    }
    spdlog::trace("Lock is not opened anymore or coordinator is not leader.");
  }};

  if (!raft_state_->AppendInstanceNeedsDemote(repl_instance_name)) {
    spdlog::error("Raft log didn't accept instance needs demote.");
    return;
  }

  if (!raft_state_->AppendCloseLock()) {
    spdlog::error("Raft log didn't accept instance close lock for demoting instance {} to replica.",
                  repl_instance_name);
    return;
  }

  repl_instance.SetCallbacks(&CoordinatorInstance::DemoteSuccessCallback, &CoordinatorInstance::DemoteFailCallback);
}

void CoordinatorInstance::ReplicaSuccessCallback(std::string_view repl_instance_name) {
  spdlog::trace("Instance {} performing replica successful callback in thread", repl_instance_name,
                std::this_thread::get_id());

  if (raft_state_->IsLockOpened()) {
    spdlog::error("Stopping main successful callback as the last action didn't successfully finish");
    return;
  }

  auto &repl_instance = FindReplicationInstance(repl_instance_name);

  // We need to get replicas UUID from time to time to ensure replica is listening to correct main
  // and that it didn't go down for less time than we could notice
  // We need to get id of main replica is listening to
  // and swap if necessary
  if (!repl_instance.EnsureReplicaHasCorrectMainUUID(raft_state_->GetCurrentMainUUID())) {
    spdlog::error("Failed to swap uuid for replica instance {} which is alive", repl_instance.InstanceName());
    return;
  }

  repl_instance.OnSuccessPing();
}

void CoordinatorInstance::ReplicaFailCallback(std::string_view repl_instance_name) {
  spdlog::trace("Instance {} performing replica failure callback in thread {}", repl_instance_name,
                std::this_thread::get_id());

  if (raft_state_->IsLockOpened()) {
    spdlog::error("Stopping main successful callback as the last action didn't successfully finish.");
    return;
  }

  auto &repl_instance = FindReplicationInstance(repl_instance_name);

  repl_instance.OnFailPing();
}

void CoordinatorInstance::DemoteSuccessCallback(std::string_view repl_instance_name) {
  spdlog::trace("Instance {} performing demote to replica successful callback in thread", repl_instance_name,
                std::this_thread::get_id());

  auto &repl_instance = FindReplicationInstance(repl_instance_name);

  if (!raft_state_->AppendOpenLock()) {
    spdlog::error("Failed to open lock for demoting instance {} to REPLICA", repl_instance_name);
    return;
  }

  utils::OnScopeExit const reconcile_cluster_state{[this]() {
    if (raft_state_->IsLockOpened() && raft_state_->IsLeader()) {
      spdlog::trace(
          "Adding task to try reconcile cluster state again as lock is opened still after demoting instance.");
      thread_pool_.AddTask([this]() { this->ReconcileClusterState(); });
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

  if (!raft_state_->AppendSetInstanceAsReplicaLog(repl_instance_name)) {
    spdlog::error("Failed to append log that OLD MAIN was demoted to REPLICA {}", repl_instance_name);
    return;
  }

  if (!repl_instance.SendSwapAndUpdateUUID(raft_state_->GetCurrentMainUUID())) {
    spdlog::error("Failed to swap uuid for demoted main instance {}", repl_instance_name);
    return;
  }

  if (!raft_state_->AppendUpdateUUIDForInstanceLog(repl_instance_name, raft_state_->GetCurrentMainUUID())) {
    spdlog::error("Failed to update log of changing instance uuid {} to {}", repl_instance_name,
                  std::string{raft_state_->GetCurrentMainUUID()});
    return;
  }

  if (!raft_state_->AppendCloseLock()) {
    spdlog::error("Failed to close lock for demoting MAIN to REPLICA", repl_instance_name);
    return;
  }

  repl_instance.SetCallbacks(&CoordinatorInstance::ReplicaSuccessCallback, &CoordinatorInstance::ReplicaFailCallback);

  repl_instance.OnSuccessPing();
}

void CoordinatorInstance::DemoteFailCallback(std::string_view repl_instance_name) {
  spdlog::trace("Instance {} performing demote to replica failure callback in thread", repl_instance_name,
                std::this_thread::get_id());
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
  return raft_state_->HasMainState(instance_name);
}

auto CoordinatorInstance::HasReplicaState(std::string_view instance_name) const -> bool {
  return raft_state_->HasReplicaState(instance_name);
}

auto CoordinatorInstance::GetRoutingTable() const -> RoutingTable { return raft_state_->GetRoutingTable(); }

auto CoordinatorInstance::IsLeader() const -> bool { return raft_state_->IsLeader(); }

auto CoordinatorInstance::GetRaftState() -> RaftState & { return *raft_state_; }
auto CoordinatorInstance::GetRaftState() const -> RaftState const & { return *raft_state_; }

}  // namespace memgraph::coordination
#endif
