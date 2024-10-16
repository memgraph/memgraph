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
  client_succ_cb_ = [](CoordinatorInstance *self, std::string_view instance_name,
                       std::optional<InstanceState> instance_state) -> void {
    if (self->raft_state_->IsLockOpened()) {
      spdlog::trace("Lock is still opened, not executing instance success callback.");
      return;
    }

    if (!self->is_leader_ready_) {
      spdlog::trace("Leader is not ready, not executing instance fail callback.");
      return;
    }

    auto lock = std::unique_lock{self->coord_instance_lock_};
    auto &instance = self->FindReplicationInstance(instance_name);
    std::invoke(instance.GetSuccessCallback(), self, instance, instance_state);
  };

  client_fail_cb_ = [](CoordinatorInstance *self, std::string_view instance_name,
                       std::optional<InstanceState> instance_state) -> void {
    if (self->raft_state_->IsLockOpened()) {
      spdlog::trace("Lock is still opened, not executing instance fail callback.");
      return;
    }

    if (!self->is_leader_ready_) {
      spdlog::trace("Leader is not ready, not executing instance fail callback.");
      return;
    }
    auto lock = std::unique_lock{self->coord_instance_lock_};
    auto &instance = self->FindReplicationInstance(instance_name);
    std::invoke(instance.GetFailCallback(), self, instance, instance_state);
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
  // thread that we are shutting down, and we need to stop state checks and thread pool before anything is destructed
  // otherwise thread pool can get task become leader, when raft_state_ ptr is already destructed
  // Order is important:
  // 1. Thread pool might be running with reconcile cluster state, or become follower, so we should shut that down
  // 2. Await shutdown of coordinator thread pool so that coordinator can't become follower or do reconcile cluster
  // state
  // 3. State checks running, we need to stop them before raft_state_ is destroyed
  ShuttingDown();
  thread_pool_.ShutDown();
  // We don't need to take lock as reconcile cluster state can't be running, coordinator can't become follower,
  // user queries can't be running as memgraph awaits server shutdown
  std::ranges::for_each(repl_instances_, [](auto &repl_instance) { repl_instance.StopStateCheck(); });
}

auto CoordinatorInstance::GetBecomeLeaderCallback() -> std::function<void()> {
  return [this]() {
    spdlog::trace("Executing become leader callback in thread {}.", std::this_thread::get_id());
    if (is_shutting_down_.load(std::memory_order_acquire)) {
      return;
    }
    is_leader_ready_.store(false, std::memory_order_release);
    // Thread pool is needed because becoming leader is blocking action, and if we don't succeed to check state of
    // cluster we will try again and again in same thread, thus blocking progress of NuRaft leader election.
    thread_pool_.AddTask([this]() { this->ReconcileClusterState(); });
    return;
  };
}

auto CoordinatorInstance::GetBecomeFollowerCallback() -> std::function<void()> {
  return [this]() {
    thread_pool_.AddTask([this]() {
      spdlog::info("Executing become follower callback in thread {}.", std::this_thread::get_id());
      is_leader_ready_.store(false, std::memory_order_release);
      // We need to stop checks before taking a lock because deadlock can happen if instances wait
      // to take a lock in state check, and this thread already has a lock and waits for instance to
      // be done with state check
      std::ranges::for_each(repl_instances_, [](auto &&repl_instance) {
        spdlog::trace("Stopping state check for instance {} in thread {}.", repl_instance.InstanceName(),
                      std::this_thread::get_id());
        repl_instance.StopStateCheck();
        spdlog::trace("Stopped state check for instance {} in thread {}.", repl_instance.InstanceName(),
                      std::this_thread::get_id());
      });
      spdlog::info("Acquiring lock in become follower callback in thread {}.", std::this_thread::get_id());
      auto lock = std::unique_lock{coord_instance_lock_};
      repl_instances_.clear();
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

  is_leader_ready_.store(false, std::memory_order_release);

  auto attempt_cnt{1};
  while (raft_state_->IsLeader()) {
    spdlog::trace("Trying to ensure cluster's healthy state. The coordinator is considered a leader. Attempt: {}.",
                  attempt_cnt++);
    auto const result = ReconcileClusterState_();
    switch (result) {
      case (ReconcileClusterStateStatus::SUCCESS):
        is_leader_ready_.store(true, std::memory_order_release);
        spdlog::trace("Reconcile cluster state finished successfully.");
        return result;
      // if status is fail, don't set is_leader_ready_ to true.
      case ReconcileClusterStateStatus::FAIL:
        spdlog::trace("ReconcileClusterState_ failed!");
        break;
      // if the instance is shutting down don't set is_leader_ready_ to true.
      case ReconcileClusterStateStatus::SHUTTING_DOWN:
        spdlog::trace("Stopping reconciliation as coordinator is shutting down.");
        return result;
      case ReconcileClusterStateStatus::NOT_LEADER_ANYMORE: {
        MG_ASSERT(false, "Invalid status handling. Crashing the database.");
      }
    }
    backoff.wait();
  }

  // It's possible that the instance got request to become follower since the start of executing this method.
  return ReconcileClusterStateStatus::NOT_LEADER_ANYMORE;
}

auto CoordinatorInstance::ReconcileClusterState_() -> ReconcileClusterStateStatus {
  spdlog::trace("Doing ReconcileClusterState_.");

  if (is_shutting_down_.load(std::memory_order_acquire)) {
    return ReconcileClusterStateStatus::SHUTTING_DOWN;
  }

  // Ordering is important here, we must stop frequent check before
  // taking lock to avoid deadlock between us stopping thread and thread wanting to take lock but can't because
  // we have it
  std::ranges::for_each(repl_instances_, [](auto &&repl_instance) {
    spdlog::trace("Stopping state checks for instance {}.", repl_instance.InstanceName());
    repl_instance.StopStateCheck();
    spdlog::trace("Stopped state check for instance {}.", repl_instance.InstanceName());
  });
  spdlog::trace("Stopped all state checks and acquiring lock in the thread {}.", std::this_thread::get_id());
  auto lock = std::unique_lock{coord_instance_lock_};
  spdlog::trace("Acquired lock in the reconciliation cluster state reset thread {}.", std::this_thread::get_id());
  repl_instances_.clear();

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
          "divergence of cluster logs.");
    } else {
      return ReconcileClusterStateStatus::SUCCESS;
    }
  }

  if (raft_state_->IsLockOpened() && !raft_state_->AppendCloseLock()) {
    spdlog::trace("Exiting ReconcileClusterState. Failed to append close lock.");
    return ReconcileClusterStateStatus::FAIL;
  }
  spdlog::trace("Lock is closed.");

  // There is nothing to do if we don't have any instances
  if (raft_state_replication_instances.empty()) {
    spdlog::trace("Exiting ReconcileClusterState. Didn't get any replication instances.");
    return ReconcileClusterStateStatus::SUCCESS;
  }

  std::ranges::for_each(raft_state_replication_instances, [this](auto &&instance) {
    auto client = std::make_unique<ReplicationInstanceClient>(this, instance.config, client_succ_cb_, client_fail_cb_);
    repl_instances_.emplace_back(std::move(client), &CoordinatorInstance::InstanceSuccessCallback,
                                 &CoordinatorInstance::InstanceFailCallback);
  });

  auto current_mains = repl_instances_ | ranges::views::filter([raft_state_ptr = raft_state_.get()](auto &&instance) {
                         return raft_state_ptr->IsCurrentMain(instance.InstanceName());
                       });

  auto const num_mains = std::ranges::distance(current_mains);

  if (num_mains == 1) {
    // If we have alive MAIN instance we expect that the cluster was in the correct state already. We can start
    // frequent checks and set all appropriate callbacks.
    auto main_instance = std::ranges::begin(current_mains);
    spdlog::trace("Found main instance {}.", main_instance->InstanceName());
  } else if (num_mains == 0) {
    spdlog::trace(
        "No main can be determined from the current state in logs. Trying to find most up to date instance by doing "
        "failover");
    if (!TryFailover()) {
      spdlog::trace("Failover failed while reconciling cluster state.");
      return ReconcileClusterStateStatus::FAIL;
    }
  } else {
    MG_ASSERT(
        false,
        "Found more than one current main. System cannot operate under this assumption, please contact the support.");
  }

  std::ranges::for_each(repl_instances_, [](auto &&instance) { instance.StartStateCheck(); });

  spdlog::trace("Exiting ReconcileClusterState. Cluster is in healthy state.");
  return ReconcileClusterStateStatus::SUCCESS;
}

auto CoordinatorInstance::TryVerifyOrCorrectClusterState() -> ReconcileClusterStateStatus {
  // Follows nomenclature from replication handler where Try<> means doing action from the
  // user query.
  return ReconcileClusterState();
}

void CoordinatorInstance::ShuttingDown() { is_shutting_down_.store(true, std::memory_order_release); }

auto CoordinatorInstance::TryFailover() -> bool {
  // TODO: (andi) Remove has replica state.
  // TODO: (andi) Ideally, we have one log which would atomically update cluster state in raft log.
  // We don't need to stop state check as long as we are holding coord_instance_lock_.
  // If I have only one append then opening and closing the lock won't be needed.

  if (!raft_state_->AppendOpenLock()) {
    spdlog::error("Failed to open the lock while doing failover.");
    return true;  // we return true because if we couldn't open the lock that means that the state in raft log is still
                  // ok. The expected caller's action is to just repeat the process.
  }

  spdlog::trace("Trying failover in thread {}.", std::this_thread::get_id());

  auto const maybe_most_up_to_date_instance = GetMostUpToDateInstanceFromHistories(repl_instances_);
  if (!maybe_most_up_to_date_instance.has_value()) {
    spdlog::error("Couldn't choose instance for failover, check logs for more details.");
    return false;
  }

  auto const &new_main_name = *maybe_most_up_to_date_instance;
  spdlog::trace("Found new main instance {} while doing failover.", new_main_name);

  auto const new_main_uuid = utils::UUID{};

  auto const failed_to_update_role = [raft_state_ptr = raft_state_.get()](auto &&instance) {
    return !raft_state_ptr->AppendSetInstanceAsReplicaLog(instance.InstanceName());
  };

  auto const failed_to_update_uuid = [raft_state_ptr = raft_state_.get(), &new_main_uuid](auto &&instance) {
    return !raft_state_ptr->AppendUpdateUUIDForInstanceLog(instance.InstanceName(), new_main_uuid);
  };

  auto const not_main = [&new_main_name](auto &&instance) { return instance.InstanceName() != new_main_name; };

  if (std::ranges::any_of(repl_instances_ | ranges::views::filter(not_main), failed_to_update_role)) {
    spdlog::error("Aborting failover. Failed to update role for one of replicas.");
    return false;
  }

  if (std::ranges::any_of(repl_instances_ | ranges::views::filter(not_main), failed_to_update_uuid)) {
    spdlog::error("Aborting failover. Failed to update uuid for one of replicas.");
    return false;
  }

  if (!raft_state_->AppendUpdateUUIDForNewMainLog(new_main_uuid)) {
    spdlog::error("Aborting failover. Failed to update uuid of main instance.");
    return false;
  }

  // it's important that this is executed after setting uuid for each replica instance. Otherwise check if IsCurrentMain
  // isn't enough in ReconcileClusterState_.
  if (!raft_state_->AppendSetInstanceAsMainLog(new_main_name, new_main_uuid)) {
    spdlog::error("Aborting failover. Failed to add log to set instance {} as new main.", new_main_name);
    return false;
  }

  if (!raft_state_->AppendCloseLock()) {
    spdlog::error("Aborting failover as we failed to close lock on action.");
    return false;
  }
  return true;
}

auto CoordinatorInstance::SetReplicationInstanceToMain(std::string_view instance_name)
    -> SetInstanceToMainCoordinatorStatus {
  spdlog::trace("Acquiring lock to set replication instance to main in thread {}.", std::this_thread::get_id());
  auto lock = std::lock_guard{coord_instance_lock_};
  spdlog::trace("Acquired lock to set replication instance to main in thrad {}.", std::this_thread::get_id());

  if (!is_leader_ready_) {
    return SetInstanceToMainCoordinatorStatus::NOT_LEADER;
  }

  if (raft_state_->IsLockOpened()) {
    return SetInstanceToMainCoordinatorStatus::LOCK_OPENED;
  }

  if (raft_state_->MainExists()) {
    return SetInstanceToMainCoordinatorStatus::MAIN_ALREADY_EXISTS;
  }

  auto const is_new_main = [&instance_name](auto &&instance) { return instance.InstanceName() == instance_name; };

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

  new_main->PauseStateCheck();
  utils::OnScopeExit const scope_exit{[&new_main] { new_main->ResumeStateCheck(); }};

  auto const is_not_new_main = [&instance_name](auto &&instance) { return instance.InstanceName() != instance_name; };

  auto const new_main_uuid = utils::UUID{};

  auto const failed_to_swap = [this, &new_main_uuid](auto &&instance) {
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

  spdlog::trace("Collected ReplicationClientsInfo, trying to promote instance {} to main", instance_name);

  if (!new_main->SendPromoteToMainRpc(new_main_uuid, std::move(repl_clients_info))) {
    spdlog::trace("Promoting instance {} failed", instance_name);
    return SetInstanceToMainCoordinatorStatus::COULD_NOT_PROMOTE_TO_MAIN;
  }

  spdlog::trace("Instance {} promoted to main.", instance_name);

  if (!raft_state_->AppendUpdateUUIDForNewMainLog(new_main_uuid)) {
    return SetInstanceToMainCoordinatorStatus::RAFT_LOG_ERROR;
  }

  spdlog::trace("Updated UUID for new main.");

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

  if (raft_state_->IsLockOpened()) {
    return RegisterInstanceCoordinatorStatus::LOCK_OPENED;
  }

  if (!is_leader_ready_) {
    spdlog::trace("Leader is not ready, existing.");
    return RegisterInstanceCoordinatorStatus::NOT_LEADER;
  }

  if (std::ranges::any_of(repl_instances_, [new_instance_name = config.instance_name](auto &&instance) {
        return instance.InstanceName() == new_instance_name;
      })) {
    return RegisterInstanceCoordinatorStatus::NAME_EXISTS;
  }

  if (std::ranges::any_of(repl_instances_, [&config](auto &&instance) {
        return instance.ManagementSocketAddress() == config.ManagementSocketAddress();
      })) {
    return RegisterInstanceCoordinatorStatus::MGMT_ENDPOINT_EXISTS;
  }

  if (std::ranges::any_of(repl_instances_, [&config](auto &&instance) {
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
  auto *new_instance = &repl_instances_.emplace_back(std::move(client), &CoordinatorInstance::InstanceSuccessCallback,
                                                     &CoordinatorInstance::InstanceFailCallback);

  // We do this here not under callbacks because we need to add replica to the current main.
  if (!new_instance->SendDemoteToReplicaRpc()) {
    spdlog::error("Failed to demote instance {} to replica.", config.instance_name);
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

  new_instance->StartStateCheck();

  spdlog::info("Instance {} registered", config.instance_name);
  return RegisterInstanceCoordinatorStatus::SUCCESS;
}

auto CoordinatorInstance::DemoteInstanceToReplica(std::string_view instance_name) -> DemoteInstanceCoordinatorStatus {
  spdlog::trace("Acquiring lock to demote instance to replica in thread {}", std::this_thread::get_id());
  auto lock = std::lock_guard{coord_instance_lock_};
  spdlog::trace("Acquired lock to demote instance to replica");

  if (!is_leader_ready_) {
    spdlog::trace("Leader isn't ready.");
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

  // if (!instance->DemoteToReplica(&CoordinatorInstance::ReplicaSuccessCallback,
  //                                &CoordinatorInstance::InstanceFailCallback)) {
  //   spdlog::error("Failed to send demote to replica rpc for instance {}", instance_name);
  //   return DemoteInstanceCoordinatorStatus::RPC_FAILED;
  // }

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

  auto const name_matches = [&instance_name](auto &&instance) { return instance.InstanceName() == instance_name; };

  auto inst_to_remove = std::ranges::find_if(repl_instances_, name_matches);
  if (inst_to_remove == repl_instances_.end()) {
    return UnregisterInstanceCoordinatorStatus::NO_INSTANCE_WITH_NAME;
  }

  auto const is_current_main = [this](auto &&instance) {
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

  inst_to_remove->StopStateCheck();

  auto curr_main = std::ranges::find_if(repl_instances_, is_current_main);

  if (curr_main != repl_instances_.end()) {
    if (!curr_main->SendUnregisterReplicaRpc(instance_name)) {
      inst_to_remove->StartStateCheck();
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

void CoordinatorInstance::InstanceSuccessCallback(ReplicationInstanceConnector &instance,
                                                  std::optional<InstanceState> instance_state) {
  auto const instance_name = instance.InstanceName();
  spdlog::trace("Instance {} performing success callback in thread {}.", instance_name, std::this_thread::get_id());

  instance.OnSuccessPing();

  auto const curr_main_uuid = raft_state_->GetCurrentMainUUID();

  // if I am main and based on Raft log I am the current main I cannot have different UUID and writing is enabled.
  // Therefore, only the situation where I am replica is handled
  if (raft_state_->IsCurrentMain(instance_name)) {
    if (instance_state->is_replica) {
      auto const is_not_main = [instance_name](auto &&instance) { return instance.InstanceName() != instance_name; };
      auto repl_clients_info = repl_instances_ | ranges::views::filter(is_not_main) |
                               ranges::views::transform(&ReplicationInstanceConnector::GetReplicationClientInfo) |
                               ranges::to<ReplicationClientsInfo>();

      if (!instance.SendPromoteToMainRpc(curr_main_uuid, std::move(repl_clients_info))) {
        spdlog::error("Failed to promote instance to main with new uuid {}. Trying to do failover again.",
                      std::string{curr_main_uuid});
        // running reconcile cluster state in thread pool is not a problem because we have a guarantee that if
        // TryFailover failed the lock remains opened, hence callbacks won't get executed until lock is closed.
        // It's possible that the lock didn't even get opened. In that case we will repeat the action.
        if (!TryFailover()) {
          thread_pool_.AddTask([this]() { this->ReconcileClusterState(); });
        }
        return;
      }
    } else {
      // TODO: (andi) I could have writing disabled because of restart.
    }
  } else {
    // The instance should be replica.
    if (!instance_state->is_replica) {
      // If instance is not replica, demote it to become replica. If request for demotion failed, return
      // and you will simply retry on the next ping.
      if (!instance.SendDemoteToReplicaRpc()) {
        spdlog::error("Couldn't demote instance {} to replica.", instance_name);
        return;
      }
    }

    if (!instance_state->uuid || *instance_state->uuid != curr_main_uuid) {
      if (!instance.SendSwapAndUpdateUUID(curr_main_uuid)) {
        spdlog::error("Failed to set new uuid for replica instance {} to {}.", instance_name,
                      std::string{curr_main_uuid});
        return;
      }
    }
  }
}

void CoordinatorInstance::InstanceFailCallback(ReplicationInstanceConnector &instance,
                                               std::optional<InstanceState> /*instance_state*/) {
  auto const instance_name = instance.InstanceName();
  spdlog::trace("Instance {} performing fail callback in thread {}.", instance_name, std::this_thread::get_id());
  instance.OnFailPing();

  if (raft_state_->IsCurrentMain(instance_name) && !instance.IsAlive()) {
    spdlog::trace("Cluster without main instance, trying failover.");
    if (!TryFailover()) {
      // running reconcile cluster state in thread pool is not a problem because we have a guarantee that if TryFailover
      // failed the lock remains opened, hence callbacks won't get executed until lock is closed.
      thread_pool_.AddTask([this]() { this->ReconcileClusterState(); });
    }
  }

  spdlog::trace("Instance {} finished fail callback.", instance_name);
}

auto CoordinatorInstance::ChooseMostUpToDateInstance(std::span<InstanceNameDbHistories> instance_database_histories)
    -> NewMainRes {
  std::optional<NewMainRes> new_main_res;

  for (auto const &instance_res_pair : instance_database_histories) {
    const auto &[instance_name, instance_db_histories] = instance_res_pair;

    // Find default db for instance and its history
    auto default_db_history_data = std::ranges::find_if(
        instance_db_histories,
        [default_db = memgraph::dbms::kDefaultDB](auto &&db_history) { return db_history.name == default_db; });

    std::ranges::for_each(instance_db_histories, [&instance_name](auto &&db_history) {
      spdlog::debug("Instance {}: db_history_name {}, default db {}.", instance_name, db_history.name,
                    memgraph::dbms::kDefaultDB);
    });

    MG_ASSERT(default_db_history_data != instance_db_histories.end(), "No history for instance");

    const auto &instance_default_db_history = default_db_history_data->history;

    std::ranges::for_each(instance_default_db_history | ranges::views::reverse,
                          [&instance_name](auto &&epoch_history_it) {
                            spdlog::debug("Instance {}: epoch {}, last_durable_timestamp: {}.", instance_name,
                                          std::get<0>(epoch_history_it), std::get<1>(epoch_history_it));
                          });

    if (!new_main_res) {
      const auto &[epoch, latest_commit_timestamp] = *instance_default_db_history.crbegin();
      new_main_res = std::make_optional<NewMainRes>({instance_name, epoch, latest_commit_timestamp});
      spdlog::debug("Currently the most up to date instance is {} with epoch {} and latest commit timestamp {}.",
                    instance_name, epoch, latest_commit_timestamp);
      continue;
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
  }

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

auto CoordinatorInstance::GetMostUpToDateInstanceFromHistories(std::list<ReplicationInstanceConnector> &instances)
    -> std::optional<std::string> {
  if (instances.empty()) {
    return std::nullopt;
  }

  auto const get_ts = [](auto &&instance) {
    spdlog::trace("Sending get instance timestamps to {}", instance.InstanceName());
    return instance.GetClient().SendGetInstanceTimestampsRpc();
  };

  auto maybe_instance_db_histories = instances | ranges::views::transform(get_ts);

  auto const ts_has_value = [](auto &&instance_history) -> bool {
    auto const &[_, history] = instance_history;
    return history.HasValue();
  };

  auto transform_to_pairs = ranges::views::transform([](auto &&instance_history) {
    auto const &[instance, history] = instance_history;
    return std::make_pair(instance.InstanceName(), history.GetValue());
  });

  auto instance_db_histories = ranges::views::zip(instances, maybe_instance_db_histories) |
                               ranges::views::filter(ts_has_value) | transform_to_pairs | ranges::to<std::vector>();

  auto [most_up_to_date_instance, latest_epoch, latest_commit_timestamp] =
      CoordinatorInstance::ChooseMostUpToDateInstance(instance_db_histories);

  spdlog::trace("The most up to date instance is {} with epoch {} and {} latest commit timestamp.",
                most_up_to_date_instance, latest_epoch, latest_commit_timestamp);  // NOLINT

  return most_up_to_date_instance;
}

}  // namespace memgraph::coordination
#endif
