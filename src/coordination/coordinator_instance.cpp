// Copyright 2025 Memgraph Ltd.
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

#include <communication/bolt/v1/encoder/base_encoder.hpp>
#include "coordination/coordination_observer.hpp"
#include "coordination/coordinator_cluster_state.hpp"
#include "coordination/coordinator_communication_config.hpp"
#include "coordination/coordinator_exceptions.hpp"
#include "coordination/coordinator_instance.hpp"
#include "coordination/coordinator_instance_management_server.hpp"
#include "coordination/coordinator_instance_management_server_handlers.hpp"
#include "coordination/coordinator_ops_status.hpp"
#include "coordination/instance_status.hpp"
#include "coordination/raft_state.hpp"
#include "coordination/replication_instance_client.hpp"
#include "coordination/replication_instance_connector.hpp"
#include "replication_coordination_glue/role.hpp"
#include "utils/event_counter.hpp"
#include "utils/exponential_backoff.hpp"
#include "utils/functional.hpp"
#include "utils/logging.hpp"
#include "utils/metrics_timer.hpp"

#include <fmt/ranges.h>
#include <spdlog/spdlog.h>
#include <range/v3/range/conversion.hpp>
#include <range/v3/view/filter.hpp>
#include <range/v3/view/transform.hpp>

namespace memgraph::metrics {
// Counters
extern const Event SuccessfulFailovers;
extern const Event RaftFailedFailovers;
extern const Event NoAliveInstanceFailedFailovers;
extern const Event BecomeLeaderSuccess;
extern const Event FailedToBecomeLeader;
extern const Event ShowInstance;
extern const Event ShowInstances;
extern const Event DemoteInstance;
extern const Event UnregisterReplInstance;
extern const Event RemoveCoordInstance;
// Histogram
extern const Event InstanceSuccCallback_us;
extern const Event InstanceFailCallback_us;
extern const Event ChooseMostUpToDateInstance_us;
extern const Event GetHistories_us;
extern const Event DataFailover_us;
}  // namespace memgraph::metrics

namespace memgraph::coordination {
namespace {
constexpr int kDisconnectedCluster = 1;
constexpr std::string_view kUp{"up"};
constexpr std::string_view kDown{"down"};
}  // namespace

using nuraft::ptr;
using namespace std::chrono_literals;

CoordinatorInstance::CoordinatorInstance(CoordinatorInstanceInitConfig const &config)
    : instance_down_timeout_sec_(config.instance_down_timeout_sec),
      instance_health_check_frequency_sec_(config.instance_health_check_frequency_sec),
      coordinator_management_server_{ManagementServerConfig{
          io::network::Endpoint{kDefaultManagementServerIp, static_cast<uint16_t>(config.management_port)}}} {
  CoordinatorInstanceManagementServerHandlers::Register(coordinator_management_server_, *this);
  MG_ASSERT(coordinator_management_server_.Start(), "Management server on coordinator couldn't be started.");

  // Delay constructing of Raft state until everything is constructed in coordinator instance
  // since raft state will call become leader callback or become follower callback on construction.
  // If something is not yet constructed in coordinator instance, we get UB
  raft_state_ = std::make_unique<RaftState>(config, GetBecomeLeaderCallback(), GetBecomeFollowerCallback(),
                                            CoordinationClusterChangeObserver{this});
  UpdateClientConnectors(raft_state_->GetCoordinatorInstancesAux());
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
    status.store(CoordinatorStatus::LEADER_NOT_READY, std::memory_order_release);
    // Thread pool is needed because becoming leader is blocking action, and if we don't succeed to check state of
    // cluster we will try again and again in same thread, thus blocking progress of NuRaft leader election.
    thread_pool_.AddTask([this]() { this->ReconcileClusterState(); });
    return;
  };
}

auto CoordinatorInstance::GetBecomeFollowerCallback() -> std::function<void()> {
  return [this]() {
    status.store(CoordinatorStatus::FOLLOWER, std::memory_order_release);
    thread_pool_.AddTask([this]() {
      spdlog::info("Executing become follower callback in thread {}.", std::this_thread::get_id());
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
      auto lock = std::unique_lock{coord_instance_lock_};
      repl_instances_.clear();
    });
  };
}

auto CoordinatorInstance::FindReplicationInstance(std::string_view replication_instance_name)
    -> std::optional<std::reference_wrapper<ReplicationInstanceConnector>> {
  auto const repl_instance =
      std::ranges::find_if(repl_instances_, [replication_instance_name](ReplicationInstanceConnector const &instance) {
        return instance.InstanceName() == replication_instance_name;
      });

  if (repl_instance != repl_instances_.end()) {
    return std::ref(*repl_instance);
  }

  return std::nullopt;
}

auto CoordinatorInstance::GetLeaderCoordinatorData() const -> std::optional<LeaderCoordinatorData> {
  return raft_state_->GetLeaderCoordinatorData();
}

auto CoordinatorInstance::YieldLeadership() const -> YieldLeadershipStatus {
  if (!raft_state_->IsLeader()) {
    return YieldLeadershipStatus::NOT_LEADER;
  }
  raft_state_->YieldLeadership();
  return YieldLeadershipStatus::SUCCESS;
}

void CoordinatorInstance::UpdateClientConnectors(std::vector<CoordinatorInstanceAux> const &coord_instances_aux) const {
  auto connectors = coordinator_connectors_.Lock();

  for (auto const &coordinator : coord_instances_aux) {
    if (coordinator.id == raft_state_->GetMyCoordinatorId()) {
      continue;
    }
    auto const coord_connector = std::ranges::find_if(
        *connectors, [coordinator_id = coordinator.id](auto &&connector) { return connector.first == coordinator_id; });
    if (coord_connector != connectors->end()) {
      continue;
    }
    spdlog::trace("Creating new connector to coordinator with id {}, on endpoint:{}.", coordinator.id,
                  coordinator.management_server);
    auto mgmt_endpoint = io::network::Endpoint::ParseAndCreateSocketOrAddress(coordinator.management_server);
    MG_ASSERT(mgmt_endpoint.has_value(), "Failed to create management server when creating new coordinator connector.");
    connectors->emplace(connectors->end(), coordinator.id, ManagementServerConfig{std::move(*mgmt_endpoint)});
  }
}

auto CoordinatorInstance::GetCoordinatorsInstanceStatus() const -> std::vector<InstanceStatus> {
  auto const get_coord_role = [](auto const coordinator_id, auto const curr_leader) -> std::string {
    return coordinator_id == curr_leader ? "leader" : "follower";
  };

  auto const stringify_coord_health = [this](auto const coordinator_id) -> std::string {
    if (status.load(std::memory_order_acquire) != CoordinatorStatus::LEADER_READY) {
      return "unknown";
    }

    return raft_state_->CoordLastSuccRespMs(coordinator_id) < instance_down_timeout_sec_ ? kUp.data() : kDown.data();
  };

  auto const coordinators = raft_state_->GetCoordinatorInstancesAux();
  auto const curr_leader_id = raft_state_->GetLeaderId();

  std::vector<InstanceStatus> results;
  results.reserve(coordinators.size());

  for (auto const &coordinator : coordinators) {
    results.emplace_back(InstanceStatus{
        .instance_name = fmt::format("coordinator_{}", coordinator.id),
        .coordinator_server = coordinator.coordinator_server,
        .management_server = coordinator.management_server,
        .bolt_server = raft_state_->GetBoltServer(coordinator.id).value_or(""),
        .cluster_role = get_coord_role(coordinator.id, curr_leader_id),
        .health = stringify_coord_health(coordinator.id),
        .last_succ_resp_ms = raft_state_->CoordLastSuccRespMs(coordinator.id).count(),

    });
  }
  return results;
}

auto CoordinatorInstance::ShowInstancesStatusAsFollower() const -> std::vector<InstanceStatus> {
  spdlog::trace("Processing show instances request as follower.");
  auto instances_status = GetCoordinatorsInstanceStatus();
  auto const stringify_inst_status = [raft_state_ptr = raft_state_.get()](auto &&instance) -> std::string {
    if (raft_state_ptr->IsCurrentMain(instance.config.instance_name)) {
      return "main";
    }
    if (raft_state_ptr->HasMainState(instance.config.instance_name)) {
      return "unknown";
    }
    return "replica";
  };

  auto process_repl_instance_as_follower = [&stringify_inst_status](auto &&instance) -> InstanceStatus {
    return {.instance_name = instance.config.instance_name,
            .management_server = instance.config.ManagementSocketAddress(),  // show non-resolved IP
            .bolt_server = instance.config.BoltSocketAddress(),              // show non-resolved IP
            .cluster_role = stringify_inst_status(instance),
            .health = "unknown"};
  };

  std::ranges::transform(raft_state_->GetDataInstancesContext(), std::back_inserter(instances_status),
                         process_repl_instance_as_follower);
  spdlog::trace("Returning set of instances as follower.");
  return instances_status;
}

auto CoordinatorInstance::ShowInstancesAsLeader() const -> std::optional<std::vector<InstanceStatus>> {
  auto instances_status = GetCoordinatorsInstanceStatus();

  auto const stringify_repl_role = [this](auto &&instance) -> std::string {
    if (!instance.IsAlive()) return "unknown";
    if (raft_state_->IsCurrentMain(instance.InstanceName())) return "main";
    return "replica";
  };

  auto const stringify_repl_health = [](auto &&instance) -> std::string { return instance.IsAlive() ? "up" : "down"; };

  auto process_repl_instance_as_leader = [&stringify_repl_role,
                                          &stringify_repl_health](auto &&instance) -> InstanceStatus {
    return {.instance_name = instance.InstanceName(),
            .management_server = instance.ManagementSocketAddress(),  // show non-resolved IP
            .bolt_server = instance.BoltSocketAddress(),              // show non-resolved IP
            .cluster_role = stringify_repl_role(instance),
            .health = stringify_repl_health(instance),
            .last_succ_resp_ms = instance.LastSuccRespMs().count()};
  };

  auto lock = std::shared_lock{coord_instance_lock_};

  spdlog::trace("Processing show instances request as leader.");

  if (status.load(std::memory_order_acquire) != CoordinatorStatus::LEADER_READY) {
    spdlog::trace("Leader is not ready, returning empty response.");
    return std::nullopt;
  }

  std::ranges::transform(repl_instances_, std::back_inserter(instances_status), process_repl_instance_as_leader);

  spdlog::trace("Returning set of instances as leader.");
  return instances_status;
}

auto CoordinatorInstance::ShowInstance() const -> InstanceStatus {
  metrics::IncrementCounter(metrics::ShowInstance);
  auto const curr_leader_id = raft_state_->GetLeaderId();
  auto const my_context = raft_state_->GetMyCoordinatorInstanceAux();
  std::string const role = std::invoke([curr_leader_id, my_id = raft_state_->GetMyCoordinatorId()] {
    return my_id == curr_leader_id ? "leader" : "follower";
  });

  return InstanceStatus{.instance_name = raft_state_->InstanceName(),
                        .coordinator_server = my_context.coordinator_server,         // show non-resolved IP
                        .management_server = my_context.management_server,           // show non-resolved IP
                        .bolt_server = raft_state_->GetMyBoltServer().value_or(""),  // show non-resolved IP
                        .cluster_role = role};
}

auto CoordinatorInstance::ShowInstances() const -> std::vector<InstanceStatus> {
  metrics::IncrementCounter(metrics::ShowInstances);
  if (auto const leader_results = ShowInstancesAsLeader(); leader_results.has_value()) {
    return *leader_results;
  }

  auto const leader_id = raft_state_->GetLeaderId();
  if (leader_id == raft_state_->GetMyCoordinatorId()) {
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

    auto connector = std::ranges::find_if(
        *connectors, [&leader_id](auto const &local_connector) { return local_connector.first == leader_id; });
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

  auto attempt_cnt{1};
  while (raft_state_->IsLeader()) {
    spdlog::trace("Trying to ensure cluster's healthy state. The coordinator is considered a leader. Attempt: {}.",
                  attempt_cnt++);
    switch (auto const result = ReconcileClusterState_()) {
      case (ReconcileClusterStateStatus::SUCCESS): {
        auto expected = CoordinatorStatus::LEADER_NOT_READY;
        if (!status.compare_exchange_strong(expected, CoordinatorStatus::LEADER_READY, std::memory_order_acq_rel,
                                            std::memory_order_acquire)) {
          if (expected == CoordinatorStatus::FOLLOWER) {
            spdlog::trace(
                "Reconcile cluster state finished successfully but coordinator in the meantime became follower.");
            metrics::IncrementCounter(metrics::FailedToBecomeLeader);
            return ReconcileClusterStateStatus::NOT_LEADER_ANYMORE;
          }
          // We should never get into such state, but we log it for observability.
          spdlog::trace("Reconcile cluster state finished successfully but coordinator is already in state ready.");
          return result;
        }
        spdlog::trace("Reconcile cluster state finished successfully. Leader is ready now.");
        metrics::IncrementCounter(metrics::BecomeLeaderSuccess);
        return result;
      }
      case ReconcileClusterStateStatus::FAIL:
        spdlog::trace("ReconcileClusterState_ failed!");
        metrics::IncrementCounter(metrics::FailedToBecomeLeader);
        break;
      case ReconcileClusterStateStatus::SHUTTING_DOWN:
        spdlog::trace("Stopping reconciliation as coordinator is shutting down.");
        metrics::IncrementCounter(metrics::FailedToBecomeLeader);
        return result;
      case ReconcileClusterStateStatus::NOT_LEADER_ANYMORE: {
        metrics::IncrementCounter(metrics::FailedToBecomeLeader);
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
  auto lock = std::unique_lock{coord_instance_lock_};
  spdlog::trace("Acquired lock in the reconciliation cluster state reset thread {}.", std::this_thread::get_id());
  repl_instances_.clear();

  auto const raft_state_data_instances = raft_state_->GetDataInstancesContext();

  // Reconciliation shouldn't be done on single coordinator
  if (raft_state_->GetCoordinatorInstancesAux().size() == kDisconnectedCluster) {
    return ReconcileClusterStateStatus::SUCCESS;
  }

  // There is nothing to do if we don't have any instances
  if (raft_state_data_instances.empty()) {
    spdlog::trace("Exiting ReconcileClusterState. Didn't get any replication instances.");
    return ReconcileClusterStateStatus::SUCCESS;
  }

  auto current_mains =
      raft_state_data_instances | ranges::views::filter([raft_state_ptr = raft_state_.get()](auto const &instance) {
        return raft_state_ptr->IsCurrentMain(instance.config.instance_name);
      });

  auto const num_mains = std::ranges::distance(current_mains);

  if (num_mains > 1) {
    LOG_FATAL(
        "Found more than one current main. System cannot operate under this assumption, please contact the support.");
  }

  std::ranges::for_each(raft_state_data_instances, [this](auto const &data_instance) {
    auto &instance = repl_instances_.emplace_back(data_instance.config, this, instance_down_timeout_sec_,
                                                  instance_health_check_frequency_sec_);
    instance.StartStateCheck();
  });

  if (num_mains == 1) {
    // If we have alive MAIN instance we expect that the cluster was in the correct state already. We can start
    // frequent checks.
    auto main_instance = std::ranges::begin(current_mains);
    spdlog::trace("Found main instance {}.", main_instance->config.instance_name);
    for (auto &instance : repl_instances_) {
      instance.StartStateCheck();
    }
    return ReconcileClusterStateStatus::SUCCESS;
  }

  if (num_mains == 0) {
    spdlog::trace(
        "No main can be determined from the current state in logs. Trying to find the most up to date instance.");
    switch (TryFailover()) {
      case FailoverStatus::SUCCESS: {
        for (auto &instance : repl_instances_) {
          instance.StartStateCheck();
        }
        spdlog::trace("Exiting ReconcileClusterState_. Cluster is in healthy state.");
        return ReconcileClusterStateStatus::SUCCESS;
      };
      case FailoverStatus::NO_INSTANCE_ALIVE: {
        spdlog::trace("Failover failed because no instance is alive.");
        return ReconcileClusterStateStatus::FAIL;
      };
      case FailoverStatus::RAFT_FAILURE: {
        spdlog::trace("Writing to Raft log failed, reconciliation task will be scheduled.");
        return ReconcileClusterStateStatus::FAIL;
      };
    };
  }

  LOG_FATAL("Unreachable code reached when leadership is changed. Num of mains: {}", num_mains);
}

auto CoordinatorInstance::TryVerifyOrCorrectClusterState() -> ReconcileClusterStateStatus {
  // Follows nomenclature from replication handler where Try<> means doing action from the
  // user query.
  auto expected = CoordinatorStatus::LEADER_READY;
  if (!status.compare_exchange_strong(expected, CoordinatorStatus::LEADER_NOT_READY, std::memory_order_acq_rel,
                                      std::memory_order_acquire)) {
    return ReconcileClusterStateStatus::NOT_LEADER_ANYMORE;
  }
  return ReconcileClusterState();
}

void CoordinatorInstance::ShuttingDown() { is_shutting_down_.store(true, std::memory_order_release); }

auto CoordinatorInstance::TryFailover() const -> FailoverStatus {
  utils::MetricsTimer const timer{metrics::DataFailover_us};
  auto const maybe_most_up_to_date_instance = GetInstanceForFailover();
  if (!maybe_most_up_to_date_instance.has_value()) {
    spdlog::error("Couldn't choose instance for failover, check logs for more details.");
    metrics::IncrementCounter(metrics::NoAliveInstanceFailedFailovers);
    return FailoverStatus::NO_INSTANCE_ALIVE;
  }

  auto const &new_main_name = *maybe_most_up_to_date_instance;
  spdlog::trace("Found new main instance {} while doing failover.", new_main_name);

  auto data_instances = raft_state_->GetDataInstancesContext();

  auto const new_main_uuid = utils::UUID{};
  auto const not_main = [&new_main_name](auto &&instance) { return instance.config.instance_name != new_main_name; };

  for (auto &data_instance : data_instances | ranges::views::filter(not_main)) {
    data_instance.status = ReplicationRole::REPLICA;
    data_instance.instance_uuid = new_main_uuid;
  }

  const auto main_data_instance = std::ranges::find_if(data_instances, [&new_main_name](auto &&data_instance) {
    return data_instance.config.instance_name == new_main_name;
  });

  main_data_instance->instance_uuid = new_main_uuid;
  main_data_instance->status = ReplicationRole::MAIN;

  // NOLINTNEXTLINE
  CoordinatorClusterStateDelta const delta_state{.data_instances_ = std::move(data_instances),
                                                 // NOLINTNEXTLINE
                                                 .current_main_uuid_ = new_main_uuid};

  if (!raft_state_->AppendClusterUpdate(delta_state)) {
    spdlog::error("Aborting failover. Writing to Raft failed.");
    metrics::IncrementCounter(metrics::RaftFailedFailovers);
    return FailoverStatus::RAFT_FAILURE;
  }

  metrics::IncrementCounter(metrics::SuccessfulFailovers);
  return FailoverStatus::SUCCESS;
}

auto CoordinatorInstance::SetReplicationInstanceToMain(std::string_view new_main_name)
    -> SetInstanceToMainCoordinatorStatus {
  auto lock = std::lock_guard{coord_instance_lock_};
  spdlog::trace("Acquired lock to set replication instance to main in thread {}.", std::this_thread::get_id());

  // The coordinator could be in LEADER_NOT_READY state because it restarted and before the restart user called `DEMOTE
  // instance <instance_name>`. The cluster is without the main instance which will forbid ReconcileClusterState from
  // succeeding.
  if (status.load(std::memory_order_acquire) == CoordinatorStatus::FOLLOWER) {
    return SetInstanceToMainCoordinatorStatus::NOT_LEADER;
  }

  if (raft_state_->MainExists()) {
    return SetInstanceToMainCoordinatorStatus::MAIN_ALREADY_EXISTS;
  }

  auto const is_new_main = [new_main_name](auto const &instance) { return instance.InstanceName() == new_main_name; };
  auto new_main = std::ranges::find_if(repl_instances_, is_new_main);

  if (new_main == repl_instances_.end()) {
    spdlog::error("Instance {} not registered. Please register it using REGISTER INSTANCE query.", new_main_name);
    return SetInstanceToMainCoordinatorStatus::NO_INSTANCE_WITH_NAME;
  }

  auto const new_main_uuid = utils::UUID{};

  for (auto const &instance : repl_instances_) {
    if (!is_new_main(instance)) {
      // Even if swapping doesn't succeed here, we will do it again in the callbacks.
      instance.SendSwapAndUpdateUUID(new_main_uuid);
    }
  }

  auto repl_clients_info = repl_instances_ | ranges::views::filter(std::not_fn(is_new_main)) |
                           ranges::views::transform(&ReplicationInstanceConnector::GetReplicationClientInfo) |
                           ranges::to<ReplicationClientsInfo>();

  if (!new_main->SendRpc<PromoteToMainRpc>(new_main_uuid, std::move(repl_clients_info))) {
    return SetInstanceToMainCoordinatorStatus::COULD_NOT_PROMOTE_TO_MAIN;
  }

  if (!new_main->SendRpc<EnableWritingOnMainRpc>()) {
    return SetInstanceToMainCoordinatorStatus::ENABLE_WRITING_FAILED;
  }

  auto data_instances = raft_state_->GetDataInstancesContext();

  auto const not_main_raft = [new_main_name](auto &&instance) {
    return instance.config.instance_name != new_main_name;
  };
  // replicas already have status replica
  for (auto &data_instance : data_instances | ranges::views::filter(not_main_raft)) {
    data_instance.instance_uuid = new_main_uuid;
  }

  auto main_data_instance = std::ranges::find_if(data_instances, [new_main_name](auto &&data_instance) {
    return data_instance.config.instance_name == new_main_name;
  });

  main_data_instance->instance_uuid = new_main_uuid;
  main_data_instance->status = ReplicationRole::MAIN;

  // NOLINTNEXTLINE
  CoordinatorClusterStateDelta const delta_state{.data_instances_ = std::move(data_instances),
                                                 // NOLINTNEXTLINE
                                                 .current_main_uuid_ = new_main_uuid};

  if (!raft_state_->AppendClusterUpdate(delta_state)) {
    spdlog::error("Aborting setting instance to main. Writing to Raft failed.");
    return SetInstanceToMainCoordinatorStatus::RAFT_LOG_ERROR;
  }

  return SetInstanceToMainCoordinatorStatus::SUCCESS;
}

auto CoordinatorInstance::DemoteInstanceToReplica(std::string_view instance_name) -> DemoteInstanceCoordinatorStatus {
  metrics::IncrementCounter(metrics::DemoteInstance);
  auto lock = std::lock_guard{coord_instance_lock_};

  if (status.load(std::memory_order_acquire) != CoordinatorStatus::LEADER_READY) {
    return DemoteInstanceCoordinatorStatus::NOT_LEADER;
  }

  auto const name_matches = [instance_name](auto &&instance) { return instance.InstanceName() == instance_name; };

  auto instance = std::ranges::find_if(repl_instances_, name_matches);
  if (instance == repl_instances_.end()) {
    return DemoteInstanceCoordinatorStatus::NO_INSTANCE_WITH_NAME;
  }

  if (!instance->SendRpc<DemoteMainToReplicaRpc>()) {
    return DemoteInstanceCoordinatorStatus::RPC_FAILED;
  }

  auto cluster_state = raft_state_->GetDataInstancesContext();

  auto data_instance = std::ranges::find_if(cluster_state, [instance_name](auto &&data_instance) {
    return data_instance.config.instance_name == instance_name;
  });
  data_instance->status = ReplicationRole::REPLICA;

  // NOLINTNEXTLINE
  CoordinatorClusterStateDelta const delta_state{.data_instances_ = std::move(cluster_state)};

  if (!raft_state_->AppendClusterUpdate(delta_state)) {
    spdlog::error("Aborting demoting instance. Writing to Raft failed.");
    return DemoteInstanceCoordinatorStatus::RAFT_LOG_ERROR;
  }

  return DemoteInstanceCoordinatorStatus::SUCCESS;
}

auto CoordinatorInstance::RegisterReplicationInstance(DataInstanceConfig const &config)
    -> RegisterInstanceCoordinatorStatus {
  auto lock = std::lock_guard{coord_instance_lock_};

  if (status.load(std::memory_order_acquire) != CoordinatorStatus::LEADER_READY) {
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

  if (config.replication_client_info.replication_mode == replication_coordination_glue::ReplicationMode::STRICT_SYNC) {
    if (std::ranges::any_of(repl_instances_, [](auto &&instance) {
          return instance.GetReplicationClientInfo().replication_mode ==
                 replication_coordination_glue::ReplicationMode::SYNC;
        })) {
      return RegisterInstanceCoordinatorStatus::STRICT_SYNC_AND_SYNC_FORBIDDEN;
    }

  } else if (config.replication_client_info.replication_mode == replication_coordination_glue::ReplicationMode::SYNC) {
    if (std::ranges::any_of(repl_instances_, [](auto &&instance) {
          return instance.GetReplicationClientInfo().replication_mode ==
                 replication_coordination_glue::ReplicationMode::STRICT_SYNC;
        })) {
      return RegisterInstanceCoordinatorStatus::STRICT_SYNC_AND_SYNC_FORBIDDEN;
    }
  }

  auto const curr_main_uuid = raft_state_->GetCurrentMainUUID();

  auto *new_instance =
      &repl_instances_.emplace_back(config, this, instance_down_timeout_sec_, instance_health_check_frequency_sec_);

  // We do this here not under callbacks because we need to add replica to the current main.
  if (!new_instance->SendRpc<DemoteMainToReplicaRpc>()) {
    spdlog::error("Failed to demote instance {} to replica.", config.instance_name);
    repl_instances_.pop_back();
    return RegisterInstanceCoordinatorStatus::RPC_FAILED;
  }

  if (auto const main_name = raft_state_->TryGetCurrentMainName(); main_name.has_value()) {
    auto const maybe_current_main = FindReplicationInstance(*main_name);
    MG_ASSERT(maybe_current_main.has_value(), "Couldn't find instance {} in local storage.", *main_name);

    if (auto const &current_main = maybe_current_main->get();
        !current_main.SendRpc<RegisterReplicaOnMainRpc>(curr_main_uuid, new_instance->GetReplicationClientInfo())) {
      spdlog::error("Failed to register instance {} on main instance {}.", config.instance_name, main_name);
      repl_instances_.pop_back();
      return RegisterInstanceCoordinatorStatus::RPC_FAILED;
    }
  }

  auto data_instances = raft_state_->GetDataInstancesContext();
  data_instances.emplace_back(config, ReplicationRole::REPLICA, curr_main_uuid);

  // NOLINTNEXTLINE
  CoordinatorClusterStateDelta const delta_state{.data_instances_ = std::move(data_instances)};

  if (!raft_state_->AppendClusterUpdate(delta_state)) {
    spdlog::error("Aborting instance registration. Writing to Raft failed.");
    repl_instances_.pop_back();
    return RegisterInstanceCoordinatorStatus::RAFT_LOG_ERROR;
  }

  new_instance->StartStateCheck();

  spdlog::info("Instance {} registered", config.instance_name);
  return RegisterInstanceCoordinatorStatus::SUCCESS;
}

auto CoordinatorInstance::UnregisterReplicationInstance(std::string_view instance_name)
    -> UnregisterInstanceCoordinatorStatus {
  metrics::IncrementCounter(metrics::UnregisterReplInstance);
  if (status.load(std::memory_order_acquire) != CoordinatorStatus::LEADER_READY) {
    return UnregisterInstanceCoordinatorStatus::NOT_LEADER;
  }

  auto lock = std::lock_guard{coord_instance_lock_};
  spdlog::trace("Acquired lock to unregister instance in thread {} 1st time", std::this_thread::get_id());

  auto maybe_instance = FindReplicationInstance(instance_name);
  if (!maybe_instance) {
    return UnregisterInstanceCoordinatorStatus::NO_INSTANCE_WITH_NAME;
  }

  auto &inst_to_remove = maybe_instance->get();

  auto const is_current_main = [this](auto const &instance) {
    return raft_state_->IsCurrentMain(instance.InstanceName()) && instance.IsAlive();
  };

  // Cannot unregister current main
  if (is_current_main(inst_to_remove)) {
    return UnregisterInstanceCoordinatorStatus::IS_MAIN;
  }

  auto curr_main = std::ranges::find_if(repl_instances_, is_current_main);
  // If there is no main in the cluster, the replica cannot be unregistered. We would commit the state without replica
  // in Raft but the in-memory state would still contain it.
  if (curr_main == repl_instances_.end()) {
    return UnregisterInstanceCoordinatorStatus::NO_MAIN;
  }

  auto old_data_instances = raft_state_->GetDataInstancesContext();
  // Intentional copy
  auto new_data_instances = old_data_instances;
  auto const [first, last] = std::ranges::remove_if(new_data_instances, [instance_name](auto const &data_instance) {
    return data_instance.config.instance_name == instance_name;
  });
  new_data_instances.erase(first, last);

  // NOLINTNEXTLINE
  CoordinatorClusterStateDelta const delta_state{.data_instances_ = std::move(new_data_instances)};

  // Append new cluster state. We may need to restore old state if something goes wrong.
  if (!raft_state_->AppendClusterUpdate(delta_state)) {
    return UnregisterInstanceCoordinatorStatus::RAFT_LOG_ERROR;
  }

  auto const name_matches = [instance_name](auto const &instance) { return instance.InstanceName() == instance_name; };

  // The network could be down or the request could fail because of some strange reason. In that case, we try to bring
  // back old raft state
  if (curr_main->SendRpc<UnregisterReplicaRpc>(instance_name)) {
    std::erase_if(repl_instances_, name_matches);
    return UnregisterInstanceCoordinatorStatus::SUCCESS;
  }

  // NOLINTNEXTLINE
  CoordinatorClusterStateDelta const delta_state_revert{.data_instances_ = std::move(old_data_instances)};

  if (!raft_state_->AppendClusterUpdate(delta_state_revert)) {
    LOG_FATAL(
        "Coordinator instances cannot be brought into the consistent state before unregistration started. Please "
        "restart coordinators with fresh data directory and reconnect the cluster. Data on main and replicas will be "
        "preserved.");
  }

  return UnregisterInstanceCoordinatorStatus::RPC_FAILED;
}

auto CoordinatorInstance::RemoveCoordinatorInstance(int coordinator_id) const -> RemoveCoordinatorInstanceStatus {
  metrics::IncrementCounter(metrics::RemoveCoordInstance);
  spdlog::trace("Started removing coordinator instance {}.", coordinator_id);
  auto const coordinator_instances_aux = raft_state_->GetCoordinatorInstancesAux();

  auto const existing_coord = std::ranges::find_if(
      coordinator_instances_aux, [coordinator_id](auto const &coord) { return coord.id == coordinator_id; });

  if (existing_coord == coordinator_instances_aux.end()) {
    return RemoveCoordinatorInstanceStatus::NO_SUCH_ID;
  }

  raft_state_->RemoveCoordinatorInstance(coordinator_id);

  auto coordinator_instances_context = raft_state_->GetCoordinatorInstancesContext();

  auto const num_removed = std::erase_if(coordinator_instances_context, [coordinator_id](auto const &coordinator) {
    return coordinator.id == coordinator_id;
  });

  if (num_removed == 1) {
    spdlog::trace("Removed coordinator {} from local coordinator instance.", coordinator_id);
  } else {
    LOG_FATAL(
        "Couldn't find coordinator {} in local application logs, there was a mistake when starting cluster. Please "
        "remove all your coordinator data directories and reconnect the cluster.",
        coordinator_id);
  }

  // NOLINTNEXTLINE
  CoordinatorClusterStateDelta const delta_state{.coordinator_instances_ = std::move(coordinator_instances_context)};

  // If we managed to remove it from the NuRaft configuration but not to our app logs. If this fails, not good.
  if (!raft_state_->AppendClusterUpdate(delta_state)) {
    LOG_FATAL("Couldn't append application log when removing coordinator {} from the cluster. ", coordinator_id);
  }

  return RemoveCoordinatorInstanceStatus::SUCCESS;
}

auto CoordinatorInstance::AddCoordinatorInstance(CoordinatorInstanceConfig const &config) const
    -> AddCoordinatorInstanceStatus {
  spdlog::trace("Adding coordinator instance {} start in CoordinatorInstance for {}", config.coordinator_id,
                raft_state_->InstanceName());

  auto const bolt_server_to_add = config.bolt_server.SocketAddress();
  auto const id_to_add = config.coordinator_id;

  auto coordinator_instances_context = raft_state_->GetCoordinatorInstancesContext();
  {
    auto const existing_coord = std::ranges::find_if(
        coordinator_instances_context,
        [&bolt_server_to_add](auto const &coord) { return coord.bolt_server == bolt_server_to_add; });

    if (existing_coord != coordinator_instances_context.end()) {
      spdlog::warn(
          "You are trying to set-up a coordinator with the same bolt server as on coordinator {}. That is a valid "
          "option but please double-check that's what you really want. The coordinator will be added so if you want "
          "to undo this action, use 'REMOVE "
          "COORDINATOR' query.",
          existing_coord->id);
    }
  }

  // Adding new coordinator
  if (id_to_add != raft_state_->GetMyCoordinatorId()) {
    auto const coordinator_instances_aux = raft_state_->GetCoordinatorInstancesAux();

    {
      auto const existing_coord = std::ranges::find_if(
          coordinator_instances_aux,
          [coordinator_id = config.coordinator_id](auto const &coord) { return coord.id == coordinator_id; });

      if (existing_coord != coordinator_instances_aux.end()) {
        return AddCoordinatorInstanceStatus::ID_ALREADY_EXISTS;
      }
    }

    {
      auto const existing_coord = std::ranges::find_if(
          coordinator_instances_aux, [mgmt_server = config.management_server.SocketAddress()](auto const &coord) {
            return coord.management_server == mgmt_server;
          });

      if (existing_coord != coordinator_instances_aux.end()) {
        return AddCoordinatorInstanceStatus::MGMT_ENDPOINT_ALREADY_EXISTS;
      }
    }

    {
      auto const existing_coord = std::ranges::find_if(
          coordinator_instances_aux, [coord_server = config.coordinator_server.SocketAddress()](auto const &coord) {
            return coord.coordinator_server == coord_server;
          });

      if (existing_coord != coordinator_instances_aux.end()) {
        return AddCoordinatorInstanceStatus::COORDINATOR_ENDPOINT_ALREADY_EXISTS;
      }
    }

    // We only need to add coordinator instance on Raft level if it's some new instance
    raft_state_->AddCoordinatorInstance(config);

  } else {
    // I am adding myself
    auto const my_aux = raft_state_->GetMyCoordinatorInstanceAux();
    if (config.coordinator_server.SocketAddress() != my_aux.coordinator_server) {
      throw RaftAddServerException(
          "Failed to add server since NuRaft server has been started with different network configuration!");
    }
    if (config.management_server.SocketAddress() != my_aux.management_server) {
      throw RaftAddServerException(
          "Failed to add server since management server has been started with different network configuration!");
    }
  }

  coordinator_instances_context.emplace_back(
      CoordinatorInstanceContext{.id = config.coordinator_id, .bolt_server = bolt_server_to_add});

  // NOLINTNEXTLINE
  CoordinatorClusterStateDelta const delta_state{.coordinator_instances_ = std::move(coordinator_instances_context)};

  // If we managed to add it to the NuRaft configuration but not to our app logs.
  if (!raft_state_->AppendClusterUpdate(delta_state)) {
    LOG_FATAL(
        "Couldn't append application log when adding coordinator {} to the cluster. Please restart your instance with "
        "a fresh data directory and try again. If you already partially connected a cluster, please delete data "
        "directories of other coordinators too.",
        config.coordinator_id);
  }

  return AddCoordinatorInstanceStatus::SUCCESS;
}

auto CoordinatorInstance::SetCoordinatorSetting(std::string_view const setting_name,
                                                std::string_view const setting_value) const
    -> SetCoordinatorSettingStatus {
  constexpr std::array settings{kEnabledReadsOnMain, kSyncFailoverOnly};

  if (std::ranges::find(settings, setting_name) == settings.end()) {
    return SetCoordinatorSettingStatus::UNKNOWN_SETTING;
  }

  bool const value = utils::ToLowerCase(setting_value) == "true"sv;
  CoordinatorClusterStateDelta delta_state;

  if (setting_name == kEnabledReadsOnMain) {
    delta_state.enabled_reads_on_main_ = value;
  } else {
    delta_state.sync_failover_only_ = value;
  }

  if (!raft_state_->AppendClusterUpdate(delta_state)) {
    spdlog::error("Aborting the update of coordinator setting {}. Writing to Raft failed.", setting_name);
    return SetCoordinatorSettingStatus::RAFT_LOG_ERROR;
  }
  return SetCoordinatorSettingStatus::SUCCESS;
}

void CoordinatorInstance::InstanceSuccessCallback(std::string_view instance_name,
                                                  const std::optional<InstanceState> &instance_state) {
  utils::MetricsTimer const timer{metrics::InstanceSuccCallback_us};

  if (status.load(std::memory_order_acquire) != CoordinatorStatus::LEADER_READY) {
    spdlog::trace("Leader is not ready, not executing instance success callback.");
    return;
  }

  auto lock = std::unique_lock{coord_instance_lock_, std::defer_lock};
  if (!lock.try_lock_for(500ms)) {
    spdlog::trace("Failed to acquire lock in InstanceSuccessCallback in 500ms");
    return;
  }
  auto const maybe_instance = FindReplicationInstance(instance_name);
  MG_ASSERT(maybe_instance.has_value(), "Couldn't find instance {} in local storage.", instance_name);
  auto &instance = maybe_instance->get();

  spdlog::trace("Instance {} performing success callback in thread {}.", instance_name, std::this_thread::get_id());

  instance.OnSuccessPing();

  auto const curr_main_uuid = raft_state_->GetCurrentMainUUID();

  if (raft_state_->IsCurrentMain(instance_name)) {
    // According to raft, this is the current MAIN
    // Check if a promotion is needed:
    //  - instance is actually a replica
    //  - instance is main, but has stale state (missed a failover)
    if (!instance_state->is_replica && instance_state->is_writing_enabled && instance_state->uuid &&
        *instance_state->uuid == curr_main_uuid) {
      // Promotion not needed
      return;
    }
    auto const is_not_main = [instance_name](auto &&instance) { return instance.InstanceName() != instance_name; };
    auto repl_clients_info = repl_instances_ | ranges::views::filter(is_not_main) |
                             ranges::views::transform(&ReplicationInstanceConnector::GetReplicationClientInfo) |
                             ranges::to<ReplicationClientsInfo>();

    if (!instance.SendRpc<PromoteToMainRpc>(curr_main_uuid, std::move(repl_clients_info))) {
      spdlog::error("Failed to promote instance to main with new uuid {}. Trying to do failover again.",
                    std::string{curr_main_uuid});
      switch (TryFailover()) {
        case FailoverStatus::SUCCESS: {
          spdlog::trace("Failover successful after failing to promote main instance.");
          break;
        };
        case FailoverStatus::NO_INSTANCE_ALIVE: {
          spdlog::trace("Failover failed because no instance is alive.");
          break;
        };
        case FailoverStatus::RAFT_FAILURE: {
          spdlog::trace("Writing to Raft failed during failover.");
          break;
        };
      };
      return;
    }
  } else {
    // According to raft, the instance should be replica
    if (!instance_state->is_replica) {
      // If instance is not replica, demote it to become replica. If request for demotion failed, return,
      // and you will simply retry on the next ping.
      if (!instance.SendRpc<DemoteMainToReplicaRpc>()) {
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

void CoordinatorInstance::InstanceFailCallback(std::string_view instance_name,
                                               const std::optional<InstanceState> & /*instance_state*/) {
  utils::MetricsTimer const timer{metrics::InstanceFailCallback_us};

  if (status.load(std::memory_order_acquire) != CoordinatorStatus::LEADER_READY) {
    spdlog::trace("Leader is not ready, not executing instance fail callback.");
    return;
  }

  auto lock = std::unique_lock{coord_instance_lock_, std::defer_lock};
  if (!lock.try_lock_for(500ms)) {
    spdlog::trace("Failed to acquire lock in InstanceFailCallback in 500ms");
    return;
  }
  auto const maybe_instance = FindReplicationInstance(instance_name);
  MG_ASSERT(maybe_instance.has_value(), "Couldn't find instance {} in local storage.", instance_name);
  auto &instance = maybe_instance->get();

  spdlog::trace("Instance {} performing fail callback in thread {}.", instance_name, std::this_thread::get_id());
  instance.OnFailPing();

  if (raft_state_->IsCurrentMain(instance_name) && !instance.IsAlive()) {
    spdlog::trace("Cluster without main instance, trying failover.");
    switch (TryFailover()) {
      case FailoverStatus::SUCCESS: {
        spdlog::trace("Failover successful after failing to promote main instance.");
        break;
      };
      case FailoverStatus::NO_INSTANCE_ALIVE: {
        spdlog::trace("Failover failed because no instance is alive.");
        break;
      };
      case FailoverStatus::RAFT_FAILURE: {
        spdlog::trace("Writing to Raft failed during failover.");
        break;
      };
    };
  }

  spdlog::trace("Instance {} finished fail callback.", instance_name);
}

auto CoordinatorInstance::ChooseMostUpToDateInstance(
    std::map<std::string, replication_coordination_glue::InstanceInfo> const &instances_info)
    -> std::optional<std::string> {
  utils::MetricsTimer const timer{metrics::ChooseMostUpToDateInstance_us};

  // Find the instance with the largest system committed timestamp
  auto const largest_sys_ts_instance = std::ranges::max_element(instances_info, [](auto const &lhs, auto const &rhs) {
    return lhs.second.last_committed_system_timestamp < rhs.second.last_committed_system_timestamp;
  });

  if (largest_sys_ts_instance == instances_info.end()) [[unlikely]] {
    spdlog::error("Couldn't retrieve history for any instance. Failover won't be performed.");
    return std::nullopt;
  }

  spdlog::trace("The instance with the newest system committed timestamp is {}", largest_sys_ts_instance->first);

  // db_uuid -> vector<std::pair<instance_name, latest_durable_timestamp>>
  std::map<std::string, std::vector<std::pair<std::string, uint64_t>>> dbs_info;

  // Use only DBs from the instance with the largest committed system timestamps
  for (auto const &largest_sys_ts_instance_dbs_info = largest_sys_ts_instance->second.dbs_info;
       auto const &[db_uuid, _] : largest_sys_ts_instance_dbs_info) {
    dbs_info.try_emplace(db_uuid);
  }

  // Pre-process received failover data
  for (auto const &[instance_name, instance_info] : instances_info) {
    for (auto const &[db_uuid, ldt] : instance_info.dbs_info) {
      if (auto db_it = dbs_info.find(db_uuid); db_it != dbs_info.end()) {
        db_it->second.emplace_back(instance_name, ldt);
      }
    }
  }

  // Instance name -> cnt on how many DBs is instance the newest
  std::map<std::string, uint64_t> total_instances_counter;
  // Instance name -> sum of timestamps on all DBs
  std::map<std::string, uint64_t> total_instances_sum;

  auto const find_newest_instances_for_db =
      [&total_instances_sum](std::vector<std::pair<std::string, uint64_t>> const &db_info)
      -> std::pair<std::vector<std::string>, uint64_t> {
    // There could be multiple instances with the same timestamp, that's why we are returning vector
    std::vector<std::string> newest_db_instances;
    uint64_t curr_ldt{0};
    // Loop through instances
    for (auto const &[instance_name, latest_durable_timestamp] : db_info) {
      // Sum timestamps for each instance
      if (auto [instance_it, inserted] = total_instances_sum.try_emplace(instance_name, latest_durable_timestamp);
          !inserted) {
        instance_it->second += latest_durable_timestamp;
      }

      // If no new instance exists, or instance has newer timestamp -> do the update
      if (newest_db_instances.empty() || curr_ldt < latest_durable_timestamp) {
        newest_db_instances.clear();
        newest_db_instances.emplace_back(instance_name);
        curr_ldt = latest_durable_timestamp;
      } else if (curr_ldt == latest_durable_timestamp) {
        // Otherwise, we have more instances with the max timestamp, add it to the vector
        newest_db_instances.emplace_back(instance_name);
      }
    }
    return {newest_db_instances, curr_ldt};
  };

  auto const update_instances_counter = [&total_instances_counter](
                                            std::vector<std::string> const &newest_db_instances) {
    for (auto const &db_newest_instance_name : newest_db_instances) {
      if (auto [instance_it, inserted] = total_instances_counter.try_emplace(db_newest_instance_name, 1); !inserted) {
        instance_it->second++;
      }
    }
  };

  // Process each DB
  for (auto const &[db_uuid, db_info] : dbs_info) {
    spdlog::trace("Trying to find newest instance for db with uuid {}", db_uuid);
    if (auto const [newest_db_instances, curr_ldt] = find_newest_instances_for_db(db_info);
        newest_db_instances.empty()) {
      spdlog::error("Couldn't find newest instance for db with uuid {}", db_uuid);
    } else {
      spdlog::info("The latest durable timestamp is {} for db with uuid {}. The following instances have it {}",
                   curr_ldt, db_uuid, fmt::join(newest_db_instances, ", "));
      update_instances_counter(newest_db_instances);
    }
  }

  std::optional<std::pair<std::string, uint64_t>> newest_instance;
  for (auto const &[instance_name, cnt_newest_dbs] : total_instances_counter) {
    // If better on more DBs, update currently the best
    if (!newest_instance.has_value() || newest_instance->second < cnt_newest_dbs) {
      newest_instance.emplace(instance_name, cnt_newest_dbs);
    } else if (newest_instance->second == cnt_newest_dbs) {
      // Instances are the best over the same number of instances, let the sum of timestamps decide
      spdlog::info("Instances {} and {} are most up to date on the same number of instances.", instance_name,
                   newest_instance->first);
      if (total_instances_sum[instance_name] > total_instances_sum[newest_instance->first]) {
        spdlog::info("Instance {} has the total sum of timestamps larger than {}. It will be considered newer.",
                     instance_name, newest_instance->first);
        newest_instance.emplace(instance_name, cnt_newest_dbs);
      }
    }
  }

  if (newest_instance.has_value()) {
    spdlog::info("The newest instance is {}", newest_instance->first);
    return newest_instance->first;
  }
  return std::nullopt;
}

auto CoordinatorInstance::GetRoutingTable() const -> RoutingTable { return raft_state_->GetRoutingTable(); }

auto CoordinatorInstance::GetInstanceForFailover() const -> std::optional<std::string> {
  utils::MetricsTimer const timer{metrics::GetHistories_us};

  if (repl_instances_.empty()) {
    return std::nullopt;
  }

  auto const get_instance_info = [](auto const &instance) {
    return instance.GetClient().SendGetDatabaseHistoriesRpc();
  };

  // instance_name -> InstanceInfo
  std::map<std::string, replication_coordination_glue::InstanceInfo> instances_info;

  auto const sync_failover_only = raft_state_->GetSyncFailoverOnly();
  auto const data_instances = raft_state_->GetDataInstancesContext();

  for (auto const &instance : repl_instances_) {
    bool const skip_instance = [instance_name = instance.InstanceName(), sync_failover_only, &data_instances]() {
      // if sync failover is false then ASYNC instances can also be used for failover
      if (!sync_failover_only) {
        return false;
      }
      // Since there is only a small number of instances in the cluster, this search is cheap
      auto const raft_instance = std::ranges::find_if(data_instances, [instance_name](auto const &local_instance) {
        return local_instance.config.instance_name == instance_name;
      });
      MG_ASSERT(raft_instance != data_instances.cend(),
                "In-memory instance not saved in the Raft. Please submit this bug since this isn't intended state.");
      return raft_instance->config.replication_client_info.replication_mode !=
             replication_coordination_glue::ReplicationMode::SYNC;
    }();

    if (skip_instance) {
      spdlog::info("Skipping instance {} for a failover since was registered as async replica",
                   instance.InstanceName());
      continue;
    }

    if (auto maybe_instance_info = get_instance_info(instance); maybe_instance_info.has_value()) {
      instances_info.emplace(instance.InstanceName(), std::move(*maybe_instance_info));
    } else {
      spdlog::error("Couldn't retrieve failover info for instance {}", instance.InstanceName());
    }
  }

  return ChooseMostUpToDateInstance(instances_info);
}

auto CoordinatorInstance::ShowCoordinatorSettings() const -> std::vector<std::pair<std::string, std::string>> {
  std::vector<std::pair<std::string, std::string>> settings{
      std::pair{std::string(kEnabledReadsOnMain), raft_state_->GetEnabledReadsOnMain() ? "true" : "false"},
      std::pair{std::string(kSyncFailoverOnly), raft_state_->GetSyncFailoverOnly() ? "true" : "false"},
  };
  return settings;
}
}  // namespace memgraph::coordination
#endif
