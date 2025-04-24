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

#pragma once

#ifdef MG_ENTERPRISE

#include <atomic>
#include <functional>
#include <memory>
#include <optional>
#include <string_view>

#include "coordination/coordinator_communication_config.hpp"
#include "coordination/coordinator_instance_connector.hpp"
#include "coordination/coordinator_instance_management_server.hpp"
#include "coordination/data_instance_management_server.hpp"
#include "coordination/instance_status.hpp"
#include "coordination/raft_state.hpp"
#include "coordination/register_main_replica_coordinator_status.hpp"
#include "coordination/replication_instance_client.hpp"
#include "coordination/replication_instance_connector.hpp"
#include "utils/resource_lock.hpp"
#include "utils/thread_pool.hpp"

#include <list>

namespace memgraph::coordination {

struct NewMainRes {
  std::string instance_name;
  uint64_t latest_durable_timestamp;
};

enum class FailoverStatus : uint8_t {
  SUCCESS,
  RAFT_FAILURE,
  NO_INSTANCE_ALIVE,
};

enum class CoordinatorStatus : uint8_t { FOLLOWER, LEADER_NOT_READY, LEADER_READY };

using InstanceNameDbHistories = std::pair<std::string, replication_coordination_glue::InstanceInfo>;

class CoordinatorInstance {
 public:
  explicit CoordinatorInstance(CoordinatorInstanceInitConfig const &config);
  CoordinatorInstance(CoordinatorInstance const &) = delete;
  CoordinatorInstance &operator=(CoordinatorInstance const &) = delete;
  CoordinatorInstance(CoordinatorInstance &&) noexcept = delete;
  CoordinatorInstance &operator=(CoordinatorInstance &&) noexcept = delete;
  ~CoordinatorInstance();

  // We don't need to open lock and close the lock since we need only one writing to raft log here.
  // If some of the actions fail like sending rpc, demoting or rpc failed, we clear in-memory structures that we have.
  // If writing to raft succeeds, we know what everything up to that point passed so all good.
  [[nodiscard]] auto RegisterReplicationInstance(DataInstanceConfig const &config) -> RegisterInstanceCoordinatorStatus;

  // Here we reverse logic from RegisterReplicationInstance. 1st we write to raft log, and then we try to unregister
  // replication instance from in-memory structures. If raft passes and some of rpc actions or deletions fails, user
  // should repeat the action. Instance will be deleted twice from raft log but since that action is idempotent, no
  // failure will actually happen.
  [[nodiscard]] auto UnregisterReplicationInstance(std::string_view instance_name)
      -> UnregisterInstanceCoordinatorStatus;

  // The logic here is that as long as we didn't set uuid for the whole cluster, actions will be reverted on
  // instances on the next state check.
  [[nodiscard]] auto SetReplicationInstanceToMain(std::string_view new_main_name) -> SetInstanceToMainCoordinatorStatus;

  // If user demotes main to replica, cluster will be without main instance. User should then call
  // TryVerifyOrCorrectClusterState or SetReplicationInstanceToMain. The logic here is that as long as we didn't set
  // uuid for the whole cluster, actions will be reverted on instances on the next state check.
  [[nodiscard]] auto DemoteInstanceToReplica(std::string_view instance_name) -> DemoteInstanceCoordinatorStatus;

  [[nodiscard]] auto TryVerifyOrCorrectClusterState() -> ReconcileClusterStateStatus;

  auto ShowInstance() const -> InstanceStatus;

  auto ShowInstances() const -> std::vector<InstanceStatus>;

  auto ShowInstancesAsLeader() const -> std::optional<std::vector<InstanceStatus>>;

  // Finds most up-to-date instance that could become new main. Only alive instances are taken into account.
  [[nodiscard]] auto TryFailover() const -> FailoverStatus;

  auto AddCoordinatorInstance(CoordinatorInstanceConfig const &config) const -> AddCoordinatorInstanceStatus;

  auto RemoveCoordinatorInstance(int coordinator_id) const -> RemoveCoordinatorInstanceStatus;

  auto GetRoutingTable() const -> RoutingTable;

  auto GetInstanceForFailover() const -> std::optional<std::string>;

  static auto ChooseMostUpToDateInstance(
      std::map<std::string, replication_coordination_glue::InstanceInfo> const &instances_info)
      -> std::optional<std::string>;

  auto GetLeaderCoordinatorData() const -> std::optional<LeaderCoordinatorData>;

  auto YieldLeadership() const -> YieldLeadershipStatus;

  auto ReconcileClusterState() -> ReconcileClusterStateStatus;

  void ShuttingDown();

  void InstanceSuccessCallback(std::string_view instance_name, const std::optional<InstanceState> &instance_state);
  void InstanceFailCallback(std::string_view instance_name, const std::optional<InstanceState> &instance_state);

  void UpdateClientConnectors(std::vector<CoordinatorInstanceAux> const &coord_instances_aux) const;

 private:
  auto FindReplicationInstance(std::string_view replication_instance_name)
      -> std::optional<std::reference_wrapper<ReplicationInstanceConnector>>;
  auto ReconcileClusterState_() -> ReconcileClusterStateStatus;
  auto ShowInstancesStatusAsFollower() const -> std::vector<InstanceStatus>;

  // When a coordinator is becoming a leader, we could be in several situations:
  // 1. Whole cluster was ok, lock was closed, we will find current main. Only last leader probably died.
  //    In that case we don't need to do anything except start state checks.
  // 2. We could be in situation where the lock is opened. That means one of steps in the failover failed to
  //    execute or something failed while we were registering instance, setting instance to main or unregistering
  //    instance. In that case we should reconcile cluster state, which means:
  //    1. close the lock.
  //    2. find main = TryFailover.
  //    3. close the lock.
  auto GetBecomeLeaderCallback() -> std::function<void()>;

  auto GetBecomeFollowerCallback() -> std::function<void()>;

  auto GetCoordinatorsInstanceStatus() const -> std::vector<InstanceStatus>;

  // Raft updates leadership before callback is executed. IsLeader() can return true, but
  // leader callback or reconcile cluster state haven't yet be executed. This flag tracks if coordinator is set up to
  // accept queries.
  std::atomic<CoordinatorStatus> status{CoordinatorStatus::FOLLOWER};
  std::atomic<bool> is_shutting_down_{false};

  std::chrono::seconds instance_down_timeout_sec_{5};
  std::chrono::seconds instance_health_check_frequency_sec_{1};
  // NOTE: Must be std::list because we rely on pointer stability.
  std::list<ReplicationInstanceConnector> repl_instances_;
  mutable utils::ResourceLock coord_instance_lock_{};

  std::unique_ptr<RaftState> raft_state_;

  // Thread pool must be destructed first, because there is a possibility we are doing reconcile cluster state in thread
  // while coordinator is destructed
  utils::ThreadPool thread_pool_{1};

  CoordinatorInstanceManagementServer coordinator_management_server_;
  mutable utils::Synchronized<std::list<std::pair<int32_t, CoordinatorInstanceConnector>>, utils::SpinLock>
      coordinator_connectors_;
};

}  // namespace memgraph::coordination
#endif
