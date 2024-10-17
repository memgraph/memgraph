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

enum class FailoverStatus : uint8_t {
  SUCCESS,
  FAILURE_LOCK_CLOSED,
  FAILURE_LOCK_OPENED,
  NO_INSTANCE_ALIVE,
};

using InstanceNameDbHistories = std::pair<std::string, replication_coordination_glue::DatabaseHistories>;

class CoordinatorInstance {
 public:
  explicit CoordinatorInstance(CoordinatorInstanceInitConfig const &config);
  CoordinatorInstance(CoordinatorInstance const &) = delete;
  CoordinatorInstance &operator=(CoordinatorInstance const &) = delete;
  CoordinatorInstance(CoordinatorInstance &&) noexcept = delete;
  CoordinatorInstance &operator=(CoordinatorInstance &&) noexcept = delete;
  ~CoordinatorInstance();

  [[nodiscard]] auto RegisterReplicationInstance(CoordinatorToReplicaConfig const &config)
      -> RegisterInstanceCoordinatorStatus;
  [[nodiscard]] auto UnregisterReplicationInstance(std::string_view instance_name)
      -> UnregisterInstanceCoordinatorStatus;

  [[nodiscard]] auto SetReplicationInstanceToMain(std::string_view instance_name) -> SetInstanceToMainCoordinatorStatus;

  // TODO: (andi) These shouldn't all be public.
  auto ShowInstances() const -> std::vector<InstanceStatus>;

  auto ShowInstancesAsLeader() const -> std::vector<InstanceStatus>;

  auto ShowInstancesStatusAsFollower() const -> std::vector<InstanceStatus>;

  // Finds most up to date instance that could become new main. Only alive instances are taken into account.
  [[nodiscard]] auto TryFailover() -> FailoverStatus;

  auto AddCoordinatorInstance(CoordinatorToCoordinatorConfig const &config) -> AddCoordinatorInstanceStatus;

  auto GetRoutingTable() const -> RoutingTable;

  static auto GetMostUpToDateInstanceFromHistories(std::list<ReplicationInstanceConnector> &instances)
      -> std::optional<std::string>;

  static auto ChooseMostUpToDateInstance(std::span<InstanceNameDbHistories> histories) -> std::optional<NewMainRes>;

  auto GetLeaderCoordinatorData() const -> std::optional<CoordinatorToCoordinatorConfig>;

  auto DemoteInstanceToReplica(std::string_view instance_name) -> DemoteInstanceCoordinatorStatus;

  auto TryVerifyOrCorrectClusterState() -> ReconcileClusterStateStatus;

  auto ReconcileClusterState() -> ReconcileClusterStateStatus;

  void ShuttingDown();

  void AddOrUpdateClientConnectors(std::vector<CoordinatorToCoordinatorConfig> const &configs);

  auto GetCoordinatorToCoordinatorConfigs() const -> std::vector<CoordinatorToCoordinatorConfig>;

  void InstanceSuccessCallback(std::string_view instance_name, std::optional<InstanceState> instance_state);
  void InstanceFailCallback(std::string_view instance_name, std::optional<InstanceState> instance_state);

 private:
  auto FindReplicationInstance(std::string_view replication_instance_name) -> ReplicationInstanceConnector &;
  auto ReconcileClusterState_() -> ReconcileClusterStateStatus;

  // When a coordinator is becoming a leader, we could be in several situations:
  // 1. Whole cluster was ok, lock was closed, we will find current main. Only last leader probably died.
  //    In that case we don't need to do anything except start state checks.
  // 2. We could be in situation where the lock is opened. That means one of steps in the failover failed to
  //    execute or something failed while we were registering instance, setting instance to main or unregistering
  //    instance. In that case we should reconcile cluster state, which means:
  //    1. close the lock.
  //    2. find main = TryFailover.
  //    3. close the lock.
  // 3. TODO: (Can some other state occur?) What happens when something in registration or setting instance to main
  // fails?
  auto GetBecomeLeaderCallback() -> std::function<void()>;

  auto GetBecomeFollowerCallback() -> std::function<void()>;

  auto GetCoordinatorsInstanceStatus() const -> std::vector<InstanceStatus>;

  // Raft updates leadership before callback is executed. IsLeader() can return true, but
  // leader callback or reconcile cluster state haven't yet be executed. This flag tracks if coordinator is set up to
  // accept queries.
  std::atomic<bool> is_leader_ready_{false};
  std::atomic<bool> is_shutting_down_{false};
  // NOTE: Must be std::list because we rely on pointer stability.
  std::list<ReplicationInstanceConnector> repl_instances_;
  mutable utils::ResourceLock coord_instance_lock_{};

  std::unique_ptr<RaftState> raft_state_;

  // Thread pool must be destructed first, because there is a possibility we are doing reconcile cluster state in thread
  // while coordinator is destructed
  utils::ThreadPool thread_pool_{1};

  CoordinatorInstanceManagementServer coordinator_management_server_;
  mutable utils::Synchronized<std::list<std::pair<uint32_t, CoordinatorInstanceConnector>>, utils::SpinLock>
      coordinator_connectors_;
};

}  // namespace memgraph::coordination
#endif
