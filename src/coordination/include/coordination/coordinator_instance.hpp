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

  auto ShowInstances() const -> std::vector<InstanceStatus>;

  auto ShowInstancesAsLeader() const -> std::vector<InstanceStatus>;

  auto ShowInstancesStatusAsFollower() const -> std::vector<InstanceStatus>;

  auto TryFailover() -> void;

  auto AddCoordinatorInstance(CoordinatorToCoordinatorConfig const &config) -> AddCoordinatorInstanceStatus;

  auto GetRoutingTable() const -> RoutingTable;

  static auto ChooseMostUpToDateInstance(std::span<InstanceNameDbHistories> histories) -> NewMainRes;

  auto HasMainState(std::string_view instance_name) const -> bool;

  auto HasReplicaState(std::string_view instance_name) const -> bool;

  auto GetLeaderCoordinatorData() const -> std::optional<CoordinatorToCoordinatorConfig>;

  auto DemoteInstanceToReplica(std::string_view instance_name) -> DemoteInstanceCoordinatorStatus;

  auto TryVerifyOrCorrectClusterState() -> ReconcileClusterStateStatus;

  auto ReconcileClusterState() -> ReconcileClusterStateStatus;

  auto IsLeader() const -> bool;

  void ShuttingDown();

  void AddOrUpdateClientConnectors(std::vector<CoordinatorToCoordinatorConfig> const &configs);

  auto GetRaftState() -> RaftState &;
  auto GetRaftState() const -> RaftState const &;

  static auto GetSuccessCallbackTypeName(ReplicationInstanceConnector const &instance) -> std::string_view;

  static auto GetFailCallbackTypeName(ReplicationInstanceConnector const &instance) -> std::string_view;

 private:
  auto FindReplicationInstance(std::string_view replication_instance_name) -> ReplicationInstanceConnector &;

  void MainFailCallback(std::string_view);

  void MainSuccessCallback(std::string_view);

  void ReplicaSuccessCallback(std::string_view);

  void ReplicaFailCallback(std::string_view);

  // Possible situations at the end of execution:
  // 1. We are the leader but the lock is not opened -> All went well, no more actions needed.
  // 2. If we aren't leader anymore -> BecomeFollowerCallback will get triggered, all callbacks will stop, new leader
  // will bring raft state into consistent state.
  // 3. If we are leader and the lock is still opened, we could be in several situations:
  //   a) Instance which we want to demote died. In that case we want to close the lock and we will repeat the action
  //   when DemoteSuccessCallback gets executed again.
  //   b) We cannot write to Raft log because we lost majority. We will probably not be leader anymore and hence same
  //   situation as number 2. c) We are still the leader but writing to Raft log failed because of some reason. Try to
  //   AppendCloseLock. If we succeed good, next execution of DemoteSuccessCallback can try again. If not, something is
  //   wrong and we are in some unknown state.
  void DemoteSuccessCallback(std::string_view repl_instance_name);

  void DemoteFailCallback(std::string_view repl_instance_name);

  auto ReconcileClusterState_() -> ReconcileClusterStateStatus;

  auto GetBecomeLeaderCallback() -> std::function<void()>;
  auto GetBecomeFollowerCallback() -> std::function<void()>;

  auto GetCoordinatorsInstanceStatus() const -> std::vector<InstanceStatus>;

  HealthCheckClientCallback client_succ_cb_, client_fail_cb_;
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
