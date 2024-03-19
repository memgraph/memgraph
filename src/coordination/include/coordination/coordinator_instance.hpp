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

#include "coordination/coordinator_server.hpp"
#include "coordination/instance_status.hpp"
#include "coordination/raft_state.hpp"
#include "coordination/register_main_replica_coordinator_status.hpp"
#include "coordination/replication_instance.hpp"
#include "utils/resource_lock.hpp"
#include "utils/rw_lock.hpp"
#include "utils/thread_pool.hpp"

#include <list>

namespace memgraph::coordination {

using RoutingTable = std::vector<std::pair<std::vector<std::string>, std::string>>;

struct NewMainRes {
  std::string most_up_to_date_instance;
  std::string latest_epoch;
  uint64_t latest_commit_timestamp;
};
using InstanceNameDbHistories = std::pair<std::string, replication_coordination_glue::DatabaseHistories>;

class CoordinatorInstance {
 public:
  CoordinatorInstance();
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

  auto AddCoordinatorInstance(coordination::CoordinatorToCoordinatorConfig const &config) -> void;

  auto GetRoutingTable(std::map<std::string, std::string> const &routing) -> RoutingTable;

  static auto ChooseMostUpToDateInstance(std::span<InstanceNameDbHistories> histories) -> NewMainRes;

  auto HasMainState(std::string_view instance_name) const -> bool;

  auto HasReplicaState(std::string_view instance_name) const -> bool;

 private:
  auto FindReplicationInstance(std::string_view replication_instance_name) -> ReplicationInstance &;

  void MainFailCallback(std::string_view);

  void MainSuccessCallback(std::string_view);

  void ReplicaSuccessCallback(std::string_view);

  void ReplicaFailCallback(std::string_view);

  HealthCheckClientCallback client_succ_cb_, client_fail_cb_;
  // NOTE: Must be std::list because we rely on pointer stability.
  std::list<ReplicationInstance> repl_instances_;
  mutable utils::ResourceLock coord_instance_lock_{};

  // Thread pool needs to be constructed before raft state as raft state can call thread pool
  utils::ThreadPool thread_pool_;

  // When instance is becoming follower, we need to stop frequent checks as soon as possible
  std::atomic<bool> is_shutting_down_{false};

  RaftState raft_state_;
};

}  // namespace memgraph::coordination
#endif
