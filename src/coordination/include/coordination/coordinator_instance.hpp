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

struct NewMainRes {
  std::string most_up_to_date_instance;
  std::string latest_epoch;
  uint64_t latest_commit_timestamp;
};
using InstanceNameDbHistories = std::pair<std::string, replication_coordination_glue::DatabaseHistories>;

class CoordinatorInstance {
 public:
  CoordinatorInstance();

  [[nodiscard]] auto RegisterReplicationInstance(CoordinatorClientConfig const &config)
      -> RegisterInstanceCoordinatorStatus;
  [[nodiscard]] auto UnregisterReplicationInstance(std::string_view instance_name)
      -> UnregisterInstanceCoordinatorStatus;

  [[nodiscard]] auto SetReplicationInstanceToMain(std::string_view instance_name) -> SetInstanceToMainCoordinatorStatus;

  auto ShowInstances() const -> std::vector<InstanceStatus>;

  auto TryFailover() -> void;

  auto AddCoordinatorInstance(uint32_t raft_server_id, uint32_t raft_port, std::string_view raft_address) -> void;

  static auto ChooseMostUpToDateInstance(std::span<InstanceNameDbHistories> histories) -> NewMainRes;

 private:
  HealthCheckClientCallback client_succ_cb_, client_fail_cb_;

  auto OnRaftCommitCallback(TRaftLog const &log_entry, RaftLogAction log_action) -> void;

  auto FindReplicationInstance(std::string_view replication_instance_name) -> ReplicationInstance &;

  void MainFailCallback(std::string_view);

  void MainSuccessCallback(std::string_view);

  void ReplicaSuccessCallback(std::string_view);

  void ReplicaFailCallback(std::string_view);

  auto IsMain(std::string_view instance_name) const -> bool;
  auto IsReplica(std::string_view instance_name) const -> bool;

  // NOTE: Must be std::list because we rely on pointer stability.
  // Leader and followers should both have same view on repl_instances_
  std::list<ReplicationInstance> repl_instances_;
  mutable utils::ResourceLock coord_instance_lock_{};

  RaftState raft_state_;
};

}  // namespace memgraph::coordination
#endif
