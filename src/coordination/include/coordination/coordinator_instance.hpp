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

class CoordinatorInstance {
 public:
  CoordinatorInstance();

  [[nodiscard]] auto RegisterReplicationInstance(CoordinatorClientConfig config) -> RegisterInstanceCoordinatorStatus;
  [[nodiscard]] auto UnregisterReplicationInstance(std::string instance_name) -> UnregisterInstanceCoordinatorStatus;

  [[nodiscard]] auto SetReplicationInstanceToMain(std::string instance_name) -> SetInstanceToMainCoordinatorStatus;

  auto ShowInstances() const -> std::vector<InstanceStatus>;

  auto TryFailover() -> void;

  auto AddCoordinatorInstance(uint32_t raft_server_id, uint32_t raft_port, std::string raft_address) -> void;

  auto GetMainUUID() const -> utils::UUID;

  auto SetMainUUID(utils::UUID new_uuid) -> void;

  auto FindReplicationInstance(std::string_view replication_instance_name) -> ReplicationInstance &;

  void MainFailCallback(std::string_view repl_instance_name, std::unique_lock<utils::ResourceLock> lock);

  void MainSuccessCallback(std::string_view repl_instance_name, std::unique_lock<utils::ResourceLock> lock);

  void ReplicaSuccessCallback(std::string_view repl_instance_name, std::unique_lock<utils::ResourceLock> lock);

  void ReplicaFailCallback(std::string_view repl_instance_name, std::unique_lock<utils::ResourceLock> lock);

 private:
  HealthCheckClientCallback client_succ_cb_, client_fail_cb_;

  // NOTE: Must be std::list because we rely on pointer stability
  std::list<ReplicationInstance> repl_instances_;
  mutable utils::ResourceLock coord_instance_lock_{};

  utils::UUID main_uuid_;

  RaftState raft_state_;
};

}  // namespace memgraph::coordination
#endif
