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

  static void MainFailCallback(CoordinatorInstance *self, std::string_view repl_instance_name,
                               std::unique_lock<utils::ResourceLock> lock) {
    MG_ASSERT(lock.owns_lock(), "Callback doesn't own lock");
    spdlog::trace("Instance {} performing main failure callback", repl_instance_name);
    auto &repl_instance = self->FindReplicationInstance(repl_instance_name);
    repl_instance.OnFailPing();
    const auto &repl_instance_uuid = repl_instance.GetMainUUID();
    MG_ASSERT(repl_instance_uuid.has_value(), "Instance must have uuid set");

    if (!repl_instance.IsAlive() && self->GetMainUUID() == repl_instance_uuid.value()) {
      spdlog::info("Cluster without main instance, trying automatic failover");
      self->TryFailover();  // TODO: (andi) Initiate failover
    }
  }

  static void MainSuccessCallback(CoordinatorInstance *self, std::string_view repl_instance_name,
                                  std::unique_lock<utils::ResourceLock> lock) {
    MG_ASSERT(lock.owns_lock(), "Callback doesn't own lock");
    spdlog::trace("Instance {} performing main successful callback", repl_instance_name);

    auto &repl_instance = self->FindReplicationInstance(repl_instance_name);

    if (repl_instance.IsAlive()) {
      repl_instance.OnSuccessPing();
      return;
    }

    const auto &repl_instance_uuid = repl_instance.GetMainUUID();
    MG_ASSERT(repl_instance_uuid.has_value(), "Instance must have uuid set.");

    auto const curr_main_uuid = self->GetMainUUID();
    if (curr_main_uuid == repl_instance_uuid.value()) {
      if (!repl_instance.EnableWritingOnMain()) {
        spdlog::error("Failed to enable writing on main instance {}", repl_instance_name);
        return;
      }

      repl_instance.OnSuccessPing();
      return;
    }

    if (repl_instance.DemoteToReplica(self->replica_succ_cb_, self->replica_fail_cb_)) {
      repl_instance.OnSuccessPing();
      spdlog::info("Instance {} demoted to replica", repl_instance_name);
    } else {
      spdlog::error("Instance {} failed to become replica", repl_instance_name);
      return;
    }

    if (!repl_instance.SendSwapAndUpdateUUID(curr_main_uuid)) {
      spdlog::error(fmt::format("Failed to swap uuid for demoted main instance {}", repl_instance.InstanceName()));
      return;
    }
  }

  static void ReplicaSuccessCallback(CoordinatorInstance *self, std::string_view repl_instance_name,
                                     std::unique_lock<utils::ResourceLock> lock) {
    MG_ASSERT(lock.owns_lock(), "Callback doesn't own lock");
    auto &repl_instance = self->FindReplicationInstance(repl_instance_name);
    if (!repl_instance.IsReplica()) {
      spdlog::error("Aborting replica callback since instance {} is not replica anymore", repl_instance_name);
      return;
    }
    spdlog::trace("Instance {} performing replica successful callback", repl_instance_name);
    // We need to get replicas UUID from time to time to ensure replica is listening to correct main
    // and that it didn't go down for less time than we could notice
    // We need to get id of main replica is listening to
    // and swap if necessary
    if (!repl_instance.EnsureReplicaHasCorrectMainUUID(self->GetMainUUID())) {
      spdlog::error("Failed to swap uuid for replica instance {} which is alive", repl_instance.InstanceName());
      return;
    }

    repl_instance.OnSuccessPing();
  }

  static void ReplicaFailCallback(CoordinatorInstance *self, std::string_view repl_instance_name,
                                  std::unique_lock<utils::ResourceLock> lock) {
    MG_ASSERT(lock.owns_lock(), "Callback doesn't own lock");
    auto &repl_instance = self->FindReplicationInstance(repl_instance_name);
    if (!repl_instance.IsReplica()) {
      spdlog::error("Aborting replica fail callback since instance {} is not replica anymore", repl_instance_name);
      return;
    }
    spdlog::trace("Instance {} performing replica failure callback", repl_instance_name);
    repl_instance.OnFailPing();
  }

 private:
  HealthCheckInstanceCallback main_succ_cb_, main_fail_cb_, replica_succ_cb_, replica_fail_cb_;
  HealthCheckClientCallback client_succ_cb_, client_fail_cb_;

  // NOTE: Must be std::list because we rely on pointer stability
  std::list<ReplicationInstance> repl_instances_;
  mutable utils::ResourceLock coord_instance_lock_{};

  utils::UUID main_uuid_;

  RaftState raft_state_;
};

}  // namespace memgraph::coordination
#endif
