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

#include "coordination/coordinator_client.hpp"
#include "coordination/coordinator_exceptions.hpp"
#include "replication_coordination_glue/role.hpp"

#include "utils/resource_lock.hpp"
#include "utils/result.hpp"
#include "utils/uuid.hpp"

#include <libnuraft/nuraft.hxx>

#include <functional>
namespace memgraph::coordination {

class CoordinatorInstance;
class ReplicationInstance;

using HealthCheckInstanceCallback = void (CoordinatorInstance::*)(std::string_view);

class ReplicationInstance {
 public:
  ReplicationInstance(CoordinatorInstance *peer, CoordinatorToReplicaConfig config, HealthCheckClientCallback succ_cb,
                      HealthCheckClientCallback fail_cb, HealthCheckInstanceCallback succ_instance_cb,
                      HealthCheckInstanceCallback fail_instance_cb);

  ReplicationInstance(ReplicationInstance const &other) = delete;
  ReplicationInstance &operator=(ReplicationInstance const &other) = delete;
  ReplicationInstance(ReplicationInstance &&other) noexcept = delete;
  ReplicationInstance &operator=(ReplicationInstance &&other) noexcept = delete;
  ~ReplicationInstance() = default;

  auto OnSuccessPing() -> void;
  auto OnFailPing() -> bool;
  auto IsReadyForUUIDPing() -> bool;

  void UpdateReplicaLastResponseUUID();

  auto IsAlive() const -> bool;

  auto InstanceName() const -> std::string;
  auto CoordinatorSocketAddress() const -> std::string;
  auto ReplicationSocketAddress() const -> std::string;

  auto PromoteToMain(utils::UUID const &uuid, ReplicationClientsInfo repl_clients_info,
                     HealthCheckInstanceCallback main_succ_cb, HealthCheckInstanceCallback main_fail_cb) -> bool;

  auto SendDemoteToReplicaRpc() -> bool;

  auto SendFrequentHeartbeat() const -> bool;

  auto DemoteToReplica(HealthCheckInstanceCallback replica_succ_cb, HealthCheckInstanceCallback replica_fail_cb)
      -> bool;

  auto StartFrequentCheck() -> void;
  auto StopFrequentCheck() -> void;
  auto PauseFrequentCheck() -> void;
  auto ResumeFrequentCheck() -> void;

  auto ReplicationClientInfo() const -> ReplicationClientInfo;

  auto EnsureReplicaHasCorrectMainUUID(utils::UUID const &curr_main_uuid) -> bool;

  auto SendSwapAndUpdateUUID(utils::UUID const &new_main_uuid) -> bool;
  auto SendUnregisterReplicaRpc(std::string_view instance_name) -> bool;

  auto SendGetInstanceUUID() -> utils::BasicResult<coordination::GetInstanceUUIDError, std::optional<utils::UUID>>;
  auto GetClient() -> CoordinatorClient &;

  auto EnableWritingOnMain() -> bool;

  auto GetSuccessCallback() -> HealthCheckInstanceCallback;
  auto GetFailCallback() -> HealthCheckInstanceCallback;

  void SetCallbacks(HealthCheckInstanceCallback succ_cb, HealthCheckInstanceCallback fail_cb);

 private:
  CoordinatorClient client_;
  std::chrono::system_clock::time_point last_response_time_{};
  bool is_alive_{false};
  std::chrono::system_clock::time_point last_check_of_uuid_{};

  HealthCheckInstanceCallback succ_cb_;
  HealthCheckInstanceCallback fail_cb_;

  friend bool operator==(ReplicationInstance const &first, ReplicationInstance const &second) {
    return first.client_ == second.client_ && first.last_response_time_ == second.last_response_time_ &&
           first.is_alive_ == second.is_alive_;
  }
};

}  // namespace memgraph::coordination
#endif
