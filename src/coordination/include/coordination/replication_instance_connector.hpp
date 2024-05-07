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

#include "coordination/coordinator_exceptions.hpp"
#include "coordination/replication_instance_client.hpp"
#include "replication_coordination_glue/role.hpp"

#include "utils/resource_lock.hpp"
#include "utils/result.hpp"
#include "utils/uuid.hpp"

#include <libnuraft/nuraft.hxx>

namespace memgraph::coordination {

using HealthCheckInstanceCallback = void (CoordinatorInstance::*)(std::string_view);

// Class used for managing the connection from coordinator to the data instance.
class ReplicationInstanceConnector {
 public:
  explicit ReplicationInstanceConnector(std::unique_ptr<ReplicationInstanceClient> client,
                                        HealthCheckInstanceCallback succ_instance_cb = nullptr,
                                        HealthCheckInstanceCallback fail_instance_cb = nullptr);

  ReplicationInstanceConnector(ReplicationInstanceConnector const &other) = delete;
  ReplicationInstanceConnector &operator=(ReplicationInstanceConnector const &other) = delete;
  ReplicationInstanceConnector(ReplicationInstanceConnector &&other) noexcept = delete;
  ReplicationInstanceConnector &operator=(ReplicationInstanceConnector &&other) noexcept = delete;
  ~ReplicationInstanceConnector() = default;

  auto OnFailPing() -> bool;
  auto OnSuccessPing() -> void;
  auto IsReadyForUUIDPing() -> bool;

  auto IsAlive() const -> bool;

  // TODO: (andi) Fetch from ClusterState
  auto InstanceName() const -> std::string;

  auto BoltSocketAddress() const -> std::string;
  auto ManagementSocketAddress() const -> std::string;
  auto ReplicationSocketAddress() const -> std::string;

  auto PromoteToMain(utils::UUID const &uuid, ReplicationClientsInfo repl_clients_info,
                     HealthCheckInstanceCallback main_succ_cb, HealthCheckInstanceCallback main_fail_cb) -> bool;

  auto SendDemoteToReplicaRpc() -> bool;

  auto SendFrequentHeartbeat() const -> bool;

  auto DemoteToReplica(HealthCheckInstanceCallback replica_succ_cb, HealthCheckInstanceCallback replica_fail_cb)
      -> bool;

  auto RegisterReplica(utils::UUID const &uuid, ReplicationClientInfo replication_client_info) -> bool;

  auto StartFrequentCheck() -> void;
  auto StopFrequentCheck() -> void;
  auto PauseFrequentCheck() -> void;
  auto ResumeFrequentCheck() -> void;

  auto GetReplicationClientInfo() const -> ReplicationClientInfo;

  auto EnsureReplicaHasCorrectMainUUID(utils::UUID const &curr_main_uuid) -> bool;

  auto SendSwapAndUpdateUUID(utils::UUID const &new_main_uuid) -> bool;
  auto SendUnregisterReplicaRpc(std::string_view instance_name) -> bool;

  auto SendGetInstanceUUID() -> utils::BasicResult<coordination::GetInstanceUUIDError, std::optional<utils::UUID>>;
  auto GetClient() -> ReplicationInstanceClient &;

  auto EnableWritingOnMain() -> bool;

  auto GetSuccessCallback() -> HealthCheckInstanceCallback;
  auto GetFailCallback() -> HealthCheckInstanceCallback;

  void SetCallbacks(HealthCheckInstanceCallback succ_cb, HealthCheckInstanceCallback fail_cb);

  // Time passed from the last successful response in milliseconds.
  auto LastSuccRespMs() const -> std::chrono::milliseconds;

 protected:
  auto UpdateReplicaLastResponseUUID() -> void;
  std::unique_ptr<ReplicationInstanceClient> client_;
  std::chrono::system_clock::time_point last_response_time_{};
  bool is_alive_{false};
  std::chrono::system_clock::time_point last_check_of_uuid_{};

  HealthCheckInstanceCallback succ_cb_;
  HealthCheckInstanceCallback fail_cb_;

  friend bool operator==(ReplicationInstanceConnector const &first, ReplicationInstanceConnector const &second) {
    return first.client_ == second.client_ && first.last_response_time_ == second.last_response_time_ &&
           first.is_alive_ == second.is_alive_;
  }
};

}  // namespace memgraph::coordination
#endif
