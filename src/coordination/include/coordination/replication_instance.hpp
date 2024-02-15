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

#include <libnuraft/nuraft.hxx>
#include "utils/result.hpp"
#include "utils/uuid.hpp"

namespace memgraph::coordination {

class CoordinatorInstance;

class ReplicationInstance {
 public:
  ReplicationInstance(CoordinatorInstance *peer, CoordinatorClientConfig config, HealthCheckCallback succ_cb,
                      HealthCheckCallback fail_cb);

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
  auto SocketAddress() const -> std::string;

  auto IsReplica() const -> bool;
  auto IsMain() const -> bool;

  auto PromoteToMain(utils::UUID uuid, ReplicationClientsInfo repl_clients_info, HealthCheckCallback main_succ_cb,
                     HealthCheckCallback main_fail_cb) -> bool;
  auto DemoteToReplica(HealthCheckCallback replica_succ_cb, HealthCheckCallback replica_fail_cb) -> bool;

  auto StartFrequentCheck() -> void;
  auto StopFrequentCheck() -> void;
  auto PauseFrequentCheck() -> void;
  auto ResumeFrequentCheck() -> void;

  auto ReplicationClientInfo() const -> ReplClientInfo;

  auto EnsureReplicaHasCorrectMainUUID(utils::UUID const &curr_main_uuid) -> bool;

  auto SendSwapAndUpdateUUID(const utils::UUID &new_main_uuid) -> bool;
  auto SendUnregisterReplicaRpc(std::string const &instance_name) -> bool;


  auto SendGetInstanceUUID() -> utils::BasicResult<coordination::GetInstanceUUIDError, std::optional<utils::UUID>>;
  auto GetClient() -> CoordinatorClient &;

  auto EnableWritingOnMain() -> bool;

  auto SetNewMainUUID(utils::UUID const &main_uuid) -> void;
  auto ResetMainUUID() -> void;
  auto GetMainUUID() const -> const std::optional<utils::UUID> &;

 private:
  CoordinatorClient client_;
  replication_coordination_glue::ReplicationRole replication_role_;
  std::chrono::system_clock::time_point last_response_time_{};
  bool is_alive_{false};
  std::chrono::system_clock::time_point last_check_of_uuid_{};

  // for replica this is main uuid of current main
  // for "main" main this same as in CoordinatorData
  // it is set to nullopt when replica is down
  // TLDR; when replica is down and comes back up we reset uuid of main replica is listening to
  // so we need to send swap uuid again
  std::optional<utils::UUID> main_uuid_;

  friend bool operator==(ReplicationInstance const &first, ReplicationInstance const &second) {
    return first.client_ == second.client_ && first.replication_role_ == second.replication_role_;
  }
};

}  // namespace memgraph::coordination
#endif
