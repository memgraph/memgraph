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
#include "coordination/coordinator_cluster_config.hpp"
#include "coordination/coordinator_exceptions.hpp"
#include "replication_coordination_glue/role.hpp"

namespace memgraph::coordination {

class CoordinatorData;

class CoordinatorInstance {
 public:
  CoordinatorInstance(CoordinatorData *data, CoordinatorClientConfig config, HealthCheckCallback succ_cb,
                      HealthCheckCallback fail_cb);

  CoordinatorInstance(CoordinatorInstance const &other) = delete;
  CoordinatorInstance &operator=(CoordinatorInstance const &other) = delete;
  CoordinatorInstance(CoordinatorInstance &&other) noexcept = delete;
  CoordinatorInstance &operator=(CoordinatorInstance &&other) noexcept = delete;
  ~CoordinatorInstance() = default;

  auto OnSuccessPing() -> void;
  auto OnFailPing() -> bool;

  auto IsAlive() const -> bool;

  auto InstanceName() const -> std::string;
  auto SocketAddress() const -> std::string;

  auto IsReplica() const -> bool;
  auto IsMain() const -> bool;

  auto PromoteToMain(ReplicationClientsInfo repl_clients_info, HealthCheckCallback main_succ_cb,
                     HealthCheckCallback main_fail_cb) -> bool;
  auto DemoteToReplica(HealthCheckCallback replica_succ_cb, HealthCheckCallback replica_fail_cb) -> bool;

  auto PauseFrequentCheck() -> void;
  auto ResumeFrequentCheck() -> void;

  auto ReplicationClientInfo() const -> ReplClientInfo;

 private:
  CoordinatorClient client_;
  replication_coordination_glue::ReplicationRole replication_role_;
  std::chrono::system_clock::time_point last_response_time_{};
  bool is_alive_{false};

  friend bool operator==(CoordinatorInstance const &first, CoordinatorInstance const &second) {
    return first.client_ == second.client_ && first.replication_role_ == second.replication_role_;
  }
};

}  // namespace memgraph::coordination
#endif
