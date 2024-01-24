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
#include "replication_coordination_glue/role.hpp"

namespace memgraph::coordination {

class CoordinatorData;

class CoordinatorInstance {
 public:
  CoordinatorInstance(CoordinatorData *data, CoordinatorClientConfig config, HealthCheckCallback succ_cb,
                      HealthCheckCallback fail_cb, replication_coordination_glue::ReplicationRole replication_role)
      : client_(data, std::move(config), std::move(succ_cb), std::move(fail_cb)),
        replication_role_(replication_role),
        is_alive_(true) {}

  CoordinatorInstance(CoordinatorInstance const &other) = delete;
  CoordinatorInstance &operator=(CoordinatorInstance const &other) = delete;
  CoordinatorInstance(CoordinatorInstance &&other) noexcept = delete;
  CoordinatorInstance &operator=(CoordinatorInstance &&other) noexcept = delete;
  ~CoordinatorInstance() = default;

  auto UpdateInstanceStatus() -> bool {
    is_alive_ = std::chrono::duration_cast<std::chrono::seconds>(std::chrono::system_clock::now() -
                                                                 last_response_time_.load(std::memory_order_acquire))
                    .count() < CoordinatorClusterConfig::alive_response_time_difference_sec_;
    return is_alive_;
  }
  auto UpdateLastResponseTime() -> void { last_response_time_ = std::chrono::system_clock::now(); }

  auto InstanceName() const -> std::string { return client_.InstanceName(); }
  auto SocketAddress() const -> std::string { return client_.SocketAddress(); }
  auto IsAlive() const -> bool { return is_alive_; }

  auto IsReplica() const -> bool {
    return replication_role_ == replication_coordination_glue::ReplicationRole::REPLICA;
  }
  auto IsMain() const -> bool { return replication_role_ == replication_coordination_glue::ReplicationRole::MAIN; }

  auto PrepareForFailover() -> void { client_.PauseFrequentCheck(); }
  auto RestoreAfterFailedFailover() -> void { client_.ResumeFrequentCheck(); }

  auto PostFailover(HealthCheckCallback main_succ_cb, HealthCheckCallback main_fail_cb) -> void {
    replication_role_ = replication_coordination_glue::ReplicationRole::MAIN;
    client_.SetSuccCallback(std::move(main_succ_cb));
    client_.SetFailCallback(std::move(main_fail_cb));
    client_.ResetReplicationClientInfo();
    client_.ResumeFrequentCheck();
  }

  CoordinatorClient client_;
  replication_coordination_glue::ReplicationRole replication_role_;
  std::atomic<std::chrono::system_clock::time_point> last_response_time_{};
  std::atomic<bool> is_alive_{false};

  friend bool operator==(CoordinatorInstance const &first, CoordinatorInstance const &second) {
    return first.client_ == second.client_ && first.replication_role_ == second.replication_role_;
  }
};

}  // namespace memgraph::coordination
#endif
