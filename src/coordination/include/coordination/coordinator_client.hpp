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

#include "coordination/coordinator_config.hpp"
#include "rpc/client.hpp"
#include "utils/scheduler.hpp"
#include "utils/thread_pool.hpp"

#include <string_view>

namespace memgraph::coordination {

class CoordinatorData;

class CoordinatorClient {
 public:
  using ReplClientInfo = CoordinatorClientConfig::ReplicationClientInfo;
  using ReplicationClientsInfo = std::vector<ReplClientInfo>;

  using HealthCheckCallback = std::function<void(CoordinatorData *, std::string_view)>;

  explicit CoordinatorClient(CoordinatorData *coord_data_, CoordinatorClientConfig config, HealthCheckCallback succ_cb,
                             HealthCheckCallback fail_cb);

  ~CoordinatorClient();

  CoordinatorClient(CoordinatorClient &) = delete;
  CoordinatorClient &operator=(CoordinatorClient const &) = delete;

  CoordinatorClient(CoordinatorClient &&) noexcept = delete;
  CoordinatorClient &operator=(CoordinatorClient &&) noexcept = delete;

  void StartFrequentCheck();
  void StopFrequentCheck();
  void PauseFrequentCheck();
  void ResumeFrequentCheck();

  auto SendPromoteReplicaToMainRpc(ReplicationClientsInfo replication_clients_info) const -> bool;

  // TODO: (andi) These several methods are probably not needed, Instance should own this info
  auto InstanceName() const -> std::string_view;
  auto SocketAddress() const -> std::string;
  auto Config() const -> CoordinatorClientConfig const &;

  auto ReplicationClientInfo() const -> ReplClientInfo const &;
  auto ReplicationClientInfo() -> std::optional<ReplClientInfo> &;

  auto SuccCallback() const -> HealthCheckCallback const &;
  auto FailCallback() const -> HealthCheckCallback const &;
  // Const &
  auto SetSuccCallback(HealthCheckCallback succ_cb) -> void;
  auto SetFailCallback(HealthCheckCallback fail_cb) -> void;

  friend bool operator==(CoordinatorClient const &first, CoordinatorClient const &second) {
    return first.config_ == second.config_;
  }

 private:
  utils::Scheduler replica_checker_;

  // TODO: (andi) Pimpl?
  communication::ClientContext rpc_context_;
  mutable rpc::Client rpc_client_;

  CoordinatorClientConfig config_;
  CoordinatorData *coord_data_;
  HealthCheckCallback succ_cb_;
  HealthCheckCallback fail_cb_;
};

}  // namespace memgraph::coordination
#endif
