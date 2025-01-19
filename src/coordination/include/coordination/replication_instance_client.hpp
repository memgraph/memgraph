// Copyright 2025 Memgraph Ltd.
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

#include "coordination/coordinator_communication_config.hpp"
#include "coordination/instance_state.hpp"
#include "replication_coordination_glue/common.hpp"
#include "rpc/client.hpp"
#include "utils/scheduler.hpp"
#include "utils/uuid.hpp"

namespace memgraph::coordination {

class CoordinatorInstance;
using ReplicationClientsInfo = std::vector<ReplicationClientInfo>;

class ReplicationInstanceClient {
 public:
  explicit ReplicationInstanceClient(DataInstanceConfig config, CoordinatorInstance *coord_instance,
                                     std::chrono::seconds instance_health_check_frequency_sec);

  virtual ~ReplicationInstanceClient() = default;

  ReplicationInstanceClient(ReplicationInstanceClient &) = delete;
  ReplicationInstanceClient &operator=(ReplicationInstanceClient const &) = delete;

  ReplicationInstanceClient(ReplicationInstanceClient &&) noexcept = delete;
  ReplicationInstanceClient &operator=(ReplicationInstanceClient &&) noexcept = delete;

  virtual void StartStateCheck();
  virtual void StopStateCheck();
  virtual void PauseStateCheck();
  virtual void ResumeStateCheck();

  virtual auto InstanceName() const -> std::string;
  virtual auto BoltSocketAddress() const -> std::string;
  virtual auto ManagementSocketAddress() const -> std::string;
  virtual auto ReplicationSocketAddress() const -> std::string;

  virtual auto SendDemoteToReplicaRpc() const -> bool;

  virtual auto SendPromoteToMainRpc(utils::UUID const &uuid, ReplicationClientsInfo replication_clients_info) const
      -> bool;

  virtual auto SendUnregisterReplicaRpc(std::string_view instance_name) const -> bool;

  virtual auto SendEnableWritingOnMainRpc() const -> bool;

  auto SendRegisterReplicaRpc(utils::UUID const &uuid, ReplicationClientInfo replication_client_info) const -> bool;

  auto SendStateCheckRpc() const -> std::optional<InstanceState>;

  auto SendGetInstanceTimestampsRpc() const -> std::optional<replication_coordination_glue::DatabaseHistories>;

  auto GetReplicationClientInfo() const -> ReplicationClientInfo;

  auto RpcClient() const -> rpc::Client & { return rpc_client_; }

  friend bool operator==(ReplicationInstanceClient const &first, ReplicationInstanceClient const &second) {
    return first.config_ == second.config_;
  }

 private:
  utils::Scheduler instance_checker_;

  communication::ClientContext rpc_context_;
  mutable rpc::Client rpc_client_;

  DataInstanceConfig config_;
  CoordinatorInstance *coord_instance_;

  std::chrono::seconds const instance_health_check_frequency_sec_{1};
};

}  // namespace memgraph::coordination
#endif
