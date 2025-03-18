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

#include "coordination/replication_instance_client.hpp"

#include "utils/uuid.hpp"

namespace memgraph::coordination {

class ReplicationInstanceConnector;

// Class used for managing the connection from coordinator to the data instance.
class ReplicationInstanceConnector {
 public:
  ReplicationInstanceConnector(DataInstanceConfig const &config, CoordinatorInstance *coord_instance,
                               std::chrono::seconds instance_down_timeout_sec,
                               std::chrono::seconds instance_health_check_frequency_sec);

  ReplicationInstanceConnector(ReplicationInstanceConnector const &other) = delete;
  ReplicationInstanceConnector &operator=(ReplicationInstanceConnector const &other) = delete;
  ReplicationInstanceConnector(ReplicationInstanceConnector &&other) noexcept = delete;
  ReplicationInstanceConnector &operator=(ReplicationInstanceConnector &&other) noexcept = delete;
  ~ReplicationInstanceConnector() = default;

  auto OnFailPing() -> bool;
  auto OnSuccessPing() -> void;

  auto IsAlive() const -> bool;
  auto LastSuccRespMs() const -> std::chrono::milliseconds;

  auto InstanceName() const -> std::string;
  auto BoltSocketAddress() const -> std::string;
  auto ManagementSocketAddress() const -> std::string;
  auto ReplicationSocketAddress() const -> std::string;

  auto SendSwapAndUpdateUUID(utils::UUID const &new_main_uuid) const -> bool;

  template <rpc::IsRpc T, typename... Args>
  auto SendRpc(Args &&...args) const -> bool {
    return client_.SendRpc<T>(std::forward<Args>(args)...);
  }

  auto StartStateCheck() -> void;
  auto StopStateCheck() -> void;
  auto PauseStateCheck() -> void;
  auto ResumeStateCheck() -> void;

  auto GetReplicationClientInfo() const -> ReplicationClientInfo;
  auto GetClient() const -> ReplicationInstanceClient const &;

 protected:
  ReplicationInstanceClient client_;
  std::chrono::system_clock::time_point last_response_time_{};
  std::chrono::seconds instance_down_timeout_sec_{5};
  bool is_alive_{false};

  friend bool operator==(ReplicationInstanceConnector const &first, ReplicationInstanceConnector const &second) {
    return first.client_ == second.client_ && first.last_response_time_ == second.last_response_time_ &&
           first.is_alive_ == second.is_alive_;
  }
};

}  // namespace memgraph::coordination
#endif
