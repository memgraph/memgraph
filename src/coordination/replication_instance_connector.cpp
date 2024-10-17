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

#ifdef MG_ENTERPRISE

#include "coordination/replication_instance_connector.hpp"

#include "replication_coordination_glue/handler.hpp"
#include "utils/result.hpp"

#include <chrono>
#include <string>
#include <utility>

namespace memgraph::coordination {

ReplicationInstanceConnector::ReplicationInstanceConnector(CoordinatorToReplicaConfig const &config,
                                                           CoordinatorInstance *coord_instance)
    : client_(ReplicationInstanceClient(config, coord_instance)) {}

void ReplicationInstanceConnector::OnSuccessPing() {
  last_response_time_ = std::chrono::system_clock::now();
  is_alive_ = true;
}

auto ReplicationInstanceConnector::OnFailPing() -> bool {
  auto elapsed_time = std::chrono::system_clock::now() - last_response_time_;
  is_alive_ = elapsed_time < client_.InstanceDownTimeoutSec();
  return is_alive_;
}

auto ReplicationInstanceConnector::IsAlive() const -> bool { return is_alive_; }

auto ReplicationInstanceConnector::LastSuccRespMs() const -> std::chrono::milliseconds {
  using std::chrono::duration_cast;
  using std::chrono::milliseconds;
  using std::chrono::system_clock;

  return duration_cast<milliseconds>(system_clock::now() - last_response_time_);
}

auto ReplicationInstanceConnector::InstanceName() const -> std::string { return client_.InstanceName(); }

auto ReplicationInstanceConnector::BoltSocketAddress() const -> std::string { return client_.BoltSocketAddress(); }

auto ReplicationInstanceConnector::ManagementSocketAddress() const -> std::string {
  return client_.ManagementSocketAddress();
}
auto ReplicationInstanceConnector::ReplicationSocketAddress() const -> std::string {
  return client_.ReplicationSocketAddress();
}

auto ReplicationInstanceConnector::SendPromoteToMainRpc(utils::UUID const &new_uuid,
                                                        ReplicationClientsInfo repl_clients_info) -> bool {
  return client_.SendPromoteReplicaToMainRpc(new_uuid, std::move(repl_clients_info));
}

auto ReplicationInstanceConnector::SendDemoteToReplicaRpc() -> bool { return client_.SendDemoteToReplicaRpc(); }

auto ReplicationInstanceConnector::SendStateCheckRpc() const -> std::optional<InstanceState> {
  return client_.SendStateCheckRpc();
}

auto ReplicationInstanceConnector::SendRegisterReplicaRpc(utils::UUID const &uuid,
                                                          ReplicationClientInfo replication_client_info) -> bool {
  return client_.SendRegisterReplicaRpc(uuid, std::move(replication_client_info));
}

auto ReplicationInstanceConnector::SendSwapAndUpdateUUID(utils::UUID const &new_main_uuid) -> bool {
  return replication_coordination_glue::SendSwapMainUUIDRpc(client_.RpcClient(), new_main_uuid);
}

auto ReplicationInstanceConnector::SendUnregisterReplicaRpc(std::string_view instance_name) -> bool {
  return client_.SendUnregisterReplicaRpc(instance_name);
}

auto ReplicationInstanceConnector::SendEnableWritingOnMainRpc() -> bool { return client_.SendEnableWritingOnMainRpc(); }

auto ReplicationInstanceConnector::StartStateCheck() -> void { client_.StartStateCheck(); }
auto ReplicationInstanceConnector::StopStateCheck() -> void { client_.StopStateCheck(); }
auto ReplicationInstanceConnector::PauseStateCheck() -> void { client_.PauseStateCheck(); }
auto ReplicationInstanceConnector::ResumeStateCheck() -> void { client_.ResumeStateCheck(); }

auto ReplicationInstanceConnector::GetReplicationClientInfo() const -> coordination::ReplicationClientInfo {
  return client_.GetReplicationClientInfo();
}

auto ReplicationInstanceConnector::GetClient() -> ReplicationInstanceClient & { return client_; }

}  // namespace memgraph::coordination
#endif
