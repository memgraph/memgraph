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

#include "coordination/coordinator_instance.hpp"

namespace memgraph::coordination {

auto CoordinatorInstance::UpdateAliveStatus() -> bool {
  is_alive_ =
      std::chrono::duration_cast<std::chrono::seconds>(std::chrono::system_clock::now() - last_response_time_).count() <
      CoordinatorClusterConfig::alive_response_time_difference_sec_;
  return is_alive_;
}
auto CoordinatorInstance::UpdateLastResponseTime() -> void { last_response_time_ = std::chrono::system_clock::now(); }

auto CoordinatorInstance::InstanceName() const -> std::string { return client_.InstanceName(); }
auto CoordinatorInstance::SocketAddress() const -> std::string { return client_.SocketAddress(); }
auto CoordinatorInstance::IsAlive() const -> bool { return is_alive_; }

auto CoordinatorInstance::SetReplicationRole(replication_coordination_glue::ReplicationRole role) -> void {
  replication_role_ = role;
}

auto CoordinatorInstance::IsReplica() const -> bool {
  return replication_role_ == replication_coordination_glue::ReplicationRole::REPLICA;
}
auto CoordinatorInstance::IsMain() const -> bool {
  return replication_role_ == replication_coordination_glue::ReplicationRole::MAIN;
}

auto CoordinatorInstance::PrepareForFailover() -> void { client_.PauseFrequentCheck(); }
auto CoordinatorInstance::RestoreAfterFailedFailover() -> void { client_.ResumeFrequentCheck(); }

auto CoordinatorInstance::PromoteToMain(HealthCheckCallback main_succ_cb, HealthCheckCallback main_fail_cb) -> void {
  replication_role_ = replication_coordination_glue::ReplicationRole::MAIN;
  client_.SetSuccCallback(std::move(main_succ_cb));
  client_.SetFailCallback(std::move(main_fail_cb));
  client_.ResumeFrequentCheck();
}

auto CoordinatorInstance::StartFrequentCheck() -> void { client_.StartFrequentCheck(); }
auto CoordinatorInstance::PauseFrequentCheck() -> void { client_.PauseFrequentCheck(); }
auto CoordinatorInstance::ResumeFrequentCheck() -> void { client_.ResumeFrequentCheck(); }

auto CoordinatorInstance::SetSuccCallback(HealthCheckCallback succ_cb) -> void {
  client_.SetSuccCallback(std::move(succ_cb));
}
auto CoordinatorInstance::SetFailCallback(HealthCheckCallback fail_cb) -> void {
  client_.SetFailCallback(std::move(fail_cb));
}

auto CoordinatorInstance::ResetReplicationClientInfo() -> void { client_.ResetReplicationClientInfo(); }

auto CoordinatorInstance::ReplicationClientInfo() const -> CoordinatorClientConfig::ReplicationClientInfo {
  return client_.ReplicationClientInfo();
}

auto CoordinatorInstance::SendPromoteReplicaToMainRpc(ReplicationClientsInfo replication_clients_info) const -> bool {
  return client_.SendPromoteReplicaToMainRpc(std::move(replication_clients_info));
}

auto CoordinatorInstance::SendSetToReplicaRpc(ReplClientInfo replication_client_info) const -> bool {
  return client_.SendSetToReplicaRpc(std::move(replication_client_info));
}

}  // namespace memgraph::coordination
#endif
