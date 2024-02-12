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

#include "coordination/replication_instance.hpp"

#include "replication_coordination_glue/handler.hpp"

namespace memgraph::coordination {

ReplicationInstance::ReplicationInstance(CoordinatorInstance *peer, CoordinatorClientConfig config,
                                         HealthCheckCallback succ_cb, HealthCheckCallback fail_cb)
    : client_(peer, std::move(config), std::move(succ_cb), std::move(fail_cb)),
      replication_role_(replication_coordination_glue::ReplicationRole::REPLICA) {
  if (!client_.DemoteToReplica()) {
    throw CoordinatorRegisterInstanceException("Failed to demote instance {} to replica", client_.InstanceName());
  }

  client_.StartFrequentCheck();
}

auto ReplicationInstance::OnSuccessPing() -> void {
  last_response_time_ = std::chrono::system_clock::now();
  is_alive_ = true;
}

auto ReplicationInstance::OnFailPing() -> bool {
  is_alive_ =
      std::chrono::duration_cast<std::chrono::seconds>(std::chrono::system_clock::now() - last_response_time_).count() <
      CoordinatorClusterConfig::alive_response_time_difference_sec_;
  return is_alive_;
}

auto ReplicationInstance::InstanceName() const -> std::string { return client_.InstanceName(); }
auto ReplicationInstance::SocketAddress() const -> std::string { return client_.SocketAddress(); }
auto ReplicationInstance::IsAlive() const -> bool { return is_alive_; }

auto ReplicationInstance::IsReplica() const -> bool {
  return replication_role_ == replication_coordination_glue::ReplicationRole::REPLICA;
}
auto ReplicationInstance::IsMain() const -> bool {
  return replication_role_ == replication_coordination_glue::ReplicationRole::MAIN;
}

auto ReplicationInstance::PromoteToMain(utils::UUID new_uuid, ReplicationClientsInfo repl_clients_info,
                                        HealthCheckCallback main_succ_cb, HealthCheckCallback main_fail_cb) -> bool {
  if (!client_.SendPromoteReplicaToMainRpc(new_uuid, std::move(repl_clients_info))) {
    return false;
  }

  replication_role_ = replication_coordination_glue::ReplicationRole::MAIN;
  main_uuid_ = new_uuid;
  client_.SetCallbacks(std::move(main_succ_cb), std::move(main_fail_cb));

  return true;
}

auto ReplicationInstance::DemoteToReplica(HealthCheckCallback replica_succ_cb, HealthCheckCallback replica_fail_cb)
    -> bool {
  if (!client_.DemoteToReplica()) {
    return false;
  }

  replication_role_ = replication_coordination_glue::ReplicationRole::REPLICA;
  client_.SetCallbacks(std::move(replica_succ_cb), std::move(replica_fail_cb));

  return true;
}

auto ReplicationInstance::StartFrequentCheck() -> void { client_.StartFrequentCheck(); }
auto ReplicationInstance::StopFrequentCheck() -> void { client_.StopFrequentCheck(); }
auto ReplicationInstance::PauseFrequentCheck() -> void { client_.PauseFrequentCheck(); }
auto ReplicationInstance::ResumeFrequentCheck() -> void { client_.ResumeFrequentCheck(); }

auto ReplicationInstance::ReplicationClientInfo() const -> CoordinatorClientConfig::ReplicationClientInfo {
  return client_.ReplicationClientInfo();
}

auto ReplicationInstance::GetClient() -> CoordinatorClient & { return client_; }
auto ReplicationInstance::SetNewMainUUID(utils::UUID const &main_uuid) -> void { main_uuid_ = main_uuid; }
auto ReplicationInstance::ResetMainUUID() -> void { main_uuid_ = std::nullopt; }
auto ReplicationInstance::GetMainUUID() -> const std::optional<utils::UUID> & { return main_uuid_; }

auto ReplicationInstance::EnsureReplicaHasCorrectMainUUID(utils::UUID const &curr_main_uuid) -> bool {
  if (!main_uuid_ || *main_uuid_ != curr_main_uuid) {
    return SendSwapAndUpdateUUID(curr_main_uuid);
  }
  return true;
}

auto ReplicationInstance::SendSwapAndUpdateUUID(const utils::UUID &new_main_uuid) -> bool {
  if (!replication_coordination_glue::SendSwapMainUUIDRpc(client_.RpcClient(), new_main_uuid)) {
    return false;
  }
  SetNewMainUUID(new_main_uuid);
  return true;
}

auto ReplicationInstance::SendUnregisterReplicaRpc(std::string const &instance_name) -> bool {
  return client_.SendUnregisterReplicaRpc(instance_name);
}

}  // namespace memgraph::coordination
#endif
