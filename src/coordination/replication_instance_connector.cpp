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

#include <utility>

namespace memgraph::coordination {

ReplicationInstanceConnector::ReplicationInstanceConnector(std::unique_ptr<ReplicationInstanceClient> client,
                                                           HealthCheckInstanceCallback succ_instance_cb,
                                                           HealthCheckInstanceCallback fail_instance_cb)
    : client_(std::move(client)), succ_cb_(succ_instance_cb), fail_cb_(fail_instance_cb) {}

void ReplicationInstanceConnector::OnSuccessPing() {
  last_response_time_ = std::chrono::system_clock::now();
  is_alive_ = true;
}

auto ReplicationInstanceConnector::OnFailPing() -> bool {
  auto elapsed_time = std::chrono::system_clock::now() - last_response_time_;
  is_alive_ = elapsed_time < client_->InstanceDownTimeoutSec();
  return is_alive_;
}

auto ReplicationInstanceConnector::IsReadyForUUIDPing() -> bool {
  return std::chrono::duration_cast<std::chrono::seconds>(std::chrono::system_clock::now() - last_check_of_uuid_) >
         client_->InstanceGetUUIDFrequencySec();
}

auto ReplicationInstanceConnector::InstanceName() const -> std::string { return client_->InstanceName(); }
auto ReplicationInstanceConnector::ManagementSocketAddress() const -> std::string {
  return client_->ManagementSocketAddress();
}
auto ReplicationInstanceConnector::ReplicationSocketAddress() const -> std::string {
  return client_->ReplicationSocketAddress();
}
auto ReplicationInstanceConnector::IsAlive() const -> bool { return is_alive_; }

// TODO: (andi) Ideally instance already knows its callbacks
auto ReplicationInstanceConnector::PromoteToMain(utils::UUID const &new_uuid, ReplicationClientsInfo repl_clients_info,
                                                 HealthCheckInstanceCallback main_succ_cb,
                                                 HealthCheckInstanceCallback main_fail_cb) -> bool {
  if (!client_->SendPromoteReplicaToMainRpc(new_uuid, std::move(repl_clients_info))) {
    return false;
  }

  succ_cb_ = main_succ_cb;
  fail_cb_ = main_fail_cb;

  return true;
}

// TODO: (andi) Duplication. We have to refactor this
auto ReplicationInstanceConnector::SendDemoteToReplicaRpc() -> bool { return client_->DemoteToReplica(); }

auto ReplicationInstanceConnector::SendFrequentHeartbeat() const -> bool { return client_->SendFrequentHeartbeat(); }

auto ReplicationInstanceConnector::DemoteToReplica(HealthCheckInstanceCallback replica_succ_cb,
                                                   HealthCheckInstanceCallback replica_fail_cb) -> bool {
  if (!client_->DemoteToReplica()) {
    return false;
  }

  succ_cb_ = replica_succ_cb;
  fail_cb_ = replica_fail_cb;

  return true;
}

auto ReplicationInstanceConnector::RegisterReplica(utils::UUID const &uuid,
                                                   ReplicationClientInfo replication_client_info) -> bool {
  return client_->RegisterReplica(uuid, std::move(replication_client_info));
}

auto ReplicationInstanceConnector::StartFrequentCheck() -> void { client_->StartFrequentCheck(); }
auto ReplicationInstanceConnector::StopFrequentCheck() -> void { client_->StopFrequentCheck(); }
auto ReplicationInstanceConnector::PauseFrequentCheck() -> void { client_->PauseFrequentCheck(); }
auto ReplicationInstanceConnector::ResumeFrequentCheck() -> void { client_->ResumeFrequentCheck(); }

auto ReplicationInstanceConnector::GetReplicationClientInfo() const -> coordination::ReplicationClientInfo {
  return client_->GetReplicationClientInfo();
}

auto ReplicationInstanceConnector::GetSuccessCallback() -> HealthCheckInstanceCallback { return succ_cb_; }
auto ReplicationInstanceConnector::GetFailCallback() -> HealthCheckInstanceCallback { return fail_cb_; }

auto ReplicationInstanceConnector::GetClient() -> ReplicationInstanceClient & { return *client_; }

auto ReplicationInstanceConnector::EnsureReplicaHasCorrectMainUUID(utils::UUID const &curr_main_uuid) -> bool {
  if (!IsReadyForUUIDPing()) {
    return true;
  }
  auto res = SendGetInstanceUUID();
  if (res.HasError()) {
    return false;
  }
  UpdateReplicaLastResponseUUID();

  // NOLINTNEXTLINE
  if (res.GetValue().has_value() && res.GetValue().value() == curr_main_uuid) {
    return true;
  }

  return SendSwapAndUpdateUUID(curr_main_uuid);
}

auto ReplicationInstanceConnector::SendSwapAndUpdateUUID(utils::UUID const &new_main_uuid) -> bool {
  return replication_coordination_glue::SendSwapMainUUIDRpc(client_->RpcClient(), new_main_uuid);
}

auto ReplicationInstanceConnector::SendUnregisterReplicaRpc(std::string_view instance_name) -> bool {
  return client_->SendUnregisterReplicaRpc(instance_name);
}

// TODO: (andi) Or remove access to client or remove all methods that just wrap call to client
auto ReplicationInstanceConnector::EnableWritingOnMain() -> bool { return client_->SendEnableWritingOnMainRpc(); }

auto ReplicationInstanceConnector::SendGetInstanceUUID()
    -> utils::BasicResult<coordination::GetInstanceUUIDError, std::optional<utils::UUID>> {
  return client_->SendGetInstanceUUIDRpc();
}

void ReplicationInstanceConnector::UpdateReplicaLastResponseUUID() {
  last_check_of_uuid_ = std::chrono::system_clock::now();
}

void ReplicationInstanceConnector::SetCallbacks(HealthCheckInstanceCallback succ_cb,
                                                HealthCheckInstanceCallback fail_cb) {
  succ_cb_ = succ_cb;
  fail_cb_ = fail_cb;
}

}  // namespace memgraph::coordination
#endif
