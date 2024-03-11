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

#include <utility>

#include "replication_coordination_glue/handler.hpp"
#include "utils/result.hpp"

namespace memgraph::coordination {

ReplicationInstance::ReplicationInstance(CoordinatorInstance *peer, CoordinatorClientConfig config,
                                         HealthCheckClientCallback succ_cb, HealthCheckClientCallback fail_cb,
                                         HealthCheckInstanceCallback succ_instance_cb,
                                         HealthCheckInstanceCallback fail_instance_cb)
    : client_(peer, std::move(config), std::move(succ_cb), std::move(fail_cb)),
      succ_cb_(succ_instance_cb),
      fail_cb_(fail_instance_cb) {}

auto ReplicationInstance::OnSuccessPing() -> void {
  last_response_time_ = std::chrono::system_clock::now();
  is_alive_ = true;
}

auto ReplicationInstance::OnFailPing() -> bool {
  auto elapsed_time = std::chrono::system_clock::now() - last_response_time_;
  is_alive_ = elapsed_time < client_.InstanceDownTimeoutSec();
  return is_alive_;
}

auto ReplicationInstance::IsReadyForUUIDPing() -> bool {
  return std::chrono::duration_cast<std::chrono::seconds>(std::chrono::system_clock::now() - last_check_of_uuid_) >
         client_.InstanceGetUUIDFrequencySec();
}

auto ReplicationInstance::InstanceName() const -> std::string { return client_.InstanceName(); }
auto ReplicationInstance::CoordinatorSocketAddress() const -> std::string { return client_.CoordinatorSocketAddress(); }
auto ReplicationInstance::ReplicationSocketAddress() const -> std::string { return client_.ReplicationSocketAddress(); }
auto ReplicationInstance::IsAlive() const -> bool { return is_alive_; }

auto ReplicationInstance::PromoteToMain(utils::UUID const &new_uuid, ReplicationClientsInfo repl_clients_info,
                                        HealthCheckInstanceCallback main_succ_cb,
                                        HealthCheckInstanceCallback main_fail_cb) -> bool {
  if (!client_.SendPromoteReplicaToMainRpc(new_uuid, std::move(repl_clients_info))) {
    return false;
  }

  main_uuid_ = new_uuid;
  succ_cb_ = main_succ_cb;
  fail_cb_ = main_fail_cb;

  return true;
}

auto ReplicationInstance::SendDemoteToReplicaRpc() -> bool { return client_.DemoteToReplica(); }

auto ReplicationInstance::DemoteToReplica(HealthCheckInstanceCallback replica_succ_cb,
                                          HealthCheckInstanceCallback replica_fail_cb) -> bool {
  if (!client_.DemoteToReplica()) {
    return false;
  }

  succ_cb_ = replica_succ_cb;
  fail_cb_ = replica_fail_cb;

  return true;
}

auto ReplicationInstance::StartFrequentCheck() -> void { client_.StartFrequentCheck(); }
auto ReplicationInstance::StopFrequentCheck() -> void { client_.StopFrequentCheck(); }
auto ReplicationInstance::PauseFrequentCheck() -> void { client_.PauseFrequentCheck(); }
auto ReplicationInstance::ResumeFrequentCheck() -> void { client_.ResumeFrequentCheck(); }

auto ReplicationInstance::ReplicationClientInfo() const -> CoordinatorClientConfig::ReplicationClientInfo {
  return client_.ReplicationClientInfo();
}

auto ReplicationInstance::GetSuccessCallback() -> HealthCheckInstanceCallback & { return succ_cb_; }
auto ReplicationInstance::GetFailCallback() -> HealthCheckInstanceCallback & { return fail_cb_; }

auto ReplicationInstance::GetClient() -> CoordinatorClient & { return client_; }

auto ReplicationInstance::SetNewMainUUID(utils::UUID const &main_uuid) -> void { main_uuid_ = main_uuid; }
auto ReplicationInstance::GetMainUUID() const -> std::optional<utils::UUID> const & { return main_uuid_; }

auto ReplicationInstance::EnsureReplicaHasCorrectMainUUID(utils::UUID const &curr_main_uuid) -> bool {
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

auto ReplicationInstance::SendSwapAndUpdateUUID(utils::UUID const &new_main_uuid) -> bool {
  if (!replication_coordination_glue::SendSwapMainUUIDRpc(client_.RpcClient(), new_main_uuid)) {
    return false;
  }
  SetNewMainUUID(new_main_uuid);
  return true;
}

auto ReplicationInstance::SendUnregisterReplicaRpc(std::string_view instance_name) -> bool {
  return client_.SendUnregisterReplicaRpc(instance_name);
}

auto ReplicationInstance::EnableWritingOnMain() -> bool { return client_.SendEnableWritingOnMainRpc(); }

auto ReplicationInstance::SendGetInstanceUUID()
    -> utils::BasicResult<coordination::GetInstanceUUIDError, std::optional<utils::UUID>> {
  return client_.SendGetInstanceUUIDRpc();
}

void ReplicationInstance::UpdateReplicaLastResponseUUID() { last_check_of_uuid_ = std::chrono::system_clock::now(); }

}  // namespace memgraph::coordination
#endif
