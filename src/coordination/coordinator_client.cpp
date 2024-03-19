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
#include "coordination/coordinator_client.hpp"

#include "coordination/coordinator_communication_config.hpp"
#include "coordination/coordinator_rpc.hpp"
#include "replication_coordination_glue/common.hpp"
#include "replication_coordination_glue/messages.hpp"
#include "utils/result.hpp"
#include "utils/uuid.hpp"

namespace memgraph::coordination {

namespace {
auto CreateClientContext(memgraph::coordination::CoordinatorToReplicaConfig const &config)
    -> communication::ClientContext {
  return (config.ssl) ? communication::ClientContext{config.ssl->key_file, config.ssl->cert_file}
                      : communication::ClientContext{};
}
}  // namespace

ReplicationInstanceClient::ReplicationInstanceClient(CoordinatorInstance *coord_instance,
                                                     CoordinatorToReplicaConfig config,
                                                     HealthCheckClientCallback succ_cb,
                                                     HealthCheckClientCallback fail_cb)
    : rpc_context_{CreateClientContext(config)},
      rpc_client_{config.mgt_server, &rpc_context_},
      config_{std::move(config)},
      coord_instance_{coord_instance},
      succ_cb_{std::move(succ_cb)},
      fail_cb_{std::move(fail_cb)} {}

auto ReplicationInstanceClient::InstanceName() const -> std::string { return config_.instance_name; }

auto ReplicationInstanceClient::CoordinatorSocketAddress() const -> std::string {
  return config_.CoordinatorSocketAddress();
}
auto ReplicationInstanceClient::ReplicationSocketAddress() const -> std::string {
  return config_.ReplicationSocketAddress();
}

auto ReplicationInstanceClient::InstanceDownTimeoutSec() const -> std::chrono::seconds {
  return config_.instance_down_timeout_sec;
}

auto ReplicationInstanceClient::InstanceGetUUIDFrequencySec() const -> std::chrono::seconds {
  return config_.instance_get_uuid_frequency_sec;
}

void ReplicationInstanceClient::StartFrequentCheck() {
  if (instance_checker_.IsRunning()) {
    return;
  }

  MG_ASSERT(config_.instance_health_check_frequency_sec > std::chrono::seconds(0),
            "Health check frequency must be greater than 0");

  instance_checker_.Run(config_.instance_name, config_.instance_health_check_frequency_sec,
                        [this, instance_name = config_.instance_name] {
                          spdlog::trace("Sending frequent heartbeat to machine {} on {}", instance_name,
                                        config_.CoordinatorSocketAddress());
                          if (SendFrequentHeartbeat()) {
                            succ_cb_(coord_instance_, instance_name);
                            return;
                          }
                          fail_cb_(coord_instance_, instance_name);
                        });
}

void ReplicationInstanceClient::StopFrequentCheck() { instance_checker_.Stop(); }
void ReplicationInstanceClient::PauseFrequentCheck() { instance_checker_.Pause(); }
void ReplicationInstanceClient::ResumeFrequentCheck() { instance_checker_.Resume(); }

auto ReplicationInstanceClient::ReplicationClientInfo() const -> coordination::ReplicationClientInfo {
  return config_.replication_client_info;
}

auto ReplicationInstanceClient::SendPromoteReplicaToMainRpc(const utils::UUID &uuid,
                                                            ReplicationClientsInfo replication_clients_info) const
    -> bool {
  try {
    auto stream{rpc_client_.Stream<PromoteReplicaToMainRpc>(uuid, std::move(replication_clients_info))};
    if (!stream.AwaitResponse().success) {
      spdlog::error("Failed to receive successful PromoteReplicaToMainRpc response!");
      return false;
    }
    return true;
  } catch (rpc::RpcFailedException const &) {
    spdlog::error("RPC error occurred while sending PromoteReplicaToMainRpc!");
  }
  return false;
}

auto ReplicationInstanceClient::DemoteToReplica() const -> bool {
  auto const &instance_name = config_.instance_name;
  try {
    auto stream{rpc_client_.Stream<DemoteMainToReplicaRpc>(config_.replication_client_info)};
    if (!stream.AwaitResponse().success) {
      spdlog::error("Failed to receive successful RPC response for setting instance {} to replica!", instance_name);
      return false;
    }
    spdlog::info("Sent request RPC from coordinator to instance to set it as replica!");
    return true;
  } catch (rpc::RpcFailedException const &) {
    spdlog::error("Failed to set instance {} to replica!", instance_name);
  }
  return false;
}

auto ReplicationInstanceClient::SendFrequentHeartbeat() const -> bool {
  try {
    auto stream{rpc_client_.Stream<memgraph::replication_coordination_glue::FrequentHeartbeatRpc>()};
    stream.AwaitResponse();
    return true;
  } catch (rpc::RpcFailedException const &) {
    return false;
  }
}

auto ReplicationInstanceClient::SendSwapMainUUIDRpc(utils::UUID const &uuid) const -> bool {
  try {
    auto stream{rpc_client_.Stream<replication_coordination_glue::SwapMainUUIDRpc>(uuid)};
    if (!stream.AwaitResponse().success) {
      spdlog::error("Failed to receive successful RPC swapping of uuid response!");
      return false;
    }
    return true;
  } catch (const rpc::RpcFailedException &) {
    spdlog::error("RPC error occurred while sending swapping uuid RPC!");
  }
  return false;
}

auto ReplicationInstanceClient::SendUnregisterReplicaRpc(std::string_view instance_name) const -> bool {
  try {
    auto stream{rpc_client_.Stream<UnregisterReplicaRpc>(instance_name)};
    if (!stream.AwaitResponse().success) {
      spdlog::error("Failed to receive successful RPC response for unregistering replica!");
      return false;
    }
    return true;
  } catch (rpc::RpcFailedException const &) {
    spdlog::error("Failed to unregister replica!");
  }
  return false;
}

auto ReplicationInstanceClient::SendGetInstanceUUIDRpc() const
    -> utils::BasicResult<GetInstanceUUIDError, std::optional<utils::UUID>> {
  try {
    auto stream{rpc_client_.Stream<GetInstanceUUIDRpc>()};
    auto res = stream.AwaitResponse();
    return res.uuid;
  } catch (const rpc::RpcFailedException &) {
    spdlog::error("RPC error occured while sending GetInstance UUID RPC");
    return GetInstanceUUIDError::RPC_EXCEPTION;
  }
}

auto ReplicationInstanceClient::SendEnableWritingOnMainRpc() const -> bool {
  try {
    auto stream{rpc_client_.Stream<EnableWritingOnMainRpc>()};
    if (!stream.AwaitResponse().success) {
      spdlog::error("Failed to receive successful RPC response for enabling writing on main!");
      return false;
    }
    return true;
  } catch (rpc::RpcFailedException const &) {
    spdlog::error("Failed to enable writing on main!");
  }
  return false;
}

auto ReplicationInstanceClient::SendGetInstanceTimestampsRpc() const
    -> utils::BasicResult<GetInstanceUUIDError, replication_coordination_glue::DatabaseHistories> {
  try {
    auto stream{rpc_client_.Stream<coordination::GetDatabaseHistoriesRpc>()};
    return stream.AwaitResponse().database_histories;

  } catch (const rpc::RpcFailedException &) {
    spdlog::error("RPC error occured while sending GetInstance UUID RPC");
    return GetInstanceUUIDError::RPC_EXCEPTION;
  }
}

}  // namespace memgraph::coordination
#endif
