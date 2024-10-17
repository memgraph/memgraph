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

#include "coordination/replication_instance_client.hpp"

#include "coordination/coordinator_communication_config.hpp"
#include "coordination/coordinator_instance.hpp"
#include "coordination/coordinator_rpc.hpp"
#include "replication_coordination_glue/common.hpp"
#include "replication_coordination_glue/messages.hpp"
#include "utils/result.hpp"
#include "utils/uuid.hpp"

#include <string>

namespace memgraph::coordination {

namespace {
auto CreateClientContext(memgraph::coordination::CoordinatorToReplicaConfig const &config)
    -> communication::ClientContext {
  return (config.ssl) ? communication::ClientContext{config.ssl->key_file, config.ssl->cert_file}
                      : communication::ClientContext{};
}
}  // namespace

ReplicationInstanceClient::ReplicationInstanceClient(CoordinatorToReplicaConfig config,
                                                     CoordinatorInstance *coord_instance)
    : rpc_context_{CreateClientContext(config)},
      rpc_client_{config.mgt_server, &rpc_context_},
      config_{std::move(config)},
      coord_instance_(coord_instance) {}

auto ReplicationInstanceClient::InstanceName() const -> std::string { return config_.instance_name; }

auto ReplicationInstanceClient::BoltSocketAddress() const -> std::string { return config_.BoltSocketAddress(); }

auto ReplicationInstanceClient::ManagementSocketAddress() const -> std::string {
  return config_.ManagementSocketAddress();
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

void ReplicationInstanceClient::StartStateCheck() {
  if (instance_checker_.IsRunning()) {
    return;
  }

  MG_ASSERT(config_.instance_health_check_frequency_sec > std::chrono::seconds(0),
            "Health check frequency must be greater than 0");

  instance_checker_.Run(config_.instance_name, config_.instance_health_check_frequency_sec,
                        [this, instance_name = config_.instance_name] {
                          spdlog::trace("Sending state check message to instance {} on {}.", instance_name,
                                        config_.ManagementSocketAddress());
                          auto const res = SendStateCheckRpc();
                          if (res) {
                            coord_instance_->InstanceSuccessCallback(instance_name, res);
                          } else {
                            coord_instance_->InstanceFailCallback(instance_name, res);
                          }
                        });
}

void ReplicationInstanceClient::StopStateCheck() { instance_checker_.Stop(); }
void ReplicationInstanceClient::PauseStateCheck() { instance_checker_.Pause(); }
void ReplicationInstanceClient::ResumeStateCheck() { instance_checker_.Resume(); }
auto ReplicationInstanceClient::GetReplicationClientInfo() const -> coordination::ReplicationClientInfo {
  return config_.replication_client_info;
}

auto ReplicationInstanceClient::SendPromoteReplicaToMainRpc(const utils::UUID &uuid,
                                                            ReplicationClientsInfo replication_clients_info) const
    -> bool {
  try {
    spdlog::trace("Sending PromoteReplicaToMainRpc");
    auto stream{rpc_client_.Stream<PromoteReplicaToMainRpc>(uuid, std::move(replication_clients_info))};
    spdlog::trace("Awaiting response after sending PromoteReplicaToMainRpc");
    if (!stream.AwaitResponse().success) {
      spdlog::error("Failed to receive successful PromoteReplicaToMainRpc response!");
      return false;
    }
    spdlog::trace("Received successful response to PromoteReplicaToMainRPC");
    return true;
  } catch (rpc::RpcFailedException const &) {
    spdlog::error("RPC error occurred while sending PromoteReplicaToMainRpc!");
  }
  return false;
}

auto ReplicationInstanceClient::SendDemoteToReplicaRpc() const -> bool {
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
    spdlog::error("Failed to receive RPC response when demoting instance {} to replica!", instance_name);
  }
  return false;
}

auto ReplicationInstanceClient::SendRegisterReplicaRpc(utils::UUID const &uuid,
                                                       ReplicationClientInfo replication_client_info) const -> bool {
  auto const instance_name = replication_client_info.instance_name;
  try {
    auto stream{rpc_client_.Stream<RegisterReplicaOnMainRpc>(uuid, std::move(replication_client_info))};
    if (!stream.AwaitResponse().success) {
      spdlog::error("Failed to receive successful RPC response for registering replica instance {} on main!",
                    instance_name);
      return false;
    }
    spdlog::trace("Sent request RPC from coordinator to register replica instance on main.");
    return true;
  } catch (rpc::RpcFailedException const &) {
    spdlog::error("Failed to receive RPC response when registering instance {} to replica!", instance_name);
  }
  return false;
}

auto ReplicationInstanceClient::SendStateCheckRpc() const -> std::optional<InstanceState> {
  try {
    auto stream{rpc_client_.Stream<StateCheckRpc>()};
    auto res = stream.AwaitResponse();
    return res.state;
  } catch (rpc::RpcFailedException const &) {
    return {};
  }
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
    spdlog::error("Failed to receive RPC response when unregistering replica!");
  }
  return false;
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
    spdlog::error("Failed to receive RPC response when enabling writing on main!");
  }
  return false;
}

auto ReplicationInstanceClient::SendGetInstanceTimestampsRpc() const
    -> utils::BasicResult<GetInstanceTimestampsError, replication_coordination_glue::DatabaseHistories> {
  try {
    auto stream{rpc_client_.Stream<coordination::GetDatabaseHistoriesRpc>()};
    return stream.AwaitResponse().database_histories;

  } catch (const rpc::RpcFailedException &) {
    spdlog::error("Failed to receive RPC response when sending GetInstanceTimestampRPC");
    return GetInstanceTimestampsError::RPC_EXCEPTION;
  }
}

}  // namespace memgraph::coordination
#endif
