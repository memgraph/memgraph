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

#ifdef MG_ENTERPRISE

#include "coordination/replication_instance_client.hpp"

#include "coordination/coordinator_communication_config.hpp"
#include "coordination/coordinator_instance.hpp"
#include "coordination/coordinator_rpc.hpp"
#include "replication_coordination_glue/common.hpp"
#include "utils/uuid.hpp"

#include <string>

namespace memgraph::coordination {

ReplicationInstanceClient::ReplicationInstanceClient(DataInstanceConfig config, CoordinatorInstance *coord_instance,
                                                     const std::chrono::seconds instance_health_check_frequency_sec)
    : rpc_context_{communication::ClientContext{}},
      rpc_client_{config.mgt_server, &rpc_context_},
      config_{std::move(config)},
      coord_instance_(coord_instance),
      instance_health_check_frequency_sec_(instance_health_check_frequency_sec) {}

auto ReplicationInstanceClient::InstanceName() const -> std::string { return config_.instance_name; }

auto ReplicationInstanceClient::BoltSocketAddress() const -> std::string { return config_.BoltSocketAddress(); }

auto ReplicationInstanceClient::ManagementSocketAddress() const -> std::string {
  return config_.ManagementSocketAddress();
}
auto ReplicationInstanceClient::ReplicationSocketAddress() const -> std::string {
  return config_.ReplicationSocketAddress();
}

void ReplicationInstanceClient::StartStateCheck() {
  if (instance_checker_.IsRunning()) {
    return;
  }

  MG_ASSERT(instance_health_check_frequency_sec_ > std::chrono::seconds(0),
            "Health check frequency must be greater than 0");

  instance_checker_.SetInterval(instance_health_check_frequency_sec_);
  instance_checker_.Run(config_.instance_name, [this, instance_name = config_.instance_name] {
    spdlog::trace("Sending state check message to instance {} on {}.", instance_name,
                  config_.ManagementSocketAddress());
    if (auto const res = SendStateCheckRpc()) {
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

auto ReplicationInstanceClient::SendPromoteToMainRpc(const utils::UUID &uuid,
                                                     ReplicationClientsInfo replication_clients_info) const -> bool {
  try {
    spdlog::trace("Sending PromoteToMainRpc");
    auto stream{rpc_client_.Stream<PromoteToMainRpc>(uuid, std::move(replication_clients_info))};
    spdlog::trace("Awaiting response after sending PromoteToMainRpc");
    if (!stream.AwaitResponse().success) {
      spdlog::error("Failed to receive successful PromoteToMainRpc response!");
      return false;
    }
    spdlog::trace("Received successful response to PromoteToMainRPC");
    return true;
  } catch (rpc::RpcFailedException const &) {
    spdlog::error("RPC error occurred while sending PromoteToMainRpc!");
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
    spdlog::trace("Received successful response to DemoteMainToReplicaRpc!");
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
    spdlog::trace("Received successful response to RegisterReplicaOnMainRpc!");
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
    -> std::optional<replication_coordination_glue::DatabaseHistories> {
  try {
    auto stream{rpc_client_.Stream<GetDatabaseHistoriesRpc>()};
    auto res = stream.AwaitResponse();
    return res.database_histories;

  } catch (const rpc::RpcFailedException &) {
    spdlog::error("Failed to receive RPC response when sending GetInstanceTimestampRPC");
    return {};
  }
}

}  // namespace memgraph::coordination
#endif
