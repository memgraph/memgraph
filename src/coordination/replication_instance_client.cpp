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

#include <string>

namespace memgraph::coordination {

// clang-format off
// NOLINTNEXTLINE(cppcoreguidelines-macro-usage)
#define RpcInfoMetrics(RPC)                                     \
  template <>                                                   \
  auto const RpcInfo<RPC>::succCounter = metrics::RPC##Success; \
  template <>                                                   \
  auto const RpcInfo<RPC>::failCounter = metrics::RPC##Fail;    \
  template <>                                                   \
  auto const RpcInfo<RPC>::timerLabel = metrics::RPC##_us;

RpcInfoMetrics(PromoteToMainRpc)
RpcInfoMetrics(DemoteMainToReplicaRpc)
RpcInfoMetrics(RegisterReplicaOnMainRpc)
RpcInfoMetrics(UnregisterReplicaRpc)
RpcInfoMetrics(EnableWritingOnMainRpc)
    // clang-format on

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
auto ReplicationInstanceClient::GetReplicationClientInfo() const -> ReplicationClientInfo {
  return config_.replication_client_info;
}

auto ReplicationInstanceClient::SendStateCheckRpc() const -> std::optional<InstanceState> {
  try {
    utils::MetricsTimer const timer{metrics::StateCheckRpc_us};
    auto stream{rpc_client_.Stream<StateCheckRpc>()};
    auto res = stream.SendAndWait();
    metrics::IncrementCounter(metrics::StateCheckRpcSuccess);
    return res.state;
  } catch (rpc::RpcFailedException const &e) {
    spdlog::error("Failed to receive response to StateCheckRpc. Error occurred: {}", e.what());
    metrics::IncrementCounter(metrics::StateCheckRpcFail);
    return {};
  }
}

auto ReplicationInstanceClient::SendGetDatabaseHistoriesRpc() const
    -> std::optional<replication_coordination_glue::InstanceInfo> {
  try {
    utils::MetricsTimer const timer{metrics::GetDatabaseHistoriesRpc_us};
    auto stream{rpc_client_.Stream<GetDatabaseHistoriesRpc>()};
    auto res = stream.SendAndWait();
    metrics::IncrementCounter(metrics::GetDatabaseHistoriesRpcSuccess);
    return res.instance_info;

  } catch (const rpc::RpcFailedException &e) {
    spdlog::error("Failed to receive response to GetDatabaseHistoriesReq. Error occurred: {}", e.what());
    metrics::IncrementCounter(metrics::GetDatabaseHistoriesRpcFail);
    return {};
  }
}

auto ReplicationInstanceClient::SendGetReplicationLagRpc() const -> std::optional<ReplicationLagInfo> {
  try {
    auto stream{rpc_client_.Stream<ReplicationLagRpc>()};
    auto res = stream.SendAndWait();
    return res.lag_info_;
  } catch (const rpc::RpcFailedException &e) {
    spdlog::error("Failed to receive response to ReplicationLagRpc. Error occurred: {}", e.what());
    return {};
  }
}

}  // namespace memgraph::coordination
#endif
