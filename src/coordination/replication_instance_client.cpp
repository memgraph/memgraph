// Copyright 2026 Memgraph Ltd.
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
RpcInfoMetrics(UpdateDataInstanceConfigRpc)
    // clang-format on

    ReplicationInstanceClient::ReplicationInstanceClient(std::string instance_name, io::network::Endpoint mgt_server,
                                                         CoordinatorInstance *coord_instance,
                                                         const std::chrono::seconds instance_health_check_frequency_sec)
    : rpc_context_{communication::ClientContext{}},
      rpc_client_{std::move(mgt_server), &rpc_context_},
      instance_name_(std::move(instance_name)),
      coord_instance_(coord_instance),
      instance_health_check_frequency_sec_(instance_health_check_frequency_sec) {}

auto ReplicationInstanceClient::InstanceName() const -> std::string const & { return instance_name_; }

void ReplicationInstanceClient::StartStateCheck() {
  if (instance_checker_.IsRunning()) {
    return;
  }

  MG_ASSERT(instance_health_check_frequency_sec_ > std::chrono::seconds(0),
            "Health check frequency must be greater than 0");

  instance_checker_.SetInterval(instance_health_check_frequency_sec_);
  instance_checker_.Run(instance_name_, [this] {
    if (auto const maybe_res = SendStateCheckRpc()) {
      coord_instance_->InstanceSuccessCallback(instance_name_, *maybe_res);
    } else {
      coord_instance_->InstanceFailCallback(instance_name_);
    }
  });
}

void ReplicationInstanceClient::StopStateCheck() { instance_checker_.Stop(); }

void ReplicationInstanceClient::PauseStateCheck() { instance_checker_.Pause(); }

void ReplicationInstanceClient::ResumeStateCheck() { instance_checker_.Resume(); }

auto ReplicationInstanceClient::SendStateCheckRpc() const -> std::optional<InstanceState> {
  auto const res = std::invoke([this]() -> std::expected<StateCheckRes, utils::RpcError> {
    utils::MetricsTimer const timer{metrics::StateCheckRpc_us};
    auto stream{rpc_client_.Stream<StateCheckRpc>()};
    if (!stream.has_value()) return std::unexpected{stream.error()};
    return stream.value().SendAndWait();
  });

  if (res.has_value()) {
    metrics::IncrementCounter(metrics::StateCheckRpcSuccess);
    return res.value().arg_;
  }
  spdlog::error("Failed to receive response to StateCheckRpc. Error occurred: {}", utils::GetRpcErrorMsg(res.error()));
  metrics::IncrementCounter(metrics::StateCheckRpcFail);
  return std::nullopt;
}

auto ReplicationInstanceClient::SendGetDatabaseHistoriesRpc() const
    -> std::optional<replication_coordination_glue::InstanceInfo> {
  auto const res = std::invoke([this]() -> std::expected<GetDatabaseHistoriesRes, utils::RpcError> {
    utils::MetricsTimer const timer{metrics::GetDatabaseHistoriesRpc_us};
    auto stream{rpc_client_.Stream<GetDatabaseHistoriesRpc>()};
    if (!stream.has_value()) return std::unexpected{stream.error()};
    return stream.value().SendAndWait();
  });

  if (res.has_value()) {
    metrics::IncrementCounter(metrics::GetDatabaseHistoriesRpcSuccess);
    return res.value().arg_;
  }
  spdlog::error("Failed to receive response to GetDatabaseHistories. Error occurred: {}",
                utils::GetRpcErrorMsg(res.error()));
  metrics::IncrementCounter(metrics::GetDatabaseHistoriesRpcFail);
  return std::nullopt;
}

auto ReplicationInstanceClient::SendGetReplicationLagRpc() const -> std::optional<ReplicationLagInfo> {
  auto const res = std::invoke([this]() -> std::expected<ReplicationLagRes, utils::RpcError> {
    auto stream{rpc_client_.Stream<ReplicationLagRpc>()};
    if (!stream.has_value()) return std::unexpected{stream.error()};
    return stream.value().SendAndWait();
  });

  if (res.has_value()) {
    return res.value().arg_;
  }
  spdlog::error("Failed to receive response to ReplicationLagRpc. Error occurred: {}",
                utils::GetRpcErrorMsg(res.error()));
  return std::nullopt;
}

}  // namespace memgraph::coordination
#endif
