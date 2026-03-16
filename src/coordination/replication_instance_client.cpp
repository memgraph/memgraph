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
#define RpcInfoSpecialize(RPC, SUCC, FAIL, HIST)                                                             \
  template <>                                                                                                \
  prometheus::Counter *RpcInfo<RPC>::succ_counter(metrics::GlobalMetricHandles &g) { return g.SUCC; }       \
  template <>                                                                                                \
  prometheus::Counter *RpcInfo<RPC>::fail_counter(metrics::GlobalMetricHandles &g) { return g.FAIL; }       \
  template <>                                                                                                \
  prometheus::Histogram *RpcInfo<RPC>::histogram(metrics::GlobalMetricHandles &g) { return g.HIST; }

RpcInfoSpecialize(PromoteToMainRpc,            promote_to_main_rpc_success,               promote_to_main_rpc_fail,               promote_to_main_rpc_seconds)
RpcInfoSpecialize(DemoteMainToReplicaRpc,      demote_main_to_replica_rpc_success,        demote_main_to_replica_rpc_fail,        demote_main_to_replica_rpc_seconds)
RpcInfoSpecialize(RegisterReplicaOnMainRpc,    register_replica_on_main_rpc_success,      register_replica_on_main_rpc_fail,      register_replica_on_main_rpc_seconds)
RpcInfoSpecialize(UnregisterReplicaRpc,        unregister_replica_rpc_success,            unregister_replica_rpc_fail,            unregister_replica_rpc_seconds)
RpcInfoSpecialize(EnableWritingOnMainRpc,      enable_writing_on_main_rpc_success,        enable_writing_on_main_rpc_fail,        enable_writing_on_main_rpc_seconds)
RpcInfoSpecialize(UpdateDataInstanceConfigRpc, update_data_instance_config_rpc_success,   update_data_instance_config_rpc_fail,   update_data_instance_config_rpc_seconds)
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

void ReplicationInstanceClient::UpdateHealthCheckFrequencySec(std::chrono::seconds const &new_config) const {
  instance_checker_.SetInterval(new_config);
}

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
  auto &g = metrics::Metrics().global;
  auto const _t0 = std::chrono::high_resolution_clock::now();
  utils::OnScopeExit const _timer{[&] {
    g.state_check_rpc_seconds->Observe(
        std::chrono::duration<double>(std::chrono::high_resolution_clock::now() - _t0).count());
  }};
  try {
    auto stream{rpc_client_.Stream<StateCheckRpc>()};
    auto res = stream.SendAndWait();
    g.state_check_rpc_success->Increment();
    return res.arg_;
  } catch (rpc::RpcFailedException const &e) {
    spdlog::error("Failed to receive response to StateCheckRpc. Error occurred: {}", e.what());
    g.state_check_rpc_fail->Increment();
    return {};
  }
}

auto ReplicationInstanceClient::SendGetDatabaseHistoriesRpc() const
    -> std::optional<replication_coordination_glue::InstanceInfo> {
  auto &g = metrics::Metrics().global;
  auto const _t0 = std::chrono::high_resolution_clock::now();
  utils::OnScopeExit const _timer{[&] {
    g.get_database_histories_rpc_seconds->Observe(
        std::chrono::duration<double>(std::chrono::high_resolution_clock::now() - _t0).count());
  }};
  try {
    auto stream{rpc_client_.Stream<GetDatabaseHistoriesRpc>()};
    auto res = stream.SendAndWait();
    g.get_database_histories_rpc_success->Increment();
    return res.arg_;
  } catch (const rpc::RpcFailedException &e) {
    spdlog::error("Failed to receive response to GetDatabaseHistoriesReq. Error occurred: {}", e.what());
    g.get_database_histories_rpc_fail->Increment();
    return {};
  }
}

auto ReplicationInstanceClient::SendGetReplicationLagRpc() const -> std::optional<ReplicationLagInfo> {
  try {
    auto stream{rpc_client_.Stream<ReplicationLagRpc>()};
    auto res = stream.SendAndWait();
    return res.arg_;
  } catch (const rpc::RpcFailedException &e) {
    spdlog::error("Failed to receive response to ReplicationLagRpc. Error occurred: {}", e.what());
    return {};
  }
}

}  // namespace memgraph::coordination
#endif
