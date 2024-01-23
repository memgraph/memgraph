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

#include "coordination/coordinator_config.hpp"
#include "coordination/coordinator_rpc.hpp"
#include "replication_coordination_glue/messages.hpp"

namespace memgraph::coordination {

namespace {
auto CreateClientContext(const memgraph::coordination::CoordinatorClientConfig &config)
    -> communication::ClientContext {
  return (config.ssl) ? communication::ClientContext{config.ssl->key_file, config.ssl->cert_file}
                      : communication::ClientContext{};
}
}  // namespace

CoordinatorClient::CoordinatorClient(CoordinatorData *coord_data, CoordinatorClientConfig config,
                                     HealthCheckCallback succ_cb, HealthCheckCallback fail_cb)
    : rpc_context_{CreateClientContext(config)},
      rpc_client_{io::network::Endpoint(io::network::Endpoint::needs_resolving, config.ip_address, config.port),
                  &rpc_context_},
      config_{std::move(config)},
      coord_data_{coord_data},
      succ_cb_{std::move(succ_cb)},
      fail_cb_{std::move(fail_cb)} {}

auto CoordinatorClient::InstanceName() const -> std::string { return config_.instance_name; }
auto CoordinatorClient::SocketAddress() const -> std::string { return rpc_client_.Endpoint().SocketAddress(); }

void CoordinatorClient::StartFrequentCheck() {
  MG_ASSERT(config_.health_check_frequency_sec > std::chrono::seconds(0),
            "Health check frequency must be greater than 0");

  replica_checker_.Run(
      "Coord checker", config_.health_check_frequency_sec, [this, instance_name = config_.instance_name] {
        try {
          spdlog::trace("Sending frequent heartbeat to machine {} on {}", instance_name,
                        rpc_client_.Endpoint().SocketAddress());
          auto stream{rpc_client_.Stream<memgraph::replication_coordination_glue::FrequentHeartbeatRpc>()};
          stream.AwaitResponse();
          succ_cb_(coord_data_, instance_name);
        } catch (const rpc::RpcFailedException &) {
          fail_cb_(coord_data_, instance_name);
        }
      });
}

void CoordinatorClient::StopFrequentCheck() { replica_checker_.Stop(); }

void CoordinatorClient::PauseFrequentCheck() { replica_checker_.Pause(); }
void CoordinatorClient::ResumeFrequentCheck() { replica_checker_.Resume(); }

auto CoordinatorClient::SetSuccCallback(HealthCheckCallback succ_cb) -> void { succ_cb_ = std::move(succ_cb); }
auto CoordinatorClient::SetFailCallback(HealthCheckCallback fail_cb) -> void { fail_cb_ = std::move(fail_cb); }

auto CoordinatorClient::ReplicationClientInfo() const
    -> const std::optional<CoordinatorClientConfig::ReplicationClientInfo> & {
  return config_.replication_client_info;
}

auto CoordinatorClient::ResetReplicationClientInfo() -> void { config_.replication_client_info.reset(); }

auto CoordinatorClient::SendPromoteReplicaToMainRpc(
    std::vector<CoordinatorClientConfig::ReplicationClientInfo> replication_clients_info) const -> bool {
  try {
    auto stream{rpc_client_.Stream<PromoteReplicaToMainRpc>(std::move(replication_clients_info))};
    if (!stream.AwaitResponse().success) {
      spdlog::error("Failed to receive successful RPC failover response!");
      return false;
    }
    return true;
  } catch (const rpc::RpcFailedException &) {
    spdlog::error("RPC error occurred while sending failover RPC!");
  }
  return false;
}

auto CoordinatorClient::SendSetToReplicaRpc(CoordinatorClient::ReplClientInfo replication_client_info) const -> bool {
  try {
    auto stream{rpc_client_.Stream<SetMainToReplicaRpc>(std::move(replication_client_info))};
    if (!stream.AwaitResponse().success) {
      spdlog::error("Failed to set main to replica!");
      return false;
    }
    spdlog::info("Sent request RPC from coordinator to instance to set it as replica!");
    return true;
  } catch (const rpc::RpcFailedException &) {
    spdlog::error("Failed to send failover RPC from coordinator to new main!");
  }
  return false;
}

}  // namespace memgraph::coordination
#endif
