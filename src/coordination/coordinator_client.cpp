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

CoordinatorClient::CoordinatorClient(CoordinatorState *coord_state, CoordinatorClientConfig config,
                                     HealthCheckCallback succ_cb, HealthCheckCallback fail_cb)
    : rpc_context_{CreateClientContext(config)},
      rpc_client_{io::network::Endpoint(io::network::Endpoint::needs_resolving, config.ip_address, config.port),
                  &rpc_context_},
      config_{std::move(config)},
      coord_state_{coord_state},
      succ_cb_{std::move(succ_cb)},
      fail_cb_{std::move(fail_cb)} {}

CoordinatorClient::~CoordinatorClient() {
  auto exit_job = utils::OnScopeExit([&] {
    StopFrequentCheck();
    thread_pool_.Shutdown();
  });
  const auto endpoint = rpc_client_.Endpoint();
  // Logging can throw
  spdlog::trace("Closing coordinator client on {}:{}", endpoint.address, endpoint.port);
}

void CoordinatorClient::StartFrequentCheck() {
  MG_ASSERT(config_.health_check_frequency_sec > std::chrono::seconds(0),
            "Health check frequency must be greater than 0");

  std::string_view instance_name = config_.instance_name;
  replica_checker_.Run("Coord checker", config_.health_check_frequency_sec, [this, instance_name] {
    try {
      spdlog::trace("Sending frequent heartbeat to machine {} on {}:{}", instance_name, rpc_client_.Endpoint().address,
                    rpc_client_.Endpoint().port);
      auto stream{rpc_client_.Stream<memgraph::replication_coordination_glue::FrequentHeartbeatRpc>()};
      if (stream.AwaitResponse().success) {
        succ_cb_(coord_state_, instance_name);
      } else {
        fail_cb_(coord_state_, instance_name);
      }
    } catch (const rpc::RpcFailedException &) {
      fail_cb_(coord_state_, instance_name);
    }
  });
}

void CoordinatorClient::StopFrequentCheck() { replica_checker_.Stop(); }

auto CoordinatorClient::InstanceName() const -> std::string_view { return config_.instance_name; }
auto CoordinatorClient::SocketAddress() const -> std::string { return rpc_client_.Endpoint().SocketAddress(); }
auto CoordinatorClient::Config() const -> CoordinatorClientConfig const & { return config_; }
auto CoordinatorClient::SuccCallback() const -> HealthCheckCallback const & { return succ_cb_; }
auto CoordinatorClient::FailCallback() const -> HealthCheckCallback const & { return fail_cb_; }

////// AF design choice
auto CoordinatorClient::ReplicationClientInfo() const -> CoordinatorClientConfig::ReplicationClientInfo const & {
  MG_ASSERT(config_.replication_client_info.has_value(), "No ReplicationClientInfo for MAIN instance!");
  return *config_.replication_client_info;
}

////// AF design choice
auto CoordinatorClient::ReplicationClientInfo() -> std::optional<CoordinatorClientConfig::ReplicationClientInfo> & {
  MG_ASSERT(config_.replication_client_info.has_value(), "No ReplicationClientInfo for MAIN instance!");
  return config_.replication_client_info;
}

auto CoordinatorClient::SendPromoteReplicaToMainRpc(
    std::vector<CoordinatorClientConfig::ReplicationClientInfo> replication_clients_info) const -> bool {
  try {
    auto stream{rpc_client_.Stream<PromoteReplicaToMainRpc>(std::move(replication_clients_info))};
    if (!stream.AwaitResponse().success) {
      spdlog::error("Failed to perform failover!");
      return false;
    }
    spdlog::info("Sent failover RPC from coordinator to new main!");
    return true;
  } catch (const rpc::RpcFailedException &) {
    spdlog::error("Failed to send failover RPC from coordinator to new main!");
  }
  return false;
}

}  // namespace memgraph::coordination
#endif
