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

#include "utils/uuid.hpp"
#ifdef MG_ENTERPRISE

#include "coordination/coordinator_client.hpp"

#include "coordination/coordinator_config.hpp"
#include "coordination/coordinator_rpc.hpp"
#include "replication_coordination_glue/messages.hpp"

namespace memgraph::coordination {

namespace {
auto CreateClientContext(memgraph::coordination::CoordinatorClientConfig const &config)
    -> communication::ClientContext {
  return (config.ssl) ? communication::ClientContext{config.ssl->key_file, config.ssl->cert_file}
                      : communication::ClientContext{};
}
}  // namespace

CoordinatorClient::CoordinatorClient(CoordinatorInstance *coord_instance, CoordinatorClientConfig config,
                                     HealthCheckCallback succ_cb, HealthCheckCallback fail_cb)
    : rpc_context_{CreateClientContext(config)},
      rpc_client_{io::network::Endpoint(io::network::Endpoint::needs_resolving, config.ip_address, config.port),
                  &rpc_context_},
      config_{std::move(config)},
      coord_instance_{coord_instance},
      succ_cb_{std::move(succ_cb)},
      fail_cb_{std::move(fail_cb)} {}

auto CoordinatorClient::InstanceName() const -> std::string { return config_.instance_name; }
auto CoordinatorClient::SocketAddress() const -> std::string { return rpc_client_.Endpoint().SocketAddress(); }

auto CoordinatorClient::InstanceDownTimeoutSec() const -> std::chrono::seconds {
  return config_.instance_down_timeout_sec;
}

void CoordinatorClient::StartFrequentCheck() {
  if (instance_checker_.IsRunning()) {
    return;
  }

  MG_ASSERT(config_.instance_health_check_frequency_sec > std::chrono::seconds(0),
            "Health check frequency must be greater than 0");

  instance_checker_.Run(
      config_.instance_name, config_.instance_health_check_frequency_sec,
      [this, instance_name = config_.instance_name] {
        try {
          spdlog::trace("Sending frequent heartbeat to machine {} on {}", instance_name,
                        rpc_client_.Endpoint().SocketAddress());
          {  // NOTE: This is intentionally scoped so that stream lock could get released.
            auto stream{rpc_client_.Stream<memgraph::replication_coordination_glue::FrequentHeartbeatRpc>()};
            stream.AwaitResponse();
          }
          succ_cb_(coord_instance_, instance_name);
        } catch (rpc::RpcFailedException const &) {
          fail_cb_(coord_instance_, instance_name);
        }
      });
}

void CoordinatorClient::StopFrequentCheck() { instance_checker_.Stop(); }
void CoordinatorClient::PauseFrequentCheck() { instance_checker_.Pause(); }
void CoordinatorClient::ResumeFrequentCheck() { instance_checker_.Resume(); }

auto CoordinatorClient::SetCallbacks(HealthCheckCallback succ_cb, HealthCheckCallback fail_cb) -> void {
  succ_cb_ = std::move(succ_cb);
  fail_cb_ = std::move(fail_cb);
}

auto CoordinatorClient::ReplicationClientInfo() const -> ReplClientInfo { return config_.replication_client_info; }

auto CoordinatorClient::SendPromoteReplicaToMainRpc(const utils::UUID &uuid,
                                                    ReplicationClientsInfo replication_clients_info) const -> bool {
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

auto CoordinatorClient::DemoteToReplica() const -> bool {
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

auto CoordinatorClient::SendSwapMainUUIDRpc(const utils::UUID &uuid) const -> bool {
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

auto CoordinatorClient::SendUnregisterReplicaRpc(std::string const &instance_name) const -> bool {
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

auto CoordinatorClient::SendEnableWritingOnMainRpc() const -> bool {
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

}  // namespace memgraph::coordination
#endif
