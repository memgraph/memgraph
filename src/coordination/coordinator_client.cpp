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

CoordinatorClient::CoordinatorClient(const CoordinatorClientConfig &config)
    : rpc_context_{CreateClientContext(config)},
      rpc_client_{io::network::Endpoint(io::network::Endpoint::needs_resolving, config.ip_address, config.port),
                  &rpc_context_},
      config_{config} {}

CoordinatorClient::~CoordinatorClient() {
  auto exit_job = utils::OnScopeExit([&] {
    StopFrequentCheck();
    thread_pool_.Shutdown();
  });
  const auto endpoint = rpc_client_.Endpoint();
  // Logging can throw
  spdlog::trace("Closing replication client on {}:{}", endpoint.address, endpoint.port);
}

void CoordinatorClient::StartFrequentCheck() {
  MG_ASSERT(config_.health_check_frequency_sec > std::chrono::seconds(0),
            "Health check frequency must be greater than 0");
  replica_checker_.Run(
      "Coord checker", config_.health_check_frequency_sec,
      [last_response_time = &last_response_time_, rpc_client = &rpc_client_] {
        try {
          {
            auto stream{rpc_client->Stream<memgraph::replication_coordination_glue::FrequentHeartbeatRpc>()};
            stream.AwaitResponse();
            last_response_time->store(std::chrono::system_clock::now(), std::memory_order_acq_rel);
          }
        } catch (const rpc::RpcFailedException &) {
          // Nothing to do...wait for a reconnect
        }
      });
}

void CoordinatorClient::StopFrequentCheck() { replica_checker_.Stop(); }

bool CoordinatorClient::DoHealthCheck() const {
  auto current_time = std::chrono::system_clock::now();
  auto duration = std::chrono::duration_cast<std::chrono::seconds>(current_time -
                                                                   last_response_time_.load(std::memory_order_acquire));
  return duration.count() <= alive_response_time_difference_sec_;
}

auto CoordinatorClient::InstanceName() const -> std::string_view { return config_.instance_name; }
auto CoordinatorClient::Endpoint() const -> io::network::Endpoint const & { return rpc_client_.Endpoint(); }
auto CoordinatorClient::Config() const -> CoordinatorClientConfig const & { return config_; }

auto CoordinatorClient::ReplicationClientInfo() const -> CoordinatorClientConfig::ReplicationClientInfo const & {
  MG_ASSERT(config_.replication_client_info.has_value(), "No ReplicationClientInfo for MAIN instance!");
  return *config_.replication_client_info;
}

////// AF design choice
auto CoordinatorClient::ReplicationClientInfo() -> std::optional<CoordinatorClientConfig::ReplicationClientInfo> & {
  MG_ASSERT(config_.replication_client_info.has_value(), "No ReplicationClientInfo for MAIN instance!");
  return config_.replication_client_info;
}

void CoordinatorClient::UpdateTimeCheck(const std::chrono::system_clock::time_point &last_checked_time) {
  last_response_time_.store(last_checked_time, std::memory_order_acq_rel);
}

auto CoordinatorClient::GetLastTimeResponse() -> std::chrono::system_clock::time_point { return last_response_time_; }

auto CoordinatorClient::SendPromoteReplicaToMainRpc(
    std::vector<CoordinatorClientConfig::ReplicationClientInfo> replication_clients_info) const -> bool {
  try {
    {
      auto stream{rpc_client_.Stream<PromoteReplicaToMainRpc>(std::move(replication_clients_info))};
      if (!stream.AwaitResponse().success) {
        spdlog::error("Failed to perform failover!");
        return false;
      }
      spdlog::info("Sent failover RPC from coordinator to new main!");
      return true;
    }
  } catch (const rpc::RpcFailedException &) {
    spdlog::error("Failed to send failover RPC from coordinator to new main!");
  }
  return false;
}

}  // namespace memgraph::coordination
#endif
