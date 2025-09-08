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

#pragma once

#ifdef MG_ENTERPRISE

#include "coordination/coordinator_communication_config.hpp"
#include "coordination/coordinator_rpc.hpp"
#include "coordination/instance_state.hpp"
#include "replication_coordination_glue/common.hpp"
#include "rpc/client.hpp"
#include "utils/event_counter.hpp"
#include "utils/metrics_timer.hpp"
#include "utils/scheduler.hpp"

namespace memgraph::metrics {
// NOLINTNEXTLINE(cppcoreguidelines-macro-usage)
#define GenerateRpcCounterEvents(RPC) \
  extern const Event RPC##Success;    \
  extern const Event RPC##Fail;       \
  extern const Event RPC##_us;

// clang-format off
GenerateRpcCounterEvents(PromoteToMainRpc)
GenerateRpcCounterEvents(DemoteMainToReplicaRpc)
GenerateRpcCounterEvents(RegisterReplicaOnMainRpc)
GenerateRpcCounterEvents(UnregisterReplicaRpc)
GenerateRpcCounterEvents(EnableWritingOnMainRpc)
GenerateRpcCounterEvents(StateCheckRpc)
GenerateRpcCounterEvents(GetDatabaseHistoriesRpc)
// clang-format on
}  // namespace memgraph::metrics

namespace memgraph::coordination {

template <rpc::IsRpc T>
struct RpcInfo {
  static const metrics::Event succCounter;
  static const metrics::Event failCounter;
  static const metrics::Event timerLabel;
};

class CoordinatorInstance;
using ReplicationClientsInfo = std::vector<ReplicationClientInfo>;

class ReplicationInstanceClient {
 public:
  explicit ReplicationInstanceClient(DataInstanceConfig config, CoordinatorInstance *coord_instance,
                                     std::chrono::seconds instance_health_check_frequency_sec);

  ~ReplicationInstanceClient() = default;

  ReplicationInstanceClient(ReplicationInstanceClient &) = delete;
  ReplicationInstanceClient &operator=(ReplicationInstanceClient const &) = delete;

  ReplicationInstanceClient(ReplicationInstanceClient &&) noexcept = delete;
  ReplicationInstanceClient &operator=(ReplicationInstanceClient &&) noexcept = delete;

  void StartStateCheck();
  void StopStateCheck();
  void PauseStateCheck();
  void ResumeStateCheck();

  auto InstanceName() const -> std::string;
  auto BoltSocketAddress() const -> std::string;
  auto ManagementSocketAddress() const -> std::string;
  auto ReplicationSocketAddress() const -> std::string;

  auto SendGetDatabaseHistoriesRpc() const -> std::optional<replication_coordination_glue::InstanceInfo>;
  auto SendGetReplicationLagRpc() const -> std::optional<ReplicationLagInfo>;
  auto GetReplicationClientInfo() const -> ReplicationClientInfo;
  auto RpcClient() const -> rpc::Client & { return rpc_client_; }

  friend bool operator==(ReplicationInstanceClient const &first, ReplicationInstanceClient const &second) {
    return first.config_ == second.config_;
  }

  template <rpc::IsRpc T, typename... Args>
  auto SendRpc(Args &&...args) const -> bool {
    utils::MetricsTimer const timer{RpcInfo<T>::timerLabel};
    try {
      // Instead of retrieving config_.replication_client_info and sending it again into this function, we have this
      // compile-time switch which decides specifically to ship config_.replication_client_info for
      // DemoteMainToReplicaRpc
      auto stream = std::invoke([&]() {
        if constexpr (std::same_as<T, DemoteMainToReplicaRpc>) {
          return rpc_client_.Stream<T>(config_.replication_client_info, std::forward<Args>(args)...);
        } else {
          return rpc_client_.Stream<T>(std::forward<Args>(args)...);
        }
      });

      if (!stream.SendAndWait().success) {
        spdlog::error("Received unsuccessful response to {}.", T::Request::kType.name);
        metrics::IncrementCounter(RpcInfo<T>::failCounter);
        return false;
      }

      metrics::IncrementCounter(RpcInfo<T>::succCounter);
      return true;
    } catch (rpc::RpcFailedException const &e) {
      spdlog::error("Failed to receive response to {}. Error occurred: {}", T::Request::kType.name, e.what());
      metrics::IncrementCounter(RpcInfo<T>::failCounter);
      return false;
    }
  }

 private:
  auto SendStateCheckRpc() const -> std::optional<InstanceState>;

  communication::ClientContext rpc_context_;
  mutable rpc::Client rpc_client_;

  DataInstanceConfig config_;
  CoordinatorInstance *coord_instance_;

  std::chrono::seconds instance_health_check_frequency_sec_{1};
  utils::Scheduler instance_checker_;
};

}  // namespace memgraph::coordination
#endif
