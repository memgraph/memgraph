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

#pragma once

#ifdef MG_ENTERPRISE

#include "coordination/coordinator_communication_config.hpp"
#include "coordination/instance_state.hpp"
#include "coordination/replication_lag_info.hpp"
#include "metrics/prometheus_metrics.hpp"
#include "replication_coordination_glue/common.hpp"
#include "rpc/client.hpp"
#include "utils/on_scope_exit.hpp"
#include "utils/scheduler.hpp"

namespace memgraph::coordination {

template <rpc::IsRpc T>
struct RpcInfo {
  static prometheus::Counter *succ_counter(metrics::GlobalMetricHandles &g);
  static prometheus::Counter *fail_counter(metrics::GlobalMetricHandles &g);
  static prometheus::Histogram *histogram(metrics::GlobalMetricHandles &g);
};

class CoordinatorInstance;
using ReplicationClientsInfo = std::vector<ReplicationClientInfo>;

class ReplicationInstanceClient {
 public:
  explicit ReplicationInstanceClient(std::string instance_name, io::network::Endpoint mgt_server,
                                     CoordinatorInstance *coord_instance,
                                     std::chrono::seconds instance_health_check_frequency_sec);

  ~ReplicationInstanceClient() = default;

  ReplicationInstanceClient(ReplicationInstanceClient &) = delete;
  ReplicationInstanceClient &operator=(ReplicationInstanceClient const &) = delete;

  ReplicationInstanceClient(ReplicationInstanceClient &&) noexcept = delete;
  ReplicationInstanceClient &operator=(ReplicationInstanceClient &&) noexcept = delete;

  void UpdateHealthCheckFrequencySec(std::chrono::seconds const &new_config) const;

  void StartStateCheck();
  void StopStateCheck();
  void PauseStateCheck();
  void ResumeStateCheck();

  auto InstanceName() const -> std::string const &;

  auto SendGetDatabaseHistoriesRpc() const -> std::optional<replication_coordination_glue::InstanceInfo>;
  auto SendGetReplicationLagRpc() const -> std::optional<ReplicationLagInfo>;

  auto RpcClient() const -> rpc::Client & { return rpc_client_; }

  friend bool operator==(ReplicationInstanceClient const &first, ReplicationInstanceClient const &second) {
    return first.instance_name_ == second.instance_name_;
  }

  template <rpc::IsRpc T, typename... Args>
  auto SendRpc(Args &&...args) const -> bool {
    auto &g = metrics::Metrics().global;
    auto const _t0 = std::chrono::high_resolution_clock::now();
    utils::OnScopeExit const _timer{[&] {
      RpcInfo<T>::histogram(g)->Observe(
          std::chrono::duration<double>(std::chrono::high_resolution_clock::now() - _t0).count());
    }};
    try {
      auto stream = rpc_client_.Stream<T>(std::forward<Args>(args)...);

      if (!stream.SendAndWait().arg_) {
        spdlog::error("Received unsuccessful response to {}.", T::Request::kType.name);
        RpcInfo<T>::fail_counter(g)->Increment();
        return false;
      }

      RpcInfo<T>::succ_counter(g)->Increment();
      return true;
    } catch (rpc::RpcFailedException const &e) {
      spdlog::error("Failed to receive response to {}. Error occurred: {}", T::Request::kType.name, e.what());
      RpcInfo<T>::fail_counter(g)->Increment();
      return false;
    }
  }

 private:
  auto SendStateCheckRpc() const -> std::optional<InstanceState>;

  communication::ClientContext rpc_context_;
  mutable rpc::Client rpc_client_;

  std::string instance_name_;
  CoordinatorInstance *coord_instance_;

  std::chrono::seconds instance_health_check_frequency_sec_{1};
  mutable utils::Scheduler instance_checker_;
};

}  // namespace memgraph::coordination
#endif
