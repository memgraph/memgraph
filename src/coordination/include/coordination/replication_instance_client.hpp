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
#include "coordinator_rpc.hpp"

#ifdef MG_ENTERPRISE

#include "coordination/coordinator_communication_config.hpp"
#include "coordination/instance_state.hpp"
#include "replication_coordination_glue/common.hpp"
#include "rpc/client.hpp"
#include "utils/event_counter.hpp"
#include "utils/scheduler.hpp"
#include "utils/uuid.hpp"

namespace memgraph::coordination {

template <typename T>
concept IsRpc = requires {
  typename T::Request;
  typename T::Response;
};

template <IsRpc T>
struct RpcInfo {
  static const metrics::Event succCounter;
  static const metrics::Event failCounter;
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

  auto SendStateCheckRpc() const -> std::optional<InstanceState>;
  auto SendGetDatabaseHistoriesRpc() const -> std::optional<replication_coordination_glue::DatabaseHistories>;
  auto GetReplicationClientInfo() const -> ReplicationClientInfo;
  auto RpcClient() const -> rpc::Client & { return rpc_client_; }

  friend bool operator==(ReplicationInstanceClient const &first, ReplicationInstanceClient const &second) {
    return first.config_ == second.config_;
  }

  template <IsRpc T, typename... Args>
  auto SendRpc(Args &&...args) const -> bool {
    try {
      if (auto stream = rpc_client_.Stream<T>(std::forward<Args>(args)...); !stream.AwaitResponse().success) {
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
  communication::ClientContext rpc_context_;
  mutable rpc::Client rpc_client_;

  DataInstanceConfig config_;
  CoordinatorInstance *coord_instance_;

  std::chrono::seconds const instance_health_check_frequency_sec_{1};
  utils::Scheduler instance_checker_;
};

}  // namespace memgraph::coordination
#endif
